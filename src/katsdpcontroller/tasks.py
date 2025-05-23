################################################################################
# Copyright (c) 2013-2025, National Research Foundation (SARAO)
#
# Licensed under the BSD 3-Clause License (the "License"); you may not use
# this file except in compliance with the License. You may obtain a copy
# of the License at
#
#   https://opensource.org/licenses/BSD-3-Clause
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

import asyncio
import ipaddress
import json
import logging
import os
import socket
import typing
from collections import defaultdict
from contextlib import asynccontextmanager
from typing import (
    AsyncContextManager,
    AsyncGenerator,
    Callable,
    List,
    MutableMapping,
    Set,
    Type,
    TypeVar,
    Union,
)

import aiokatcp
import async_timeout
import yarl
from addict import Dict
from aiokatcp import FailReply, InvalidReply, Sensor
from katsdptelstate.endpoint import Endpoint, endpoint_list_parser
from prometheus_client import Histogram

from . import product_config, scheduler, sensor_proxy
from .consul import CONSUL_PORT, ConsulService
from .defaults import LOCALHOST

_T = TypeVar("_T")

logger = logging.getLogger(__name__)
# Name of edge attribute, as a constant to better catch typos
DEPENDS_INIT = "depends_init"
# Buckets appropriate for measuring postprocessing task times (in seconds)
POSTPROCESSING_TIME_BUCKETS = [
    1 * 60,
    2 * 60,
    5 * 60,
    10 * 60,
    20 * 60,
    30 * 60,
    40 * 60,
    50 * 60,
    1 * 3600,
    2 * 3600,
    3 * 3600,
    4 * 3600,
    5 * 3600,
    6 * 3600,
    8 * 3600,
    10 * 3600,
    12 * 3600,
    14 * 3600,
    16 * 3600,
    18 * 3600,
    20 * 3600,
    22 * 3600,
    24 * 3600,
]
# Buckets appropriate for measuring processing times relative to observation
# times.
POSTPROCESSING_REL_BUCKETS = [
    0.001,
    0.0025,
    0.005,
    0.0075,
    0.01,
    0.025,
    0.05,
    0.075,
    0.1,
    0.25,
    0.5,
    0.75,
    1.0,
    1.5,
    2.5,
    5.0,
    7.5,
    10.0,
]
BATCH_RUNTIME = Histogram(
    "katsdpcontroller_batch_runtime_seconds",
    "Wall-clock execution time of batch tasks",
    ["task_type"],
    buckets=POSTPROCESSING_TIME_BUCKETS,
)
BATCH_RUNTIME_REL = Histogram(
    "katsdpcontroller_batch_runtime_rel",
    "Wall-clock execution time of batch jobs as a fraction of data length",
    ["task_type"],
    buckets=POSTPROCESSING_REL_BUCKETS,
)


class CaptureBlockState(scheduler.OrderedEnum):
    """State of a single capture block."""

    INITIALISING = 0  # Only occurs briefly on construction
    CAPTURING = 1  # capture-init called, capture-done not yet called
    BURNDOWN = 2  # capture-done returned, but real-time processing still happening
    POSTPROCESSING = 3  # real-time processing complete, running batch processing
    DEAD = 4  # fully complete


class KatcpTransition:
    """A katcp request to issue on a state transition

    Parameters
    ----------
    name : str
        Request name
    *args : str
        Request arguments. String arguments are passed through
        :meth:`str.format`: see
        :meth:`.SubarrayProduct.exec_transitions` for the keys that can
        be substituted.
    timeout : float
        Maximum time to wait for the query to succeed.
    """

    def __init__(self, name, *args, timeout=None):
        self.name = name
        self.args = args
        if timeout is None:
            raise ValueError("timeout is required")
        self.timeout = timeout

    def format(self, *args, **kwargs):
        """Apply string formatting to each argument and return a new object"""
        formatted_args = [
            arg.format(*args, **kwargs) if isinstance(arg, str) else arg for arg in self.args
        ]
        return KatcpTransition(self.name, *formatted_args, timeout=self.timeout)

    def __repr__(self):
        args = [f"{arg!r}" for arg in (self.name,) + self.args]
        return "KatcpTransition({}, timeout={!r})".format(", ".join(args), self.timeout)


class ProductLogicalTask(scheduler.LogicalTask):
    def __init__(self, name, streams=(), index=None):
        super().__init__(name)
        self.task_type = name.split(".", 1)[0]
        self.streams = list(streams)
        self.index = index
        self.stream_names = frozenset(stream.name for stream in streams)
        self.physical_factory = ProductPhysicalTask
        self.fake_katcp_server_cls: Type[FakeDeviceServer] = FakeDeviceServer
        self.transitions = {}
        # Capture block state at which the node no longer deals with the capture block
        self.final_state = CaptureBlockState.BURNDOWN
        # List of dictionaries for a .gui-urls sensor. The fields are expanded
        # using str.format(self).
        self.gui_urls = []
        # Overrides for sensor name remapping
        self.sensor_renames = {}
        # Passes values to SubarrayProduct to set as gauges
        self.static_gauges: MutableMapping[str, float] = defaultdict(float)
        # Whether we should abort the capture block if the task fails
        self.critical = True
        # Whether to set config keys in telstate (only useful for processes that use katsdpservices)
        self.katsdpservices_config = True
        # Whether to report katcp sensors about the task (state, version etc).
        # This is separate from mirroring katcp sensors generated by the task
        # itself. It should generally be disabled for batch tasks for avoid
        # overloading CAM.
        self.metadata_katcp_sensors = True
        # Set to true if the image uses katsdpservices.setup_logging() and hence
        # can log directly to logstash without logspout.
        self.katsdpservices_logging = True
        # Set to true if the task should receive --telstate on the command line.
        self.pass_telstate = True
        # Set to a time in seconds to indicate time spent collecting the data
        # to be processed.
        self.batch_data_time = None
        # Tell the product controller to pass extra arguments to the physical_factory.
        self.sdp_physical_factory = True
        # Optional sensors of 0's/1's that will be updated if the task dies. The bits
        # in the range given by the tuple data_suspect_range will be set to 1's.
        self.data_suspect_sensors: List[Sensor[str]] = []
        self.data_suspect_range = (0, 0)
        # Extra metadata to populate in consul
        self.consul_meta: typing.Dict[str, str] = {}


class ConfigMixin:
    """Mixin class that takes config information from the graph and sets it in telstate."""

    def _graph_config(self, resolver, graph):
        return graph.nodes[self].get("config", lambda task_, resolver_: {})(self, resolver)

    async def resolve(self, resolver, graph, image=None):
        await super().resolve(resolver, graph, image)
        if not self.logical_node.katsdpservices_config:
            if self._graph_config(resolver, graph):
                logger.warning(
                    "Graph node %s has explicit config but katsdpservices_config=False", self.name
                )
            return
        if not resolver.telstate:
            if self._graph_config(resolver, graph):
                logger.warning(
                    "Graph node %s has explicit config but there is no telstate", self.name
                )
            return

        # Not every task will take a --external-hostname option, but the
        # katsdpservices argument parser doesn't mind unused arguments.
        config = {}
        if self.host is not None:  # Can happen if this isn't a PhysicalTask
            config["external_hostname"] = self.host
        for name, value in self.ports.items():
            config[name] = value
        for _src, trg, attr in graph.out_edges(self, data=True):
            endpoint = None
            if "port" in attr and trg.state >= scheduler.TaskState.STARTING:
                port = attr["port"]
                endpoint = Endpoint(trg.address, trg.ports[port])
            config.update(
                attr.get("config", lambda task_, resolver_, endpoint_: {})(self, resolver, endpoint)
            )
        config.update(self._graph_config(resolver, graph))
        overrides = resolver.service_overrides.get(
            self.logical_node.name, product_config.ServiceOverride()
        ).config
        if overrides:
            logger.warning("Overriding config for %s", self.name)
            config = product_config.override(config, overrides)
        logger.debug("Config for %s: %s", self.name, config)
        self.task_config = config
        if config:
            await resolver.telstate.set("config." + self.logical_node.name, config)


class CaptureBlockStateObserver:
    """Watches a capture-block-state sensor in a child.
    Users can wait for specific conditions to be satisfied.
    """

    def __init__(self, sensor, logger):
        self.sensor = sensor
        self.logger = logger
        self._last = {}
        self._waiters = []  # Each a tuple of a predicate and a future
        self(sensor, sensor.reading)
        sensor.attach(self)

    def __call__(self, sensor, reading):
        if reading.status in [Sensor.Status.NOMINAL, Sensor.Status.WARN, Sensor.Status.ERROR]:
            try:
                value = json.loads(reading.value.decode("utf-8"))
            except ValueError:
                self.logger.warning("Invalid JSON in %s: %r", sensor.name, reading.value)
            else:
                if not isinstance(value, dict):
                    self.logger.warning("%s is not a dict: %r", sensor.name, reading.value)
                else:
                    self._last = value
                    self._trigger()

    def _trigger(self):
        """Called when the sensor value changes, to wake up waiters"""
        new_waiters = []
        for waiter in self._waiters:
            if not waiter[1].done():  # Skip over cancelled futures
                if waiter[0](self._last):
                    waiter[1].set_result(None)
                else:
                    new_waiters.append(waiter)  # Not ready yet, keep for later
        self._waiters = new_waiters

    async def wait(self, condition):
        if condition(self._last):
            return  # Already satisfied, no need to wait
        future = asyncio.Future()
        self._waiters.append((condition, future))
        await future

    async def wait_capture_block_done(self, capture_block_id):
        await self.wait(lambda value: capture_block_id not in value)

    def close(self):
        """Close down the observer. This should be called when the connection
        to the server is closed. It sets the capture block states list to
        empty (which will wake up any waiters whose condition is satisfied by
        this). Any remaining waiters receive a
        :exc:`.asyncio.ConnectionResetError`.
        """
        self.sensor.detach(self)
        self._last = {}
        self._trigger()  # Give waiters a chance to react to an empty map
        for waiter in self._waiters:
            waiter[1].set_exception(ConnectionResetError())
        self._waiters = []


class DeviceStatusObserver:
    """Watches a device-status sensor from a child, and reports when it is in error."""

    def __init__(self, sensor, task):
        self.sensor = sensor
        self.task = task
        sensor.attach(self)
        # Arrange to observe the initial value
        asyncio.get_event_loop().call_soon(self, sensor, sensor.reading)

    def __call__(self, sensor, reading):
        if reading.status == Sensor.Status.ERROR:
            self.task.subarray_product.bad_device_status(self.task)

    def close(self):
        self.sensor.detach(self)


class _ElidedArgs:
    """Reports request arguments elided from a log message."""

    def __init__(self, n: int) -> None:
        self.n = n

    def __repr__(self) -> str:
        return f"[{self.n} more arguments omitted]"


class ProductPhysicalTaskMixin(scheduler.PhysicalNode):
    """Augments task classes with functionality specific to subarray products.

    This class is used in a mixin with either :class:`scheduler.PhysicalTask`
    or :class:`scheduler.FakePhysicalTask`.

    It
    - Tracks the owning controller, subarray product and capture block ID
    - Provides a number of internal katcp sensors
    - Tracks live capture block IDs for this task
    - Connects to the service's katcp port and mirrors katcp sensors. Such
      are exposed at the product controller level using the
      following syntax:
        <name>.<sensor_name>
      For example:
        ingest.sdp_l0.1.input_rate
    """

    def __init__(
        self,
        logical_task: ProductLogicalTask,
        sdp_controller,
        subarray_product,
        capture_block_id: str,
    ) -> None:
        # Turn .status into a property that updates a sensor
        self._status = None
        self.sdp_controller = sdp_controller
        self.subarray_product = subarray_product
        self.capture_block_id = capture_block_id  # Only useful for batch tasks
        self.logger = logging.LoggerAdapter(logger, dict(child_task=self.name))
        if capture_block_id is None:
            self.name = logical_task.name
        else:
            self.name = ".".join([capture_block_id, logical_task.name])
        self.gui_urls: List[dict] = []
        # Capture block names for CBs that haven't terminated on this node yet.
        # Names are used rather than the objects to reduce the number of cyclic
        # references.
        self._capture_blocks: Set[str] = set()
        # Event set to true whenever _capture_blocks is empty
        self._capture_blocks_empty = asyncio.Event()
        self._capture_blocks_empty.set()

        self._state_sensor = Sensor(
            scheduler.TaskState,
            self.name + ".state",
            "State of the state machine",
            "",
            default=self.state,
            initial_status=Sensor.Status.NOMINAL,
        )
        self._mesos_state_sensor = Sensor(
            str, self.name + ".mesos-state", "Mesos-reported task state"
        )
        self._version_sensor = Sensor(str, self.name + ".version", "Image of executing container")
        self._source_sensor = Sensor(
            str, self.name + ".source", "Version control source for the container"
        )
        self._revision_sensor = Sensor(
            str, self.name + ".revision", "Version control revision for the container"
        )
        self._host_sensor = Sensor(str, self.name + ".host", "Host running the task")
        self._interface_sensors = {}
        for request in self.logical_node.interfaces:
            self._interface_sensors[request.network] = Sensor(
                str,
                f"{self.name}.interfaces.{request.network}.name",
                f"Network interface for connection to the '{request.network}' network",
            )
        if logical_task.metadata_katcp_sensors:
            # Note: these sensors are added to the subarray product and not self
            # so that they don't get removed when the task dies. The sensors
            # themselves are created unconditionally because it avoids having to
            # make all the updates conditional.
            self.subarray_product.add_sensor(self._state_sensor)
            self.subarray_product.add_sensor(self._mesos_state_sensor)
            self.subarray_product.add_sensor(self._version_sensor)
            self.subarray_product.add_sensor(self._source_sensor)
            self.subarray_product.add_sensor(self._revision_sensor)
            self.subarray_product.add_sensor(self._host_sensor)
            for sensor in self._interface_sensors.values():
                self.subarray_product.add_sensor(sensor)

        self.katcp_connection = None
        self.capture_block_state_observer = None
        self.device_status_observer = None

    @property
    def subarray_product_id(self):
        return self.subarray_product.subarray_product_id

    def get_transition(self, state):
        """Get state transition actions"""
        return self.logical_node.transitions.get(state, [])

    async def issue_req(self, req, args=(), timeout=None, log_level=logging.INFO):
        """Issue a request to the katcp connection.

        The reply and informs are returned. If the request failed, a log
        message is printed, a FailReply is raised. If the failure is due to
        a timeout or an OSError, the data-suspect flags are set.
        """
        if self.katcp_connection is None:
            raise FailReply(
                f"Cannot issue request {req} to node {self.name} without a katcp connection"
            )
        log_args = args
        # Limit the number of arguments we report to avoid spamming the logs
        # with huge messages. But don't bother trimming if it won't be logged
        # anyway.
        if self.logger.isEnabledFor(log_level):
            max_args = 20
            if len(args) > max_args:
                log_args = tuple(args[:max_args]) + (_ElidedArgs(len(args) - max_args),)
        if timeout is None:
            self.logger.log(
                log_level,
                "Issuing request %s %s to node %s (no timeout)",
                req,
                log_args,
                self.name,
            )
        else:
            self.logger.log(
                log_level,
                "Issuing request %s %s to node %s (timeout %gs)",
                req,
                log_args,
                self.name,
                timeout,
            )
        try:
            async with async_timeout.timeout(timeout):
                await self.katcp_connection.wait_connected()
                reply, informs = await self.katcp_connection.request(req, *args)
            self.logger.log(
                log_level, "Request %s %s to node %s successful", req, log_args, self.name
            )
            return (reply, informs)
        except (FailReply, InvalidReply, OSError, asyncio.TimeoutError) as error:
            if isinstance(error, asyncio.TimeoutError):
                error_msg = f"Timed out after {timeout} s."
            else:
                error_msg = str(error)
            msg = f"Failed to issue req {req} to node {self.name}. {error_msg}"
            self.logger.warning("%s", msg)
            if not isinstance(error, (FailReply, InvalidReply)):
                self.mark_suspect()
            raise FailReply(msg) from error

    async def wait_ready(self):
        if not await super().wait_ready():
            return False
        # establish katcp connection to this node if appropriate
        if "port" in self.ports:
            while True:
                self.logger.info(
                    "Attempting to establish katcp connection to %s:%s for node %s",
                    self.address,
                    self.ports["port"],
                    self.name,
                )
                prefix = self.name + "."
                self.katcp_connection = sensor_proxy.SensorProxyClient(
                    self.sdp_controller,
                    prefix,
                    renames=self.logical_node.sensor_renames,
                    close_action=sensor_proxy.CloseAction.UNREACHABLE,
                    host=self.address,
                    port=self.ports["port"],
                    notify=self.subarray_product.notify_sensors_changed,
                )
                try:
                    await self.katcp_connection.wait_synced()
                    self.logger.info(
                        "Connected to %s:%s for node %s",
                        self.address,
                        self.ports["port"],
                        self.name,
                    )
                    sensor = self.sdp_controller.sensors.get(prefix + "capture-block-state")
                    if sensor is not None:
                        self.capture_block_state_observer = CaptureBlockStateObserver(
                            sensor, logger=self.logger
                        )
                    sensor = self.sdp_controller.sensors.get(prefix + "device-status")
                    if sensor is not None:
                        self.device_status_observer = DeviceStatusObserver(sensor, self)
                    break
                except RuntimeError:
                    self.katcp_connection.close()
                    await self.katcp_connection.wait_closed()
                    # no need for these to lurk around
                    self.katcp_connection = None
                    self.logger.exception(
                        "Failed to connect to %s via katcp on %s:%d. "
                        "Check to see if networking issues could be to blame.",
                        self.name,
                        self.address,
                        self.ports["port"],
                    )
                    # Sleep for a bit to avoid hammering the port if there
                    # is a quick failure, before trying again.
                    await asyncio.sleep(1.0)
        return True

    def mark_suspect(self):
        """Mark a task as producing suspect data.

        This updates product-level sensors according to the
        :attr:`data_suspect_sensors` and :attr:`data_suspect_range`
        attributes.
        """
        for sensor in self.logical_node.data_suspect_sensors:
            data_suspect = sensor.value
            a, b = self.logical_node.data_suspect_range
            assert a <= b
            data_suspect = data_suspect[:a] + "1" * (b - a) + data_suspect[b:]
            if data_suspect != sensor.value:
                sensor.set_value(
                    data_suspect,
                    status=Sensor.Status.WARN
                    if data_suspect.count("0") > 0
                    else Sensor.Status.ERROR,
                )

    def _disconnect(self):
        """Clean up when killing the task or when it has died.

        This must be idempotent, because it will be called when the task is
        killed and again when it actually dies.
        """
        self.mark_suspect()
        if self.katcp_connection is not None:
            try:
                self.katcp_connection.close()
            except RuntimeError:
                self.logger.error("Failed to shut down katcp connection to %s", self.name)
            self.katcp_connection = None
        if self.capture_block_state_observer is not None:
            self.capture_block_state_observer.close()
            self.capture_block_state_observer = None
        if self.device_status_observer is not None:
            self.device_status_observer.close()
            self.device_status_observer = None

    def kill(self, driver, **kwargs):
        force = kwargs.pop("force", False)
        if not force:
            asyncio.ensure_future(self.graceful_kill(driver, **kwargs))
        else:
            self._disconnect()
            super().kill(driver, **kwargs)

    async def resolve(self, resolver, graph, image=None):
        await super().resolve(resolver, graph, image)
        # If metadata_katcp_sensors is true, the constructor adds sensors
        sensors_added = self.logical_node.metadata_katcp_sensors

        self.gui_urls = gui_urls = []
        for entry in self.logical_node.gui_urls:
            gui_urls.append({})
            for key, value in entry.items():
                if isinstance(value, str):
                    gui_urls[-1][key] = value.format(self)
                else:
                    gui_urls[-1][key] = value
        if gui_urls:
            gui_urls_sensor = Sensor(str, self.name + ".gui-urls", "URLs for GUIs")
            gui_urls_sensor.set_value(json.dumps(gui_urls))
            self.subarray_product.add_sensor(gui_urls_sensor)
            sensors_added = True

        self._host_sensor.value = self.host
        for key, value in self.interfaces.items():
            self._interface_sensors[key].value = value.name
        for key, value in self.ports.items():
            endpoint_sensor = Sensor(
                aiokatcp.Address, f"{self.name}.{key}", f"IP endpoint for {key}"
            )
            endpoint_sensor.set_value(aiokatcp.Address(ipaddress.ip_address(self.address), value))
            self.subarray_product.add_sensor(endpoint_sensor)
            sensors_added = True
        if sensors_added:
            self.subarray_product.notify_sensors_changed()

    def set_state(self, state):
        super().set_state(state)
        self._state_sensor.value = state
        if self.state == scheduler.TaskState.DEAD:
            self._disconnect()
            self._capture_blocks.clear()
            self._capture_blocks_empty.set()
            if not self.death_expected:
                self.subarray_product.unexpected_death(self)

    def clone(self):
        clone = self.logical_node.physical_factory(
            self.logical_node, self.sdp_controller, self.subarray_product, self.capture_block_id
        )
        clone.generation = self.generation + 1
        return clone

    def add_capture_block(self, capture_block):
        self._capture_blocks.add(capture_block.name)
        self._capture_blocks_empty.clear()

    def remove_capture_block(self, capture_block):
        self._capture_blocks.discard(capture_block.name)
        if not self._capture_blocks:
            self._capture_blocks_empty.set()

    async def graceful_kill(self, driver, **kwargs):
        try:
            if self.logical_node.final_state == CaptureBlockState.DEAD:
                capture_blocks = kwargs.get("capture_blocks", {})
                # Explicitly copy the values because it will mutate
                for capture_block in list(capture_blocks.values()):
                    await capture_block.dead_event.wait()
        except Exception:
            self.logger.exception("Exception in graceful shutdown of %s, killing it", self.name)

        self.logger.info("Waiting for capture blocks on %s", self.name)
        await self._capture_blocks_empty.wait()
        self.logger.info("All capture blocks for %s completed", self.name)
        self._disconnect()
        super().kill(driver, **kwargs)


class ProductPhysicalTask(ConfigMixin, ProductPhysicalTaskMixin, scheduler.PhysicalTask):
    """Augments the parent class with functionality specific to subarray products.

    In addition to the augmentations from the mixin classes, this class:
    - Provides Docker labels.
    - Registers the service with consul for Prometheus metrics, if there is
      a port called ``prometheus``.
    """

    logical_node: ProductLogicalTask

    def __init__(self, logical_task, sdp_controller, subarray_product, capture_block_id):
        scheduler.PhysicalTask.__init__(self, logical_task)
        ProductPhysicalTaskMixin.__init__(
            self, logical_task, sdp_controller, subarray_product, capture_block_id
        )
        self.consul_services = []

    def subst_args(self, resolver):
        """Add extra values for substitution into command lines.

        The extra keys are:

        capture_block_id
            If this task is specific to a single capture block, contains the
            capture block ID. Otherwise it is absent.
        endpoints_vector
            A dictionary with the same keys as ``endpoints``, but where each
            value is an array of endpoints. Typically this will just be a
            singleton array, but where the endpoint address has the form
            x.x.x.x+n, it will be expanded to n+1 sequential IP addresses.
        """
        args = super().subst_args(resolver)
        if self.capture_block_id is not None:
            args["capture_block_id"] = self.capture_block_id
        args["endpoints_vector"] = {
            name: endpoint_list_parser(endpoint.port)(endpoint.host)
            for name, endpoint in self.endpoints.items()
        }
        return args

    @property  # type: ignore
    def status(self):
        return self._status

    @status.setter
    def status(self, value):
        self._status = value
        if value is not None:
            self._mesos_state_sensor.value = value.state

    async def wait_ready(self):
        if not await super().wait_ready():
            return False
        # register Prometheus metrics with consul if appropriate
        if "prometheus" in self.ports:
            prometheus_port = self.ports["prometheus"]
            service = {
                "Name": self.logical_node.task_type,
                "Tags": ["prometheus-metrics"],
                "Meta": {
                    "subarray_product_id": self.subarray_product_id,
                    "task_name": self.logical_node.name,
                    **self.logical_node.consul_meta,
                },
                "Port": prometheus_port,
                "Checks": [
                    {
                        "Interval": "15s",
                        "Timeout": "5s",
                        # Using the metrics endpoint as a health check
                        "HTTP": f"http://{LOCALHOST}:{prometheus_port}/metrics",
                        "DeregisterCriticalServiceAfter": "90s",
                    }
                ],
            }
            # Connect to the Consul agent on the machine that will be running
            # the task. Note that this requires it to be configured to listen
            # on external interfaces - the default is to listen only on
            # localhost.
            # TODO: see if there is some way this requirement can be
            # eliminated e.g. by running via a wrapper script / sidecar
            # container that handles the registration and deregistration.
            consul_url = yarl.URL.build(scheme="http", host=self.address, port=CONSUL_PORT)
            self.consul_services.append(await ConsulService.register(service, consul_url))
        return True

    def _disconnect(self):
        for service in self.consul_services:
            # This might cause double-deregistration if this method is called
            # twice, but at worst that should cause a warning.
            asyncio.get_event_loop().create_task(service.deregister())
        super()._disconnect()

    async def resolve(self, resolver, graph, image=None):
        await super().resolve(resolver, graph, image)

        # Provide info about which container this is for logspout to collect.
        labels = {
            "task": self.logical_node.name,
            "task_type": self.logical_node.task_type,
            "task_id": self.taskinfo.task_id.value,
            "subarray_product_id": self.subarray_product_id,
        }
        if self.capture_block_id is not None:
            labels["capture_block_id"] = self.capture_block_id
        self.taskinfo.container.docker.setdefault("parameters", []).extend(
            [
                {"key": "label", "value": f"za.ac.kat.sdp.katsdpcontroller.{key}={value}"}
                for (key, value) in labels.items()
            ]
        )

        # Set extra fields for katsdpservices-using services to log to logstash
        if self.logical_node.katsdpservices_logging and "KATSDP_LOG_GELF_ADDRESS" in os.environ:
            extras = {
                **json.loads(os.environ.get("KATSDP_LOG_GELF_EXTRA", "{}")),
                **labels,
                "docker.image": self.taskinfo.container.docker.image,
            }
            env = {
                "KATSDP_LOG_GELF_ADDRESS": os.environ["KATSDP_LOG_GELF_ADDRESS"],
                "KATSDP_LOG_GELF_EXTRA": json.dumps(extras),
                "KATSDP_LOG_GELF_LOCALNAME": self.host,
                "LOGSPOUT": "ignore",
            }
            self.taskinfo.command.environment.setdefault("variables", []).extend(
                [{"name": key, "value": value} for (key, value) in env.items()]
            )

        # Apply overrides to taskinfo given by the user
        overrides = resolver.service_overrides.get(
            self.logical_node.name, product_config.ServiceOverride()
        ).taskinfo
        if overrides:
            self.logger.warning("Applying overrides to taskinfo of %s", self.name)
            self.taskinfo = Dict(product_config.override(self.taskinfo.to_dict(), overrides))

        # Fill in values for version sensors
        self._version_sensor.value = self.taskinfo.container.docker.image
        if self.image.source is not None:
            self._source_sensor.value = self.image.source
        if self.image.revision is not None:
            self._revision_sensor.value = self.image.revision

    def set_status(self, status):
        # Ensure we only count once, even in corner cases like a lost task
        # being rediscovered
        old_end_time = self.end_time
        super().set_status(status)
        if self.logical_node.batch_data_time is not None:
            # Create these as early as possible so that the metrics are exposed
            labels = (self.logical_node.task_type,)
            batch_runtime = BATCH_RUNTIME.labels(*labels)
            batch_runtime_rel = BATCH_RUNTIME_REL.labels(*labels)
            if self.end_time is not None and old_end_time is None:
                elapsed = self.end_time - self.start_time
                batch_runtime.observe(elapsed)
                batch_runtime_rel.observe(elapsed / self.logical_node.batch_data_time)
                logger.info("Task %s ran for %s s", self.name, elapsed)


class FakeDeviceServer(aiokatcp.DeviceServer):
    VERSION = "fake-1.0"
    BUILD_STATE = "fake-1.0"

    def __init__(self, host: str, port: int, logical_task: ProductLogicalTask, **kwargs) -> None:
        super().__init__(host, port, **kwargs)
        self.logical_task = logical_task

    async def unhandled_request(self, ctx, req: aiokatcp.core.Message) -> None:
        """Respond to any unknown requests with an empty reply."""
        ctx.reply(aiokatcp.core.Message.OK)
        await ctx.drain()

    def get_command_argument(self, value_type: Callable[[str], _T], argument_name: str) -> _T:
        """Return the value passed to the fake device as a CLI parameter.

        This function is not completely robust, as it assumes that parameters are
        only passed as separate strings, such as ``--foo bar``, and not using
        ``--foo=bar`` syntax. The argument name (e.g. ``--foo``) is located in
        the logical tasks's comamnd list, and it is assumed that the "value"
        passed is simply the next string in the list.
        """
        position = self.logical_task.command.index(argument_name)
        return value_type(self.logical_task.command[position + 1])

    def get_command_arguments(
        self, value_type: Callable[[str], _T], argument_name: str
    ) -> List[_T]:
        """Return the values passed to the fake device as CLI parameters.

        This is similar to :meth:`get_command_argument` (with the same
        limitations), but supports arguments that can be passed multiple times,
        returning a list of the values.
        """
        ret = []
        for i, arg in enumerate(self.logical_task.command):
            if arg == argument_name:
                ret.append(value_type(self.logical_task.command[i + 1]))
        return ret


@asynccontextmanager
async def wrap_katcp_server(
    server: aiokatcp.DeviceServer,
) -> AsyncGenerator[aiokatcp.DeviceServer, None]:
    await server.start()
    yield server
    await server.stop()


# TODO: see if ConfigMixin can be re-enabled here. It will need
# FakePhysicalTask to behave much more like PhysicalTask for generator.py to
# extract config.
class ProductFakePhysicalTask(ProductPhysicalTaskMixin, scheduler.FakePhysicalTask):
    logical_node: ProductLogicalTask

    def __init__(self, logical_task, sdp_controller, subarray_product, capture_block_id):
        scheduler.FakePhysicalTask.__init__(self, logical_task)
        ProductPhysicalTaskMixin.__init__(
            self, logical_task, sdp_controller, subarray_product, capture_block_id
        )

    async def _create_server(self, port: str, sock: socket.socket) -> AsyncContextManager:
        assert self.host is not None
        if port != "port":  # conventional name for katcp port
            return await super()._create_server(port, sock)
        host, port_no = sock.getsockname()[:2]
        sock.close()  # TODO: allow aiokatcp to take an existing socket
        katcp_server = self.logical_node.fake_katcp_server_cls(host, port_no, self.logical_node)
        return wrap_katcp_server(katcp_server)


ProductAnyPhysicalTask = Union[ProductPhysicalTask, ProductFakePhysicalTask]


class LogicalGroup(scheduler.LogicalExternal):
    """Dummy node that presents a set of related real nodes.

    This allows the graph to contain a single edge dependency to this node
    instead of one to each of the real nodes. It also allows for shared config
    to be stored once rather than repeated.
    """

    def __init__(self, name):
        super().__init__(name)
        self.physical_factory = PhysicalGroup
        # Whether to set config keys in telstate (only useful if processes
        # in the group use katsdpservices).
        self.katsdpservices_config = True


class PhysicalGroup(ConfigMixin, scheduler.PhysicalExternal):
    pass


class PoweroffLogicalTask(scheduler.LogicalTask):
    """Logical task for powering off a machine."""

    def __init__(self, host):
        super().__init__("kibisis." + host)
        self.host = host
        # Use minimal resources, to reduce chance it that it won't fit
        self.cpus = 0.001
        self.mem = 64
        self.image = "docker-base-runtime"
        self.command = ["/sbin/poweroff"]

        # See https://groups.google.com/forum/#!topic/coreos-dev/AXCs_2_J6Mc
        self.taskinfo.container.volumes = []
        for path in ["/var/run/dbus", "/run/systemd"]:
            volume = Dict()
            volume.mode = "RW"
            volume.container_path = path
            volume.host_path = path
            self.taskinfo.container.volumes.append(volume)
        self.taskinfo.container.docker.setdefault("parameters", [])
        self.taskinfo.container.docker.parameters.append({"key": "user", "value": "root"})

    def valid_agent(self, agent):
        if not super().valid_agent(agent):
            return False
        return agent.host == self.host
