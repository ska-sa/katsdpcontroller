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

"""Control of a single subarray product"""

import asyncio
import copy
import functools
import itertools
import json
import logging
import os
import re
import time
from typing import (
    Callable,
    Dict,
    Generator,
    Iterable,
    List,
    Mapping,
    Optional,
    Sequence,
    Set,
    Tuple,
    Union,
)

import addict
import aiokatcp
import jsonschema
import katsdpmodels.fetch.aiohttp
import katsdptelstate.aio.memory
import katsdptelstate.aio.redis
import networkx
import yarl
from aiokatcp import DeviceStatus, FailReply, Sensor
from katsdptelstate.endpoint import Endpoint
from prometheus_client import REGISTRY, CollectorRegistry, Counter, Gauge, Histogram

import katsdpcontroller

from . import generator, product_config, scheduler, sensor_proxy, tasks
from .consul import ConsulService
from .controller import (
    ProductState,
    device_status_to_sensor_status,
    load_json_dict,
    log_task_exceptions,
)
from .defaults import CONNECTION_MAX_BACKLOG, LOCALHOST, SHUTDOWN_DELAY
from .generator import TransmitState
from .product_config import Configuration
from .tasks import (
    DEPENDS_INIT,
    POSTPROCESSING_REL_BUCKETS,
    POSTPROCESSING_TIME_BUCKETS,
    CaptureBlockState,
    KatcpTransition,
)

BATCH_PRIORITY = 1  #: Scheduler priority for batch queues
BATCH_RESOURCES_TIMEOUT = 7 * 86400  # A week
GAIN_TIMEOUT = 10.0  # Gains have a large payload, so give them plenty of time
DELAYS_TIMEOUT = 1.0  # Delays are time-sensitive, so require them to be set fast
_HINT_RE = re.compile(
    r"\bprometheus: *(?P<type>[a-z]+)(?:\((?P<args>[^)]*)\)|\b)"
    r"(?: +labels: *(?P<labels>[a-z,]+))?",
    re.IGNORECASE,
)
POSTPROCESSING_TIME = Histogram(
    "katsdpcontroller_postprocessing_time_seconds",
    "Wall-clock time for postprocessing each capture block (including burndown phase)",
    buckets=POSTPROCESSING_TIME_BUCKETS,
)
POSTPROCESSING_TIME_REL = Histogram(
    "katsdpcontroller_postprocessing_time_rel",
    "Wall-clock time for postprocessing each capture block (including burndown phase)"
    ", relative to observation time",
    buckets=POSTPROCESSING_REL_BUCKETS,
)
STATIC_GAUGES = [
    Gauge(
        "fgpu_expected_input_heaps_per_second", "Number of heaps that should be received per second"
    ),
    Gauge("fgpu_expected_engines", "Number of F-engines that should be present"),
    Gauge(
        "xbgpu_expected_input_heaps_per_second",
        "Number of heaps that should be received per second",
    ),
    Gauge("xbgpu_expected_engines", "Number of XB-engines that should be present"),
]
logger = logging.getLogger(__name__)


def _redact_arg(arg: str, s3_config: dict) -> str:
    """Process one argument for _redact_keys"""
    for config in s3_config.values():
        for mode in ["read", "write"]:
            if mode in config:
                for name in ["access_key", "secret_key"]:
                    key = config[mode].get(name)
                    if key and (arg == key or arg.endswith("=" + key)):
                        return arg[: -len(key)] + "REDACTED"
    return arg


def _redact_keys(taskinfo: addict.Dict, s3_config: dict) -> addict.Dict:
    """Return a copy of a Mesos TaskInfo with command-line secret keys redacted.

    This is intended for putting the taskinfo into telstate without revealing
    secrets. Any occurrences of the secrets in s3_config in a command-line
    argument are replaced by REDACTED.

    It will handle both '--secret=foo' and '--secret foo'.

    .. note::

        While the original `taskinfo` is not modified, the copy is not a full deep copy.

    Parameters
    ----------
    taskinfo
        Taskinfo structure
    s3_config
        Secret keys

    Returned
    --------
    redacted : :class:`addict.Dict`
        Copy of `taskinfo` with secrets redacted
    """
    taskinfo = taskinfo.copy()
    if taskinfo.command.arguments:
        taskinfo.command = taskinfo.command.copy()
        taskinfo.command.arguments = [
            _redact_arg(arg, s3_config) for arg in taskinfo.command.arguments
        ]
    return taskinfo


def _normalise_s3_config(s3_config: dict) -> dict:
    """Normalise s3_config to have separate `url` fields for `read` and `write`."""
    s3_config = copy.deepcopy(s3_config)
    for config in s3_config.values():
        if "url" in config:
            for mode in ["read", "write"]:
                config.setdefault(mode, {})["url"] = config["url"]
            del config["url"]
    return s3_config


def _prometheus_factory(
    registry: CollectorRegistry, sensor: aiokatcp.Sensor
) -> Optional[sensor_proxy.PrometheusInfo]:
    assert sensor.description is not None
    match = _HINT_RE.search(sensor.description)
    if not match:
        return None
    type_ = match.group("type").lower()
    args_ = match.group("args")
    if type_ == "counter":
        class_ = Counter
        if args_ is not None:
            logger.warning("Arguments are not supported for counters (%s)", sensor.name)
    elif type_ == "gauge":
        class_ = Gauge
        if args_ is not None:
            logger.warning("Arguments are not supported for gauges (%s)", sensor.name)
    elif type_ == "histogram":
        class_ = Histogram
        if args_ is not None:
            try:
                buckets = [float(x.strip()) for x in args_.split(",")]
                class_ = functools.partial(Histogram, buckets=buckets)
            except ValueError as exc:
                logger.warning("Could not parse histogram buckets (%s): %s", sensor.name, exc)
    else:
        logger.warning("Ignoring unknown Prometheus metric type %s for %s", type_, sensor.name)
        return None
    parts = sensor.name.rsplit(".")
    base = parts.pop()
    label_names = (match.group("labels") or "").split(",")
    label_names = [label for label in label_names if label]  # ''.split(',') is [''], want []
    if len(parts) < len(label_names):
        logger.warning("Not enough parts in name %s for labels %s", sensor.name, label_names)
        return None
    service_parts = len(parts) - len(label_names)
    service = ".".join(parts[:service_parts])
    labels = dict(zip(label_names, parts[service_parts:]))
    if service:
        labels["service"] = service
    normalised_name = "katsdpcontroller_" + base.replace("-", "_")
    return sensor_proxy.PrometheusInfo(
        class_, normalised_name, sensor.description, labels, registry
    )


def _relative_url(base: yarl.URL, target: yarl.URL) -> yarl.URL:
    """Produce URL `rel` such that ``base.join(rel) == target``.

    It does not deal with query strings or fragments.

    Raises
    ------
    ValueError
        if either of the URLs are not absolute
    ValueError
        if either URL contains fragments or query strings
    ValueError
        if `target` is not nested under `base`
    """
    if not base.is_absolute() or not target.is_absolute():
        raise ValueError("Absolute URLs expected")
    if base.query_string or target.query_string:
        raise ValueError("Query strings are not supported")
    if base.fragment or target.fragment:
        raise ValueError("Fragments are not supported")
    if base.origin() != target.origin():
        raise ValueError("URLs have different origins")
    # Strip off the last component, which is empty if the URL ends with a /
    # other than at the root, or the filename if it does not.
    base_parts = base.raw_parts[:-1] if len(base.parts) > 1 else base.raw_parts
    if target.raw_parts[: len(base_parts)] != base_parts:
        raise ValueError("Target URL is not nested under the base")
    rel_parts = target.raw_parts[len(base_parts) :]
    rel = yarl.URL.build(path="/".join(rel_parts), encoded=True)
    assert base.join(rel) == target
    return rel


async def _resolve_model(
    fetcher: katsdpmodels.fetch.aiohttp.Fetcher, base_url: str, rel_url: str
) -> Tuple[str, str]:
    """Compute model URLs to store in katsdptelstate.

    Returns
    -------
    config_url
        URL relative to `base_url` that is specific to the config.
    fixed_url
        URL relative to `base_url` for an immutable model.
    """
    base = yarl.URL(base_url)
    url = base.join(yarl.URL(rel_url))
    urls = await fetcher.resolve(str(url))
    fixed = urls[-1]
    config = urls[-2] if len(urls) >= 2 else fixed
    return (str(_relative_url(base, yarl.URL(config))), str(_relative_url(base, yarl.URL(fixed))))


class KatcpImageLookup(scheduler.ImageLookup):
    """Image lookup that asks the master controller to do the work.

    Tunnelling the lookup avoids the need for the product controller to
    have the right CA certificates and credentials for the Docker registry.
    """

    def __init__(self, conn: aiokatcp.Client) -> None:
        self._conn = conn

    async def __call__(self, repo: str, tag: str) -> scheduler.Image:
        reply, informs = await self._conn.request("image-lookup-v2", repo, tag)
        data = json.loads(aiokatcp.decode(str, reply[0]))
        return scheduler.Image(**data)


class Resolver(scheduler.Resolver):
    """Resolver with some extra fields"""

    def __init__(
        self,
        image_resolver: scheduler.ImageResolver,
        task_id_allocator: scheduler.TaskIDAllocator,
        http_url: Optional[str],
        service_overrides: Mapping[str, product_config.ServiceOverride],
        s3_config: dict,
        localhost: bool,
    ) -> None:
        super().__init__(image_resolver, task_id_allocator, http_url)
        self.service_overrides = service_overrides
        self.s3_config = s3_config
        self.telstate: Optional[katsdptelstate.aio.TelescopeState] = None
        self.resources: Optional[Resources] = None
        self.localhost = localhost


class Resources:
    """Helper class to allocate resources for a single subarray-product."""

    def __init__(self, master_controller: aiokatcp.Client, subarray_product_id: str) -> None:
        self.master_controller = master_controller
        self.subarray_product_id = subarray_product_id

    async def get_multicast_groups(self, n_addresses: int) -> str:
        """Assign multicast addresses for a group."""
        reply, informs = await self.master_controller.request(
            "get-multicast-groups", self.subarray_product_id, n_addresses
        )
        return reply[0].decode()

    @staticmethod
    async def get_port() -> int:
        """Return an assigned port for a multicast group"""
        return 7148


class TaskStats(scheduler.TaskStats):
    def __init__(self) -> None:
        def add_sensor(name: str, description: str) -> None:
            self.sensors.add(
                aiokatcp.Sensor(
                    int, name, description, initial_status=aiokatcp.Sensor.Status.NOMINAL
                )
            )

        def add_counter(name: str, description: str) -> None:
            add_sensor(name, description + " (prometheus: counter)")

        self.sensors = aiokatcp.SensorSet()
        for queue in ["default", "batch"]:
            for state in scheduler.TaskState:
                name = state.name.lower()
                add_sensor(
                    f"{queue}.{name}.tasks-in-state",
                    f"Number of tasks in queue {queue} and state {state.name} "
                    "(prometheus: gauge labels: queue,state)",
                )
        add_counter("batch-tasks-created", "Number of batch tasks that have been created")
        add_counter("batch-tasks-started", "Number of batch tasks that have become ready to start")
        add_counter(
            "batch-tasks-skipped",
            "Number of batch tasks that were skipped because a dependency failed",
        )
        add_counter(
            "batch-tasks-done", "Number of completed batch tasks (including failed and skipped)"
        )
        add_counter("batch-tasks-retried", "Number of batch tasks that failed and were rescheduled")
        add_counter("batch-tasks-failed", "Number of batch tasks that failed (after all retries)")

    def task_state_changes(
        self, changes: Mapping[scheduler.LaunchQueue, Mapping[scheduler.TaskState, int]]
    ) -> None:
        now = time.time()
        for queue, deltas in changes.items():
            for state, delta in deltas.items():
                state_name = state.name.lower()
                sensor_name = f"{queue.name}.{state_name}.tasks-in-state"
                sensor = self.sensors.get(sensor_name)
                if sensor:
                    sensor.set_value(sensor.value + delta, timestamp=now)

    def batch_tasks_created(self, n_tasks: int) -> None:
        self.sensors["batch-tasks-created"].value += n_tasks

    def batch_tasks_started(self, n_tasks: int) -> None:
        self.sensors["batch-tasks-started"].value += n_tasks

    def batch_tasks_skipped(self, n_tasks: int) -> None:
        self.sensors["batch-tasks-skipped"].value += n_tasks

    def batch_tasks_retried(self, n_tasks: int) -> None:
        self.sensors["batch-tasks-retried"].value += n_tasks

    def batch_tasks_failed(self, n_tasks: int) -> None:
        self.sensors["batch-tasks-failed"].value += n_tasks

    def batch_tasks_done(self, n_tasks: int) -> None:
        self.sensors["batch-tasks-done"].value += n_tasks


class CaptureBlock:
    """A capture block is book-ended by a capture-init and a capture-done,
    although processing on it continues after the capture-done."""

    def __init__(self, name: str, configuration: Configuration) -> None:
        self.name = name
        self.configuration = configuration
        self._state = CaptureBlockState.INITIALISING
        self.postprocess_task: Optional[asyncio.Task] = None
        self.postprocess_physical_graph: Optional[networkx.MultiDiGraph] = None
        self.dead_event = asyncio.Event()
        self.state_change_callback: Optional[Callable[[], None]] = None
        # Time each state is reached
        self.state_time: Dict[CaptureBlockState, float] = {}

    @property
    def state(self) -> CaptureBlockState:
        return self._state

    @state.setter
    def state(self, value: CaptureBlockState) -> None:
        if self._state != value:
            self._state = value
            if value not in self.state_time:
                self.state_time[value] = time.time()
            if value == CaptureBlockState.DEAD:
                self.dead_event.set()
            if self.state_change_callback is not None:
                self.state_change_callback()


def _error_on_error(state: ProductState) -> Sensor.Status:
    return Sensor.Status.ERROR if state == ProductState.ERROR else Sensor.Status.NOMINAL


class _IndexedKey(dict):
    """Wrapper class indicating that the contents form a telstate indexed key.

    This is used in dictionaries containing initial values to be placed into
    telstate.
    """

    pass


class SubarrayProduct:
    """Represents an instance of a subarray product.

    This includes ingest, an appropriate telescope model, and any required
    post-processing.

    In general each telescope subarray product is handled in a completely
    parallel fashion. This class encapsulates these instances, handling control
    input and sensor feedback to CAM.

    State changes are asynchronous operations. There can only be one
    asynchronous operation at a time. Attempting a second one will either
    fail, or in some cases will cancel the prior operation. To avoid race
    conditions, changes to :attr:`state` should generally only be made from
    inside the asynchronous tasks.

    Some methods that are asynchronous but don't change state (such as
    setting delays) may run concurrently. They are best-effort, and may fail if
    the subarray product is deconfigured concurrently.

    There are some invariants that must hold at yield points:
    - There is at most one capture block in state CAPTURING.
    - :attr:`current_capture_block` is the capture block in state
      CAPTURING, or ``None`` if there isn't one.
    - :attr:`current_capture_block` is set if and only if the subarray state
      is CAPTURING.
    - Elements of :attr:`capture_blocks` are not in state DEAD.
    """

    def _instantiate(
        self, logical_node: scheduler.LogicalNode, capture_block_id: Optional[str]
    ) -> scheduler.PhysicalNode:
        if getattr(logical_node, "sdp_physical_factory", False):
            return logical_node.physical_factory(
                logical_node, self.sdp_controller, self, capture_block_id
            )
        return logical_node.physical_factory(logical_node)

    def _instantiate_physical_graph(
        self, logical_graph: networkx.MultiDiGraph, capture_block_id: Optional[str] = None
    ) -> networkx.MultiDiGraph:
        mapping = {
            logical: self._instantiate(logical, capture_block_id) for logical in logical_graph
        }
        return networkx.relabel_nodes(logical_graph, mapping)

    def __init__(
        self,
        sched: scheduler.SchedulerBase,
        configuration: Configuration,
        config_dict: dict,
        resolver: Resolver,
        subarray_product_id: str,
        sdp_controller: "DeviceServer",
        telstate_name: str = "telstate",
    ) -> None:
        #: Current background task (can only be one)
        self._async_task: Optional[asyncio.Task] = None
        self.sched = sched
        self.configuration = configuration
        self.config_dict = config_dict
        self.resolver = resolver
        self.subarray_product_id = subarray_product_id
        self.sdp_controller = sdp_controller
        self.logical_graph = generator.build_logical_graph(
            configuration, config_dict, sdp_controller.sensors
        )
        self.telstate_endpoint = ""
        self.telstate: Optional[katsdptelstate.aio.TelescopeState] = None
        self.capture_blocks: Dict[str, CaptureBlock] = {}  # live capture blocks, indexed by name
        # set between capture_init and capture_done
        self.current_capture_block: Optional[CaptureBlock] = None
        self.dead_event = asyncio.Event()  # Set when reached state DEAD
        # Callbacks that are called when we reach state DEAD. These are
        # provided in addition to dead_event, because sometimes it's
        # necessary to react immediately rather than waiting for next time
        # around the event loop. Each callback takes self as the argument.
        self.dead_callbacks = [lambda product: product.dead_event.set()]
        self._state: ProductState = ProductState.CONFIGURING
        # Set of sensors to remove when the product is removed
        self.sensors: Set[aiokatcp.Sensor] = set()
        self._capture_block_sensor = Sensor(
            str,
            "capture-block-state",
            "JSON dictionary of capture block states for active capture blocks",
            default="{}",
            initial_status=Sensor.Status.NOMINAL,
        )
        self._state_sensor = Sensor(
            ProductState,
            "product-state",
            "State of the subarray product state machine (prometheus: gauge)",
            status_func=_error_on_error,
        )
        self._device_status_sensor = sdp_controller.sensors["device-status"]

        # Set gauges for expected data rates
        for gauge in STATIC_GAUGES:
            value = 0.0
            gauge_name = gauge.collect()[0].name
            for node in self.logical_graph:
                if isinstance(node, tasks.ProductLogicalTask):
                    value += node.static_gauges.get(gauge_name, 0.0)
            gauge.set(value)

        self.state = ProductState.CONFIGURING  # This sets the sensor
        self.add_sensor(self._capture_block_sensor)
        self.add_sensor(self._state_sensor)
        self.add_sensor(
            Sensor(
                str,
                "product-config",
                "Configuration dictionary used to build the subarray product",
                default=json.dumps(config_dict),
                initial_status=Sensor.Status.NOMINAL,
            )
        )
        # Priority is lower (higher number) than the default queue
        self.batch_queue = scheduler.LaunchQueue(
            sdp_controller.batch_role, "batch", priority=BATCH_PRIORITY
        )
        sched.add_queue(self.batch_queue)
        # generate physical nodes
        self.physical_graph = self._instantiate_physical_graph(self.logical_graph)
        # Nodes indexed by logical name
        self._nodes = {node.logical_node.name: node for node in self.physical_graph}
        self.telstate_node = self._nodes.get(telstate_name)
        self.master_controller = sdp_controller.master_controller
        # Note: this doesn't take into account the postprocessing graph.
        # However, there can only be postprocessing if there is a telstate,
        # and telstate has a final_state of DEAD, so it would make
        # _delayed_deconfigure true anyway.
        self._delayed_deconfigure = any(
            isinstance(node.logical_node, tasks.ProductLogicalTask)
            and node.logical_node.final_state >= CaptureBlockState.POSTPROCESSING
            for node in self.physical_graph
        )

        logger.info("Created: %r", self)
        logger.info("Logical graph nodes:\n" + "\n".join(repr(node) for node in self.logical_graph))

    def __del__(self) -> None:
        if hasattr(self, "batch_queue"):
            self.sched.remove_queue(self.batch_queue)

    def __repr__(self) -> str:
        return f"Subarray product {self.subarray_product_id} (State: {self.state.name})"

    @property
    def state(self) -> ProductState:
        return self._state

    @state.setter
    def state(self, value: ProductState) -> None:
        if self._state == ProductState.ERROR and value not in (
            ProductState.DECONFIGURING,
            ProductState.DEAD,
        ):
            return  # Never leave error state other than by deconfiguring
        now = time.time()
        if value == ProductState.ERROR and self._state != value:
            self._device_status_sensor.set_value(DeviceStatus.FAIL, timestamp=now)
        self._state = value
        self._state_sensor.set_value(value, timestamp=now)

    def add_sensor(self, sensor: Sensor) -> None:
        """Add the supplied sensor to the top-level device and track it locally.

        It does *not* send an ``interface-changed`` inform; that is left to the
        caller, which should call :meth:`notify_sensors_changed`.
        """
        self.sensors.add(sensor)
        self.sdp_controller.sensors.add(sensor)

    def remove_sensors(self):
        """Remove all sensors added via :meth:`add_sensor`.

        It does *not* send an ``interface-changed`` inform; that is left to the
        caller, which should call :meth:`notify_sensors_changed`.
        """
        for sensor in self.sensors:
            self.sdp_controller.sensors.discard(sensor)
        self.sensors.clear()

    def notify_sensors_changed(self) -> None:
        """Notify subscribers that the set of sensors have changed.

        This should be called after using :meth:`add_sensor` or otherwise
        changing the sensors. It will notify katcp clients that the interface
        has changed. However, while in CONFIGURING state these notifications
        are suppressed to avoid clients continually updating the sensor list
        as each task starts.
        """
        if self.state != ProductState.CONFIGURING:
            self.sdp_controller.mass_inform("interface-changed", "sensor-list")

    @property
    def async_busy(self) -> bool:
        """Whether there is an asynchronous state-change operation in progress."""
        return self._async_task is not None and not self._async_task.done()

    def _fail_if_busy(self) -> None:
        """Raise a FailReply if there is an asynchronous operation in progress."""
        if self.async_busy:
            raise FailReply(
                f"Subarray product {self.subarray_product_id} is busy with an operation. "
                "Please wait for it to complete first."
            )

    def _fail_if_no_telstate(self) -> None:
        """Raise a FailReply if there is no telescope state."""
        if self.telstate is None:
            raise FailReply(f"Subarray product {self.subarray_product_id} has no SDP components.")

    def _find_stream(self, stream_name: str) -> product_config.Stream:
        """Find the stream with a given name.

        Raises
        ------
        FailReply
            If no such stream exists
        """
        for stream in self.configuration.streams:
            if stream.name == stream_name:
                return stream
        raise FailReply(f"Unknown stream {stream_name!r}")

    def find_nodes(
        self,
        *,
        task_type: Optional[str] = None,
        streams: Optional[Iterable[product_config.Stream]] = None,
        indices: Optional[Iterable[int]] = None,
    ) -> Generator[tasks.ProductAnyPhysicalTask, None, None]:
        """Find physical nodes matching given criteria.

        Parameters
        ----------
        task_type
            The :attr:`.ProductLogicalTask.task_type` attribute of the logical
            node. If ``None``, any node can match.
        streams
            Streams to match against the :attr:`.ProductLogicalTask.streams`
            attribute of the logical node. To match, there must be at least
            one stream name in the intersection (only the names are matched,
            not the identity). If ``None``, any node can match.
        indices
            Indices to match against the :attr:`.ProductLogicalTask.index`
            attribute of the logical node. If ``None``, any node can match.
        """
        stream_names = frozenset(stream.name for stream in streams) if streams is not None else None
        for node in self.physical_graph:
            logical_node = node.logical_node
            if not isinstance(logical_node, tasks.ProductLogicalTask):
                continue
            if task_type is not None and task_type != logical_node.task_type:
                continue
            if stream_names is not None and not stream_names.intersection(
                logical_node.stream_names
            ):
                continue
            if indices is not None and logical_node.index not in indices:
                continue
            yield node

    async def _exec_node_transition(
        self,
        node: tasks.ProductPhysicalTask,
        reqs: Sequence[KatcpTransition],
        deps: Sequence[asyncio.Future],
        state: CaptureBlockState,
        capture_block: CaptureBlock,
    ) -> None:
        try:
            if deps:
                # If we're starting a capture and a dependency fails, there is
                # no point trying to continue. On the shutdown path, we should
                # continue anyway to try to close everything off as neatly as
                # possible.
                await asyncio.gather(
                    *deps, return_exceptions=(state != CaptureBlockState.CAPTURING)
                )
            if reqs:
                if node.katcp_connection is None:
                    logger.warning(
                        "Cannot issue %s to %s because there is no katcp connection",
                        reqs[0],
                        node.name,
                    )
                else:
                    for req in reqs:
                        await node.issue_req(req.name, req.args, timeout=req.timeout)
            if (
                isinstance(node, tasks.ProductPhysicalTask)
                and state == node.logical_node.final_state
            ):
                observer = node.capture_block_state_observer
                if observer is not None:
                    logger.info("Waiting for %s on %s", capture_block.name, node.name)
                    await observer.wait_capture_block_done(capture_block.name)
                    logger.info("Done waiting for %s on %s", capture_block.name, node.name)
                else:
                    logger.debug("Task %s has no capture-block-state observer", node.name)
        finally:
            if (
                isinstance(node, tasks.ProductPhysicalTask)
                and state == node.logical_node.final_state
            ):
                node.remove_capture_block(capture_block)

    async def exec_transitions(
        self, state: CaptureBlockState, reverse: bool, capture_block: CaptureBlock
    ) -> None:
        """Issue requests to nodes on state transitions.

        The requests are made in parallel, but respects `depends_init`
        dependencies in the graph.

        Parameters
        ----------
        state
            New state
        reverse
            If there is a `depends_init` edge from A to B in the graph, A's
            request will be made first if `reverse` is false, otherwise B's
            request will be made first.
        capture_block
            The capture block is that being transitioned
        """
        # Create a copy of the graph containing only dependency edges.
        deps_graph = scheduler.subgraph(self.physical_graph, DEPENDS_INIT)
        # Reverse it
        if not reverse:
            deps_graph = deps_graph.reverse(copy=False)

        futures: Dict[object, asyncio.Future] = {}  # Keyed by node
        # Lexicographical tie-breaking isn't strictly required, but it makes
        # behaviour predictable.
        now = time.time()  # Outside loop to be consistent across all nodes
        for node in networkx.lexicographical_topological_sort(deps_graph, key=lambda x: x.name):
            reqs: List[KatcpTransition] = []
            try:
                reqs = node.get_transition(state)
            except AttributeError:
                # Not all nodes are ProductPhysicalTask
                pass
            if reqs:
                # Apply {} substitutions to request data
                subst = dict(capture_block_id=capture_block.name, time=now)
                reqs = [req.format(**subst) for req in reqs]
            deps = [futures[trg] for trg in deps_graph.predecessors(node) if trg in futures]
            task = asyncio.get_event_loop().create_task(
                self._exec_node_transition(node, reqs, deps, state, capture_block)
            )
            futures[node] = task
        if futures:
            # We want to wait for all the futures to complete, even if one of
            # them fails early (to give the others time to do cleanup). But
            # then we want to raise the first exception.
            results = await asyncio.gather(*futures.values(), return_exceptions=True)
            for result in results:
                if isinstance(result, Exception):
                    raise result

    async def _multi_request(
        self,
        nodes: Iterable[tasks.ProductAnyPhysicalTask],
        messages: Iterable[Iterable],
        timeout: Union[float, None] = None,
        log_level: int = logging.INFO,
    ) -> None:
        """Send katcp requests for multiple nodes in parallel.

        If any of the critical nodes has no current katcp connection, abort
        before sending any of the messages. Otherwise, send the messages and
        wait for all of them to complete (even if some of them fail). If any
        fail, raise the first failure.

        Each message is given as an iterable of arguments (starting with the
        request name). It is acceptable for the messages iterator to be
        longer than the node list. In particular, :meth:`itertools.repeat`
        can be used to replicate a single message to every node.
        """
        reqs: List[Tuple[tasks.ProductAnyPhysicalTask, tuple]] = []
        messages_iter = iter(messages)
        non_critical = []
        for node in nodes:
            message = next(messages_iter)
            if node.katcp_connection is not None:
                reqs.append((node, tuple(message)))
            elif node.logical_node.critical:
                raise FailReply(f"No katcp connection to {node.name}")
            else:
                non_critical.append(node.name)
        # Now that we've validated things, send the messages
        futures = []
        for node, msg in reqs:
            futures.append(node.issue_req(msg[0], msg[1:], timeout=timeout, log_level=log_level))
        if futures:  # gather doesn't like having zero futures
            results = await asyncio.gather(*futures, return_exceptions=True)
            for result in results:
                if isinstance(result, Exception):
                    raise result
        if non_critical:
            logger.debug("multi_request: no katcp connection to some nodes: %s", non_critical)

    async def _wait_rx_device_status(self) -> None:
        """Wait for task rx.device-status sensors to become nominal.

        The wait is limited to the user-specified `data_timeout`.
        If it times out, a helpful :exc:`.FailReply` is raised.
        """
        timeout = self.configuration.options.develop.data_timeout
        if timeout <= 0.0:
            return  # 0.0 disables the check

        def observer(sensor, reading):
            if reading.status == Sensor.Status.NOMINAL:
                task_name = sensor.name.rsplit(".", 2)[0]
                missing.discard(task_name)
                if not missing and not future.done():
                    future.set_result(None)

        sensors = []
        missing: Set[str] = set()  # Task names we're still waiting for
        future = asyncio.get_running_loop().create_future()
        for task in self.physical_graph:
            if isinstance(task, tasks.ProductPhysicalTaskMixin):
                sensor = self.sdp_controller.sensors.get(f"{task.name}.rx.device-status")
                if sensor is not None and sensor.status != Sensor.Status.NOMINAL:
                    sensors.append(sensor)
                    sensor.attach(observer)
                    missing.add(task.name)
        if not sensors:
            # Nothing to do
            return

        logger.info(f"Waiting for {len(sensors)} tasks to receive good data")
        try:
            await asyncio.wait_for(future, timeout=timeout)
        except asyncio.TimeoutError:
            raise FailReply(
                f"Some tasks did not receive good data within {timeout}s: "
                + ", ".join(sorted(missing))
            ) from None
        finally:
            for sensor in sensors:
                sensor.detach(observer)
        logger.info("All tasks receiving good data")

    async def _capture_done_impl(self, capture_block: CaptureBlock) -> None:
        """Stop a capture block.

        This should only do the work needed for the ``capture-done`` master
        controller request to return. The caller takes care of calling
        :meth:`_postprocess`.

        It needs to be safe to run from DECONFIGURING and ERROR states, because
        it may be run as part of forced deconfigure.

        If it raises an exception, the capture block is assumed to be dead,
        and the subarray product goes into state ERROR unless this occurred as
        part of deconfiguring.
        """
        await self.exec_transitions(CaptureBlockState.BURNDOWN, False, capture_block)

    async def _postprocess(self, capture_block: CaptureBlock) -> None:
        """Complete the post-processing for a capture block.

        Note that a failure here does **not** put the subarray product into
        ERROR state, as it is assumed that this does not interfere with
        subsequent operation.

        This function should move the capture block to POSTPROCESSING after
        burndown of the real-time processing, but should not set it to DEAD.
        """
        assert self.telstate is not None
        assert self.telstate_node is not None
        try:
            await self.exec_transitions(CaptureBlockState.POSTPROCESSING, False, capture_block)
            capture_block.state = CaptureBlockState.POSTPROCESSING
            logical_graph = await generator.build_postprocess_logical_graph(
                capture_block.configuration,
                capture_block.name,
                self.telstate,
                self.telstate_endpoint,
            )
            physical_graph = self._instantiate_physical_graph(logical_graph, capture_block.name)
            capture_block.postprocess_physical_graph = physical_graph
            nodes = {node.logical_node.name: node for node in physical_graph}
            telstate_node = nodes["telstate"]
            telstate_node.host = self.telstate_node.host
            telstate_node.ports = dict(self.telstate_node.ports)
            # This doesn't actually run anything, just marks the fake telstate node
            # as READY. It could block for a while behind real tasks in the batch
            # queue, but that doesn't matter because our real tasks will block too.
            # However, because of this blocking it needs a large resources_timeout,
            # even though it uses no resources.
            await self.sched.launch(
                physical_graph,
                self.resolver,
                [telstate_node],
                queue=self.batch_queue,
                resources_timeout=BATCH_RESOURCES_TIMEOUT,
            )
            nodelist = [
                node
                for node in physical_graph
                if isinstance(node, (scheduler.PhysicalTask, scheduler.FakePhysicalTask))
            ]
            await self.sched.batch_run(
                physical_graph,
                self.resolver,
                nodelist,
                queue=self.batch_queue,
                resources_timeout=BATCH_RESOURCES_TIMEOUT,
                attempts=3,
            )
        finally:
            init_time = capture_block.state_time[CaptureBlockState.CAPTURING]
            done_time = capture_block.state_time[CaptureBlockState.BURNDOWN]
            observation_time = done_time - init_time
            postprocessing_time = time.time() - done_time
            POSTPROCESSING_TIME.observe(postprocessing_time)
            logger.info(
                "Capture block %s postprocessing finished in %.3fs (obs time: %.3fs)",
                capture_block.name,
                postprocessing_time,
                observation_time,
                extra=dict(
                    capture_block_id=capture_block.name,
                    observation_time=observation_time,
                    postprocessing_time=postprocessing_time,
                ),
            )
            # In unit tests the obs time might be zero, which leads to errors here
            if observation_time > 0:
                POSTPROCESSING_TIME_REL.observe(postprocessing_time / observation_time)
            await self.exec_transitions(CaptureBlockState.DEAD, False, capture_block)

    async def _configure(self) -> None:
        """Asynchronous task that does the configuration."""
        try:
            try:
                resolver = self.resolver
                resolver.resources = Resources(self.master_controller, self.subarray_product_id)

                # Register stream-specific KATCP sensors.
                for ss in self.physical_graph.graph["stream_sensors"].values():
                    self.add_sensor(ss)

                # launch the telescope state for this graph
                telstate: Optional[katsdptelstate.aio.TelescopeState]
                if self.telstate_node is not None:
                    telstate = await self._launch_telstate()
                else:
                    telstate = None
                # launch containers for those nodes that require them
                await self.sched.launch(self.physical_graph, self.resolver)
                alive, died = self.check_nodes()
                # is everything we asked for alive
                if not alive:
                    fail_list = ", ".join(node.logical_node.name for node in died) or "Some nodes"
                    ret_msg = (
                        f"{fail_list} failed to start. Check the error log for specific details."
                    )
                    logger.error(ret_msg)
                    raise FailReply(ret_msg)
                # Record the TaskInfo for each task in telstate, as well as details
                # about the image resolver.
                details = {}
                for task in self.physical_graph:
                    if isinstance(task, scheduler.PhysicalTask):
                        details[task.logical_node.name] = {
                            "host": task.host,
                            "taskinfo": _redact_keys(task.taskinfo, resolver.s3_config).to_dict(),
                        }
                if telstate is not None:
                    await telstate.add("sdp_task_details", details, immutable=True)
                    await telstate.add("sdp_image_tag", resolver.image_resolver.tag, immutable=True)
                    await telstate.add(
                        "sdp_image_overrides", resolver.image_resolver.overrides, immutable=True
                    )
                await self._wait_rx_device_status()
            except BaseException as exc:
                # If there was a problem the graph might be semi-running. Shut it all down.
                await self._shutdown(force=True)
                raise exc
        except scheduler.InsufficientResourcesError as error:
            raise FailReply(
                f"Insufficient resources to launch {self.subarray_product_id}: {error}"
            ) from error
        except scheduler.ImageError as error:
            raise FailReply(str(error)) from error
        self.state = ProductState.IDLE

    async def _deconfigure(self, force: bool, ready: asyncio.Event) -> None:
        """Asynchronous task that does the deconfiguration.

        This handles the entire burndown cycle. The end of the synchronous
        part (at which point the katcp request returns) is signalled by
        setting the event `ready`.
        """
        self.state = ProductState.DECONFIGURING
        if self.current_capture_block is not None:
            try:
                capture_block = self.current_capture_block
                # To prevent trying again if we get a second forced-deconfigure.
                self.current_capture_block = None
                await self._capture_done_impl(capture_block)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception(
                    "Failed to issue capture-done during shutdown request. "
                    "Will continue with graph shutdown."
                )

        if force:
            for capture_block in list(self.capture_blocks.values()):
                if capture_block.postprocess_task is not None:
                    logger.warning(
                        "Cancelling postprocessing for capture block %s", capture_block.name
                    )
                    capture_block.postprocess_task.cancel()
                else:
                    self._capture_block_dead(capture_block)
            await self._shutdown(force=force)
            ready.set()
        else:

            def must_wait(node):
                return (
                    isinstance(node.logical_node, tasks.ProductLogicalTask)
                    and node.logical_node.final_state <= CaptureBlockState.BURNDOWN
                )

            # Start the shutdown in a separate task, so that we can monitor
            # for task shutdown.
            wait_tasks = [node.dead_event.wait() for node in self.physical_graph if must_wait(node)]
            shutdown_task = asyncio.get_event_loop().create_task(self._shutdown(force=force))
            await asyncio.gather(*wait_tasks)
            self.state = ProductState.POSTPROCESSING
            # If it's going to take a while to shut down, let product-deconfigure
            # return now. If it's going to be quick then we just wait
            if self._delayed_deconfigure:
                ready.set()
            await shutdown_task

        # Allow all the postprocessing tasks to finish up
        # Note: this needs to be done carefully, because self.capture_blocks
        # can change during the await.
        while self.capture_blocks:
            name, capture_block = next(iter(self.capture_blocks.items()))
            logging.info("Waiting for capture block %s to terminate", name)
            await capture_block.dead_event.wait()
            self.capture_blocks.pop(name, None)

        if self.telstate is not None:
            self.telstate.backend.close()

        self.state = ProductState.DEAD
        # Setting dead_event is done by the first callback
        for callback in self.dead_callbacks:
            callback(self)
        # Might have been set earlier, but harmless to set it twice
        ready.set()

    def _capture_block_dead(self, capture_block: CaptureBlock) -> None:
        """Mark a capture block as dead and remove it from the list."""
        try:
            del self.capture_blocks[capture_block.name]
        except KeyError:
            pass  # Allows this function to be called twice
        # Setting the state will trigger _update_capture_block_sensor, which
        # will update the sensor with the value removed
        capture_block.state = CaptureBlockState.DEAD
        for node in self.physical_graph:
            if isinstance(node, tasks.ProductPhysicalTask):
                node.remove_capture_block(capture_block)

    def _update_capture_block_sensor(self) -> None:
        value = {
            name: capture_block.state.name.lower()
            for name, capture_block in self.capture_blocks.items()
        }
        self._capture_block_sensor.set_value(json.dumps(value, sort_keys=True))

    async def _capture_init(self, capture_block: CaptureBlock) -> None:
        self.capture_blocks[capture_block.name] = capture_block
        capture_block.state_change_callback = self._update_capture_block_sensor
        # Update the sensor with the INITIALISING state
        self._update_capture_block_sensor()
        try:
            assert self.telstate is not None
            await self.telstate.add("sdp_capture_block_id", capture_block.name)
            for node in self.physical_graph:
                if isinstance(node, tasks.ProductPhysicalTask):
                    node.add_capture_block(capture_block)
            await self.exec_transitions(CaptureBlockState.CAPTURING, True, capture_block)
            if self.state == ProductState.ERROR:
                raise FailReply("Subarray product went into ERROR while starting capture")
        except asyncio.CancelledError:
            self._capture_block_dead(capture_block)
            raise
        except Exception:
            self.state = ProductState.ERROR
            self._capture_block_dead(capture_block)
            raise
        assert self.current_capture_block is None
        self.state = ProductState.CAPTURING
        self.current_capture_block = capture_block
        capture_block.state = CaptureBlockState.CAPTURING

    async def _capture_done(self, error_expected: bool = False) -> CaptureBlock:
        """The asynchronous task that handles ?capture-done. See
        :meth:`_capture_done_impl` for additional details.

        This is only called for a "normal" capture-done. Forced deconfigures
        call :meth:`_capture_done_impl` directly.

        Returns
        -------
        The capture block that was stopped
        """
        capture_block = self.current_capture_block
        done_exc: Optional[Exception] = None
        assert capture_block is not None
        try:
            await self._capture_done_impl(capture_block)
            if self.state == ProductState.ERROR and not error_expected:
                raise FailReply("Subarray product went into ERROR while stopping capture")
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self.state = ProductState.ERROR
            done_exc = exc
        assert self.current_capture_block is capture_block
        if self.state == ProductState.CAPTURING:
            self.state = ProductState.IDLE
        else:
            assert done_exc is not None or error_expected
        self.current_capture_block = None
        capture_block.state = CaptureBlockState.BURNDOWN
        capture_block.postprocess_task = asyncio.get_event_loop().create_task(
            self._postprocess(capture_block)
        )
        log_task_exceptions(
            capture_block.postprocess_task,
            logger,
            f"Exception in postprocessing for {self.subarray_product_id}/{capture_block.name}",
        )

        def done_callback(task: asyncio.Future, capture_block=capture_block) -> None:
            self._capture_block_dead(capture_block)

        capture_block.postprocess_task.add_done_callback(done_callback)
        if done_exc is not None:
            raise done_exc
        return capture_block

    def _clear_async_task(self, future: asyncio.Task) -> None:
        """Clear the current async task.

        Parameters
        ----------
        future
            The expected value of :attr:`_async_task`. If it does not match,
            it is not cleared (this can happen if another task replaced it
            already).
        """
        if self._async_task is future:
            self._async_task = None

    async def _replace_async_task(self, new_task: asyncio.Task) -> bool:
        """Set the current asynchronous task.

        If there is an existing task, it is atomically replaced then cancelled.

        Returns
        -------
        bool
            Whether `new_task` is now the current async task (it might not be
            if it was replaced after being installed but before we returned).
        """
        old_task = self._async_task
        self._async_task = new_task
        if old_task is not None:
            old_task.cancel()
            # Using asyncio.wait instead of directly yielding from the task
            # avoids re-raising any exception raised from the task.
            await asyncio.wait([old_task])
        return self._async_task is new_task

    async def configure(self) -> None:
        assert not self.async_busy, "configure should be the first thing to happen"
        assert (
            self.state == ProductState.CONFIGURING
        ), "configure should be the first thing to happen"
        task = asyncio.get_event_loop().create_task(self._configure())
        log_task_exceptions(
            task, logger, f"Configuring subarray product {self.subarray_product_id} failed"
        )
        self._async_task = task
        try:
            await task
        finally:
            self._clear_async_task(task)
        logger.info("Subarray product %s successfully configured", self.subarray_product_id)
        # A number of sensors are added without individually triggering interface-changed
        # notifications, so that subscribers don't try to repeatly refresh
        # their sensor lists.
        self.sdp_controller.mass_inform("interface-changed", "sensor-list")

    async def deconfigure(self, force: bool = False) -> bool:
        """Start deconfiguration of the subarray, but does not wait for it to complete.

        Returns
        -------
        delayed_deconfigure
            If true, there are remaining tasks which will take an
            indeterminate amount of time to complete before the product
            controller will terminate. If false, all the tasks are dead
            and the product controller will exit soon.
        """
        if self.state == ProductState.DEAD:
            return False
        if self.async_busy:
            if not force:
                self._fail_if_busy()
            else:
                logger.warning(
                    "Subarray product %s is busy with an operation, but deconfiguring anyway",
                    self.subarray_product_id,
                )

        if self.state not in (ProductState.IDLE, ProductState.ERROR):
            if not force:
                raise FailReply(
                    "Subarray product is not idle and thus cannot be deconfigured. "
                    "Please issue capture_done first."
                )
            else:
                logger.warning(
                    "Subarray product %s is in state %s, but deconfiguring anyway",
                    self.subarray_product_id,
                    self.state.name,
                )
        logger.info("Deconfiguring subarray product %s", self.subarray_product_id)

        ready = asyncio.Event()
        task = asyncio.get_event_loop().create_task(self._deconfigure(force, ready))
        log_task_exceptions(task, logger, f"Deconfiguring {self.subarray_product_id} failed")
        # Make sure that ready gets unblocked even if task throws.
        task.add_done_callback(lambda future: ready.set())
        task.add_done_callback(self._clear_async_task)
        if await self._replace_async_task(task):
            await ready.wait()
        # We don't necessarily wait for task to complete, but if it's already
        # done we pass back any exceptions.
        if task.done() or not self._delayed_deconfigure:
            await task
        return self._delayed_deconfigure

    async def capture_init(self, capture_block_id: str, configuration: Configuration) -> str:
        self._fail_if_busy()
        if self.state != ProductState.IDLE:
            raise FailReply(
                f"Subarray product {self.subarray_product_id} is currently in state "
                f"{self.state.name}, not IDLE as expected. Cannot be inited."
            )
        self._fail_if_no_telstate()
        logger.info("Using capture block ID %s", capture_block_id)

        capture_block = CaptureBlock(capture_block_id, configuration)
        task = asyncio.get_event_loop().create_task(self._capture_init(capture_block))
        self._async_task = task
        try:
            await task
        finally:
            self._clear_async_task(task)
        logger.info(
            "Started capture block %s on subarray product %s",
            capture_block_id,
            self.subarray_product_id,
        )
        return capture_block_id

    async def capture_done(self) -> str:
        self._fail_if_busy()
        if self.state != ProductState.CAPTURING:
            raise FailReply(
                f"Subarray product is currently in state {self.state.name}, "
                "not CAPTURING as expected. Cannot be stopped."
            )
        assert self.current_capture_block is not None
        capture_block_id = self.current_capture_block.name
        task = asyncio.get_event_loop().create_task(self._capture_done())
        self._async_task = task
        try:
            await task
        finally:
            self._clear_async_task(task)
        logger.info(
            "Finished capture block %s on subarray product %s",
            capture_block_id,
            self.subarray_product_id,
        )
        return capture_block_id

    def write_graphs(self, output_dir: str) -> None:
        """Write visualisations to `output_dir`."""
        for name in ["ready", "init", "kill", "resolve", "resources"]:
            if name != "resources":
                g = scheduler.subgraph(self.logical_graph, "depends_" + name)
            else:
                g = scheduler.subgraph(
                    self.logical_graph, scheduler.SchedulerBase.depends_resources
                )
            g = networkx.relabel_nodes(g, {node: node.name for node in g})
            g = networkx.drawing.nx_pydot.to_pydot(g)
            filename = os.path.join(output_dir, f"{self.subarray_product_id}_{name}.svg")
            try:
                g.write_svg(filename)
            except OSError as error:
                logger.warning("Could not write %s: %s", filename, error)

    async def set_delays(
        self, stream_name: str, timestamp: aiokatcp.Timestamp, coefficient_sets: Sequence[str]
    ) -> None:
        """Set F-engine delays."""
        # Validation
        if self.state not in {ProductState.CAPTURING, ProductState.IDLE}:
            raise FailReply(f"Cannot set delays in state {self.state}")
        stream = self._find_stream(stream_name)
        if not isinstance(stream, product_config.GpucbfAntennaChannelisedVoltageStream):
            raise FailReply(f"Stream {stream_name!r} is of the wrong type")
        if len(coefficient_sets) != len(stream.src_streams):
            raise FailReply(
                f"Wrong number of coefficient sets ({len(coefficient_sets)}, "
                f"expected {len(stream.src_streams)})"
            )
        for coefficient_set in coefficient_sets:
            try:
                parts = coefficient_set.split(":")
                if len(parts) != 2:
                    raise ValueError
                for part in parts:
                    terms = part.split(",")
                    for term in terms:
                        float(term)
            except ValueError:
                raise FailReply(f"Invalid coefficient-set {coefficient_set!r}")

        # Looks valid, now make the requests
        nodes = list(self.find_nodes(task_type="f", streams=[stream]))
        msgs = [
            (
                "delays",
                stream_name,
                timestamp,
                coefficient_sets[2 * node.logical_node.index],
                coefficient_sets[2 * node.logical_node.index + 1],
            )
            for node in nodes
        ]
        # Use debug-level logging because we expect to get this request every few seconds
        await self._multi_request(nodes, msgs, timeout=DELAYS_TIMEOUT, log_level=logging.DEBUG)

    async def gain(self, stream_name: str, input: str, values: Sequence[str]) -> Sequence[str]:
        """Set F-engine gains."""
        if self.state not in {ProductState.CAPTURING, ProductState.IDLE}:
            raise FailReply(f"Cannot set gains in state {self.state}")
        stream = self._find_stream(stream_name)
        if not isinstance(stream, product_config.GpucbfAntennaChannelisedVoltageStream):
            raise FailReply(f"Stream {stream_name!r} is of the wrong type")
        if len(values) not in {0, 1, stream.n_chans}:
            raise FailReply(f"Expected 0, 1, or {stream.n_chans} values, received {len(values)}")
        if input not in stream.input_labels:
            raise FailReply(f"Unknown input {input!r}")
        try:
            for v in values:
                complex(v)  # Evaluated just for the exception
        except ValueError as exc:
            raise FailReply(str(exc))

        # Looks valid, now make the request
        idx = stream.input_labels.index(input)
        node_idx = idx // 2
        nodes = list(self.find_nodes(task_type="f", streams=[stream], indices=[node_idx]))
        assert len(nodes) == 1
        node = nodes[0]
        reply, _ = await node.issue_req(
            "gain", (stream.name, idx % 2) + tuple(values), timeout=GAIN_TIMEOUT
        )
        return reply

    async def gain_all(self, stream_name: str, values: Sequence[str]) -> None:
        """Set F-engine gains for all inputs."""
        if self.state not in {ProductState.CAPTURING, ProductState.IDLE}:
            raise FailReply(f"Cannot set gains in state {self.state}")
        stream = self._find_stream(stream_name)
        if not isinstance(stream, product_config.GpucbfAntennaChannelisedVoltageStream):
            raise FailReply(f"Stream {stream_name!r} is of the wrong type")
        if len(values) not in {1, stream.n_chans}:
            raise FailReply(f"Expected 1, or {stream.n_chans} values, received {len(values)}")
        values = tuple(values)
        if values != ("default",):
            try:
                for v in values:
                    complex(v)  # Evaluated just for the exception
            except ValueError as exc:
                raise FailReply(str(exc))
        await self._multi_request(
            self.find_nodes(task_type="f", streams=[stream]),
            itertools.repeat(("gain-all", stream_name) + values),
            timeout=GAIN_TIMEOUT,
        )

    async def beam_weights(self, stream_name: str, weights: Sequence[float]) -> None:
        if self.state not in {ProductState.CAPTURING, ProductState.IDLE}:
            raise FailReply(f"Cannot set beam gains in state {self.state}")
        stream = self._find_stream(stream_name)
        if not isinstance(stream, product_config.GpucbfTiedArrayChannelisedVoltageStream):
            raise FailReply(f"Stream {stream_name!r} is of the wrong type")
        expected = len(stream.antenna_channelised_voltage.src_streams) // 2
        if len(weights) != expected:
            raise FailReply(f"Expected {expected} values, received {len(weights)}")
        await self._multi_request(
            self.find_nodes(task_type="xb", streams=[stream]),
            itertools.repeat(("beam-weights", stream_name) + tuple(weights)),
            timeout=GAIN_TIMEOUT,
        )

    async def beam_quant_gains(self, stream_name: str, value: float) -> None:
        if self.state not in {ProductState.CAPTURING, ProductState.IDLE}:
            raise FailReply(f"Cannot set beam gains in state {self.state}")
        stream = self._find_stream(stream_name)
        if not isinstance(stream, product_config.GpucbfTiedArrayChannelisedVoltageStream):
            raise FailReply(f"Stream {stream_name!r} is of the wrong type")
        await self._multi_request(
            self.find_nodes(task_type="xb", streams=[stream]),
            itertools.repeat(("beam-quant-gains", stream_name, value)),
            timeout=GAIN_TIMEOUT,
        )

    async def beam_delays(self, stream_name: str, coefficient_sets: Sequence[str]) -> None:
        if self.state not in {ProductState.CAPTURING, ProductState.IDLE}:
            raise FailReply(f"Cannot set beam gains in state {self.state}")
        stream = self._find_stream(stream_name)
        if not isinstance(stream, product_config.GpucbfTiedArrayChannelisedVoltageStream):
            raise FailReply(f"Stream {stream_name!r} is of the wrong type")
        expected = len(stream.antenna_channelised_voltage.src_streams) // 2
        if len(coefficient_sets) != expected:
            raise FailReply(f"Expected {expected} values, received {len(coefficient_sets)}")
        for coefficient_set in coefficient_sets:
            try:
                # Parse it just for validation
                parts = coefficient_set.split(":")
                if len(parts) != 2:
                    raise ValueError
                for part in parts:
                    float(part)
            except ValueError:
                raise FailReply(f"Invalid coefficient-set {coefficient_set!r}")
        await self._multi_request(
            self.find_nodes(task_type="xb", streams=[stream]),
            itertools.repeat(("beam-delays", stream_name) + tuple(coefficient_sets)),
            timeout=DELAYS_TIMEOUT,
        )

    async def vlbi_delay(self, stream_name: str, delay: float) -> None:
        if self.state != ProductState.IDLE:
            raise FailReply(f"Cannot set VLBI delays in state {self.state}")
        stream = self._find_stream(stream_name)
        if not isinstance(stream, product_config.GpucbfTiedArrayResampledVoltageStream):
            raise FailReply(f"Stream {stream_name!r} is of the wrong type")
        # TODO: Do we need a multi_request here? Considering we're only launching
        # one vgpu task per stream. Maybe future-proofing?
        pass

    def _get_dsim_katcp(self, dsim: str) -> aiokatcp.Client:
        """Get the katcp client for connecting to a dsim.

        Parameters
        ----------
        dsim
            The task name for the dsim

        Raises
        ------
        FailReply
            if the dsim name is not known, there is no connection to it, or the
            subarray product is not in a suitable state.
        """
        if self.state not in {ProductState.CAPTURING, ProductState.IDLE}:
            raise FailReply(f"Cannot set dsim signals in state {self.state}")
        for task in self.physical_graph:
            if (
                isinstance(task, tasks.ProductPhysicalTaskMixin)
                and task.logical_node.task_type == "sim"
                and task.name == dsim
            ):
                dsim_task = task
                break
        else:
            raise FailReply(f"No dsim named {dsim}")
        if dsim_task.katcp_connection is None:
            raise FailReply(f"No katcp connection to {dsim}")
        return dsim_task.katcp_connection

    async def dsim_signals(self, dsim: str, signals_str: str, period: Optional[int] = None) -> int:
        conn = self._get_dsim_katcp(dsim)
        # Have to annotate as 'list', as otherwise mypy infers list[str]
        args: list = ["signals", signals_str]
        if period is not None:
            args.append(period)
        reply, _ = await conn.request(*args)
        return aiokatcp.decode(int, reply[0])

    async def dsim_time(self, dsim: str) -> float:
        conn = self._get_dsim_katcp(dsim)
        reply, _ = await conn.request("time")
        return aiokatcp.decode(float, reply[0])

    async def capture_start_stop(self, stream_name: str, *, start: bool) -> None:
        """Either start or stop transmission on a stream."""
        if self.state not in {ProductState.CAPTURING, ProductState.IDLE}:
            raise FailReply(f"Cannot start or stop streams in state {self.state}")
        stream = self._find_stream(stream_name)
        if not isinstance(
            stream,
            (
                product_config.GpucbfBaselineCorrelationProductsStream,
                product_config.GpucbfTiedArrayChannelisedVoltageStream,
            ),
        ):
            raise FailReply(f"Stream {stream_name!r} is of the wrong type")

        nodes = self.find_nodes(task_type="xb", streams=[stream])
        if start:
            start_timestamp = 0
            for sensor in self.sdp_controller.sensors.values():
                if (
                    sensor.stype is int
                    and sensor.name.endswith(".steady-state-timestamp")
                    and sensor.status.valid_value()
                ):
                    start_timestamp = max(start_timestamp, sensor.value)
            await self._multi_request(
                nodes,
                itertools.repeat(("capture-start", stream_name, start_timestamp)),
            )
        else:
            await self._multi_request(
                nodes,
                itertools.repeat(("capture-stop", stream_name)),
            )

        for node in self.physical_graph.nodes:
            if (
                isinstance(node, generator.PhysicalMulticast)
                and node.logical_node.name == "multicast." + stream_name
            ):
                node.transmit_state = TransmitState.UP if start else TransmitState.DOWN

    def capture_list(self) -> Sequence[generator.PhysicalMulticast]:
        """Return all :class:`.PhysicalMulticast` nodes that have known stream state."""
        return [
            node
            for node in self.physical_graph
            if (
                isinstance(node, generator.PhysicalMulticast)
                and node.transmit_state != TransmitState.UNKNOWN
            )
        ]

    async def _launch_telstate(self) -> katsdptelstate.aio.TelescopeState:
        """Make sure the telstate node is launched"""
        assert self.telstate_node is not None
        boot = [self.telstate_node]

        init_telstate = copy.deepcopy(self.physical_graph.graph.get("init_telstate", {}))
        init_telstate["subarray_product_id"] = self.subarray_product_id
        config_func = self.physical_graph.graph.get("config", lambda resolver: {})
        init_telstate["config"] = config_func(self.resolver)
        # Provide attributes to describe the relationships between CBF streams
        # and instruments. This could be extracted from sdp_config, but these
        # specific sensors are easier to mock.
        for stream in self.configuration.streams:
            if isinstance(stream, product_config.CbfStream):
                init_telstate[(stream.name, "instrument_dev_name")] = stream.instrument_dev_name
                if stream.src_streams:
                    init_telstate[(stream.name, "src_streams")] = [
                        src_stream.name for src_stream in stream.src_streams
                    ]

        # Load canonical model URLs
        if not self.configuration.options.interface_mode:
            model_base_url = self.resolver.s3_config["models"]["read"]["url"]
            if not model_base_url.endswith("/"):
                model_base_url += "/"  # Ensure it is a directory
            init_telstate["sdp_model_base_url"] = model_base_url
            async with katsdpmodels.fetch.aiohttp.Fetcher() as fetcher:
                rfi_mask_model_urls = await _resolve_model(
                    fetcher, model_base_url, "rfi_mask/current.alias"
                )
                init_telstate[("model", "rfi_mask", "config")] = rfi_mask_model_urls[0]
                init_telstate[("model", "rfi_mask", "fixed")] = rfi_mask_model_urls[1]
                for stream in itertools.chain(
                    self.configuration.by_class(product_config.AntennaChannelisedVoltageStream),
                    self.configuration.by_class(product_config.SimAntennaChannelisedVoltageStream),
                    self.configuration.by_class(
                        product_config.GpucbfAntennaChannelisedVoltageStream
                    ),
                ):
                    ratio = round(stream.adc_sample_rate / 2 / stream.bandwidth)
                    band_mask_model_urls = await _resolve_model(
                        fetcher,
                        model_base_url,
                        f"band_mask/current/{stream.band}/nb_ratio={ratio}.alias",
                    )
                    prefix: Tuple[str, ...] = (stream.normalised_name, "model", "band_mask")
                    init_telstate[prefix + ("config",)] = band_mask_model_urls[0]
                    init_telstate[prefix + ("fixed",)] = band_mask_model_urls[1]
                    for group in ["individual", "cohort"]:
                        config_value = _IndexedKey()
                        fixed_value = _IndexedKey()
                        for ant in stream.antennas:
                            pb_model_urls = await _resolve_model(
                                fetcher,
                                model_base_url,
                                f"primary_beam/current/{group}/{ant}/{stream.band}.alias",
                            )
                            config_value[ant] = pb_model_urls[0]
                            fixed_value[ant] = pb_model_urls[1]
                        prefix = (stream.normalised_name, "model", "primary_beam", group)
                        init_telstate[prefix + ("config",)] = config_value
                        init_telstate[prefix + ("fixed",)] = fixed_value

        logger.debug("Launching telstate. Initial values %s", init_telstate)
        await self.sched.launch(self.physical_graph, self.resolver, boot)
        # connect to telstate store
        telstate_backend: katsdptelstate.aio.backend.Backend
        if self.configuration.options.interface_mode:
            telstate_backend = katsdptelstate.aio.memory.MemoryBackend()
        else:
            self.telstate_endpoint = "{}:{}".format(
                self.telstate_node.host, self.telstate_node.ports["telstate"]
            )
            telstate_backend = await katsdptelstate.aio.redis.RedisBackend.from_url(
                f"redis://{self.telstate_endpoint}"
            )
        telstate = katsdptelstate.aio.TelescopeState(telstate_backend)
        self.telstate = telstate
        self.resolver.telstate = telstate

        # set the configuration
        for k, v in init_telstate.items():
            key = telstate.join(*k) if isinstance(k, tuple) else k
            if isinstance(v, _IndexedKey):
                for sub_key, sub_value in v.items():
                    await telstate.set_indexed(key, sub_key, sub_value)
            else:
                await telstate.set(key, v)
        return telstate

    def check_nodes(self) -> Tuple[bool, List[scheduler.PhysicalNode]]:
        """Check that all requested nodes are actually running.

        Returns
        -------
        result
            True if all tasks are in state :const:`~scheduler.TaskState.READY`.
        died
            Nodes that have died unexpectedly (does not include nodes that we
            killed).

        .. todo::

           Also check health state sensors
        """
        died = []
        result = True
        for node in self.physical_graph:
            if node.state != scheduler.TaskState.READY:
                if node.state == scheduler.TaskState.DEAD and not node.death_expected:
                    died.append(node)
                result = False
        return result, died

    def unexpected_death(self, task: tasks.ProductAnyPhysicalTask) -> None:
        logger.warning("Task %s died unexpectedly", task.name)
        if task.logical_node.critical:
            self._go_to_error()

    def bad_device_status(self, task: tasks.ProductAnyPhysicalTask) -> None:
        logger.warning("Task %s has failed (device-status)", task.name)
        if task.logical_node.critical:
            self._go_to_error()

    def _go_to_error(self) -> None:
        """Switch to :const:`ProductState.ERROR` due to an external event.

        This is used when a failure in some task is detected asynchronously, but
        not when a katcp transition fails.
        """
        # Try to wind up the current capture block so that we don't lose any
        # data already captured.  However, if we're in the middle of another
        # async operation we just let that run, because that operation is either
        # a deconfigure or it will notice the ERROR state when it finishes and
        # fail.
        #
        # Some of this code is copy-pasted from capture_done. Unfortunately
        # it's not straightforward to reuse the code because we have to do the
        # initial steps (particularly replacement of _async_task) synchronously
        # after checking async_busy, rather than creating a new task to run
        # capture_done.
        if self.state == ProductState.CAPTURING and not self.async_busy:
            assert self.current_capture_block is not None
            capture_block_id = self.current_capture_block.name
            logger.warning("Attempting to terminate capture block %s", capture_block_id)
            task = asyncio.get_event_loop().create_task(self._capture_done(error_expected=True))
            self._async_task = task
            log_task_exceptions(
                task, logger, f"Failed to terminate capture block {capture_block_id}"
            )

            def cleanup(task):
                self._clear_async_task(task)
                logger.info(
                    "Finished capture block %s on subarray product %s",
                    capture_block_id,
                    self.subarray_product_id,
                )

            task.add_done_callback(cleanup)

        # We don't go to error state from CONFIGURING because we check all
        # nodes at the end of configuration and will fail the configure
        # there; and from DECONFIGURING/POSTPROCESSING we don't want to go to
        # ERROR because that may prevent deconfiguring.
        if self.state in (ProductState.IDLE, ProductState.CAPTURING):
            self.state = ProductState.ERROR

    async def _shutdown(self, force: bool) -> None:
        # Rather than marking data as bad as each engine shuts down, just do a
        # single update per sensor now.
        data_suspect_sensors = set()
        for task in self.physical_graph:
            if isinstance(task, tasks.ProductPhysicalTaskMixin):
                data_suspect_sensors.update(task.logical_node.data_suspect_sensors)
        for sensor in data_suspect_sensors:
            sensor.set_value("1" * len(sensor.value), status=Sensor.Status.ERROR)
        # TODO: issue progress reports as tasks stop
        await self.sched.kill(self.physical_graph, force=force, capture_blocks=self.capture_blocks)


class DeviceServer(aiokatcp.DeviceServer):
    VERSION = "product-controller-1.1"
    BUILD_STATE = "katsdpcontroller-" + katsdpcontroller.__version__

    def __init__(
        self,
        host: str,
        port: int,
        master_controller: aiokatcp.Client,
        subarray_product_id: str,
        sched: scheduler.SchedulerBase,
        batch_role: str,
        interface_mode: bool,
        localhost: bool,
        image_resolver_factory: scheduler.ImageResolverFactory,
        s3_config: dict,
        graph_dir: Optional[str] = None,
        dashboard_url: Optional[str] = None,
        prometheus_registry: CollectorRegistry = REGISTRY,
        shutdown_delay: float = SHUTDOWN_DELAY,
    ) -> None:
        self.sched = sched
        self.subarray_product_id = subarray_product_id
        self.batch_role = batch_role
        self.interface_mode = interface_mode
        self.localhost = localhost
        self.image_resolver_factory = image_resolver_factory
        self.s3_config = _normalise_s3_config(s3_config)
        self.graph_dir = graph_dir
        self.master_controller = master_controller
        self.product: Optional[SubarrayProduct] = None
        self.shutdown_delay = shutdown_delay

        super().__init__(host, port, max_backlog=CONNECTION_MAX_BACKLOG)
        # setup sensors (note: ProductController adds other sensors)
        self.sensors.add(
            Sensor(
                DeviceStatus,
                "device-status",
                "Devices status of the subarray product controller",
                default=DeviceStatus.OK,
                status_func=device_status_to_sensor_status,
            )
        )
        gui_urls: List[Dict[str, str]] = []
        if dashboard_url is not None:
            gui_urls.append(
                {
                    "title": "Dashboard",
                    "description": "Product controller dashboard",
                    "category": "Dashboard",
                    "href": dashboard_url,
                }
            )
        self.sensors.add(
            Sensor(
                str,
                "gui-urls",
                "URLs for product-wide GUIs",
                default=json.dumps(gui_urls),
                initial_status=Sensor.Status.NOMINAL,
            )
        )
        self._prometheus_watcher = sensor_proxy.PrometheusWatcher(
            self.sensors,
            {"subarray_product_id": subarray_product_id},
            functools.partial(_prometheus_factory, prometheus_registry),
        )
        task_stats = sched.task_stats
        if isinstance(task_stats, TaskStats):
            for sensor in task_stats.sensors.values():
                self.sensors.add(sensor)
        self._consul_service = ConsulService()

    async def _consul_register(self) -> None:
        if not isinstance(self.sched, scheduler.Scheduler):
            # This indicates we're running in interface mode, so avoid
            # advertising ourselves to consul and causing confusion.
            return
        port = self.sched.http_port
        service = {
            "Name": "product-controller",
            "Tags": ["prometheus-metrics"],
            "Meta": {"subarray_product_id": self.subarray_product_id},
            "Port": port,
            "Checks": [
                {
                    "Interval": "15s",
                    "Timeout": "5s",
                    "HTTP": f"http://{LOCALHOST}:{port}/health",
                    "DeregisterCriticalServiceAfter": "90s",
                }
            ],
        }
        self._consul_service = await ConsulService.register(service)

    async def start(self) -> None:
        await self.master_controller.wait_connected()
        await self._consul_register()
        await super().start()

    async def on_stop(self) -> None:
        await self._consul_service.deregister()
        self._prometheus_watcher.close()
        if self.product is not None and self.product.state != ProductState.DEAD:
            logger.warning("Product controller interrupted - deconfiguring running product")
            try:
                await self.product.deconfigure(force=True)
            except Exception:
                logger.warning("Failed to deconfigure product %s during shutdown", exc_info=True)
        self.master_controller.close()
        await self.master_controller.wait_closed()
        await self.sched.close()

    async def join(self) -> None:
        await super().join()
        # Ensure that the connection is closed even if start() did not
        # complete (in which case on_stop might not run). This could happen
        # if cancelled while waiting to connect to the master controller.
        self.master_controller.close()
        await self.master_controller.wait_closed()

    async def configure_product(
        self, name: str, configuration: Configuration, config_dict: dict
    ) -> None:
        """Configure a subarray product in response to a request.

        Raises
        ------
        FailReply
            if a configure/deconfigure is in progress
        FailReply
            If any of the following occur
            - The specified subarray product id already exists, but the config
              differs from that specified
            - If docker python libraries are not installed and we are not using interface mode
            - There are insufficient resources to launch
            - A docker image could not be found
            - If one or more nodes fail to launch (e.g. container not found)
            - If one or more nodes fail to become alive
            - If we fail to establish katcp connection to all nodes requiring them.

        Returns
        -------
        str
            Final name of the subarray-product.
        """
        if configuration.options.shutdown_delay is not None:
            shutdown_delay = configuration.options.shutdown_delay
        else:
            shutdown_delay = self.shutdown_delay

        def dead_callback(product):
            if shutdown_delay > 0:
                logger.info(
                    "Sleeping %.1f seconds to give time for final Prometheus scrapes",
                    shutdown_delay,
                )
                asyncio.get_event_loop().call_later(shutdown_delay, self.halt, False)
            else:
                self.halt(False)

        logger.debug("config is %s", json.dumps(config_dict, indent=2, sort_keys=True))
        logger.info("Launching subarray product.")

        image_tag = configuration.options.image_tag
        if image_tag is not None:
            resolver_factory_args = dict(tag=image_tag)
        else:
            resolver_factory_args = {}
        image_resolver = self.image_resolver_factory(**resolver_factory_args)
        for image_name, image_path in configuration.options.image_overrides.items():
            image_resolver.override(image_name, image_path)
        resolver = Resolver(
            image_resolver,
            scheduler.TaskIDAllocator(name + "-"),
            self.sched.http_url if self.sched else "",
            configuration.options.service_overrides,
            self.s3_config,
            self.localhost,
        )

        # create graph object and build physical graph from specified resources
        product = SubarrayProduct(self.sched, configuration, config_dict, resolver, name, self)
        if self.graph_dir is not None:
            product.write_graphs(self.graph_dir)
        self.product = product  # Prevents another attempt to configure
        self.product.dead_callbacks.append(dead_callback)
        try:
            await product.configure()
        except BaseException:
            self.product = None
            raise

    async def request_product_configure(self, ctx, name: str, config: str) -> None:
        """Configure a subarray product instance.

        Parameters
        ----------
        name : str
            Name of the subarray product.
        config : str
            A JSON-encoded dictionary of configuration data.
        """
        # TODO: remove name - it is already a command-line argument
        logger.info("?product-configure called with: %s", ctx.req)

        if self.product is not None:
            raise FailReply("Already configured or configuring")
        try:
            config_dict = load_json_dict(config)
            configuration = await Configuration.from_config(config_dict)
            configuration.options.interface_mode = self.interface_mode
        except product_config.SensorFailure as exc:
            retmsg = f"Error retrieving sensor data from CAM: {exc}"
            logger.error(retmsg)
            raise FailReply(retmsg) from exc
        except (ValueError, jsonschema.ValidationError) as exc:
            retmsg = f"Failed to process config: {exc}"
            logger.error(retmsg)
            raise FailReply(retmsg) from exc

        await self.configure_product(name, configuration, config_dict)

    def _get_product(self) -> SubarrayProduct:
        """Check that self.product exists (i.e. ?product-configure has been called).

        If it has not, raises a :exc:`FailReply`.
        """
        if self.product is None:
            raise FailReply(
                "?product-configure has not been called yet. "
                "It must be called before other requests."
            )
        return self.product

    async def request_product_deconfigure(self, ctx, force: bool = False) -> bool:
        """Deconfigure the product and shut down the server.

        Returns
        -------
        delayed_deconfigure : bool
            If true, there are remaining tasks which will take an
            indeterminate amount of time to complete before the product
            controller will terminate. If false, all the tasks are dead
            and the product controller will exit soon.
        """
        return await self._get_product().deconfigure(force=force)

    async def request_capture_init(
        self, ctx, capture_block_id: str, override_dict_json: str = "{}"
    ) -> None:
        """Request capture of the specified subarray product to start.

        Parameters
        ----------
        capture_block_id : str
            The capture block ID for the new capture block.
        override_dict_json : str, optional
            Configuration dictionary to merge with the subarray config.
        """
        product = self._get_product()
        try:
            overrides = load_json_dict(override_dict_json)
        except ValueError as error:
            retmsg = f"Override {override_dict_json} is not a valid JSON dict: {error}"
            logger.error(retmsg)
            raise FailReply(retmsg) from error

        config_dict = product_config.override(product.config_dict, overrides)
        # Re-validate, since the override may have broken it
        try:
            configuration = await Configuration.from_config(config_dict)
            configuration.options.interface_mode = self.interface_mode
        except (ValueError, jsonschema.ValidationError) as error:
            retmsg = f"Overrides make the config invalid: {error}"
            logger.error(retmsg)
            raise FailReply(retmsg) from error

        try:
            product_config.validate_capture_block(product.config_dict, config_dict)
        except ValueError as error:
            retmsg = f"Invalid config override: {error}"
            logger.error(retmsg)
            raise FailReply(retmsg) from error

        await product.capture_init(capture_block_id, configuration)

    async def request_telstate_endpoint(self, ctx) -> str:
        """Returns the endpoint for the telescope state repository.

        Returns
        -------
        endpoint : str
        """
        return self._get_product().telstate_endpoint

    async def request_capture_status(self, ctx) -> ProductState:
        """Returns the status of the subarray product.

        Returns
        -------
        state : str
        """
        return self._get_product().state

    async def request_capture_done(self, ctx) -> str:
        """Halts the current capture block.

        Returns
        -------
        cbid : str
            Capture-block ID that was stopped
        """
        cbid = await self._get_product().capture_done()
        return cbid

    async def request_delays(
        self, ctx, stream: str, time: aiokatcp.Timestamp, *coefficient_set: str
    ) -> None:
        """Set F-engine delays.

        Parameters
        ----------
        stream
            Antenna-channelised-voltage stream on which to operate
        time
            Time at which delays will be applied
        coefficient_set
            Coefficients for each input, in the format ``delay,delay_rate:phase,phase_rate``
        """
        await self._get_product().set_delays(stream, time, coefficient_set)

    async def request_gain(self, ctx, stream: str, input: str, *values: str) -> Tuple[str, ...]:
        """Set or query F-engine gains.

        Parameters
        ----------
        stream
            Antenna-channelised-voltage stream on which to operate
        input
            Single-pol input name on which to operate
        values
            A complex gain per channel, in the format <real>+<imag>j. It may
            also be a single value to be applied across all channels, or
            omitted to query the current gains.

        Returns
        -------
        values
            A complex gain per channel, or a single value that is used for all
            channels, or nothing if gains were being set.
        """
        return tuple(await self._get_product().gain(stream, input, values))

    async def request_gain_all(self, ctx, stream: str, *values: str) -> None:
        """Set F-engine gains for all inputs.

        Parameters
        ----------
        stream
            Antenna-channelised-voltage stream on which to operate
        values
            A complex gain per channel, in the format <real>+<imag>j. It may
            also be a single value to be applied across all channels, or
            "default" to restore the gains used at startup.
        """
        await self._get_product().gain_all(stream, values)

    async def request_beam_weights(self, ctx, stream: str, *weights: float) -> None:
        """Set input weights for a single beamformer data stream.

        Parameters
        ----------
        stream
            Tied-array-channelised-voltage stream on which to operate
        *weights
            Real-valued weights (one per input)
        """
        await self._get_product().beam_weights(stream, weights)

    async def request_beam_quant_gains(self, ctx, stream: str, value: float) -> None:
        """Set output gain of a single beamformer data stream.

        Parameters
        ----------
        stream
            Tied-array-channelised-voltage stream on which to operate
        value
            A real-value scaled factor
        """
        await self._get_product().beam_quant_gains(stream, value)

    async def request_beam_delays(self, ctx, stream: str, *coefficient_sets: str) -> None:
        """Set delays for a single beamformer data stream.

        Parameters
        ----------
        stream
            Tied-array-channelised-voltage stream on which to operate
        *coefficient_sets
            One coefficient set per input, each in the form <delay>:<phase>
            where the delay is in seconds, the phase in radians and the phase
            specifies the overall phase to apply at the centre of the band.
        """
        await self._get_product().beam_delays(stream, coefficient_sets)

    async def request_vlbi_delay(self, ctx, stream: str, delay: float) -> None:
        """Set delay for a single VLBI data stream.

        Parameters
        ----------
        stream
            Tied-array-resampled-voltage stream on which to operate
        delay
            Delay in seconds to apply to the stream
        """
        await self._get_product().vlbi_delay(stream, delay)

    async def request_dsim_signals(
        self, ctx, dsim: str, signals_str: str, period: Optional[int] = None
    ) -> int:
        """Update the signals that are generated by a dsim.

        Parameters
        ----------
        dsim
            The task name for the dsim (implementation-defined).
        signals_str
            Textual description of the signals. Refer to the katgpucbf manual
            for details. The description must produce one signal per polarisation.
        period
            Period for the generated signal. It must divide into the value
            indicated by the ``max-period`` sensor of the dsim. If not
            specified, the value of ``max-period`` is used.

        Returns
        -------
        timestamp
            First timestamp which will use the new signals.
        """
        return await self._get_product().dsim_signals(dsim, signals_str, period)

    async def request_dsim_time(self, ctx, dsim: str) -> float:
        """Return the current UNIX timestamp from a dsim."""
        return await self._get_product().dsim_time(dsim)

    async def request_capture_start(self, ctx, stream: str) -> None:
        """Enable data transmission for the named data stream."""
        await self._get_product().capture_start_stop(stream, start=True)

    async def request_capture_stop(self, ctx, stream: str) -> None:
        """Halt data transmission for the named data stream."""
        await self._get_product().capture_start_stop(stream, start=False)

    async def request_capture_list(self, ctx, stream: Optional[str] = None) -> None:
        """List CBF data streams."""
        multicasts = self._get_product().capture_list()
        response = []
        for mc in multicasts:
            name = mc.logical_node.name.split(".", 1)[1]  # Strip off "multicast." prefix
            if stream is not None and stream != name:
                continue
            response.append(
                (name, str(Endpoint(mc.host, mc.ports["spead"])), mc.transmit_state.name.lower())
            )
        if stream is not None and not response:
            raise FailReply(f"Unknown stream {stream!r}")
        ctx.informs(response)
