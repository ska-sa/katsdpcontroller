import logging
import json
import asyncio
import socket
import ipaddress
import enum
import os
import re

from addict import Dict
import async_timeout

import aiokatcp
from aiokatcp import FailReply, InvalidReply, Sensor
from prometheus_client import Gauge, Counter, Histogram

from katsdptelstate.endpoint import Endpoint

from . import scheduler, sensor_proxy, product_config


logger = logging.getLogger(__name__)
# Name of edge attribute, as a constant to better catch typos
DEPENDS_INIT = 'depends_init'
PROMETHEUS_LABELS = ('subarray_product_id', 'service')
# Dictionary of sensors to be exposed via Prometheus.
# Some of these will match multiple nodes, which is fine since they get labels
# in Prometheus.
PROMETHEUS_SENSORS = {}
_HINT_RE = re.compile(r'\bprometheus: *(?P<type>[a-z]+)(?:\((?P<args>[^)]*)\)|\b)', re.IGNORECASE)


def _prometheus_factory(name, sensor):
    match = _HINT_RE.search(sensor.description)
    if not match:
        return None, None
    type_ = match.group('type').lower()
    args_ = match.group('args')
    kwargs = {}
    if type_ == 'counter':
        class_ = Counter
        if args_ is not None:
            logger.warning('Arguments are not supported for counters (%s)', sensor.name)
    elif type_ == 'gauge':
        class_ = Gauge
        if args_ is not None:
            logger.warning('Arguments are not supported for gauges (%s)', sensor.name)
    elif type_ == 'histogram':
        class_ = Histogram
        if args_ is not None:
            try:
                buckets = [float(x.strip()) for x in args_.split(',')]
                kwargs['buckets'] = buckets
            except ValueError as exc:
                logger.warning('Could not parse histogram buckets (%s): %s', sensor.name, exc)
    else:
        logger.warning('Unknown Prometheus metric type %s for %s', type_, sensor.name)
        return None, None
    return (
        class_('katsdpcontroller_' + name, sensor.description, PROMETHEUS_LABELS, **kwargs),
        Gauge('katsdpcontroller_' + name + '_status',
              'Status of katcp sensor ' + name, PROMETHEUS_LABELS)
    )


class CaptureBlockState(enum.Enum):
    """State of a single capture block."""
    INITIALISING = 0
    CAPTURING = 1
    POSTPROCESSING = 2
    DEAD = 3


class KatcpTransition(object):
    """A katcp request to issue on a state transition

    Parameters
    ----------
    name : str
        Request name
    *args : str
        Request arguments. String arguments are passed through
        :meth:`str.format`: see
        :meth:`.SDPSubarrayProduct.exec_node_transitions` for the keys that can
        be substituted.
    timeout : float
        Maximum time to wait for the query to succeed.
    """
    def __init__(self, name, *args, timeout=None):
        self.name = name
        self.args = args
        if timeout is None:
            raise ValueError('timeout is required')
        self.timeout = timeout

    def format(self, *args, **kwargs):
        """Apply string formatting to each argument and return a new object"""
        formatted_args = [arg.format(*args, **kwargs) if isinstance(arg, str) else arg
                          for arg in self.args]
        return KatcpTransition(self.name, *formatted_args, timeout=self.timeout)

    def __repr__(self):
        args = ['{!r}'.format(arg) for arg in (self.name,) + self.args]
        return 'KatcpTransition({}, timeout={!r})'.format(', '.join(args), self.timeout)


class SDPLogicalTask(scheduler.LogicalTask):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.physical_factory = SDPPhysicalTask
        self.transitions = {}
        # List of dictionaries for a .gui-urls sensor. The fields are expanded
        # using str.format(self).
        self.gui_urls = []
        # Whether to wait for it to die before returning from product-deconfigure
        self.deconfigure_wait = True
        # Whether to wait until all capture blocks are completely dead before killing
        self.wait_capture_blocks_dead = False
        # Whether we should abort the capture block if the task fails
        self.critical = True
        # Whether to set config keys in telstate (only useful for processes that use katsdpservices)
        self.katsdpservices_config = True
        # Set to true if the image uses katsdpservices.setup_logging() and hence
        # can log directly to logstash without logspout.
        self.katsdpservices_logging = True


class SDPConfigMixin:
    """Mixin class that takes config information from the graph and sets it in telstate."""
    def _graph_config(self, resolver, graph):
        return graph.node[self].get('config', lambda task_, resolver_: {})(self, resolver)

    async def resolve(self, resolver, graph, loop):
        await super().resolve(resolver, graph, loop)
        if not self.logical_node.katsdpservices_config:
            if self._graph_config(resolver, graph):
                logger.warning('Graph node %s has explicit config but katsdpservices_config=False',
                               self.name)
            return

        # Not every task will take a --external-hostname option, but the
        # katsdpservices argument parser doesn't mind unused arguments.
        config = {}
        if self.host is not None:   # Can happen if this isn't a PhysicalTask
            config['external_hostname'] = self.host
        for name, value in self.ports.items():
            config[name] = value
        for _src, trg, attr in graph.out_edges(self, data=True):
            endpoint = None
            if 'port' in attr and trg.state >= scheduler.TaskState.STARTING:
                port = attr['port']
                endpoint = Endpoint(trg.host, trg.ports[port])
            config.update(attr.get('config', lambda task_, resolver_, endpoint_: {})(
                self, resolver, endpoint))
        config.update(self._graph_config(resolver, graph))
        overrides = resolver.service_overrides.get(self.logical_node.name, {}).get('config')
        if overrides:
            logger.warning('Overriding config for %s', self.name)
            config = product_config.override(config, overrides)
        logger.debug('Config for %s: %s', self.name, config)
        self.task_config = config
        if config:
            resolver.telstate.add('config.' + self.logical_node.name, config, immutable=True)


class CaptureBlockStateObserver:
    """Watches a capture-block-state sensor in a child.
    Users can wait for specific conditions to be satisfied.
    """
    def __init__(self, sensor, loop, logger):
        self.sensor = sensor
        self.loop = loop
        self.logger = logger
        self._last = {}
        self._waiters = []    # Each a tuple of a predicate and a future
        self(sensor, sensor.reading)
        sensor.attach(self)

    def __call__(self, sensor, reading):
        if reading.status in [Sensor.Status.NOMINAL, Sensor.Status.WARN, Sensor.Status.ERROR]:
            try:
                value = json.loads(reading.value.decode('utf-8'))
            except ValueError:
                self.logger.warning('Invalid JSON in %s: %r', sensor.name, reading.value)
            else:
                if not isinstance(value, dict):
                    self.logger.warning('%s is not a dict: %r', sensor.name, reading.value)
                else:
                    self._last = value
                    self._trigger()

    def _trigger(self):
        """Called when the sensor value changes, to wake up waiters"""
        new_waiters = []
        for waiter in self._waiters:
            if not waiter[1].done():   # Skip over cancelled futures
                if waiter[0](self._last):
                    waiter[1].set_result(None)
                else:
                    new_waiters.append(waiter)  # Not ready yet, keep for later
        self._waiters = new_waiters

    async def wait(self, condition):
        if condition(self._last):
            return      # Already satisfied, no need to wait
        future = asyncio.Future(loop=self.loop)
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
        self._trigger()   # Give waiters a chance to react to an empty map
        for waiter in self._waiters:
            waiter[1].set_exception(ConnectionResetError())
        self._waiters = []


class DeviceStatusObserver:
    """Watches a device-status sensor from a child, and reports when it is in error."""

    def __init__(self, sensor, task, loop):
        self.sensor = sensor
        self.task = task
        sensor.attach(self)
        # Arrange to observe the initial value
        loop.call_soon(self, sensor, sensor.reading)

    def __call__(self, sensor, reading):
        if reading.status == Sensor.Status.ERROR:
            self.task.subarray_product.bad_device_status(self.task)

    def close(self):
        self.sensor.detach(self)


class SDPPhysicalTask(SDPConfigMixin, scheduler.PhysicalTask):
    """Augments the parent class with SDP-specific functionality.

    It
    - Tracks the owning controller, subarray product and capture block ID
    - Provides Docker labels to report the above
    - Provides a number of internal katcp sensors
    - Tracks live capture block IDs for this task
    - Connects to the service's katcp port and mirrors katcp sensors. Such
      are exposed at the master controller level using the
      following syntax:
        <subarray_product_id>.<name>.<sensor_name>
      For example:
        array_1.ingest.1.input_rate
    """
    def __init__(self, logical_task, loop, sdp_controller, subarray_product, capture_block_id):
        # Turn .status into a property that updates a sensor
        self._status = None
        super().__init__(logical_task, loop)
        self.sdp_controller = sdp_controller
        self.subarray_product = subarray_product
        self.capture_block_id = capture_block_id   # Only useful for batch tasks
        self.logger = logging.LoggerAdapter(
            logger, dict(subarray_product_id=self.subarray_product_id, child_task=self.name))
        if capture_block_id is None:
            self.name = '.'.join([self.subarray_product_id, logical_task.name])
        else:
            self.name = '.'.join([self.subarray_product_id, capture_block_id, logical_task.name])
        self.gui_urls = []
        # dict of exposed KATCP sensors. This excludes the state sensors, which
        # are present even when the process is not running.
        self.sensors = {}
        # Capture block names for CBs that haven't terminated on this node yet.
        # Names are used rather than the objects to reduce the number of cyclic
        # references.
        self._capture_blocks = set()
        # Event set to true whenever _capture_blocks is empty
        self._capture_blocks_empty = asyncio.Event(loop=loop)
        self._capture_blocks_empty.set()

        self._state_sensor = Sensor(scheduler.TaskState, self.name + '.state',
                                    "State of the state machine", "",
                                    default=self.state,
                                    initial_status=Sensor.Status.NOMINAL)
        self._mesos_state_sensor = Sensor(
            str, self.name + '.mesos-state', 'Mesos-reported task state', '')
        # Note: these sensors are added to the subarray product and not self
        # so that they don't get removed when the task dies.
        self.subarray_product.add_sensor(self._state_sensor)
        self.subarray_product.add_sensor(self._mesos_state_sensor)

        self.katcp_connection = None
        self.capture_block_state_observer = None
        self.device_status_observer = None

    def subst_args(self, resolver):
        args = super().subst_args(resolver)
        if self.capture_block_id is not None:
            args['capture_block_id'] = self.capture_block_id
        return args

    @property
    def subarray_product_id(self):
        return self.subarray_product.subarray_product_id

    @property
    def status(self):
        return self._status

    @status.setter
    def status(self, value):
        self._status = value
        if value is not None:
            self._mesos_state_sensor.value = value.state

    def get_transition(self, state):
        """Get state transition actions"""
        return self.logical_node.transitions.get(state, [])

    async def issue_req(self, req, args=(), timeout=None):
        """Issue a request to the katcp connection.

        The reply and informs are returned. If the request failed, a log
        message is printed and FailReply is raised.
        """
        if self.katcp_connection is None:
            raise ValueError('Cannot issue request without a katcp connection')
        self.logger.info("Issuing request %s %s to node %s (timeout %gs)",
                         req, args, self.name, timeout)
        try:
            with async_timeout.timeout(timeout):
                await self.katcp_connection.wait_connected()
                reply, informs = await self.katcp_connection.request(req, *args)
            self.logger.info("Request %s %s to node %s successful", req, args, self.name)
            return (reply, informs)
        except (FailReply, InvalidReply, OSError, asyncio.TimeoutError) as error:
            msg = "Failed to issue req {} to node {}. {}".format(req, self.name, error)
            self.logger.warning('%s', msg)
            raise FailReply(msg) from error

    async def wait_ready(self):
        await super().wait_ready()
        # establish katcp connection to this node if appropriate
        if 'port' in self.ports:
            while True:
                self.logger.info("Attempting to establish katcp connection to %s:%s for node %s",
                                 self.host, self.ports['port'], self.name)
                prefix = self.name + '.'
                labels = (self.subarray_product_id, self.logical_node.name)
                self.katcp_connection = sensor_proxy.SensorProxyClient(
                    self.sdp_controller, prefix,
                    PROMETHEUS_SENSORS, labels, _prometheus_factory,
                    host=self.host, port=self.ports['port'], loop=self.loop)
                try:
                    await self.katcp_connection.wait_synced()
                    self.logger.info("Connected to %s:%s for node %s",
                                     self.host, self.ports['port'], self.name)
                    sensor = self.sdp_controller.sensors.get(prefix + 'capture-block-state')
                    if sensor is not None:
                        self.capture_block_state_observer = CaptureBlockStateObserver(
                            sensor, loop=self.loop, logger=self.logger)
                    sensor = self.sdp_controller.sensors.get(prefix + 'device-status')
                    if sensor is not None:
                        self.device_status_observer = DeviceStatusObserver(
                            sensor, self, loop=self.loop)
                    return
                except RuntimeError:
                    self.katcp_connection.close()
                    await self.katcp_connection.wait_closed()
                    # no need for these to lurk around
                    self.katcp_connection = None
                    self.logger.exception("Failed to connect to %s via katcp on %s:%d. "
                                          "Check to see if networking issues could be to blame.",
                                          self.name, self.host, self.ports['port'])
                    # Sleep for a bit to avoid hammering the port if there
                    # is a quick failure, before trying again.
                    await asyncio.sleep(1.0, loop=self.loop)

    def _add_sensor(self, sensor):
        """Add the supplied Sensor object to the top level device and
           track it locally.
        """
        self.sensors[sensor.name] = sensor
        self.sdp_controller.sensors.add(sensor)

    def _remove_sensors(self):
        """Removes all attached sensors. It does *not* send an
        ``interface-changed`` inform; that is left to the caller.
        """
        for sensor_name in self.sensors:
            self.logger.debug("Removing sensor %s", sensor_name)
            del self.sdp_controller.sensors[sensor_name]
        self.sensors = {}

    def _disconnect(self):
        """Clean up when killing the task or when it has died.

        This must be idempotent, because it will be called when the task is
        killed and again when it actually dies.
        """
        need_inform = False
        if self.sensors:
            self._remove_sensors()
            need_inform = True
        if self.katcp_connection is not None:
            try:
                self.katcp_connection.close()
                need_inform = False  # katcp_connection.close() sends an inform itself
            except RuntimeError:
                self.logger.error('Failed to shut down katcp connection to %s', self.name)
            self.katcp_connection = None
        if self.capture_block_state_observer is not None:
            self.capture_block_state_observer.close()
            self.capture_block_state_observer = None
        if self.device_status_observer is not None:
            self.device_status_observer.close()
            self.device_status_observer = None
        if need_inform:
            self.sdp_controller.mass_inform('interface-changed', 'sensor-list')

    def kill(self, driver, **kwargs):
        force = kwargs.pop('force', False)
        if not force:
            asyncio.ensure_future(self.graceful_kill(driver, **kwargs), loop=self.loop)
        else:
            self._disconnect()
            super().kill(driver, **kwargs)

    async def resolve(self, resolver, graph, loop):
        await super().resolve(resolver, graph, loop)

        self.gui_urls = gui_urls = []
        for entry in self.logical_node.gui_urls:
            gui_urls.append({})
            for key, value in entry.items():
                if isinstance(value, str):
                    gui_urls[-1][key] = value.format(self)
                else:
                    gui_urls[-1][key] = value
        if gui_urls:
            gui_urls_sensor = Sensor(str, self.name + '.gui-urls', 'URLs for GUIs')
            gui_urls_sensor.set_value(json.dumps(gui_urls))
            self._add_sensor(gui_urls_sensor)

        for key, value in self.ports.items():
            endpoint_sensor = Sensor(
                aiokatcp.Address,
                '{}.{}'.format(self.name, key), 'IP endpoint for {}'.format(key))
            try:
                addrinfo = await loop.getaddrinfo(self.host, value)
                host, port = addrinfo[0][4][:2]
                endpoint_sensor.set_value(aiokatcp.Address(ipaddress.ip_address(host), port))
            except socket.gaierror as error:
                self.logger.warning('Could not resolve %s: %s', self.host, error)
                endpoint_sensor.set_value(aiokatcp.Address(ipaddress.IPv4Address('0.0.0.0')),
                                          status=Sensor.Status.FAILURE)
            self._add_sensor(endpoint_sensor)
        # Provide info about which container this is for logspout to collect.
        labels = {
            'task': self.logical_node.name,
            'task_id': self.taskinfo.task_id.value,
            'subarray_product_id': self.subarray_product_id
        }
        self.taskinfo.container.docker.setdefault('parameters', []).extend([
            {'key': 'label', 'value': 'za.ac.kat.sdp.katsdpcontroller.{}={}'.format(key, value)}
            for (key, value) in labels.items()])

        # Set extra fields for SDP services to log to logspout
        if self.logical_node.katsdpservices_logging and 'KATSDP_LOG_GELF_ADDRESS' in os.environ:
            extras = dict(labels)
            extras['docker.image'] = self.taskinfo.container.docker.image
            env = {
                'KATSDP_LOG_GELF_ADDRESS': os.environ['KATSDP_LOG_GELF_ADDRESS'],
                'KATSDP_LOG_GELF_EXTRA': json.dumps(extras),
                'KATSDP_LOG_GELF_LOCALNAME': self.host,
                'LOGSPOUT': 'ignore'
            }
            self.taskinfo.command.environment.setdefault('variables', []).extend([
                {'name': key, 'value': value} for (key, value) in env.items()
            ])

        # Apply overrides to taskinfo given by the user
        overrides = resolver.service_overrides.get(self.logical_node.name, {}).get('taskinfo')
        if overrides:
            self.logger.warning('Applying overrides to taskinfo of %s', self.name)
            self.taskinfo = Dict(product_config.override(self.taskinfo.to_dict(), overrides))

        # Add some useful sensors
        self._add_sensor(Sensor(str, self.name + '.version', "Image of executing container.", "",
                                default=self.taskinfo.container.docker.image,
                                initial_status=Sensor.Status.NOMINAL))

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
        return self.logical_node.physical_factory(
            self.logical_node, self.loop, self.sdp_controller, self.subarray_product,
            self.capture_block_id)

    def add_capture_block(self, capture_block):
        self._capture_blocks.add(capture_block.name)
        self._capture_blocks_empty.clear()

    def remove_capture_block(self, capture_block):
        self._capture_blocks.discard(capture_block.name)
        if not self._capture_blocks:
            self._capture_blocks_empty.set()

    async def graceful_kill(self, driver, **kwargs):
        try:
            if self.logical_node.wait_capture_blocks_dead:
                capture_blocks = kwargs.get('capture_blocks', {})
                # Explicitly copy the values because it will mutate
                for capture_block in list(capture_blocks.values()):
                    await capture_block.dead_event.wait()
        except Exception:
            self.logger.exception('Exception in graceful shutdown of %s, killing it', self.name)

        self.logger.info('Waiting for capture blocks on %s', self.name)
        await self._capture_blocks_empty.wait()
        self.logger.info('All capture blocks for %s completed', self.name)
        self._disconnect()
        super().kill(driver, **kwargs)


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


class PhysicalGroup(SDPConfigMixin, scheduler.PhysicalExternal):
    pass


class PoweroffLogicalTask(scheduler.LogicalTask):
    """Logical task for powering off a machine."""
    def __init__(self, host):
        super().__init__('kibisis.' + host)
        self.host = host
        # Use minimal resources, to reduce chance it that it won't fit
        self.cpus = 0.001
        self.mem = 64
        self.image = 'docker-base-runtime'
        self.command = ['/sbin/poweroff']

        # See https://groups.google.com/forum/#!topic/coreos-dev/AXCs_2_J6Mc
        self.taskinfo.container.volumes = []
        for path in ['/var/run/dbus', '/run/systemd']:
            volume = Dict()
            volume.mode = 'RW'
            volume.container_path = path
            volume.host_path = path
            self.taskinfo.container.volumes.append(volume)
        self.taskinfo.container.docker.setdefault('parameters', [])
        self.taskinfo.container.docker.parameters.append({'key': 'user', 'value': 'root'})

    def valid_agent(self, agent):
        if not super().valid_agent(agent):
            return False
        return agent.host == self.host
