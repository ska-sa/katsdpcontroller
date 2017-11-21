from __future__ import print_function, division, absolute_import

import logging
import json
import contextlib

import six
from addict import Dict

import trollius
from trollius import From, Return
import tornado.concurrent
from tornado import gen
from prometheus_client import Gauge, Counter

from katcp import Sensor, Message
import katcp.core
from katsdptelstate.endpoint import Endpoint

from . import scheduler, sensor_proxy, product_config


def _add_prometheus_sensor(name, description, class_):
    PROMETHEUS_SENSORS[name] = (
        class_('katsdpcontroller_' + name, description, PROMETHEUS_LABELS),
        Gauge('katsdpcontroller_' + name + '_status', 'Status of katcp sensor ' + name, PROMETHEUS_LABELS)
    )


logger = logging.getLogger(__name__)
# Name of edge attribute, as a constant to better catch typos
DEPENDS_INIT = 'depends_init'
PROMETHEUS_LABELS = ('subarray_product_id', 'service')
# Dictionary of sensors to be exposed via Prometheus.
# Some of these will match multiple nodes, which is fine since they get labels
# in Prometheus.
PROMETHEUS_SENSORS = {}
_add_prometheus_sensor('input_rate', 'Current input data rate in Bps', Gauge)
_add_prometheus_sensor('output_rate', 'Current output data rate in Bps', Gauge)
_add_prometheus_sensor('disk_free', 'Disk free on filewriter partition in Bytes', Gauge)
_add_prometheus_sensor('packets_captured', 'Total number of data dumps received', Counter)
_add_prometheus_sensor('dumps', 'Total number of data dumps received', Counter)
_add_prometheus_sensor('input_bytes_total', 'Number of payload bytes received', Counter)
_add_prometheus_sensor('input_heaps_total', 'Number of payload heaps received', Counter)
_add_prometheus_sensor('input_dumps_total', 'Number of payload dumps received', Counter)
_add_prometheus_sensor('output_bytes_total', 'Number of payload bytes sent', Counter)
_add_prometheus_sensor('output_heaps_total', 'Number of payload heaps sent', Counter)
_add_prometheus_sensor('output_dumps_total', 'Number of payload dumps sent', Counter)
_add_prometheus_sensor('last_dump_timestamp',
                       'Timestamp of most recently received dump in Unix seconds', Gauge)

_add_prometheus_sensor('accumulator_batches',
                       'Number of batches completed by the accumulator', Counter)
# No longer used in cal, but kept around for now in case an old version is run
_add_prometheus_sensor('accumulate_buffer_filled',
                       'Fraction of buffer that the current accumulation has written to', Gauge)
_add_prometheus_sensor('slots', 'Total number of buffer slots', Gauge)
_add_prometheus_sensor('accumulator_slots',
                        'Number of buffer slots the current accumulation has written to', Gauge)
_add_prometheus_sensor('free_slots', 'Number of unused buffer slots', Gauge)
_add_prometheus_sensor('pipeline_slots', 'Number of buffer slots in use by the pipeline', Gauge)
_add_prometheus_sensor('accumulator_capture_active', 'Whether an observation is in progress', Gauge)
_add_prometheus_sensor('accumulator_input_heaps', 'Number of L0 heaps received', Counter)
_add_prometheus_sensor('accumulator_last_wait',
                       'Time the accumulator had to wait for a free buffer', Gauge)
_add_prometheus_sensor('accumulator_observations',
                       'Number of observations completed by the accumulator', Counter)
_add_prometheus_sensor('pipeline_last_slots',
                       'Number of slots filled in the most recent buffer', Gauge)
_add_prometheus_sensor('pipeline_last_time',
                       'Time taken to process the most recent buffer', Gauge)
_add_prometheus_sensor('report_last_time',
                       'Elapsed time to generate most recent report', Gauge)
_add_prometheus_sensor('reports_written', 'Number of calibration reports written', Counter)


class State(scheduler.OrderedEnum):
    """State of a subarray. This does not really belong in this module, but it
    is placed here to avoid a circular dependency between :mod:`generator` and
    :mod:`sdpcontroller`.

    Only the following transitions can occur (TODO: make a picture):
    - CONFIGURING -> IDLE (via product-configure)
    - IDLE -> CAPTURING (via capture-init)
    - CAPTURING -> IDLE (via capture-done)
    - CONFIGURING/IDLE/CAPTURING -> DECONFIGURING -> DEAD (via product-deconfigure)
    """
    CONFIGURING = 0
    IDLE = 1
    CAPTURING = 2
    DECONFIGURING = 3
    DEAD = 4


def to_trollius_future(tornado_future, loop=None):
    """Variant of :func:`tornado.platform.asyncio.to_asyncio_future` that
    allows a custom event loop to be specified."""
    tornado_future = gen.convert_yielded(tornado_future)
    af = trollius.Future(loop=loop)
    tornado.concurrent.chain_future(tornado_future, af)
    return af


class SDPLogicalTask(scheduler.LogicalTask):
    def __init__(self, *args, **kwargs):
        super(SDPLogicalTask, self).__init__(*args, **kwargs)
        self.physical_factory = SDPPhysicalTask
        self.transitions = {}
        # List of dictionaries for a .gui-urls sensor. The fields are expanded
        # using str.format(self).
        self.gui_urls = []


class SDPPhysicalTaskBase(scheduler.PhysicalTask):
    """Adds some additional utilities to the parent class for SDP nodes."""
    def __init__(self, logical_task, loop, sdp_controller, subarray_product_id):
        super(SDPPhysicalTaskBase, self).__init__(logical_task, loop)
        self.name = '{}.{}'.format(subarray_product_id, logical_task.name)
        self.sdp_controller = sdp_controller
        self.subarray_product_id = subarray_product_id
        self.sensors = {}
         # list of exposed KATCP sensors

    def _add_sensor(self, sensor):
        """Add the supplied Sensor object to the top level device and
           track it locally.
        """
        self.sensors[sensor.name] = sensor
        if self.sdp_controller:
            self.sdp_controller.add_sensor(sensor)
        else:
            logger.warning("Attempted to add sensor {} to node {}, but the node has "
                           "no SDP controller available.".format(sensor.name, self.name))

    def _remove_sensors(self):
        """Removes all attached sensors. It does *not* send an
        ``interface-changed`` inform; that is left to the caller.
        """
        for sensor_name in self.sensors.iterkeys():
            logger.debug("Removing sensor {}".format(sensor_name))
            self.sdp_controller.remove_sensor(sensor_name)
        self.sensors = {}

    def _disconnect(self):
        """Clean up when killing the task or when it has died.

        This must be idempotent, because it will be called when the task is
        killed and again when it actually dies.
        """
        if self.sensors:
            self._remove_sensors()
            self.sdp_controller.mass_inform(Message.inform('interface-changed', 'sensor-list'))

    def kill(self, driver, **kwargs):
        self._disconnect()
        super(SDPPhysicalTaskBase, self).kill(driver)

    @trollius.coroutine
    def resolve(self, resolver, graph, loop):
        yield From(super(SDPPhysicalTaskBase, self).resolve(resolver, graph, loop))

        gui_urls = []
        for entry in self.logical_node.gui_urls:
            gui_urls.append({})
            for key, value in six.iteritems(entry):
                if isinstance(value, six.string_types):
                    gui_urls[-1][key] = value.format(self)
                else:
                    gui_urls[-1][key] = value
        if gui_urls:
            gui_urls_sensor = Sensor.string(self.name + '.gui-urls', 'URLs for GUIs')
            gui_urls_sensor.set_value(json.dumps(gui_urls))
            self._add_sensor(gui_urls_sensor)

        for key, value in six.iteritems(self.ports):
            endpoint_sensor = Sensor.address(
                '{}.{}'.format(self.name, key), 'IP endpoint for {}'.format(key))
            endpoint_sensor.set_value((self.host, value))
            self._add_sensor(endpoint_sensor)
        # Provide info about which container this is for logspout to collect
        self.taskinfo.container.docker.setdefault('parameters', []).extend([
            {'key': 'label',
             'value': 'za.ac.kat.sdp.katsdpcontroller.task={}'.format(self.logical_node.name)},
            {'key': 'label',
             'value': 'za.ac.kat.sdp.katsdpcontroller.task_id={}'.format(self.taskinfo.task_id.value)},
            {'key': 'label',
             'value': 'za.ac.kat.sdp.katsdpcontroller.subarray_product_id={}'.format(self.subarray_product_id)}
        ])
        # Request SDP services to escape newlines, for the benefit of
        # logstash.
        self.taskinfo.command.environment.setdefault('variables', []).append(
            {'name': 'KATSDP_LOG_ONELINE', 'value': '1'})

        # Apply overrides to taskinfo given by the user
        overrides = resolver.service_overrides.get(self.logical_node.name, {}).get('taskinfo')
        if overrides:
            logger.warn('Applying overrides to taskinfo of %s', self.name)
            self.taskinfo = Dict(product_config.override(self.taskinfo.to_dict(), overrides))

        # Add some useful sensors
        version_sensor = Sensor.string(self.name + '.version', "Image of executing container.", "")
        version_sensor.set_value(self.taskinfo.container.docker.image)
        self._add_sensor(version_sensor)

    def set_state(self, state):
        # TODO: extend this to set a sensor indicating the task state
        super(SDPPhysicalTaskBase, self).set_state(state)
        if self.state == scheduler.TaskState.DEAD:
            self._disconnect()


class SDPConfigMixin(object):
    """Mixin class that takes config information from the graph and sets it in telstate."""
    @trollius.coroutine
    def resolve(self, resolver, graph, loop):
        yield From(super(SDPConfigMixin, self).resolve(resolver, graph, loop))
        config = graph.node[self].get('config', lambda task_, resolver_: {})(self, resolver)
        for name, value in six.iteritems(self.ports):
            config[name] = value
        for src, trg, attr in graph.out_edges(self, data=True):
            endpoint = None
            if 'port' in attr and trg.state >= scheduler.TaskState.STARTING:
                port = attr['port']
                endpoint = Endpoint(trg.host, trg.ports[port])
            config.update(attr.get('config', lambda task_, resolver_, endpoint_: {})(
                self, resolver, endpoint))
        overrides = resolver.service_overrides.get(self.logical_node.name, {}).get('config')
        if overrides:
            logger.warning('Overriding config for %s', self.name)
            config = product_config.override(config, overrides)
        logger.debug('Config for %s: %s', self.name, config)
        resolver.telstate.add('config.' + self.logical_node.name, config, immutable=True)


class ProgramBlockStateObserver(object):
    """Watches a program-block-state sensor in a child."""
    def __init__(self, sensor, loop):
        self._ready = trollius.Event(loop=loop)
        self._sensor = sensor
        self.update(sensor, sensor.read())
        sensor.attach(self)

    def update(self, sensor, reading):
        if reading.status in [Sensor.NOMINAL, Sensor.WARN, Sensor.ERROR]:
            try:
                value = json.loads(reading.value)
            except ValueError:
                logger.warning('Invalid JSON in %s: %r', sensor.name, reading.value)
            else:
                if not isinstance(value, dict):
                    logger.warning('%s is not a dict: %r', sensor.name, reading.value)
                else:
                    logger.info('%s has %d remaining program blocks', sensor.name, len(value))
                    if not value:
                        self._ready.set()

    @trollius.coroutine
    def wait_empty(self):
        yield From(self._ready.wait())

    def close(self):
        self._sensor.detach(self)


class SDPPhysicalTask(SDPConfigMixin, SDPPhysicalTaskBase):
    """Augments the base :class:`~scheduler.PhysicalTask` to handle katcp and
    telstate.

    Also responsible for managing any available KATCP sensors of the controlled
    object. Such sensors are exposed at the master controller level using the
    following syntax:
      sdp_<subarray_product_id>.<node_type>.<node_index>.<sensor_name>
    For example:
      sdp.array_1.ingest.1.input_rate
    """
    def __init__(self, logical_task, loop, sdp_controller, subarray_product_id):
        super(SDPPhysicalTask, self).__init__(logical_task, loop, sdp_controller, subarray_product_id)
        self.katcp_connection = None

    def get_transition(self, old_state, new_state):
        """If this node has a specified state transition action, return it"""
        return self.logical_node.transitions.get((old_state, new_state), None)

    def _disconnect(self):
        """Close the katcp connection (if open) and remove the sensors."""
        # Note: specifically does not call super's _disconnect, because that
        # could lead to extra interface-changed informs.
        need_inform = False
        if self.sensors:
            self._remove_sensors()
            need_inform = True
        if self.katcp_connection is not None:
            try:
                self.katcp_connection.stop()
                need_inform = False  # katcp_connection.stop() sends an inform itself
            except RuntimeError:
                logger.error('Failed to shut down katcp connection to %s', self.name)
            self.katcp_connection = None
        if need_inform:
            self.sdp_controller.mass_inform(Message.inform('interface-changed', 'sensor-list'))

    @trollius.coroutine
    def wait_ready(self):
        yield From(super(SDPPhysicalTask, self).wait_ready())
        # establish katcp connection to this node if appropriate
        if 'port' in self.ports:
            while True:
                logger.info("Attempting to establish katcp connection to {}:{} for node {}".format(
                    self.host, self.ports['port'], self.name))
                prefix = self.name + '.'
                labels = (self.subarray_product_id, self.logical_node.name)
                self.katcp_connection = sensor_proxy.SensorProxyClient(
                    self.sdp_controller, prefix,
                    PROMETHEUS_SENSORS, labels,
                    host=self.host, port=self.ports['port'])
                try:
                    yield From(to_trollius_future(self.katcp_connection.start(), loop=self.loop))
                    # The design of wait_ready is that it shouldn't time out,
                    # instead relying on higher-level timeouts to decide
                    # whether to cancel it. We use a timeout here, together
                    # with the while True loop, rather than letting
                    # until_synced run forever, because Tornado futures have no
                    # concept of cancellation. If wait_ready is cancelled, then
                    # it will stop the connection immediately, but the Tornado
                    # future will hang around until it times out.
                    #
                    # Timing out also allows recovery if the TCP connection
                    # somehow gets wedged badly enough that katcp can't recover
                    # itself.
                    yield From(to_trollius_future(self.katcp_connection.until_synced(timeout=20), loop=self.loop))
                    logging.info("Connected to {}:{} for node {}".format(
                        self.host, self.ports['port'], self.name))
                    return
                except RuntimeError:
                    self.katcp_connection.stop()
                    self.katcp_connection = None
                     # no need for these to lurk around
                    logger.error("Failed to connect to %s via katcp on %s:%d. Check to see if networking issues could be to blame.",
                                 self.name, self.host, self.ports['port'], exc_info=True)
                    # Sleep for a bit to avoid hammering the port if there
                    # is a quick failure, before trying again.
                    yield From(trollius.sleep(1.0, loop=self.loop))

    @trollius.coroutine
    def issue_req(self, req, args=[], **kwargs):
        """Issue a request to the katcp connection.

        The reply and informs are returned. If the request failed, a log
        message is printed.
        """
        if self.katcp_connection is None:
            raise ValueError('Cannot issue request without a katcp connection')
        logger.info("Issued request {} {} to node {}".format(req, args, self.name))
        reply, informs = yield From(to_trollius_future(
            self.katcp_connection.katcp_client.future_request(Message.request(req, *args), **kwargs),
            loop=self.loop))
        if not reply.reply_ok():
            msg = "Failed to issue req {} to node {}. {}".format(req, self.name, reply.arguments[-1])
            logger.warning(msg)
        raise Return((reply, informs))

    @trollius.coroutine
    def graceful_kill(self, driver, **kwargs):
        force_kill = True
        try:
            if self.katcp_connection is not None:
                sensor = yield From(to_trollius_future(
                    self.katcp_connection.future_get_sensor('program-block-state')))
                if sensor is not None:
                    observer = ProgramBlockStateObserver(sensor, self.loop)
                    with contextlib.closing(observer):
                        yield From(observer.wait_empty())
        except Exception:
            logger.exception('Exception in graceful shutdown of %s, killing it', self.name)
        super(SDPPhysicalTask, self).kill(driver, **kwargs)

    def kill(self, driver, **kwargs):
        force = kwargs.pop('force', False)
        if not force:
            trollius.ensure_future(self.graceful_kill(driver, **kwargs), loop=self.loop)
        else:
            super(SDPPhysicalTask, self).kill(driver, **kwargs)


class LogicalGroup(scheduler.LogicalExternal):
    """Dummy node that presents a set of related real nodes.

    This allows the graph to contain a single edge dependency to this node
    instead of one to each of the real nodes. It also allows for shared config
    to be stored once rather than repeated.
    """
    def __init__(self, name):
        super(LogicalGroup, self).__init__(name)
        self.physical_factory = PhysicalGroup


class PhysicalGroup(SDPConfigMixin, scheduler.PhysicalExternal):
    pass


class PoweroffLogicalTask(scheduler.LogicalTask):
    """Logical task for powering off a machine."""
    def __init__(self, host):
        super(PoweroffLogicalTask, self).__init__('kibisis')
        self.host = host
        # Use minimal resources, to reduce chance it that it won't fit
        self.cpus = 0.001
        self.mem = 64
        self.image = 'docker-base'
        self.command = ['/sbin/poweroff']

        # See https://groups.google.com/forum/#!topic/coreos-dev/AXCs_2_J6Mc
        self.container.volumes = []
        for path in ['/var/run/dbus', '/run/systemd']:
            volume = Dict()
            volume.mode = 'RW'
            volume.container_path = path
            volume.host_path = path
            self.container.volumes.append(volume)
        self.container.docker.parameters = []
        self.container.docker.parameters.append({'key': 'user', 'value': 'root'})

    def valid_agent(self, agent):
        if not super(PoweroffLogicalTask, self).valid_agent(agent):
            return False
        return agent.host == self.host
