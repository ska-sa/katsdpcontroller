from __future__ import print_function, division, absolute_import

import logging
import six
import trollius
from trollius import From, Return
import tornado.concurrent
from tornado import gen
from addict import Dict
from katcp import Sensor, Message
from prometheus_client import Gauge, Counter
from katsdptelstate.endpoint import Endpoint
from . import scheduler, sensor_proxy


def _add_prometheus_sensor(name, description, class_):
    PROMETHEUS_SENSORS[name] = (
        class_('katsdpcontroller_' + name, description, PROMETHEUS_LABELS),
        Gauge('katsdpcontroller_' + name + '_status', 'Status of katcp sensor ' + name, PROMETHEUS_LABELS)
    )


logger = logging.getLogger(__name__)
PROMETHEUS_LABELS = ('subarray', 'service')
# Dictionary of sensors to be exposed via Prometheus.
# Some of these will match multiple nodes, which is fine since they get labels
# in Prometheus.
PROMETHEUS_SENSORS = {}
_add_prometheus_sensor('input_rate', 'Current input data rate in Bps', Gauge)
_add_prometheus_sensor('output_rate', 'Current output data rate in Bps', Gauge)
_add_prometheus_sensor('packets_captured', 'Total number of data dumps received', Counter)
_add_prometheus_sensor('dumps', 'Total number of data dumps received', Counter)


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
        self.transitions = {}


class SDPPhysicalTaskBase(scheduler.PhysicalTask):
    """Adds some additional utilities to the parent class for SDP nodes."""
    def __init__(self, logical_task, loop, sdp_controller, subarray_name, subarray_product_id):
        super(SDPPhysicalTaskBase, self).__init__(logical_task, loop)
        self.name = '{}.{}'.format(subarray_name, logical_task.name)
        self.sdp_controller = sdp_controller
        self.subarray_name = subarray_name
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

    def kill(self, driver):
        # shutdown this task
        self._disconnect()
        super(SDPPhysicalTaskBase, self).kill(driver)

    def resolve(self, resolver, graph):
        super(SDPPhysicalTaskBase, self).resolve(resolver, graph)
        s_name = "{}.version".format(self.name)
        version_sensor = Sensor(Sensor.STRING, s_name, "Image of executing container.", "")
        version_sensor.set_value(self.taskinfo.container.docker.image)
        self._add_sensor(version_sensor)
        # Provide info about which container this is for logspout to collect
        self.taskinfo.container.docker.setdefault('parameters', []).extend([
            {'key': 'label',
             'value': 'za.ac.kat.sdp.katsdpcontroller.task={}'.format(self.logical_node.name)},
            {'key': 'label',
             'value': 'za.ac.kat.sdp.katsdpcontroller.task_id={}'.format(self.taskinfo.task_id.value)},
            {'key': 'label',
             'value': 'za.ac.kat.sdp.katsdpcontroller.subarray_name={}'.format(self.subarray_name)},
            {'key': 'label',
             'value': 'za.ac.kat.sdp.katsdpcontroller.subarray_product_id={}'.format(self.subarray_product_id)}
        ])

    def set_state(self, state):
        # TODO: extend this to set a sensor indicating the task state
        super(SDPPhysicalTaskBase, self).set_state(state)
        if self.state == scheduler.TaskState.DEAD:
            self._disconnect()


class SDPPhysicalTask(SDPPhysicalTaskBase):
    """Augments the base :class:`~scheduler.PhysicalTask` to handle katcp and
    telstate.

    Also responsible for managing any available KATCP sensors of the controlled
    object. Such sensors are exposed at the master controller level using the
    following syntax:
      sdp_<subarray_name>.<node_type>.<node_index>.<sensor_name>
    For example:
      sdp.array_1.ingest.1.input_rate
    """
    def __init__(self, logical_task, loop, sdp_controller, subarray_name, subarray_product_id):
        super(SDPPhysicalTask, self).__init__(logical_task, loop, sdp_controller, subarray_name, subarray_product_id)
        self.katcp_connection = None

    def get_transition(self, state):
         # if this node has a specified state transition action return it
        return self.logical_node.transitions.get(state, None)

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
                labels = (self.subarray_name, self.logical_node.name)
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
                    # concept of cancellation. If wait_ready is called, then it will
                    # stop the connection immediately, but the Tornado future
                    # will hang around until it times out.
                    #
                    # Timing out also allows recovery if the TCP connection
                    # some gets wedged badly enough that katcp can't recover
                    # itself.
                    yield From(to_trollius_future(self.katcp_connection.until_synced(timeout=20), loop=self.loop))
                     # some katcp connections, particularly to ingest can take a while to establish
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

    def resolve(self, resolver, graph):
        super(SDPPhysicalTask, self).resolve(resolver, graph)
        config = graph.node[self].get('config', lambda resolver_: {})(resolver)
        for r in scheduler.RANGE_RESOURCES:
            for name, value in six.iteritems(getattr(self, r)):
                config[name] = value
        for src, trg, attr in graph.out_edges_iter(self, data=True):
            endpoint = None
            if 'port' in attr and trg.state >= scheduler.TaskState.STARTING:
                port = attr['port']
                endpoint = Endpoint(trg.host, trg.ports[port])
            config.update(attr.get('config', lambda resolver_, endpoint_: {})(resolver, endpoint))
        logger.debug('Config for {}: {}'.format(self.name, config))
        resolver.telstate.add('config.' + self.logical_node.name, config, immutable=True)

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
