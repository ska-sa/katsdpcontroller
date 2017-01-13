from __future__ import print_function, division, absolute_import

import logging
import six
import trollius
from trollius import From, Return
import tornado.concurrent
from tornado import gen
from katcp import Sensor, Message
from katsdptelstate.endpoint import Endpoint
from . import scheduler, sensor_proxy


logger = logging.getLogger(__name__)
PROMETHEUS_SENSORS = ['input_rate','input-rate','dumps','packets_captured','output-rate']
 # list of sensors to be exposed via prometheus
 # some of these will match multiple nodes, which is fine since they get fully qualified names when
 # created as a prometheus metric
 # TODO: harmonise sodding -/_ convention


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
        self.name = logical_task.name + '-' + subarray_name
        self.sdp_controller = sdp_controller
        self.subarray_name = subarray_name
        self.subarray_product_id = subarray_product_id
        self.sensors = {}
         # list of exposed KATCP sensors

    def add_sensor(self, sensor):
        """Add the supplied Sensor object to the top level device and
           track it locally.
        """
        self.sensors[sensor.name] = sensor
        if self.sdp_controller:
            self.sdp_controller.add_sensor(sensor)
        else:
            logger.warning("Attempted to add sensor {} to node {}, but the node has "
                           "no SDP controller available.".format(sensor.name, self.name))

    def remove_sensors(self):
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
            self.remove_sensors()
            self.sdp_controller.mass_inform(Message.inform('interface-changed', 'sensor-list'))

    def kill(self, driver):
        # shutdown this task
        self._disconnect()
        super(SDPPhysicalTaskBase, self).kill(driver)

    def resolve(self, resolver, graph):
        super(SDPPhysicalTaskBase, self).resolve(resolver, graph)
        s_name = "{}.{}.version".format(self.subarray_name, self.logical_node.name)
        version_sensor = Sensor(Sensor.STRING, s_name, "Image of executing container.", "")
        version_sensor.set_value(self.taskinfo.container.docker.image)
        self.add_sensor(version_sensor)
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
            self.remove_sensors()
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
                prefix = '{}.{}.'.format(self.subarray_name, self.logical_node.name)
                self.katcp_connection = sensor_proxy.SensorProxyClient(
                    self.sdp_controller, prefix, PROMETHEUS_SENSORS,
                    host=self.host, port=self.ports['port'])
                try:
                    yield From(to_trollius_future(self.katcp_connection.start(), loop=self.loop))
                    yield From(to_trollius_future(self.katcp_connection.until_synced(timeout=20), loop=self.loop))
                     # some katcp connections, particularly to ingest can take a while to establish
                    return
                except RuntimeError:
                    self.katcp_connection.stop()
                    self.katcp_connection = None
                     # no need for these to lurk around
                    logger.error("Failed to connect to %s via katcp on %s:%d. Check to see if networking issues could be to blame.",
                                 self.name, self.host, self.ports['port'], exc_info=True)
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
