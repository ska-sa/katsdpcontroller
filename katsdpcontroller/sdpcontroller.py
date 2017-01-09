"""Core classes for the SDP Controller.

"""

import os
import time
import uuid
import logging
import subprocess
import shlex
import json
import signal
import socket
import re

import concurrent.futures
from tornado import gen
import tornado.platform.asyncio
from tornado.platform.asyncio import to_asyncio_future
import tornado.ioloop
import tornado.concurrent
import trollius
from trollius import From, Return
import pymesos
import networkx
import six

import ipaddress
import faulthandler

from katcp import AsyncDeviceServer, Sensor, Message, AsyncClient, AsyncReply
from katcp.kattypes import request, return_reply, Str, Int, Float
import katsdpgraphs.generator
from . import scheduler

from prometheus_client import Gauge

try:
    import requests
    requests.packages.urllib3.disable_warnings()
     # disable HTTPS security warnings
except ImportError:
    pass
     # docker is not needed when running in interface only mode.
     # when not running in this mode we check to make sure that
     # docker has been imported or we exit.

try:
    import katsdptelstate
    from katsdptelstate.endpoint import Endpoint
except ImportError:
    katsdptelstate = None
     # katsdptelstate is not needed when running in interface only mode.
     # when not running in this mode we check to make sure that
     # katsdptelstate has been imported or we exit.

faulthandler.register(signal.SIGUSR2, all_threads=True)


class State(scheduler.OrderedEnum):
    UNCONFIGURED = 0
    IDLE = 1
    INIT_WAIT = 2
    CAPTURING = 3
    CAPTURE_COMPLETE = 4
    DONE = 5

TASK_STATES = {0:'init',1:'running',2:'killed'}
PROMETHEUS_SENSORS = ['input_rate','input-rate','dumps','packets_captured','output-rate']
 # list of sensors to be exposed via prometheus
 # some of these will match multiple nodes, which is fine since they get fully qualified names when
 # created as a prometheus metric
 # TODO: harmonise sodding -/_ convention

logger = logging.getLogger("katsdpcontroller.katsdpcontroller")


def to_tornado_future(trollius_future):
    """Wrapper around :func:`tornado.platform.asyncio.to_tornado_future` that
    is a bit more robust: it allows taking a coroutine rather than a future,
    it passes through error tracebacks, and if a future is cancelled it
    properly propagates the CancelledError.
    """
    f = trollius.ensure_future(trollius_future)
    tf = tornado.concurrent.Future()
    def copy(future):
        assert future is f
        if f.cancelled():
            tf.set_exception(trollius.CancelledError())
        elif hasattr(f, '_get_exception_tb') and f._get_exception_tb() is not None:
            # Note: f.exception() clears the traceback, so must retrieve it first
            tb = f._get_exception_tb()
            exc = f.exception()
            tf.set_exc_info((type(exc), exc, tb))
        elif f.exception() is not None:
            tf.set_exception(f.exception())
        else:
            tf.set_result(f.result())
    f.add_done_callback(copy)
    return tf


class CallbackSensor(Sensor):
    """KATCP Sensor that uses a callback to obtain the next sensor value."""
    def __init__(self, *args, **kwargs):
        self._read_callback = None
        self._read_callback_kwargs = {}
        self._busy_updating = False
        super(CallbackSensor, self).__init__(*args, **kwargs)

    def set_read_callback(self, callback, **kwargs):
        self._read_callback = callback
        self._read_callback_kwargs = kwargs

    def read(self):
        """Provide a callback function that is executed when read is called.
        Callback should provide a (timestamp, status, value) tuple

        Returns
        -------
        reading : :class:`katcp.core.Reading` object
            Sensor reading as a (timestamp, status, value) tuple

        """
        if self._read_callback and not self._busy_updating:
            self._busy_updating = True
            (_val, _status, _timestamp) = self._read_callback(**self._read_callback_kwargs)
            self.set_value(self.parse_value(_val), _status, _timestamp)
        # Having this outside the above if-statement ensures that we cannot get
        # stuck with busy_updating=True if read callback crashes in one thread
        self._busy_updating = False
        return (self._timestamp, self._status, self._value)

class GraphResolver(object):
    """Provides graph name resolution services for use when presented with
       a subarray product id.

       A subarray product id has the form <subarray_name>_<data_product_name>
       and in general the graph name will be the same as the data_product_name.

       This resolver class allows specified subarray product ids to be mapped
       to a user specified graph.

       Parameters
       ----------
       overrides : list, optional
            A list of override strings in the form <subarray_product_id>:<override_graph_name>
       simulate : bool, optional
            Resolver will product graph name suitable for use in simulation when set to true. (default: False)
       """
    def __init__(self, overrides=[], simulate=False):
        self._overrides = {}
        self.simulate = simulate

        for override in overrides:
            fields = override.split(':', 1)
            if len(fields) < 2:
                logger.warning("Ignoring graph resolver override {} as it does not conform to \
                                the required <subarray_product_id>:<graph_name>".format(override))
            self._overrides[fields[0]] = fields[1]
            logger.info("Registering graph override {} => {}".format(fields[0], fields[1]))

    def __call__(self, subarray_product_id):
        """Returns a full qualified graph name from the specified subarray product id.
           e.g. array_1_c856M4k => c856M4k
        """
        try:
            base_graph_name = self._overrides[subarray_product_id]
            logger.warning("Graph name specified by subarray_product_id ({}) has been overriden to {}".format(subarray_product_id, base_graph_name))
             # if an override is set use this instead, but warn the user about this
        except KeyError:
            base_graph_name = subarray_product_id.split("_")[-1]
             # default graph name is to split out the trailing name from the subarray product id specifier
        return "{}{}".format(base_graph_name, "sim" if self.simulate else "")

    def get_subarray_numeric_id(self, subarray_product_id):
        """Returns the numeric subarray identifier string from the specified subarray product id.
           e.g. array_1_c856M4k => 1
        """
        matched_group = re.match(r'^\w+\_(\d+)\_[a-zA-Z0-9]+$',subarray_product_id)
         # should match the last underscore delimited group of digits
        if matched_group: return int(matched_group.group(1))
        return None


class MulticastIPResources(object):
    def __init__(self, network):
        self._network = network
        self._hosts = network.hosts()
        self._allocated = {}      # Contains strings, not IPv4Address objects

    def _new_ip(self, host_class):
        try:
            ip = str(next(self._hosts))
            self._allocated[host_class] = ip
            return ip
        except StopIteration:
            raise RuntimeError('Multicast IP addresses exhausted')

    def set_ip(self, host_class, ip):
        self._allocated[host_class] = ip

    def get_ip(self, host_class):
        ip = self._allocated.get(host_class)
        if ip is None:
            ip = self._new_ip(host_class)
        return ip


class SDPCommonResources(object):
    """Assigns multicast groups and ports across all subarrays."""
    def __init__(self, safe_multicast_cidr, safe_port_range=range(30000,31000)):
        self.safe_ports = safe_port_range
        logger.info("Using {} for multicast subnet allocation".format(safe_multicast_cidr))
        multicast_subnets = ipaddress.ip_network(unicode(safe_multicast_cidr)).subnets(new_prefix=24)
        self.multicast_resources = {}
        self.multicast_resources_fallback = MulticastIPResources(next(multicast_subnets))
        for name in ['l0_spectral_spead',
                     'l0_continuum_spead',
                     'l1_spectral_spead',
                     'l1_continuum_spead']:
            self.multicast_resources[name] = MulticastIPResources(next(multicast_subnets))
        self.allocated_ports = {}

    def new_port(self, group):
        port = self.safe_ports.pop()
        self.allocated_ports[group] = port
        return port


class SDPResources(object):
    """Helper class to allocate resources for a single subarray-product."""
    def __init__(self, common, subarray_product_id):
        self.subarray_product_id = subarray_product_id
        self._common = common
        self._urls = {}

    def _qualify(self, name):
        return "{}_{}".format(self.subarray_product_id, name)

    def set_multicast_ip(self, group, ip):
        """"Override system-generated multicast IP address with specified one"""
        mr = self._common.multicast_resources.get(group, self._common.multicast_resources_fallback)
        mr.set_ip(self._qualify(group), ip)

    def get_multicast_ip(self, group):
        """For the specified host class, return an available / assigned multicast address"""
        mr = self._common.multicast_resources.get(group, self._common.multicast_resources_fallback)
        return mr.get_ip(self._qualify(group))

    def set_port(self, group, port):
        """override system-generated port with the specified one"""
        self._common.allocated_ports[self._qualify(group)] = port

    def get_port(self, group):
        """Return an assigned port for a multicast group"""
        group = self._qualify(group)
        port = self._common.allocated_ports.get(group, None)
        if port is None: port = self._common.new_port(group)
        return port

    def get_url(self, service):
        return self._urls.get(service)

    def set_url(self, service, url):
        self._urls[service] = url

    def get_multicast(self, group):
        """For the specified multicast group, return a multicast endpoint in
        the form ``ipaddress:port``"""
        return '{}:{}'.format(self.get_multicast_ip(group), self.get_port(group))


class SDPLogicalTask(scheduler.LogicalTask):
    def __init__(self, *args, **kwargs):
        super(SDPLogicalTask, self).__init__(*args, **kwargs)
        self.transitions = {}


class SDPPhysicalTask(scheduler.PhysicalTask):
    """Augments the base :class:`~scheduler.PhysicalTask` to handle katcp.

    Also responsible for managing any available KATCP sensors of the controlled
    object. Such sensors are exposed at the master controller level using the
    following syntax:
      sdp_<subarray_name>.<node_type>.<node_index>.<sensor_name>
    For example:
      sdp.array_1.ingest.1.input_rate
    """
    def __init__(self, logical_task, loop, sdp_controller, subarray_name, subarray_product_id):
        super(SDPPhysicalTask, self).__init__(logical_task, loop)
        self.name = logical_task.name + '-' + subarray_name
        self.katcp_connection = None
        self.sdp_controller = sdp_controller
        self.subarray_name = subarray_name
        self.subarray_product_id = subarray_product_id
        self.sensors = {}
         # list of exposed KATCP sensors

    def get_transition(self, state):
         # if this node has a specified state transition action return it
        return self.logical_node.transitions.get(state,None)

    def kill(self, driver):
        # shutdown this task
        for sensor_name in self.sensors.iterkeys():
            logger.debug("Removing sensor {}".format(sensor_name))
            self.sdp_controller.remove_sensor(sensor_name)
        if self.katcp_connection is not None:
            try:
                self.katcp_connection.stop()
            except RuntimeError:
                pass # best effort
            self.katcp_connection = None
        super(SDPPhysicalTask, self).kill(driver)

    def _chained_read(self, base_name='', prometheus_gauge=None):
         # used to chain a sensor in the master controller
         # to the actual sensor on the subordinate device
        if self.katcp_connection:
            # TODO: this will block up the ioloop any time a client requests a
            # sensor. Unfortunately there is no way to avoid this without
            # modifying katcp, because the Sensor API doesn't have an
            # asynchronous way to read a sensor.
            reply, informs = self.katcp_connection.blocking_request(Message.request('sensor-value',base_name),timeout=5)
            if not reply.reply_ok():
                return None
            if len(informs) < 1: return None
            args = informs[0].arguments
             # sensor value inform has format: <ts>,<not sure>,<name>,<state>,<value>
             # all strings
            logger.debug("Chained read on sensor {}: {}".format(base_name,args))
            try:
                s_ts = float(args[0])
            except ValueError:
                s_ts = time.time()
            try:
                s_state = Sensor.STATUS_NAMES[args[3]]
            except KeyError:
                s_state = Sensor.UNKNOWN
            s_value = args[4]
            if prometheus_gauge:
                try:
                    prometheus_gauge.set(s_value)
                except ValueError:
                    logger.warning("Failed to set prometheus gauge {} to {}".format(base_name, s_value))
            logger.debug("Returning ({},{},{})".format(s_value, s_state, s_ts))
            return (s_value, s_state, s_ts)
        else:
            return ('',Sensor.UNREACHABLE,time.time())

    def add_sensor(self, sensor):
        """Add the supplied Sensor object to the top level device and
           track it locally.
        """
        # TODO: reenable this code. It doesn't work because there is no way to have
        # an asynchronous sensor read callback.
        return
        self.sensors[sensor.name] = sensor
        if self.sdp_controller:
            self.sdp_controller.add_sensor(sensor)
        else:
            logger.warning("Attempted to add sensor {} to node {}, but the node has no SDP controller available.".format(sensor.name,self.name))

    @trollius.coroutine
    def wait_ready(self):
        yield From(super(SDPPhysicalTask, self).wait_ready())
        # establish katcp connection to this node if appropriate
        if 'port' in self.ports:
            while True:
                logger.info("Attempting to establish katcp connection to {}:{} for node {}".format(
                    self.host, self.ports['port'], self.name))
                self.katcp_connection = AsyncClient(self.host, self.ports['port'])
                try:
                    self.katcp_connection.set_ioloop(tornado.ioloop.IOLoop.current())
                    self.katcp_connection.start()
                    yield From(to_asyncio_future(self.katcp_connection.until_connected(timeout=20)))
                     # some katcp connections, particularly to ingest can take a while to establish
                    reply, informs = yield From(to_asyncio_future(
                        self.katcp_connection.future_request(Message.request('sensor-list'), timeout=5)))
                     # get available sensors
                     # each inform as format: <name>,<description>,<units>,<type>[,<params>]
                    for i in informs:
                        args = i.arguments
                        s_name = "{}.{}.{}".format(self.subarray_name,self.name,args[0])
                        s_description = args[1]
                        s_units = args[2]
                        s_type = Sensor.parse_type(args[3])
                        params = args[4:]
                         # all trailing arguments (if any) are params
                        try:
                            if s_type == Sensor.DISCRETE:
                                s = CallbackSensor(s_type, s_name, s_description, s_units, params=params)
                            else:
                                s = CallbackSensor(s_type, s_name, s_description, s_units)
                                 # leaving out params for now as these are specific to each type of
                                 # sensor and thus will need conversion out of their string form
                                 # before use. They shouldn't really matter for this application.
                                 # NOTE: Not true for discretes, hence the kludge above
                            prometheus_gauge = None
                            if args[0] in PROMETHEUS_SENSORS:
                                logger.info("Exposing sensor {} as a prometheus metric.".format(s_name))
                                prom_name = '{}_{}'.format(self.name, args[0]).replace(".","_").replace("-","_")
                                try:
                                    prometheus_gauge = Gauge(prom_name, s_description)
                                except ValueError as e:
                                    logger.info("Prometheus Gauge {} already exists - not adding again. ({})".format(prom_name, e))
                                     # set to info as this only happens when the gauge already exists, and this only
                                     # happens during a reconfigure.
                            s.set_read_callback(self._chained_read, base_name=args[0], prometheus_gauge=prometheus_gauge)
                            self.add_sensor(s)
                            if self.logical_node.name == 'sdp.ingest.1' and args[0] == 'status':
                                s.name = "data-product-status-{}".format(self.subarray_product_id)
                                logger.info("Adding fake status sensor {}".format(s.name))
                                self.add_sensor(s)
                                 # unfortunately it is too hard to change a baselined ICD
                                 # so we need to add a fake sensor to mirror the ingest status
                                 # of a particular graph into a top level sensor
                            logger.info("Added sensor {} of type {} for node {} (params: {})".format(s_name, args[3], self.name, params))
                             # probably back off to DEBUG once stable
                        except KeyError:
                            logger.error("Failed to add sensor {} of type {} for node {}".format(s_name, args[3], self.name))
                        except IndexError:
                            logger.info("Failed to add sensor {} of type {} for node {} (args: {}) due to internal sensor init failure".format(s_name, args[3], self.name, args))
                    for s in self.sensors.itervalues(): s.read()
                     # refresh each added sensor so we have up to date values
                    return
                except RuntimeError:
                    self.katcp_connection.stop()
                    self.katcp_connection = None
                     # no need for these to lurk around
                    logger.error("Failed to connect to %s via katcp on %s:%d. Check to see if networking issues could be to blame.",
                                 self.name, self.host, self.data['port'])
                    yield From(trollius.sleep(1.0))

    def resolve(self, resolver, graph):
        super(SDPPhysicalTask, self).resolve(resolver, graph)
        s_name = "{}.{}.version".format(self.subarray_name, self.logical_node.name)
        version_sensor = Sensor(Sensor.STRING, s_name, "Image of executing container.", "")
        version_sensor.set_value(self.taskinfo.container.docker.image)
        self.add_sensor(version_sensor)
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

    def set_state(self, state):
        # TODO: extend this to set a sensor indicating the task state
        super(SDPPhysicalTask, self).set_state(state)

    @trollius.coroutine
    def issue_req(self, req, args=[], **kwargs):
        """Issue a request to the katcp connection.

        The reply and informs are returned. If the request failed, a log
        message is printed.
        """
        if self.katcp_connection is None:
            raise ValueError('Cannot issue request without a katcp connection')
        logger.info("Issued request {} {} to node {}".format(req, args, self.name))
        reply, informs = yield From(to_asyncio_future(
            self.katcp_connection.future_request(Message.request(req, *args), **kwargs)))
        if not reply.reply_ok():
            msg = "Failed to issue req {} to node {}. {}".format(req, self.name, reply.arguments[-1])
            logger.warning(msg)
        raise Return((reply, informs))


class SDPGraph(object):
    def _instantiate(self, logical_node):
        if isinstance(logical_node, SDPLogicalTask):
            return logical_node.physical_factory(
                logical_node, self.loop,
                self.sdp_controller, self.subarray_name, self.subarray_product_id)
        else:
            return logical_node.physical_factory(logical_node, self.loop)

    """Wrapper around a physical graph used to instantiate
    a particular SDP product/capability/subarray."""
    def __init__(self, sched, graph_name, resolver, subarray_product_id, loop, sdp_controller=None, telstate_name='sdp.telstate'):
        self.sched = sched
        self.resolver = resolver
        self.loop = loop
        self.subarray_product_id = subarray_product_id
        name_parts = subarray_product_id.split("_")
         # expect subarray product name to be of form [<subarray_name>_]<data_product_name>
        self.subarray_name = name_parts[:-1] and "_".join(name_parts[:-1]) or "unknown"
         # make sure we have some subarray name even if not specified
        self.sdp_controller = sdp_controller
        kwargs = katsdpgraphs.generator.graph_parameters(graph_name)
        self.logical_graph = katsdpgraphs.generator.build_logical_graph(**kwargs)
        resolver.image_resolver.reread_tag_file()
         # pick up any updates to the tag file
        # generate physical nodes
        mapping = {logical: self._instantiate(logical) for logical in self.logical_graph}
        self.physical_graph = networkx.relabel_nodes(self.logical_graph, mapping)
        # Nodes indexed by logical name
        self._nodes = {node.logical_node.name: node for node in self.physical_graph}
        self.telstate_node = self._nodes[telstate_name]
        self.telstate_endpoint = ""
        self.telstate = None

    def get_json(self):
        from networkx.readwrite import json_graph
        return json_graph.node_link_data(self.physical_graph)

    @trollius.coroutine
    def launch_telstate(self, additional_config={}, base_params={}):
        """Make sure the telstate node is launched"""
        boot = [node for node in self.physical_graph if not isinstance(node, scheduler.PhysicalTask)]
        boot.append(self.telstate_node)
        yield From(self.sched.launch(self.physical_graph, self.resolver, boot))

        logger.debug("Launching telstate. Base parameters {}".format(base_params))
        graph_base_params = self.physical_graph.graph.get(
            'config', lambda resolver: {})(self.resolver)
        base_params.update(graph_base_params)
         # encode metadata into the telescope state for use
         # in component configuration
         # connect to telstate store
        self.telstate_endpoint = '{}:{}'.format(self.telstate_node.host,
                                                self.telstate_node.ports['telstate'])
        self.telstate = katsdptelstate.TelescopeState(endpoint=self.telstate_endpoint)
        self.resolver.telstate = self.telstate

        logger.debug("global config: %s", additional_config)
        logger.debug("base params: %s", base_params)
        self.telstate.add('config', additional_config, immutable=True)
         # set the configuration
        for k,v in base_params.iteritems():
            self.telstate.add(k,v, immutable=True)

    @trollius.coroutine
    def execute_graph(self, req):
        """Launch the remainder of the graph after :meth:`launch_telstate` has completed."""
        try:
            yield From(self.sched.launch(self.physical_graph, self.resolver))
        except Exception:
            # Interrupting a launch can leave a graph half-configured. Make
            # sure it is killed off completely.
            yield From(self.sched.kill(self.physical_graph))
            raise
        # TODO: issue progress reports as tasks start running

    @trollius.coroutine
    def shutdown(self):
        yield From(self.sched.kill(self.physical_graph))

    def check_nodes(self):
         # check if all requested nodes are actually running
         # TODO: Health state sensors should be controlled from here
        for node in self.physical_graph:
            if node.state != scheduler.TaskState.READY:
                logger.warn('Task %s is in state %s instead of READY', node.name, node.state.name)
                return False
        return True


class SDPTask(object):
    """SDP Task wrapper.

    Represents an executing task within the scope of the SDP.

    Eventually this management will be fairly intelligent and will
    deploy and provision tasks automatically based on the available
    resources within the SDP.

    It is expected that SDPTask will be subclassed for specific types
    of execution.

    This is a very thin wrapper for now to support RTS.
    """
    def __init__(self, task_id, task_cmd, host):
        self.task_id = task_id
        self.task_cmd = task_cmd
        self._task_cmd_array = shlex.split(task_cmd)
        self.host = host
        self._task = None
        self.state = TASK_STATES[0]
        self.start_time = None

    def launch(self):
        try:
            self._task = subprocess.Popen(self._task_cmd_array)
            self.state = TASK_STATES[1]
            self.start_time = time.time()
            logger.info("Launched task ({0}): {1}".format(self.task_id, self.task_cmd))
        except OSError, err:
            retmsg = "Failed to launch SDP task. {0}".format(err)
            logger.error(retmsg)
            return ('fail',retmsg)
        return ('ok',"New task launched successfully")

    def halt(self):
        self._task.terminate()
        self.state = TASK_STATES[2]
        return ('ok',"Task terminated successfully.")

    def uptime(self):
        if self.start_time is None: return 0
        else: return time.time() - self.start_time

    def __repr__(self):
        return "SDP Task: status => {0}, uptime => {1:.2f}, cmd => {2}".format(self.state, self.uptime(), self._task_cmd_array[0])

class SDPSubarrayProductBase(object):
    """SDP Subarray Product Base

    Represents an instance of an SDP subarray product. This includes ingest, an appropriate
    telescope model, and any required post-processing.

    In general each telescope subarray product is handled in a completely parallel fashion by the SDP.
    This class encapsulates these instances, handling control input and sensor feedback to CAM.

    ** This can be used directly as a stubbed interface for use in standalone testing and validation.
    It conforms to the functional interface, but does not launch tasks or generate data **
    """
    def __init__(self, subarray_product_id, antennas, n_channels, dump_rate, n_beams, graph, simulate):
        self.subarray_product_id = subarray_product_id
        self.antennas = antennas
        self.n_antennas = len(antennas.split(","))
        self.n_channels = n_channels
        self.dump_rate = dump_rate
        self.n_beams = n_beams
        self._async_busy = False
         # protection used to avoid external state changes during async activity on this subarray
        self.state = State.IDLE
        self.psb_id = 0
         # TODO: Most of the above parameters are now deprecated - remove
        self.simulate = simulate
        self.graph = graph
        if self.n_beams == 0:
           self.data_rate = (((self.n_antennas*(self.n_antennas+1))/2) * 4 * dump_rate * n_channels * 64) / 1e9
        else:
           self.data_rate = (n_beams * dump_rate * n_channels * 32) / 1e9
           # TODO: this should be *added* to the visibility output rate
        logger.info("Created: {0}".format(self.__repr__()))

    def set_psb(self, psb_id):
        if self.psb_id > 0:
            return ('fail', 'An existing processing schedule block is already active. Please stop the subarray product before adding a new one.')
        if self.state < State.INIT_WAIT:
            return ('fail','The subarray product specified has not yet be inited. Please do this before init post processing.')
        self.psb_id = psb_id
        time.sleep(2) # simulation
        return ('ok','Post processing has been initialised')

    def get_psb(self, psb_id):
        if self.psb_id > 0:
            return ('ok','Post processing id %i is configured and active on this subarray product' % self.psb_id)
        return ('fail','No post processing block is active on this subarray product')

    @trollius.coroutine
    def _set_state(self, state):
        if state == State.DONE: state = State.IDLE
         # handle capture done in simulator
        self.state = state
        raise Return(('ok',''))

    @trollius.coroutine
    def deconfigure(self, force=False):
        if self.state == State.IDLE or force:
            yield From(self._deconfigure())
            raise Return(('ok', 'Subarray product has been deconfigured'))
        else:
            raise Return(('fail','Subarray product is not idle and thus cannot be deconfigured. Please issue capture_done first.'))

    @trollius.coroutine
    def _deconfigure(self):
        yield From(self._set_state(State.UNCONFIGURED))

    @trollius.coroutine
    def set_state(self, state):
        # TODO: check that state change is allowed.
        if state == State.DONE:
            if self.state < State.INIT_WAIT:
                raise Return(('fail','Can only halt subarray_products that have been inited'))

        if state == State.INIT_WAIT:
            if self.state != State.IDLE:
                raise Return(('fail','Subarray product is currently in state %s, not IDLE as expected. Cannot be inited.' % (self.state.name,)))

        rcode, rval = yield From(self._set_state(state))
        if rcode == 'fail': raise Return(('fail',rval))
        else:
            if rval == '': raise Return(('ok','State changed to %s' % self.state.name))
        raise Return(('ok', rval))

    def __repr__(self):
        return "Subarray product %s: %s antennas, %i channels, %.2f dump_rate ==> %.2f Gibps (State: %s, PSB ID: %i)" % (self.subarray_product_id, self.antennas, self.n_channels, self.dump_rate, self.data_rate, self.state.name, self.psb_id)


class SDPSubarrayProduct(SDPSubarrayProductBase):
    def __init__(self, sched, *args, **kwargs):
        super(SDPSubarrayProduct, self).__init__(*args, **kwargs)
        self.sched = sched

    @trollius.coroutine
    def deconfigure(self, force=False):
        if self.state == State.IDLE or force:
            if self.state != State.IDLE:
                logger.warning("Forcing capture_done on external request.")
                try:
                    yield From(self._issue_req('capture-done', timeout=300))
                except RuntimeError:
                    pass # we gave it our best shot...
            yield From(self._deconfigure())
            raise Return(('ok', 'Subarray product has been deconfigured'))
        else:
            raise Return(('fail','Subarray product is not idle and thus cannot be deconfigured. Please issue capture_done first.'))

    @trollius.coroutine
    def _deconfigure(self):
         # handle shutdown of this subarray product in as graceful a fashion as possible.
         # TODO: be graceful :)
        logger.info("Deconfiguring subarray product")
         # issue shutdown commands for individual nodes via katcp
         # then terminate katcp connection
        try:
            yield From(self._issue_req('capture-done', node_type='ingest', timeout=300))
        except RuntimeError, e:
            logger.error("Failed to issue capture-done during shutdown request. Will continue with graph shutdown. Error was {}".format(e))
        yield From(self.graph.shutdown())

    @trollius.coroutine
    def _issue_req(self, req, args=[], node_type='ingest', **kwargs):
         # issue a request against all nodes of a particular type.
         # typically usage is to issue a command such as 'capture-init'
         # to all ingest nodes. A single failure is treated as terminal.
        logger.debug("Issuing request {} to node_type {}".format(req, node_type))
        ret_args = ""
        for node in self.graph.physical_graph:
            katcp = getattr(node, 'katcp_connection', None)
            if katcp is None:
                # Can happen either if node is not an SDPPhysicalTask or if
                # it has no katcp connection
                continue
            try:
                node.logical_node.name.index(node_type)
                 # filter out node_type(s) we don't want
                 # TODO: probably needs a regexp
            except ValueError:
                 # node name does not match requested node_type so ignore
                continue
            reply, informs = yield From(node.issue_req(req, args, **kwargs))
            if not reply.reply_ok():
                retmsg = "Failed to issue req {} to node {}. {}".format(req, node.name, reply.arguments[-1])
                raise Return(('fail', retmsg))
            ret_args += "," + reply.arguments[-1]
        if ret_args == "":
            ret_args = "Note: Req {} not issued as no nodes of type {} found.".format(req, node_type)
        raise Return(('ok', ret_args))

    @trollius.coroutine
    def exec_transitions(self, state):
        """Check for nodes that require action on state transitions."""
        for node in self.graph.physical_graph:
             # TODO: For now this is a dumb loop through all nodes.
             # when we have more this will need to be improved. It should
             # ideally also respect graph ordering constraints.
            try:
                req = node.get_transition(state)
                katcp = node.katcp_connection
            except AttributeError:
                # Not all nodes are SDPPhysicalTask
                pass
            else:
                if req is not None and katcp is not None:
                    yield From(node.issue_req(req, timeout=300))
                 # failure not necessarily catastrophic, follow the schwardtian approach of bumble on...

    @trollius.coroutine
    def _set_state(self, state):
        """The meat of the problem. Handles starting and stopping ingest processes and echo'ing requests."""
        rcode = 'ok'
        rval = ''
        logger.info("Switching state to {} from state {}".format(state.name, self.state.name))
        if state == State.INIT_WAIT:
            yield From(self.exec_transitions(State.INIT_WAIT))
            if self.simulate:
                logger.info("SIMULATE: Issuing a capture-start to the simulator")
                rcode, rval = yield From(
                    self._issue_req('configure-subarray-from-telstate', node_type='sdp.sim'))
                 # instruct the simulator to rebuild its local config from the values in telstate
                if rcode == 'ok':
                    rcode, rval = yield From(
                        self._issue_req('capture-start', args=[self.subarray_product_id], node_type='sdp.sim'))
                else:
                    logger.error("SIMULATE: configure-subarray-from-telstate failed ({})".format(rval))
        if state == State.DONE:
            if self.simulate:
                logger.info("SIMULATE: Issuing a capture-stop to the simulator")
                rcode, rval = yield From(
                    self._issue_req('capture-stop', args=[self.subarray_product_id], node_type='sdp.sim', timeout=120))
            yield From(self.exec_transitions(State.DONE))
             # called in an explicit fashion (above as well) so we can manage
             # execution order correctly when dealing with a simulator
             # all other commands can execute in arbitrary order

        if state == State.DONE or rcode == 'ok':
            if state == State.DONE: state = State.IDLE
             # make sure that we dont get stuck if capture-done is failing...
            self.state = state
        if rval == '': rval = "State changed to {0}".format(self.state)
        raise Return((rcode, rval))


class SDPControllerServer(AsyncDeviceServer):

    VERSION_INFO = ("sdpcontroller", 0, 1)
    BUILD_INFO = ("sdpcontroller", 0, 1, "rc2")

    def __init__(self, host, port, framework_info, master, loop, safe_multicast_cidr,
                 simulate=False, interface_mode=False,
                 graph_resolver=None, image_resolver=None, **kwargs):
         # setup sensors
        self._build_state_sensor = Sensor(Sensor.STRING, "build-state", "SDP Controller build state.", "")
        self._api_version_sensor = Sensor(Sensor.STRING, "api-version", "SDP Controller API version.", "")
        self._device_status_sensor = Sensor(Sensor.DISCRETE, "device-status", "Devices status of the SDP Master Controller", "", ["ok", "degraded", "fail"])
        self._fmeca_sensors = {}
        self._fmeca_sensors['FD0001'] = Sensor(Sensor.BOOLEAN, "fmeca.FD0001", "Sub-process limits", "")
         # example FMECA sensor. In this case something to keep track of issues arising from launching to many processes.
         # TODO: Add more sensors exposing resource usage and currently executing graphs
        self._ntp_sensor = CallbackSensor(Sensor.BOOLEAN, "time-synchronised","SDP Controller container (and host) is synchronised to NTP", "")

        self.simulate = simulate
        if self.simulate: logger.warning("Note: Running in simulation mode. This will simulate certain external components such as the CBF.")
        self.interface_mode = interface_mode
        if self.interface_mode: logger.warning("Note: Running master controller in interface mode. This allows testing of the interface only, no actual command logic will be enacted.")
        self.loop = loop
        if not self.interface_mode:
            self.sched = scheduler.Scheduler(loop)
            driver = pymesos.MesosSchedulerDriver(
                self.sched, framework_info, master, use_addict=True,
                implicit_acknowledgements=False)
            self.sched.set_driver(driver)
            driver.start()
        else:
            self.sched = None
        self.components = {}
         # dict of currently managed SDP components
        self._conf_future = None
         # track async product configure request to avoid handling more than one at a time

        if graph_resolver is None:
            graph_resolver = GraphResolver(simulate=self.simulate)
        self.graph_resolver = graph_resolver
        if image_resolver is None:
            image_resolver = scheduler.ImageResolver()
        self.image_resolver = image_resolver

        logger.debug("Building initial resource pool")
        self.resources = SDPCommonResources(safe_multicast_cidr)
         # create a new resource pool.

        self.subarray_products = {}
         # dict of currently configured SDP subarray_products
        self.subarray_product_config = {}
         # store calling arguments used to create a specified subarray_product
         # this has either the current args or those most recently
         # configured for this subarray_product
        self.override_dicts = {}
         # per subarray product dictionaries used to override internal config
        self.tasks = {}
         # dict of currently managed SDP tasks

        super(SDPControllerServer, self).__init__(host, port)

    def setup_sensors(self):
        """Add sensors for processes."""
        self._build_state_sensor.set_value(self.build_state())
        self.add_sensor(self._build_state_sensor)
        self._api_version_sensor.set_value(self.version())
        self.add_sensor(self._api_version_sensor)
        self._device_status_sensor.set_value('ok')
        self.add_sensor(self._device_status_sensor)

        self._ntp_sensor.set_value('0')
        self._ntp_sensor.set_read_callback(self._check_ntp_status)
        self.add_sensor(self._ntp_sensor)

          # until we know any better, failure modes are all inactive
        for s in self._fmeca_sensors.itervalues():
            s.set_value(0)
            self.add_sensor(s)

    def _check_ntp_status(self):
        try:
            return (subprocess.check_output(["/usr/bin/ntpq","-p"]).find('*') > 0 and '1' or '0', Sensor.NOMINAL, time.time())
        except OSError:
            return ('0', Sensor.NOMINAL, time.time())

    def request_halt(self, req, msg):
        """Halt the device server.

        Returns
        -------
        success : {'ok', 'fail'}
            Whether scheduling the halt succeeded.

        Examples
        --------
        ::

            ?halt
            !halt ok
        """
        self.stop()
        logger.warning("Halt requested. Attempting cleanup...")
        # this message makes it through because stop
        # only registers in .run(...) after the reply
        # has been sent.
        return req.make_reply("ok")

    @request(Str())
    @return_reply(Str())
    def request_task_terminate(self, req, task_id):
        """Terminate the specified SDP task.

        Request Arguments
        -----------------
        task_id : string
            The ID of the task to terminate

        Returns
        -------
        success : {'ok', 'fail'}
        """
        if not task_id in self.tasks: return ('fail',"Specified task ID ({0}) is unknown".format(task_id))
        task = self.tasks.pop(task_id)
        rcode, rval = task.halt()
        return (rcode, rval)


    @request(Str(optional=True),Str(optional=True),Str(optional=True))
    @return_reply(Str())
    def request_task_launch(self, req, task_id, task_cmd, host):
        """Launch a task within the SDP.
        This command allows tasks to be listed and launched within the SDP. Specification of a desired host
        is optional, as in general the master controller will decide on the most appropriate location on
        which to run the task.

        Request Arguments
        -----------------
        task_id : string
            The unique ID used to identify this task.
            If empty then all managed tasks are listed.
        task_cmd : string
            The complete command to run including fully qualified executable and arguments
            If empty then the status of the specified id is shown
        host : string
            Force the controller to launch the task on the specified host

        Returns
        -------
        success : {'ok', 'fail'}
        host,port : If appropriate, the host/port pair to connect to the task via katcp is returned.
        """
        if not task_id:
            for (task_id, task) in self.tasks.iteritems():
                req.inform(task_id, task)
            return ('ok', "{0}".format(len(self.tasks)))

        if task_id in self.tasks:
            if not task_cmd: return ('ok',"{0}: {1}".format(task_id, self.tasks[task_id]))
            else: return ('fail',"A task with the specified ID is already running and cannot be reconfigured.")

        if task_id not in self.tasks and not task_cmd: return ('fail',"You must specify a command line to run for a new task")

        self.tasks[task_id] = SDPTask(task_id, task_cmd, host)
        rcode, rval = self.tasks[task_id].launch()
        if rcode == 'fail': self.tasks.pop(task_id)
         # launch failed, discard task
        return (rcode, rval)

    @trollius.coroutine
    def deregister_product(self,subarray_product_id,force=False):
        """Deregister a subarray product.

        This first checks to make sure the product is in an appropriate state
        (ideally idle), and then shuts down the ingest and plotting
        processes associated with it.

        Forcing skips the check on state and is basically used in an emergency."""
        dp_handle = self.subarray_products[subarray_product_id]
        rcode, rval = yield From(dp_handle.deconfigure(force=force))
        raise Return((rcode, rval))

    @trollius.coroutine
    def deconfigure_on_exit(self):
        """Try to shutdown as gracefully as possible when interrupted."""
        logger.warning("SDP Master Controller interrupted - deconfiguring existing products.")
        if self._conf_future and not self._conf_future.done():
            self._conf_future.cancel()
            # Give it a bit of time to finish any cleanup
            yield From(trollius.wait([self._conf_future], 1.0))
            self._conf_future = None
        for subarray_product_id in self.subarray_products.keys():
            try:
                rcode, rval = yield From(self.deregister_product(subarray_product_id,force=True))
                if rcode != 'fail':
                    self.subarray_products.pop(subarray_product_id)
                    logger.info("Deconfigured subarray product {0} ({1},{2})".format(subarray_product_id, rcode, rval))
                else:
                    logger.warning("Failed to deconfigure product {0} ({1},{2})".format(subarray_product_id, rcode, rval))
            except Exception as e:
                logger.warning("Failed to deconfigure product {} during master controller exit ({}). Forging ahead...".format(subarray_product_id, e))

    @trollius.coroutine
    def async_stop(self):
        super(SDPControllerServer, self).stop()
        yield From(self.deconfigure_on_exit())
        yield From(self.sched.close())

    @request(Str(), Str())
    @return_reply(Str())
    def request_set_config_override(self, req, subarray_product_id, override_dict_json):
        """Override internal configuration parameters for the next configure of the
        specified subarray product.

        An existing override for this subarry product will be completely overwritten.

        The override will only persist until a successful configure has been called on the subarray product.

        Request Arguments
        -----------------
        subarray_product_id : string
            The ID of the subarray product to set overrides for in the form [<subarray_name>_]<data_product_name>.
        override_dict_json : string
            A json string containing a dict of config key:value overrides to use.
        """
        logger.info("?set-config-override called on {} with {}".format(subarray_product_id, override_dict_json))
        try:
            odict = json.loads(override_dict_json)
            if type(odict) is not dict: raise ValueError
            logger.info("Set override for subarray product {} for the following: {}".format(subarray_product_id, odict))
            self.override_dicts[subarray_product_id] = json.loads(override_dict_json)
        except ValueError as e:
            msg = "The supplied override string {} does not appear to be a valid json string containing a dict. {}".format(override_dict_json, e)
            logger.error(msg)
            return ('fail', msg)
        return ('ok', "Set {} override keys for subarray product {}".format(len(self.override_dicts[subarray_product_id]), subarray_product_id))

    @request(Str(), include_msg=True)
    @return_reply(Str())
    def request_data_product_reconfigure(self, req, req_msg, subarray_product_id):
        """Reconfigure the specified SDP subarray product instance.

           The primary use of this command is to restart the SDP components for a particular
           subarray without having to reconfigure the rest of the system.

           Essentially this runs a deconfigure() followed by a configure() with the same parameters as originally
           specified via the data_product_configure katcp call.

           Request Arguments
           -----------------
           subarray_product_id : string
             The ID of the subarray product to reconfigure in the form [<subarray_name>_]<data_product_name>.

        """
        logger.info("?data-product-reconfigure called on {}".format(subarray_product_id))
        if not subarray_product_id in self.subarray_products:
            return ('fail',"The specified subarray product id {} has no existing configuration and thus cannot be reconfigured.".format(subarray_product_id))

        if self._conf_future:
            return ('fail',"A configure/deconfigure command is currently running. Please wait until this completes to issue the reconfigure.")

        try:
            config_args = self.subarray_product_config[subarray_product_id]
        except KeyError:
            return ('fail',"No pre-existing config found for subarray {}. Unable to reconfigure.".format(subarray_product_id))

         # we are only going to allow a single conf/deconf at a time
        self._conf_future = trollius.ensure_future(self._async_data_product_configure(
            req, req_msg, subarray_product_id, "0", None, None, None, None, None))
         # start with a deconfigure

        @gen.coroutine
        def delayed_cmd():
            try:
                logger.info("Deconfiguring {} as part of a reconfigure request".format(subarray_product_id))
                (retval, retmsg, product) = yield to_tornado_future(self._conf_future)
                if retval != 'ok':
                    retmsg = "Unable to deconfigure as part of reconfigure. {}".format(retmsg)
                else:
                    del self.subarray_products[subarray_product_id]
                    del self.subarray_product_config[subarray_product_id]
                     # we already have a copy and if config now fails we don't want this lurking around anyway
                    logger.info("Removing subarray product {} reference.".format(subarray_product_id))
                    req.inform(retmsg)

                    logger.info("Issuing new configure for {} as part of reconfigure request.".format(subarray_product_id))
                    self._conf_future = trollius.ensure_future(self._async_data_product_configure(
                        req, req_msg, subarray_product_id, *config_args))
                    (retval, retmsg, product) = yield to_tornado_future(self._conf_future)
                    if retval != 'fail' and product:
                        self.subarray_products[subarray_product_id] = product
                        self.subarray_product_config[subarray_product_id] = config_args
                    else:
                        retmsg = "Unable to configure as part of reconfigure, original array deconfigured. {}".format(retmsg)
                        self.subarray_product_config.pop(subarray_product_id)
                         # deconf succeeded, but we kept the config around for the new configure which failed, so remove it
                    self.mass_inform(Message.inform("interface-changed"))
                     # let CAM know that our interface has changed
                req.reply(retval, retmsg)
            except Exception as e:
                retmsg = "Exception during ?data_product_reconfigure: {}".format(e)
                logger.error(retmsg, exc_info=True)
                req.reply('fail', retmsg)
            finally:
                try:
                    dp_handle = self.subarray_products[subarray_product_id]
                    dp_handle._async_busy = False
                     # we must have failed to deconfigure, so lets clean up
                except KeyError:
                    pass
                self._conf_future = None
        self.ioloop.add_callback(delayed_cmd)
        raise AsyncReply

    @request(Str(optional=True),Str(optional=True),Int(min=1,max=65535,optional=True),Float(optional=True),Int(min=0,max=16384,optional=True),Str(optional=True),Str(optional=True),include_msg=True)
    @return_reply(Str())
    def request_data_product_configure(self, req, req_msg, subarray_product_id, antennas, n_channels, dump_rate, n_beams, stream_sources, deprecated_cam_source):
        """Configure a SDP subarray product instance.

        A subarray product instance is comprised of a telescope state, a collection of
        containers running required SDP services, and a networking configuration
        appropriate for the required data movement.

        On configuring a new product, several steps occur:
         * Build initial static configuration. Includes elements such as IP addresses of deployment machines, multicast subscription details, etc...
         * Launch a new Telescope State Repository (redis instance) for this product and copy in static config.
         * Launch service containers as described in the static configuration.
         * Verify all services are running and reachable.


        Request Arguments
        -----------------
        subarray_product_id : string
            The ID to use for this subarray product, in the form [<subarray_name>_]<data_product_name>.
        antennas : string
            A comma-separated list of antenna names to use in this subarray product.
            These will be matched to the CBF output and used to pull only the specific
            data. If antennas is "0" or "", then this subarray product is de-configured.
            Trailing arguments can be omitted.
        n_channels : int
            Number of channels used in this subarray product (based on CBF config)
        dump_rate : float
            Dump rate of subarray product in Hz
        n_beams : int
            Number of beams in the subarray product (0 = Correlator output, 1+ = Beamformer)
        stream_sources: string
            Either:
              DEPRECATED: A specification of the multicast/unicast sources from which to receive the CBF spead stream in the form <ip>[+<count>]:<port>
            Or:
              A comma-separated list of stream identifiers in the form <stream_name>:<ip>[+<count>]:<port>
              These are used directly by the graph to configure the SDP system and thus rely on the stream_name as a key
        deprecated_cam_source : string
            DEPRECATED (only used when stream_source is in DEPRECATED use):
              A specification of the multicast/unicast sources from which to receive the CAM spead stream in the form <ip>[+<count>]:<port>

        Returns
        -------
        success : {'ok', 'fail'}
            If ok, returns the port on which the ingest process for this product is running.
        """
        logger.info("?data-product-configure called with: {}".format(req_msg))
         # INFO for now, but should be DEBUG post integration
        if not subarray_product_id:
            for (subarray_product_id,subarray_product) in self.subarray_products.iteritems():
                req.inform(subarray_product_id,subarray_product)
            return ('ok',"%i" % len(self.subarray_products))

        if antennas is None:
            if subarray_product_id in self.subarray_products:
                return ('ok',"%s is currently configured: %s" % (subarray_product_id,repr(self.subarray_products[subarray_product_id])))
            else: return ('fail',"This subarray product id has no current configuration.")

         # we have either a configure or deconfigure, which may take time, so we proceed with async if allowed
        if self._conf_future:
            return ('fail',"A data product configure command is currently executing.")

         # a configure is essentially thread safe since the array object is only exposed
         # as a last step. deconf needs some protection since the object does exist, thus
         # we first mark the product into a deconfiguring state before going async
        if antennas == "0" or antennas == "":
            try:
                dp_handle = self.subarray_products[subarray_product_id]
                dp_handle._async_busy = True
                req.inform("Starting deconfiguration of {}. This may take a few minutes...".format(subarray_product_id))
            except KeyError:
                return ('fail',"Deconfiguration of subarray product {} requested, but no configuration found.".format(subarray_product_id))
        else:
            req.inform("Starting configuration of new product {}. This may take a few minutes...".format(subarray_product_id))

         # we are only going to allow a single conf/deconf at a time
        self._conf_future = trollius.ensure_future(
            self._async_data_product_configure(
                req, req_msg, subarray_product_id, antennas, n_channels, dump_rate,
                n_beams, stream_sources, deprecated_cam_source))
        config_args = [antennas, n_channels, dump_rate, n_beams, stream_sources, deprecated_cam_source]
         # store our calling context for later use in the reconfigure command

        @gen.coroutine
        def delayed_cmd():
            try:
                (retval, retmsg, product) = yield to_tornado_future(self._conf_future)
                if retval != 'fail':
                    if (antennas == "0" or antennas == ""):
                        self.subarray_products.pop(subarray_product_id)
                        self.subarray_product_config.pop(subarray_product_id)
                        logger.info("Removing subarray product {} reference.".format(subarray_product_id))
                    if product:
                         # we can now safely expose this product for use in other katcp commands like ?capture-init
                        self.subarray_products[subarray_product_id] = product
                        self.subarray_product_config[subarray_product_id] = config_args
                    self.mass_inform(Message.inform("interface-changed"))
                     # let CAM know that our interface has changed
                req.reply(retval, retmsg)
            except Exception as e:
                retmsg = "Exception during ?data_product_configure: {}".format(e)
                logger.error(retmsg, exc_info=True)
                req.reply('fail', retmsg)
            finally:
                try:
                    dp_handle = self.subarray_products[subarray_product_id]
                    dp_handle._async_busy = False
                     # we may have failed to deconfigure, so make sure we clean up
                except KeyError:
                    pass
                self._conf_future = None
        self.ioloop.add_callback(delayed_cmd)
        raise AsyncReply

    @trollius.coroutine
    def _async_data_product_configure(self, req, req_msg, subarray_product_id, antennas, n_channels, dump_rate, n_beams, stream_sources, deprecated_cam_source):
        """Asynchronous portion of data product configure. See docstring for request_data_product_configure above.

        Returns
        -------
        success : ('ok'|'fail', message, product)
            - Returns OK on either a successful configure or if the configuration already exists.
                - If a new subarray has been configured then product is a reference to newly created SDPSubarrayProduct, else None
            - Returns fail on the following (product is always None):
                - The specified subarray product id already exists, but the config differs from that specified
                - If the antennas, channels, dump rate, beams and stream sources are not specified
                - If the stream_sources specified do not conform to either a URI or SPEAD endpoint syntax
                - If the specified subarray_product_id cannot be parsed into suitable components
                - If neither telstate nor docker python libraries are installed and we are not using interface mode
                - If one or more nodes fail to launch (e.g. container not found)
                - If one or more nodes fail to become alive (essentially a NOP for now)
                - If we fail to establish katcp connection to all nodes requiring them.
        """
        if antennas == "0" or antennas == "":
            (rcode, rval) = yield From(self.deregister_product(subarray_product_id))
            raise Return((rcode, rval, None))

        logger.info("Using '{}' as antenna mask".format(antennas))
        antennas = antennas.replace(" ",",")
         # temp hack to make sure we have a comma delimited set of antennas

        if subarray_product_id in self.subarray_products:
            dp = self.subarray_products[subarray_product_id]
            if dp.antennas == antennas and dp.n_channels == n_channels and dp.dump_rate == dump_rate and dp.n_beams == n_beams:
                raise Return(('ok',"Subarray product with this configuration already exists. Pass.", None))
            else:
                raise Return(('fail',"A subarray product with this id ({0}) already exists, but has a different configuration. Please deconfigure this product or choose a new product id to continue.".format(subarray_product_id), None))

         # all good so far, lets check arguments for validity
        if not(antennas and n_channels >= 0 and dump_rate >= 0 and n_beams >= 0 and stream_sources):
            raise Return(('fail',"You must specify antennas, n_channels, dump_rate, n_beams and appropriate spead stream sources to configure a subarray product", None))

        streams = {}
        urls = {}
         # local dict to hold streams associated with the specified data product
        try:
            streams['baseline-correlation-products_spead'] = stream_sources.split(":",2)
            streams['CAM_spead'] = deprecated_cam_source.split(":",2)
            logger.info("Adding DEPRECATED endpoints for baseline-correlation-products_spead ({}) and CAM_spead ({})".format(streams['baseline-correlation-products_spead'],streams['CAM_spead']))
        except (AttributeError, ValueError):
             # check to see if we are using the new stream_sources specifier
            try:
                for stream in stream_sources.split(","):
                    (stream_name, value) = stream.split(":", 1)
                    if re.match(r'^\w+://', value):
                        urls[stream_name] = value
                        logger.info("Adding stream {} with URL {}".format(stream_name, value))
                    else:
                        (host, port) = value.split(":", 2)
                         # just to make it explicit what we are expecting
                        # Hack to handle CAM not yet passing us fully-qualified
                        # stream names. This code should be deleted once CAM is
                        # updated.
                        if (stream_name == 'baseline-correlation-products' or
                                stream_name == 'antenna-channelised-voltage' or
                                stream_name.startswith('tied-array-channelised-voltage.')):
                            stream_name = 'corr.' + stream_name
                        streams["{}_spead".format(stream_name)] = (host, port)
                        logger.info("Adding stream {}_spead with endpoint ({},{})".format(stream_name, host, port))
            except ValueError:
                 # something is definitely wrong with these
                retmsg = "Failed to parse source stream specifiers. You must either supply cbf and cam sources in the form <ip>[+<count>]:port or a single stream_sources string that contains a comma-separated list of streams in the form <stream_name>:<ip>[+<count>]:<port> or <stream_name>:url"
                logger.error(retmsg)
                raise Return(('fail',retmsg,None))

        graph_name = self.graph_resolver(subarray_product_id)
        subarray_numeric_id = self.graph_resolver.get_subarray_numeric_id(subarray_product_id)
        if not subarray_numeric_id:
            retmsg = "Failed to parse numeric subarray identifier from specified subarray product string ({})".format(subarray_product_id)
            raise Return(('fail',retmsg,None))

        logger.info("Launching graph {}.".format(graph_name))

        resolver = scheduler.Resolver(self.image_resolver, scheduler.TaskIDAllocator(subarray_product_id + '-'))
        resolver.resources = SDPResources(self.resources, subarray_product_id)
        resolver.telstate = None

        for (stream_name, endpoint) in streams.iteritems():
            resolver.resources.set_multicast_ip(stream_name, endpoint[0])
            resolver.resources.set_port(stream_name, endpoint[1])
        for (stream_name, url) in urls.iteritems():
            resolver.resources.set_url(stream_name, url)
         # TODO: For now we encode the cam and cbf spead specification directly into the resource object.
         # Once we have multiple ingest nodes we need to factor this out into appropriate addreses for each ingest process

        graph = SDPGraph(self.sched, graph_name, resolver, subarray_product_id, self.loop, sdp_controller=self)
         # create graph object and build physical graph from specified resources

        logger.debug(graph.get_json())
         # determine additional configuration
        calculated_int_time = 1 / float(dump_rate)
        base_params = {
            'subarray_product_id':subarray_product_id
        }
        additional_config = {
            'antenna_mask':antennas,
            'output_int_time':calculated_int_time,
            'sd_int_time':calculated_int_time,
            'stream_sources':stream_sources,
            'subarray_numeric_id':subarray_numeric_id
        }
         # holds additional config that must reside within the config dict in the telstate
        req.inform("Graph {} construction complete.".format(graph_name))
        if subarray_product_id in self.override_dicts:
            odict = self.override_dicts.pop(subarray_product_id)
             # this is a use-once set of overrides
            logger.warning("Setting overrides on {} for the following: {}".format(subarray_product_id, odict))
            additional_config.update(odict)

        if self.interface_mode:
            logger.debug("Telstate configured. Base parameters {}".format(config))
            logger.warning("No components will be started - running in interface mode")
            product = SDPSubarrayProductBase(subarray_product_id, antennas, n_channels, dump_rate, n_beams, graph, self.simulate)
            raise Return(('ok',"", product))

        if katsdptelstate is None:
             # from here onwards we require the katsdptelstate module to be installed.
            retmsg = "You must have the katsdptelstate library installed to use the master controller in non interface only mode."
            logger.error(retmsg)
            raise Return(('fail',retmsg,None))

        yield From(graph.launch_telstate(additional_config, base_params))
         # launch the telescope state for this graph
        req.inform("Telstate launched. [{}]".format(graph.telstate_endpoint))
        logger.debug("Executing graph {}".format(graph_name))
        yield From(graph.execute_graph(req))
         # launch containers for those nodes that require them
        req.inform("All nodes launched")
        alive = graph.check_nodes()
         # is everything we asked for alive
        if not alive:
            ret_msg = "Some nodes in the graph failed to start. Check the error log for specific details."
            yield From(graph.shutdown())
            logger.error(ret_msg)
            raise Return(('fail', ret_msg,None))

         # at this point telstate is up, nodes have been launched, katcp connections established
         # we can now safely expose this product for use in other katcp commands like ?capture-init
         # adding a product is also safe with regard to commands like ?capture-status
        product = SDPSubarrayProduct(self.sched, subarray_product_id, antennas, n_channels, dump_rate, n_beams, graph, self.simulate)

        raise Return(('ok',"",product))

    @request(Str())
    @return_reply(Str())
    def request_capture_init(self, req, subarray_product_id):
        """Request capture of the specified subarray product to start.

        Note: This command is used to prepare the SDP for reception of data
        as specified by the subarray product provided. It is necessary to call this
        command before issuing a start command to the CBF. Essentially the SDP
        will, once this command has returned 'OK', be in a wait state until
        reception of the stream control start packet.

        Request Arguments
        -----------------
        subarray_product_id : string
            The id of the subarray product to initialise. This must have already been
            configured via the data-product-configure command.

        Returns
        -------
        success : {'ok', 'fail'}
            Whether the system is ready to capture or not.
        """
        if subarray_product_id not in self.subarray_products:
            return ('fail','No existing subarray product configuration with this id found')
        sa = self.subarray_products[subarray_product_id]
        if sa._async_busy:
            return ('fail',"The specified subarray product id ({}) is busy with an asynchronous operation and cannot be manipulated at this time.".format(subarray_product_id))

        @gen.coroutine
        def delayed_cmd():
            try:
                rcode, rval = yield to_tornado_future(trollius.ensure_future(sa.set_state(State.INIT_WAIT)))
                 # attempt to set state to init
                if rcode == 'fail':
                    req.reply(rcode, rval)
                else:
                    req.reply('ok','SDP ready')
            finally:
                sa._async_busy = False

        sa._async_busy = True
        self.ioloop.add_callback(delayed_cmd)
        raise AsyncReply

    @request(Str(optional=True))
    @return_reply(Str())
    def request_telstate_endpoint(self, req, subarray_product_id):
        """Returns the endpoint for the telescope state repository of the
        specified subarray product.

        Request Arguments
        -----------------
        subarray_product_id : string
            The id of the subarray product whose state we wish to return.

        Returns
        -------
        success : {'ok', 'fail'}
        state : str
        """
        if not subarray_product_id:
            for (subarray_product_id,subarray_product) in self.subarray_products.iteritems():
                req.inform(subarray_product_id, subarray_product.graph.telstate_endpoint)
            return ('ok',"%i" % len(self.subarray_products))

        if subarray_product_id not in self.subarray_products:
            return ('fail','No existing subarray product configuration with this id found')
        return ('ok',self.subarray_products[subarray_product_id].graph.telstate_endpoint)

    @request(Str(optional=True))
    @return_reply(Str())
    def request_capture_status(self, req, subarray_product_id):
        """Returns the status of the specified subarray product.

        Request Arguments
        -----------------
        subarray_product_id : string
            The id of the subarray product whose state we wish to return.

        Returns
        -------
        success : {'ok', 'fail'}
        state : str
        """
        if not subarray_product_id:
            for (subarray_product_id,subarray_product) in self.subarray_products.iteritems():
                req.inform(subarray_product_id,subarray_product.state.name)
            return ('ok',"%i" % len(self.subarray_products))

        if subarray_product_id not in self.subarray_products:
            return ('fail','No existing subarray product configuration with this id found')
        return ('ok',self.subarray_products[subarray_product_id].state.name)

    @request(Str(),Int(optional=True))
    @return_reply(Str())
    def request_postproc_init(self, req, subarray_product_id, psb_id):
        """Returns the status of the specified subarray product.

        Request Arguments
        -----------------
        subarray_product_id : string
            The id of the subarray product that will provide data to the post processor
        psb_id : integer
            The id of the post processing schedule block to retrieve
            from the observations database that containts the configuration
            to apply to the post processor.

        Returns
        -------
        success : {'ok', 'fail'}
        """
        if subarray_product_id not in self.subarray_products:
            return ('fail','No existing subarray product configuration with this id found')
        sa = self.subarray_products[subarray_product_id]

        if not psb_id >= 0:
            rcode, rval = sa.get_psb(psb_id)
            return (rcode, rval)

        rcode, rval = sa.set_psb(psb_id)
        return (rcode, rval)

    @request(Str())
    @return_reply(Str())
    def request_capture_done(self, req, subarray_product_id):
        """Halts the currently specified subarray product

        Request Arguments
        -----------------
        subarray_product_id : string
            The id of the subarray product whose state we wish to halt.

        Returns
        -------
        success : {'ok', 'fail'}
        state : str
        """
        if subarray_product_id not in self.subarray_products:
            return ('fail','No existing subarray product configuration with this id found')
        sa = self.subarray_products[subarray_product_id]
        if sa._async_busy:
            return ('fail',"The specified subarray product id ({}) is busy with an asynchronous operation and cannot be manipulated at this time.".format(subarray_product_id))

        @gen.coroutine
        def delayed_cmd():
            try:
                rcode, rval = yield to_tornado_future(trollius.ensure_future(
                    self.subarray_products[subarray_product_id].set_state(State.DONE)))
                req.reply(rcode, rval)
            finally:
                sa._async_busy = False

        sa._async_busy = True
        self.ioloop.add_callback(delayed_cmd)
        raise AsyncReply

    @request()
    @return_reply(Str())
    def request_sdp_shutdown(self, req):
        """Shut down the SDP master controller and all controlled nodes.

        Returns
        -------
        success : {'ok', 'fail'}
            Whether the shutdown sequence of all other nodes succeeded.
        hosts : str
            Comma separated lists of hosts that have been shutdown (excl mc host)
        """
        logger.info("SDP Shutdown called.")

        @gen.coroutine
        def delayed_cmd():
            yield to_tornado_future(trollius.ensure_future(self.deconfigure_on_exit()))
             # attempt to deconfigure any existing subarrays
             # will always succeed even if some deconfigure fails
            # TODO: reimplement this
            req.reply('fail', 'sdp-shutdown not implemented')

        # TODO: is this async-safe with concurrent configure calls?
        self.ioloop.add_callback(delayed_cmd)
        raise AsyncReply

    @request(include_msg=True)
    @return_reply(Int(min=0))
    def request_sdp_status(self, req, reqmsg):
        """Request status of SDP components.

        Request Arguments
        -----------------
        process : str
            Name of a registered process.

        Returns
        -------
        success : {'ok', 'fail'}
            Whether retrieving component status succeeded.
        informs : int
            Number of #sdp_status informs sent
        """
        for (component_name,component) in self.components:
            req.inform("%s:%s",component_name,component.status)
        return ("ok", len(self.components))
