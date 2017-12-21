"""Core classes for the SDP Controller.

"""

import time
import logging
import subprocess
import shlex
import json
import signal
import re
import sys
import os.path
from collections import deque
import asyncio

from tornado import gen
import tornado.platform.asyncio
import tornado.ioloop
import tornado.concurrent

import networkx
import networkx.drawing.nx_pydot
import six
import enum
from decorator import decorator

import jsonschema
import ipaddress
import netifaces
import faulthandler

from prometheus_client import Histogram

from katcp import AsyncDeviceServer, Sensor, AsyncReply, FailReply, Message
from katcp.kattypes import request, return_reply, Str, Int, Float, Bool
import katsdpcontroller
from katsdpservices.asyncio import to_tornado_future
import katsdptelstate
from katsdptelstate.endpoint import endpoint_list_parser
from . import scheduler, tasks, product_config, generator
from .tasks import State, DEPENDS_INIT


faulthandler.register(signal.SIGUSR2, all_threads=True)


REQUEST_TIME = Histogram(
    'katsdpcontroller_request_time_seconds', 'Time to process katcp requests', ['request'],
    buckets=(0.001, 0.01, 0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0, 600.0))
logger = logging.getLogger("katsdpcontroller.katsdpcontroller")


def log_task_exceptions(task, msg):
    """Add a done callback to a task that logs any exception it raised.

    Parameters
    ----------
    task : :class:`asyncio.Future`
        Task (or any future) on which the callback will be added
    msg : str
        Message that will be logged
    """
    def done_callback(task):
        if not task.cancelled():
            try:
                task.result()
            except Exception:
                logger.warning('%s', msg, exc_info=True)
    task.add_done_callback(done_callback)


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
       """
    def __init__(self, overrides=[]):
        self._overrides = {}

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
            logger.warning("Graph name specified by subarray_product_id (%s) has been overridden "
                           "to %s", subarray_product_id, base_graph_name)
             # if an override is set use this instead, but warn the user about this
        except KeyError:
            base_graph_name = subarray_product_id.split("_")[-1]
             # default graph name is to split out the trailing name from the subarray product id specifier
        return base_graph_name


class MulticastIPResources(object):
    def __init__(self, network):
        self._hosts = network.hosts()

    def get_ip(self, n_addresses):
        try:
            ip = str(next(self._hosts))
            if n_addresses > 1:
                for i in range(1, n_addresses):
                    next(self._hosts)
                ip = '{}+{}'.format(ip, n_addresses - 1)
            return ip
        except StopIteration:
            raise RuntimeError('Multicast IP addresses exhausted')


class SDPCommonResources(object):
    """Assigns multicast groups and ports across all subarrays."""
    def __init__(self, safe_multicast_cidr):
        logger.info("Using {} for multicast subnet allocation".format(safe_multicast_cidr))
        self.multicast_subnets = deque(
            ipaddress.ip_network(safe_multicast_cidr).subnets(new_prefix=24))


class SDPResources(object):
    """Helper class to allocate resources for a single subarray-product."""
    def __init__(self, common, subarray_product_id):
        self.subarray_product_id = subarray_product_id
        self._common = common
        try:
            self._subnet = self._common.multicast_subnets.popleft()
        except IndexError:
            raise RuntimeError("Multicast subnets exhausted")
        logger.info("Using {} for {}".format(self._subnet, subarray_product_id))
        self._multicast_resources = MulticastIPResources(self._subnet)

    def get_multicast_ip(self, n_addresses):
        """Assign multicast addresses for a group."""
        return self._multicast_resources.get_ip(n_addresses)

    def get_port(self):
        """Return an assigned port for a multicast group"""
        return 7148

    def close(self):
        if self._subnet is not None:
            self._common.multicast_subnets.append(self._subnet)
            self._subnet = None
            self._multicast_resources = None


class CaptureBlock(object):
    """A capture block is book-ended by a capture-init and a capture-done,
    although processing on it continues after the capture-done."""

    class State(enum.Enum):
        INITIALISING = 0
        CAPTURING = 1
        POSTPROCESSING = 2
        DEAD = 3

    def __init__(self, name, loop):
        self.name = name
        self._state = CaptureBlock.State.INITIALISING
        self.postprocess_task = None
        self.dead_event = asyncio.Event(loop=loop)
        self.state_change_callback = None

    @property
    def state(self):
        return self._state

    @state.setter
    def state(self, value):
        if self._state != value:
            self._state = value
            if value == CaptureBlock.State.DEAD:
                self.dead_event.set()
            if self.state_change_callback is not None:
                self.state_change_callback()


class SDPSubarrayProductBase(object):
    """SDP Subarray Product Base

    Represents an instance of an SDP subarray product. This includes ingest, an
    appropriate telescope model, and any required post-processing.

    In general each telescope subarray product is handled in a completely
    parallel fashion by the SDP. This class encapsulates these instances,
    handling control input and sensor feedback to CAM.

    State changes are asynchronous operations. There can only be one
    asynchronous operation at a time. Attempting a second one will either
    fail, or in some cases will cancel the prior operation. To avoid race
    conditions, changes to :attr:`state` should generally only be made from
    inside the asynchronous tasks.

    There are some invariants that must hold at yield points:
    - There is at most one capture block in state CAPTURING.
    - :attr:`current_capture_block` is the capture block in state
      CAPTURING, or ``None`` if there isn't one.
    - :attr:`current_capture_block` is set if and only if the subarray state
      is CAPTURING.
    - Keys in :attr:`capture_blocks` also appear in `capture_block_names`.
    - Elements of :attr:`capture_blocks` are not in state DEAD.

    This is a base class that is intended to be subclassed. The methods whose
    names end in ``_impl`` are extension points that should be implemented in
    subclasses to do the real work. These methods are run as part of the
    asynchronous operations. They need to be cancellation-safe, to allow for
    forced deconfiguration to abort them.
    """
    def __init__(self, sched, config, resolver, subarray_product_id, loop, sdp_controller):
        self._async_task = None    #: Current background task (can only be one)
        self.sched = sched
        self.config = config
        self.resolver = resolver
        self.subarray_product_id = subarray_product_id
        self.loop = loop
        self.sdp_controller = sdp_controller
        self.logical_graph = generator.build_logical_graph(config)
        self.telstate_endpoint = ""
        self.telstate = None
        self.capture_blocks = {}              # live capture blocks, indexed by name
        self.capture_block_names = set()      # all capture block names used
        self.current_capture_block = None     # set between capture_init and capture_done
        self.dead_event = asyncio.Event(loop)   # Set when reached state DEAD
        # Callbacks that are called when we reach state DEAD. These are
        # provided in addition to dead_event, because sometimes it's
        # necessary to react immediately rather than waiting for next time
        # around the event loop. Each callback takes self as the argument.
        self.dead_callbacks = [lambda product: product.dead_event.set()]
        self._state = None
        self.capture_block_sensor = Sensor.string(
            subarray_product_id + ".capture-block-state",
            "JSON dictionary of capture block states for active capture blocks",
            default="{}", initial_status=Sensor.NOMINAL)
        self.state_sensor = Sensor.discrete(
            subarray_product_id + ".state",
            "State of the subarray product state machine",
            params=[member.lower() for member in State.__members__])
        self.state = State.CONFIGURING   # This sets the sensor
        logger.info("Created: {!r}".format(self))

    @property
    def state(self):
        return self._state

    @state.setter
    def state(self, value):
        self._state = value
        self.state_sensor.set_value(value.name.lower())

    @property
    def async_busy(self):
        """Whether there is an asynchronous state-change operation in progress."""
        return self._async_task is not None and not self._async_task.done()

    def _fail_if_busy(self):
        """Raise a FailReply if there is an asynchronous operation in progress."""
        if self.async_busy:
            raise FailReply('Subarray product {} is busy with an operation. '
                            'Please wait for it to complete first.'.format(self.subarray_product_id))

    @asyncio.coroutine
    def configure_impl(self, req):
        """Extension point to configure the subarray."""
        pass

    @asyncio.coroutine
    def deconfigure_impl(self, force, ready):
        """Extension point to deconfigure the subarray.

        Parameters
        ----------
        force : bool
            Whether to do an abrupt deconfiguration without waiting for
            postprocessing.
        ready : :class:`asyncio.Event`
            If the ?product-deconfigure command should return before
            deconfiguration is complete, this event can be set at that point.
        """
        pass

    @asyncio.coroutine
    def capture_init_impl(self, capture_block):
        """Extension point to start a capture block."""
        pass

    @asyncio.coroutine
    def capture_done_impl(self, capture_block):
        """Extension point to stop a capture block.

        This should only do the work needed for the ``capture-done`` master
        controller request to return. The caller takes care of calling
        :meth:`postprocess_impl`.

        It needs to be safe to run from DECONFIGURING state, because it may be
        run as part of forced deconfigure.
        """
        pass

    @asyncio.coroutine
    def postprocess_impl(self, capture_block):
        """Complete the post-processing for a capture block.

        Subclasses should override this if a capture block is not finished when
        :meth:`_capture_done` returns.
        """
        pass

    @asyncio.coroutine
    def _configure(self, req):
        """Asynchronous task that does he configuration."""
        yield from self.configure_impl(req)
        self.state = State.IDLE

    @asyncio.coroutine
    def _deconfigure(self, force, ready):
        """Asynchronous task that does the deconfiguration.

        This handles the entire burndown cycle. The end of the synchronous
        part (at which point the katcp request returns) is signalled by
        setting the event `ready`.
        """
        self.state = State.DECONFIGURING
        if self.current_capture_block is not None:
            try:
                capture_block = self.current_capture_block
                # To prevent trying again if we get a second forced-deconfigure.
                self.current_capture_block = None
                yield from self.capture_done_impl(capture_block)
            except asyncio.CancelledError:
                raise
            except Exception as error:
                logger.error("Failed to issue capture-done during shutdown request. "
                             "Will continue with graph shutdown.", exc_info=True)

        if force:
            for capture_block in list(self.capture_blocks.values()):
                if capture_block.postprocess_task is not None:
                    logger.warning('Cancelling postprocessing for capture block %s',
                                   capture_block.name)
                    capture_block.postprocess_task.cancel()
                else:
                    self._capture_block_dead(capture_block)

        yield from self.deconfigure_impl(force, ready)

        # Allow all the postprocessing tasks to finish up
        # Note: this needs to be done carefully, because self.capture_blocks
        # can change during the yield.
        while self.capture_blocks:
            name, capture_block = next(iter(self.capture_blocks.items()))
            logging.info('Waiting for capture block %s to terminate', name)
            yield from capture_block.dead_event.wait()
            self.capture_blocks.pop(name, None)

        self.state = State.DEAD
        ready.set()     # In case deconfigure_impl didn't already do this
        # Setting dead_event is done by the first callback
        for callback in self.dead_callbacks:
            callback(self)

    def _capture_block_dead(self, capture_block):
        """Mark a capture block as dead and remove it from the list."""
        try:
            del self.capture_blocks[capture_block.name]
        except KeyError:
            pass      # Allows this function to be called twice
        # Setting the state will trigger _update_capture_block_sensor, which
        # will update the sensor with the value removed
        capture_block.state = CaptureBlock.State.DEAD

    def _update_capture_block_sensor(self):
        value = {name: capture_block.state.name.lower()
                 for name, capture_block in six.iteritems(self.capture_blocks)}
        self.capture_block_sensor.set_value(json.dumps(value, sort_keys=True))

    @asyncio.coroutine
    def _capture_init(self, capture_block):
        self.capture_block_names.add(capture_block.name)
        self.capture_blocks[capture_block.name] = capture_block
        capture_block.state_change_callback = self._update_capture_block_sensor
        # Update the sensor with the INITIALISING state
        self._update_capture_block_sensor()
        try:
            yield from self.capture_init_impl(capture_block)
        except Exception:
            self._capture_block_dead(capture_block)
            raise
        assert self.current_capture_block is None
        self.state = State.CAPTURING
        self.current_capture_block = capture_block
        capture_block.state = CaptureBlock.State.CAPTURING

    @asyncio.coroutine
    def _capture_done(self):
        """The asynchronous task that handles ?capture-done. See
        :meth:`capture_done_impl` for additional details.

        This is only called for a "normal" capture-done. Forced deconfigures
        call :meth:`capture_done_impl` directly.

        Returns
        -------
        The capture block that was stopped
        """
        capture_block = self.current_capture_block
        assert capture_block is not None
        yield from self.capture_done_impl(capture_block)
        assert self.state == State.CAPTURING
        assert self.current_capture_block is capture_block
        self.state = State.IDLE
        self.current_capture_block = None
        capture_block.state = CaptureBlock.State.POSTPROCESSING
        capture_block.postprocessing_task = asyncio.ensure_future(
            self.postprocess_impl(capture_block), loop=self.loop)
        log_task_exceptions(
            capture_block.postprocessing_task,
            "Exception in postprocessing for {}/{}".format(self.subarray_product_id,
                                                           capture_block.name))
        capture_block.postprocessing_task.add_done_callback(
            lambda task: self._capture_block_dead(capture_block))
        return capture_block

    def _clear_async_task(self, future):
        """Clear the current async task.

        Parameters
        ----------
        future : :class:`asyncio.Future`
            The expected value of :attr:`_async_task`. If it does not match,
            it is not cleared (this can happen if another task replaced it
            already).
        """
        if self._async_task is future:
            self._async_task = None

    @asyncio.coroutine
    def _replace_async_task(self, new_task):
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
            yield from asyncio.wait([old_task], loop=self.loop)
        return self._async_task is new_task

    @asyncio.coroutine
    def configure(self, req):
        assert not self.async_busy, "configure should be the first thing to happen"
        assert self.state == State.CONFIGURING, "configure should be the first thing to happen"
        task = asyncio.ensure_future(self._configure(req), loop=self.loop)
        log_task_exceptions(task, "Configuring subarray product {} failed".format(
            self.subarray_product_id))
        self._async_task = task
        try:
            yield from task
        finally:
            self._clear_async_task(task)

    @asyncio.coroutine
    def deconfigure(self, force=False):
        """Start deconfiguration of the subarray, but does not wait for it to complete."""
        if self.state == State.DEAD:
            return
        if self.async_busy:
            if not force:
                self._fail_if_busy()
            else:
                logger.warning('Subarray product %s is busy with an operation, but deconfiguring anyway',
                               self.subarray_product_id)

        if self.state != State.IDLE:
            if not force:
                raise FailReply('Subarray product is not idle and thus cannot be deconfigured. Please issue capture_done first.')
            else:
                logger.warning('Subarray product %s is in state %s, but deconfiguring anyway',
                               self.subarray_product_id, self.state.name)
        logger.info("Deconfiguring subarray product %s", self.subarray_product_id)

        ready = asyncio.Event(self.loop)
        task = asyncio.ensure_future(self._deconfigure(force, ready), loop=self.loop)
        log_task_exceptions(task, "Deconfiguring {} failed".format(self.subarray_product_id))
        # Make sure that ready gets unblocked even if task throws.
        task.add_done_callback(lambda future: ready.set())
        task.add_done_callback(self._clear_async_task)
        if (yield from self._replace_async_task(task)):
            yield from ready.wait()
        # We don't wait for task to complete, but if it's already done we
        # pass back any exceptions.
        if task.done():
            yield from task

    @asyncio.coroutine
    def capture_init(self, program_block_id):
        def format_cbid(seq):
            return '{}-{:05}'.format(program_block_id, seq)

        self._fail_if_busy()
        if self.state != State.IDLE:
            raise FailReply('Subarray product {} is currently in state {}, not IDLE as expected. '
                            'Cannot be inited.'.format(self.subarray_product_id, self.state.name))
        if program_block_id is None:
            # Match the layout of program block IDs
            program_block_id = '00000000-00000'
        # Find first unique capture block ID for that PB ID
        seq = 0
        while format_cbid(seq) in self.capture_block_names:
            seq += 1
        capture_block_id = format_cbid(seq)
        logger.info('Using capture block ID %s', capture_block_id)

        capture_block = CaptureBlock(capture_block_id, self.loop)
        task = asyncio.ensure_future(self._capture_init(capture_block), loop=self.loop)
        self._async_task = task
        try:
            yield from task
        finally:
            self._clear_async_task(task)

    @asyncio.coroutine
    def capture_done(self):
        self._fail_if_busy()
        if self.state != State.CAPTURING:
            raise FailReply('Subarray product is currently in state {}, not CAPTURING as expected. '
                            'Cannot be stopped.'.format(self.state.name))
        task = asyncio.ensure_future(self._capture_done(), loop=self.loop)
        self._async_task = task
        try:
            yield from task
        finally:
            self._clear_async_task(task)

    def write_graphs(self, output_dir):
        """Write visualisations to `output_dir`."""
        for name in ['ready', 'init', 'kill', 'resolve', 'resources']:
            if name != 'resources':
                g = scheduler.subgraph(self.logical_graph, 'depends_' + name)
            else:
                g = scheduler.subgraph(self.logical_graph, scheduler.Scheduler.depends_resources)
            g = networkx.relabel_nodes(g, {node: node.name for node in g})
            g = networkx.drawing.nx_pydot.to_pydot(g)
            filename = os.path.join(output_dir,
                                    '{}_{}.svg'.format(self.subarray_product_id, name))
            try:
                g.write_svg(filename)
            except (IOError, OSError) as error:
                logger.warn('Could not write %s: %s', filename, error)

    def __repr__(self):
        return "Subarray product {} (State: {})".format(self.subarray_product_id, self.state.name)


class SDPSubarrayProductInterface(SDPSubarrayProductBase):
    """Dummy implementation of SDPSubarrayProductBase interface that does not
    actually run anything.
    """
    def __init__(self, *args, **kwargs):
        super(SDPSubarrayProductInterface, self).__init__(*args, **kwargs)
        self._interface_mode_sensors = InterfaceModeSensors(self.subarray_product_id)
        sensors = self._interface_mode_sensors.sensors
        self._capture_block_states = [
            sensor for sensor in sensors.values() if sensor.name.endswith('.capture-block-state')]

    def _update_capture_block_state(self, capture_block_id, state):
        """Update the simulated *.capture-block-state sensors.

        The dictionary that is JSON-encoded in the sensor value is updated to
        set the value associated with the key `capture_block_id`. If `state` is
        `None`, the key is removed instead.
        """
        for name, sensor in self._interface_mode_sensors.sensors.items():
            if name.endswith('.capture-block-state'):
                states = json.loads(sensor.value())
                if state is None:
                    states.pop(capture_block_id, None)
                else:
                    states[capture_block_id] = state
                sensor.set_value(json.dumps(states))

    @asyncio.coroutine
    def capture_init_impl(self, capture_block):
        self._update_capture_block_state(capture_block.name, 'CAPTURING')

    @asyncio.coroutine
    def capture_done_impl(self, capture_block):
        self._update_capture_block_state(capture_block.name, 'PROCESSING')

    @asyncio.coroutine
    def postprocess_impl(self, capture_block):
        yield from asyncio.sleep(0.1, loop=self.loop)
        self._update_capture_block_state(capture_block.name, None)

    @asyncio.coroutine
    def configure_impl(self, req):
        logger.warning("No components will be started - running in interface mode")
        # Add dummy sensors for this product
        self._interface_mode_sensors.add_sensors(self.sdp_controller)

    @asyncio.coroutine
    def deconfigure_impl(self, force, ready):
        self._interface_mode_sensors.remove_sensors(self.sdp_controller)


class SDPSubarrayProduct(SDPSubarrayProductBase):
    """Subarray product that actually launches nodes."""
    def _instantiate(self, logical_node, sdp_controller):
        if isinstance(logical_node, tasks.SDPLogicalTask):
            return logical_node.physical_factory(
                logical_node, self.loop,
                sdp_controller, self.subarray_product_id)
        else:
            return logical_node.physical_factory(logical_node, self.loop)

    def __init__(self, sched, config, resolver, subarray_product_id, loop,
                 sdp_controller, telstate_name='telstate'):
        super(SDPSubarrayProduct, self).__init__(
            sched, config, resolver, subarray_product_id, loop, sdp_controller)
        # generate physical nodes
        mapping = {logical: self._instantiate(logical, sdp_controller)
                   for logical in self.logical_graph}
        self.physical_graph = networkx.relabel_nodes(self.logical_graph, mapping)
        # Nodes indexed by logical name
        self._nodes = {node.logical_node.name: node for node in self.physical_graph}
        self.telstate_node = self._nodes[telstate_name]

    @asyncio.coroutine
    def _issue_req(self, req, args=[], node_type='ingest', **kwargs):
        """Issue a request against all nodes of a particular type. Typical
        usage is to issue a command such as 'capture-init' to all ingest nodes.
        A single failure is treated as terminal.

        Returns
        -------
        results : str
            Human-readable representation of the ok replies

        Raises
        ------
        katcp.FailReply
            If any of the underlying requests fail
        Exception
            Any exceptions raised by katcp itself will propagate
        """
        logger.debug("Issuing request {} to node_type {}".format(req, node_type))
        ret_args = ""
        for node in self.physical_graph:
            katcp = getattr(node, 'katcp_connection', None)
            if katcp is None:
                # Can happen either if node is not an SDPPhysicalTask or if
                # it has no katcp connection
                continue
            # filter out node_type(s) we don't want
            if (not node.logical_node.name.startswith(node_type + '.')
                    and node.logical_node.name != node_type):
                continue
            reply, informs = yield from node.issue_req(req, args, **kwargs)
            if not reply.reply_ok():
                retmsg = "Failed to issue req {} to node {}. {}".format(req, node.name, reply.arguments[-1])
                raise FailReply(retmsg)
            ret_args += "," + reply.arguments[-1]
        if ret_args == "":
            ret_args = "Note: Req {} not issued as no nodes of type {} found.".format(req, node_type)
        return ret_args

    @asyncio.coroutine
    def _exec_node_transition(self, node, req, deps):
        if deps:
            yield from asyncio.gather(*deps, loop=node.loop)
        if req is not None:
            if node.katcp_connection is None:
                logger.warning('Cannot issue %s to %s because there is no katcp connection',
                               req, node.name)
            else:
                # TODO: should handle katcp exceptions or failed replies
                yield from node.issue_req(req[0], req[1:], timeout=300)

    @asyncio.coroutine
    def exec_transitions(self, old_state, new_state, reverse, capture_block):
        """Issue requests to nodes on state transitions.

        The requests are made in parallel, but respects `depends_init`
        dependencies in the graph.

        Parameters
        ----------
        old_state : :class:`~katsdpcontroller.tasks.State`
            Previous state
        new_state : :class:`~katsdpcontroller.tasks.State`
            New state
        reverse : bool
            If there is a `depends_init` edge from A to B in the graph, A's
            request will be made first if `reverse` is false, otherwise B's
            request will be made first.
        capture_block : :class:`CaptureBlock`
            The capture block is that being transitioned
        """
        # Create a copy of the graph containing only dependency edges.
        deps_graph = scheduler.subgraph(self.physical_graph, DEPENDS_INIT)
        # Reverse it
        if not reverse:
            deps_graph = deps_graph.reverse(copy=False)

        tasks = {}     # Keyed by node
        # We grab the ioloop of the first task we create.
        loop = None
        # Lexicographical tie-breaking isn't strictly required, but it makes
        # behaviour predictable.
        now = time.time()   # Outside loop to be consistent across all nodes
        for node in networkx.lexicographical_topological_sort(deps_graph, key=lambda x: x.name):
            req = None
            try:
                req = node.get_transition(old_state, new_state)
            except AttributeError:
                # Not all nodes are SDPPhysicalTask
                pass
            if req is not None:
                # Apply {} substitutions to request data
                subst = dict(capture_block_id=capture_block.name,
                             time=now)
                req = [field.format(**subst) for field in req]
            deps = [tasks[trg] for trg in deps_graph.predecessors(node) if trg in tasks]
            if deps or req is not None:
                task = asyncio.ensure_future(self._exec_node_transition(node, req, deps),
                                              loop=node.loop)
                loop = node.loop
                tasks[node] = task
        if tasks:
            yield from asyncio.gather(*tasks.values(), loop=loop)

    @asyncio.coroutine
    def capture_init_impl(self, capture_block):
        if any(node.logical_node.name.startswith('sim.') for node in self.physical_graph):
            logger.info('SIMULATE: Configuring antennas in simulator(s)')
            try:
                # Replace temporary fake antennas with ones configured by kattelmod
                yield from self._issue_req('configure-subarray-from-telstate', node_type='sim')
            except asyncio.CancelledError:
                raise
            except Exception as error:
                logger.error("SIMULATE: configure-subarray-from-telstate failed", exc_info=True)
                raise FailReply(
                    "SIMULATE: configure-subarray-from-telstate failed: {}".format(error))
        yield from self.exec_transitions(State.IDLE, State.CAPTURING, True, capture_block)

    @asyncio.coroutine
    def capture_done_impl(self, capture_block):
        yield from self.exec_transitions(State.CAPTURING, State.IDLE, False, capture_block)

    @asyncio.coroutine
    def postprocess_impl(self, capture_block):
        for node in self.physical_graph:
            if isinstance(node, tasks.SDPPhysicalTask):
                observer = node.capture_block_state_observer
                if observer is not None:
                    yield from observer.wait_capture_block_done(capture_block.name)

    @asyncio.coroutine
    def _launch_telstate(self):
        """Make sure the telstate node is launched"""
        boot = [self.telstate_node]

        base_params = self.physical_graph.graph.get(
            'config', lambda resolver: {})(self.resolver)
        base_params['subarray_product_id'] = self.subarray_product_id
        base_params['sdp_config'] = self.config
        # Provide attributes to describe the relationships between CBF streams
        # and instruments. This could be extracted from sdp_config, but these
        # specific sensors are easier to mock.
        for name, stream in six.iteritems(self.config['inputs']):
            if stream['type'].startswith('cbf.'):
                prefix = name + katsdptelstate.TelescopeState.SEPARATOR
                for suffix in ['src_streams', 'instrument_dev_name']:
                    if suffix in stream:
                        base_params[prefix + suffix] = stream[suffix]

        logger.debug("Launching telstate. Base parameters {}".format(base_params))
        yield from self.sched.launch(self.physical_graph, self.resolver, boot)
         # encode metadata into the telescope state for use
         # in component configuration
         # connect to telstate store
        self.telstate_endpoint = '{}:{}'.format(self.telstate_node.host,
                                                self.telstate_node.ports['telstate'])
        self.telstate = katsdptelstate.TelescopeState(endpoint=self.telstate_endpoint)
        self.resolver.telstate = self.telstate

        logger.debug("base params: %s", base_params)
         # set the configuration
        for k, v in base_params.items():
            self.telstate.add(k, v, immutable=True)

    def check_nodes(self):
        """Check that all requested nodes are actually running.

        .. todo::

           Also check health state sensors
        """
        for node in self.physical_graph:
            if node.state != scheduler.TaskState.READY:
                logger.warn('Task %s is in state %s instead of READY', node.name, node.state.name)
                return False
        return True

    @asyncio.coroutine
    def _shutdown(self, force):
        try:
            # TODO: issue progress reports as tasks stop
            yield from self.sched.kill(self.physical_graph, force=force)
        finally:
            if hasattr(self.resolver, 'resources'):
                self.resolver.resources.close()

    @asyncio.coroutine
    def configure_impl(self, req):
        try:
            try:
                resolver = self.resolver
                resolver.resources = SDPResources(self.sdp_controller.resources,
                                                  self.subarray_product_id)
                # launch the telescope state for this graph
                yield from self._launch_telstate()
                req.inform("Telstate launched. [{}]".format(self.telstate_endpoint))
                 # launch containers for those nodes that require them
                yield from self.sched.launch(self.physical_graph, self.resolver)
                req.inform("All nodes launched")
                alive = self.check_nodes()
                # is everything we asked for alive
                if not alive:
                    ret_msg = "Some nodes in the graph failed to start. Check the error log for specific details."
                    logger.error(ret_msg)
                    raise FailReply(ret_msg)
                # Record the TaskInfo for each task in telstate, as well as details
                # about the image resolver.
                details = {}
                for task in self.physical_graph:
                    if isinstance(task, scheduler.PhysicalTask):
                        details[task.logical_node.name] = {
                            'host': task.host,
                            'taskinfo': task.taskinfo.to_dict()
                        }
                self.telstate.add('sdp_task_details', details, immutable=True)
                self.telstate.add('sdp_image_tag', resolver.image_resolver.tag, immutable=True)
                self.telstate.add('sdp_image_overrides', resolver.image_resolver.overrides,
                                   immutable=True)
            except Exception:
                # If there was a problem the graph might be semi-running. Shut it all down.
                exc_info = sys.exc_info()
                yield from self._shutdown(force=True)
                six.reraise(*exc_info)
        except scheduler.InsufficientResourcesError as error:
            raise FailReply('Insufficient resources to launch {}: {}'.format(
                self.subarray_product_id, error))
        except scheduler.ImageError as error:
            raise FailReply(str(error))

    @asyncio.coroutine
    def deconfigure_impl(self, force, ready):
        if force:
            yield from self._shutdown(force=force)
            ready.set()
        else:
            def must_wait(node):
                return (isinstance(node.logical_node, tasks.SDPLogicalTask)
                        and node.logical_node.deconfigure_wait)
            # Start the shutdown in a separate task, so that we can monitor
            # for task shutdown.
            wait_tasks = [node.dead_event.wait() for node in self.physical_graph if must_wait(node)]
            shutdown_task = asyncio.ensure_future(self._shutdown(force=force), loop=self.loop)
            yield from asyncio.gather(*wait_tasks, loop=self.loop)
            ready.set()
            yield from shutdown_task


def async_request(func):
    """Decorator for requests that run asynchronously on the tornado ioloop
    of :class:`SDPControllerServer`. This converts a handler that returns a
    future to one that uses AsyncReply. The difference is that this allows
    further commands to be immediately processed *on the same connection*.

    The function itself must return a future that resolves to a reply. Thus,
    a typical order of decorators is async_request, request, return_reply,
    gen.coroutine.
    """
    @six.wraps(func)
    def wrapper(self, req, msg):
        # Note that this will run the coroutine until it first yields. The initial
        # code until the first yield thus happens synchronously.
        future = func(self, req, msg)
        @gen.coroutine
        def callback():
            with REQUEST_TIME.labels(msg.name).time():
                try:
                    reply = yield future
                    req.reply_with_message(reply)
                except FailReply as error:
                    reason = str(error)
                    self._logger.error('Request %s FAIL: %s', msg.name, reason)
                    req.reply('fail', reason)
                except asyncio.CancelledError:
                    self._logger.error('Request %s CANCELLED', msg.name)
                    req.reply('fail', 'request was cancelled')
                except Exception:
                    reply = self.create_exception_reply_and_log(msg, sys.exc_info())
                    req.reply_with_message(reply)
        self.ioloop.add_callback(callback)
        raise AsyncReply
    return wrapper


def time_request(func):
    """Decorator to record request servicing time as a Prometheus histogram.

    Use this on the outside of ``@request``.

    .. note::
        Only suitable for use on synchronous request handlers. Asynchronous
        handlers get the functionality as part of :func:`async_request`.
    """
    @six.wraps(func)
    def wrapper(self, req, msg):
        with REQUEST_TIME.labels(msg.name).time():
            return func(self, req, msg)
    return wrapper


class SDPControllerServer(AsyncDeviceServer):

    VERSION_INFO = ("sdpcontroller", 1, 1)
    BUILD_INFO = ("sdpcontroller",) + tuple(katsdpcontroller.__version__.split('.', 1)) + ('',)

    def __init__(self, host, port, sched, loop, safe_multicast_cidr,
                 simulate=False, develop=False, interface_mode=False, wrapper=None,
                 graph_resolver=None, image_resolver_factory=None,
                 gui_urls=None, graph_dir=None):
         # setup sensors
        self._build_state_sensor = Sensor(Sensor.STRING, "build-state", "SDP Controller build state.", "")
        self._api_version_sensor = Sensor(Sensor.STRING, "api-version", "SDP Controller API version.", "")
        self._device_status_sensor = Sensor(Sensor.DISCRETE, "device-status", "Devices status of the SDP Master Controller", "", ["ok", "degraded", "fail"])
        self._gui_urls_sensor = Sensor(Sensor.STRING, "gui-urls", "Links to associated GUIs", "")
        self._fmeca_sensors = {}
        self._fmeca_sensors['FD0001'] = Sensor(Sensor.BOOLEAN, "fmeca.FD0001", "Sub-process limits", "")
         # example FMECA sensor. In this case something to keep track of issues arising from launching to many processes.
         # TODO: Add more sensors exposing resource usage and currently executing graphs
        self._ntp_sensor = CallbackSensor(Sensor.BOOLEAN, "time-synchronised","SDP Controller container (and host) is synchronised to NTP", "")

        self.simulate = simulate
        if self.simulate: logger.warning("Note: Running in simulation mode. This will simulate certain external components such as the CBF.")
        self.develop = develop
        if self.develop:
            logger.warning("Note: Running in developer mode. This will relax some constraints.")
        self.interface_mode = interface_mode
        if self.interface_mode:
            logger.warning("Note: Running master controller in interface mode. This allows testing of the interface only, no actual command logic will be enacted.")
        self.wrapper = wrapper
        if self.wrapper is not None:
            logger.warning('Note: Using wrapper %s in all containers. This may alter behaviour.',
                           self.wrapper)
        self.loop = loop
        self.sched = sched
        self.components = {}
         # dict of currently managed SDP components
        self.gui_urls = gui_urls if gui_urls is not None else []
        self.graph_dir = graph_dir

        if graph_resolver is None:
            graph_resolver = GraphResolver()
        self.graph_resolver = graph_resolver
        if image_resolver_factory is None:
            image_resolver_factory = scheduler.ImageResolver
        self.image_resolver_factory = image_resolver_factory

        logger.debug("Building initial resource pool")
        self.resources = SDPCommonResources(safe_multicast_cidr)
         # create a new resource pool.

        self.subarray_products = {}
         # dict of currently configured SDP subarray_products
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
        self._gui_urls_sensor.set_value(json.dumps(self.gui_urls))
        self.add_sensor(self._gui_urls_sensor)

        self._ntp_sensor.set_value('0')
        self._ntp_sensor.set_read_callback(self._check_ntp_status)
        self.add_sensor(self._ntp_sensor)

          # until we know any better, failure modes are all inactive
        for s in self._fmeca_sensors.values():
            s.set_value(0)
            self.add_sensor(s)

    def _check_ntp_status(self):
        try:
            return (subprocess.check_output(["/usr/bin/ntpq","-p"]).find('*') > 0 and '1' or '0', Sensor.NOMINAL, time.time())
        except OSError:
            return ('0', Sensor.NOMINAL, time.time())

    @time_request
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

    @asyncio.coroutine
    def deregister_product(self, product, force=False):
        """Deregister a subarray product and remove it form the list of products.

        Raises
        ------
        FailReply
            if an asynchronous operation is in progress on the subarray product and
            `force` is false.
        """
        yield from product.deconfigure(force=force)

    @asyncio.coroutine
    def deconfigure_product(self, subarray_product_id, force=False):
        """Deconfigure a subarray product in response to a request.

        The difference between this method and :meth:`deregister_product` is
        that this method takes the subarray product by name.

        Raises
        ------
        FailReply
            if an asynchronous operation is is progress on the subarray product and
            `force` is false.
        FailReply
            if `subarray_product_id` does not exist
        """
        try:
            product = self.subarray_products[subarray_product_id]
        except KeyError:
            raise FailReply("Deconfiguration of subarray product {} requested, "
                            "but no configuration found.".format(subarray_product_id))
        yield from self.deregister_product(product, force)

    @asyncio.coroutine
    def configure_product(self, req, subarray_product_id, config):
        """Configure a subarray product in response to a request.

        Raises
        ------
        FailReply
            if a configure/deconfigure is in progress
        FailReply
            If any of the following occur
            - The specified subarray product id already exists, but the config differs from that specified
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
        def remove_product(product):
            # Protect against potential race conditions
            if self.subarray_products.get(product.subarray_product_id) is product:
                del self.subarray_products[product.subarray_product_id]
                self.remove_sensor(product.state_sensor)
                self.remove_sensor(product.capture_block_sensor)
                logger.info("Deconfigured subarray product {}".format(product.subarray_product_id))

        if subarray_product_id in self.override_dicts:
            odict = self.override_dicts.pop(subarray_product_id)
             # this is a use-once set of overrides
            logger.warning("Setting overrides on {} for the following: {}".format(subarray_product_id, odict))
            config = product_config.override(config, odict)
            # Re-validate, since the override may have broken it
            try:
                product_config.validate(config)
            except (ValueError, jsonschema.ValidationError) as error:
                retmsg = "Overrides make the config invalid: {}".format(error)
                logger.error(retmsg)
                raise FailReply(retmsg)

        if subarray_product_id.endswith('*'):
            # Requested a unique name. NB: it is important not to yield
            # between here and assigning the new product into
            # self.subarray_products, as doing so would introduce a race
            # condition.
            seq = 0
            base = subarray_product_id[:-1]
            while base + str(seq) in self.subarray_products:
                seq += 1
            subarray_product_id = base + str(seq)
        elif subarray_product_id in self.subarray_products:
            dp = self.subarray_products[subarray_product_id]
            if dp.config == config:
                logger.info("Subarray product with this configuration already exists. Pass.")
                return subarray_product_id
            else:
                raise FailReply("A subarray product with this id ({0}) already exists, but has a "
                                "different configuration. Please deconfigure this product or "
                                "choose a new product id to continue.".format(subarray_product_id))

        logger.debug('config is %s', json.dumps(config, indent=2, sort_keys=True))
        logger.info("Launching graph {}.".format(subarray_product_id))
        req.inform("Starting configuration of new product {}. This may take a few minutes..."
            .format(subarray_product_id))

        image_tag = config['config'].get('image_tag')
        if image_tag is not None:
            resolver_factory_args=dict(tag=image_tag)
        else:
            resolver_factory_args={}
        resolver = scheduler.Resolver(self.image_resolver_factory(**resolver_factory_args),
                                      scheduler.TaskIDAllocator(subarray_product_id + '-'),
                                      self.sched.http_url if self.sched else '')
        resolver.service_overrides = config['config'].get('service_overrides', {})
        resolver.telstate = None

        # create graph object and build physical graph from specified resources
        if self.interface_mode:
            product_cls = SDPSubarrayProductInterface
        else:
            product_cls = SDPSubarrayProduct
        product = product_cls(self.sched, config, resolver, subarray_product_id,
                              self.loop, self)
        if self.graph_dir is not None:
            product.write_graphs(self.graph_dir)
        # Speculatively put the product into the list of products, to prevent
        # a second configuration with the same name, and to allow a forced
        # deconfigure to cancel the configure.
        self.subarray_products[subarray_product_id] = product
        self.add_sensor(product.state_sensor)
        self.add_sensor(product.capture_block_sensor)
        product.dead_callbacks.append(remove_product)
        try:
            yield from product.configure(req)
        except Exception:
            remove_product(product)
            raise
        return subarray_product_id

    @asyncio.coroutine
    def deconfigure_on_exit(self):
        """Try to shutdown as gracefully as possible when interrupted."""
        logger.warning("SDP Master Controller interrupted - deconfiguring existing products.")
        for subarray_product_id, product in list(self.subarray_products.items()):
            try:
                yield from self.deregister_product(product, force=True)
            except Exception:
                logger.warning("Failed to deconfigure product %s during master controller exit. "
                               "Forging ahead...", subarray_product_id, exc_info=True)

    @asyncio.coroutine
    def async_stop(self):
        super(SDPControllerServer, self).stop()
        # TODO: set a flag to prevent new async requests being entertained
        yield from self.deconfigure_on_exit()
        if self.sched is not None:
            yield from self.sched.close()

    @time_request
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
            The ID of the subarray product to set overrides for in the form <subarray_name>_<data_product_name>.
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

    @gen.coroutine
    def _product_reconfigure(self, req, req_msg, subarray_product_id):
        logger.info("?product-reconfigure called on {}".format(subarray_product_id))
        try:
            product = self.subarray_products[subarray_product_id]
        except KeyError:
            raise FailReply("The specified subarray product id {} has no existing configuration and thus cannot be reconfigured.".format(subarray_product_id))
        config = product.config

        logger.info("Deconfiguring {} as part of a reconfigure request".format(subarray_product_id))
        try:
            yield to_tornado_future(self.deregister_product(product), loop=self.loop)
        except Exception as error:
            msg = "Unable to deconfigure as part of reconfigure"
            logger.error(msg, exc_info=True)
            raise FailReply("{}. {}".format(msg, error))

        logger.info("Waiting for {} to disappear".format(subarray_product_id))
        yield to_tornado_future(product.dead_event.wait(), loop=self.loop)

        logger.info("Issuing new configure for {} as part of reconfigure request.".format(subarray_product_id))
        try:
            yield to_tornado_future(self.configure_product(req, subarray_product_id, config),
                                    loop=self.loop)
        except Exception as error:
            msg = "Unable to configure as part of reconfigure, original array deconfigured"
            logger.error(msg, exc_info=True)
            raise FailReply("{}. {}".format(msg, error))

        raise gen.Return(('ok', ''))

    @async_request
    @request(Str(), include_msg=True)
    @return_reply(Str())
    @gen.coroutine
    def request_product_reconfigure(self, req, req_msg, subarray_product_id):
        """Reconfigure the specified SDP subarray product instance.

           The primary use of this command is to restart the SDP components for a particular
           subarray without having to reconfigure the rest of the system.

           Essentially this runs a deconfigure() followed by a configure() with the same parameters as originally
           specified via the product-configure katcp request.

           Request Arguments
           -----------------
           subarray_product_id : string
             The ID of the subarray product to reconfigure.

        """
        ret = yield self._product_reconfigure(req, req_msg, subarray_product_id)
        raise gen.Return(ret)

    # Backwards-compatibility alias
    @async_request
    @request(Str(), include_msg=True)
    @return_reply(Str())
    @gen.coroutine
    def request_data_product_reconfigure(self, req, req_msg, subarray_product_id):
        ret = yield self._product_reconfigure(req, req_msg, subarray_product_id)
        raise gen.Return(ret)

    request_data_product_reconfigure.__doc__ = request_product_reconfigure.__doc__

    @async_request
    @request(Str(optional=True),Str(optional=True),Int(min=1,max=65535,optional=True),Float(optional=True),Int(min=0,max=16384,optional=True),Str(optional=True),include_msg=True)
    @return_reply(Str())
    @gen.coroutine
    def request_data_product_configure(self, req, req_msg, subarray_product_id, antennas, n_channels, dump_rate, n_beams, stream_sources):
        """Configure a SDP subarray product instance (legacy interface).

        A subarray product instance is comprised of a telescope state, a
        collection of containers running required SDP services, and a
        networking configuration appropriate for the required data movement.

        On configuring a new product, several steps occur:
         * Build initial static configuration. Includes elements such as IP
           addresses of deployment machines, multicast subscription details etc
         * Launch a new Telescope State Repository (redis instance) for this
           product and copy in static config.
         * Launch service containers as described in the static configuration.
         * Verify all services are running and reachable.


        Request Arguments
        -----------------
        subarray_product_id : string
            The ID to use for this subarray product, in the form
            <subarray_name>_<data_product_name>.
        antennas : string
            A comma-separated list of antenna names to use in this subarray
            product. These will be matched to the CBF output and used to pull
            only the specific data. If antennas is "0" or "", then this subarray
            product is de-configured. Trailing arguments can be omitted.
        n_channels : int
            Number of channels used in this subarray product (based on CBF config)
        dump_rate : float
            Dump rate of subarray product in Hz
        n_beams : int
            Number of beams in the subarray product
            (0 = Correlator output, 1+ = Beamformer)
        stream_sources: string
            A JSON dict of the form {<type>: {<name>: <url>, ...}, ...}
            These stream specifiers are used directly by the graph to configure
            the SDP system and thus rely on the stream_name as a key

        Returns
        -------
        success : {'ok', 'fail'}
        """
        logger.info("?data-product-configure called with: {}".format(req_msg))
         # INFO for now, but should be DEBUG post integration
        if antennas is None:
            if subarray_product_id is None:
                for (subarray_product_id, subarray_product) in self.subarray_products.items():
                    req.inform(subarray_product_id,subarray_product)
                raise gen.Return(('ok', "%i" % len(self.subarray_products)))
            elif subarray_product_id in self.subarray_products:
                raise gen.Return(('ok', "%s is currently configured: %s" %
                        (subarray_product_id, repr(self.subarray_products[subarray_product_id]))))
            else:
                raise FailReply("This subarray product id has no current configuration.")

        if antennas == "0" or antennas == "":
            req.inform("Starting deconfiguration of {}. This may take a few minutes...".format(subarray_product_id))
            yield to_tornado_future(self.deconfigure_product(subarray_product_id), loop=self.loop)
            raise gen.Return(('ok', ''))

        logger.info("Using '{}' as antenna mask".format(antennas))
        antennas = antennas.replace(" ",",")
         # temp hack to make sure we have a comma delimited set of antennas
        antennas = antennas.split(',')

        # all good so far, lets check arguments for validity
        if not(antennas and n_channels >= 0 and dump_rate >= 0 and n_beams >= 0 and stream_sources):
            raise FailReply("You must specify antennas, n_channels, dump_rate, n_beams and appropriate spead stream sources to configure a subarray product")

        graph_name = self.graph_resolver(subarray_product_id)
        try:
            streams_dict = json.loads(stream_sources)
            config = product_config.convert(graph_name, streams_dict, antennas, dump_rate,
                                            self.simulate, self.develop, self.wrapper)
        except (ValueError, jsonschema.ValidationError) as error:
             # something is definitely wrong with these
            retmsg = "Failed to process source stream specifiers: {}".format(error)
            logger.error(retmsg)
            raise FailReply(retmsg)

        yield to_tornado_future(self.configure_product(req, subarray_product_id, config),
                                loop=self.loop)
        raise gen.Return(('ok', ''))

    @async_request
    @request(Str(), Str(), include_msg=True)
    @return_reply(Str())
    @gen.coroutine
    def request_product_configure(self, req, req_msg, subarray_product_id, config):
        """Configure a SDP subarray product instance.

        A subarray product instance is comprised of a telescope state, a
        collection of containers running required SDP services, and a
        networking configuration appropriate for the required data movement.

        On configuring a new product, several steps occur:
         * Build initial static configuration. Includes elements such as IP
           addresses of deployment machines, multicast subscription details etc
         * Launch a new Telescope State Repository (redis instance) for this
           product and copy in static config.
         * Launch service containers as described in the static configuration.
         * Verify all services are running and reachable.

        Request Arguments
        -----------------
        subarray_product_id : string
            The ID to use for this product (an arbitrary string, with
            characters A-Z, a-z, 0-9 and _). It may optionally be
            suffixed with a "*" to request that a unique name is generated
            by replacing the "*" with a suffix.
        config : string
            A JSON-encoded dictionary of configuration data.

        Returns
        -------
        success : {'ok', 'fail'}
        name : str
            Actual subarray-product-id
        """
        logger.info("?product-configure called with: {}".format(req_msg))

        if not re.match('^[A-Za-z0-9_]+\*?$', subarray_product_id):
            raise FailReply('Subarray_product_id contains illegal characters')
        try:
            config_dict = json.loads(config)
            product_config.validate(config_dict)
        except (ValueError, jsonschema.ValidationError) as error:
            retmsg = "Failed to process config: {}".format(error)
            logger.error(retmsg)
            raise FailReply(retmsg)

        subarray_product_id = yield to_tornado_future(
            self.configure_product(req, subarray_product_id, config_dict), loop=self.loop)
        raise gen.Return(('ok', subarray_product_id))

    @async_request
    @request(Str(), Bool(optional=True, default=False))
    @return_reply()
    @gen.coroutine
    def request_product_deconfigure(self, req, subarray_product_id, force=False):
        """Deconfigure an existing subarray product.

        Parameters
        ----------
        subarray_product_id : string
            Subarray product to deconfigure
        force : bool, optional
            Take down the subarray immediately, even if it is still capturing,
            and without waiting for completion.

        Returns
        -------
        success : {'ok', 'fail'}
        """
        req.inform("Starting deconfiguration of {}. This may take a few minutes...".format(subarray_product_id))
        yield to_tornado_future(self.deconfigure_product(subarray_product_id, force),
                                loop=self.loop)
        raise gen.Return(('ok',))

    @request(Str(optional=True))
    @return_reply(Int())
    def request_product_list(self, req, subarray_product_id):
        """List existing subarray products

        Parameters
        ----------
        subarray_product_id : string, optional
            If specified, report on only this subarray product ID

        Returns
        -------
        success : {'ok', 'fail'}

        num_informs : integer
            Number of subarray products listed
        """
        if subarray_product_id is None:
            for (subarray_product_id, subarray_product) in self.subarray_products.items():
                req.inform(subarray_product_id, subarray_product)
            return ('ok', len(self.subarray_products))
        elif subarray_product_id in self.subarray_products:
            req.inform(subarray_product_id, self.subarray_products[subarray_product_id])
            return ('ok', 1)
        else:
            raise FailReply("This product id has no current configuration.")

    @async_request
    @request(Str(), Str(optional=True))
    @return_reply(Str())
    @gen.coroutine
    def request_capture_init(self, req, subarray_product_id, program_block_id=None):
        """Request capture of the specified subarray product to start.

        Note: This command is used to prepare the SDP for reception of data
        as specified by the subarray product provided. It is necessary to call this
        command before issuing a start command to the CBF. Essentially the SDP
        will, once this command has returned 'OK', be in a wait state until
        reception of the stream control start packet.

        Request Arguments
        -----------------
        subarray_product_id : string
            The ID of the subarray product to initialise. This must have already been
            configured via the product-configure command.
        program_block_id : string
            The ID of the program block being started.

        Returns
        -------
        success : {'ok', 'fail'}
            Whether the system is ready to capture or not.
        """
        if subarray_product_id not in self.subarray_products:
            raise FailReply('No existing subarray product configuration with this id found')
        sa = self.subarray_products[subarray_product_id]
        yield to_tornado_future(sa.capture_init(program_block_id), loop=self.loop)
        raise gen.Return(('ok','SDP ready'))

    @time_request
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
            for (subarray_product_id,subarray_product) in self.subarray_products.items():
                req.inform(subarray_product_id, subarray_product.telstate_endpoint)
            return ('ok',"%i" % len(self.subarray_products))

        if subarray_product_id not in self.subarray_products:
            return ('fail','No existing subarray product configuration with this id found')
        return ('ok',self.subarray_products[subarray_product_id].telstate_endpoint)

    @time_request
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
            for (subarray_product_id,subarray_product) in self.subarray_products.items():
                req.inform(subarray_product_id,subarray_product.state.name)
            return ('ok',"%i" % len(self.subarray_products))

        if subarray_product_id not in self.subarray_products:
            return ('fail','No existing subarray product configuration with this id found')
        return ('ok',self.subarray_products[subarray_product_id].state.name)

    @async_request
    @request(Str())
    @return_reply(Str())
    @gen.coroutine
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
            raise FailReply('No existing subarray product configuration with this id found')
        sa = self.subarray_products[subarray_product_id]
        yield to_tornado_future(sa.capture_done(), loop=self.loop)
        raise gen.Return(('ok', 'capture complete'))

    @async_request
    @request()
    @return_reply(Str())
    @gen.coroutine
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

        yield to_tornado_future(self.deconfigure_on_exit(), loop=self.loop)
         # attempt to deconfigure any existing subarrays
         # will always succeed even if some deconfigure fails
        try:
            master, slaves = yield to_tornado_future(
                self.sched.get_master_and_slaves(timeout=5), loop=self.loop)
        except Exception as error:
            logger.error('Failed to get list of slaves, so not powering them off.', exc_info=True)
            raise FailReply('could not get a list of slaves to power off')
        # If for some reason two slaves are running on the same machine, do
        # not kill it twice.
        slaves = list(set(slaves))
        logger.info('Preparing to kill slaves: %s', ','.join(slaves))
        # Build a graph to power off each slave
        logical_graph = networkx.MultiDiGraph()
        for slave in slaves:
            logical_graph.add_node(tasks.PoweroffLogicalTask(slave))
        physical_graph = scheduler.instantiate(logical_graph, self.loop)

        # We want to avoid killing either the Mesos master or ourselves too
        # early (usually but not always the same machine). Make a list of IP
        # addresses for which we want to delay powering off.
        master_addresses = set()
        for interface in netifaces.interfaces():
            ifaddresses = netifaces.ifaddresses(interface)
            addresses = ifaddresses.get(netifaces.AF_INET, []) + \
                        ifaddresses.get(netifaces.AF_INET6, [])
            for entry in addresses:
                address = entry.get('addr', '')
                if address:
                    master_addresses.add(address)
        for (family, (address, port)) in (yield tornado.netutil.Resolver().resolve(master, 0)):
            master_addresses.add(address)
        logger.debug('Master IP addresses: %s', ','.join(master_addresses))
        non_master = []
        for node in physical_graph:
            is_master = False
            addresses = yield tornado.netutil.Resolver().resolve(node.logical_node.host, 0)
            for (family, (address, port)) in addresses:
                if address in master_addresses:
                    is_master = True
                    break
            if not is_master:
                non_master.append(node)

        resolver = scheduler.Resolver(self.image_resolver_factory(),
                                      scheduler.TaskIDAllocator('poweroff-'),
                                      self.sched.http_url)
        # Shut down everything except the master/self
        yield to_tornado_future(
            self.sched.launch(physical_graph, resolver, non_master), loop=self.loop)
        # Shut down the rest
        yield to_tornado_future(
            self.sched.launch(physical_graph, resolver), loop=self.loop)
        # Check for failures. This won't detect everything (it's possible that
        # the task will start running then fail), but handles cases like the
        # agent having disappeared under us or failing to pull the image.
        response = []
        for node in physical_graph:
            status = '(failed)'
            if node.state >= scheduler.TaskState.RUNNING and \
                    node.state <= scheduler.TaskState.READY:
                status = '(shutting down)'
            elif node.state == scheduler.TaskState.DEAD and node.status.state == 'TASK_FINISHED':
                status = ''
            response.append(node.logical_node.host + status)
        raise gen.Return(('ok', ','.join(response)))

    @time_request
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


class InterfaceModeSensors(object):
    def __init__(self, subarray_product_id):
        """Manage dummy subarray product sensors on a SDPControllerServer instance

        Parameters
        ----------
        subarray_product_id : str
            Subarray product id, e.g. `array_1_c856M4k`

        """
        self.subarray_product_id = subarray_product_id
        self.sensors = {}

    def add_sensors(self, server):
        """Add dummy subarray product sensors and issue #interface-changed"""

        interface_sensor_params = {
            'bf_ingest.beamformer.1.port': dict(
                default=("ing1.sdp.mkat.fake.kat.ac.za", 31048),
                sensor_type=Sensor.ADDRESS,
                description='IP endpoint for port',
                initial_status=Sensor.NOMINAL),
            'filewriter.sdp_l0.1.filename': dict(
                default='/var/kat/data/148966XXXX.h5',
                sensor_type=Sensor.STRING,
                description='Final name for file being captured',
                initial_status=Sensor.NOMINAL),
            'ingest.sdp_l0.1.capture-active': dict(
                default=False,
                sensor_type=Sensor.BOOLEAN,
                description='Is there a currently active capture session.',
                initial_status=Sensor.NOMINAL),
            'timeplot.sdp_l0.1.gui-urls': dict(
                default='[{"category": "Plot", '
                '"href": "http://ing1.sdp.mkat.fake.kat.ac.za:31054/", '
                '"description": "Signal displays for array_1_bc856M4k", '
                '"title": "Signal Display"}]',
                sensor_type=Sensor.STRING,
                description='URLs for GUIs',
                initial_status=Sensor.NOMINAL),
            'timeplot.sdp_l0.1.html_port': dict(
                default=("ing1.sdp.mkat.fake.kat.ac.za", 31054),
                sensor_type=Sensor.ADDRESS,
                description='IP endpoint for html_port',
                initial_status=Sensor.NOMINAL),
            'cal.sdp_l0.1.capture-block-state': dict(
                default='{}',
                sensor_type=Sensor.STRING,
                description='JSON dict with the state of each capture block',
                initial_status=Sensor.NOMINAL)
        }

        sensors_added = False
        try:
            for postfix, sensor_params in interface_sensor_params.items():
                sensor_name = self.subarray_product_id + '.' + postfix
                if sensor_name in self.sensors:
                    logger.info('Simulated sensor %r already exists, skipping',
                                sensor_name)
                    continue
                sensor_params['name'] = sensor_name
                sensor = Sensor(**sensor_params)
                self.sensors[sensor_name] = sensor
                server.add_sensor(sensor)
                sensors_added = True
        finally:
            if sensors_added:
                server.mass_inform(Message.inform('interface-changed', 'sensor-list'))

    def remove_sensors(self, server):
        """Remove dummy subarray product sensors and issue #interface-changed"""
        sensors_removed = False
        try:
            for sensor_name, sensor in list(self.sensors.items()):
                server.remove_sensor(sensor)
                del self.sensors[sensor_name]
                sensors_removed = True
        finally:
            if sensors_removed:
                server.mass_inform(Message.inform('interface-changed', 'sensor-list'))
