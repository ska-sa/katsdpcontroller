"""Core classes for the SDP Controller."""

import time
import logging
import subprocess
import json
import signal
import re
import os.path
from collections import deque
import asyncio
import functools
import enum
import ipaddress
import faulthandler

import networkx
import networkx.drawing.nx_pydot

import jsonschema
import netifaces

from prometheus_client import Histogram

from aiokatcp import DeviceServer, Sensor, FailReply, Address
import katsdpcontroller
import katsdptelstate
from . import scheduler, tasks, product_config, generator, schemas
from .tasks import CaptureBlockState, DEPENDS_INIT


faulthandler.register(signal.SIGUSR2, all_threads=True)


BATCH_PRIORITY = 1        #: Scheduler priority for batch queues
REQUEST_TIME = Histogram(
    'katsdpcontroller_request_time_seconds', 'Time to process katcp requests', ['request'],
    buckets=(0.001, 0.01, 0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0, 600.0))
logger = logging.getLogger("katsdpcontroller.katsdpcontroller")
_capture_block_names = set()      #: all capture block names used


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


def _load_s3_config(filename):
    with open(filename, 'r') as f:
        config = json.load(f)
    schemas.S3_CONFIG.validate(config)
    return config


def _redact_arg(arg, s3_config):
    """Process one argument for _redact_keys"""
    for mode in ['read', 'write']:
        for name in ['access_key', 'secret_key']:
            key = s3_config[mode][name]
            if arg == key or arg.endswith('=' + key):
                return arg[:-len(key)] + 'REDACTED'
    return arg


def _redact_keys(taskinfo, s3_config):
    """Return a copy of a Mesos TaskInfo with command-line secret keys redacted.

    This is intended for putting the taskinfo into telstate without revealing
    secrets. Any occurrences of the secrets in s3_config in a command-line
    argument are replaced by REDACTED.

    It will handle both '--secret=foo' and '--secret foo'.

    .. note::

        While the original `taskinfo` is not modified, the copy is not a full deep copy.

    Parameters
    ----------
    taskinfo : :class:`addict.Dict`
        Taskinfo structure

    Returned
    --------
    redacted : :class:`addict.Dict`
        Copy of `taskinfo` with secrets redacted
    """
    taskinfo = taskinfo.copy()
    if taskinfo.command.arguments:
        taskinfo.command = taskinfo.command.copy()
        taskinfo.command.arguments = [_redact_arg(arg, s3_config)
                                      for arg in taskinfo.command.arguments]
    return taskinfo


class ProductState(scheduler.OrderedEnum):
    """State of a subarray.

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


class MulticastIPResources:
    def __init__(self, network):
        self._hosts = network.hosts()

    def get_ip(self, n_addresses):
        try:
            ip = str(next(self._hosts))
            if n_addresses > 1:
                for _ in range(1, n_addresses):
                    next(self._hosts)
                ip = '{}+{}'.format(ip, n_addresses - 1)
            return ip
        except StopIteration:
            raise RuntimeError('Multicast IP addresses exhausted')


class SDPCommonResources:
    """Assigns multicast groups and ports across all subarrays."""
    def __init__(self, safe_multicast_cidr):
        logger.info("Using %s for multicast subnet allocation", safe_multicast_cidr)
        self.multicast_subnets = deque(
            ipaddress.ip_network(safe_multicast_cidr).subnets(new_prefix=24))


class SDPResources:
    """Helper class to allocate resources for a single subarray-product."""
    def __init__(self, common, subarray_product_id):
        self.subarray_product_id = subarray_product_id
        self._common = common
        try:
            self._subnet = self._common.multicast_subnets.popleft()
        except IndexError:
            raise RuntimeError("Multicast subnets exhausted")
        logger.info("Using %s for %s", self._subnet, subarray_product_id)
        self._multicast_resources = MulticastIPResources(self._subnet)

    def get_multicast_ip(self, n_addresses):
        """Assign multicast addresses for a group."""
        return self._multicast_resources.get_ip(n_addresses)

    @staticmethod
    def get_port():
        """Return an assigned port for a multicast group"""
        return 7148

    def close(self):
        if self._subnet is not None:
            self._common.multicast_subnets.append(self._subnet)
            self._subnet = None
            self._multicast_resources = None


class CaptureBlock:
    """A capture block is book-ended by a capture-init and a capture-done,
    although processing on it continues after the capture-done."""

    def __init__(self, name, loop):
        self.name = name
        self._state = CaptureBlockState.INITIALISING
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
            if value == CaptureBlockState.DEAD:
                self.dead_event.set()
            if self.state_change_callback is not None:
                self.state_change_callback()


class SDPSubarrayProductBase:
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
    - Keys in :attr:`capture_blocks` also appear in
      :py:data:`_capture_block_names`.
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
        self.postprocess_logical_graph = generator.build_postprocess_logical_graph(config)
        self.telstate_endpoint = ""
        self.telstate = None
        self.capture_blocks = {}              # live capture blocks, indexed by name
        self.current_capture_block = None     # set between capture_init and capture_done
        self.dead_event = asyncio.Event(loop=loop)   # Set when reached state DEAD
        # Callbacks that are called when we reach state DEAD. These are
        # provided in addition to dead_event, because sometimes it's
        # necessary to react immediately rather than waiting for next time
        # around the event loop. Each callback takes self as the argument.
        self.dead_callbacks = [lambda product: product.dead_event.set()]
        self._state = None
        self.capture_block_sensor = Sensor(
            str, subarray_product_id + ".capture-block-state",
            "JSON dictionary of capture block states for active capture blocks",
            default="{}", initial_status=Sensor.Status.NOMINAL)
        self.state_sensor = Sensor(
            ProductState, subarray_product_id + ".state",
            "State of the subarray product state machine")
        self.state = ProductState.CONFIGURING   # This sets the sensor
        self.logger = logging.LoggerAdapter(logger, dict(subarray_product_id=subarray_product_id))
        self.logger.info("Created: %r", self)

    @property
    def state(self):
        return self._state

    @state.setter
    def state(self, value):
        self._state = value
        self.state_sensor.value = value

    @property
    def async_busy(self):
        """Whether there is an asynchronous state-change operation in progress."""
        return self._async_task is not None and not self._async_task.done()

    def _fail_if_busy(self):
        """Raise a FailReply if there is an asynchronous operation in progress."""
        if self.async_busy:
            raise FailReply('Subarray product {} is busy with an operation. '
                            'Please wait for it to complete first.'.format(
                                self.subarray_product_id))

    async def configure_impl(self, ctx):
        """Extension point to configure the subarray."""
        pass

    async def deconfigure_impl(self, force, ready):
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

    async def capture_init_impl(self, capture_block):
        """Extension point to start a capture block."""
        pass

    async def capture_done_impl(self, capture_block):
        """Extension point to stop a capture block.

        This should only do the work needed for the ``capture-done`` master
        controller request to return. The caller takes care of calling
        :meth:`postprocess_impl`.

        It needs to be safe to run from DECONFIGURING state, because it may be
        run as part of forced deconfigure.
        """
        pass

    async def postprocess_impl(self, capture_block):
        """Complete the post-processing for a capture block.

        Subclasses should override this if a capture block is not finished when
        :meth:`_capture_done` returns.
        """
        pass

    async def _configure(self, ctx):
        """Asynchronous task that does he configuration."""
        await self.configure_impl(ctx)
        self.state = ProductState.IDLE

    async def _deconfigure(self, force, ready):
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
                await self.capture_done_impl(capture_block)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger.exception("Failed to issue capture-done during shutdown request. "
                                      "Will continue with graph shutdown.")

        if force:
            for capture_block in list(self.capture_blocks.values()):
                if capture_block.postprocess_task is not None:
                    self.logger.warning('Cancelling postprocessing for capture block %s',
                                        capture_block.name)
                    capture_block.postprocess_task.cancel()
                else:
                    self._capture_block_dead(capture_block)

        await self.deconfigure_impl(force, ready)

        # Allow all the postprocessing tasks to finish up
        # Note: this needs to be done carefully, because self.capture_blocks
        # can change during the yield.
        while self.capture_blocks:
            name, capture_block = next(iter(self.capture_blocks.items()))
            logging.info('Waiting for capture block %s to terminate', name)
            await capture_block.dead_event.wait()
            self.capture_blocks.pop(name, None)

        self.state = ProductState.DEAD
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
        capture_block.state = CaptureBlockState.DEAD

    def _update_capture_block_sensor(self):
        value = {name: capture_block.state.name.lower()
                 for name, capture_block in self.capture_blocks.items()}
        self.capture_block_sensor.set_value(json.dumps(value, sort_keys=True))

    async def _capture_init(self, capture_block):
        self.capture_blocks[capture_block.name] = capture_block
        capture_block.state_change_callback = self._update_capture_block_sensor
        # Update the sensor with the INITIALISING state
        self._update_capture_block_sensor()
        try:
            await self.capture_init_impl(capture_block)
        except Exception:
            self._capture_block_dead(capture_block)
            raise
        assert self.current_capture_block is None
        self.state = ProductState.CAPTURING
        self.current_capture_block = capture_block
        capture_block.state = CaptureBlockState.CAPTURING

    async def _capture_done(self):
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
        await self.capture_done_impl(capture_block)
        assert self.state == ProductState.CAPTURING
        assert self.current_capture_block is capture_block
        self.state = ProductState.IDLE
        self.current_capture_block = None
        capture_block.state = CaptureBlockState.POSTPROCESSING
        capture_block.postprocess_task = asyncio.ensure_future(
            self.postprocess_impl(capture_block), loop=self.loop)
        log_task_exceptions(
            capture_block.postprocess_task,
            "Exception in postprocessing for {}/{}".format(self.subarray_product_id,
                                                           capture_block.name))
        capture_block.postprocess_task.add_done_callback(
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

    async def _replace_async_task(self, new_task):
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
            await asyncio.wait([old_task], loop=self.loop)
        return self._async_task is new_task

    async def configure(self, ctx):
        assert not self.async_busy, "configure should be the first thing to happen"
        assert self.state == ProductState.CONFIGURING, \
            "configure should be the first thing to happen"
        task = asyncio.ensure_future(self._configure(ctx), loop=self.loop)
        log_task_exceptions(task, "Configuring subarray product {} failed".format(
            self.subarray_product_id))
        self._async_task = task
        try:
            await task
        finally:
            self._clear_async_task(task)
        self.logger.info('Subarray product %s successfully configured', self.subarray_product_id)

    async def deconfigure(self, force=False):
        """Start deconfiguration of the subarray, but does not wait for it to complete."""
        if self.state == ProductState.DEAD:
            return
        if self.async_busy:
            if not force:
                self._fail_if_busy()
            else:
                self.logger.warning('Subarray product %s is busy with an operation, '
                                    'but deconfiguring anyway', self.subarray_product_id)

        if self.state != ProductState.IDLE:
            if not force:
                raise FailReply('Subarray product is not idle and thus cannot be deconfigured. '
                                'Please issue capture_done first.')
            else:
                self.logger.warning('Subarray product %s is in state %s, but deconfiguring anyway',
                                    self.subarray_product_id, self.state.name)
        self.logger.info("Deconfiguring subarray product %s", self.subarray_product_id)

        ready = asyncio.Event(loop=self.loop)
        task = asyncio.ensure_future(self._deconfigure(force, ready), loop=self.loop)
        log_task_exceptions(task, "Deconfiguring {} failed".format(self.subarray_product_id))
        # Make sure that ready gets unblocked even if task throws.
        task.add_done_callback(lambda future: ready.set())
        task.add_done_callback(self._clear_async_task)
        if await self._replace_async_task(task):
            await ready.wait()
        # We don't wait for task to complete, but if it's already done we
        # pass back any exceptions.
        if task.done():
            await task

    async def capture_init(self):
        self._fail_if_busy()
        if self.state != ProductState.IDLE:
            raise FailReply('Subarray product {} is currently in state {}, not IDLE as expected. '
                            'Cannot be inited.'.format(self.subarray_product_id, self.state.name))
        # Find first unique capture block ID, starting from the current
        # timestamp (this protects against the unlikely case of two capture
        # blocks started in the same second, or oddities from clock warping).
        seq = int(time.time())
        while str(seq) in _capture_block_names:
            seq -= 1
        capture_block_id = str(seq)
        _capture_block_names.add(capture_block_id)
        self.logger.info('Using capture block ID %s', capture_block_id)

        capture_block = CaptureBlock(capture_block_id, self.loop)
        task = asyncio.ensure_future(self._capture_init(capture_block), loop=self.loop)
        self._async_task = task
        try:
            await task
        finally:
            self._clear_async_task(task)
        self.logger.info('Started capture block %s on subarray product %s',
                         capture_block_id, self.subarray_product_id)
        return capture_block_id

    async def capture_done(self):
        self._fail_if_busy()
        if self.state != ProductState.CAPTURING:
            raise FailReply('Subarray product is currently in state {}, not CAPTURING as expected. '
                            'Cannot be stopped.'.format(self.state.name))
        capture_block_id = self.current_capture_block.name
        task = asyncio.ensure_future(self._capture_done(), loop=self.loop)
        self._async_task = task
        try:
            await task
        finally:
            self._clear_async_task(task)
        self.logger.info('Finished capture block %s on subarray product %s',
                         capture_block_id, self.subarray_product_id)
        return capture_block_id

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
            except OSError as error:
                self.logger.warning('Could not write %s: %s', filename, error)

    def __repr__(self):
        return "Subarray product {} (State: {})".format(self.subarray_product_id, self.state.name)


class SDPSubarrayProductInterface(SDPSubarrayProductBase):
    """Dummy implementation of SDPSubarrayProductBase interface that does not
    actually run anything.
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._interface_mode_sensors = InterfaceModeSensors(self.subarray_product_id, self.logger)
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
                states = json.loads(sensor.value)
                if state is None:
                    states.pop(capture_block_id, None)
                else:
                    states[capture_block_id] = state
                sensor.set_value(json.dumps(states))

    async def capture_init_impl(self, capture_block):
        self._update_capture_block_state(capture_block.name, 'CAPTURING')

    async def capture_done_impl(self, capture_block):
        self._update_capture_block_state(capture_block.name, 'PROCESSING')

    async def postprocess_impl(self, capture_block):
        await asyncio.sleep(0.1, loop=self.loop)
        self._update_capture_block_state(capture_block.name, None)

    async def configure_impl(self, ctx):
        self.logger.warning("No components will be started - running in interface mode")
        # Add dummy sensors for this product
        self._interface_mode_sensors.add_sensors(self.sdp_controller)

    async def deconfigure_impl(self, force, ready):
        self._interface_mode_sensors.remove_sensors(self.sdp_controller)


class SDPSubarrayProduct(SDPSubarrayProductBase):
    """Subarray product that actually launches nodes."""
    def _instantiate(self, logical_node, capture_block_id):
        if isinstance(logical_node, tasks.SDPLogicalTask):
            return logical_node.physical_factory(
                logical_node, self.loop,
                self.sdp_controller, self.subarray_product_id, capture_block_id)
        return logical_node.physical_factory(logical_node, self.loop)

    def _instantiate_physical_graph(self, logical_graph, capture_block_id=None):
        mapping = {logical: self._instantiate(logical, capture_block_id)
                   for logical in logical_graph}
        return networkx.relabel_nodes(logical_graph, mapping)

    def __init__(self, sched, config, resolver, subarray_product_id, loop,
                 sdp_controller, telstate_name='telstate'):
        super().__init__(sched, config, resolver, subarray_product_id, loop, sdp_controller)
        # Priority is lower (higher number) than the default queue
        self.batch_queue = scheduler.LaunchQueue(subarray_product_id, priority=BATCH_PRIORITY)
        sched.add_queue(self.batch_queue)
        # generate physical nodes
        self.physical_graph = self._instantiate_physical_graph(self.logical_graph)
        # Nodes indexed by logical name
        self._nodes = {node.logical_node.name: node for node in self.physical_graph}
        self.telstate_node = self._nodes[telstate_name]

    def __del__(self):
        if hasattr(self, 'batch_queue'):
            self.sched.remove_queue(self.batch_queue)

    async def _exec_node_transition(self, node, reqs, deps, state, capture_block):
        try:
            if deps:
                await asyncio.gather(*deps, loop=node.loop)
            if reqs:
                if node.katcp_connection is None:
                    self.logger.warning(
                        'Cannot issue %s to %s because there is no katcp connection',
                        reqs[0], node.name)
                else:
                    # TODO: should handle katcp exceptions or failed replies
                    try:
                        for req in reqs:
                            await node.issue_req(req.name, req.args, timeout=req.timeout)
                    except FailReply:
                        pass   # Callee logs a warning
            if state == CaptureBlockState.DEAD and isinstance(node, tasks.SDPPhysicalTask):
                observer = node.capture_block_state_observer
                if observer is not None:
                    self.logger.info('Waiting for %s on %s', capture_block.name, node.name)
                    await observer.wait_capture_block_done(capture_block.name)
                    self.logger.info('Done waiting for %s on %s', capture_block.name, node.name)
                else:
                    self.logger.debug('Task %s has no capture-block-state observer', node.name)
        finally:
            if state == CaptureBlockState.DEAD and isinstance(node, tasks.SDPPhysicalTaskBase):
                node.remove_capture_block(capture_block)

    async def exec_transitions(self, state, reverse, capture_block):
        """Issue requests to nodes on state transitions.

        The requests are made in parallel, but respects `depends_init`
        dependencies in the graph.

        Parameters
        ----------
        state : :class:`~katsdpcontroller.tasks.CaptureBlockState`
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

        futures = {}     # Keyed by node
        # We grab the ioloop of the first task we create.
        loop = None
        # Lexicographical tie-breaking isn't strictly required, but it makes
        # behaviour predictable.
        now = time.time()   # Outside loop to be consistent across all nodes
        for node in networkx.lexicographical_topological_sort(deps_graph, key=lambda x: x.name):
            reqs = []
            try:
                reqs = node.get_transition(state)
            except AttributeError:
                # Not all nodes are SDPPhysicalTask
                pass
            if reqs:
                # Apply {} substitutions to request data
                subst = dict(capture_block_id=capture_block.name,
                             time=now)
                reqs = [req.format(**subst) for req in reqs]
            deps = [futures[trg] for trg in deps_graph.predecessors(node) if trg in futures]
            task = asyncio.ensure_future(
                self._exec_node_transition(node, reqs, deps, state, capture_block),
                loop=node.loop)
            loop = node.loop
            futures[node] = task
        if futures:
            await asyncio.gather(*futures.values(), loop=loop)

    async def capture_init_impl(self, capture_block):
        self.telstate.add('sdp_capture_block_id', capture_block.name)
        for node in self.physical_graph:
            if isinstance(node, tasks.SDPPhysicalTaskBase):
                node.add_capture_block(capture_block)
        await self.exec_transitions(CaptureBlockState.CAPTURING, True, capture_block)

    async def capture_done_impl(self, capture_block):
        await self.exec_transitions(CaptureBlockState.POSTPROCESSING, False, capture_block)

    async def postprocess_impl(self, capture_block):
        await self.exec_transitions(CaptureBlockState.DEAD, False, capture_block)

        physical_graph = self._instantiate_physical_graph(self.postprocess_logical_graph,
                                                          capture_block.name)
        nodes = {node.logical_node.name: node for node in physical_graph}
        telstate_node = nodes['telstate']
        telstate_node.host = self.telstate_node.host
        telstate_node.ports = dict(self.telstate_node.ports)
        batch = []
        for node in physical_graph:
            if isinstance(node, scheduler.PhysicalTask):
                coro = self.sched.batch_run(
                    physical_graph, self.resolver, [telstate_node, node], queue=self.batch_queue,
                    resources_timeout=7*86400, attempts=3)
                batch.append(coro)
        await asyncio.gather(*batch, loop=self.loop)

    async def _launch_telstate(self):
        """Make sure the telstate node is launched"""
        boot = [self.telstate_node]

        base_params = self.physical_graph.graph.get(
            'config', lambda resolver: {})(self.resolver)
        base_params['subarray_product_id'] = self.subarray_product_id
        base_params['sdp_config'] = self.config
        # Provide attributes to describe the relationships between CBF streams
        # and instruments. This could be extracted from sdp_config, but these
        # specific sensors are easier to mock.
        for name, stream in self.config['inputs'].items():
            if stream['type'].startswith('cbf.'):
                prefix = name + katsdptelstate.TelescopeState.SEPARATOR
                for suffix in ['src_streams', 'instrument_dev_name']:
                    if suffix in stream:
                        base_params[prefix + suffix] = stream[suffix]

        self.logger.debug("Launching telstate. Base parameters %s", base_params)
        await self.sched.launch(self.physical_graph, self.resolver, boot)
        # connect to telstate store
        self.telstate_endpoint = '{}:{}'.format(self.telstate_node.host,
                                                self.telstate_node.ports['telstate'])
        self.telstate = katsdptelstate.TelescopeState(endpoint=self.telstate_endpoint)
        self.resolver.telstate = self.telstate

        self.logger.debug("base params: %s", base_params)
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
                self.logger.warning('Task %s is in state %s instead of READY',
                                    node.name, node.state.name)
                return False
        return True

    async def _shutdown(self, force):
        try:
            # TODO: issue progress reports as tasks stop
            await self.sched.kill(self.physical_graph, force=force,
                                  capture_blocks=self.capture_blocks)
        finally:
            if hasattr(self.resolver, 'resources'):
                self.resolver.resources.close()

    async def configure_impl(self, ctx):
        try:
            try:
                resolver = self.resolver
                resolver.resources = SDPResources(self.sdp_controller.resources,
                                                  self.subarray_product_id)
                # launch the telescope state for this graph
                await self._launch_telstate()
                ctx.inform("Telstate launched. [{}]".format(self.telstate_endpoint))
                # launch containers for those nodes that require them
                await self.sched.launch(self.physical_graph, self.resolver)
                ctx.inform("All nodes launched")
                alive = self.check_nodes()
                # is everything we asked for alive
                if not alive:
                    ret_msg = ("Some nodes in the graph failed to start. "
                               "Check the error log for specific details.")
                    self.logger.error(ret_msg)
                    raise FailReply(ret_msg)
                # Record the TaskInfo for each task in telstate, as well as details
                # about the image resolver.
                details = {}
                for task in self.physical_graph:
                    if isinstance(task, scheduler.PhysicalTask):
                        details[task.logical_node.name] = {
                            'host': task.host,
                            'taskinfo': _redact_keys(task.taskinfo, resolver.s3_config).to_dict()
                        }
                self.telstate.add('sdp_task_details', details, immutable=True)
                self.telstate.add('sdp_image_tag', resolver.image_resolver.tag, immutable=True)
                self.telstate.add('sdp_image_overrides', resolver.image_resolver.overrides,
                                  immutable=True)
            except Exception as exc:
                # If there was a problem the graph might be semi-running. Shut it all down.
                await self._shutdown(force=True)
                raise exc
        except scheduler.InsufficientResourcesError as error:
            raise FailReply('Insufficient resources to launch {}: {}'.format(
                self.subarray_product_id, error)) from error
        except scheduler.ImageError as error:
            raise FailReply(str(error)) from error

    async def deconfigure_impl(self, force, ready):
        if force:
            await self._shutdown(force=force)
            ready.set()
        else:
            def must_wait(node):
                return (isinstance(node.logical_node, tasks.SDPLogicalTask)
                        and node.logical_node.deconfigure_wait)
            # Start the shutdown in a separate task, so that we can monitor
            # for task shutdown.
            wait_tasks = [node.dead_event.wait() for node in self.physical_graph if must_wait(node)]
            shutdown_task = asyncio.ensure_future(self._shutdown(force=force), loop=self.loop)
            await asyncio.gather(*wait_tasks, loop=self.loop)
            ready.set()
            await shutdown_task


def time_request(func):
    """Decorator to record request servicing time as a Prometheus histogram."""
    @functools.wraps(func)
    async def wrapper(self, ctx, *args, **kwargs):
        with REQUEST_TIME.labels(ctx.req.name).time():
            return await func(self, ctx, *args, **kwargs)
    return wrapper


class DeviceStatus(enum.Enum):
    OK = 1
    DEGRADED = 2
    FAIL = 3


class SDPControllerServer(DeviceServer):
    VERSION = "sdpcontroller-3.0"
    BUILD_STATE = "sdpcontroller-" + katsdpcontroller.__version__

    def __init__(self, host, port, sched, loop, safe_multicast_cidr,
                 interface_mode=False,
                 image_resolver_factory=None,
                 s3_config_file=None,
                 gui_urls=None, graph_dir=None):
        # setup sensors
        self._build_state_sensor = Sensor(str, "build-state", "SDP Controller build state.")
        self._api_version_sensor = Sensor(str, "api-version", "SDP Controller API version.")
        self._device_status_sensor = Sensor(DeviceStatus, "device-status",
                                            "Devices status of the SDP Master Controller")
        self._gui_urls_sensor = Sensor(str, "gui-urls", "Links to associated GUIs")
        # example FMECA sensor. In this case something to keep track of issues
        # arising from launching to many processes.
        # TODO: Add more sensors exposing resource usage and currently executing graphs
        self._fmeca_sensors = {}
        self._fmeca_sensors['FD0001'] = Sensor(bool, "fmeca.FD0001", "Sub-process limits")
        self._ntp_sensor = Sensor(bool, "time-synchronised",
                                  "SDP Controller container (and host) is synchronised to NTP")

        self.interface_mode = interface_mode
        if self.interface_mode:
            logger.warning("Note: Running master controller in interface mode. "
                           "This allows testing of the interface only, "
                           "no actual command logic will be enacted.")
        self.sched = sched
        self.gui_urls = gui_urls if gui_urls is not None else []
        self.graph_dir = graph_dir

        if image_resolver_factory is None:
            image_resolver_factory = scheduler.ImageResolver
        self.image_resolver_factory = image_resolver_factory
        self.s3_config_file = s3_config_file

        # create a new resource pool.
        logger.debug("Building initial resource pool")
        self.resources = SDPCommonResources(safe_multicast_cidr)

        # dict of currently configured SDP subarray_products
        self.subarray_products = {}
        # per subarray product dictionaries used to override internal config
        self.override_dicts = {}

        super().__init__(host, port)

        self._build_state_sensor.set_value(self.BUILD_STATE)
        self.sensors.add(self._build_state_sensor)
        self._api_version_sensor.set_value(self.VERSION)
        self.sensors.add(self._api_version_sensor)
        self._device_status_sensor.set_value('ok')
        self.sensors.add(self._device_status_sensor)
        self._gui_urls_sensor.set_value(json.dumps(self.gui_urls))
        self.sensors.add(self._gui_urls_sensor)

        self._ntp_sensor.set_value('0')
        # TODO disabled since aiokatcp doesn't support callback sensors. It was
        # broken anyway.
        # self._ntp_sensor.set_read_callback(self._check_ntp_status)
        self.sensors.add(self._ntp_sensor)

        # until we know any better, failure modes are all inactive
        for s in self._fmeca_sensors.values():
            s.set_value(0)
            self.sensors.add(s)

    @staticmethod
    def _check_ntp_status():
        # Note: currently unused, because it can't talk to the host's NTP anyway
        try:
            return (subprocess.check_output(["/usr/bin/ntpq", "-p"]).find('*') > 0 and '1' or '0',
                    Sensor.Status.NOMINAL, time.time())
        except OSError:
            return ('0', Sensor.Status.NOMINAL, time.time())

    async def deregister_product(self, product, force=False):
        """Deregister a subarray product and remove it form the list of products.

        Raises
        ------
        FailReply
            if an asynchronous operation is in progress on the subarray product and
            `force` is false.
        """
        await product.deconfigure(force=force)

    async def deconfigure_product(self, subarray_product_id, force=False):
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
        await self.deregister_product(product, force)

    async def configure_product(self, ctx, subarray_product_id, config):
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
        product_logger = logging.LoggerAdapter(
            logger, dict(subarray_product_id=subarray_product_id))

        def remove_product(product):
            # Protect against potential race conditions
            if self.subarray_products.get(product.subarray_product_id) is product:
                del self.subarray_products[product.subarray_product_id]
                self.sensors.discard(product.state_sensor)
                self.sensors.discard(product.capture_block_sensor)
                self.mass_inform('interface-changed', 'sensor-list')
                product_logger.info("Deconfigured subarray product %s",
                                    product.subarray_product_id)

        if subarray_product_id in self.override_dicts:
            # this is a use-once set of overrides
            odict = self.override_dicts.pop(subarray_product_id)
            product_logger.warning("Setting overrides on %s for the following: %s",
                                   subarray_product_id, odict)
            config = product_config.override(config, odict)
            # Re-validate, since the override may have broken it
            try:
                product_config.validate(config)
            except (ValueError, jsonschema.ValidationError) as error:
                retmsg = "Overrides make the config invalid: {}".format(error)
                product_logger.error(retmsg)
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
                product_logger.info("Subarray product with this configuration already exists. "
                                    "Pass.")
                return subarray_product_id
            else:
                raise FailReply("A subarray product with this id ({0}) already exists, but has a "
                                "different configuration. Please deconfigure this product or "
                                "choose a new product id to continue.".format(subarray_product_id))

        if self.interface_mode:
            s3_config = {}
        else:
            try:
                s3_config = _load_s3_config(self.s3_config_file)
            except (OSError, KeyError, ValueError) as error:
                raise FailReply("Could not load S3 config: {}".format(str(error)))

        product_logger.debug('config is %s', json.dumps(config, indent=2, sort_keys=True))
        product_logger.info("Launching subarray product %s.", subarray_product_id)
        ctx.inform("Starting configuration of new product {}. This may take a few minutes..."
                   .format(subarray_product_id))

        image_tag = config['config'].get('image_tag')
        if image_tag is not None:
            resolver_factory_args = dict(tag=image_tag)
        else:
            resolver_factory_args = {}
        resolver = scheduler.Resolver(self.image_resolver_factory(**resolver_factory_args),
                                      scheduler.TaskIDAllocator(subarray_product_id + '-'),
                                      self.sched.http_url if self.sched else '')
        resolver.service_overrides = config['config'].get('service_overrides', {})
        resolver.telstate = None
        resolver.s3_config = s3_config

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
        self.sensors.add(product.state_sensor)
        self.sensors.add(product.capture_block_sensor)
        product.dead_callbacks.append(remove_product)
        try:
            await product.configure(ctx)
        except Exception:
            remove_product(product)
            raise
        return subarray_product_id

    async def deconfigure_on_exit(self):
        """Try to shutdown as gracefully as possible when interrupted."""
        logger.warning("SDP Master Controller interrupted - deconfiguring existing products.")
        for subarray_product_id, product in list(self.subarray_products.items()):
            try:
                await self.deregister_product(product, force=True)
            except Exception:
                logger.warning("Failed to deconfigure product %s during master controller exit. "
                               "Forging ahead...", subarray_product_id, exc_info=True,
                               extra=dict(subarray_product_id=subarray_product_id))

    async def stop(self, cancel: bool = True):
        # TODO: set a flag to prevent new async requests being entertained
        await self.deconfigure_on_exit()
        if self.sched is not None:
            await self.sched.close()
        await super().stop(cancel)

    @time_request
    async def request_set_config_override(
            self, ctx, subarray_product_id: str, override_dict_json: str) -> str:
        """Override internal configuration parameters for the next configure of the
        specified subarray product.

        An existing override for this subarry product will be completely overwritten.

        The override will only persist until a successful configure has been
        called on the subarray product.

        Request Arguments
        -----------------
        subarray_product_id : string
            The ID of the subarray product to set overrides for in the form
            <subarray_name>_<data_product_name>.
        override_dict_json : string
            A json string containing a dict of config key:value overrides to use.
        """
        product_logger = logging.LoggerAdapter(
            logger, dict(subarray_product_id=subarray_product_id))
        product_logger.info("?set-config-override called on %s with %s",
                            subarray_product_id, override_dict_json)
        try:
            odict = json.loads(override_dict_json)
            if not isinstance(odict, dict):
                raise ValueError
            product_logger.info("Set override for subarray product %s to the following: %s",
                                subarray_product_id, odict)
            self.override_dicts[subarray_product_id] = json.loads(override_dict_json)
        except ValueError as e:
            msg = ("The supplied override string {} does not appear to be a valid json string "
                   "containing a dict. {}".format(override_dict_json, e))
            product_logger.error(msg)
            raise FailReply(msg)
        return "Set {} override keys for subarray product {}".format(
            len(self.override_dicts[subarray_product_id]), subarray_product_id)

    @time_request
    async def request_product_reconfigure(self, ctx, subarray_product_id: str) -> None:
        """Reconfigure the specified SDP subarray product instance.

           The primary use of this command is to restart the SDP components for a particular
           subarray without having to reconfigure the rest of the system.

           Essentially this runs a deconfigure() followed by a configure() with
           the same parameters as originally specified via the
           product-configure katcp request.

           Request Arguments
           -----------------
           subarray_product_id : string
             The ID of the subarray product to reconfigure.

        """
        product_logger = logging.LoggerAdapter(
            logger, dict(subarray_product_id=subarray_product_id))
        product_logger.info("?product-reconfigure called on %s", subarray_product_id)
        try:
            product = self.subarray_products[subarray_product_id]
        except KeyError:
            raise FailReply("The specified subarray product id {} has no existing configuration "
                            "and thus cannot be reconfigured.".format(subarray_product_id))
        config = product.config

        product_logger.info("Deconfiguring %s as part of a reconfigure request",
                            subarray_product_id)
        try:
            await self.deregister_product(product)
        except Exception as error:
            msg = "Unable to deconfigure as part of reconfigure"
            product_logger.exception(msg)
            raise FailReply("{}. {}".format(msg, error))

        product_logger.info("Waiting for %s to disappear", subarray_product_id)
        await product.dead_event.wait()

        product_logger.info("Issuing new configure for %s as part of reconfigure request.",
                            subarray_product_id)
        try:
            await self.configure_product(ctx, subarray_product_id, config)
        except Exception as error:
            msg = "Unable to configure as part of reconfigure, original array deconfigured"
            product_logger.exception(msg)
            raise FailReply("{}. {}".format(msg, error))

    @time_request
    async def request_product_configure(self, ctx, subarray_product_id: str, config: str) -> str:
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
        product_logger = logging.LoggerAdapter(
            logger, dict(subarray_product_id=subarray_product_id))
        product_logger.info("?product-configure called with: %s", ctx.req)

        if not re.match(r'^[A-Za-z0-9_]+\*?$', subarray_product_id):
            raise FailReply('Subarray_product_id contains illegal characters')
        try:
            config_dict = json.loads(config)
            product_config.validate(config_dict)
        except (ValueError, jsonschema.ValidationError) as error:
            retmsg = "Failed to process config: {}".format(error)
            product_logger.error(retmsg)
            raise FailReply(retmsg)

        subarray_product_id = await self.configure_product(ctx, subarray_product_id, config_dict)
        return subarray_product_id

    @time_request
    async def request_product_deconfigure(
            self, ctx, subarray_product_id: str, force: bool = False) -> None:
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
        ctx.inform("Starting deconfiguration of {}. This may take a few minutes..."
                   .format(subarray_product_id))
        await self.deconfigure_product(subarray_product_id, force)

    @time_request
    async def request_product_list(self, ctx, subarray_product_id: str = None) -> None:
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
            ctx.informs((product_id, str(product))
                        for (product_id, product) in self.subarray_products.items())
        elif subarray_product_id in self.subarray_products:
            ctx.informs([(subarray_product_id,
                          str(self.subarray_products[subarray_product_id]))])
        else:
            raise FailReply("This product id has no current configuration.")

    @time_request
    async def request_capture_init(self, ctx, subarray_product_id: str) -> str:
        """Request capture of the specified subarray product to start.

        Note: This command is used to prepare the SDP for reception of data as
        specified by the subarray product provided. It is necessary to call this
        command before issuing a start command to the CBF. Essentially the SDP
        will, once this command has returned 'OK', be in a wait state until
        reception of the stream control start packet.

        Upon capture-init the subarray product starts a new capture block which
        lasts until the next capture-done command. This corresponds to the
        notion of a "file". The capture-init command returns an ID string that
        uniquely identifies the capture block and can be used to link various
        output products and data sets produced during the capture block.

        Request Arguments
        -----------------
        subarray_product_id : string
            The ID of the subarray product to initialise. This must have
            already been configured via the product-configure command.

        Returns
        -------
        success : {'ok', 'fail'}
            Whether the system is ready to capture or not.
        cbid : str
            ID of the new capture block
        """
        if subarray_product_id not in self.subarray_products:
            raise FailReply('No existing subarray product configuration with this id found')
        sa = self.subarray_products[subarray_product_id]
        cbid = await sa.capture_init()
        return cbid

    @time_request
    async def request_telstate_endpoint(self, ctx, subarray_product_id: str = None):
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
            ctx.informs((product_id, product.telstate_endpoint)
                        for (product_id, product) in self.subarray_products.items())
            return   # ctx.informs sends the reply
        if subarray_product_id not in self.subarray_products:
            raise FailReply('No existing subarray product configuration with this id found')
        return self.subarray_products[subarray_product_id].telstate_endpoint

    @time_request
    async def request_capture_status(self, ctx, subarray_product_id: str = None):
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
            ctx.informs((product_id, product.state)
                        for (product_id, product) in self.subarray_products.items())
            return   # ctx.informs sends the reply

        if subarray_product_id not in self.subarray_products:
            raise FailReply('No existing subarray product configuration with this id found')
        return self.subarray_products[subarray_product_id].state

    @time_request
    async def request_capture_done(self, ctx, subarray_product_id: str) -> str:
        """Halts the currently specified subarray product

        Request Arguments
        -----------------
        subarray_product_id : string
            The id of the subarray product whose state we wish to halt.

        Returns
        -------
        success : {'ok', 'fail'}
            Whether the command succeeded
        cbid : str
            Capture-block ID that was stopped
        """
        if subarray_product_id not in self.subarray_products:
            raise FailReply('No existing subarray product configuration with this id found')
        sa = self.subarray_products[subarray_product_id]
        cbid = await sa.capture_done()
        return cbid

    @time_request
    async def request_sdp_shutdown(self, ctx) -> str:
        """Shut down the SDP master controller and all controlled nodes.

        Returns
        -------
        success : {'ok', 'fail'}
            Whether the shutdown sequence of all other nodes succeeded.
        hosts : str
            Comma separated lists of hosts that have been shutdown (excl mc host)
        """
        logger.info("SDP Shutdown called.")

        # attempt to deconfigure any existing subarrays
        # will always succeed even if some deconfigure fails
        await self.deconfigure_on_exit()
        try:
            master, slaves = await self.sched.get_master_and_slaves(timeout=5)
        except Exception:
            logger.exception('Failed to get list of slaves, so not powering them off.')
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
            addresses = (ifaddresses.get(netifaces.AF_INET, [])
                         + ifaddresses.get(netifaces.AF_INET6, []))
            for entry in addresses:
                address = entry.get('addr', '')
                if address:
                    master_addresses.add(address)
        addresses = await self.loop.getaddrinfo(master, 0)
        for (_family, _type, _proto, _canonname, sockaddr) in addresses:
            master_addresses.add(sockaddr[0])
        logger.debug('Master IP addresses: %s', ','.join(master_addresses))
        non_master = []
        for node in physical_graph:
            is_master = False
            addresses = await self.loop.getaddrinfo(node.logical_node.host, 0)
            for (_family, _type, _proto, _canonname, sockaddr) in addresses:
                if sockaddr[0] in master_addresses:
                    is_master = True
                    break
            if not is_master:
                non_master.append(node)

        resolver = scheduler.Resolver(self.image_resolver_factory(),
                                      scheduler.TaskIDAllocator('poweroff-'),
                                      self.sched.http_url)
        # Shut down everything except the master/self
        await self.sched.launch(physical_graph, resolver, non_master)
        # Shut down the rest
        await self.sched.launch(physical_graph, resolver)
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
        return ','.join(response)

    @time_request
    async def request_sdp_status(self, ctx) -> int:
        """Request status of SDP components (deprecated).

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
        return 0


class InterfaceModeSensors:
    def __init__(self, subarray_product_id, logger):
        """Manage dummy subarray product sensors on a SDPControllerServer instance

        Parameters
        ----------
        subarray_product_id : str
            Subarray product id, e.g. `array_1_c856M4k`
        logger : logging.LoggerAdapter
            Logger with context for subarray_product_id
        """
        self.subarray_product_id = subarray_product_id
        self.logger = logger
        self.sensors = {}

    def add_sensors(self, server):
        """Add dummy subarray product sensors and issue #interface-changed"""

        interface_sensor_params = {
            'bf_ingest.beamformer.1.port': dict(
                default=Address("ing1.sdp.mkat.fake.kat.ac.za", 31048),
                sensor_type=Address,
                description='IP endpoint for port',
                initial_status=Sensor.Status.NOMINAL),
            'ingest.sdp_l0.1.capture-active': dict(
                default=False,
                sensor_type=bool,
                description='Is there a currently active capture session.',
                initial_status=Sensor.Status.NOMINAL),
            'timeplot.sdp_l0.1.gui-urls': dict(
                default='[{"category": "Plot", '
                '"href": "http://ing1.sdp.mkat.fake.kat.ac.za:31054/", '
                '"description": "Signal displays for array_1_bc856M4k", '
                '"title": "Signal Display"}]',
                sensor_type=str,
                description='URLs for GUIs',
                initial_status=Sensor.Status.NOMINAL),
            'timeplot.sdp_l0.1.html_port': dict(
                default=Address("ing1.sdp.mkat.fake.kat.ac.za", 31054),
                sensor_type=Address,
                description='IP endpoint for html_port',
                initial_status=Sensor.Status.NOMINAL),
            'cal.1.capture-block-state': dict(
                default='{}',
                sensor_type=str,
                description='JSON dict with the state of each capture block',
                initial_status=Sensor.Status.NOMINAL)
        }

        sensors_added = False
        try:
            for postfix, sensor_params in interface_sensor_params.items():
                sensor_name = self.subarray_product_id + '.' + postfix
                if sensor_name in self.sensors:
                    self.logger.info('Simulated sensor %r already exists, skipping',
                                     sensor_name)
                    continue
                sensor_params['name'] = sensor_name
                sensor = Sensor(**sensor_params)
                self.sensors[sensor_name] = sensor
                server.sensors.add(sensor)
                sensors_added = True
        finally:
            if sensors_added:
                server.mass_inform('interface-changed', 'sensor-list')

    def remove_sensors(self, server):
        """Remove dummy subarray product sensors and issue #interface-changed"""
        sensors_removed = False
        try:
            for sensor_name, sensor in list(self.sensors.items()):
                server.sensors.discard(sensor)
                del self.sensors[sensor_name]
                sensors_removed = True
        finally:
            if sensors_removed:
                server.mass_inform('interface-changed', 'sensor-list')
