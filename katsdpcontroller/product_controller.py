"""Control of a single subarray product"""

# TODO:
# - validate S3 config
# - fill in remaining requests

import asyncio
import logging
import json
import time
import os
from ipaddress import IPv4Address
from typing import Dict, Set, List, Callable, Sequence, Optional, Type

import addict
import jsonschema
import networkx
import aiokatcp
from aiokatcp import FailReply, Sensor, Address
import katsdptelstate

import katsdpcontroller
from . import scheduler, product_config, generator, tasks
from .controller import (time_request, load_json_dict, log_task_exceptions,
                         DeviceStatus, ProductState)
from .tasks import CaptureBlockState, KatcpTransition, DEPENDS_INIT


BATCH_PRIORITY = 1        #: Scheduler priority for batch queues
BATCH_RESOURCES_TIMEOUT = 7 * 86400   # A week
logger = logging.getLogger(__name__)


def _redact_arg(arg: str, s3_config: dict) -> str:
    """Process one argument for _redact_keys"""
    for config in s3_config.values():
        for mode in ['read', 'write']:
            if mode in config:
                for name in ['access_key', 'secret_key']:
                    key = config[mode][name]
                    if arg == key or arg.endswith('=' + key):
                        return arg[:-len(key)] + 'REDACTED'
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
        taskinfo.command.arguments = [_redact_arg(arg, s3_config)
                                      for arg in taskinfo.command.arguments]
    return taskinfo


class KatcpImageLookup(scheduler.ImageLookup):
    """Image lookup that asks the master controller to do the work.

    Tunnelling the lookup avoids the need for the product controller to
    have the right CA certificates and credentials for the Docker registry.
    """
    def __init__(self, conn: aiokatcp.Client) -> None:
        self._conn = conn

    async def __call__(self, repo: str, tag: str) -> str:
        reply, informs = await self._conn.request('image-lookup', repo, tag)
        return reply[0].decode()


class Resolver(scheduler.Resolver):
    """Resolver with some extra fields"""
    def __init__(self,
                 image_resolver: scheduler.ImageResolver,
                 task_id_allocator: scheduler.TaskIDAllocator,
                 http_url: Optional[str],
                 service_overrides: dict,
                 s3_config: dict) -> None:
        super().__init__(image_resolver, task_id_allocator, http_url)
        self.service_overrides = service_overrides
        self.s3_config = s3_config
        self.telstate: Optional[katsdptelstate.TelescopeState] = None
        self.resources: Optional[SDPResources] = None


class SDPResources:
    """Helper class to allocate resources for a single subarray-product."""
    def __init__(self, master_controller: aiokatcp.Client, subarray_product_id: str) -> None:
        self.master_controller = master_controller
        self.subarray_product_id = subarray_product_id

    async def get_multicast_groups(self, n_addresses: int) -> str:
        """Assign multicast addresses for a group."""
        reply, informs = await self.master_controller.request(
            'get-multicast-groups', self.subarray_product_id, n_addresses)
        return reply[0].decode()

    @staticmethod
    async def get_port() -> int:
        """Return an assigned port for a multicast group"""
        return 7148


class CaptureBlock:
    """A capture block is book-ended by a capture-init and a capture-done,
    although processing on it continues after the capture-done."""

    def __init__(self, name: str, config: dict) -> None:
        self.name = name
        self.config = config
        self._state = CaptureBlockState.INITIALISING
        self.postprocess_task: Optional[asyncio.Task] = None
        self.postprocess_physical_graph: Optional[networkx.MultiDiGraph] = None
        self.dead_event = asyncio.Event()
        self.state_change_callback: Optional[Callable[[], None]] = None

    @property
    def state(self) -> CaptureBlockState:
        return self._state

    @state.setter
    def state(self, value: CaptureBlockState) -> None:
        if self._state != value:
            self._state = value
            if value == CaptureBlockState.DEAD:
                self.dead_event.set()
            if self.state_change_callback is not None:
                self.state_change_callback()


def _error_on_error(state: ProductState) -> Sensor.Status:
    return Sensor.Status.ERROR if state == ProductState.ERROR else Sensor.Status.NOMINAL


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
    - Elements of :attr:`capture_blocks` are not in state DEAD.

    This is a base class that is intended to be subclassed. The methods whose
    names end in ``_impl`` are extension points that should be implemented in
    subclasses to do the real work. These methods are run as part of the
    asynchronous operations. They need to be cancellation-safe, to allow for
    forced deconfiguration to abort them.
    """
    def __init__(self, sched: Optional[scheduler.Scheduler],
                 config: dict,
                 resolver: Resolver,
                 subarray_product_id: str,
                 sdp_controller: 'DeviceServer') -> None:
        #: Current background task (can only be one)
        self._async_task: Optional[asyncio.Task] = None
        self.sched = sched
        self.config = config
        self.resolver = resolver
        self.subarray_product_id = subarray_product_id
        self.sdp_controller = sdp_controller
        self.logical_graph = generator.build_logical_graph(config)
        self.telstate_endpoint = ""
        self.telstate: katsdptelstate.TelescopeState = None
        self.capture_blocks: Dict[str, CaptureBlock] = {}  # live capture blocks, indexed by name
        # set between capture_init and capture_done
        self.current_capture_block: Optional[CaptureBlock] = None
        self.dead_event = asyncio.Event()                # Set when reached state DEAD
        # Callbacks that are called when we reach state DEAD. These are
        # provided in addition to dead_event, because sometimes it's
        # necessary to react immediately rather than waiting for next time
        # around the event loop. Each callback takes self as the argument.
        self.dead_callbacks = [lambda product: product.dead_event.set()]
        self._state: ProductState = ProductState.CONFIGURING
        # Set of sensors to remove when the product is removed
        self.sensors: Set[aiokatcp.Sensor] = set()
        self._capture_block_sensor = Sensor(
            str, subarray_product_id + ".capture-block-state",
            "JSON dictionary of capture block states for active capture blocks",
            default="{}", initial_status=Sensor.Status.NOMINAL)
        self._state_sensor = Sensor(
            ProductState, subarray_product_id + ".state",
            "State of the subarray product state machine",
            status_func=_error_on_error)
        self.state = ProductState.CONFIGURING   # This sets the sensor
        self.add_sensor(self._capture_block_sensor)
        self.add_sensor(self._state_sensor)
        self.logger = logging.LoggerAdapter(logger, dict(subarray_product_id=subarray_product_id))
        self.logger.info("Created: %r", self)
        self.logger.info('Logical graph nodes:\n'
                         + '\n'.join(repr(node) for node in self.logical_graph))

    @property
    def state(self) -> ProductState:
        return self._state

    @state.setter
    def state(self, value: ProductState) -> None:
        if (self._state == ProductState.ERROR
                and value not in (ProductState.DECONFIGURING, ProductState.DEAD)):
            return      # Never leave error state other than by deconfiguring
        self._state = value
        self._state_sensor.value = value

    def add_sensor(self, sensor: Sensor) -> None:
        """Add the supplied sensor to the top-level device and track it locally."""
        self.sensors.add(sensor)
        self.sdp_controller.sensors.add(sensor)

    def remove_sensors(self):
        """Remove all sensors added via :meth:`add_sensor`.

        It does *not* send an ``interface-changed`` inform; that is left to the
        caller.
        """
        for sensor in self.sensors:
            self.sdp_controller.sensors.discard(sensor)
        self.sensors.clear()

    @property
    def async_busy(self) -> bool:
        """Whether there is an asynchronous state-change operation in progress."""
        return self._async_task is not None and not self._async_task.done()

    def _fail_if_busy(self) -> None:
        """Raise a FailReply if there is an asynchronous operation in progress."""
        if self.async_busy:
            raise FailReply('Subarray product {} is busy with an operation. '
                            'Please wait for it to complete first.'.format(
                                self.subarray_product_id))

    async def configure_impl(self) -> None:
        """Extension point to configure the subarray."""
        pass

    async def deconfigure_impl(self, force: bool, ready: asyncio.Event) -> None:
        """Extension point to deconfigure the subarray.

        Parameters
        ----------
        force
            Whether to do an abrupt deconfiguration without waiting for
            postprocessing.
        ready
            If the ?product-deconfigure command should return before
            deconfiguration is complete, this event can be set at that point.
        """
        pass

    async def capture_init_impl(self, capture_block: CaptureBlock) -> None:
        """Extension point to start a capture block.

        If it raises an exception, the capture block is assumed to not have
        been started, and the subarray product goes into state ERROR.
        """
        pass

    async def capture_done_impl(self, capture_block: CaptureBlock) -> None:
        """Extension point to stop a capture block.

        This should only do the work needed for the ``capture-done`` master
        controller request to return. The caller takes care of calling
        :meth:`postprocess_impl`.

        It needs to be safe to run from DECONFIGURING and ERROR states, because
        it may be run as part of forced deconfigure.

        If it raises an exception, the capture block is assumed to be dead,
        and the subarray product goes into state ERROR unless this occurred as
        part of deconfiguring.
        """
        pass

    async def postprocess_impl(self, capture_block: CaptureBlock) -> None:
        """Complete the post-processing for a capture block.

        Subclasses should override this if a capture block is not finished when
        :meth:`_capture_done` returns.

        Note that a failure here does **not** put the subarray product into
        ERROR state, as it is assumed that this does not interfere with
        subsequent operation.

        This function should move the capture block to POSTPROCESSING after
        burndown of the real-time processing, but should not set it to DEAD.
        """
        pass

    def capture_block_dead_impl(self, capture_block: CaptureBlock) -> None:
        """Clean up after a capture block is no longer active.

        This should only be overridden to clean up the state machine, not to
        do processing. It is called both for the normal lifecycle, but also
        when there is a failure e.g. if capture_init_impl or capture_done_impl
        raised an exception.
        """
        pass

    async def _configure(self) -> None:
        """Asynchronous task that does the configuration."""
        await self.configure_impl()
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
        # can change during the await.
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

    def _capture_block_dead(self, capture_block: CaptureBlock) -> None:
        """Mark a capture block as dead and remove it from the list."""
        try:
            del self.capture_blocks[capture_block.name]
        except KeyError:
            pass      # Allows this function to be called twice
        # Setting the state will trigger _update_capture_block_sensor, which
        # will update the sensor with the value removed
        capture_block.state = CaptureBlockState.DEAD
        self.capture_block_dead_impl(capture_block)

    def _update_capture_block_sensor(self) -> None:
        value = {name: capture_block.state.name.lower()
                 for name, capture_block in self.capture_blocks.items()}
        self._capture_block_sensor.set_value(json.dumps(value, sort_keys=True))

    async def _capture_init(self, capture_block: CaptureBlock) -> None:
        self.capture_blocks[capture_block.name] = capture_block
        capture_block.state_change_callback = self._update_capture_block_sensor
        # Update the sensor with the INITIALISING state
        self._update_capture_block_sensor()
        try:
            await self.capture_init_impl(capture_block)
            if self.state == ProductState.ERROR:
                raise FailReply('Subarray product went into ERROR while starting capture')
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
        :meth:`capture_done_impl` for additional details.

        This is only called for a "normal" capture-done. Forced deconfigures
        call :meth:`capture_done_impl` directly.

        Returns
        -------
        The capture block that was stopped
        """
        capture_block = self.current_capture_block
        assert capture_block is not None
        try:
            await self.capture_done_impl(capture_block)
            if self.state == ProductState.ERROR and not error_expected:
                raise FailReply('Subarray product went into ERROR while stopping capture')
        except asyncio.CancelledError:
            raise
        except Exception:
            self.state = ProductState.ERROR
            self.current_capture_block = None
            self._capture_block_dead(capture_block)
            raise
        assert self.current_capture_block is capture_block
        if self.state == ProductState.CAPTURING:
            self.state = ProductState.IDLE
        else:
            assert error_expected
        self.current_capture_block = None
        capture_block.state = CaptureBlockState.BURNDOWN
        capture_block.postprocess_task = asyncio.get_event_loop().create_task(
            self.postprocess_impl(capture_block))
        log_task_exceptions(
            capture_block.postprocess_task, self.logger,
            "Exception in postprocessing for {}/{}".format(self.subarray_product_id,
                                                           capture_block.name))

        def done_callback(task: asyncio.Future, capture_block=capture_block) -> None:
            self._capture_block_dead(capture_block)

        capture_block.postprocess_task.add_done_callback(done_callback)
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
        assert self.state == ProductState.CONFIGURING, \
            "configure should be the first thing to happen"
        task = asyncio.get_event_loop().create_task(self._configure())
        log_task_exceptions(task, self.logger, "Configuring subarray product {} failed".format(
            self.subarray_product_id))
        self._async_task = task
        try:
            await task
        finally:
            self._clear_async_task(task)
        self.logger.info('Subarray product %s successfully configured', self.subarray_product_id)

    async def deconfigure(self, force: bool = False) -> None:
        """Start deconfiguration of the subarray, but does not wait for it to complete."""
        if self.state == ProductState.DEAD:
            return
        if self.async_busy:
            if not force:
                self._fail_if_busy()
            else:
                self.logger.warning('Subarray product %s is busy with an operation, '
                                    'but deconfiguring anyway', self.subarray_product_id)

        if self.state not in (ProductState.IDLE, ProductState.ERROR):
            if not force:
                raise FailReply('Subarray product is not idle and thus cannot be deconfigured. '
                                'Please issue capture_done first.')
            else:
                self.logger.warning('Subarray product %s is in state %s, but deconfiguring anyway',
                                    self.subarray_product_id, self.state.name)
        self.logger.info("Deconfiguring subarray product %s", self.subarray_product_id)

        ready = asyncio.Event()
        task = asyncio.get_event_loop().create_task(self._deconfigure(force, ready))
        log_task_exceptions(task, self.logger,
                            "Deconfiguring {} failed".format(self.subarray_product_id))
        # Make sure that ready gets unblocked even if task throws.
        task.add_done_callback(lambda future: ready.set())
        task.add_done_callback(self._clear_async_task)
        if await self._replace_async_task(task):
            await ready.wait()
        # We don't wait for task to complete, but if it's already done we
        # pass back any exceptions.
        if task.done():
            await task

    async def capture_init(self, capture_block_id: str, config: dict) -> str:
        self._fail_if_busy()
        if self.state != ProductState.IDLE:
            raise FailReply('Subarray product {} is currently in state {}, not IDLE as expected. '
                            'Cannot be inited.'.format(self.subarray_product_id, self.state.name))
        self.logger.info('Using capture block ID %s', capture_block_id)

        capture_block = CaptureBlock(capture_block_id, config)
        task = asyncio.get_event_loop().create_task(self._capture_init(capture_block))
        self._async_task = task
        try:
            await task
        finally:
            self._clear_async_task(task)
        self.logger.info('Started capture block %s on subarray product %s',
                         capture_block_id, self.subarray_product_id)
        return capture_block_id

    async def capture_done(self) -> str:
        self._fail_if_busy()
        if self.state != ProductState.CAPTURING:
            raise FailReply('Subarray product is currently in state {}, not CAPTURING as expected. '
                            'Cannot be stopped.'.format(self.state.name))
        assert self.current_capture_block is not None
        capture_block_id = self.current_capture_block.name
        task = asyncio.get_event_loop().create_task(self._capture_done())
        self._async_task = task
        try:
            await task
        finally:
            self._clear_async_task(task)
        self.logger.info('Finished capture block %s on subarray product %s',
                         capture_block_id, self.subarray_product_id)
        return capture_block_id

    def write_graphs(self, output_dir: str) -> None:
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

    def __repr__(self) -> str:
        return "Subarray product {} (State: {})".format(self.subarray_product_id, self.state.name)


class InterfaceModeSensors:
    def __init__(self, subarray_product_id: str) -> None:
        """Manage dummy subarray product sensors on a SDPControllerServer instance

        Parameters
        ----------
        subarray_product_id
            Subarray product id, e.g. `array_1_c856M4k`
        """
        self.subarray_product_id = subarray_product_id
        self.sensors: Dict[str, Sensor] = {}

    def add_sensors(self, server: aiokatcp.DeviceServer) -> None:
        """Add dummy subarray product sensors and issue #interface-changed"""

        interface_sensor_params = {
            'bf_ingest.beamformer.1.port': dict(
                default=Address(IPv4Address("1.2.3.4"), 31048),
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
                default=Address(IPv4Address("1.2.3.5"), 31054),
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
                    logger.info('Simulated sensor %r already exists, skipping',
                                sensor_name)
                    continue
                sensor_params['name'] = sensor_name
                sensor = Sensor(**sensor_params)     # type: ignore
                self.sensors[sensor_name] = sensor
                server.sensors.add(sensor)
                sensors_added = True
        finally:
            if sensors_added:
                server.mass_inform('interface-changed', 'sensor-list')

    def remove_sensors(self, server: aiokatcp.DeviceServer) -> None:
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


class SDPSubarrayProductInterface(SDPSubarrayProductBase):
    """Dummy implementation of SDPSubarrayProductBase interface that does not
    actually run anything.
    """
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._interface_mode_sensors = InterfaceModeSensors(self.subarray_product_id)
        sensors = self._interface_mode_sensors.sensors
        self._capture_block_states = [
            sensor for sensor in sensors.values() if sensor.name.endswith('.capture-block-state')]

    def _update_capture_block_state(self, capture_block_id: str,
                                    state: Optional[CaptureBlockState]) -> None:
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
                    states[capture_block_id] = state.name.lower()
                sensor.set_value(json.dumps(states))

    async def capture_init_impl(self, capture_block: CaptureBlock) -> None:
        self._update_capture_block_state(capture_block.name, CaptureBlockState.CAPTURING)

    async def capture_done_impl(self, capture_block: CaptureBlock) -> None:
        self._update_capture_block_state(capture_block.name, CaptureBlockState.BURNDOWN)

    async def postprocess_impl(self, capture_block: CaptureBlock) -> None:
        await asyncio.sleep(0.1)
        self._update_capture_block_state(capture_block.name, CaptureBlockState.POSTPROCESSING)
        capture_block.state = CaptureBlockState.POSTPROCESSING
        await asyncio.sleep(0.1)
        self._update_capture_block_state(capture_block.name, None)

    async def configure_impl(self) -> None:
        self.logger.warning("No components will be started - running in interface mode")
        # Add dummy sensors for this product
        self._interface_mode_sensors.add_sensors(self.sdp_controller)

    async def deconfigure_impl(self, force: bool, ready: asyncio.Event) -> None:
        self._interface_mode_sensors.remove_sensors(self.sdp_controller)


class SDPSubarrayProduct(SDPSubarrayProductBase):
    """Subarray product that actually launches nodes."""
    sched: scheduler.Scheduler     # Override Optional[] from base class

    def _instantiate(self, logical_node: scheduler.LogicalNode,
                     capture_block_id: Optional[str]) -> scheduler.PhysicalNode:
        if isinstance(logical_node, tasks.SDPLogicalTask):
            return logical_node.physical_factory(
                logical_node, self.sdp_controller, self, capture_block_id)
        return logical_node.physical_factory(logical_node)

    def _instantiate_physical_graph(self, logical_graph: networkx.MultiDiGraph,
                                    capture_block_id: str = None) -> networkx.MultiDiGraph:
        mapping = {logical: self._instantiate(logical, capture_block_id)
                   for logical in logical_graph}
        return networkx.relabel_nodes(logical_graph, mapping)

    def __init__(self, sched: scheduler.Scheduler, config: dict,
                 resolver: Resolver, subarray_product_id: str,
                 sdp_controller: 'DeviceServer', telstate_name: str = 'telstate') -> None:
        super().__init__(sched, config, resolver, subarray_product_id, sdp_controller)
        # Priority is lower (higher number) than the default queue
        self.batch_queue = scheduler.LaunchQueue(
            sdp_controller.batch_role, subarray_product_id, priority=BATCH_PRIORITY)
        sched.add_queue(self.batch_queue)
        # generate physical nodes
        self.physical_graph = self._instantiate_physical_graph(self.logical_graph)
        # Nodes indexed by logical name
        self._nodes = {node.logical_node.name: node for node in self.physical_graph}
        self.telstate_node = self._nodes[telstate_name]
        self.master_controller = sdp_controller.master_controller

    def __del__(self) -> None:
        if hasattr(self, 'batch_queue'):
            self.sched.remove_queue(self.batch_queue)

    async def _exec_node_transition(self, node: tasks.SDPPhysicalTask,
                                    reqs: Sequence[KatcpTransition],
                                    deps: Sequence[asyncio.Future],
                                    state: CaptureBlockState,
                                    capture_block: CaptureBlock) -> None:
        try:
            if deps:
                # If we're starting a capture and a dependency fails, there is
                # no point trying to continue. On the shutdown path, we should
                # continue anyway to try to close everything off as neatly as
                # possible.
                await asyncio.gather(*deps,
                                     return_exceptions=(state != CaptureBlockState.CAPTURING))
            if reqs:
                if node.katcp_connection is None:
                    self.logger.warning(
                        'Cannot issue %s to %s because there is no katcp connection',
                        reqs[0], node.name)
                else:
                    for req in reqs:
                        await node.issue_req(req.name, req.args, timeout=req.timeout)
            if isinstance(node, tasks.SDPPhysicalTask) and state == node.logical_node.final_state:
                observer = node.capture_block_state_observer
                if observer is not None:
                    self.logger.info('Waiting for %s on %s', capture_block.name, node.name)
                    await observer.wait_capture_block_done(capture_block.name)
                    self.logger.info('Done waiting for %s on %s', capture_block.name, node.name)
                else:
                    self.logger.debug('Task %s has no capture-block-state observer', node.name)
        finally:
            if isinstance(node, tasks.SDPPhysicalTask) and state == node.logical_node.final_state:
                node.remove_capture_block(capture_block)

    async def exec_transitions(self, state: CaptureBlockState, reverse: bool,
                               capture_block: CaptureBlock) -> None:
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

        futures: Dict[object, asyncio.Future] = {}     # Keyed by node
        # Lexicographical tie-breaking isn't strictly required, but it makes
        # behaviour predictable.
        now = time.time()   # Outside loop to be consistent across all nodes
        for node in networkx.lexicographical_topological_sort(deps_graph, key=lambda x: x.name):
            reqs: List[KatcpTransition] = []
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
            task = asyncio.get_event_loop().create_task(
                self._exec_node_transition(node, reqs, deps, state, capture_block))
            futures[node] = task
        if futures:
            # We want to wait for all the futures to complete, even if one of
            # them fails early (to give the others time to do cleanup). But
            # then we want to raise the first exception.
            results = await asyncio.gather(*futures.values(), return_exceptions=True)
            for result in results:
                if isinstance(result, Exception):
                    raise result

    async def capture_init_impl(self, capture_block: CaptureBlock) -> None:
        self.telstate.add('sdp_capture_block_id', capture_block.name)
        for node in self.physical_graph:
            if isinstance(node, tasks.SDPPhysicalTask):
                node.add_capture_block(capture_block)
        await self.exec_transitions(CaptureBlockState.CAPTURING, True, capture_block)

    async def capture_done_impl(self, capture_block: CaptureBlock) -> None:
        await self.exec_transitions(CaptureBlockState.BURNDOWN, False, capture_block)

    async def postprocess_impl(self, capture_block: CaptureBlock) -> None:
        await self.exec_transitions(CaptureBlockState.POSTPROCESSING, False, capture_block)
        capture_block.state = CaptureBlockState.POSTPROCESSING

        try:
            logical_graph = generator.build_postprocess_logical_graph(
                capture_block.config, capture_block.name, self.telstate)
            physical_graph = self._instantiate_physical_graph(
                logical_graph, capture_block.name)
            capture_block.postprocess_physical_graph = physical_graph
            nodes = {node.logical_node.name: node for node in physical_graph}
            telstate_node = nodes['telstate']
            telstate_node.host = self.telstate_node.host
            telstate_node.ports = dict(self.telstate_node.ports)
            # This doesn't actually run anything, just marks the fake telstate node
            # as READY. It could block for a while behind real tasks in the batch
            # queue, but that doesn't matter because our real tasks will block too.
            await self.sched.launch(physical_graph, self.resolver, [telstate_node],
                                    queue=self.batch_queue)
            nodelist = [node for node in physical_graph if isinstance(node, scheduler.PhysicalTask)]
            await self.sched.batch_run(physical_graph, self.resolver, nodelist,
                                       queue=self.batch_queue,
                                       resources_timeout=BATCH_RESOURCES_TIMEOUT, attempts=3)
        finally:
            await self.exec_transitions(CaptureBlockState.DEAD, False, capture_block)

    def capture_block_dead_impl(self, capture_block: CaptureBlock) -> None:
        for node in self.physical_graph:
            if isinstance(node, tasks.SDPPhysicalTask):
                node.remove_capture_block(capture_block)

    async def _launch_telstate(self) -> None:
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
                for suffix in ['src_streams', 'instrument_dev_name']:
                    if suffix in stream:
                        base_params[(name, suffix)] = stream[suffix]

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
            key = self.telstate.join(*k) if isinstance(k, tuple) else k
            self.telstate[key] = v

    def check_nodes(self) -> bool:
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

    def unexpected_death(self, task: scheduler.PhysicalTask) -> None:
        self.logger.warning('Task %s died unexpectedly', task.name)
        if task.logical_node.critical:
            self._go_to_error()

    def bad_device_status(self, task: scheduler.PhysicalTask) -> None:
        self.logger.warning('Task %s has failed (device-status)', task.name)
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
            self.logger.warning('Attempting to terminate capture block %s', capture_block_id)
            task = asyncio.get_event_loop().create_task(self._capture_done(error_expected=True))
            self._async_task = task
            log_task_exceptions(task, self.logger,
                                "Failed to terminate capture block {}".format(capture_block_id))

            def cleanup(task):
                self._clear_async_task(task)
                self.logger.info('Finished capture block %s on subarray product %s',
                                 capture_block_id, self.subarray_product_id)

            task.add_done_callback(cleanup)

        # We don't go to error state from CONFIGURING because we check all
        # nodes at the end of configuration and will fail the configure
        # there; and from DECONFIGURING we don't want to go to ERROR because
        # that may prevent deconfiguring.
        if self.state in (ProductState.IDLE, ProductState.CAPTURING):
            self.state = ProductState.ERROR

    async def _shutdown(self, force: bool) -> None:
        # TODO: issue progress reports as tasks stop
        await self.sched.kill(self.physical_graph, force=force,
                              capture_blocks=self.capture_blocks)

    async def configure_impl(self) -> None:
        try:
            try:
                resolver = self.resolver
                resolver.resources = SDPResources(self.master_controller, self.subarray_product_id)
                # launch the telescope state for this graph
                await self._launch_telstate()
                # launch containers for those nodes that require them
                await self.sched.launch(self.physical_graph, self.resolver)
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

    async def deconfigure_impl(self, force: bool, ready: asyncio.Event) -> None:
        if force:
            await self._shutdown(force=force)
            ready.set()
        else:
            def must_wait(node):
                return (isinstance(node.logical_node, tasks.SDPLogicalTask)
                        and node.logical_node.final_state <= CaptureBlockState.BURNDOWN)
            # Start the shutdown in a separate task, so that we can monitor
            # for task shutdown.
            wait_tasks = [node.dead_event.wait() for node in self.physical_graph if must_wait(node)]
            shutdown_task = asyncio.get_event_loop().create_task(self._shutdown(force=force))
            await asyncio.gather(*wait_tasks)
            ready.set()
            await shutdown_task


class DeviceServer(aiokatcp.DeviceServer):
    VERSION = 'product-controller-1.0'
    BUILD_STATE = "katsdpcontroller-" + katsdpcontroller.__version__

    def __init__(self, host: str, port: int, master_controller: aiokatcp.Client,
                 sched: Optional[scheduler.Scheduler],
                 batch_role: str,
                 interface_mode: bool,
                 image_resolver_factory: scheduler.ImageResolverFactory,
                 s3_config: dict,
                 graph_dir: str = None) -> None:
        self.sched = sched
        self.batch_role = batch_role
        self.interface_mode = interface_mode
        self.image_resolver_factory = image_resolver_factory
        self.s3_config = s3_config
        self.graph_dir = graph_dir
        self.master_controller = master_controller
        self.product: Optional[SDPSubarrayProductBase] = None

        super().__init__(host, port)
        # setup sensors
        self.sensors.add(Sensor(DeviceStatus, "device-status",
                                "Devices status of the subarray product controller",
                                default=DeviceStatus.OK, initial_status=Sensor.Status.NOMINAL))

    async def start(self) -> None:
        await self.master_controller.wait_connected()
        await super().start()

    async def stop(self, cancel: bool = True) -> None:
        await super().stop(cancel)
        self.master_controller.close()
        await self.master_controller.wait_closed()

    async def configure_product(self, name: str, config: dict) -> None:
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

        logger.debug('config is %s', json.dumps(config, indent=2, sort_keys=True))
        logger.info("Launching subarray product.")

        image_tag = config['config'].get('image_tag')
        if image_tag is not None:
            resolver_factory_args = dict(tag=image_tag)
        else:
            resolver_factory_args = {}
        resolver = Resolver(
            self.image_resolver_factory(**resolver_factory_args),
            scheduler.TaskIDAllocator(name + '-'),
            self.sched.http_url if self.sched else '',
            config['config'].get('service_overrides', {}),
            self.s3_config)

        # create graph object and build physical graph from specified resources
        product_cls: Type[SDPSubarrayProductBase]
        if self.interface_mode:
            product_cls = SDPSubarrayProductInterface
        else:
            product_cls = SDPSubarrayProduct
        product = product_cls(self.sched, config, resolver, name, self)
        if self.graph_dir is not None:
            product.write_graphs(self.graph_dir)
        self.product = product   # Prevents another attempt to configure
        await product.configure()

    @time_request
    async def request_product_configure(self, ctx, name: str, config: str) -> None:
        """Configure a SDP subarray product instance.

        Parameters
        ----------
        name : str
            Name of the subarray product.
        config : str
            A JSON-encoded dictionary of configuration data.
        """
        logger.info("?product-configure called with: %s", ctx.req)

        if self.product is not None:
            raise FailReply('Already configured or configuring')
        try:
            config_dict = load_json_dict(config)
            product_config.validate(config_dict)
            config_dict = product_config.normalise(config_dict)
        except (ValueError, jsonschema.ValidationError) as exc:
            retmsg = f"Failed to process config: {exc}"
            logger.error(retmsg)
            raise FailReply(retmsg)

        await self.configure_product(name, config_dict)

    @time_request
    async def request_product_deconfigure(self, ctx, force: bool = False) -> None:
        """Deconfigure the product and shut down the server."""
        if self.product is None:
            raise FailReply('Have not yet configured')
        await self.product.deconfigure(force=force)
        self.halt()

    async def request_capture_init(self, ctx, capture_block_id: str,
                                   override_dict_json: str = '{}') -> None:
        """Request capture of the specified subarray product to start.

        Parameters
        ----------
        capture_block_id : str
            The capture block ID for the new capture block.
        override_dict_json : str, optional
            Configuration dictionary to merge with the subarray config.
        """
        if self.product is None:
            raise FailReply('Have not yet configured')
        try:
            overrides = load_json_dict(override_dict_json)
        except ValueError as error:
            retmsg = f'Override {override_dict_json} is not a valid JSON dict: {error}'
            logger.error(retmsg)
            raise FailReply(retmsg) from error

        config = product_config.override(self.product.config, overrides)
        # Re-validate, since the override may have broken it
        try:
            product_config.validate(config)
        except (ValueError, jsonschema.ValidationError) as error:
            retmsg = f"Overrides make the config invalid: {error}"
            logger.error(retmsg)
            raise FailReply(retmsg) from error

        config = product_config.normalise(config)
        try:
            product_config.validate_capture_block(self.product.config, config)
        except ValueError as error:
            retmsg = f"Invalid config override: {error}"
            logger.error(retmsg)
            raise FailReply(retmsg) from error

        await self.product.capture_init(capture_block_id, config)

    @time_request
    async def request_capture_status(self, ctx) -> ProductState:
        """Returns the status of the subarray product.

        Returns
        -------
        state : str
        """
        if self.product is None:
            raise FailReply('Have not yet configured')
        return self.product.state

    async def request_capture_done(self, ctx) -> str:
        """Halts the current capture block.

        Returns
        -------
        cbid : str
            Capture-block ID that was stopped
        """
        if self.product is None:
            raise FailReply('Have not yet configured')
        cbid = await self.product.capture_done()
        return cbid
