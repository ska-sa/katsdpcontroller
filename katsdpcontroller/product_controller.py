"""Control of a single subarray product"""

import asyncio
import logging
import json
import time
import os
import re
import copy
import functools
import itertools
import numbers
from typing import Dict, Set, List, Tuple, Callable, Sequence, Optional, Mapping

import addict
import jsonschema
import networkx
import aiokatcp
from aiokatcp import FailReply, Sensor
from prometheus_client import Gauge, Counter, Histogram, CollectorRegistry, REGISTRY
import yarl
import katsdptelstate.aio.memory
import katsdptelstate.aio.redis
import katsdpmodels.fetch.aiohttp

import katsdpcontroller
from . import scheduler, product_config, generator, tasks, sensor_proxy
from .consul import ConsulService
from .controller import (load_json_dict, log_task_exceptions,
                         DeviceStatus, device_status_to_sensor_status, ProductState)
from .defaults import LOCALHOST
from .tasks import (CaptureBlockState, KatcpTransition, DEPENDS_INIT,
                    POSTPROCESSING_TIME_BUCKETS, POSTPROCESSING_REL_BUCKETS)
from .product_config import Configuration


BATCH_PRIORITY = 1        #: Scheduler priority for batch queues
BATCH_RESOURCES_TIMEOUT = 7 * 86400   # A week
_HINT_RE = re.compile(r'\bprometheus: *(?P<type>[a-z]+)(?:\((?P<args>[^)]*)\)|\b)'
                      r'(?: +labels: *(?P<labels>[a-z,]+))?',
                      re.IGNORECASE)
POSTPROCESSING_TIME = Histogram(
    'katsdpcontroller_postprocessing_time_seconds',
    'Wall-clock time for postprocessing each capture block (including burndown phase)',
    buckets=POSTPROCESSING_TIME_BUCKETS)
POSTPROCESSING_TIME_REL = Histogram(
    'katsdpcontroller_postprocessing_time_rel',
    'Wall-clock time for postprocessing each capture block (including burndown phase)'
    ', relative to observation time',
    buckets=POSTPROCESSING_REL_BUCKETS)
logger = logging.getLogger(__name__)


def _redact_arg(arg: str, s3_config: dict) -> str:
    """Process one argument for _redact_keys"""
    for config in s3_config.values():
        for mode in ['read', 'write']:
            if mode in config:
                for name in ['access_key', 'secret_key']:
                    key = config[mode].get(name)
                    if key and (arg == key or arg.endswith('=' + key)):
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


def _normalise_s3_config(s3_config: dict) -> dict:
    """Normalise s3_config to have separate `url` fields for `read` and `write`."""
    s3_config = copy.deepcopy(s3_config)
    for config in s3_config.values():
        if 'url' in config:
            for mode in ['read', 'write']:
                config.setdefault(mode, {})['url'] = config['url']
            del config['url']
    return s3_config


def _prometheus_factory(registry: CollectorRegistry,
                        sensor: aiokatcp.Sensor) -> Optional[sensor_proxy.PrometheusInfo]:
    assert sensor.description is not None
    match = _HINT_RE.search(sensor.description)
    if not match:
        return None
    type_ = match.group('type').lower()
    args_ = match.group('args')
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
                class_ = functools.partial(Histogram, buckets=buckets)
            except ValueError as exc:
                logger.warning('Could not parse histogram buckets (%s): %s', sensor.name, exc)
    else:
        logger.warning('Ignoring unknown Prometheus metric type %s for %s', type_, sensor.name)
        return None
    parts = sensor.name.rsplit('.')
    base = parts.pop()
    label_names = (match.group('labels') or '').split(',')
    label_names = [label for label in label_names if label]    # ''.split(',') is [''], want []
    if len(parts) < len(label_names):
        logger.warning('Not enough parts in name %s for labels %s', sensor.name, label_names)
        return None
    service_parts = len(parts) - len(label_names)
    service = '.'.join(parts[:service_parts])
    labels = dict(zip(label_names, parts[service_parts:]))
    if service:
        labels['service'] = service
    normalised_name = 'katsdpcontroller_' + base.replace('-', '_')
    return sensor_proxy.PrometheusInfo(class_, normalised_name, sensor.description,
                                       labels, registry)


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
        raise ValueError('Absolute URLs expected')
    if base.query_string or target.query_string:
        raise ValueError('Query strings are not supported')
    if base.fragment or target.fragment:
        raise ValueError('Fragments are not supported')
    if base.origin() != target.origin():
        raise ValueError('URLs have different origins')
    # Strip off the last component, which is empty if the URL ends with a /
    # other than at the root, or the filename if it does not.
    base_parts = base.raw_parts[:-1] if len(base.parts) > 1 else base.raw_parts
    if target.raw_parts[:len(base_parts)] != base_parts:
        raise ValueError('Target URL is not nested under the base')
    rel_parts = target.raw_parts[len(base_parts):]
    rel = yarl.URL.build(path='/'.join(rel_parts), encoded=True)
    assert base.join(rel) == target
    return rel


async def _resolve_model(fetcher: katsdpmodels.fetch.aiohttp.Fetcher,
                         base_url: str, rel_url: str) -> Tuple[str, str]:
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
    return (str(_relative_url(base, yarl.URL(config))),
            str(_relative_url(base, yarl.URL(fixed))))


def _format_complex(value: numbers.Complex) -> str:
    """Format a complex number for a katcp request.

    This is copied from katgpucbf.
    """
    return f"{value.real}{value.imag:+}j"


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
                 service_overrides: Mapping[str, product_config.ServiceOverride],
                 s3_config: dict,
                 localhost: bool) -> None:
        super().__init__(image_resolver, task_id_allocator, http_url)
        self.service_overrides = service_overrides
        self.s3_config = s3_config
        self.telstate: Optional[katsdptelstate.aio.TelescopeState] = None
        self.resources: Optional[SDPResources] = None
        self.localhost = localhost


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


class TaskStats(scheduler.TaskStats):
    def __init__(self) -> None:
        def add_sensor(name: str, description: str) -> None:
            self.sensors.add(aiokatcp.Sensor(int, name, description,
                                             initial_status=aiokatcp.Sensor.Status.NOMINAL))

        def add_counter(name: str, description: str) -> None:
            add_sensor(name, description + ' (prometheus: counter)')

        self.sensors = aiokatcp.SensorSet()
        for queue in ['default', 'batch']:
            for state in scheduler.TaskState:
                name = state.name.lower()
                add_sensor(
                    f'{queue}.{name}.tasks-in-state',
                    f'Number of tasks in queue {queue} and state {state.name} '
                    '(prometheus: gauge labels: queue,state)')
        add_counter('batch-tasks-created',
                    'Number of batch tasks that have been created')
        add_counter('batch-tasks-started',
                    'Number of batch tasks that have become ready to start')
        add_counter('batch-tasks-skipped',
                    'Number of batch tasks that were skipped because a dependency failed')
        add_counter('batch-tasks-done',
                    'Number of completed batch tasks (including failed and skipped)')
        add_counter('batch-tasks-retried',
                    'Number of batch tasks that failed and were rescheduled')
        add_counter('batch-tasks-failed',
                    'Number of batch tasks that failed (after all retries)')

    def task_state_changes(self, changes: Mapping[scheduler.LaunchQueue,
                                                  Mapping[scheduler.TaskState, int]]) -> None:
        now = time.time()
        for queue, deltas in changes.items():
            for state, delta in deltas.items():
                state_name = state.name.lower()
                sensor_name = f'{queue.name}.{state_name}.tasks-in-state'
                sensor = self.sensors.get(sensor_name)
                if sensor:
                    sensor.set_value(sensor.value + delta, timestamp=now)

    def batch_tasks_created(self, n_tasks: int) -> None:
        self.sensors['batch-tasks-created'].value += n_tasks

    def batch_tasks_started(self, n_tasks: int) -> None:
        self.sensors['batch-tasks-started'].value += n_tasks

    def batch_tasks_skipped(self, n_tasks: int) -> None:
        self.sensors['batch-tasks-skipped'].value += n_tasks

    def batch_tasks_retried(self, n_tasks: int) -> None:
        self.sensors['batch-tasks-retried'].value += n_tasks

    def batch_tasks_failed(self, n_tasks: int) -> None:
        self.sensors['batch-tasks-failed'].value += n_tasks

    def batch_tasks_done(self, n_tasks: int) -> None:
        self.sensors['batch-tasks-done'].value += n_tasks


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

    This is a base class that is intended to be subclassed. The methods whose
    names end in ``_impl`` are extension points that should be implemented in
    subclasses to do the real work. These methods are run as part of the
    asynchronous operations. They need to be cancellation-safe, to allow for
    forced deconfiguration to abort them.
    """

    def __init__(self, sched: scheduler.SchedulerBase,
                 configuration: Configuration,
                 config_dict: dict,
                 resolver: Resolver,
                 subarray_product_id: str,
                 sdp_controller: 'DeviceServer') -> None:
        #: Current background task (can only be one)
        self._async_task: Optional[asyncio.Task] = None
        self.sched = sched
        self.configuration = configuration
        self.config_dict = config_dict
        self.resolver = resolver
        self.subarray_product_id = subarray_product_id
        self.sdp_controller = sdp_controller
        self.logical_graph = generator.build_logical_graph(configuration, config_dict)
        self.telstate_endpoint = ""
        self.telstate: Optional[katsdptelstate.aio.TelescopeState] = None
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
            str, "capture-block-state",
            "JSON dictionary of capture block states for active capture blocks",
            default="{}", initial_status=Sensor.Status.NOMINAL)
        self._state_sensor = Sensor(
            ProductState, "state",
            "State of the subarray product state machine (prometheus: gauge)",
            status_func=_error_on_error)
        self._device_status_sensor = sdp_controller.sensors['device-status']

        self.state = ProductState.CONFIGURING   # This sets the sensor
        self.add_sensor(self._capture_block_sensor)
        self.add_sensor(self._state_sensor)
        logger.info("Created: %r", self)
        logger.info('Logical graph nodes:\n'
                    + '\n'.join(repr(node) for node in self.logical_graph))

    @property
    def state(self) -> ProductState:
        return self._state

    @state.setter
    def state(self, value: ProductState) -> None:
        if (self._state == ProductState.ERROR
                and value not in (ProductState.DECONFIGURING, ProductState.DEAD)):
            return      # Never leave error state other than by deconfiguring
        now = time.time()
        if value == ProductState.ERROR and self._state != value:
            self._device_status_sensor.set_value(DeviceStatus.FAIL, timestamp=now)
        self._state = value
        self._state_sensor.set_value(value, timestamp=now)

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

    def _fail_if_no_telstate(self) -> None:
        """Raise a FailReply if there is no telescope state."""
        if self.telstate is None:
            raise FailReply(f'Subarray product {self.subarray_product_id} has no SDP components.')

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
                logger.exception("Failed to issue capture-done during shutdown request. "
                                 "Will continue with graph shutdown.")

        if force:
            for capture_block in list(self.capture_blocks.values()):
                if capture_block.postprocess_task is not None:
                    logger.warning('Cancelling postprocessing for capture block %s',
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

        if self.telstate is not None:
            self.telstate.backend.close()

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
        done_exc: Optional[Exception] = None
        assert capture_block is not None
        try:
            await self.capture_done_impl(capture_block)
            if self.state == ProductState.ERROR and not error_expected:
                raise FailReply('Subarray product went into ERROR while stopping capture')
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
            self.postprocess_impl(capture_block))
        log_task_exceptions(
            capture_block.postprocess_task, logger,
            "Exception in postprocessing for {}/{}".format(self.subarray_product_id,
                                                           capture_block.name))

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
        assert self.state == ProductState.CONFIGURING, \
            "configure should be the first thing to happen"
        task = asyncio.get_event_loop().create_task(self._configure())
        log_task_exceptions(task, logger,
                            f"Configuring subarray product {self.subarray_product_id} failed")
        self._async_task = task
        try:
            await task
        finally:
            self._clear_async_task(task)
        logger.info('Subarray product %s successfully configured', self.subarray_product_id)
        # A number of sensors are added without individually triggering interface-changed
        # notifications, so that subscribers don't try to repeatly refresh
        # their sensor lists.
        self.sdp_controller.mass_inform('interface-changed', 'sensor-list')

    async def deconfigure(self, force: bool = False) -> None:
        """Start deconfiguration of the subarray, but does not wait for it to complete."""
        if self.state == ProductState.DEAD:
            return
        if self.async_busy:
            if not force:
                self._fail_if_busy()
            else:
                logger.warning('Subarray product %s is busy with an operation, '
                               'but deconfiguring anyway', self.subarray_product_id)

        if self.state not in (ProductState.IDLE, ProductState.ERROR):
            if not force:
                raise FailReply('Subarray product is not idle and thus cannot be deconfigured. '
                                'Please issue capture_done first.')
            else:
                logger.warning('Subarray product %s is in state %s, but deconfiguring anyway',
                               self.subarray_product_id, self.state.name)
        logger.info("Deconfiguring subarray product %s", self.subarray_product_id)

        ready = asyncio.Event()
        task = asyncio.get_event_loop().create_task(self._deconfigure(force, ready))
        log_task_exceptions(task, logger,
                            f"Deconfiguring {self.subarray_product_id} failed")
        # Make sure that ready gets unblocked even if task throws.
        task.add_done_callback(lambda future: ready.set())
        task.add_done_callback(self._clear_async_task)
        if await self._replace_async_task(task):
            await ready.wait()
        # We don't wait for task to complete, but if it's already done we
        # pass back any exceptions.
        if task.done():
            await task

    async def capture_init(self, capture_block_id: str, configuration: Configuration) -> str:
        self._fail_if_busy()
        if self.state != ProductState.IDLE:
            raise FailReply('Subarray product {} is currently in state {}, not IDLE as expected. '
                            'Cannot be inited.'.format(self.subarray_product_id, self.state.name))
        self._fail_if_no_telstate()
        logger.info('Using capture block ID %s', capture_block_id)

        capture_block = CaptureBlock(capture_block_id, configuration)
        task = asyncio.get_event_loop().create_task(self._capture_init(capture_block))
        self._async_task = task
        try:
            await task
        finally:
            self._clear_async_task(task)
        logger.info('Started capture block %s on subarray product %s',
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
        logger.info('Finished capture block %s on subarray product %s',
                    capture_block_id, self.subarray_product_id)
        return capture_block_id

    def write_graphs(self, output_dir: str) -> None:
        """Write visualisations to `output_dir`."""
        for name in ['ready', 'init', 'kill', 'resolve', 'resources']:
            if name != 'resources':
                g = scheduler.subgraph(self.logical_graph, 'depends_' + name)
            else:
                g = scheduler.subgraph(self.logical_graph,
                                       scheduler.SchedulerBase.depends_resources)
            g = networkx.relabel_nodes(g, {node: node.name for node in g})
            g = networkx.drawing.nx_pydot.to_pydot(g)
            filename = os.path.join(output_dir,
                                    '{}_{}.svg'.format(self.subarray_product_id, name))
            try:
                g.write_svg(filename)
            except OSError as error:
                logger.warning('Could not write %s: %s', filename, error)

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

    async def set_delays(
            self,
            stream_name: str,
            timestamp: aiokatcp.Timestamp,
            coefficient_sets: Sequence[str]) -> None:
        """Set F-engine delays."""
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
        await self.set_delays_impl(stream, timestamp, coefficient_sets)

    async def set_delays_impl(
            self,
            stream: product_config.GpucbfAntennaChannelisedVoltageStream,
            timestamp: aiokatcp.Timestamp,
            coefficient_sets: Sequence[str]) -> None:
        """Back-end implementation of :meth:`set_delays`.

        This method can assume that the inputs and current state are valid.
        """
        pass

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
        return await self.gain_impl(stream, input, values)

    async def gain_impl(
            self,
            stream: product_config.GpucbfAntennaChannelisedVoltageStream,
            input: str,
            values: Sequence[str]) -> Sequence[str]:
        """Back-end implementation of :meth:`gain`.

        This method can assume that the inputs and current state are valid.
        """
        raise NotImplementedError()

    def __repr__(self) -> str:
        return "Subarray product {} (State: {})".format(self.subarray_product_id, self.state.name)


class _IndexedKey(dict):
    """Wrapper class indicating that the contents form a telstate indexed key.

    This is used in dictionaries containing initial values to be placed into
    telstate.
    """
    pass


class SDPSubarrayProduct(SDPSubarrayProductBase):
    """Subarray product that actually launches nodes."""

    def _instantiate(self, logical_node: scheduler.LogicalNode,
                     capture_block_id: Optional[str]) -> scheduler.PhysicalNode:
        if getattr(logical_node, 'sdp_physical_factory', False):
            return logical_node.physical_factory(
                logical_node, self.sdp_controller, self, capture_block_id)
        return logical_node.physical_factory(logical_node)

    def _instantiate_physical_graph(self, logical_graph: networkx.MultiDiGraph,
                                    capture_block_id: str = None) -> networkx.MultiDiGraph:
        mapping = {logical: self._instantiate(logical, capture_block_id)
                   for logical in logical_graph}
        return networkx.relabel_nodes(logical_graph, mapping)

    def __init__(self, sched: scheduler.SchedulerBase,
                 configuration: Configuration,
                 config_dict: dict,
                 resolver: Resolver, subarray_product_id: str,
                 sdp_controller: 'DeviceServer',
                 telstate_name: str = 'telstate') -> None:
        super().__init__(sched, configuration, config_dict, resolver,
                         subarray_product_id, sdp_controller)
        # Priority is lower (higher number) than the default queue
        self.batch_queue = scheduler.LaunchQueue(
            sdp_controller.batch_role, 'batch', priority=BATCH_PRIORITY)
        sched.add_queue(self.batch_queue)
        # generate physical nodes
        self.physical_graph = self._instantiate_physical_graph(self.logical_graph)
        # Nodes indexed by logical name
        self._nodes = {node.logical_node.name: node for node in self.physical_graph}
        self.telstate_node = self._nodes.get(telstate_name)
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
                    logger.warning(
                        'Cannot issue %s to %s because there is no katcp connection',
                        reqs[0], node.name)
                else:
                    for req in reqs:
                        await node.issue_req(req.name, req.args, timeout=req.timeout)
            if isinstance(node, tasks.SDPPhysicalTask) and state == node.logical_node.final_state:
                observer = node.capture_block_state_observer
                if observer is not None:
                    logger.info('Waiting for %s on %s', capture_block.name, node.name)
                    await observer.wait_capture_block_done(capture_block.name)
                    logger.info('Done waiting for %s on %s', capture_block.name, node.name)
                else:
                    logger.debug('Task %s has no capture-block-state observer', node.name)
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
        assert self.telstate is not None
        await self.telstate.add('sdp_capture_block_id', capture_block.name)
        for node in self.physical_graph:
            if isinstance(node, tasks.SDPPhysicalTask):
                node.add_capture_block(capture_block)
        await self.exec_transitions(CaptureBlockState.CAPTURING, True, capture_block)

    async def capture_done_impl(self, capture_block: CaptureBlock) -> None:
        await self.exec_transitions(CaptureBlockState.BURNDOWN, False, capture_block)

    async def postprocess_impl(self, capture_block: CaptureBlock) -> None:
        assert self.telstate is not None
        assert self.telstate_node is not None
        try:
            await self.exec_transitions(CaptureBlockState.POSTPROCESSING, False, capture_block)
            capture_block.state = CaptureBlockState.POSTPROCESSING
            logical_graph = await generator.build_postprocess_logical_graph(
                capture_block.configuration, capture_block.name,
                self.telstate, self.telstate_endpoint)
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
            # However, because of this blocking it needs a large resources_timeout,
            # even though it uses no resources.
            await self.sched.launch(physical_graph, self.resolver, [telstate_node],
                                    queue=self.batch_queue,
                                    resources_timeout=BATCH_RESOURCES_TIMEOUT)
            nodelist = [
                node for node in physical_graph
                if isinstance(node, (scheduler.PhysicalTask, scheduler.FakePhysicalTask))
            ]
            await self.sched.batch_run(physical_graph, self.resolver, nodelist,
                                       queue=self.batch_queue,
                                       resources_timeout=BATCH_RESOURCES_TIMEOUT, attempts=3)
        finally:
            init_time = capture_block.state_time[CaptureBlockState.CAPTURING]
            done_time = capture_block.state_time[CaptureBlockState.BURNDOWN]
            observation_time = done_time - init_time
            postprocessing_time = time.time() - done_time
            POSTPROCESSING_TIME.observe(postprocessing_time)
            logger.info('Capture block %s postprocessing finished in %.3fs (obs time: %.3fs)',
                        capture_block.name, postprocessing_time, observation_time,
                        extra=dict(capture_block_id=capture_block.name,
                                   observation_time=observation_time,
                                   postprocessing_time=postprocessing_time))
            # In unit tests the obs time might be zero, which leads to errors here
            if observation_time > 0:
                POSTPROCESSING_TIME_REL.observe(postprocessing_time / observation_time)
            await self.exec_transitions(CaptureBlockState.DEAD, False, capture_block)

    def capture_block_dead_impl(self, capture_block: CaptureBlock) -> None:
        for node in self.physical_graph:
            if isinstance(node, tasks.SDPPhysicalTask):
                node.remove_capture_block(capture_block)

    async def _launch_telstate(self) -> katsdptelstate.aio.TelescopeState:
        """Make sure the telstate node is launched"""
        assert self.telstate_node is not None
        boot = [self.telstate_node]

        init_telstate = copy.deepcopy(self.physical_graph.graph.get('init_telstate', {}))
        init_telstate['subarray_product_id'] = self.subarray_product_id
        init_telstate['config'] = self.physical_graph.graph.get(
            'config', lambda resolver: {})(self.resolver)
        # Provide attributes to describe the relationships between CBF streams
        # and instruments. This could be extracted from sdp_config, but these
        # specific sensors are easier to mock.
        for stream in self.configuration.streams:
            if isinstance(stream, product_config.CbfStream):
                init_telstate[(stream.name, 'instrument_dev_name')] = stream.instrument_dev_name
                if stream.src_streams:
                    init_telstate[(stream.name, 'src_streams')] = [
                        src_stream.name for src_stream in stream.src_streams
                    ]

        # Load canonical model URLs
        if not self.configuration.options.interface_mode:
            model_base_url = self.resolver.s3_config['models']['read']['url']
            if not model_base_url.endswith('/'):
                model_base_url += '/'      # Ensure it is a directory
            init_telstate['sdp_model_base_url'] = model_base_url
            async with katsdpmodels.fetch.aiohttp.Fetcher() as fetcher:
                rfi_mask_model_urls = await _resolve_model(
                    fetcher, model_base_url, 'rfi_mask/current.alias')
                init_telstate[('model', 'rfi_mask', 'config')] = rfi_mask_model_urls[0]
                init_telstate[('model', 'rfi_mask', 'fixed')] = rfi_mask_model_urls[1]
                for stream in itertools.chain(
                        self.configuration.by_class(
                            product_config.AntennaChannelisedVoltageStream),
                        self.configuration.by_class(
                            product_config.SimAntennaChannelisedVoltageStream)):
                    ratio = round(stream.adc_sample_rate / 2 / stream.bandwidth)
                    band_mask_model_urls = await _resolve_model(
                        fetcher, model_base_url,
                        f'band_mask/current/{stream.band}/nb_ratio={ratio}.alias'
                    )
                    prefix: Tuple[str, ...] = (stream.name, 'model', 'band_mask')
                    init_telstate[prefix + ('config',)] = band_mask_model_urls[0]
                    init_telstate[prefix + ('fixed',)] = band_mask_model_urls[1]
                    for group in ['individual', 'cohort']:
                        config_value = _IndexedKey()
                        fixed_value = _IndexedKey()
                        for ant in stream.antennas:
                            pb_model_urls = await _resolve_model(
                                fetcher, model_base_url,
                                f'primary_beam/current/{group}/{ant}/{stream.band}.alias'
                            )
                            config_value[ant] = pb_model_urls[0]
                            fixed_value[ant] = pb_model_urls[1]
                        prefix = (stream.name, 'model', 'primary_beam', group)
                        init_telstate[prefix + ('config',)] = config_value
                        init_telstate[prefix + ('fixed',)] = fixed_value

        logger.debug("Launching telstate. Initial values %s", init_telstate)
        await self.sched.launch(self.physical_graph, self.resolver, boot)
        # connect to telstate store
        telstate_backend: katsdptelstate.aio.backend.Backend
        if self.configuration.options.interface_mode:
            telstate_backend = katsdptelstate.aio.memory.MemoryBackend()
        else:
            self.telstate_endpoint = '{}:{}'.format(self.telstate_node.host,
                                                    self.telstate_node.ports['telstate'])
            telstate_backend = await katsdptelstate.aio.redis.RedisBackend.from_url(
                f'redis://{self.telstate_endpoint}'
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

    def unexpected_death(self, task: tasks.SDPAnyPhysicalTask) -> None:
        logger.warning('Task %s died unexpectedly', task.name)
        if task.logical_node.critical:
            self._go_to_error()

    def bad_device_status(self, task: tasks.SDPAnyPhysicalTask) -> None:
        logger.warning('Task %s has failed (device-status)', task.name)
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
            logger.warning('Attempting to terminate capture block %s', capture_block_id)
            task = asyncio.get_event_loop().create_task(self._capture_done(error_expected=True))
            self._async_task = task
            log_task_exceptions(task, logger,
                                f"Failed to terminate capture block {capture_block_id}")

            def cleanup(task):
                self._clear_async_task(task)
                logger.info('Finished capture block %s on subarray product %s',
                            capture_block_id, self.subarray_product_id)

            task.add_done_callback(cleanup)

        # We don't go to error state from CONFIGURING because we check all
        # nodes at the end of configuration and will fail the configure
        # there; and from DECONFIGURING/POSTPROCESSING we don't want to go to
        # ERROR because that may prevent deconfiguring.
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

                # Register static KATCP sensors.
                for ss in self.physical_graph.graph["static_sensors"].values():
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
                    fail_list = ', '.join(node.logical_node.name for node in died) or 'Some nodes'
                    ret_msg = (f"{fail_list} failed to start. "
                               "Check the error log for specific details.")
                    logger.error(ret_msg)
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
                if telstate is not None:
                    await telstate.add('sdp_task_details', details, immutable=True)
                    await telstate.add('sdp_image_tag', resolver.image_resolver.tag, immutable=True)
                    await telstate.add('sdp_image_overrides', resolver.image_resolver.overrides,
                                       immutable=True)
            except BaseException as exc:
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
            self.state = ProductState.POSTPROCESSING
            ready.set()
            await shutdown_task

    async def set_delays_impl(
            self,
            stream: product_config.GpucbfAntennaChannelisedVoltageStream,
            timestamp: aiokatcp.Timestamp,
            coefficient_sets: Sequence[str]) -> None:
        n_inputs = len(stream.src_streams)
        conns = []
        for i in range(n_inputs // 2):
            node = self._nodes[f"f.{stream.name}.{i}"]
            if node.katcp_connection is None:
                raise FailReply(f"No katcp connection to {node.name}")
            conns.append(node.katcp_connection)

        reqs = []
        for i, conn in enumerate(conns):
            reqs.append(conn.request(
                "delays", timestamp, coefficient_sets[2 * i], coefficient_sets[2 * i + 1]
            ))
        await asyncio.gather(*reqs)

    async def gain_impl(
            self,
            stream: product_config.GpucbfAntennaChannelisedVoltageStream,
            input: str,
            values: Sequence[str]) -> Sequence[str]:
        idx = stream.input_labels.index(input)
        node_idx = idx // 2
        node = self._nodes[f"f.{stream.name}.{node_idx}"]
        if node.katcp_connection is None:
            raise FailReply(f"No katcp connection to {node.name}")
        reply, _ = await node.katcp_connection.request("gain", idx % 2, *values)
        return reply


class DeviceServer(aiokatcp.DeviceServer):
    VERSION = 'product-controller-1.1'
    BUILD_STATE = "katsdpcontroller-" + katsdpcontroller.__version__

    def __init__(self, host: str, port: int, master_controller: aiokatcp.Client,
                 subarray_product_id: str,
                 sched: scheduler.SchedulerBase,
                 batch_role: str,
                 interface_mode: bool,
                 localhost: bool,
                 image_resolver_factory: scheduler.ImageResolverFactory,
                 s3_config: dict,
                 graph_dir: str = None,
                 dashboard_url: str = None,
                 prometheus_registry: CollectorRegistry = REGISTRY,
                 shutdown_delay: float = 10.0) -> None:
        self.sched = sched
        self.subarray_product_id = subarray_product_id
        self.batch_role = batch_role
        self.interface_mode = interface_mode
        self.localhost = localhost
        self.image_resolver_factory = image_resolver_factory
        self.s3_config = _normalise_s3_config(s3_config)
        self.graph_dir = graph_dir
        self.master_controller = master_controller
        self.product: Optional[SDPSubarrayProductBase] = None
        self.shutdown_delay = shutdown_delay

        super().__init__(host, port)
        # setup sensors (note: SDPProductController adds other sensors)
        self.sensors.add(Sensor(DeviceStatus, "device-status",
                                "Devices status of the subarray product controller",
                                default=DeviceStatus.OK,
                                status_func=device_status_to_sensor_status))
        gui_urls: List[Dict[str, str]] = []
        if dashboard_url is not None:
            gui_urls.append({
                "title": "Dashboard",
                "description": "Product controller dashboard",
                "category": "Dashboard",
                "href": dashboard_url
            })
        self.sensors.add(Sensor(str, 'gui-urls', 'URLs for product-wide GUIs',
                                default=json.dumps(gui_urls),
                                initial_status=Sensor.Status.NOMINAL))
        self._prometheus_watcher = sensor_proxy.PrometheusWatcher(
            self.sensors, {'subarray_product_id': subarray_product_id},
            functools.partial(_prometheus_factory, prometheus_registry))
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
            'Name': 'product-controller',
            'Tags': ['prometheus-metrics'],
            'Meta': {
                'subarray_product_id': self.subarray_product_id
            },
            'Port': port,
            'Checks': [
                {
                    "Interval": "15s",
                    "Timeout": "5s",
                    "HTTP": f"http://{LOCALHOST}:{port}/health",
                    "DeregisterCriticalServiceAfter": "90s"
                }
            ]
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
            logger.warning('Product controller interrupted - deconfiguring running product')
            try:
                await self.product.deconfigure(force=True)
            except Exception:
                logger.warning('Failed to deconfigure product %s during shutdown', exc_info=True)
        self.master_controller.close()
        await self.master_controller.wait_closed()

    async def configure_product(self, name: str, configuration: Configuration,
                                config_dict: dict) -> None:
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

        def dead_callback(product):
            if self.shutdown_delay > 0:
                logger.info('Sleeping %.1f seconds to give time for final Prometheus scrapes',
                            self.shutdown_delay)
                asyncio.get_event_loop().call_later(self.shutdown_delay, self.halt, False)
            else:
                self.halt(False)

        logger.debug('config is %s', json.dumps(config_dict, indent=2, sort_keys=True))
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
            scheduler.TaskIDAllocator(name + '-'),
            self.sched.http_url if self.sched else '',
            configuration.options.service_overrides,
            self.s3_config,
            self.localhost)

        # create graph object and build physical graph from specified resources
        product = SDPSubarrayProduct(self.sched, configuration, config_dict, resolver, name, self)
        if self.graph_dir is not None:
            product.write_graphs(self.graph_dir)
        self.product = product   # Prevents another attempt to configure
        self.product.dead_callbacks.append(dead_callback)
        try:
            await product.configure()
        except BaseException:
            self.product = None
            raise

    async def request_product_configure(self, ctx, name: str, config: str) -> None:
        """Configure a SDP subarray product instance.

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
            raise FailReply('Already configured or configuring')
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

    def _get_product(self) -> SDPSubarrayProductBase:
        """Check that self.product exists (i.e. ?product-configure has been called).

        If it has not, raises a :exc:`FailReply`.
        """
        if self.product is None:
            raise FailReply('?product-configure has not been called yet. '
                            'It must be called before other requests.')
        return self.product

    async def request_product_deconfigure(self, ctx, force: bool = False) -> None:
        """Deconfigure the product and shut down the server."""
        await self._get_product().deconfigure(force=force)

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
        product = self._get_product()
        try:
            overrides = load_json_dict(override_dict_json)
        except ValueError as error:
            retmsg = f'Override {override_dict_json} is not a valid JSON dict: {error}'
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
            self, ctx, stream: str, time: aiokatcp.Timestamp, *coefficient_set: str) -> None:
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

    async def request_gain(
            self, ctx, stream: str, input: str, *values: str) -> Tuple[str, ...]:
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
            channels.
        """
        return tuple(await self._get_product().gain(stream, input, values))
