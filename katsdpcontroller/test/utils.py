"""Utilities for unit tests"""

import asyncio
import functools
import inspect
import sys
import unittest
from unittest import mock
from typing import (List, Tuple, Iterable, Coroutine, Awaitable, Optional,
                    Type, Any, TypeVar, Generic)
from types import TracebackType

import aiokatcp
import asynctest
import async_timeout
from nose.tools import assert_raises, assert_equal, assert_true


_T = TypeVar('_T')

# The "gpucbf" correlator components are not connected up to any SDP components.
# They're there just as a smoke test for generator.py.
CONFIG = '''{
    "version": "3.1",
    "outputs": {
        "gpucbf_m900v": {
            "type": "sim.dig.raw_antenna_voltage",
            "band": "l",
            "adc_sample_rate": 1712000000.0,
            "centre_frequency": 1284000000.0,
            "antenna": "m900, 0:0:0, 0:0:0, 0, 0"
        },
        "gpucbf_m900h": {
            "type": "sim.dig.raw_antenna_voltage",
            "band": "l",
            "adc_sample_rate": 1712000000.0,
            "centre_frequency": 1284000000.0,
            "antenna": "m900, 0:0:0, 0:0:0, 0, 0"
        },
        "gpucbf_m901v": {
            "type": "sim.dig.raw_antenna_voltage",
            "band": "l",
            "adc_sample_rate": 1712000000.0,
            "centre_frequency": 1284000000.0,
            "antenna": "m901, 0:0:0, 0:0:0, 0, 0"
        },
        "gpucbf_m901h": {
            "type": "sim.dig.raw_antenna_voltage",
            "band": "l",
            "adc_sample_rate": 1712000000.0,
            "centre_frequency": 1284000000.0,
            "antenna": "m901, 0:0:0, 0:0:0, 0, 0"
        },
        "gpucbf_antenna_channelised_voltage": {
            "type": "gpucbf.antenna_channelised_voltage",
            "src_streams": ["gpucbf_m900v", "gpucbf_m900h", "gpucbf_m901v", "gpucbf_m901h"],
            "n_chans": 4096
        },
        "gpucbf_baseline_correlation_products": {
            "type": "gpucbf.baseline_correlation_products",
            "src_streams": ["gpucbf_antenna_channelised_voltage"],
            "int_time": 0.5
        },

        "i0_antenna_channelised_voltage": {
            "type": "sim.cbf.antenna_channelised_voltage",
            "antennas": [
                "m000, -30:42:39.8, 21:26:38.0, 1035.0, 13.5, -8.258 -207.289 1.2075 5874.184 5875.444, -0:00:39.7 0 -0:04:04.4 -0:04:53.0 0:00:57.8 -0:00:13.9 0:13:45.2 0:00:59.8, 1.14",
                "m001, -30:42:39.8, 21:26:38.0, 1035.0, 13.5, 1.126 -171.761 1.0605 5868.979 5869.998, -0:42:08.0 0 0:01:44.0 0:01:11.9 -0:00:14.0 -0:00:21.0 -0:36:13.1 0:01:36.2, 1.14",
                "m062, -30:42:39.8, 21:26:38.0, 1035.0, 13.5, -1440.6235 -2503.7705 14.288 5932.94 5934.732, -0:15:23.0 0 0:00:04.6 -0:03:30.4 0:01:12.2 0:00:37.5 0:00:15.6 0:01:11.8, 1.14",
                "m063, -30:42:39.8, 21:26:38.0, 1035.0, 13.5, -3419.58 -1840.4655 9.005 5684.815 5685.969, -0:59:43.2 0 0:01:58.6 0:01:49.8 0:01:23.3 0:02:04.6 -0:08:15.7 0:03:47.1, 1.14"
            ],
            "n_chans": 4096,
            "adc_sample_rate": 1712000000.0,
            "bandwidth": 856000000.0,
            "centre_frequency": 1284000000.0,
            "band": "l"
        },
        "i0_baseline_correlation_products": {
            "type": "sim.cbf.baseline_correlation_products",
            "n_endpoints": 16,
            "src_streams": ["i0_antenna_channelised_voltage"],
            "int_time": 0.499,
            "n_chans_per_substream": 256
        },
        "i0_tied_array_channelised_voltage_0x": {
            "type": "sim.cbf.tied_array_channelised_voltage",
            "n_endpoints": 16,
            "src_streams": ["i0_antenna_channelised_voltage"],
            "spectra_per_heap": 256,
            "n_chans_per_substream": 256
        },
        "i0_tied_array_channelised_voltage_0y": {
            "type": "sim.cbf.tied_array_channelised_voltage",
            "n_endpoints": 16,
            "src_streams": ["i0_antenna_channelised_voltage"],
            "spectra_per_heap": 256,
            "n_chans_per_substream": 256
        },
        "sdp_l0": {
            "type": "sdp.vis",
            "src_streams": ["i0_baseline_correlation_products"],
            "output_int_time": 4.0,
            "continuum_factor": 1,
            "archive": true
        },
        "sdp_l0_continuum": {
            "type": "sdp.vis",
            "src_streams": ["i0_baseline_correlation_products"],
            "output_int_time": 4.0,
            "continuum_factor": 16,
            "archive": true
        },
        "sdp_l0_spectral_only": {
            "type": "sdp.vis",
            "src_streams": ["i0_baseline_correlation_products"],
            "output_int_time": 1.9,
            "continuum_factor": 1,
            "archive": true
        },
        "sdp_l0_continuum_only": {
            "type": "sdp.vis",
            "src_streams": ["i0_baseline_correlation_products"],
            "output_int_time": 2.1,
            "continuum_factor": 16,
            "output_channels": [117, 3472],
            "excise": false,
            "archive": true
        },
        "sdp_beamformer": {
            "type": "sdp.beamformer",
            "src_streams": [
                "i0_tied_array_channelised_voltage_0x",
                "i0_tied_array_channelised_voltage_0y"
            ]
        },
        "sdp_beamformer_engineering_ssd": {
            "type": "sdp.beamformer_engineering",
            "src_streams": [
                "i0_tied_array_channelised_voltage_0x",
                "i0_tied_array_channelised_voltage_0y"
            ],
            "output_channels": [0, 4096],
            "store": "ssd"
        },
        "sdp_beamformer_engineering_ram": {
            "type": "sdp.beamformer_engineering",
            "src_streams": [
                "i0_tied_array_channelised_voltage_0x",
                "i0_tied_array_channelised_voltage_0y"
            ],
            "output_channels": [0, 4096],
            "store": "ram"
        },
        "cal": {
            "type": "sdp.cal",
            "src_streams": ["sdp_l0"],
            "buffer_time": 1800.0
        },
        "sdp_l1_flags": {
            "type": "sdp.flags",
            "src_streams": ["sdp_l0", "cal"],
            "archive": true
        },
        "sdp_l1_flags_continuum": {
            "type": "sdp.flags",
            "src_streams": ["sdp_l0_continuum", "cal"],
            "archive": true
        },
        "continuum_image": {
            "type": "sdp.continuum_image",
            "src_streams": ["sdp_l1_flags_continuum"]
        },
        "spectral_image": {
            "type": "sdp.spectral_image",
            "src_streams": ["sdp_l1_flags"],
            "output_channels": [510, 520],
            "parameters": {
                "major": 6,
                "major_gain": 0.15
            }
        }
    },
    "config": {}
}'''     # noqa: E501

S3_CONFIG = '''
{
    "continuum": {
        "read": {
            "access_key": "not-really-an-access-key",
            "secret_key": "tellno1"
        },
        "write": {
            "access_key": "another-fake-key",
            "secret_key": "s3cr3t"
        },
        "url": "http://continuum.s3.invalid/",
        "expiry_days": 7
    },
    "spectral": {
        "read": {
            "access_key": "not-really-an-access-key",
            "secret_key": "tellno1"
        },
        "write": {
            "access_key": "another-fake-key",
            "secret_key": "s3cr3t"
        },
        "url": "http://spectral.s3.invalid/",
        "expiry_days": 7
    },
    "archive": {
        "read": {
            "url": "http://archive.s3.invalid/"
        }
    },
    "models": {
        "read": {
            "url": "https://models.s3.invalid/models"
        }
    }
}'''

EXPECTED_PRODUCT_CONTROLLER_SENSOR_LIST: List[Tuple[bytes, ...]] = [
    (b'device-status', b'', b'discrete', b'ok', b'degraded', b'fail'),
    (b'gui-urls', b'', b'string')
]

EXPECTED_INTERFACE_SENSOR_LIST: List[Tuple[bytes, ...]] = [
    (b'bf_ingest.beamformer.1.port', b'', b'address'),
    (b'ingest.sdp_l0.1.capture-active', b'', b'boolean'),
    (b'timeplot.sdp_l0.1.gui-urls', b'', b'string'),
    (b'timeplot.sdp_l0.1.html_port', b'', b'address'),
    (b'cal.1.capture-block-state', b'', b'string'),
    (b'state', b'', b'discrete',
     b'configuring', b'idle', b'capturing', b'deconfiguring', b'dead', b'error', b'postprocessing'),
    (b'capture-block-state', b'', b'string')
]


def create_patch(test_case: unittest.TestCase, *args, **kwargs) -> Any:
    """Wrap mock.patch such that it will be unpatched as part of test case cleanup"""
    patcher = asynctest.patch(*args, **kwargs)
    mock_obj = patcher.start()
    test_case.addCleanup(patcher.stop)
    return mock_obj


async def assert_request_fails(client: aiokatcp.Client, name: str, *args: Any) -> None:
    with assert_raises(aiokatcp.FailReply):
        await client.request(name, *args)


async def assert_sensor_value(
        client: aiokatcp.Client, name: str, value: Any,
        status: aiokatcp.Sensor.Status = aiokatcp.Sensor.Status.NOMINAL) -> None:
    encoded = aiokatcp.encode(value)
    reply, informs = await client.request("sensor-value", name)
    assert_equal(informs[0].arguments[4], encoded)
    assert_equal(informs[0].arguments[3], aiokatcp.encode(status))


async def assert_sensors(client: aiokatcp.Client,
                         expected_list: Iterable[Tuple[bytes, ...]]) -> None:
    expected = {item[0]: item[1:] for item in expected_list}
    reply, informs = await client.request("sensor-list")
    actual = {}
    for inform in informs:
        # Skip the description
        actual[inform.arguments[0]] = tuple(inform.arguments[2:])
    assert_equal(expected, actual)


class DelayedManager:
    """Asynchronous context manager that runs its block with a task in progress.

    The `mock` is modified to return a future that only resolves to
    `return_value` after exiting the context manager completed (the first
    time it is called).

    If `cancelled` is true, the request is expected to fail with a message
    about being cancelled, otherwise it is expected to succeed.
    """
    def __init__(self, coro: Coroutine, mock: mock.Mock, return_value: Any,
                 cancelled: bool) -> None:
        # Set when the call to the mock is made
        self._started: asyncio.Future[None] = asyncio.Future()
        self.mock = mock
        self.return_value = return_value
        self.cancelled = cancelled
        # Set to return_value when exiting the manager
        self._result: asyncio.Future[Any] = asyncio.Future()
        self._old_side_effect = mock.side_effect
        mock.side_effect = self._side_effect
        self._request_task = asyncio.get_event_loop().create_task(coro)

    async def _side_effect(self, *args, **kwargs) -> Any:
        self._started.set_result(None)
        self.mock.side_effect = self._old_side_effect
        return await self._result

    async def __aenter__(self) -> 'DelayedManager':
        await self._started
        return self

    async def __aexit__(self, exc_type: Optional[Type[BaseException]],
                        exc_value: Optional[BaseException],
                        traceback: Optional[TracebackType]) -> None:
        # Unblock the mock call
        if not self._result.cancelled():
            self._result.set_result(self.return_value)
        if exc_type:
            # If we already have an exception, don't make more assertions
            self._request_task.cancel()
            return
        if self.cancelled:
            with assert_raises(aiokatcp.FailReply) as cm:
                await self._request_task
            assert_equal('request cancelled', str(cm.exception))
        else:
            await self._request_task     # Will raise if it failed


class Background(Generic[_T]):
    """Asynchronous context manager that runs its argument in a separate task.

    It can also be used as a normal context manager, but in that case it
    asserts that the task is finished when it exits.

    Parameters
    ----------
    awaitable
        Coroutine to run in a separate task (or any future).

    Example
    -------
    .. code:: python

        async with Background(my_coro()) as cm:
            await asyncio.sleep(1)
        print(cm.result)
    """

    def __init__(self, awaitable: Awaitable[_T]) -> None:
        self._future = asyncio.ensure_future(awaitable)

    def __enter__(self) -> 'Background':
        return self

    def __exit__(self, exc_type: Optional[Type[BaseException]],
                 exc_value: Optional[BaseException],
                 traceback: Optional[TracebackType]) -> None:
        if not exc_type:
            assert_true(self._future.done())

    async def __aenter__(self) -> 'Background':
        return self

    async def __aexit__(self, exc_type: Optional[Type[BaseException]],
                        exc_value: Optional[BaseException],
                        traceback: Optional[TracebackType]) -> None:
        if not exc_type:
            await self._future

    @property
    def result(self) -> _T:
        return self._future.result()


async def run_clocked(test_case: asynctest.ClockedTestCase, time: float,
                      awaitable: Awaitable[_T]) -> _T:
    """Run a coroutine while advancing the clock on a clocked test case.

    This is useful if the implementation of the `awaitable` sleeps and hence
    needs the time advanced to make progress.
    """
    with Background(awaitable) as cm:
        await test_case.advance(time)
    return cm.result


def future_return(mock: mock.Mock) -> asyncio.Future:
    """Modify a callable mock so that it blocks on a future then returns it."""
    async def replacement(*args, **kwargs):
        return await future

    future = asyncio.Future()         # type: asyncio.Future[Any]
    mock.side_effect = replacement
    return future


def timelimit(limit=5.0):
    """Decorator to run tests with a time limit. It is designed to be used
    with :class:`asynctest.TestCase`. It can be used as either a method or
    a class decorator. It can be used as either ``@timelimit(limit)`` or
    just ``@timelimit`` to use the default of 5 seconds.
    """
    if inspect.isfunction(limit) or inspect.isclass(limit):
        # Used without parameters
        return timelimit()(limit)

    def decorator(arg):
        if inspect.isclass(arg):
            for key, value in arg.__dict__.items():
                if (inspect.iscoroutinefunction(value) and key.startswith('test_')
                        and not hasattr(arg, '_timelimit')):
                    setattr(arg, key, decorator(value))
            return arg
        else:
            @functools.wraps(arg)
            async def wrapper(self, *args, **kwargs):
                try:
                    async with async_timeout.timeout(limit, loop=self.loop) as cm:
                        await arg(self, *args, **kwargs)
                except asyncio.TimeoutError:
                    if not cm.expired:
                        raise
                    for task in asyncio.Task.all_tasks(loop=self.loop):
                        if task.get_stack(limit=1):
                            print()
                            task.print_stack(file=sys.stdout)
                    raise asyncio.TimeoutError('Test did not complete within {}s'.format(limit))
            wrapper._timelimit = limit
            return wrapper
    return decorator
