"""Utilities for unit tests"""

import asyncio
import unittest
from unittest import mock
from typing import (List, Tuple, Iterable, Coroutine, Awaitable, Optional,
                    Type, Any, TypeVar, Generic)
from types import TracebackType

import aiokatcp
import asynctest
from nose.tools import assert_raises, assert_equal, assert_true


_T = TypeVar('_T')

CONFIG = '''{
    "version": "2.6",
    "inputs": {
        "camdata": {
            "type": "cam.http",
            "url": "http://127.0.0.1:8999"
        },
        "i0_antenna_channelised_voltage": {
            "type": "cbf.antenna_channelised_voltage",
            "url": "spead://239.102.252.0+15:7148",
            "antennas": ["m000", "m001", "m063", "m064"],
            "n_chans": 4096,
            "n_pols": 2,
            "adc_sample_rate": 1712000000.0,
            "bandwidth": 856000000.0,
            "n_samples_between_spectra": 8192,
            "instrument_dev_name": "i0"
        },
        "i0_baseline_correlation_products": {
            "type": "cbf.baseline_correlation_products",
            "url": "spead://239.102.255.0+15:7148",
            "src_streams": ["i0_antenna_channelised_voltage"],
            "int_time": 0.499,
            "n_bls": 40,
            "xeng_out_bits_per_sample": 32,
            "n_chans_per_substream": 256,
            "instrument_dev_name": "i0",
            "simulate": {
                "center_freq": 1284000000.0,
                "sources": ["PKS 1934-638, radec, 19:39:25.03, -63:42:45.63"]
            }
        },
        "i0_tied_array_channelised_voltage_0x": {
            "type": "cbf.tied_array_channelised_voltage",
            "url": "spead://239.102.254.1+15:7148",
            "src_streams": ["i0_antenna_channelised_voltage"],
            "spectra_per_heap": 256,
            "n_chans_per_substream": 256,
            "beng_out_bits_per_sample": 8,
            "instrument_dev_name": "i0"
        },
        "i0_tied_array_channelised_voltage_0y": {
            "type": "cbf.tied_array_channelised_voltage",
            "url": "spead://239.102.253.1+15:7148",
            "src_streams": ["i0_antenna_channelised_voltage"],
            "spectra_per_heap": 256,
            "n_chans_per_substream": 256,
            "beng_out_bits_per_sample": 8,
            "instrument_dev_name": "i0"
        }
    },
    "outputs": {
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
            "src_streams": ["sdp_l0"],
            "calibration": ["cal"],
            "archive": true
        },
        "sdp_l1_flags_continuum": {
            "type": "sdp.flags",
            "src_streams": ["sdp_l0_continuum"],
            "calibration": ["cal"],
            "archive": true
        },
        "continuum_image": {
            "type": "sdp.continuum_image",
            "src_streams": ["sdp_l1_flags_continuum"]
        },
        "spectral_image": {
            "type": "sdp.spectral_image",
            "src_streams": ["sdp_l1_flags"],
            "output_channels": [120, 130],
            "parameters": {
                "major": 6,
                "major_gain": 0.15
            }
        }
    },
    "config": {}
}'''

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

    def _side_effect(self, *args, **kwargs) -> asyncio.Future:
        self._started.set_result(None)
        self.mock.side_effect = self._old_side_effect
        return self._result

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
