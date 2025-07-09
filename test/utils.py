################################################################################
# Copyright (c) 2013-2023, National Research Foundation (SARAO)
#
# Licensed under the BSD 3-Clause License (the "License"); you may not use
# this file except in compliance with the License. You may obtain a copy
# of the License at
#
#   https://opensource.org/licenses/BSD-3-Clause
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

"""Utilities for unit tests"""

import asyncio
import base64
import json
import uuid
from types import TracebackType
from typing import (
    Any,
    Awaitable,
    Coroutine,
    Generic,
    Iterable,
    List,
    Optional,
    Tuple,
    Type,
    TypeVar,
)
from unittest import mock

import aiokatcp
import pytest
from addict import Dict

_T = TypeVar("_T")

# The "gpucbf" correlator components are not connected up to any SDP components.
# They're there just as a smoke test for generator.py.
CONFIG = """{
    "version": "4.6",
    "outputs": {
        "gpucbf_m900v": {
            "type": "sim.dig.baseband_voltage",
            "band": "l",
            "adc_sample_rate": 1712000000.0,
            "centre_frequency": 1284000000.0,
            "antenna": "m900, 0:0:0, 0:0:0, 0, 0"
        },
        "gpucbf_m900h": {
            "type": "sim.dig.baseband_voltage",
            "band": "l",
            "adc_sample_rate": 1712000000.0,
            "centre_frequency": 1284000000.0,
            "antenna": "m900, 0:0:0, 0:0:0, 0, 0"
        },
        "gpucbf_m901v": {
            "type": "sim.dig.baseband_voltage",
            "band": "l",
            "adc_sample_rate": 1712000000.0,
            "centre_frequency": 1284000000.0,
            "antenna": "m901, 0:0:0, 0:0:0, 0, 0"
        },
        "gpucbf_m901h": {
            "type": "sim.dig.baseband_voltage",
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
        "gpucbf_tied_array_channelised_voltage_0x": {
            "type": "gpucbf.tied_array_channelised_voltage",
            "src_streams": ["gpucbf_antenna_channelised_voltage"],
            "src_pol": 0
        },
        "gpucbf_tied_array_channelised_voltage_0y": {
            "type": "gpucbf.tied_array_channelised_voltage",
            "src_streams": ["gpucbf_antenna_channelised_voltage"],
            "src_pol": 1
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
}"""  # noqa: E501

CONFIG_CBF_ONLY = """{
    "version": "4.6",
    "outputs": {
        "gpucbf_m900v": {
            "type": "sim.dig.baseband_voltage",
            "band": "l",
            "adc_sample_rate": 1712000000.0,
            "centre_frequency": 1284000000.0,
            "antenna": "m900, 0:0:0, 0:0:0, 0, 0"
        },
        "gpucbf_m900h": {
            "type": "sim.dig.baseband_voltage",
            "band": "l",
            "adc_sample_rate": 1712000000.0,
            "centre_frequency": 1284000000.0,
            "antenna": "m900, 0:0:0, 0:0:0, 0, 0"
        },
        "gpucbf_m901v": {
            "type": "sim.dig.baseband_voltage",
            "band": "l",
            "adc_sample_rate": 1712000000.0,
            "centre_frequency": 1284000000.0,
            "antenna": "m901, 0:0:0, 0:0:0, 0, 0"
        },
        "gpucbf_m901h": {
            "type": "sim.dig.baseband_voltage",
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
        "gpucbf_antenna_channelised_voltage_narrowband": {
            "type": "gpucbf.antenna_channelised_voltage",
            "src_streams": ["gpucbf_m900v", "gpucbf_m900h", "gpucbf_m901v", "gpucbf_m901h"],
            "n_chans": 4096,
            "narrowband": {
                "decimation_factor": 8,
                "centre_frequency": 300e6
            }
        },
        "gpucbf_antenna_channelised_voltage_narrowband_vlbi": {
            "type": "gpucbf.antenna_channelised_voltage",
            "src_streams": ["gpucbf_m900v", "gpucbf_m900h", "gpucbf_m901v", "gpucbf_m901h"],
            "n_chans": 4096,
            "narrowband": {
                "decimation_factor": 8,
                "centre_frequency": 300e6,
                "vlbi": {
                    "pass_bandwidth": 64e6
                }
            }
        },
        "gpucbf_baseline_correlation_products": {
            "type": "gpucbf.baseline_correlation_products",
            "src_streams": ["gpucbf_antenna_channelised_voltage"],
            "int_time": 0.5
        },
        "gpucbf_tied_array_channelised_voltage_0x": {
            "type": "gpucbf.tied_array_channelised_voltage",
            "src_streams": ["gpucbf_antenna_channelised_voltage"],
            "src_pol": 0
        },
        "gpucbf_tied_array_channelised_voltage_0y": {
            "type": "gpucbf.tied_array_channelised_voltage",
            "src_streams": ["gpucbf_antenna_channelised_voltage"],
            "src_pol": 1
        }
    },
    "config": {}
}"""

S3_CONFIG = """
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
}"""

EXPECTED_PRODUCT_CONTROLLER_SENSOR_LIST: List[Tuple[bytes, ...]] = [
    (b"device-status", b"", b"discrete", b"ok", b"degraded", b"fail"),
    (b"gui-urls", b"", b"string"),
]

# This is just a subset meant to check common classes of sensors.
# There are too many for it to be practical to maintain a canonical list.
EXPECTED_INTERFACE_SENSOR_LIST: List[Tuple[bytes, ...]] = [
    (b"bf_ingest.sdp_beamformer_engineering_ram.1.port", b"", b"address"),
    (b"bf_ingest.sdp_beamformer_engineering_ram.1.mesos-state", b"", b"string"),
    (
        b"bf_ingest.sdp_beamformer_engineering_ram.2.state",
        b"",
        b"discrete",
        b"not-ready",
        b"starting",
        b"started",
        b"running",
        b"ready",
        b"killing",
        b"dead",
    ),
    (b"cal.1.capture-block-state", b"", b"string"),
    (b"cal.1.gui-urls", b"", b"string"),
    (b"capture-block-state", b"", b"string"),
    (b"gpucbf_antenna_channelised_voltage.bandwidth", b"Hz", b"float"),
    (b"gpucbf_m900h.destination", b"", b"string"),
    (b"gui-urls", b"", b"string"),
    (b"ingest.sdp_l0.1.capture-active", b"", b"boolean"),
    (
        b"product-state",
        b"",
        b"discrete",
        b"configuring",
        b"idle",
        b"capturing",
        b"deconfiguring",
        b"dead",
        b"error",
        b"postprocessing",
    ),
    (b"telstate.telstate", b"", b"address"),
    (b"timeplot.sdp_l0.gui-urls", b"", b"string"),
    (b"timeplot.sdp_l0.html_port", b"", b"address"),
]


async def assert_request_fails(client: aiokatcp.Client, name: str, *args: Any) -> None:
    with pytest.raises(aiokatcp.FailReply):
        await client.request(name, *args)


async def assert_sensor_value(
    client: aiokatcp.Client,
    name: str,
    value: Any,
    status: aiokatcp.Sensor.Status = aiokatcp.Sensor.Status.NOMINAL,
) -> None:
    encoded = mock.ANY if value is mock.ANY else aiokatcp.encode(value)
    reply, informs = await client.request("sensor-value", name)
    assert informs[0].arguments[4] == encoded
    assert informs[0].arguments[3] == aiokatcp.encode(status)


async def assert_sensors(
    client: aiokatcp.Client, expected_list: Iterable[Tuple[bytes, ...]], subset: bool = False
) -> None:
    """Check that the expected sensors are all present.

    The values are not checked. Each sensor is described by the raw data
    returned from the ``?sensor-list`` request, excluding the description.
    If `subset` is true, then it is acceptable for there to be additional
    sensors.
    """
    expected = {item[0]: item[1:] for item in expected_list}
    reply, informs = await client.request("sensor-list")
    actual = {}
    for inform in informs:
        if not subset or inform.arguments[0] in expected:
            # Skip the description
            actual[inform.arguments[0]] = tuple(inform.arguments[2:])
    assert expected == actual


class DelayedManager:
    """Asynchronous context manager that runs its block with a task in progress.

    The `mock` is modified to return a future that only resolves to
    `return_value` after exiting the context manager completed (the first
    time it is called).

    If `cancelled` is true, the request is expected to fail with a message
    about being cancelled, otherwise it is expected to succeed.
    """

    def __init__(
        self, coro: Coroutine, mock: mock.Mock, return_value: Any, cancelled: bool
    ) -> None:
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

    async def __aenter__(self) -> "DelayedManager":
        await self._started
        return self

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None:
        # Unblock the mock call
        if not self._result.cancelled():
            self._result.set_result(self.return_value)
        if exc_type:
            # If we already have an exception, don't make more assertions
            self._request_task.cancel()
            return
        if self.cancelled:
            with pytest.raises(aiokatcp.FailReply, match="request cancelled"):
                await self._request_task
        else:
            await self._request_task  # Will raise if it failed


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

    def __enter__(self) -> "Background":
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None:
        if not exc_type:
            assert self._future.done()

    async def __aenter__(self) -> "Background":
        return self

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None:
        if not exc_type:
            await self._future

    @property
    def result(self) -> _T:
        return self._future.result()


def future_return(mock: mock.Mock) -> asyncio.Future:
    """Modify a callable mock so that it blocks on a future then returns it."""

    async def replacement(*args, **kwargs):
        return await future

    future = asyncio.Future()  # type: asyncio.Future[Any]
    mock.side_effect = replacement
    return future


async def exhaust_callbacks():
    """Run the loop until all immediately-scheduled work has finished.

    This is inspired by :func:`asynctest.exhaust_callbacks`.
    """
    loop = asyncio.get_running_loop()
    while loop._ready:
        await asyncio.sleep(0)


class AnyOrderList(list):
    """Used for asserting that a list is present in a call, but without
    constraining the order. It does not require the elements to be hashable.
    """

    def __eq__(self, other):
        if isinstance(other, list):
            if len(self) != len(other):
                return False
            tmp = list(other)
            for item in self:
                try:
                    tmp.remove(item)
                except ValueError:
                    return False
            return True
        return NotImplemented

    def __ne__(self, other):
        if isinstance(other, list):
            return not (self == other)
        return NotImplemented


def make_resources(resources, role="default"):
    out = AnyOrderList()
    for name, value in resources.items():
        resource = Dict()
        resource.name = name
        resource.allocation_info.role = role
        if isinstance(value, (int, float)):
            resource.type = "SCALAR"
            resource.scalar.value = float(value)
        else:
            resource.type = "RANGES"
            resource.ranges.range = []
            for start, stop in value:
                resource.ranges.range.append(Dict(begin=start, end=stop - 1))
        out.append(resource)
    return out


def make_offer(framework_id, agent_id, host, resources, attrs=(), role="default"):
    offer = Dict()
    offer.id.value = uuid.uuid4().hex
    offer.framework_id.value = framework_id
    offer.agent_id.value = agent_id
    offer.allocation_info.role = role
    offer.hostname = host
    offer.resources = make_resources(resources, role)
    offer.attributes = attrs
    return offer


def make_text_attr(name, value):
    attr = Dict()
    attr.name = name
    attr.type = "TEXT"
    attr.text.value = value
    return attr


def make_json_attr(name, value):
    return make_text_attr(name, base64.urlsafe_b64encode(json.dumps(value).encode("utf-8")))
