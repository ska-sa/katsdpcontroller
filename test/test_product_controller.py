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

"""Tests for :mod:`katsdpcontroller.product_controller."""

import asyncio
import copy
import functools
import io
import ipaddress
import itertools
import json
import re
import typing

# Needs to be imported this way so that it is unaffected by mocking of socket.getaddrinfo
from socket import getaddrinfo
from typing import (
    Any,
    AsyncGenerator,
    Callable,
    Generator,
    List,
    Mapping,
    Optional,
    Sequence,
    Set,
    Tuple,
)
from unittest import mock

import aiokatcp
import astropy.table
import astropy.units as u
import katdal
import katpoint
import katsdpmodels.band_mask
import katsdpmodels.primary_beam
import katsdpmodels.rfi_mask
import katsdptelstate.aio.memory
import netifaces
import networkx
import numpy as np
import pymesos
import pytest
import yarl
from addict import Dict
from aiokatcp import FailReply, Message, Sensor
from aioresponses import aioresponses
from katsdptelstate.endpoint import Endpoint
from prometheus_client import CollectorRegistry

from katsdpcontroller import scheduler, sensor_proxy
from katsdpcontroller.consul import ConsulService
from katsdpcontroller.controller import device_server_sockname
from katsdpcontroller.product_controller import (
    DeviceServer,
    DeviceStatus,
    ProductState,
    Resources,
    SubarrayProduct,
    _normalise_s3_config,
    _redact_keys,
    _relative_url,
)

from . import fake_katportalclient
from .utils import (
    CONFIG,
    EXPECTED_INTERFACE_SENSOR_LIST,
    EXPECTED_PRODUCT_CONTROLLER_SENSOR_LIST,
    S3_CONFIG,
    DelayedManager,
    assert_request_fails,
    assert_sensor_value,
    assert_sensors,
    exhaust_callbacks,
)

ANTENNAS = ["m000", "m001", "m062", "m063"]
SUBARRAY_PRODUCT = "array_1_0"
CAPTURE_BLOCK = "1122334455"

STREAMS = """{
    "cam.http": {"camdata": "http://127.0.0.1:8999"},
    "cbf.baseline_correlation_products": {
        "i0.baseline-correlation-products": "spead://239.102.255.0+15:7148"
    },
    "cbf.antenna_channelised_voltage": {
        "i0.antenna-channelised-voltage": "spead://239.102.252.0+15:7148"
    },
    "cbf.tied_array_channelised_voltage": {
        "i0.tied-array-channelised-voltage-0x": "spead://239.102.254.1+15:7148",
        "i0.tied-array-channelised-voltage-0y": "spead://239.102.253.1+15:7148"
    }
}"""

EXPECTED_REQUEST_LIST = [
    "capture-done",
    "capture-init",
    "capture-list",
    "capture-start",
    "capture-status",
    "capture-stop",
    "delays",
    "gain",
    "gain-all",
    "product-configure",
    "product-deconfigure",
    "telstate-endpoint",
    # Standard katcp commands
    "client-list",
    "halt",
    "help",
    "log-level",
    "sensor-list",
    "sensor-sampling",
    "sensor-value",
    "watchdog",
    "version-list",
]


class DummyMasterController(aiokatcp.DeviceServer):
    VERSION = "dummy-1.0"
    BUILD_STATE = "dummy-1.0"

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._network = ipaddress.IPv4Network("239.192.0.0/18")
        self._next = self._network.network_address + 1

    async def request_get_multicast_groups(
        self, ctx: aiokatcp.RequestContext, subarray_product_id: str, n_addresses: int
    ) -> str:
        """Dummy docstring"""
        ans = str(self._next)
        if n_addresses > 1:
            ans += f"+{n_addresses - 1}"
        self._next += n_addresses
        return ans


class DummyDataSet:
    """Stub replacement for :class:`katdal.DataSet`."""

    def __init__(self):
        catalogue = katpoint.Catalogue()
        catalogue.add("PKS 1934-63, radec target, 19:39:25.03, -63:42:45.7")
        catalogue.add("3C286, radec, 13:31:08.29, +30:30:33.0,(800.0 43200.0 0.956 0.584 -0.1644)")
        # Two targets with the same name, to check disambiguation
        catalogue.add("My Target, radec target, 0:00:00.00, -10:00:00.0")
        catalogue.add("My Target, radec target, 0:00:00.00, -20:00:00.0")
        # Target that is only observed for 20 minutes: will produce continuum
        # but not spectral image.
        catalogue.add("Continuum, radec target, 0:00:00.00, -30:00:00.0")
        # Target that is set but never tracked
        catalogue.add("notrack, radec target, 0:00:00.00, -30:00:00.0")

        self.catalogue = catalogue
        self.dump_period = 8.0
        scan_state = []
        target_index = []
        for i in range(4):
            for j in range(1000):
                scan_state.append("track")
                target_index.append(i)
        for i in range(150):
            scan_state.append("track")
            target_index.append(4)
        for i in range(4, 6):
            for j in range(1000):
                scan_state.append("scan")
                target_index.append(i)
        self.sensor = {
            "Observation/scan_state": np.array(scan_state),
            "Observation/target_index": np.array(target_index),
        }
        self.spw = 0
        self.spectral_windows = [
            katdal.SpectralWindow(1284e6, 856e6 / 4096, 4096, "c856M4k", band="L")
        ]
        self.channel_freqs = self.spectral_windows[self.spw].channel_freqs


def get_metric(metric):
    """Get the current value of a Prometheus metric"""
    return metric.collect()[0].samples[0][2]


class TestRedactKeys:
    @pytest.fixture
    def s3_config(self) -> dict:
        return {
            "archive": {
                "read": {"access_key": "ACCESS_KEY", "secret_key": "tellno1"},
                "write": {"access_key": "s3cr3t", "secret_key": "mores3cr3t"},
                "url": "http://invalid/",
            }
        }

    def test_no_command(self, s3_config: dict) -> None:
        taskinfo = Dict()
        result = _redact_keys(taskinfo, s3_config)
        assert result == taskinfo

    @pytest.mark.parametrize(
        "arguments, expected",
        [
            (
                ["--secret=mores3cr3t", "--key=ACCESS_KEY", "--other=safe"],
                ["--secret=REDACTED", "--key=REDACTED", "--other=safe"],
            ),
            (
                ["--secret", "s3cr3t", "--key", "tellno1", "--other", "safe"],
                ["--secret", "REDACTED", "--key", "REDACTED", "--other", "safe"],
            ),
        ],
    )
    def test(self, arguments: List[str], expected: List[str], s3_config: dict) -> None:
        taskinfo = Dict()
        taskinfo.command.arguments = arguments
        result = _redact_keys(taskinfo, s3_config)
        assert result.command.arguments == expected


class TestNormaliseS3Config:
    def test_single_url(self) -> None:
        s3_config = {
            "archive": {
                "read": {"access_key": "ACCESS_KEY", "secret_key": "tellno1"},
                "write": {"access_key": "s3cr3t", "secret_key": "mores3cr3t"},
                "url": "http://invalid/",
            }
        }
        expected = {
            "archive": {
                "read": {
                    "access_key": "ACCESS_KEY",
                    "secret_key": "tellno1",
                    "url": "http://invalid/",
                },
                "write": {
                    "access_key": "s3cr3t",
                    "secret_key": "mores3cr3t",
                    "url": "http://invalid/",
                },
            }
        }
        result = _normalise_s3_config(s3_config)
        assert result == expected

        del s3_config["archive"]["read"]
        expected["archive"]["read"] = {"url": "http://invalid/"}
        result = _normalise_s3_config(s3_config)
        assert result == expected

    def test_already_split(self) -> None:
        s3_config = {
            "archive": {
                "read": {
                    "access_key": "ACCESS_KEY",
                    "secret_key": "tellno1",
                    "url": "http://read.invalid/",
                },
                "write": {
                    "access_key": "s3cr3t",
                    "secret_key": "mores3cr3t",
                    "url": "http://write.invalid/",
                },
            }
        }
        expected = copy.deepcopy(s3_config)
        result = _normalise_s3_config(s3_config)
        assert result == expected


class TestRelativeUrl:
    @pytest.fixture
    def base(self) -> yarl.URL:
        return yarl.URL("http://test.invalid/foo/bar/")

    def test_success(self, base: yarl.URL) -> None:
        url = yarl.URL("http://test.invalid/foo/bar/baz")
        assert _relative_url(base, url) == yarl.URL("baz")
        url = yarl.URL("http://test.invalid/foo/bar/baz/")
        assert _relative_url(base, url) == yarl.URL("baz/")
        assert _relative_url(base, base) == yarl.URL()

    def test_root_relative(self) -> None:
        base = yarl.URL("http://test.invalid/")
        url = yarl.URL("http://test.invalid/foo/bar/")
        assert _relative_url(base, url) == yarl.URL("foo/bar/")

    def test_different_origin(self, base: yarl.URL) -> None:
        with pytest.raises(ValueError):
            _relative_url(base, yarl.URL("https://test.invalid/foo/bar/baz"))
        with pytest.raises(ValueError):
            _relative_url(base, yarl.URL("http://another.test.invalid/foo/bar/baz"))
        with pytest.raises(ValueError):
            _relative_url(base, yarl.URL("http://test.invalid:1234/foo/bar/baz"))

    def test_outside_tree(self, base: yarl.URL) -> None:
        with pytest.raises(ValueError):
            _relative_url(base, yarl.URL("http://test.invalid/foo/bart"))
        with pytest.raises(ValueError):
            _relative_url(base, yarl.URL("http://test.invalid/"))

    def test_query_strings(self, base: yarl.URL) -> None:
        qs = yarl.URL("http://test.invalid/foo/bar/?query=yes")
        with pytest.raises(ValueError):
            _relative_url(base, qs)
        with pytest.raises(ValueError):
            _relative_url(qs, base)

    def test_fragments(self, base: yarl.URL) -> None:
        frag = yarl.URL("http://test.invalid/foo/bar/#frag")
        with pytest.raises(ValueError):
            _relative_url(base, frag)
        with pytest.raises(ValueError):
            _relative_url(frag, base)

    def test_not_absolute(self, base: yarl.URL) -> None:
        with pytest.raises(ValueError):
            _relative_url(base, yarl.URL("relative/url"))
        with pytest.raises(ValueError):
            _relative_url(yarl.URL("relative/url"), base)


class BaseTestController:
    """Utilities for test classes"""

    @pytest.fixture
    def setup_model(
        self, mock_aioresponses: aioresponses
    ) -> Callable[[katsdpmodels.models.Model, str, str, str], None]:
        def _setup_model(
            model: katsdpmodels.models.Model, current_path: str, config_path: str, fixed_path: str
        ) -> None:
            fh = io.BytesIO()
            model.to_file(fh, content_type="application/x-hdf5")
            mock_aioresponses.get(
                f"https://models.s3.invalid{current_path}",
                content_type="text/plain",
                body=f"{config_path}\n",
            )
            mock_aioresponses.get(
                f"https://models.s3.invalid{config_path}",
                content_type="text/plain",
                body=f"{fixed_path}\n",
            )
            mock_aioresponses.get(
                f"https://models.s3.invalid{fixed_path}",
                content_type="application/x-hdf5",
                body=fh.getvalue(),
            )

        return _setup_model

    @pytest.fixture(autouse=True)
    def _setup_rfi_mask_model(self, setup_model) -> None:
        model = katsdpmodels.rfi_mask.RFIMaskRanges(
            astropy.table.Table(
                [[0.0] * u.Hz, [0.0] * u.Hz, [0.0] * u.m],
                names=("min_frequency", "max_frequency", "max_baseline"),
            ),
            False,
        )
        model.version = 1
        setup_model(
            model,
            "/models/rfi_mask/current.alias",
            "/models/rfi_mask/config/2020-06-15.alias",
            "/models/rfi_mask/fixed/test.h5",
        )

    @pytest.fixture(autouse=True)
    def _setup_band_mask_model(self, setup_model) -> None:
        model = katsdpmodels.band_mask.BandMaskRanges(
            astropy.table.Table(
                rows=[[0.0, 0.05], [0.95, 1.0]], names=("min_fraction", "max_fraction")
            )
        )
        model.version = 1
        setup_model(
            model,
            "/models/band_mask/current/l/nb_ratio=1.alias",
            "/models/band_mask/config/l/nb_ratio=1/2020-06-22.alias",
            "/models/band_mask/fixed/test.h5",
        )

    @pytest.fixture(autouse=True)
    def _setup_primary_beam_model(self, setup_model) -> None:
        # Model is not used for anything, so do a minimal amount to get it working.
        model = katsdpmodels.primary_beam.PrimaryBeamAperturePlane(
            -1 * u.m,
            -1 * u.m,
            1 * u.m,
            1 * u.m,
            [1, 2] * u.GHz,
            np.zeros((2, 2, 2, 8, 8), np.complex64),
            band="l",
        )
        model.version = 1
        for antenna in ANTENNAS:
            for group in ["individual", "cohort"]:
                setup_model(
                    model,
                    f"/models/primary_beam/current/{group}/{antenna}/l.alias",
                    "/models/primary_beam/config/cohort/meerkat/l/v1.alias",
                    "/models/primary_beam/fixed/test.h5",
                )

    @pytest.fixture
    async def mc_server(self) -> AsyncGenerator[DummyMasterController, None]:
        mc_server = DummyMasterController("127.0.0.1", 0)
        await mc_server.start()
        yield mc_server
        await mc_server.stop()

    @pytest.fixture
    async def mc_client(self, mc_server) -> AsyncGenerator[aiokatcp.Client, None]:
        mc_address = device_server_sockname(mc_server)
        mc_client = await aiokatcp.Client.connect(mc_address[0], mc_address[1])
        yield mc_client
        mc_client.close()
        await mc_client.wait_closed()

    @pytest.fixture(autouse=True)
    def mock_aioresponses(self) -> Generator[aioresponses, None, None]:
        with aioresponses() as rmock:
            yield rmock

    @pytest.fixture(autouse=True)
    def mock_consul(self, mocker) -> None:
        # server.start will try to register with consul. Mock that out.
        mocker.patch(
            "katsdpcontroller.consul.ConsulService.register",
            return_value=ConsulService(),
            autospec=True,
        )

    @pytest.fixture
    def registry(self) -> CollectorRegistry:
        return CollectorRegistry()

    @pytest.fixture
    async def server(
        self, mc_client, mc_server, mock_aioresponses, mock_consul, registry, server_kwargs
    ) -> AsyncGenerator[DeviceServer, None]:
        # The mock_* fixtures are autouse but are listed explicitly in the
        # parameter list to ensure they're ordered before this fixture.
        if "sched" not in server_kwargs:
            server_kwargs["sched"] = scheduler.SchedulerBase("realtime", "127.0.0.1", 0)
        server = DeviceServer(
            master_controller=mc_client,
            subarray_product_id=SUBARRAY_PRODUCT,
            prometheus_registry=registry,
            shutdown_delay=0.0,
            **server_kwargs,
        )
        await server.start()
        yield server
        await server.stop()

    @pytest.fixture
    async def client(self, server: DeviceServer) -> AsyncGenerator[aiokatcp.Client, None]:
        bind_address = device_server_sockname(server)
        client = await aiokatcp.Client.connect(bind_address[0], bind_address[1])
        yield client
        client.close()
        await client.wait_closed()

    @pytest.fixture(autouse=True)
    def mock_katportalclient(self, mocker) -> None:
        # Mock the CBF sensors
        dummy_client = fake_katportalclient.KATPortalClient(
            components={"cbf": "cbf_1", "sub": "subarray_1"},
            sensors={
                "cbf_1_i0_antenna_channelised_voltage_n_chans": 4096,
                "cbf_1_i0_adc_sample_rate": 1712e6,
                "cbf_1_i0_antenna_channelised_voltage_n_samples_between_spectra": 8192,
                "cbf_1_i0_baseline_correlation_products_int_time": 0.499,
                "cbf_1_i0_baseline_correlation_products_n_bls": 40,
                "cbf_1_i0_baseline_correlation_products_xeng_out_bits_per_sample": 32,
                "cbf_1_i0_baseline_correlation_products_n_chans_per_substream": 256,
                "cbf_1_i0_tied_array_channelised_voltage_0x_spectra_per_heap": 256,
                "cbf_1_i0_tied_array_channelised_voltage_0x_n_chans_per_substream": 256,
                "cbf_1_i0_tied_array_channelised_voltage_0x_beng_out_bits_per_sample": 8,
                "cbf_1_i0_tied_array_channelised_voltage_0y_spectra_per_heap": 256,
                "cbf_1_i0_tied_array_channelised_voltage_0y_n_chans_per_substream": 256,
                "cbf_1_i0_tied_array_channelised_voltage_0y_beng_out_bits_per_sample": 8,
            },
        )
        mocker.patch("katportalclient.KATPortalClient", return_value=dummy_client)


@pytest.mark.timeout(5)
class TestControllerInterface(BaseTestController):
    """Testing of the controller in interface mode."""

    @pytest.fixture
    def server_kwargs(self) -> dict:
        image_resolver_factory = scheduler.ImageResolverFactory(scheduler.SimpleImageLookup("sdp"))
        return dict(
            host="127.0.0.1",
            port=0,
            batch_role="batch",
            interface_mode=True,
            localhost=True,
            image_resolver_factory=image_resolver_factory,
            s3_config={},
        )

    @pytest.fixture(autouse=True)
    def mock_time(self, mocker) -> None:
        mocker.patch("time.time", return_value=123456789.5)

    async def test_capture_init(self, client: aiokatcp.Client) -> None:
        await assert_request_fails(client, "capture-init", CAPTURE_BLOCK)
        await client.request("product-configure", SUBARRAY_PRODUCT, CONFIG)
        await client.request("capture-init", CAPTURE_BLOCK)

        reply, informs = await client.request("capture-status")
        assert reply == [b"capturing"]
        await assert_request_fails(client, "capture-init", CAPTURE_BLOCK)

    async def test_interface_sensors(self, client: aiokatcp.Client) -> None:
        await assert_sensors(client, EXPECTED_PRODUCT_CONTROLLER_SENSOR_LIST)
        interface_changed_callback = mock.MagicMock()
        client.add_inform_callback("interface-changed", interface_changed_callback)
        await client.request("product-configure", SUBARRAY_PRODUCT, CONFIG)
        interface_changed_callback.assert_called_with(b"sensor-list")
        await assert_sensors(
            client,
            EXPECTED_PRODUCT_CONTROLLER_SENSOR_LIST + EXPECTED_INTERFACE_SENSOR_LIST,
            subset=True,
        )

        # Deconfigure and check that the server shuts down
        interface_changed_callback.reset_mock()
        await client.request("product-deconfigure")
        await client.wait_disconnected()
        client.remove_inform_callback("interface-changed", interface_changed_callback)

    async def test_capture_done(self, client: aiokatcp.Client) -> None:
        await assert_request_fails(client, "capture-done")
        await client.request("product-configure", SUBARRAY_PRODUCT, CONFIG)
        await assert_request_fails(client, "capture-done")

        await client.request("capture-init", CAPTURE_BLOCK)
        reply, informs = await client.request("capture-done")
        assert reply == [CAPTURE_BLOCK.encode()]
        await assert_request_fails(client, "capture-done")

    async def test_deconfigure_subarray_product(self, client: aiokatcp.Client) -> None:
        await assert_request_fails(client, "product-configure")
        await client.request("product-configure", SUBARRAY_PRODUCT, CONFIG)
        await client.request("capture-init", CAPTURE_BLOCK)
        # should not be able to deconfigure when not in idle state
        await assert_request_fails(client, "product-deconfigure")
        await client.request("capture-done")
        reply, _informs = await client.request("product-deconfigure")
        assert reply == [b"1"]
        # server should now shut itself down
        await client.wait_disconnected()

    async def test_configure_subarray_product(self, client: aiokatcp.Client) -> None:
        await assert_request_fails(client, "product-deconfigure")
        await client.request("product-configure", SUBARRAY_PRODUCT, CONFIG)
        # Can't reconfigure when already configured
        await assert_request_fails(client, "product-configure", SUBARRAY_PRODUCT, CONFIG)
        await client.request("product-deconfigure")

    async def test_help(self, client: aiokatcp.Client) -> None:
        reply, informs = await client.request("help")
        requests = [inform.arguments[0].decode("utf-8") for inform in informs]
        assert set(EXPECTED_REQUEST_LIST) == set(requests)

    async def test_gain_single(self, client: aiokatcp.Client) -> None:
        """Test gain with a single gain to apply to all channels."""
        await client.request("product-configure", SUBARRAY_PRODUCT, CONFIG)
        reply, _ = await client.request(
            "gain", "gpucbf_antenna_channelised_voltage", "gpucbf_m901h", "1+2j"
        )
        assert reply == []
        reply, _ = await client.request(
            "gain", "gpucbf_antenna_channelised_voltage", "gpucbf_m901h"
        )
        assert reply == [b"1.0+2.0j"]
        await assert_sensor_value(
            client, "gpucbf_antenna_channelised_voltage.gpucbf_m901h.eq", "[1.0+2.0j]"
        )
        await client.request("product-deconfigure")

    async def test_gain_multi(self, client: aiokatcp.Client) -> None:
        """Test gain with a different gain for each channel."""
        await client.request("product-configure", SUBARRAY_PRODUCT, CONFIG)
        gains = [b"%d.0+2.0j" % i for i in range(4096)]
        reply, _ = await client.request(
            "gain", "gpucbf_antenna_channelised_voltage", "gpucbf_m901h", *gains
        )
        assert reply == []
        reply, _ = await client.request(
            "gain", "gpucbf_antenna_channelised_voltage", "gpucbf_m901h"
        )
        assert reply == gains
        await assert_sensor_value(
            client,
            "gpucbf_antenna_channelised_voltage.gpucbf_m901h.eq",
            "[" + ", ".join(g.decode() for g in gains) + "]",
        )
        await client.request("product-deconfigure")

    async def test_gain_all_single(self, client: aiokatcp.Client) -> None:
        """Test gain-all with a single gain to apply to all channels."""
        await client.request("product-configure", SUBARRAY_PRODUCT, CONFIG)
        await client.request("gain-all", "gpucbf_antenna_channelised_voltage", "1.0+2.0j")
        for name in ["gpucbf_m900v", "gpucbf_m900h", "gpucbf_m901v", "gpucbf_m901h"]:
            await assert_sensor_value(
                client, f"gpucbf_antenna_channelised_voltage.{name}.eq", "[1.0+2.0j]"
            )
        await client.request("product-deconfigure")

    async def test_gain_all_multi(self, client: aiokatcp.Client) -> None:
        """Test gain-all with a different gain for each channel."""
        await client.request("product-configure", SUBARRAY_PRODUCT, CONFIG)
        gains = [b"%d.0+2.0j" % i for i in range(4096)]
        await client.request("gain-all", "gpucbf_antenna_channelised_voltage", *gains)
        for name in ["gpucbf_m900v", "gpucbf_m900h", "gpucbf_m901v", "gpucbf_m901h"]:
            await assert_sensor_value(
                client,
                f"gpucbf_antenna_channelised_voltage.{name}.eq",
                "[" + ", ".join(g.decode() for g in gains) + "]",
            )
        await client.request("product-deconfigure")

    async def test_gain_all_default(self, client: aiokatcp.Client) -> None:
        """Test setting gains to default with gain-all."""
        await client.request("product-configure", SUBARRAY_PRODUCT, CONFIG)
        await client.request("gain-all", "gpucbf_antenna_channelised_voltage", "default")
        for name in ["gpucbf_m900v", "gpucbf_m900h", "gpucbf_m901v", "gpucbf_m901h"]:
            await assert_sensor_value(
                client, f"gpucbf_antenna_channelised_voltage.{name}.eq", "[1.0+0.0j]"
            )
        await client.request("product-deconfigure")

    async def test_delays(self, client: aiokatcp.Client) -> None:
        """Test setting delays and retrieving the sensor."""
        await client.request("product-configure", SUBARRAY_PRODUCT, CONFIG)
        await assert_sensor_value(
            client,
            "gpucbf_antenna_channelised_voltage.sync-time",
            123456789.0,  # Just to detect any change in sync time logic in generator.py
        )
        await client.request(
            "delays",
            "gpucbf_antenna_channelised_voltage",
            "123456791.5",
            "0.5,0.0:0.0,0.0",
            "0.0,0.125:0.0,0.0",
            "0.0,0.0:0.25,0.0",
            "0.0,0.0:0.0,1.0",
        )
        await assert_sensor_value(
            client,
            "gpucbf_antenna_channelised_voltage.gpucbf_m900v.delay",
            "(4280000000, 0.5, 0.0, 0.0, 0.0)",
        )
        await assert_sensor_value(
            client,
            "gpucbf_antenna_channelised_voltage.gpucbf_m900h.delay",
            "(4280000000, 0.0, 0.125, 0.0, 0.0)",
        )
        await assert_sensor_value(
            client,
            "gpucbf_antenna_channelised_voltage.gpucbf_m901v.delay",
            "(4280000000, 0.0, 0.0, 0.25, 0.0)",
        )
        await assert_sensor_value(
            client,
            "gpucbf_antenna_channelised_voltage.gpucbf_m901h.delay",
            "(4280000000, 0.0, 0.0, 0.0, 1.0)",
        )
        await client.request("product-deconfigure")

    async def test_input_data_suspect(self, client: aiokatcp.Client, server: DeviceServer) -> None:
        await client.request("product-configure", SUBARRAY_PRODUCT, CONFIG)
        await assert_sensor_value(
            client, "gpucbf_antenna_channelised_voltage.input-data-suspect", b"0000"
        )
        # Kill off one of the tasks
        assert server.product is not None  # Keeps mypy happy
        server.product._nodes["f.gpucbf_antenna_channelised_voltage.1"].kill(None)
        await assert_sensor_value(
            client,
            "gpucbf_antenna_channelised_voltage.input-data-suspect",
            b"0011",
            status=Sensor.Status.WARN,
        )
        # Check that the engine's sensors are modified appropriately
        await assert_sensor_value(
            client,
            "f.gpucbf_antenna_channelised_voltage.1.time.esterror",
            mock.ANY,
            status=Sensor.Status.UNREACHABLE,
        )
        await assert_sensor_value(
            client,
            "f.gpucbf_antenna_channelised_voltage.1.device-status",
            b"fail",
            status=Sensor.Status.ERROR,
        )
        # Kill off the other, to check that the sensor goes into ERROR
        server.product._nodes["f.gpucbf_antenna_channelised_voltage.0"].kill(None)
        await assert_sensor_value(
            client,
            "gpucbf_antenna_channelised_voltage.input-data-suspect",
            b"1111",
            status=Sensor.Status.ERROR,
        )

    async def test_channel_data_suspect(
        self, client: aiokatcp.Client, server: DeviceServer
    ) -> None:
        await client.request("product-configure", SUBARRAY_PRODUCT, CONFIG)
        await assert_sensor_value(
            client, "gpucbf_baseline_correlation_products.channel-data-suspect", b"0" * 4096
        )
        # Kill off one of the tasks
        assert server.product is not None
        server.product._nodes["xb.gpucbf_baseline_correlation_products.1"].kill(None)
        await assert_sensor_value(
            client,
            "gpucbf_baseline_correlation_products.channel-data-suspect",
            b"0" * 1024 + b"1" * 1024 + b"0" * 2048,
            status=Sensor.Status.WARN,
        )
        # Check that the engine's sensors have been modified appropriately
        await assert_sensor_value(
            client,
            "xb.gpucbf_baseline_correlation_products.1.xeng-clip-cnt",
            mock.ANY,
            status=Sensor.Status.UNREACHABLE,
        )
        await assert_sensor_value(
            client,
            "xb.gpucbf_baseline_correlation_products.1.device-status",
            b"fail",
            status=Sensor.Status.ERROR,
        )


class DummyScheduler:
    """Implements some functionality needed to mock a :class:`.Scheduler`.

    It doesn't provide all the methods. Instead, a mock scheduler is created
    by mocking the original scheduler and setting methods from this class as
    side effects on its methods.
    """

    def __init__(self) -> None:
        self.n_batch_tasks = 0
        self.driver = mock.create_autospec(spec=pymesos.MesosSchedulerDriver, instance=True)
        # Dict mapping task name to Mesos task status string
        self.fail_launches: typing.Dict[str, str] = {}

    async def launch(
        self,
        graph: networkx.MultiDiGraph,
        resolver: scheduler.Resolver,
        nodes: Optional[Sequence[scheduler.PhysicalNode]] = None,
        *,
        queue: Optional[scheduler.LaunchQueue] = None,
        resources_timeout: Optional[float] = None,
    ) -> None:
        """Mock implementation of Scheduler.launch."""
        if nodes is None:
            nodes = graph.nodes()
        for node in nodes:
            if node.state < scheduler.TaskState.RUNNING:
                node.set_state(scheduler.TaskState.STARTING)
        for node in nodes:
            if node.state < scheduler.TaskState.RUNNING:
                if hasattr(node.logical_node, "ports"):
                    port_num = 20000
                    for port in node.logical_node.ports:
                        if port is not None:
                            node.ports[port] = port_num
                            port_num += 1
                if hasattr(node.logical_node, "cores"):
                    core_num = 0
                    for core in node.logical_node.cores:
                        if core is not None:
                            node.cores[core] = core_num  # type: ignore
                            core_num += 1
                if isinstance(node.logical_node, scheduler.LogicalTask):
                    assert isinstance(node, scheduler.PhysicalTask)
                    node.allocation = mock.MagicMock()
                    node.allocation.agent.host = "host." + node.logical_node.name
                    node.allocation.agent.agent_id = "agent-id." + node.logical_node.name
                    node.allocation.agent.gpus[0].name = "GeForce GTX TITAN X"
                    for request in node.logical_node.interfaces:
                        interface = mock.Mock()
                        interface.name = "em1"
                        node.interfaces[request.network] = interface
        order_graph = scheduler.subgraph(graph, scheduler.DEPENDS_RESOLVE, nodes)
        for node in reversed(list(networkx.topological_sort(order_graph))):
            if node.state < scheduler.TaskState.RUNNING:
                await node.resolve(resolver, graph)
                if node.logical_node.name in self.fail_launches:
                    node.set_state(scheduler.TaskState.DEAD)
                    # This may need to be fleshed out if sdp_controller looks
                    # at other fields.
                    node.status = Dict(state=self.fail_launches[node.logical_node.name])
                else:
                    node.set_state(scheduler.TaskState.RUNNING)
        futures = []
        for node in nodes:
            futures.append(node.ready_event.wait())
        await asyncio.gather(*futures)

    async def batch_run(
        self,
        graph: networkx.MultiDiGraph,
        resolver: scheduler.Resolver,
        nodes: Optional[Sequence[scheduler.PhysicalNode]] = None,
        *,
        queue: Optional[scheduler.LaunchQueue] = None,
        resources_timeout: Optional[float] = None,
        attempts: int = 1,
    ) -> None:
        """Mock implementation of Scheduler.batch_run.

        For now this is a much lighter-weight emulation than :meth:`_launch`,
        and does not model the internal state machine of the nodes. It may
        need to be improved later.
        """
        if nodes is None:
            nodes = list(graph.nodes())
        for node in nodes:
            # Batch tasks die on their own
            node.death_expected = True
        for node in nodes:
            node.set_state(scheduler.TaskState.READY)
            node.set_state(scheduler.TaskState.DEAD)
        self.n_batch_tasks += len(nodes)

    async def kill(
        self,
        graph: networkx.MultiDiGraph,
        nodes: Optional[Sequence[scheduler.PhysicalNode]] = None,
        **kwargs,
    ) -> None:
        """Mock implementation of Scheduler.kill."""
        if nodes is not None:
            kill_graph = graph.subgraph(nodes)
        else:
            kill_graph = graph
        for node in kill_graph:
            if scheduler.TaskState.STARTED <= node.state <= scheduler.TaskState.KILLING:
                if hasattr(node, "graceful_kill") and not kwargs.get("force"):
                    await node.graceful_kill(self.driver, **kwargs)
                else:
                    node.kill(self.driver, **kwargs)
            node.set_state(scheduler.TaskState.DEAD)

    async def close(self) -> None:
        pass


class DummyClient:
    """Mock implementation of :class:`aiokatcp.client.Client`.

    Like :class:`DummyScheduler`, this isn't a complete mock. Its methods
    should be set as side effects on a mock.
    """

    def __init__(self) -> None:
        # Set of katcp requests to return failures for
        self.fail_requests: Set[str] = set()
        # Return values for katcp requests
        self.katcp_replies: typing.Dict[str, Tuple[List[bytes], List[Message]]] = {}

    async def request(
        self, msg: str, *args: Any, **kwargs: Any
    ) -> Tuple[List[bytes], List[Message]]:
        """Mock implementation of aiokatcp.Client.request"""
        if msg in self.fail_requests:
            raise FailReply("dummy failure")
        else:
            return self.katcp_replies.get(msg, ([], []))


@pytest.mark.timeout(5)
class TestController(BaseTestController):
    """Test :class:`katsdpcontroller.product_controller.DeviceServer` by mocking the scheduler."""

    def _request_slow(
        self, client: aiokatcp.Client, name: str, *args: Any, cancelled: bool = False
    ) -> DelayedManager:
        """Asynchronous context manager that runs its block with a request in progress.

        The request must operate by issuing requests to the tasks, as this is
        used to block it from completing.
        """
        # grab the mock
        sensor_proxy_client = sensor_proxy.SensorProxyClient.return_value  # type: ignore
        return DelayedManager(
            client.request(name, *args), sensor_proxy_client.request, ([], []), cancelled
        )

    def _capture_init_slow(
        self, client: aiokatcp.Client, capture_block: str, cancelled: bool = False
    ) -> DelayedManager:
        """Asynchronous context manager that runs its block with a capture-init
        in progress. The subarray product must already be configured.
        """
        return self._request_slow(client, "capture-init", capture_block, cancelled=cancelled)

    def _capture_done_slow(
        self, client: aiokatcp.Client, cancelled: bool = False
    ) -> DelayedManager:
        """Asynchronous context manager that runs its block with a capture-done
        in progress. The subarray product must already be configured and
        capturing.
        """
        return self._request_slow(client, "capture-done", cancelled=cancelled)

    def _product_configure_slow(
        self, client: aiokatcp.Client, subarray_product: str, cancelled: bool = False
    ) -> DelayedManager:
        """Asynchronous context manager that runs its block with a
        product-configure in progress.
        """
        # grab the mock
        sensor_proxy_client = sensor_proxy.SensorProxyClient.return_value  # type: ignore
        return DelayedManager(
            client.request(*self._configure_args(subarray_product)),
            sensor_proxy_client.wait_synced,
            None,
            cancelled,
        )

    # The return annotation is deliberately vague because typeshed changed
    # its annotation at some point and so any more specific annotation will
    # error out on *some* version of mypy.
    def _getaddrinfo(self, host: str, *args, **kwargs) -> List[Any]:
        """Mock getaddrinfo that replaces all hosts with a dummy IP address"""
        if host.startswith("host"):
            host = "127.0.0.2"
        return getaddrinfo(host, *args, **kwargs)

    @pytest.fixture
    def dummy_sched(self) -> DummyScheduler:
        return DummyScheduler()

    @pytest.fixture
    def sched(self, dummy_sched) -> mock.MagicMock:
        sched = mock.create_autospec(spec=scheduler.Scheduler, instance=True)
        sched.launch.side_effect = dummy_sched.launch
        sched.kill.side_effect = dummy_sched.kill
        sched.batch_run.side_effect = dummy_sched.batch_run
        sched.close.side_effect = dummy_sched.close
        sched.http_url = "http://scheduler:8080/"
        sched.http_port = 8080
        sched.task_stats = scheduler.TaskStats()
        return sched

    @pytest.fixture
    def dummy_client(self) -> DummyClient:
        return DummyClient()

    @pytest.fixture(autouse=True)
    def sensor_proxy_client(self, dummy_client: DummyClient, mocker) -> mock.MagicMock:
        # Future that is already resolved with no return value
        done_future: asyncio.Future[None] = asyncio.Future()
        done_future.set_result(None)

        sensor_proxy_client_class = mocker.patch(
            "katsdpcontroller.sensor_proxy.SensorProxyClient", autospec=True
        )
        sensor_proxy_client = sensor_proxy_client_class.return_value
        sensor_proxy_client.wait_connected.return_value = done_future
        sensor_proxy_client.wait_synced.return_value = done_future
        sensor_proxy_client.wait_closed.return_value = done_future
        sensor_proxy_client.request.side_effect = dummy_client.request
        return sensor_proxy_client

    @pytest.fixture
    def server_kwargs(self, sched: mock.MagicMock) -> dict:
        return dict(
            host="127.0.0.1",
            port=0,
            sched=sched,
            s3_config=json.loads(S3_CONFIG),
            image_resolver_factory=scheduler.ImageResolverFactory(
                scheduler.SimpleImageLookup("sdp")
            ),
            interface_mode=False,
            localhost=True,
            batch_role="batch",
        )

    @pytest.fixture
    def telstate(self) -> katsdptelstate.aio.TelescopeState:
        return katsdptelstate.aio.TelescopeState()

    @pytest.fixture(autouse=True)
    def backend_from_url_mock(
        self, telstate: katsdptelstate.aio.TelescopeState, mocker
    ) -> mock.MagicMock:
        # Mock RedisBackend to create an in-memory telstate instead
        return mocker.patch(
            "katsdptelstate.aio.redis.RedisBackend.from_url",
            return_value=telstate.backend,
            autospec=True,
        )

    @pytest.fixture(autouse=True)
    async def setup_fixture(self, server, backend_from_url_mock, mocker) -> None:
        # Future that is already resolved with no return value
        done_future: asyncio.Future[None] = asyncio.Future()
        done_future.set_result(None)
        mocker.patch("time.time", return_value=123456789.5)
        mocker.patch("socket.getaddrinfo", side_effect=self._getaddrinfo)

        # We don't have a delay_run.sh to abort when told to, so we need to
        # ensure that dependency_abort actually kills the task.
        orig_dependency_abort = scheduler.PhysicalTask.dependency_abort

        def _dependency_abort(task: scheduler.PhysicalTask) -> None:
            orig_dependency_abort(task)
            asyncio.get_event_loop().call_soon(task.set_state, scheduler.TaskState.DEAD)

        mocker.patch(
            "katsdpcontroller.scheduler.PhysicalTask.dependency_abort",
            side_effect=_dependency_abort,
            autospec=True,
        )

        mocker.patch(
            "katsdpcontroller.scheduler.poll_ports", autospec=True, return_value=done_future
        )
        mocker.patch("netifaces.interfaces", autospec=True, return_value=["lo", "em1"])
        mocker.patch("netifaces.ifaddresses", autospec=True, side_effect=self._ifaddresses)
        # Creating the sensor here isn't quite accurate (it is a dynamic sensor
        # created on subarray activation), but it shouldn't matter.
        server.sensors.add(
            Sensor(
                bytes,
                "cal.1.capture-block-state",
                "Dummy implementation of sensor",
                default=b"{}",
                initial_status=Sensor.Status.NOMINAL,
            )
        )
        server.sensors.add(
            Sensor(
                DeviceStatus,
                "ingest.sdp_l0.1.device-status",
                "Dummy implementation of sensor",
                initial_status=Sensor.Status.NOMINAL,
            )
        )

        # Mock out use of katdal to get the targets
        mocker.patch("katdal.open", return_value=DummyDataSet())

    def _ifaddresses(self, interface: str) -> Mapping[int, Sequence[Mapping[str, str]]]:
        if interface == "lo":
            return {
                netifaces.AF_INET: [
                    {"addr": "127.0.0.1", "netmask": "255.0.0.0", "peer": "127.0.0.1"}
                ],
                netifaces.AF_INET6: [
                    {"addr": "::1", "netmask": "ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff/128"}
                ],
            }
        elif interface == "em1":
            return {
                netifaces.AF_INET: [
                    {"addr": "10.0.0.2", "broadcast": "10.255.255.255", "netmask": "255.0.0.0"}
                ],
            }
        else:
            raise ValueError("You must specify a valid interface name")

    def _configure_args(self, subarray_product: str) -> Tuple[str, ...]:
        return ("product-configure", subarray_product, CONFIG)

    async def _configure_subarray(self, client: aiokatcp.Client, subarray_product: str) -> None:
        reply, informs = await client.request(*self._configure_args(subarray_product))

    async def assert_immutable(
        self,
        telstate: katsdptelstate.aio.TelescopeState,
        key: str,
        value: Any,
        key_type: katsdptelstate.KeyType = katsdptelstate.KeyType.IMMUTABLE,
    ) -> None:
        """Check the value of a telstate key and also that it is immutable"""
        assert telstate is not None
        # Uncomment for debugging
        # import json
        # with open('expected.json', 'w') as f:
        #     json.dump(value, f, indent=2, default=str, sort_keys=True)
        # with open('actual.json', 'w') as f:
        #     json.dump(telstate[key], f, indent=2, default=str, sort_keys=True)
        assert await telstate[key] == value
        assert await telstate.key_type(key) == key_type

    async def test_product_configure_success(
        self,
        client: aiokatcp.Client,
        server: DeviceServer,
        telstate: katsdptelstate.aio.TelescopeState,
        backend_from_url_mock,
    ) -> None:
        """A ?product-configure request must wait for the tasks to come up,
        then indicate success.
        """
        await self._configure_subarray(client, SUBARRAY_PRODUCT)
        backend_from_url_mock.assert_called_once_with("redis://host.telstate:20000")

        # Verify the telescope state.
        # This is not a complete list of calls. It checks that each category of stuff
        # is covered: base_params, per node, per edge
        assert telstate is not None
        await self.assert_immutable(telstate, "subarray_product_id", SUBARRAY_PRODUCT)
        await self.assert_immutable(
            telstate, "sdp_model_base_url", "https://models.s3.invalid/models/"
        )
        await self.assert_immutable(
            telstate,
            telstate.join("model", "rfi_mask", "config"),
            "rfi_mask/config/2020-06-15.alias",
        )
        await self.assert_immutable(
            telstate, telstate.join("model", "rfi_mask", "fixed"), "rfi_mask/fixed/test.h5"
        )
        await self.assert_immutable(
            telstate,
            telstate.join("i0_antenna_channelised_voltage", "model", "band_mask", "config"),
            "band_mask/config/l/nb_ratio=1/2020-06-22.alias",
        )
        await self.assert_immutable(
            telstate,
            telstate.join("i0_antenna_channelised_voltage", "model", "band_mask", "fixed"),
            "band_mask/fixed/test.h5",
        )
        await self.assert_immutable(
            telstate,
            telstate.join(
                "i0_antenna_channelised_voltage", "model", "primary_beam", "cohort", "config"
            ),
            {ant: "primary_beam/config/cohort/meerkat/l/v1.alias" for ant in ANTENNAS},
            key_type=katsdptelstate.KeyType.INDEXED,
        )
        await self.assert_immutable(
            telstate,
            telstate.join(
                "i0_antenna_channelised_voltage", "model", "primary_beam", "individual", "fixed"
            ),
            {ant: "primary_beam/fixed/test.h5" for ant in ["m000", "m001", "m062", "m063"]},
            key_type=katsdptelstate.KeyType.INDEXED,
        )
        await self.assert_immutable(
            telstate,
            "config.vis_writer.sdp_l0",
            {
                "external_hostname": "host.vis_writer.sdp_l0",
                "npy_path": "/var/kat/data",
                "obj_size_mb": mock.ANY,
                "port": 20000,
                "aiomonitor_port": 20001,
                "aioconsole_port": 20002,
                "aiomonitor": True,
                "l0_spead": mock.ANY,
                "l0_interface": "em1",
                "l0_name": "sdp_l0",
                "s3_endpoint_url": "http://archive.s3.invalid/",
                "s3_expiry_days": None,
                "workers": mock.ANY,
                "buffer_dumps": mock.ANY,
                "obj_max_dumps": mock.ANY,
                "direct_write": True,
            },
        )
        # Test that the output channel rounding was done correctly
        await self.assert_immutable(
            telstate,
            "config.ingest.sdp_l0_continuum_only",
            {
                "antenna_mask": mock.ANY,
                "cbf_spead": mock.ANY,
                "cbf_ibv": True,
                "cbf_name": "i0_baseline_correlation_products",
                "continuum_factor": 16,
                "l0_continuum_spead": mock.ANY,
                "l0_continuum_name": "sdp_l0_continuum_only",
                "sd_continuum_factor": 16,
                "sd_spead_rate": mock.ANY,
                "sd_output_channels": "64:3520",
                "sd_int_time": mock.ANY,
                "output_int_time": mock.ANY,
                "output_channels": "64:3520",
                "servers": 4,
                "clock_ratio": 1.0,
                "use_data_suspect": True,
                "excise": False,
                "aiomonitor": True,
            },
        )

        # Verify the state of the subarray
        product = server.product
        assert product is not None
        assert isinstance(product, SubarrayProduct)  # For mypy's benefit
        assert not product.async_busy
        assert product.state == ProductState.IDLE

        # Verify katcp sensors.

        n_xengs = 4  # Update if sizing logic changes
        # antenna-channelised-voltage sensors
        await assert_sensor_value(
            client, "gpucbf_antenna_channelised_voltage.adc-sample-rate", 1712e6
        )
        await assert_sensor_value(client, "gpucbf_antenna_channelised_voltage.bandwidth", 856e6)
        await assert_sensor_value(
            client, "gpucbf_antenna_channelised_voltage.scale-factor-timestamp", 1712e6
        )
        await assert_sensor_value(client, "gpucbf_antenna_channelised_voltage.n-ants", 2)
        await assert_sensor_value(client, "gpucbf_antenna_channelised_voltage.n-inputs", 4)
        await assert_sensor_value(client, "gpucbf_antenna_channelised_voltage.n-fengs", 2)
        await assert_sensor_value(
            client, "gpucbf_antenna_channelised_voltage.feng-out-bits-per-sample", 8
        )
        await assert_sensor_value(client, "gpucbf_antenna_channelised_voltage.n-chans", 4096)
        await assert_sensor_value(
            client, "gpucbf_antenna_channelised_voltage.n-chans-per-substream", 4096 // n_xengs
        )

        # baseline-correlation-products sensors
        await assert_sensor_value(client, "gpucbf_baseline_correlation_products.n-accs", 408 * 256)
        await assert_sensor_value(
            client, "gpucbf_baseline_correlation_products.int-time", 408 * 256 * 4096 / 856e6
        )
        await assert_sensor_value(
            client, "gpucbf_baseline_correlation_products.xeng-out-bits-per-sample", 32
        )
        expected_bls_ordering = (
            "[('gpucbf_m900v', 'gpucbf_m900v'), "
            "('gpucbf_m900h', 'gpucbf_m900v'), "
            "('gpucbf_m900v', 'gpucbf_m900h'), "
            "('gpucbf_m900h', 'gpucbf_m900h'), "
            "('gpucbf_m900v', 'gpucbf_m901v'), "
            "('gpucbf_m900h', 'gpucbf_m901v'), "
            "('gpucbf_m900v', 'gpucbf_m901h'), "
            "('gpucbf_m900h', 'gpucbf_m901h'), "
            "('gpucbf_m901v', 'gpucbf_m901v'), "
            "('gpucbf_m901h', 'gpucbf_m901v'), "
            "('gpucbf_m901v', 'gpucbf_m901h'), "
            "('gpucbf_m901h', 'gpucbf_m901h')]"
        )
        await assert_sensor_value(
            client, "gpucbf_baseline_correlation_products.bls-ordering", expected_bls_ordering
        )
        await assert_sensor_value(client, "gpucbf_baseline_correlation_products.n-bls", 12)
        await assert_sensor_value(client, "gpucbf_baseline_correlation_products.n-chans", 4096)
        await assert_sensor_value(
            client, "gpucbf_baseline_correlation_products.n-chans-per-substream", 4096 // n_xengs
        )
        # Test a multicast stream destination sensor
        stream_name = "gpucbf_baseline_correlation_products"
        node = product._nodes[f"multicast.{stream_name}"]
        await assert_sensor_value(
            client, f"{stream_name}.destination", str(Endpoint(node.host, node.ports["spead"]))
        )

    async def test_product_configure_telstate_fail(
        self,
        client: aiokatcp.Client,
        server: DeviceServer,
        sched,
        dummy_sched: DummyScheduler,
        backend_from_url_mock,
    ) -> None:
        """If the telstate task fails, product-configure must fail"""
        dummy_sched.fail_launches["telstate"] = "TASK_FAILED"
        backend_from_url_mock.side_effect = ConnectionError
        await assert_request_fails(client, *self._configure_args(SUBARRAY_PRODUCT))
        sched.launch.assert_called_with(mock.ANY, mock.ANY, mock.ANY)
        sched.kill.assert_called_with(mock.ANY, capture_blocks=mock.ANY, force=True)
        # Must not have created the subarray product internally
        assert server.product is None

    async def test_product_configure_task_fail(
        self,
        client: aiokatcp.Client,
        server: DeviceServer,
        sched,
        dummy_sched: DummyScheduler,
        backend_from_url_mock,
    ) -> None:
        """If a task other than telstate fails, product-configure must fail"""
        dummy_sched.fail_launches["ingest.sdp_l0.1"] = "TASK_FAILED"
        await assert_request_fails(client, *self._configure_args(SUBARRAY_PRODUCT))
        backend_from_url_mock.assert_called_once_with("redis://host.telstate:20000")
        sched.launch.assert_called_with(mock.ANY, mock.ANY)
        sched.kill.assert_called_with(mock.ANY, capture_blocks=mock.ANY, force=True)
        # Must not have created the subarray product internally
        assert server.product is None

    async def test_product_deconfigure(
        self, client: aiokatcp.Client, sched, server: DeviceServer
    ) -> None:
        """Checks success path of product-deconfigure"""
        await self._configure_subarray(client, SUBARRAY_PRODUCT)
        await client.request("product-deconfigure")
        # Check that the graph was shut down
        sched.kill.assert_called_with(mock.ANY, capture_blocks=mock.ANY, force=False)
        # The server must shut itself down
        await server.join()

    async def test_product_deconfigure_capturing(self, client: aiokatcp.Client) -> None:
        """product-deconfigure must fail while capturing"""
        await self._configure_subarray(client, SUBARRAY_PRODUCT)
        await client.request("capture-init", CAPTURE_BLOCK)
        await assert_request_fails(client, "product-deconfigure")

    async def test_product_deconfigure_capturing_force(
        self, client: aiokatcp.Client, server: DeviceServer
    ) -> None:
        """forced product-deconfigure must succeed while capturing"""
        await self._configure_subarray(client, SUBARRAY_PRODUCT)
        await client.request("capture-init", CAPTURE_BLOCK)
        await client.request("product-deconfigure", True)
        # The server must shut itself down
        await server.join()

    async def test_product_deconfigure_busy(
        self, client: aiokatcp.Client, server: DeviceServer
    ) -> None:
        """product-deconfigure cannot happen concurrently with capture-init"""
        await self._configure_subarray(client, SUBARRAY_PRODUCT)
        async with self._capture_init_slow(client, CAPTURE_BLOCK):
            await assert_request_fails(client, "product-deconfigure", False)
        # Check that the subarray still exists and has the right state
        product = server.product
        assert product is not None
        assert not product.async_busy
        assert product.state == ProductState.CAPTURING

    async def test_product_deconfigure_busy_force(
        self, client: aiokatcp.Client, server: DeviceServer, sched
    ) -> None:
        """forced product-deconfigure must succeed while in capture-init"""
        await self._configure_subarray(client, SUBARRAY_PRODUCT)
        async with self._capture_init_slow(client, CAPTURE_BLOCK, cancelled=True):
            await client.request("product-deconfigure", True)
        # Check that the graph was shut down
        sched.kill.assert_called_with(mock.ANY, capture_blocks=mock.ANY, force=True)
        # The server must shut itself down
        await server.join()

    async def test_product_deconfigure_while_configuring_force(
        self, client: aiokatcp.Client, server: DeviceServer, sched
    ) -> None:
        """forced product-deconfigure must succeed while in product-configure"""
        async with self._product_configure_slow(client, SUBARRAY_PRODUCT, cancelled=True):
            await client.request("product-deconfigure", True)
        # Check that the graph was shut down
        sched.kill.assert_called_with(mock.ANY, capture_blocks=mock.ANY, force=True)
        # Verify the state
        assert server.product is None

    async def test_capture_init(
        self, client: aiokatcp.Client, server: DeviceServer, sensor_proxy_client
    ) -> None:
        """Checks that capture-init succeeds and sets appropriate state"""
        await self._configure_subarray(client, SUBARRAY_PRODUCT)
        await client.request("capture-init", CAPTURE_BLOCK)
        # check that the subarray is in an appropriate state
        product = server.product
        assert product is not None
        assert not product.async_busy
        assert product.state == ProductState.CAPTURING
        # Check that the graph transitions were called. Each call may be made
        # multiple times, depending on the number of instances of each child.
        # We thus collapse them into groups of equal calls and don't worry
        # about the number, which would otherwise make the test fragile.
        katcp_client = sensor_proxy_client
        sorted_calls = sorted(katcp_client.request.mock_calls, key=lambda call: call[1])
        grouped_calls = [k for k, g in itertools.groupby(sorted_calls)]
        expected_calls = [
            mock.call("capture-init", CAPTURE_BLOCK),
            mock.call("capture-start", "i0_baseline_correlation_products", mock.ANY),
            mock.call("capture-start", "i0_tied_array_channelised_voltage_0x", mock.ANY),
            mock.call("capture-start", "i0_tied_array_channelised_voltage_0y", mock.ANY),
        ]
        assert grouped_calls == expected_calls

    async def test_capture_init_bad_json(self, client: aiokatcp.Client) -> None:
        """Check that capture-init fails if an override is illegal"""
        await self._configure_subarray(client, SUBARRAY_PRODUCT)
        with pytest.raises(FailReply):
            await client.request("capture-init", CAPTURE_BLOCK, "not json")

    async def test_capture_init_bad_override(self, client: aiokatcp.Client) -> None:
        """Check that capture-init fails if an override makes the config illegal"""
        await self._configure_subarray(client, SUBARRAY_PRODUCT)
        with pytest.raises(FailReply):
            await client.request("capture-init", CAPTURE_BLOCK, '{"outputs": null}')

    async def test_capture_init_bad_override_change(self, client: aiokatcp.Client) -> None:
        """Check that capture-init fails if an override makes an invalid change"""
        await self._configure_subarray(client, SUBARRAY_PRODUCT)
        with pytest.raises(FailReply):
            await client.request(
                "capture-init",
                CAPTURE_BLOCK,
                '{"inputs": {"camdata": {"url": "http://127.0.0.1:8888"}}}',
            )

    async def test_capture_init_failed_req(
        self, client: aiokatcp.Client, server: DeviceServer, dummy_client: DummyClient
    ) -> None:
        """Capture-init fails on some task"""
        await self._configure_subarray(client, SUBARRAY_PRODUCT)
        dummy_client.fail_requests.add("capture-init")
        await assert_request_fails(client, "capture-init", CAPTURE_BLOCK)
        # check that the subarray is in an appropriate state
        product = server.product
        assert product is not None
        assert product.state == ProductState.ERROR
        assert server.sensors["state"].value == ProductState.ERROR
        assert server.sensors["device-status"].value == DeviceStatus.FAIL
        assert server.sensors["device-status"].status == Sensor.Status.ERROR
        assert product.capture_blocks == {}
        # check that the subarray can be safely deconfigured, and that it
        # goes via DECONFIGURING state. Rather than trying to directly
        # observe the internal state during deconfigure (e.g. with
        # DelayedManager), we'll just observe the sensor
        state_observer = mock.Mock()
        server.sensors["state"].attach(state_observer)
        await client.request("product-deconfigure")
        # call 0, arguments, argument 1
        assert state_observer.mock_calls[0][1][1].value == ProductState.DECONFIGURING
        assert product.state == ProductState.DEAD

    async def test_capture_done_failed_req(
        self,
        client: aiokatcp.Client,
        server: DeviceServer,
        dummy_client: DummyClient,
        sensor_proxy_client,
    ) -> None:
        """Capture-done fails on some task"""
        await self._configure_subarray(client, SUBARRAY_PRODUCT)
        dummy_client.fail_requests.add("capture-done")
        reply, informs = await client.request("capture-init", CAPTURE_BLOCK)
        await assert_request_fails(client, "capture-done")
        # check that the subsequent transitions still run
        katcp_client = sensor_proxy_client
        katcp_client.request.assert_called_with("write-meta", CAPTURE_BLOCK, True)
        # check that postprocessing transitions still run.
        await exhaust_callbacks()
        katcp_client.request.assert_called_with("write-meta", CAPTURE_BLOCK, False)
        # check that the subarray is in an appropriate state
        product = server.product
        assert product is not None
        assert product.state == ProductState.ERROR
        assert product.capture_blocks == {}
        # check that the subarray can be safely deconfigured
        await client.request("product-deconfigure")
        assert product.state == ProductState.DEAD

    async def _test_busy(self, client: aiokatcp.Client, command: str, *args: Any) -> None:
        """Test that a command fails if issued while ?capture-init or
        ?product-configure is in progress
        """
        async with self._product_configure_slow(client, SUBARRAY_PRODUCT):
            await assert_request_fails(client, command, *args)
        async with self._capture_init_slow(client, CAPTURE_BLOCK):
            await assert_request_fails(client, command, *args)

    async def test_capture_init_busy(self, client: aiokatcp.Client) -> None:
        """Capture-init fails if an asynchronous operation is already in progress"""
        await self._test_busy(client, "capture-init", CAPTURE_BLOCK)

    def _ingest_died(self, subarray_product: SubarrayProduct) -> None:
        """Mark an ingest process as having died"""
        for node in subarray_product.physical_graph:
            if node.logical_node.name == "ingest.sdp_l0.1":
                node.set_state(scheduler.TaskState.DEAD)
                node.status = Dict(state="TASK_FAILED")
                break
        else:
            raise ValueError("Could not find ingest node")

    def _ingest_bad_device_status(
        self, server: DeviceServer, subarray_product: SubarrayProduct
    ) -> None:
        """Mark an ingest process as having bad status"""
        sensor = server.sensors["ingest.sdp_l0.1.device-status"]
        sensor.set_value(DeviceStatus.FAIL, Sensor.Status.ERROR)

    async def test_capture_init_dead_process(
        self, client: aiokatcp.Client, server: DeviceServer
    ) -> None:
        """Capture-init fails if a child process is dead."""
        await client.request("product-configure", SUBARRAY_PRODUCT, CONFIG)
        product = server.product
        assert product is not None
        self._ingest_died(product)
        assert product.state == ProductState.ERROR
        await assert_request_fails(client, "capture-init", CAPTURE_BLOCK)
        # check that the subarray is in an appropriate state
        assert product.state == ProductState.ERROR
        assert product.capture_blocks == {}

    async def test_capture_init_process_dies(self, client: aiokatcp.Client, server) -> None:
        """Capture-init fails if a child dies half-way through."""
        await client.request("product-configure", SUBARRAY_PRODUCT, CONFIG)
        product = server.product
        assert product is not None
        with pytest.raises(FailReply):
            async with self._capture_init_slow(client, CAPTURE_BLOCK):
                self._ingest_died(product)
        # check that the subarray is in an appropriate state
        assert product.state == ProductState.ERROR
        assert product.capture_blocks == {}

    async def test_capture_done(
        self,
        client: aiokatcp.Client,
        server: DeviceServer,
        sensor_proxy_client,
        dummy_sched: DummyScheduler,
    ) -> None:
        """Checks that capture-done succeeds and sets appropriate state"""
        await self._configure_subarray(client, SUBARRAY_PRODUCT)
        await client.request("capture-init", CAPTURE_BLOCK)
        cal_sensor = server.sensors["cal.1.capture-block-state"]
        cal_sensor.value = b'{"1122334455: "capturing"}'
        await client.request("capture-done")
        # check that the subarray is in an appropriate state
        product = server.product
        assert product is not None
        assert not product.async_busy
        assert product.state == ProductState.IDLE
        # Check that the graph transitions succeeded
        katcp_client = sensor_proxy_client
        katcp_client.request.assert_any_call("capture-done")
        katcp_client.request.assert_called_with("write-meta", CAPTURE_BLOCK, True)
        # Now simulate cal finishing with the capture block
        cal_sensor.value = b"{}"
        await exhaust_callbacks()
        # write-meta full dump must be last, hence assert_called_with not assert_any_call
        katcp_client.request.assert_called_with("write-meta", CAPTURE_BLOCK, False)

        # Check that postprocessing ran and didn't fail
        assert product.capture_blocks == {}
        assert dummy_sched.n_batch_tasks == 11  # 4 continuum, 3x2 spectral, 1 report

    async def test_capture_done_disable_batch(
        self, client: aiokatcp.Client, server: DeviceServer, dummy_sched: DummyScheduler
    ) -> None:
        """Checks that capture-init with override takes effect"""
        await self._configure_subarray(client, SUBARRAY_PRODUCT)
        await client.request("capture-init", CAPTURE_BLOCK, '{"outputs": {"spectral_image": null}}')
        cal_sensor = server.sensors["cal.1.capture-block-state"]
        cal_sensor.value = b'{"1122334455": "capturing"}'
        await client.request("capture-done")
        cal_sensor.value = b"{}"
        await exhaust_callbacks()
        assert dummy_sched.n_batch_tasks == 4  # 4 continuum, no spectral

    async def test_capture_done_busy(self, client: aiokatcp.Client):
        """Capture-done fails if an asynchronous operation is already in progress"""
        await self._test_busy(client, "capture-done")

    async def _test_failure_while_capturing(
        self,
        client: aiokatcp.Client,
        server: DeviceServer,
        sensor_proxy_client,
        failfunc: Callable[[SubarrayProduct], None],
    ) -> None:
        await client.request("product-configure", SUBARRAY_PRODUCT, CONFIG)
        await client.request("capture-init", CAPTURE_BLOCK)
        product = server.product
        assert product is not None
        assert product.state == ProductState.CAPTURING
        failfunc(product)
        assert product.state == ProductState.ERROR
        assert product.capture_blocks == {CAPTURE_BLOCK: mock.ANY}
        # In the background it will terminate the capture block
        await exhaust_callbacks()
        assert product.capture_blocks == {}
        katcp_client = sensor_proxy_client
        katcp_client.request.assert_called_with("write-meta", CAPTURE_BLOCK, False)

    async def test_process_dies_while_capturing(
        self, client: aiokatcp.Client, server: DeviceServer, sensor_proxy_client
    ) -> None:
        await self._test_failure_while_capturing(
            client, server, sensor_proxy_client, self._ingest_died
        )

    async def test_bad_device_status_while_capturing(
        self, client: aiokatcp.Client, server: DeviceServer, sensor_proxy_client
    ) -> None:
        await self._test_failure_while_capturing(
            client,
            server,
            sensor_proxy_client,
            functools.partial(self._ingest_bad_device_status, server),
        )

    async def test_capture_done_process_dies(
        self, client: aiokatcp.Client, server: DeviceServer, sensor_proxy_client
    ) -> None:
        """Capture-done fails if a child dies half-way through."""
        await client.request("product-configure", SUBARRAY_PRODUCT, CONFIG)
        await client.request("capture-init", CAPTURE_BLOCK)
        product = server.product
        assert product is not None
        assert product.state == ProductState.CAPTURING
        assert product.capture_blocks == {CAPTURE_BLOCK: mock.ANY}
        with pytest.raises(FailReply):
            async with self._capture_done_slow(client):
                self._ingest_died(product)
        # check that the subarray is in an appropriate state and that final
        # writeback occurred.
        assert product.state == ProductState.ERROR
        await exhaust_callbacks()
        katcp_client = sensor_proxy_client
        katcp_client.request.assert_called_with("write-meta", CAPTURE_BLOCK, False)
        assert product.capture_blocks == {}

    async def test_deconfigure_on_stop(
        self, client: aiokatcp.Client, server: DeviceServer, sensor_proxy_client, sched
    ) -> None:
        """Calling stop will force-deconfigure existing subarrays, even if capturing."""
        await self._configure_subarray(client, SUBARRAY_PRODUCT)
        await client.request("capture-init", CAPTURE_BLOCK)
        await server.stop()

        sensor_proxy_client.request.assert_any_call("capture-done")
        # Forced deconfigure, so we only get the light dump
        sensor_proxy_client.request.assert_called_with("write-meta", mock.ANY, True)
        sched.kill.assert_called_with(mock.ANY, capture_blocks=mock.ANY, force=True)

    async def test_deconfigure_on_stop_busy(
        self, client: aiokatcp.Client, server: DeviceServer, sched
    ) -> None:
        """Calling deconfigure_on_exit while a capture-init or capture-done is busy
        kills off the graph anyway.
        """
        await self._configure_subarray(client, SUBARRAY_PRODUCT)
        async with self._capture_init_slow(client, CAPTURE_BLOCK, cancelled=True):
            await server.stop()
        sched.kill.assert_called_with(mock.ANY, capture_blocks=mock.ANY, force=True)

    async def test_deconfigure_on_stop_cancel(
        self, client: aiokatcp.Client, server: DeviceServer, sched
    ) -> None:
        """Calling deconfigure_on_exit while a configure is in process cancels
        that configure and kills off the graph."""
        async with self._product_configure_slow(client, SUBARRAY_PRODUCT, cancelled=True):
            await server.stop()
        # We must have killed off the partially-launched graph
        sched.kill.assert_called_with(mock.ANY, capture_blocks=mock.ANY, force=True)

    async def test_telstate_endpoint(self, client: aiokatcp.Client) -> None:
        """Test telstate-endpoint"""
        await assert_request_fails(client, "telstate-endpoint")
        await self._configure_subarray(client, SUBARRAY_PRODUCT)
        reply, _ = await client.request("telstate-endpoint")
        assert reply == [b"host.telstate:20000"]

    async def test_capture_status(self, client: aiokatcp.Client) -> None:
        """Test capture-status"""
        await assert_request_fails(client, "capture-status")
        await self._configure_subarray(client, SUBARRAY_PRODUCT)
        reply, _ = await client.request("capture-status")
        assert reply == [b"idle"]
        await client.request("capture-init", CAPTURE_BLOCK)
        reply, _ = await client.request("capture-status")
        assert reply == [b"capturing"]

    async def test_gain_bad_stream(self, client: aiokatcp.Client) -> None:
        """Test gain with a stream that doesn't exist."""
        await self._configure_subarray(client, SUBARRAY_PRODUCT)
        await assert_request_fails(client, "gain", "foo", "m000h", "1")

    async def test_gain_wrong_stream_type(self, client: aiokatcp.Client) -> None:
        """Test gain with a stream that is of the wrong type."""
        await self._configure_subarray(client, SUBARRAY_PRODUCT)
        await assert_request_fails(client, "gain", "i0_antenna_channelised_voltage", "m000h", "1")

    async def test_gain_wrong_length(self, client: aiokatcp.Client) -> None:
        """Test gain with the wrong number of channels."""
        await self._configure_subarray(client, SUBARRAY_PRODUCT)
        await assert_request_fails(
            client, "gain", "gpucbf_antenna_channelised_voltage", "gpucbf_m900h", "1", "0.5"
        )

    async def test_gain_bad_format(self, client: aiokatcp.Client) -> None:
        """Test gain with badly-formatted gains."""
        await self._configure_subarray(client, SUBARRAY_PRODUCT)
        await assert_request_fails(
            client,
            "gain",
            "gpucbf_antenna_channelised_voltage",
            "gpucbf_m900h",
            "not a complex number",
        )

    async def test_gain_single(
        self, client: aiokatcp.Client, dummy_client: DummyClient, sensor_proxy_client
    ) -> None:
        """Test gain with a single gain to apply to all channels."""
        await self._configure_subarray(client, SUBARRAY_PRODUCT)
        dummy_client.katcp_replies["gain"] = ([b"1.0+2.0j"], [])
        reply, _ = await client.request(
            "gain", "gpucbf_antenna_channelised_voltage", "gpucbf_m901h", "1.0+2.0j"
        )
        # TODO: this doesn't check that it goes to the correct node
        katcp_client = sensor_proxy_client
        katcp_client.request.assert_any_call(
            "gain", "gpucbf_antenna_channelised_voltage", 1, "1.0+2.0j"
        )
        assert reply == [b"1.0+2.0j"]

    async def test_gain_multi(
        self, client: aiokatcp.Client, dummy_client: DummyClient, sensor_proxy_client
    ) -> None:
        """Test gain with a different gain for each channel."""
        await self._configure_subarray(client, SUBARRAY_PRODUCT)
        gains = [b"%d.0+2.0j" % i for i in range(4096)]
        dummy_client.katcp_replies["gain"] = (gains, [])
        reply, _ = await client.request(
            "gain", "gpucbf_antenna_channelised_voltage", "gpucbf_m901h", *gains
        )
        # TODO: this doesn't check that it goes to the correct node
        katcp_client = sensor_proxy_client
        katcp_client.request.assert_any_call(
            "gain", "gpucbf_antenna_channelised_voltage", 1, *[g.decode() for g in gains]
        )
        assert reply == gains

    async def test_gain_all_bad_stream(self, client: aiokatcp.Client) -> None:
        """Test gain-all with a stream that doesn't exist."""
        await self._configure_subarray(client, SUBARRAY_PRODUCT)
        await assert_request_fails(client, "gain-all", "foo", "1")

    async def test_gain_all_wrong_stream_type(self, client: aiokatcp.Client) -> None:
        """Test gain-all with a stream that is of the wrong type."""
        await self._configure_subarray(client, SUBARRAY_PRODUCT)
        await assert_request_fails(client, "gain-all", "i0_antenna_channelised_voltage", "1")

    async def test_gain_all_wrong_length(self, client: aiokatcp.Client) -> None:
        """Test gain-all with the wrong number of channels."""
        await self._configure_subarray(client, SUBARRAY_PRODUCT)
        await assert_request_fails(
            client, "gain-all", "gpucbf_antenna_channelised_voltage", "1", "0.5"
        )

    async def test_gain_all_bad_format(self, client: aiokatcp.Client) -> None:
        """Test gain-all with badly-formatted gains."""
        await self._configure_subarray(client, SUBARRAY_PRODUCT)
        await assert_request_fails(
            client, "gain-all", "gpucbf_antenna_channelised_voltage", "not a complex number"
        )

    async def test_gain_all_single(self, client: aiokatcp.Client, sensor_proxy_client) -> None:
        """Test gain-all with a single gain to apply to all channels."""
        await self._configure_subarray(client, SUBARRAY_PRODUCT)
        await client.request("gain-all", "gpucbf_antenna_channelised_voltage", "1+2j")
        # TODO: this doesn't check that it goes to the correct nodes
        katcp_client = sensor_proxy_client
        katcp_client.request.assert_any_call(
            "gain-all", "gpucbf_antenna_channelised_voltage", "1+2j"
        )

    async def test_gain_all_multi(self, client: aiokatcp.Client, sensor_proxy_client) -> None:
        """Test gain-all with a different gain for each channel."""
        await self._configure_subarray(client, SUBARRAY_PRODUCT)
        gains = [b"%d+2j" % i for i in range(4096)]
        await client.request("gain-all", "gpucbf_antenna_channelised_voltage", *gains)
        # TODO: this doesn't check that it goes to the correct nodes
        katcp_client = sensor_proxy_client
        katcp_client.request.assert_any_call(
            "gain-all", "gpucbf_antenna_channelised_voltage", *[g.decode() for g in gains]
        )

    async def test_gain_all_default(self, client: aiokatcp.Client, sensor_proxy_client) -> None:
        """Test setting gains to default with gain-all."""
        await self._configure_subarray(client, SUBARRAY_PRODUCT)
        await client.request("gain-all", "gpucbf_antenna_channelised_voltage", "default")
        # TODO: this doesn't check that it goes to the correct nodes
        katcp_client = sensor_proxy_client
        katcp_client.request.assert_any_call(
            "gain-all", "gpucbf_antenna_channelised_voltage", "default"
        )

    async def test_delays_bad_stream(self, client: aiokatcp.Client) -> None:
        """Test delays with a stream that doesn't exist."""
        await self._configure_subarray(client, SUBARRAY_PRODUCT)
        await assert_request_fails(client, "delays", "foo", 1234567890.0, "0,0:0,0")

    async def test_delays_wrong_stream_type(self, client: aiokatcp.Client) -> None:
        """Test delays with a stream that is of the wrong type."""
        await self._configure_subarray(client, SUBARRAY_PRODUCT)
        await assert_request_fails(
            client, "delays", "i0_antenna_channelised_voltage", 1234567890.0, "0,0:0,0"
        )

    async def test_delays_wrong_length(self, client: aiokatcp.Client) -> None:
        """Test delays with the wrong number of arguments."""
        await self._configure_subarray(client, SUBARRAY_PRODUCT)
        await assert_request_fails(
            client, "delays", "gpucbf_antenna_channelised_voltage", 1234567890.0, "0,0:0,0"
        )

    async def test_delays_bad_format(self, client: aiokatcp.Client) -> None:
        """Test delays with a badly-formatted coefficient set."""
        await self._configure_subarray(client, SUBARRAY_PRODUCT)
        await assert_request_fails(
            client,
            "delays",
            "gpucbf_antenna_channelised_voltage",
            1234567890.0,
            "0,0:0,0:extra",
            "0,0:0,0",
            "0,0:0,0",
            "0,0:0,0",
        )
        await assert_request_fails(
            client,
            "delays",
            "gpucbf_antenna_channelised_voltage",
            1234567890.0,
            "0,0:0,0,extra",
            "0,0:0,0",
            "0,0:0,0",
            "0,0:0,0",
        )
        await assert_request_fails(
            client,
            "delays",
            "gpucbf_antenna_channelised_voltage",
            1234567890.0,
            "a,0:0,0",
            "0,0:0,0",
            "0,0:0,0",
            "0,0:0,0",
        )

    async def test_delays_success(self, client: aiokatcp.Client, sensor_proxy_client) -> None:
        await self._configure_subarray(client, SUBARRAY_PRODUCT)
        await client.request(
            "delays",
            "gpucbf_antenna_channelised_voltage",
            1234567890.0,
            "0,0:0,0",
            "0,0:0,1",
            "0,1:0,0",
            "0,1:0,1",
        )
        katcp_client = sensor_proxy_client
        # TODO: this doesn't check that the requests are going to the correct
        # nodes.
        katcp_client.request.assert_any_call(
            "delays", "gpucbf_antenna_channelised_voltage", 1234567890.0, "0,0:0,0", "0,0:0,1"
        )
        katcp_client.request.assert_any_call(
            "delays", "gpucbf_antenna_channelised_voltage", 1234567890.0, "0,1:0,0", "0,1:0,1"
        )

    async def test_capture_start(self, client: aiokatcp.Client, sensor_proxy_client) -> None:
        """Test capture-start in the success case."""
        await self._configure_subarray(client, SUBARRAY_PRODUCT)
        await client.request("capture-start", "gpucbf_baseline_correlation_products")
        katcp_client = sensor_proxy_client
        # TODO: this doesn't check that the requests are going to the correct
        # nodes.
        katcp_client.request.assert_any_call("capture-start")

    async def test_capture_start_bad_stream(self, client: aiokatcp.Client) -> None:
        await self._configure_subarray(client, SUBARRAY_PRODUCT)
        await assert_request_fails(client, "capture-start", "foo")

    async def test_capture_start_wrong_stream_type(self, client: aiokatcp.Client) -> None:
        await self._configure_subarray(client, SUBARRAY_PRODUCT)
        await assert_request_fails(client, "capture-start", "gpucbf_antenna_channelised_voltage")

    async def test_capture_stop(self, client: aiokatcp.Client, sensor_proxy_client) -> None:
        # Note: most of the code for capture-stop is shared with capture-start,
        # so the testing does not need to be as thorough.
        await self._configure_subarray(client, SUBARRAY_PRODUCT)
        await client.request("capture-stop", "gpucbf_baseline_correlation_products")
        katcp_client = sensor_proxy_client
        # TODO: this doesn't check that the requests are going to the correct
        # nodes.
        katcp_client.request.assert_any_call("capture-stop")
        # Check that the state changed to down in capture-list
        _, informs = await client.request("capture-list", "gpucbf_baseline_correlation_products")
        assert len(informs) == 1
        assert informs[0].arguments[2] == b"down"

    async def test_capture_list_no_args(self, client: aiokatcp.Client) -> None:
        await self._configure_subarray(client, SUBARRAY_PRODUCT)
        _, informs = await client.request("capture-list")
        assert len(informs) == 2
        assert informs[0].arguments == [b"gpucbf_antenna_channelised_voltage", mock.ANY, b"up"]
        assert re.fullmatch(rb"239\.192\.\d+\.\d+\+3:7148", informs[0].arguments[1])
        assert informs[1].arguments == [b"gpucbf_baseline_correlation_products", mock.ANY, b"down"]
        assert re.fullmatch(rb"239\.192\.\d+\.\d+\+3:7148", informs[1].arguments[1])

    async def test_capture_list_explicit_stream(self, client: aiokatcp.Client) -> None:
        await self._configure_subarray(client, SUBARRAY_PRODUCT)
        _, informs = await client.request("capture-list", "gpucbf_antenna_channelised_voltage")
        assert len(informs) == 1
        assert informs[0].arguments == [b"gpucbf_antenna_channelised_voltage", mock.ANY, b"up"]
        assert re.fullmatch(rb"239\.192\.\d+\.\d+\+3:7148", informs[0].arguments[1])

    async def test_capture_list_bad_stream(self, client: aiokatcp.Client) -> None:
        await self._configure_subarray(client, SUBARRAY_PRODUCT)
        await assert_request_fails(client, "capture-list", "sdp_l0")

    def _check_prom(
        self,
        registry: CollectorRegistry,
        name: str,
        service: str,
        type: str,
        value: float,
        sample_name: Optional[str] = None,
        extra_labels: Optional[Mapping[str, str]] = None,
    ) -> None:
        name = "katsdpcontroller_" + name
        for metric in registry.collect():
            if metric.name == name:
                break
        else:
            raise KeyError(f"Metric {name} not found")
        assert metric.type == type
        labels = {"subarray_product_id": SUBARRAY_PRODUCT, "service": service}
        if sample_name is None:
            sample_name = name
        else:
            sample_name = "katsdpcontroller_" + sample_name
        if extra_labels is not None:
            labels.update(extra_labels)
        assert registry.get_sample_value(sample_name, labels) == value

    async def test_prom_sensors(self, registry: CollectorRegistry, server: DeviceServer) -> None:
        """Test that sensors are mirrored into Prometheus."""
        server.sensors.add(
            aiokatcp.Sensor(
                float,
                "foo.gauge",
                "(prometheus: gauge)",
                default=1.5,
                initial_status=Sensor.Status.NOMINAL,
            )
        )
        server.sensors.add(
            aiokatcp.Sensor(
                float,
                "foo.cheese.labelled-gauge",
                "(prometheus: gauge labels: type)",
                default=1,
                initial_status=Sensor.Status.NOMINAL,
            )
        )
        server.sensors.add(
            aiokatcp.Sensor(int, "foo.histogram", "(prometheus: histogram(1, 10, 100))")
        )
        self._check_prom(registry, "gauge", "foo", "gauge", 1.5)
        self._check_prom(
            registry, "labelled_gauge", "foo", "gauge", 1, extra_labels={"type": "cheese"}
        )
        self._check_prom(
            registry, "histogram", "foo", "histogram", 0, "histogram_bucket", {"le": "10.0"}
        )


class TestResources:
    """Test :class:`katsdpcontroller.product_controller.Resources`."""

    @pytest.fixture
    async def mc_server(self) -> AsyncGenerator[DummyMasterController, None]:
        mc_server = DummyMasterController("127.0.0.1", 0)
        await mc_server.start()
        yield mc_server
        await mc_server.stop()

    @pytest.fixture
    async def mc_client(self, mc_server) -> AsyncGenerator[aiokatcp.Client, None]:
        mc_address = device_server_sockname(mc_server)
        mc_client = await aiokatcp.Client.connect(mc_address[0], mc_address[1])
        yield mc_client
        mc_client.close()
        await mc_client.wait_closed()

    @pytest.fixture
    def resources(self, mc_client):
        return Resources(mc_client, SUBARRAY_PRODUCT)

    async def test_get_multicast_groups(self, resources) -> None:
        assert "239.192.0.1" == await resources.get_multicast_groups(1)
        assert "239.192.0.2+3" == await resources.get_multicast_groups(4)
