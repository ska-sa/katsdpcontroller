################################################################################
# Copyright (c) 2013-2024, National Research Foundation (SARAO)
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

"""Tests for :mod:`katsdpcontroller.web."""

import argparse
import asyncio
import copy
import functools
import json
import logging
import signal
import socket
from typing import Any, AsyncGenerator, Dict, List, Optional, Tuple, cast
from unittest import mock

import async_solipsism
import pytest
import yarl
from aiohttp.test_utils import TestClient, TestServer
from aiohttp.web import Application
from aiokatcp import Sensor, SensorSet

from katsdpcontroller import web

EXTERNAL_URL = yarl.URL("http://proxy.invalid:1234")
ROOT_GUI_URLS: List[Dict[str, Any]] = [
    {
        "title": "Test GUI 1",
        "description": "First GUI",
        "href": "http://gui1.invalid/foo",
        "category": "Link",
    },
    {
        "title": "Test GUI 2",
        "description": "Second GUI",
        "href": "http://gui2.invalid/foo",
        "category": "Dashboard",
    },
]
PRODUCT1_GUI_URLS: List[Dict[str, Any]] = [
    {
        "title": "Dashboard",
        "description": "Product dashboard",
        "href": "http://dashboard.product1.invalid:31000/",
        "category": "Dashboard",
    }
]
PRODUCT1_GUI_URLS_OUT: List[Dict[str, Any]] = [
    {
        "title": "Dashboard",
        "description": "Product dashboard",
        "href": yarl.URL("http://proxy.invalid:1234/gui/product1/product/dashboard/"),
        "orig_href": yarl.URL("http://dashboard.product1.invalid:31000/"),
        "category": "Dashboard",
        "label": "dashboard",
        "service": "",
    }
]
PRODUCT2_CAL_GUI_URLS: List[Dict[str, str]] = [
    {
        "title": "Cal diagnostics",
        "description": "Dask diagnostics for cal.1",
        "href": "http://product2.invalid:31001/gui/product2/cal.1/cal-diagnostics/status",
        "category": "Plot",
    }
]
PRODUCT2_GUI_URLS_OUT: List[Dict[str, Any]] = [
    {
        "title": "Cal diagnostics",
        "description": "Dask diagnostics for cal.1",
        "href": yarl.URL("http://proxy.invalid:1234/gui/product2/cal.1/cal-diagnostics/status"),
        "orig_href": yarl.URL(
            "http://product2.invalid:31001/gui/product2/cal.1/cal-diagnostics/status"
        ),
        "category": "Plot",
        "service": "cal.1",
        "label": "cal-diagnostics",
    }
]


def expected_gui_urls(
    gui_urls: List[Dict[str, Any]], haproxy: bool, yarl: bool
) -> List[Dict[str, Any]]:
    """Get the expected value of a gui-url list for a single product.

    The implementation transforms the list in slightly different ways depending
    on context.

    Parameters
    ----------
    gui_urls
        Expected value, with `href` properties set to the haproxy URL and URLs represented
        as :class:`yarl.URL` objects.
    haproxy
        If false, replace `href` with `orig_href`.
    yarl
        If false, replace `href` and `orig_href` with plain strings
    """
    gui_urls = copy.deepcopy(gui_urls)
    for gui in gui_urls:
        if not haproxy:
            gui["href"] = gui["orig_href"]
        if not yarl:
            gui["href"] = str(gui["href"])
            gui["orig_href"] = str(gui["orig_href"])
    return gui_urls


class DummyHaproxyProcess:
    """Mock for :class:`asyncio.subprocess.Process`.

    It is very specific to the way haproxy is run, and is not a
    general-purposes process mock.
    """

    def __init__(self, *argv: str) -> None:
        parser = argparse.ArgumentParser()
        parser.add_argument("-W", dest="master_worker", action="store_true")
        parser.add_argument("-p", required=True, dest="pid_file")
        parser.add_argument("-f", required=True, dest="config_file")
        try:
            self.args = parser.parse_args(argv[1:])
        except SystemExit as exc:
            raise RuntimeError(str(exc)) from exc
        self._update_config()
        self._dead = asyncio.Event()
        self.returncode: Optional[int] = None

    def _update_config(self) -> None:
        with open(self.args.config_file) as f:
            self.config = f.read()

    def _check_proc(self) -> None:
        if self.returncode is not None:
            raise ProcessLookupError()

    def died(self, returncode: int = 0) -> None:
        """Mock function to indicate that the process died"""
        if self.returncode is not None:
            raise RuntimeError("Process cannot die twice")
        self.returncode = returncode
        self._dead.set()

    def send_signal(self, sig: int) -> None:
        self._check_proc()
        if sig != signal.SIGUSR2:
            raise ValueError("Only SIGUSR2 is simulated")
        self._update_config()

    def terminate(self) -> None:
        self._check_proc()
        asyncio.get_event_loop().call_soon(self.died)

    def kill(self) -> None:
        self._check_proc()
        asyncio.get_event_loop().call_soon(functools.partial(self.died, -9))

    async def wait(self) -> int:
        await self._dead.wait()
        assert self.returncode is not None  # to keep mypy happy
        return self.returncode


async def dummy_create_subprocess_exec(*args: str) -> DummyHaproxyProcess:
    return DummyHaproxyProcess(*args)


def socket_factory(host: str, port: int, family: socket.AddressFamily) -> socket.socket:
    """Connects aiohttp test_utils to async-solipsism."""
    return async_solipsism.ListenSocket((host, port if port else 80))


class TestWeb:
    @pytest.fixture
    def use_haproxy(self) -> bool:
        return False  # Overridden in TestHaproxyWeb

    @pytest.fixture
    def haproxy_bind(self, use_haproxy) -> Optional[Tuple[str, int]]:
        return ("localhost.invalid", 80) if use_haproxy else None

    @pytest.fixture
    def event_loop_policy(self) -> async_solipsism.EventLoopPolicy:
        return async_solipsism.EventLoopPolicy()

    @pytest.fixture
    def mc_server(self, use_haproxy: bool) -> mock.MagicMock:
        mc_server = mock.MagicMock()
        mc_server.sensors = SensorSet()
        mc_server.orig_sensors = SensorSet()

        mc_server.sensors.add(
            Sensor(
                str,
                "products",
                "",
                default='["product1", "product2"]',
                initial_status=Sensor.Status.NOMINAL,
            )
        )
        mc_server.sensors.add(
            Sensor(
                str,
                "gui-urls",
                "",
                default=json.dumps(ROOT_GUI_URLS),
                initial_status=Sensor.Status.NOMINAL,
            )
        )

        mc_server.orig_sensors.add(
            Sensor(
                str,
                "product1.gui-urls",
                "",
                default=json.dumps(PRODUCT1_GUI_URLS),
                initial_status=Sensor.Status.NOMINAL,
            )
        )
        mc_server.orig_sensors.add(
            Sensor(
                str,
                "product2.cal.1.gui-urls",
                "",
                default=json.dumps(PRODUCT2_CAL_GUI_URLS),
                initial_status=Sensor.Status.NOMINAL,
            )
        )
        mc_server.orig_sensors.add(
            Sensor(
                str,
                "product2.ingest.1.gui-urls",
                "",
                default=json.dumps(PRODUCT2_CAL_GUI_URLS),
                initial_status=Sensor.Status.UNKNOWN,
            )
        )
        for sensor in mc_server.orig_sensors.values():
            if use_haproxy and sensor.name.endswith(".gui-urls"):
                new_value = web.rewrite_gui_urls(EXTERNAL_URL, sensor)
                new_sensor = Sensor(sensor.stype, sensor.name, sensor.description, sensor.units)
                new_sensor.set_value(new_value, timestamp=sensor.timestamp, status=sensor.status)
                mc_server.sensors.add(new_sensor)
            else:
                mc_server.sensors.add(sensor)
        return mc_server

    @pytest.fixture
    async def app(self, mc_server, haproxy_bind) -> Application:
        # Declared async to ensure that the pytest-asyncio event loop is
        # installed before anything tries to get the current event loop.
        return web.make_app(mc_server, haproxy_bind)

    @pytest.fixture
    async def server(self, app: Application) -> AsyncGenerator[TestServer, None]:
        server = TestServer(app, socket_factory=socket_factory)
        async with server:
            yield server

    @pytest.fixture
    async def client(self, server: TestServer) -> AsyncGenerator[TestClient, None]:
        async with TestClient(server) as client:
            yield client

    @pytest.fixture(autouse=True)
    def dirty_set(self, mc_server, client: TestClient) -> None:
        mc_server.add_interface_changed_callback.assert_called_once()
        dirty_set = mc_server.add_interface_changed_callback.mock_calls[0][1][0]
        dirty_set()
        return dirty_set

    async def test_get_guis(self, mc_server, use_haproxy: bool) -> None:
        guis = web._get_guis(mc_server)
        expected = {
            "general": ROOT_GUI_URLS,
            "products": {
                "product1": expected_gui_urls(PRODUCT1_GUI_URLS_OUT, use_haproxy, True),
                "product2": expected_gui_urls(PRODUCT2_GUI_URLS_OUT, use_haproxy, True),
            },
        }
        assert guis == expected

    def test_get_guis_not_nominal(self, mc_server) -> None:
        for name in ["gui-urls", "product1.gui-urls", "product2.cal.1.gui-urls"]:
            sensor = mc_server.sensors[name]
            sensor.set_value(sensor.value, status=Sensor.Status.ERROR)
        guis = web._get_guis(mc_server)
        expected = {"general": [], "products": {"product1": [], "product2": []}}
        assert guis == expected

    async def test_update_bad_gui_sensor(self, mc_server, caplog) -> None:
        with caplog.at_level(logging.ERROR, logger="katsdpcontroller.web"):
            mc_server.sensors["product1.gui-urls"].value = "not valid json"
            await asyncio.sleep(1)
        assert caplog.text

    async def test_index(self, client: TestClient) -> None:
        async with client.get("/") as resp:
            assert resp.status == 200
            assert resp.headers["Content-type"] == "text/html; charset=utf-8"
            assert resp.headers["Cache-control"] == "no-store"

    async def test_favicon(self, client: TestClient) -> None:
        async with client.get("/favicon.ico") as resp:
            assert resp.status == 200
            assert resp.headers["Content-type"] == "image/vnd.microsoft.icon"

    async def test_prometheus(self, client: TestClient) -> None:
        async with client.get("/metrics") as resp:
            assert resp.status == 200

    async def test_rotate(self, client: TestClient) -> None:
        # A good test of this probably needs Selenium plus a whole lot of
        # extra infrastructure.
        async with client.get("/rotate?width=500&height=600") as resp:
            text = await resp.text()
            assert resp.status == 200
            assert "width: 500" in text
            assert "height: 600" in text

    async def test_missing_gui(self, client: TestClient) -> None:
        for path in ["/gui/product3/service/label", "/gui/product3/service/label/foo"]:
            async with client.get(path) as resp:
                text = await resp.text()
                assert resp.status == 404
                assert "product3" in text

    async def test_websocket(
        self, client: TestClient, mc_server, dirty_set, use_haproxy: bool
    ) -> None:
        expected = {
            "general": ROOT_GUI_URLS,
            "products": {
                "product1": expected_gui_urls(PRODUCT1_GUI_URLS_OUT, use_haproxy, False),
                "product2": expected_gui_urls(PRODUCT2_GUI_URLS_OUT, use_haproxy, False),
            },
        }
        async with client.ws_connect("/ws") as ws:
            await ws.send_str("guis")
            guis = await ws.receive_json()
            assert guis == expected
            # Updating should trigger an unsolicited update
            sensor = mc_server.sensors["products"]
            sensor.value = '["product1"]'
            del expected["products"]["product2"]  # type: ignore
            dirty_set()
            await asyncio.sleep(1)
            guis = await ws.receive_json()
            assert guis == expected
            await ws.close()

    async def test_websocket_close_server(self, client: TestClient, server: TestServer) -> None:
        """Close the server while a websocket is still connected"""
        async with client.ws_connect("/ws"):
            await server.close()


class TestWebHaproxy(TestWeb):
    @pytest.fixture
    def use_haproxy(self) -> bool:
        return True

    @pytest.fixture(autouse=True)
    def mock_subprocess(self, mocker) -> None:
        mocker.patch("asyncio.create_subprocess_exec", dummy_create_subprocess_exec)

    @pytest.fixture(autouse=True)
    def _set_app_internal_port(self, app) -> None:
        app[web.updater_key].internal_port = 2345
        return app

    def test_internal_port(self, app) -> None:
        """Tests the internal_port property getter"""
        assert app[web.updater_key].internal_port == 2345

    async def test_null_update(self, mocker, dirty_set) -> None:
        """Triggered update must not poke haproxy if not required"""
        m = mocker.patch.object(DummyHaproxyProcess, "send_signal")
        dirty_set()
        await asyncio.sleep(1)
        m.assert_not_called()

    async def test_update(self, app: Application, mc_server) -> None:
        """Sensor update must update haproxy config and reload it"""
        sensor = mc_server.sensors["product1.gui-urls"]
        guis = json.loads(sensor.value)
        guis[0]["title"] = "Test Title"
        sensor.value = json.dumps(guis).encode()
        await asyncio.sleep(1)
        haproxy = app[web.updater_key]._haproxy
        assert haproxy is not None
        assert "test-title" in cast(DummyHaproxyProcess, haproxy._process).config

    async def test_haproxy_died(self, app: Application, client: TestClient, caplog) -> None:
        """Gracefully handle haproxy dying on its own"""
        with caplog.at_level(logging.WARNING, "katsdpcontroller.web"):
            await asyncio.sleep(1)  # Give simulated process a chance to start
            haproxy = app[web.updater_key]._haproxy
            assert haproxy is not None
            cast(DummyHaproxyProcess, haproxy._process).died(-9)
            await client.close()
        assert caplog.record_tuples == [
            ("katsdpcontroller.web", logging.WARNING, "haproxy exited with non-zero exit status -9")
        ]
