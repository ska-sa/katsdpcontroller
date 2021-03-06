"""Tests for :mod:`katsdpcontroller.web."""

import asyncio
import argparse
import copy
import json
import functools
import logging
import signal
from unittest import mock
from typing import List, Dict, Tuple, Any, Optional

import yarl
from aiokatcp import Sensor, SensorSet
from aiohttp.test_utils import TestClient, TestServer
import asynctest

from .. import web
from .utils import create_patch


EXTERNAL_URL = yarl.URL('http://proxy.invalid:1234')
ROOT_GUI_URLS: List[Dict[str, Any]] = [
    {
        "title": "Test GUI 1",
        "description": "First GUI",
        "href": "http://gui1.invalid/foo",
        "category": "Link"
    },
    {
        "title": "Test GUI 2",
        "description": "Second GUI",
        "href": "http://gui2.invalid/foo",
        "category": "Dashboard"
    }
]
PRODUCT1_GUI_URLS: List[Dict[str, Any]] = [
    {
        "title": "Dashboard",
        "description": "Product dashboard",
        "href": "http://dashboard.product1.invalid:31000/",
        "category": "Dashboard"
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
        "service": ""
    }
]
PRODUCT2_CAL_GUI_URLS: List[Dict[str, str]] = [
    {
        "title": "Cal diagnostics",
        "description": "Dask diagnostics for cal.1",
        "href": "http://product2.invalid:31001/gui/product2/cal.1/cal-diagnostics/status",
        "category": "Plot"
    }
]
PRODUCT2_GUI_URLS_OUT: List[Dict[str, Any]] = [
    {
        "title": "Cal diagnostics",
        "description": "Dask diagnostics for cal.1",
        "href": yarl.URL("http://proxy.invalid:1234/gui/product2/cal.1/cal-diagnostics/status"),
        "orig_href":
            yarl.URL("http://product2.invalid:31001/gui/product2/cal.1/cal-diagnostics/status"),
        "category": "Plot",
        "service": "cal.1",
        "label": "cal-diagnostics"
    }
]


def expected_gui_urls(gui_urls: List[Dict[str, Any]],
                      haproxy: bool, yarl: bool) -> List[Dict[str, Any]]:
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
            gui['href'] = gui['orig_href']
        if not yarl:
            gui['href'] = str(gui['href'])
            gui['orig_href'] = str(gui['orig_href'])
    return gui_urls


class DummyHaproxyProcess:
    """Mock for :class:`asyncio.subprocess.Process`.

    It is very specific to the way haproxy is run, and is not a
    general-purposes process mock.
    """
    def __init__(self, *argv: str) -> None:
        parser = argparse.ArgumentParser()
        parser.add_argument('-W', dest='master_worker', action='store_true')
        parser.add_argument('-p', required=True, dest='pid_file')
        parser.add_argument('-f', required=True, dest='config_file')
        try:
            self.args = parser.parse_args(argv[1:])
        except SystemExit as exc:
            raise RuntimeError(str(exc)) from exc
        self._update_config()
        self._dead = asyncio.Event()
        self.returncode: Optional[int] = None

    def _update_config(self) -> None:
        with open(self.args.config_file, 'r') as f:
            self.config = f.read()

    def _check_proc(self) -> None:
        if self.returncode is not None:
            raise ProcessLookupError()

    def died(self, returncode: int = 0) -> None:
        """Mock function to indicate that the process died"""
        if self.returncode is not None:
            raise RuntimeError('Process cannot die twice')
        self.returncode = returncode
        self._dead.set()

    def send_signal(self, sig: int) -> None:
        self._check_proc()
        if sig != signal.SIGUSR2:
            raise ValueError('Only SIGUSR2 is simulated')
        self._update_config()

    def terminate(self) -> None:
        self._check_proc()
        asyncio.get_event_loop().call_soon(self.died)

    def kill(self) -> None:
        self._check_proc()
        asyncio.get_event_loop().call_soon(functools.partial(self.died, -9))

    async def wait(self) -> int:
        await self._dead.wait()
        assert self.returncode is not None     # to keep mypy happy
        return self.returncode


async def dummy_create_subprocess_exec(*args: str) -> DummyHaproxyProcess:
    return DummyHaproxyProcess(*args)


class TestWeb(asynctest.ClockedTestCase):
    haproxy_bind: Optional[Tuple[str, int]] = None
    maxDiff = None

    async def setUp(self) -> None:
        self.mc_server = mock.MagicMock()
        self.mc_server.sensors = SensorSet()
        self.mc_server.orig_sensors = SensorSet()

        self.mc_server.sensors.add(
            Sensor(str, 'products', '', default='["product1", "product2"]',
                   initial_status=Sensor.Status.NOMINAL))
        self.mc_server.sensors.add(
            Sensor(str, 'gui-urls', '', default=json.dumps(ROOT_GUI_URLS),
                   initial_status=Sensor.Status.NOMINAL))

        self.mc_server.orig_sensors.add(
            Sensor(str, 'product1.gui-urls', '', default=json.dumps(PRODUCT1_GUI_URLS),
                   initial_status=Sensor.Status.NOMINAL))
        self.mc_server.orig_sensors.add(
            Sensor(str, 'product2.cal.1.gui-urls', '', default=json.dumps(PRODUCT2_CAL_GUI_URLS),
                   initial_status=Sensor.Status.NOMINAL))
        self.mc_server.orig_sensors.add(
            Sensor(str, 'product2.ingest.1.gui-urls', '', default=json.dumps(PRODUCT2_CAL_GUI_URLS),
                   initial_status=Sensor.Status.UNKNOWN))
        for sensor in self.mc_server.orig_sensors.values():
            if self.haproxy_bind is not None and sensor.name.endswith('.gui-urls'):
                new_value = web.rewrite_gui_urls(EXTERNAL_URL, sensor)
                new_sensor = Sensor(sensor.stype, sensor.name, sensor.description, sensor.units)
                new_sensor.set_value(new_value, timestamp=sensor.timestamp, status=sensor.status)
                self.mc_server.sensors.add(new_sensor)
            else:
                self.mc_server.sensors.add(sensor)

        self.app = web.make_app(self.mc_server, self.haproxy_bind)
        self.server = TestServer(self.app)
        self.client = TestClient(self.server)
        await self.client.start_server()
        self.addCleanup(self.client.close)
        self.mc_server.add_interface_changed_callback.assert_called_once()
        self.dirty_set = self.mc_server.add_interface_changed_callback.mock_calls[0][1][0]
        self.dirty_set()
        await self.advance(1)

    async def test_get_guis(self) -> None:
        guis = web._get_guis(self.mc_server)
        haproxy = self.haproxy_bind is not None
        expected = {
            "general": ROOT_GUI_URLS,
            "products": {
                "product1": expected_gui_urls(PRODUCT1_GUI_URLS_OUT, haproxy, True),
                "product2": expected_gui_urls(PRODUCT2_GUI_URLS_OUT, haproxy, True)
            }
        }
        self.assertEqual(guis, expected)

    async def test_get_guis_not_nominal(self) -> None:
        for name in ['gui-urls', 'product1.gui-urls', 'product2.cal.1.gui-urls']:
            sensor = self.mc_server.sensors[name]
            sensor.set_value(sensor.value, status=Sensor.Status.ERROR)
        guis = web._get_guis(self.mc_server)
        expected = {
            "general": [],
            "products": {"product1": [], "product2": []}
        }
        self.assertEqual(guis, expected)

    async def test_update_bad_gui_sensor(self) -> None:
        with self.assertLogs('katsdpcontroller.web', logging.ERROR):
            self.mc_server.sensors['product1.gui-urls'].value = 'not valid json'
            await self.advance(1)

    async def test_index(self) -> None:
        async with self.client.get('/') as resp:
            self.assertEqual(resp.status, 200)
            self.assertEqual(resp.headers['Content-type'], 'text/html; charset=utf-8')
            self.assertEqual(resp.headers['Cache-control'], 'no-store')

    async def test_favicon(self) -> None:
        async with self.client.get('/favicon.ico') as resp:
            self.assertEqual(resp.status, 200)
            self.assertEqual(resp.headers['Content-type'], 'image/vnd.microsoft.icon')

    async def test_prometheus(self) -> None:
        async with self.client.get('/metrics') as resp:
            self.assertEqual(resp.status, 200)

    async def test_rotate(self) -> None:
        # A good test of this probably needs Selenium plus a whole lot of
        # extra infrastructure.
        async with self.client.get('/rotate?width=500&height=600') as resp:
            text = await resp.text()
            self.assertEqual(resp.status, 200)
            self.assertIn('width: 500', text)
            self.assertIn('height: 600', text)

    async def test_missing_gui(self) -> None:
        for path in ['/gui/product3/service/label', '/gui/product3/service/label/foo']:
            async with self.client.get(path) as resp:
                text = await resp.text()
                self.assertEqual(resp.status, 404)
                self.assertIn('product3', text)

    async def test_websocket(self) -> None:
        haproxy = self.haproxy_bind is not None
        expected = {
            "general": ROOT_GUI_URLS,
            "products": {
                "product1": expected_gui_urls(PRODUCT1_GUI_URLS_OUT, haproxy, False),
                "product2": expected_gui_urls(PRODUCT2_GUI_URLS_OUT, haproxy, False)
            }
        }
        async with self.client.ws_connect('/ws') as ws:
            await ws.send_str('guis')
            guis = await ws.receive_json()
            self.assertEqual(guis, expected)
            # Updating should trigger an unsolicited update
            sensor = self.mc_server.sensors['products']
            sensor.value = '["product1"]'
            del expected["products"]["product2"]    # type: ignore
            self.dirty_set()
            await self.advance(1)
            guis = await ws.receive_json()
            self.assertEqual(guis, expected)
            await ws.close()

    async def test_websocket_close_server(self) -> None:
        """Close the server while a websocket is still connected"""
        async with self.client.ws_connect('/ws'):
            await self.server.close()


class TestWebHaproxy(TestWeb):
    haproxy_bind = ('localhost.invalid', 80)

    async def setUp(self) -> None:
        create_patch(self, 'asyncio.create_subprocess_exec',
                     dummy_create_subprocess_exec)
        await super().setUp()
        self.app['updater'].internal_port = 2345
        await self.advance(1)

    def test_internal_port(self) -> None:
        """Tests the internal_port property getter"""
        self.assertEqual(self.app['updater'].internal_port, 2345)

    async def test_null_update(self) -> None:
        """Triggered update must not poke haproxy if not required"""
        with mock.patch.object(DummyHaproxyProcess, 'send_signal') as m:
            self.dirty_set()
            await self.advance(1)
            m.assert_not_called()

    async def test_update(self) -> None:
        """Sensor update must update haproxy config and reload it"""
        sensor = self.mc_server.sensors['product1.gui-urls']
        guis = json.loads(sensor.value)
        guis[0]['title'] = 'Test Title'
        sensor.value = json.dumps(guis).encode()
        await self.advance(1)
        self.assertIn('test-title', self.app['updater']._haproxy._process.config)

    async def test_haproxy_died(self) -> None:
        """Gracefully handle haproxy dying on its own"""
        with self.assertLogs('katsdpcontroller.web', logging.WARNING) as cm:
            self.app['updater']._haproxy._process.died(-9)
            await self.client.close()
        self.assertEqual(
            cm.output,
            ['WARNING:katsdpcontroller.web:haproxy exited with non-zero exit status -9'])
