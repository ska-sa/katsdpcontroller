"""User-facing web server for the master controller.

It also (optionally) handles writing and maintaining a config file for
haproxy to provide stable URLs for dynamically-provisioned GUIs provided
by subarray products.

The URL scheme for proxying is designed for easy parsing with haproxy. A
sensor of the form :samp:`{product}.{service}.gui-urls` with a GUI titled
:samp:`{gui}` is mapped to :samp:`/gui/{product}/{service}/{gui}/`. If
:samp:`{service}` is empty, it becomes the literal string `product`. The title
is turned into a canonical form.
"""

import asyncio
import re
import json
import logging
import tempfile
import copy
import signal
import weakref
from typing import Dict, List, Set, Optional

import pkg_resources
import prometheus_async
import aiokatcp
import yarl
from aiohttp import web, WSMsgType
import aiohttp_jinja2
import jinja2

from . import master_controller, web_utils
from .controller import gui_label


logger = logging.getLogger(__name__)
GUI_URLS_RE = re.compile(r'^(?P<product>[^.]+)(?:\.(?P<service>.*))?\.gui-urls$')


async def prometheus_handler(request: web.Request) -> web.Response:
    response = await prometheus_async.aio.web.server_stats(request)
    if response.status == 200:
        # Avoid spamming logs (feeds into web_utils.AccessLogger).
        response.log_level = logging.DEBUG
    return response


def _get_guis(server: master_controller.DeviceServer) -> dict:
    general = json.loads(server.sensors['gui-urls'].value)
    product_names = json.loads(server.sensors['products'].value)
    products: Dict[str, List[dict]] = {name: [] for name in product_names}
    for sensor in server.sensors.values():
        if sensor.status != aiokatcp.Sensor.Status.NOMINAL:
            continue
        match = GUI_URLS_RE.match(sensor.name)
        if match and match.group('product') in products:
            guis = json.loads(sensor.value)
            product_name = match.group('product')
            service = match.group('service') or ''
            for gui in guis:
                products[product_name].append({**gui, 'service': service})
    for product in products.values():
        product.sort(key=lambda gui: (gui['service'], gui['title']))
    return {'general': general, 'products': products}


@aiohttp_jinja2.template('rotate_sd.html.j2')
def rotate_handler(request: web.Request) -> dict:
    defaults = {'rotate_interval': 5000, 'width': 500, 'height': 500}
    defaults.update(request.query)
    return defaults


@aiohttp_jinja2.template('index.html.j2')
async def index_handler(request: web.Request) -> dict:
    return _get_guis(request.app['server'])


async def favicon_handler(request: web.Request) -> web.Response:
    raise web.HTTPFound(location='static/favicon.ico')


@aiohttp_jinja2.template('missing_gui.html.j2', status=404)
async def missing_gui_handler(request: web.Request) -> dict:
    return {}


async def websocket_handler(request: web.Request) -> web.WebSocketResponse:
    """Run a websocket connection to inform client about GUIs.

    It accepts two commands:
    - `close`: closes the websocket
    - `guis`: returns :meth:`_get_guis` (JSON-encoded)

    Additionally, :meth:`_get_guis` is sent unsolicited when the sensors change.
    """
    ws = web.WebSocketResponse(timeout=60, autoping=True, heartbeat=30)
    await ws.prepare(request)

    updater = request.app['updater']
    updater.add_websocket(ws)
    try:
        async for msg in ws:
            if msg.type == WSMsgType.TEXT:
                if msg.data == 'close':
                    updater.remove_websocket(ws)
                    await ws.close()
                elif msg.data == 'guis':
                    await updater.update_websocket(ws)
                else:
                    logger.warning('unhandled command %r on websocket %s', msg.data, ws)
            elif msg.type == WSMsgType.ERROR:
                logger.error('ws connection closed with exception %s', ws.exception())
            else:
                logger.error('unhandled non-text data on websocket %s', ws)
    except asyncio.CancelledError:
        logger.info("Connection cancelled...")
    except Exception:
        logger.exception("Exception in websocket msg handler")
    finally:
        updater.remove_websocket(ws)
        logger.info("Websocket %s closed.", ws)
    return ws


class Haproxy:
    """Wraps an haproxy process and updates its config file on the fly."""

    def __init__(self) -> None:
        self._cfg = tempfile.NamedTemporaryFile(mode='w+', suffix='.cfg')
        self._pidfile = tempfile.NamedTemporaryFile(suffix='.pid')
        self._content = ''
        self._process: Optional[asyncio.subprocess.Process] = None
        env = jinja2.Environment(loader=jinja2.PackageLoader('katsdpcontroller'),
                                 undefined=jinja2.StrictUndefined,
                                 autoescape=False)   # autoescaping is for HTML
        self._template = env.get_template('haproxy.conf.j2')

    async def update(self, guis: dict) -> None:
        # Turn all the URLs into yarl.URL objects so that the template can extract
        # pieces of them, and make an URL-safe label for each.
        guis = copy.deepcopy(guis)
        for product in guis['products'].values():
            for gui in product:
                gui['href'] = yarl.URL(gui['href'])
                gui['label'] = gui_label(gui)
        # TODO: get the proper numbers
        content = self._template.render(proxy_port=8080, fallback_port=5004, guis=guis)
        if content != self._content:
            logger.info('Updating haproxy')
            self._cfg.seek(0)
            self._cfg.truncate(0)
            self._cfg.write(content)
            self._cfg.flush()
            if self._process is None:
                self._process = await asyncio.create_subprocess_exec(
                    'haproxy', '-W', '-p', self._pidfile.name, '-f', self._cfg.name)
            else:
                self._process.send_signal(signal.SIGUSR2)
            self._content = content

    async def close(self) -> None:
        if self._process is not None:
            if self._process.returncode is None:
                self._process.terminate()
            ret = await self._process.wait()
            if ret:
                logger.warning('haproxy exited with non-zero exit status %d', ret)
        self._cfg.close()
        self._pidfile.close()


class Updater:
    def __init__(self, server: master_controller.DeviceServer, use_haproxy: bool) -> None:
        def observer(sensor: aiokatcp.Sensor, reading: aiokatcp.Reading) -> None:
            dirty.set()

        self._server = server
        self._websockets: Set[web.WebSocketResponse] = set()
        self._dirty = dirty = asyncio.Event()
        self._guis: dict = {}
        self._observer = observer
        # Sensors for which we've set the observer
        self._observer_set: weakref.WeakSet[aiokatcp.Sensor] = weakref.WeakSet()
        self._haproxy: Optional[Haproxy] = Haproxy() if use_haproxy else None
        server.add_interface_changed_callback(dirty.set)
        self._task = asyncio.get_event_loop().create_task(self._update())
        dirty.set()     # Run the first update immediately

    def add_websocket(self, ws: web.WebSocketResponse) -> None:
        self._websockets.add(ws)
        ws['last_guis'] = None

    def remove_websocket(self, ws: web.WebSocketResponse) -> None:
        self._websockets.discard(ws)

    async def _send_update(self, ws: web.WebSocketResponse, gui: dict, force: bool) -> bool:
        if gui != ws['last_guis'] or force:
            # If send_json raises we don't know what state the WS client will
            # have, so just ensure we update it next time.
            ws['last_guis'] = None
            await ws.send_json(gui)
            ws['last_guis'] = gui
            return True
        else:
            return False

    async def update_websocket(self, ws: web.WebSocketResponse) -> None:
        await self._send_update(ws, _get_guis(self._server), True)

    async def close(self, app: web.Application) -> None:
        for sensor in self._observer_set:
            sensor.detach(self._observer)
        self._task.cancel()
        try:
            await self._task
        except asyncio.CancelledError:
            pass
        for ws in self._websockets:
            await ws.close()
        if self._haproxy:
            await self._haproxy.close()

    async def _update(self) -> None:
        while True:
            await self._dirty.wait()
            self._dirty.clear()
            logger.info('Updater woken up and updating state')

            # Ensure we are subscribed to changed in gui-urls sensors
            # (they don't typically change, but they are only initialised
            # after we are woken up to indicate that the sensor exists).
            for sensor in self._server.sensors.values():
                if sensor.name.endswith('.gui-urls') and sensor not in self._observer_set:
                    sensor.attach(self._observer)
                    self._observer_set.add(sensor)

            try:
                guis = _get_guis(self._server)
            except Exception:
                logger.exception('Failed to get GUI list')
                continue

            if self._haproxy:
                await self._haproxy.update(guis)

            sent = 0
            for ws in self._websockets:
                try:
                    sent += int(await self._send_update(ws, guis, False))
                except Exception:
                    logger.exception('Failed to send update to %s', ws)
            if sent:
                logger.info('Sent new GUIs to %d websocket(s)', sent)


def make_app(server: master_controller.DeviceServer, use_haproxy: bool) -> web.Application:
    app = web.Application(middlewares=[web_utils.cache_control])
    app['server'] = server
    app['updater'] = updater = Updater(server, use_haproxy)
    app.on_shutdown.append(updater.close)
    app.add_routes([
        web.get('/', index_handler),
        web.get('/favicon.ico', favicon_handler),
        web.get('/metrics', prometheus_handler),
        web.get('/ws', websocket_handler),
        web.get('/rotate', rotate_handler),
        web.get('/gui/{product}/{service}/{gui}/', missing_gui_handler),
        web.static('/static', pkg_resources.resource_filename('katsdpcontroller', 'static'))
    ])
    aiohttp_jinja2.setup(
        app,
        loader=jinja2.PackageLoader('katsdpcontroller'),
        context_processors=[aiohttp_jinja2.request_processor],
        autoescape=True)
    return app
