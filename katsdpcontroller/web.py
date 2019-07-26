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
import textwrap
from typing import Dict, List, Set

import pkg_resources
import prometheus_async
import aiokatcp
from aiohttp import web, WSMsgType
import aiohttp_jinja2
import jinja2

from . import master_controller, web_utils


logger = logging.getLogger(__name__)
GUI_URLS_RE = re.compile(r'^(?P<product>[^.]+)(?:\.(?P<service>.*))?\.gui-urls$')
HAPROXY_HEADER = textwrap.dedent(r"""
    global
        maxconn 256

    defaults
        mode http
        timeout connect 5s
        timeout client 50s
        timeout server 50s

    frontend http-in
        bind *:{proxy_port}
        acl missing_slash path_reg '^/gui/[^/]+/[^/]+/[^/]+$'
        acl has_gui path_reg '^/gui/[^/]+/[^/]+/[^/]+/'
        http-request redirect code 301 prefix / drop-query append-slash if missing_slash
        http-request set-var(req.product) path,field(1,/) if has_gui
        http-request set-var(req.service) path,field(2,/) if has_gui
        http-request set-var(req.gui) path,field(3,/) if has_gui
        use_backend %[var(req.product)]:%[var(req.service)]:%[var(req.gui)] if has_gui
        default_backend fallback

    backend fallback
        server fallback_html_server 127.0.0.1:{fallback_port}
    """)


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
def index_handler(request: web.Request) -> dict:
    return _get_guis(request.app['server'])


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


class Updater:
    def __init__(self, server: master_controller.DeviceServer) -> None:
        self._server = server
        self._websockets: Set[web.WebSocketResponse] = set()
        self._dirty = asyncio.Event()
        self._guis: dict = {}
        server.add_interface_changed_callback(self._dirty.set)
        self._task = asyncio.get_event_loop().create_task(self._update())

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
        self._task.cancel()
        try:
            await self._task
        except asyncio.CancelledError:
            pass
        for ws in self._websockets:
            await ws.close()

    async def _update(self) -> None:
        while True:
            await self._dirty.wait()
            self._dirty.clear()
            logger.info('Updater woken up and updating state')
            try:
                guis = _get_guis(self._server)
            except Exception:
                logger.exception('Failed to get GUI list')
                continue
            # TODO: haproxy stuff goes here

            sent = 0
            for ws in self._websockets:
                try:
                    sent += int(await self._send_update(ws, guis, False))
                except Exception:
                    logger.exception('Failed to send update to %s', ws)
            if sent:
                logger.info('Sent new GUIs to %d websocket(s)', sent)


def make_app(server: master_controller.DeviceServer) -> web.Application:
    app = web.Application(middlewares=[web_utils.cache_control])
    app['server'] = server
    app['updater'] = updater = Updater(server)
    app.on_shutdown.append(updater.close)
    app.add_routes([
        web.get('/', index_handler),
        web.get('/metrics', prometheus_handler),
        web.get('/ws', websocket_handler),
        web.get('/rotate', rotate_handler),
        #web.get('/gui/{product}/{service}/{gui}/', missing_gui_handler),
        web.static('/static', pkg_resources.resource_filename('katsdpcontroller', 'static'))
    ])
    aiohttp_jinja2.setup(app, loader=jinja2.PackageLoader('katsdpcontroller'), autoescape=True)
    return app
