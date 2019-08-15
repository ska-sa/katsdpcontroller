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
import signal
import weakref
import functools
from typing import Dict, List, Set, Tuple, Optional

import pkg_resources
import prometheus_async
from aiokatcp import Sensor, Reading
import yarl
from aiohttp import web, WSMsgType
import aiohttp_jinja2
import jinja2

from . import master_controller, web_utils


logger = logging.getLogger(__name__)
GUI_URLS_RE = re.compile(r'^(?P<product>[^.]+)(?:\.(?P<service>.*))?\.gui-urls$')


async def _prometheus_handler(request: web.Request) -> web.Response:
    response = await prometheus_async.aio.web.server_stats(request)
    if response.status == 200:
        # Avoid spamming logs (feeds into web_utils.AccessLogger).
        response.log_level = logging.DEBUG
    return response


def _dump_yarl(obj: object) -> str:
    """Use as a ``default`` function for :func:`json.dumps`.

    It converts :class:`yarl.URL` objects to their string form.
    """
    if isinstance(obj, yarl.URL):
        return str(obj)
    else:
        raise TypeError


def _get_guis(server: master_controller.DeviceServer) -> dict:
    if server.sensors['gui-urls'].status == Sensor.Status.NOMINAL:
        general = json.loads(server.sensors['gui-urls'].value)
    else:
        general = []
    product_names = json.loads(server.sensors['products'].value)
    products: Dict[str, List[dict]] = {name: [] for name in product_names}
    for sensor in server.sensors.values():
        if sensor.status != Sensor.Status.NOMINAL:
            continue
        match = GUI_URLS_RE.match(sensor.name)
        if match and match.group('product') in products:
            guis = json.loads(sensor.value)
            orig_guis = json.loads(server.orig_sensors[sensor.name].value)
            product_name = match.group('product')
            service = match.group('service') or ''
            for gui, orig_gui in zip(guis, orig_guis):
                products[product_name].append({**gui,
                                               'service': service,
                                               'label': gui_label(gui),
                                               'href': yarl.URL(gui['href']),
                                               'orig_href': yarl.URL(orig_gui['href'])})
    for product in products.values():
        product.sort(key=lambda gui: (gui['service'], gui['title']))
    return {'general': general, 'products': products}


@aiohttp_jinja2.template('rotate_sd.html.j2')
def _rotate_handler(request: web.Request) -> dict:
    defaults = {'rotate_interval': 5000, 'width': 500, 'height': 500}
    defaults.update(request.query)
    return defaults


@aiohttp_jinja2.template('index.html.j2')
async def _index_handler(request: web.Request) -> dict:
    return _get_guis(request.app['server'])


async def _favicon_handler(request: web.Request) -> web.Response:
    raise web.HTTPFound(location='static/favicon.ico')


@aiohttp_jinja2.template('missing_gui.html.j2', status=404)
async def _missing_gui_handler(request: web.Request) -> dict:
    return {}


async def _websocket_handler(request: web.Request) -> web.WebSocketResponse:
    """Run a websocket connection to inform client about GUIs.

    It only accepts one command:
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
                if msg.data == 'guis':
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
        await ws.close()
        logger.info("Websocket %s closed.", ws)
    return ws


class _Haproxy:
    """Wraps an haproxy process and updates its config file on the fly."""

    def __init__(self, haproxy_bind: Tuple[str, int]) -> None:
        self._cfg = tempfile.NamedTemporaryFile(mode='w+', suffix='.cfg')
        self._pidfile = tempfile.NamedTemporaryFile(suffix='.pid')
        self._content = ''
        self._process: Optional[asyncio.subprocess.Process] = None
        self.haproxy_bind = haproxy_bind
        env = jinja2.Environment(loader=jinja2.PackageLoader('katsdpcontroller'),
                                 undefined=jinja2.StrictUndefined,
                                 autoescape=False)   # autoescaping is for HTML
        self._template = env.get_template('haproxy.conf.j2')

    async def update(self, guis: dict, internal_port: int) -> None:
        content = self._template.render(haproxy_bind=self.haproxy_bind,
                                        internal_port=internal_port,
                                        guis=guis)
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


class _Updater:
    def __init__(self, server: master_controller.DeviceServer,
                 haproxy_bind: Optional[Tuple[str, int]]) -> None:
        def observer(sensor: Sensor, reading: Reading) -> None:
            dirty.set()

        self._server = server
        self._websockets: Set[web.WebSocketResponse] = set()
        self._dirty = dirty = asyncio.Event()
        self._guis: dict = {}
        self._observer = observer
        # Sensors for which we've set the observer
        self._observer_set: weakref.WeakSet[Sensor] = weakref.WeakSet()
        self._haproxy: Optional[_Haproxy] = _Haproxy(haproxy_bind) if haproxy_bind else None
        server.add_interface_changed_callback(dirty.set)
        self._task = asyncio.get_event_loop().create_task(self._update())
        self._internal_port = 0    # Port running the internal web server (set by property later)

    @property
    def internal_port(self) -> int:
        return self._internal_port

    @internal_port.setter
    def internal_port(self, value: int) -> None:
        self._internal_port = value
        self._dirty.set()

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
            await ws.send_json(gui, dumps=functools.partial(json.dumps, default=_dump_yarl))
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
        for ws in list(self._websockets):
            await ws.close()
        if self._haproxy:
            await self._haproxy.close()

    async def _update(self) -> None:
        while True:
            await self._dirty.wait()
            # May be multiple back-to-back events, and we don't want to wake up
            # for each of them. So wait a bit to collect them together.
            await asyncio.sleep(0.1)
            self._dirty.clear()
            if not self._internal_port and self._haproxy:
                logger.info('Updater woken but internal port is not yet set')
                continue
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
                await self._haproxy.update(guis, self._internal_port)

            sent = 0
            for ws in self._websockets:
                try:
                    sent += int(await self._send_update(ws, guis, False))
                except Exception:
                    logger.exception('Failed to send update to %s', ws)
            if sent:
                logger.info('Sent new GUIs to %d websocket(s)', sent)


def gui_label(gui: dict) -> str:
    return re.sub(r'[^a-z0-9_-]', '-', gui['title'].lower())


def rewrite_gui_urls(external_url: yarl.URL, sensor: Sensor) -> bytes:
    if sensor.status != Sensor.Status.NOMINAL:
        return sensor.value
    parts = sensor.name.split('.')
    product = parts[0]
    service = '.'.join(parts[1:-1])
    if service == '':
        service = 'product'
    try:
        value = json.loads(sensor.value)
        for gui in value:
            label = gui_label(gui)
            prefix = f'gui/{product}/{service}/{label}/'
            orig_path = yarl.URL(gui['href']).path[1:]
            # If the original URL is already under the right path, we
            # assume that it has been set up to avoid path rewriting by
            # haproxy. In that case, anything after the prefix is a path
            # within the dashboard, rather than its root, and should be
            # preserved in the rewritten URL.
            if orig_path.startswith(prefix):
                prefix = orig_path
            gui['href'] = str(external_url / prefix)
        return json.dumps(value).encode()
    except (TypeError, ValueError, KeyError) as exc:
        logger.warning('Invalid gui-url in %s: %s', sensor.name, exc)
        return sensor.value


def make_app(server: master_controller.DeviceServer,
             haproxy_bind: Optional[Tuple[str, int]]) -> web.Application:
    """Create the web application.

    If `haproxy_bind` is provided, also run an haproxy process on this host:port
    which will forward requests either to the web application or to individual
    dashboards. In this case, the caller must update
    `app['updater'].internal_port` with the port on which this web application
    is listening.
    """
    app = web.Application(middlewares=[web_utils.cache_control])
    app['server'] = server
    app['updater'] = updater = _Updater(server, haproxy_bind)
    app.on_shutdown.append(updater.close)
    app.add_routes([
        web.get('/', _index_handler),
        web.get('/favicon.ico', _favicon_handler),
        web.get('/metrics', _prometheus_handler),
        web.get('/ws', _websocket_handler),
        web.get('/rotate', _rotate_handler),
        web.get('/gui/{product}/{service}/{gui}{path:.*}', _missing_gui_handler),
        web.static('/static', pkg_resources.resource_filename('katsdpcontroller', 'static'))
    ])
    aiohttp_jinja2.setup(
        app,
        loader=jinja2.PackageLoader('katsdpcontroller'),
        context_processors=[aiohttp_jinja2.request_processor],
        autoescape=True)
    return app
