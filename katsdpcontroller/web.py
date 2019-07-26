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

import re
import json
import logging
import textwrap
from typing import Dict, List

import pkg_resources
import prometheus_async
from aiohttp import web
import aiohttp_jinja2
import jinja2

from . import master_controller, web_utils


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


@aiohttp_jinja2.template('index.html.j2')
def index_handler(request: web.Request) -> dict:
    server = request.app['server']
    general = json.loads(server.sensors['gui-urls'].value)
    product_names = json.loads(server.sensors['products'].value)
    products: Dict[str, List[dict]] = {name: [] for name in product_names}
    for sensor in server.sensors.values():
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


def make_app(server: master_controller.DeviceServer) -> web.Application:
    app = web.Application(middlewares=[web_utils.cache_control])
    app['server'] = server
    app.add_routes([
        web.get('/', index_handler),
        web.get('/metrics', prometheus_handler),
        #web.get('/ws', websocket_handler),
        #web.get('/rotate', rotate_handler),
        #web.get('/gui/{product}/{service}/{gui}/', missing_gui_handler),
        web.static('/static', pkg_resources.resource_filename('katsdpcontroller', 'static'))
    ])
    aiohttp_jinja2.setup(app, loader=jinja2.PackageLoader('katsdpcontroller'), autoescape=True)
    return app
