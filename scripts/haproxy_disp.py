#!/usr/bin/env python3

"""
Run haproxy to reverse-proxy signal displays. To use it, run as

docker run -p <PORT>:8080 sdp-docker-registry.kat.ac.za:5000/katsdpcontroller \
haproxy_disp.py <sdpmchost>:5001

Then connect to the machine on http://<HOST>:<PORT>/<subarray_product> to get the signal
displays from that subarray-product. Any URL that doesn't correspond to a known subarray product
redirects to the root, which will have a list of links.
"""

import re
import argparse
import signal
import logging
import tempfile
import textwrap
from collections import defaultdict
import asyncio
import json

import async_timeout
import aiohttp
import aiohttp.web
import aiohttp_jinja2
import jinja2
import aiokatcp
import katsdpservices
from katsdptelstate.endpoint import endpoint_parser


logger = logging.getLogger(__name__)
HAPROXY_HEADER = textwrap.dedent(r"""
    global
        maxconn 256

    defaults
        mode http
        timeout connect 5s
        timeout client 50s
        timeout server 50s

    frontend http-in
        bind *:8080
        acl missing_slash path_reg '^/array_\d+_[a-zA-Z0-9]+/[^/]+$'
        acl has_array_stream path_reg '^/array_\d+_[a-zA-Z0-9]+/[^/]+/'
        http-request redirect code 301 prefix / drop-query append-slash if missing_slash
        http-request set-var(req.array) path,field(2,/) if has_array_stream
        http-request set-var(req.stream) path,field(3,/) if has_array_stream
        use_backend %[var(req.array)]_%[var(req.stream)] if has_array_stream
        default_backend fallback

    backend fallback
        acl root path_reg '^/$'
        acl favicon path_reg '^/favicon.ico$'
        acl rotate path_reg '^/rotate'
        acl ws path_reg '^/ws'
        http-request redirect code 303 location / drop-query if !root !favicon !rotate !ws
        server fallback_html_server 127.0.0.1:{port}
    """)


class Client(aiokatcp.Client):
    def __init__(self, wake, *args, **kwargs):
        self.wake = wake
        super().__init__(*args, **kwargs)
        self.add_connected_callback(self._notify_connected)
        self.add_disconnected_callback(self._notify_disconnected)

    def _notify_connected(self):
        logger.debug('Waking open on connect')
        self.wake.set()

    def _notify_disconnected(self):
        logger.debug('Waking open on disconnect')
        self.wake.set()

    def inform_interface_changed(self, *args):
        """Handle the interface-changed inform"""
        logger.debug('Waking up on interface-changed')
        self.wake.set()


def terminate():
    loop = asyncio.get_event_loop()
    loop.remove_signal_handler(signal.SIGINT)
    loop.remove_signal_handler(signal.SIGTERM)
    main_task.cancel()


class Server:
    def __init__(self):
        self.html_endpoint = None

    def __str__(self):
        return str(self.html_endpoint) if self.html_endpoint else ''


async def get_servers(client):
    servers = defaultdict(Server)
    if client.is_connected:
        sensor_regex = r'^(array_\d+_[A-Za-z0-9]+)\.timeplot\.([^.]*)\.html_port$'
        with async_timeout.timeout(30):
            reply, informs = await client.request('sensor-value', '/' + sensor_regex + '/')
        for inform in informs:
            if len(inform.arguments) != 5:
                logger.warning('#sensor-value inform has wrong number of arguments, ignoring')
                continue
            name = inform.arguments[2].decode('utf-8')
            status = inform.arguments[3].decode('utf-8')
            value = inform.arguments[4].decode('utf-8')
            if status != 'nominal':
                logger.warning('sensor %s is in state %s, ignoring', name, status)
                continue
            match = re.match(sensor_regex, name)
            if not match:
                logger.warning('sensor %s does not match the requested regex, ignoring', name)
                continue
            array = match.group(1)
            stream = match.group(2)
            servers[(array, stream)].html_endpoint = value
    logger.info("Get servers complete: %d found", len(servers))
    return servers


@aiohttp_jinja2.template('rotate_sd.html.j2')
def rotate_handler(request):
    defaults = {'rotate_interval': 5000, 'width': 500, 'height': 500}
    defaults.update(request.query)
    return dict(args=defaults)


@aiohttp_jinja2.template('index.html.j2')
def request_handler(request):
    servers = request.app['servers']
    return dict(servers=servers)


async def websocket_handler(request):
    ws = aiohttp.web.WebSocketResponse(timeout=60, autoping=True, heartbeat=30)
    await ws.prepare(request)

    request.app['websockets'].add(ws)
    try:
        async for msg in ws:
            if msg.type == aiohttp.WSMsgType.TEXT:
                if msg.data == 'close':
                    request.app['websockets'].discard(ws)
                    await ws.close()
                elif msg.data == 'servers':
                    server_dict = {pretty(array_stream): "http://{}".format(server)
                                   for array_stream, server in request.app['servers'].items()}
                    await ws.send_json(server_dict)
                else:
                    await ws.send_str(msg.data + '/ping/')
            elif msg.type == aiohttp.WSMsgType.ERROR:
                logger.error('ws connection closed with exception %s', ws.exception())
    except asyncio.CancelledError:
        logger.info("Connection cancelled...")
    except Exception:
        logger.exception("Exception in websocket msg handler")
    finally:
        request.app['websockets'].discard(ws)
        logger.info("Websocket %s closed.", ws)
    return ws


def pretty(array_stream):
    return "{0[0]} {0[1]}".format(array_stream)


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('sdpmc', type=endpoint_parser(5001),
                        help='host[:port] of the SDP master controller')
    parser.add_argument('-p', '--aioport', dest='aioport', metavar='PORT', default=5005,
                        help='Port to assign to the internal aiohttp webserver. '
                             '[default=%(default)s]')
    args = parser.parse_args()

    loop = asyncio.get_event_loop()
    wake = asyncio.Event()
    for signum in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(signum, terminate)

    app = aiohttp.web.Application()
    app['servers'] = {}
    app['websockets'] = set()
    app.router.add_get('/', request_handler)
    app.router.add_get('/rotate', rotate_handler)
    app.router.add_get('/ws', websocket_handler)
    aiohttp_jinja2.setup(app, loader=jinja2.PackageLoader('katsdpcontroller'))
    handler = app.make_handler()
    srv = await loop.create_server(handler, '', args.aioport)
    port = srv.sockets[0].getsockname()[1]
    logger.info("Local webserver on {}".format(args.aioport))

    cfg = tempfile.NamedTemporaryFile(mode='w+', suffix='.cfg')
    pidfile = tempfile.NamedTemporaryFile(suffix='.pid')
    client = None
    haproxy = None
    content = None
    try:
        client = Client(wake, args.sdpmc.host, args.sdpmc.port)
        while True:
            await wake.wait()
            wake.clear()
            logger.info('Updating state')
            try:
                servers = await get_servers(client)
            except Exception:
                logger.exception('Failed to get server list')
                continue
            old_content = content
            content = HAPROXY_HEADER.format(port=port)
            for array_stream, server in sorted(servers.items()):
                array, stream = array_stream
                if not server.html_endpoint:
                    logger.warning('Stream %s/%s has no signal display html port', array, stream)
                    continue
                logger.info("Adding server %s", server.html_endpoint)
                content += textwrap.dedent(r"""
                    backend {array}_{stream}
                        http-request set-path %[path,regsub(^/.*?/.*?/,/)]
                        server {array}_{stream}_html_server {server.html_endpoint}
                    """.format(array=array, stream=stream, server=server))
            if content != old_content:
                cfg.seek(0)
                cfg.truncate(0)
                cfg.write(content)
                cfg.flush()
                if haproxy is None:
                    haproxy = await asyncio.create_subprocess_exec(
                        '/usr/sbin/haproxy', '-p', pidfile.name, '-f', cfg.name)
                else:
                    haproxy.send_signal(signal.SIGHUP)
                server_dict = {pretty(array_stream): "http://{}".format(server)
                               for array_stream, server in servers.items()}
                for _ws in app['websockets']:
                    await _ws.send_str(json.dumps(server_dict))
                logger.info('haproxy (re)started with servers %s', servers)
            else:
                logger.info('No change, not restarted haproxy')
            app['servers'] = servers
    finally:
        if client is not None:
            client.close()
            await client.wait_closed()
        if haproxy is not None:
            if haproxy.returncode is None:
                haproxy.terminate()
            ret = await haproxy.wait()
            if ret:
                logger.warning('haproxy exited with non-zero exit status %d', ret)
        cfg.close()
        pidfile.close()
        # Shut down aiohttp server
        # (https://docs.aiohttp.org/en/stable/web.html#aiohttp-web-graceful-shutdown)
        srv.close()
        await srv.wait_closed()
        await app.shutdown()
        await handler.shutdown(5.0)
        await app.cleanup()


if __name__ == '__main__':
    katsdpservices.setup_logging()
    loop = asyncio.get_event_loop()
    main_task = loop.create_task(main())
    try:
        loop.run_until_complete(main_task)
    except asyncio.CancelledError:
        pass
    finally:
        loop.close()
