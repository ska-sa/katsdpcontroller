#!/usr/bin/env python3

"""
Run haproxy to reverse-proxy signal displays. To use it, run as

docker run -p <PORT>:8080 sdp-docker-registry.kat.ac.za:5000/katsdpcontroller haproxy_disp.py <sdpmchost>:5001

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

import async_timeout
import aiohttp
import aiohttp.web
import aiohttp_jinja2
import jinja2
import aiokatcp
import katsdpservices
from katsdptelstate.endpoint import endpoint_parser


logger = logging.getLogger(__name__)


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


async def get_servers(client):
    servers = defaultdict(Server)
    if client.is_connected:
        sensor_regex = r'^(array_\d+_[A-Za-z0-9]+)\.timeplot\.[^.]*\.html_port$'
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
            servers[array].html_endpoint = value
    return servers


@aiohttp_jinja2.template('index.html.j2')
def request_handler(request):
    servers = request.app['servers']
    return dict(servers=servers)


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('sdpmc', type=endpoint_parser(5001),
                        help='host[:port] of the SDP master controller')
    args = parser.parse_args()

    loop = asyncio.get_event_loop()
    wake = asyncio.Event()
    for signum in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(signum, terminate)

    app = aiohttp.web.Application()
    app['servers'] = {}
    app.router.add_get('/', request_handler)
    aiohttp_jinja2.setup(app, loader=jinja2.PackageLoader('katsdpcontroller'))
    handler = app.make_handler()
    srv = await loop.create_server(handler, '127.0.0.1', 0)
    port = srv.sockets[0].getsockname()[1]

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
            content = textwrap.dedent(r"""
                global
                    maxconn 256

                defaults
                    mode http
                    timeout connect 5s
                    timeout client 50s
                    timeout server 50s

                frontend http-in
                    bind *:8080
                    acl missing_slash path_reg '^/array_\d+_[a-zA-Z0-9]+$'
                    acl has_array path_reg '^/array_\d+_[a-zA-Z0-9]+/'
                    http-request redirect code 301 prefix / drop-query append-slash if missing_slash
                    http-request set-var(req.array) path,field(2,/) if has_array
                    use_backend %[var(req.array)] if has_array
                    default_backend fallback

                backend fallback
                    acl root path_reg '^/$'
                    http-request redirect code 301 location / drop-query if !root
                    server fallback_html_server 127.0.0.1:{port}
                """.format(port=port))
            for array, server in sorted(servers.items()):
                if not server.html_endpoint:
                    logger.warning('Array %s has no signal display html port', array)
                    continue
                content += textwrap.dedent(r"""
                    backend {array}
                        http-request set-path %[path,regsub(^/.*?/,/)]
                        server {array}_html_server {server.html_endpoint}
                    """.format(array=array, server=server))
            if content != old_content:
                cfg.seek(0)
                cfg.truncate(0)
                cfg.write(content)
                cfg.flush()
                if haproxy is None:
                    haproxy = await asyncio.create_subprocess_exec(
                        '/usr/sbin/haproxy-systemd-wrapper', '-p', pidfile.name, '-f', cfg.name)
                else:
                    haproxy.send_signal(signal.SIGHUP)
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
