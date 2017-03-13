#!/usr/bin/env python

"""
Run haproxy to reverse-proxy signal displays. To use it, run as

docker run -p <PORT>:8080 sdp-docker-registry.kat.ac.za:5000/katsdpcontroller haproxy_disp.py <sdpmchost>:5001

Then connect to the machine on http://<HOST>:<PORT>/<subarray_product> to get the signal
displays from that subarray-product. If they aren't running, haproxy will return a 503 error.
"""

from __future__ import print_function, division, absolute_import
import re
import argparse
import signal
import logging
import tempfile
import textwrap
from collections import defaultdict
import tornado.ioloop
import tornado.gen
import tornado.process
import tornado.locks
import katcp
from katsdptelstate.endpoint import endpoint_parser


logger = logging.getLogger(__name__)


class Client(katcp.AsyncClient):
    def __init__(self, wake, *args, **kwargs):
        self.wake = wake
        super(Client, self).__init__(*args, **kwargs)

    def notify_connected(self, connected):
        logger.debug('Waking open on %s', 'connect' if connected else 'disconnect')
        self.wake.set()

    def inform_interface_changed(self, msg):
        """Handle the interface-changed inform"""
        logger.debug('Waking up on interface-changed')
        self.wake.set()


def terminate(wake, die):
    signal.signal(signal.SIGINT, signal.SIG_DFL)
    signal.signal(signal.SIGTERM, signal.SIG_DFL)
    die.set_result(None)
    wake.set()


class Server(object):
    def __init__(self):
        self.endpoints = {}   # Indexed with 'html' and 'data'


@tornado.gen.coroutine
def get_servers(client):
    servers = defaultdict(Server)
    if client.is_connected():
        reply, informs = yield client.future_request(katcp.Message.request(
            'sensor-value', r'/^array_\d+_[A-Za-z0-9]+\.sdp\.timeplot\.1\.(?:html|data)_port$/'))
        for inform in informs:
            if len(inform.arguments) != 5:
                logger.warning('#sensor-value inform has wrong number of arguments, ignoring')
                continue
            name = inform.arguments[2]
            status = inform.arguments[3]
            value = inform.arguments[4]
            if status != 'nominal':
                logger.warning('sensor %s is in state %s, ignoring', name, status)
                continue
            match = re.match(r'^(array_\d+_[A-Za-z0-9]+)\.sdp\.timeplot\.1\.(html|data)_port$', name)
            if not match:
                logger.warning('sensor %s does not match the requested regex, ignoring', name)
                continue
            array = match.group(1)
            servers[array].endpoints[match.group(2)] = value
    raise tornado.gen.Return(servers)


@tornado.gen.coroutine
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('sdpmc', type=endpoint_parser(5001),
                        help='host[:port] of the SDP master controller')
    args = parser.parse_args()

    ioloop = tornado.ioloop.IOLoop.current()
    wake = tornado.locks.Event()
    # Future set when the process is interrupted.
    die = tornado.gen.Future()
    for signum in (signal.SIGINT, signal.SIGTERM):
        signal.signal(signum, lambda signum_, frame:
            ioloop.add_callback_from_signal(terminate, wake, die))

    cfg = tempfile.NamedTemporaryFile(mode='w+', suffix='.cfg')
    pidfile = tempfile.NamedTemporaryFile(suffix='.pid')
    client = None
    haproxy = None
    content = None
    try:
        client = Client(wake, args.sdpmc.host, args.sdpmc.port)
        client.set_ioloop(ioloop)
        client.start()
        while True:
            yield wake.wait()
            wake.clear()
            if die.done():
                break
            logger.info('Updating state')
            servers = yield get_servers(client)
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
                    acl has_ws path_reg '^/array_\d+_[a-zA-Z0-9]+/data$'
                    http-request redirect code 301 prefix / drop-query append-slash if missing_slash
                    http-request set-var(req.array) path,field(2,/) if has_array
                    use_backend %[var(req.array)]_html if has_array !has_ws
                    use_backend %[var(req.array)]_data if has_ws
                """)
            for array, server in sorted(servers.items()):
                if 'html' not in server.endpoints:
                    logger.warn('Array %s has no signal display html port', array)
                    continue
                if 'data' not in server.endpoints:
                    logger.warn('Array %s has no signal display data port', array)
                    continue
                content += textwrap.dedent(r"""
                    backend {array}_html
                        http-request set-path %[path,regsub(^/.*?/,/)]
                        http-request set-header X-Timeplot-Data-Address ws://%[req.hdr(Host)]/%[var(req.array)]/data
                        server {array}_html_server {server.endpoints[html]}

                    backend {array}_data
                        http-request set-path %[path,regsub(^/.*/data$,/)]
                        server {array}_data_server {server.endpoints[data]}
                    """.format(array=array, server=server))
            if content != old_content:
                cfg.seek(0)
                cfg.truncate(0)
                cfg.write(content)
                cfg.flush()
                if haproxy is None:
                    haproxy = tornado.process.Subprocess(
                        ['/usr/sbin/haproxy-systemd-wrapper', '-p', pidfile.name,
                         '-f', cfg.name])
                else:
                    haproxy.proc.send_signal(signal.SIGHUP)
                logger.info('haproxy (re)started with servers %s', servers)
            else:
                logger.info('No change, not restarted haproxy')
    finally:
        if client is not None:
            client.stop()
        if haproxy is not None:
            haproxy.proc.send_signal(signal.SIGTERM)
            ret = yield haproxy.wait_for_exit(raise_error=False)
            if ret:
                logger.warning('haproxy exited with non-zero exit status %d', ret)
        cfg.close()
        pidfile.close()


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    ioloop = tornado.ioloop.IOLoop.current()
    ioloop.run_sync(main)
