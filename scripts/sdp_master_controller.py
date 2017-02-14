#!/usr/bin/env python

"""Script for launching the Science Data Processor Master Controller.

   Copyright (c) 2013 SKA/KAT. All Rights Reserved.
"""

import sys
import os
import json
import signal
import tornado
import tornado.gen
import argparse
import logging
import logging.handlers
import addict
import trollius
from trollius import From
from tornado.platform.asyncio import AsyncIOMainLoop
import tornado.netutil
from prometheus_client import start_http_server
import pymesos
from katsdpcontroller import scheduler, sdpcontroller

try:
    import manhole
except ImportError:
    manhole = None

@trollius.coroutine
def on_shutdown(loop, server):
    loop.remove_signal_handler(signal.SIGINT)
    loop.remove_signal_handler(signal.SIGTERM)
     # in case the exit code below borks, we allow shutdown via traditional means
    yield From(server.async_stop())
    ioloop.stop()
    loop.stop()

if __name__ == "__main__":

    usage = "%(prog)s [options] master"
    parser = argparse.ArgumentParser(usage=usage)
    parser.add_argument('-a', '--host', dest='host', default="", metavar='HOST',
                        help='attach to server HOST (default=localhost)')
    parser.add_argument('-p', '--port', dest='port', type=int, default=5001, metavar='N',
                        help='katcp listen port (default=%(default)s)')
    parser.add_argument('-l', '--loglevel', dest='loglevel',
                        default="info", metavar='LOGLEVEL',
                        help='set the Python logging level (default=%(default)s)')
    parser.add_argument('-s', '--simulate', dest='simulate', default=False,
                        action='store_true',
                        help='run controller in simulation mode suitable for lab testing (default: %(default)s)')
    parser.add_argument('--no-prometheus', dest='prometheus', default=True,
                        action='store_false',
                        help='disable Prometheus client HTTP service')
    parser.add_argument('-i', '--interface_mode', dest='interface_mode', default=False,
                        action='store_true',
                        help='run the controller in interface only mode for testing integration and ICD compliance. (default: %(default)s)')
    parser.add_argument('--registry', dest='private_registry',
                        default='sdp-docker-registry.kat.ac.za:5000', metavar='HOST:PORT',
                        help='registry from which to pull images (use empty string to disable) (default: %(default)s)')
    parser.add_argument('--image-override', dest='image_override', action='append',
                        default=[], metavar='NAME:IMAGE',
                        help='Override an image name lookup (default: none)')
    parser.add_argument('--image-tag-file', dest='image_tag_file',
                        metavar='FILE', help='Load image tag to run from file (on each configure)')
    parser.add_argument('--safe-multicast-cidr', dest='safe_multicast_cidr', default='225.100.0.0/16',
                        metavar='MULTICAST-CIDR', help='Block of multicast addresses from which to draw internal allocation. Needs to be at least /16. (default: %(default)s)')
    parser.add_argument('--graph-override', dest='graph_override', action='append',
                        default=[], metavar='SUBARRAY_PRODUCT_ID:NEW_GRAPH',
                        help='Override the graph to be used for the specified subarray product id (default: none)')
    parser.add_argument('--gui-urls', metavar='FILE',
                        help='File containing JSON describing related GUIs (default: none)')
    parser.add_argument('--no-pull', action='store_true', default=False,
                        help='Skip pulling images from the registry if already present')
    parser.add_argument('--role', default='katsdpcontroller',
                        help='Mesos role for the framework (default: %(default)s)')
    parser.add_argument('--principal', default='katsdpcontroller',
                        help='Mesos principal for the principal (default: %(default)s')
    parser.add_argument('-v', '--verbose', dest='verbose', default=False,
                        action='store_true',
                        help='print verbose output (default: %(default)s)')
    parser.add_argument('master', help='Zookeeper URL for discovering Mesos master e.g. zk://server.domain:2181/mesos')
    opts = parser.parse_args()

    def die(msg=None):
        if msg:
            print msg
        else:
            parser.print_help()
        sys.exit(1)

    if len(logging.root.handlers) > 0: logging.root.removeHandler(logging.root.handlers[0])
    formatter = logging.Formatter("%(asctime)s.%(msecs)dZ - %(filename)s:%(lineno)s - %(levelname)s - %(message)s",
                                      datefmt="%Y-%m-%d %H:%M:%S")
    sh = logging.StreamHandler()
    sh.setFormatter(formatter)
    logging.root.addHandler(sh)
    if isinstance(opts.loglevel, basestring):
        opts.loglevel = getattr(logging, opts.loglevel.upper())
    logging.root.setLevel(opts.loglevel)
    logging.captureWarnings(True)
    logger = logging.getLogger('sdpcontroller')
    logger.info("Starting SDP Controller...")

    # Use an asynchronous resolver, so that DNS lookups for katcp connections
    # does not block the IOLoop.
    tornado.netutil.Resolver.configure('tornado.netutil.ThreadedResolver')

    image_resolver_factory = scheduler.ImageResolverFactory(
        private_registry=opts.private_registry or None,
        tag_file=opts.image_tag_file,
        use_digests=not opts.no_pull)
    for override in opts.image_override:
        fields = override.split(':', 1)
        if len(fields) < 2:
            die("--image-override option must have a colon")
        image_resolver_factory.override(fields[0], fields[1])

    for override in opts.graph_override:
        if len(override.split(':', 1)) < 2:
            die("--graph-override option must be in the form <subarray_product_id>:<graph_name>")
    graph_resolver = sdpcontroller.GraphResolver(overrides=opts.graph_override, simulate=opts.simulate)

    gui_urls = None
    if opts.gui_urls is not None:
        try:
            with open(opts.gui_urls) as gui_urls_file:
                gui_urls = json.load(gui_urls_file)
        except IOError as error:
            die('Cannot read {}: {}'.format(opts.gui_urls, error))
        except ValueError as error:
            die('Invalid JSON in {}: {}'.format(opts.gui_urls, error))

    framework_info = addict.Dict()
    framework_info.user = 'root'
    framework_info.name = 'katsdpcontroller'
    framework_info.checkpoint = True
    framework_info.principal = opts.principal
    framework_info.role = opts.role

    loop = trollius.get_event_loop()
    ioloop = AsyncIOMainLoop()
    ioloop.install()
    if opts.interface_mode:
        sched = None
    else:
        sched = scheduler.Scheduler(loop)
        driver = pymesos.MesosSchedulerDriver(
            sched, framework_info, opts.master, use_addict=True,
            implicit_acknowledgements=False)
        sched.set_driver(driver)
        driver.start()
    server = sdpcontroller.SDPControllerServer(
        opts.host, opts.port, sched, loop,
        simulate=opts.simulate,
        interface_mode=opts.interface_mode,
        image_resolver_factory=image_resolver_factory,
        graph_resolver=graph_resolver,
        safe_multicast_cidr=opts.safe_multicast_cidr,
        gui_urls=gui_urls)

    if manhole:
        manhole.install(oneshot_on='USR1', locals={'logger':logger, 'server':server, 'opts':opts})
         # allow remote debug connections and expose server and opts

    logger.info("Starting SDP...")

    if opts.prometheus:
        start_http_server(8081)
         # expose any prometheus metrics that we create

    for sig in [signal.SIGINT, signal.SIGTERM]:
        loop.add_signal_handler(sig, lambda: trollius.ensure_future(on_shutdown(loop, server)))
    ioloop.add_callback(server.start)
    ioloop.start()
