#!/usr/bin/env python3

"""Script for launching the Science Data Processor Master Controller.

   Copyright (c) 2013 SKA/KAT. All Rights Reserved.
"""

import sys
import os
import os.path
import json
import signal
import argparse
import logging
import addict
import asyncio
from prometheus_client import start_http_server
import pymesos
import katsdpservices
from katsdpcontroller import scheduler, sdpcontroller

try:
    import manhole
except ImportError:
    manhole = None


def on_shutdown(loop, server):
    loop.remove_signal_handler(signal.SIGINT)
    loop.remove_signal_handler(signal.SIGTERM)
     # in case the exit code below borks, we allow shutdown via traditional means
    server.halt()


class InvalidGuiUrlsError(RuntimeError):
    pass


def load_gui_urls_file(filename):
    try:
        with open(filename) as gui_urls_file:
            gui_urls = json.load(gui_urls_file)
    except (IOError, OSError) as error:
        raise InvalidGuiUrlsError('Cannot read {}: {}'.format(filename, error))
    except ValueError as error:
        raise InvalidGuiUrlsError('Invalid JSON in {}: {}'.format(filename, error))
    if not isinstance(gui_urls, list):
        raise InvalidGuiUrlsError('{} does not contain a list'.format(filename))
    return gui_urls


def load_gui_urls_dir(dirname):
    try:
        gui_urls = []
        for name in sorted(os.listdir(dirname)):
            filename = os.path.join(dirname, name)
            if filename.endswith('.json') and os.path.isfile(filename):
                gui_urls.extend(load_gui_urls_file(filename))
    except (IOError, OSError) as error:
        raise InvalidGuiUrlsError('Cannot read {}: {}'.format(dirname, error))
    return gui_urls


async def run(loop, sched, server):
    if sched is not None:
        await sched.start()
    await server.start()
    for sig in [signal.SIGINT, signal.SIGTERM]:
        loop.add_signal_handler(sig, lambda: on_shutdown(loop, server))
    await server.join()


if __name__ == "__main__":
    katsdpservices.setup_logging()
    katsdpservices.setup_restart()

    usage = "%(prog)s [options] master"
    parser = argparse.ArgumentParser(usage=usage)
    parser.add_argument('-a', '--host', dest='host', default="", metavar='HOST',
                        help='attach to server HOST (default=localhost)')
    parser.add_argument('-p', '--port', dest='port', type=int, default=5001, metavar='N',
                        help='katcp listen port (default=%(default)s)')
    parser.add_argument('-l', '--loglevel', dest='loglevel',
                        default="info", metavar='LOGLEVEL',
                        help='set the Python logging level (default=%(default)s)')
    parser.add_argument('--http-port', type=int, default=8080, metavar='PORT',
                        help='port that slaves communicate with (default=%(default)s)')
    parser.add_argument('--http-url', type=str, metavar='URL',
                        help='URL at which slaves connect to the HTTP port (default=auto)')
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
    parser.add_argument('--gui-urls', metavar='FILE-OR-DIR',
                        help='File containing JSON describing related GUIs, or directory with .json files (default: none)')
    parser.add_argument('--no-pull', action='store_true', default=False,
                        help='Skip pulling images from the registry if already present')
    parser.add_argument('--write-graphs', metavar='DIR',
                        help='Write visualisations of the processing graph to directory')
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
            print(msg)
        else:
            parser.print_help()
        sys.exit(1)

    if opts.loglevel is not None:
        logging.root.setLevel(opts.loglevel.upper())
    logger = logging.getLogger('sdpcontroller')
    logger.info("Starting SDP Controller...")

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
    graph_resolver = sdpcontroller.GraphResolver(overrides=opts.graph_override)

    gui_urls = None
    if opts.gui_urls is not None:
        try:
            if os.path.isdir(opts.gui_urls):
                gui_urls = load_gui_urls_dir(opts.gui_urls)
            else:
                gui_urls = load_gui_urls_file(opts.gui_urls)
        except InvalidGuiUrlsError as error:
            die(str(error))
        except Exception as error:
            die('Could not read {}: {}'.format(opts.gui_urls, error))

    framework_info = addict.Dict()
    framework_info.user = 'root'
    framework_info.name = 'katsdpcontroller'
    framework_info.checkpoint = True
    framework_info.principal = opts.principal
    framework_info.role = opts.role

    loop = asyncio.get_event_loop()
    if opts.interface_mode:
        sched = None
    else:
        sched = scheduler.Scheduler(loop, opts.http_port, opts.http_url)
        driver = pymesos.MesosSchedulerDriver(
            sched, framework_info, opts.master, use_addict=True,
            implicit_acknowledgements=False)
        sched.set_driver(driver)
        driver.start()
    server = sdpcontroller.SDPControllerServer(
        opts.host, opts.port, sched, loop,
        interface_mode=opts.interface_mode,
        image_resolver_factory=image_resolver_factory,
        graph_resolver=graph_resolver,
        safe_multicast_cidr=opts.safe_multicast_cidr,
        gui_urls=gui_urls,
        graph_dir=opts.write_graphs)

    if manhole:
        manhole.install(oneshot_on='USR1', locals={'logger':logger, 'server':server, 'opts':opts})
         # allow remote debug connections and expose server and opts

    logger.info("Starting SDP...")

    if opts.prometheus:
        start_http_server(8081)
         # expose any prometheus metrics that we create

    loop.run_until_complete(run(loop, sched, server))
    loop.close()
