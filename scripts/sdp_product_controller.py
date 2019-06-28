#!/usr/bin/env python3

"""Script for launching the Science Data Processor Subarray Product Controller.

   Copyright (c) 2013 SKA/KAT. All Rights Reserved.
"""

import os
import os.path
import json
import signal
import argparse
import logging
import asyncio
import socket
import urllib.parse

import addict
import prometheus_async
import pymesos
import aiomonitor
import katsdpservices

from katsdpcontroller import scheduler, sdpcontroller, web


async def quiet_prometheus_stats(request):
    response = await prometheus_async.aio.web.server_stats(request)
    if response.status == 200:
        # Avoid spamming logs (feeds into web.AccessLogger).
        response.log_level = logging.DEBUG
    return response


def on_shutdown(loop, server):
    # in case the exit code below borks, we allow shutdown via traditional means
    loop.remove_signal_handler(signal.SIGINT)
    loop.remove_signal_handler(signal.SIGTERM)
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


def init_dashboard(controller, opts):
    from katsdpcontroller.dashboard import Dashboard

    dashboard = Dashboard(controller)
    dashboard.start(opts.dashboard_port)


if __name__ == "__main__":
    katsdpservices.setup_logging()
    katsdpservices.setup_restart()

    usage = "%(prog)s [options] master"
    parser = argparse.ArgumentParser(usage=usage)
    parser.add_argument('-a', '--host', default="", metavar='HOST',
                        help='attach to server HOST [localhost]')
    parser.add_argument('-p', '--port', type=int, default=5001, metavar='N',
                        help='katcp listen port [%(default)s]')
    parser.add_argument('-l', '--loglevel',
                        default="info", metavar='LOGLEVEL',
                        help='set the Python logging level [%(default)s]')
    parser.add_argument('--external-hostname', metavar='FQDN', default=socket.getfqdn(),
                        help='Name by which others connect to this machine [%(default)s]')
    parser.add_argument('--http-port', type=int, default=8080, metavar='PORT',
                        help='port that slaves communicate with [%(default)s]')
    parser.add_argument('--http-url', type=str, metavar='URL',
                        help='URL at which slaves connect to the HTTP port')
    parser.add_argument('--dashboard-port', type=int, default=5006, metavar='PORT',
                        help='port for the Dash backend for the GUI [%(default)s]')
    parser.add_argument('--no-aiomonitor', dest='aiomonitor', default=True,
                        action='store_false',
                        help='disable aiomonitor debugging server')
    parser.add_argument('-i', '--interface-mode', default=False,
                        action='store_true',
                        help='run the controller in interface only mode for testing '
                             'integration and ICD compliance. [%(default)s]')
    parser.add_argument('--registry', dest='private_registry',
                        default='sdp-docker-registry.kat.ac.za:5000', metavar='HOST:PORT',
                        help='registry from which to pull images (use empty string to disable) '
                             '[%(default)s]')
    parser.add_argument('--image-override', action='append',
                        default=[], metavar='NAME:IMAGE',
                        help='Override an image name lookup [none]')
    parser.add_argument('--image-tag-file',
                        metavar='FILE', help='Load image tag to run from file (on each configure)')
    parser.add_argument('--s3-config-file',
                        metavar='FILE',
                        help='Configuration for connecting services to S3 '
                             '(loaded on each configure)')
    parser.add_argument('--safe-multicast-cidr', default='225.100.0.0/16',
                        metavar='MULTICAST-CIDR',
                        help='Block of multicast addresses from which to draw internal allocation. '
                             'Needs to be at least /16. [%(default)s]')
    parser.add_argument('--gui-urls', metavar='FILE-OR-DIR',
                        help='File containing JSON describing related GUIs, '
                             'or directory with .json files [none]')
    parser.add_argument('--no-pull', action='store_true', default=False,
                        help='Skip pulling images from the registry if already present')
    parser.add_argument('--write-graphs', metavar='DIR',
                        help='Write visualisations of the processing graph to directory')
    parser.add_argument('--realtime-role', default='realtime',
                        help='Mesos role for realtime capture tasks [%(default)s]')
    parser.add_argument('--batch-role', default='batch',
                        help='Mesos role for batch processing tasks [%(default)s]')
    parser.add_argument('--principal', default='katsdpcontroller',
                        help='Mesos principal for the principal [%(default)s]')
    parser.add_argument('--user', default='root',
                        help='User to run as on the Mesos agents [%(default)s]')
    parser.add_argument('master',
                        help='Zookeeper URL for discovering Mesos master '
                             'e.g. zk://server.domain:2181/mesos')
    opts = parser.parse_args()

    if opts.loglevel is not None:
        logging.root.setLevel(opts.loglevel.upper())
    logger = logging.getLogger('sdpcontroller')
    logger.info("Starting SDP Controller...")
    if opts.http_url is None:
        opts.http_url = 'http://{}:{}/'.format(urllib.parse.quote(opts.external_hostname),
                                               opts.http_port)
        logger.info('Setting --http-url to %s', opts.http_url)

    image_resolver_factory = scheduler.ImageResolverFactory(
        private_registry=opts.private_registry or None,
        tag_file=opts.image_tag_file,
        use_digests=not opts.no_pull)
    for override in opts.image_override:
        fields = override.split(':', 1)
        if len(fields) < 2:
            parser.error("--image-override option must have a colon")
        image_resolver_factory.override(fields[0], fields[1])

    gui_urls = None
    if opts.gui_urls is not None:
        try:
            if os.path.isdir(opts.gui_urls):
                gui_urls = load_gui_urls_dir(opts.gui_urls)
            else:
                gui_urls = load_gui_urls_file(opts.gui_urls)
        except InvalidGuiUrlsError as error:
            parser.error(str(error))
        except Exception as error:
            parser.error('Could not read {}: {}'.format(opts.gui_urls, error))

    if opts.s3_config_file is None and not opts.interface_mode:
        parser.error('--s3-config-file is required (unless --interface-mode is given)')

    framework_info = addict.Dict()
    framework_info.user = opts.user
    framework_info.name = 'katsdpcontroller'
    framework_info.checkpoint = True
    framework_info.principal = opts.principal
    framework_info.roles = [opts.realtime_role, opts.batch_role]
    framework_info.capabilities = [{'type': 'MULTI_ROLE'}]

    loop = asyncio.get_event_loop()
    if opts.interface_mode:
        sched = None
    else:
        sched = scheduler.Scheduler(loop, opts.realtime_role, opts.http_port, opts.http_url,
                                    dict(access_log_class=web.AccessLogger))
        sched.app.router.add_route('GET', '/metrics', quiet_prometheus_stats)
        driver = pymesos.MesosSchedulerDriver(
            sched, framework_info, opts.master, use_addict=True,
            implicit_acknowledgements=False)
        sched.set_driver(driver)
        driver.start()
    server = sdpcontroller.SDPControllerServer(
        opts.host, opts.port, sched, loop,
        batch_role=opts.batch_role,
        interface_mode=opts.interface_mode,
        image_resolver_factory=image_resolver_factory,
        s3_config_file=opts.s3_config_file,
        safe_multicast_cidr=opts.safe_multicast_cidr,
        gui_urls=gui_urls,
        graph_dir=opts.write_graphs)
    if not opts.interface_mode and opts.dashboard_port != 0:
        init_dashboard(server, opts)

    logger.info("Starting SDP...")

    if opts.aiomonitor:
        with aiomonitor.start_monitor(loop=loop):
            loop.run_until_complete(run(loop, sched, server))
    else:
        loop.run_until_complete(run(loop, sched, server))
    loop.close()
