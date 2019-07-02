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
from katsdptelstate.endpoint import endpoint_parser

from katsdpcontroller import scheduler, product_controller, web
from katsdpcontroller.controller import add_shared_options


# TODO: move Prometheus stats to master
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


async def run(sched, server):
    if sched is not None:
        await sched.start()
    await server.start()
    loop = asyncio.get_event_loop()
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
    if 'TASK_HOST' in os.environ:
        # Set by Singularity
        default_external_hostname = os.environ['TASK_HOST']
    else:
        default_external_hostname = socket.getfqdn()
    parser.add_argument('-a', '--host', default="", metavar='HOST',
                        help='attach to server HOST [localhost]')
    parser.add_argument('-p', '--port', type=int, default=5101, metavar='N',
                        help='katcp listen port [%(default)s]')
    parser.add_argument('-l', '--log-level', metavar='LEVEL',
                        help='set the Python logging level [%(default)s]')
    parser.add_argument('--external-hostname', metavar='FQDN', default=default_external_hostname,
                        help='Name by which others connect to this machine [%(default)s]')
    parser.add_argument('--http-port', type=int, default=5102, metavar='PORT',
                        help='port that slaves communicate with [%(default)s]')
    parser.add_argument('--http-url', type=str, metavar='URL',
                        help='URL at which slaves connect to the HTTP port')
    parser.add_argument('--dashboard-port', type=int, default=5006, metavar='PORT',
                        help='port for the Dash backend for the GUI [%(default)s]')
    parser.add_argument('--no-aiomonitor', dest='aiomonitor', default=True,
                        action='store_false',
                        help='disable aiomonitor debugging server')
    parser.add_argument('--image-tag',
                        metavar='TAG', help='Image tag to use')
    parser.add_argument('--s3-config',
                        metavar='JSON',
                        help='Configuration for connecting services to S3')
    parser.add_argument('master_controller', type=endpoint_parser(None),
                        help='Master controller katcp endpoint')
    parser.add_argument('mesos_master',
                        help='Zookeeper URL for discovering Mesos master '
                             'e.g. zk://server.domain:2181/mesos')
    add_shared_options(parser)
    args = parser.parse_args()

    if args.log_level is not None:
        logging.root.setLevel(args.log_level.upper())
    logger = logging.getLogger('sdpcontroller')
    logger.info("Starting SDP product controller...")
    if args.http_url is None:
        # When Singularity creates the port mapping, it puts the host ports
        # in PORT0 (katcp) and PORT1 (http).
        http_port = os.environ.get('PORT1', args.http_port)
        args.http_url = 'http://{}:{}/'.format(urllib.parse.quote(args.external_hostname),
                                               http_port)
        logger.info('Setting --http-url to %s', args.http_url)

    image_lookup: scheduler.ImageLookup
    if args.no_pull:
        image_lookup = scheduler.SimpleImageLookup(args.registry)
    else:
        image_lookup = scheduler.HTTPImageLookup(args.registry)
    image_resolver_factory = scheduler.ImageResolverFactory(lookup=image_lookup, tag=args.image_tag)
    for override in args.image_override:
        fields = override.split(':', 1)
        if len(fields) < 2:
            parser.error("--image-override option must have a colon")
        image_resolver_factory.override(fields[0], fields[1])

    if args.s3_config is None and not args.interface_mode:
        parser.error('--s3-config is required (unless --interface-mode is given)')

    framework_info = addict.Dict()
    framework_info.user = args.user
    framework_info.name = 'katsdpcontroller'
    framework_info.checkpoint = True
    framework_info.principal = args.principal
    framework_info.roles = [args.realtime_role, args.batch_role]
    framework_info.capabilities = [{'type': 'MULTI_ROLE'}]

    loop = asyncio.get_event_loop()
    if args.interface_mode:
        sched = None
    else:
        sched = scheduler.Scheduler(args.realtime_role, args.http_port, args.http_url,
                                    dict(access_log_class=web.AccessLogger))
        sched.app.router.add_route('GET', '/metrics', quiet_prometheus_stats)
        driver = pymesos.MesosSchedulerDriver(
            sched, framework_info, args.mesos_master, use_addict=True,
            implicit_acknowledgements=False)
        sched.set_driver(driver)
        driver.start()
    server = product_controller.DeviceServer(
        args.host, args.port,
        args.master_controller.host, args.master_controller.port,
        sched,
        batch_role=args.batch_role,
        interface_mode=args.interface_mode,
        image_resolver_factory=image_resolver_factory,
        s3_config=json.loads(args.s3_config),   # TODO: validate
        graph_dir=args.write_graphs)
    if not args.interface_mode and args.dashboard_port != 0:
        init_dashboard(server, args)

    logger.info("Starting SDP...")

    if args.aiomonitor:
        with aiomonitor.start_monitor(loop=loop):
            loop.run_until_complete(run(sched, server))
    else:
        loop.run_until_complete(run(sched, server))
    loop.close()
