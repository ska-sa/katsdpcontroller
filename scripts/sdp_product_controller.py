#!/usr/bin/env python3

"""Script for launching the Science Data Processor Subarray Product Controller.

   Copyright (c) 2013 SKA/KAT. All Rights Reserved.
"""

import os
import os.path
import signal
import argparse
import logging
import asyncio
import socket
import json
import urllib.parse
from typing import Tuple, Optional

import addict
import jsonschema
import yarl
import pymesos
import aiokatcp
import aiomonitor
import katsdpservices
from katsdptelstate.endpoint import endpoint_parser

from katsdpcontroller import scheduler, schemas, product_controller, web_utils
from katsdpcontroller.controller import (
    add_shared_options, load_json_dict, make_image_resolver_factory)


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


def init_dashboard(controller, opts, dashboard_path):
    from katsdpcontroller.dashboard import Dashboard

    dashboard = Dashboard(controller, routes_pathname_prefix=dashboard_path, update_title=None)
    dashboard.start(opts.host, opts.dashboard_port)


def parse_s3_config(value: str) -> dict:
    try:
        s3_config = load_json_dict(value)
        schemas.S3_CONFIG.validate(s3_config)   # type: ignore
    except jsonschema.ValidationError as exc:
        raise ValueError(str(exc))
    return s3_config


def parse_args() -> Tuple[argparse.ArgumentParser, argparse.Namespace]:
    usage = "%(prog)s [options] master_controller mesos_master"
    parser = argparse.ArgumentParser(usage=usage)
    if 'TASK_HOST' in os.environ:
        # Set by Singularity
        default_external_hostname = os.environ['TASK_HOST']
    else:
        default_external_hostname = socket.getfqdn()
    parser.add_argument('-a', '--host', default="", metavar='HOST',
                        help='attach to server HOST [localhost]')
    parser.add_argument('-p', '--port', type=int, default=os.environ.get('PORT0', '5101'),
                        help='katcp listen port [%(default)s]')
    parser.add_argument('-l', '--log-level', metavar='LEVEL',
                        help='set the Python logging level [%(default)s]')
    parser.add_argument('--external-hostname', metavar='FQDN', default=default_external_hostname,
                        help='Name by which others connect to this machine [%(default)s]')
    parser.add_argument('--http-port', type=int, default=os.environ.get('PORT1', '5102'),
                        metavar='PORT',
                        help='port that slaves communicate with [%(default)s]')
    parser.add_argument('--http-url', type=str, metavar='URL',
                        help='URL at which slaves connect to the HTTP port')
    parser.add_argument('--dashboard-port', type=int, default=os.environ.get('PORT4', 5006),
                        metavar='PORT',
                        help='port for the Dash backend for the GUI [%(default)s]')
    parser.add_argument('--dashboard-url', type=str, metavar='URL',
                        help='External URL for the dashboard')
    parser.add_argument('--subarray-product-id', default='UNKNOWN',
                        help='Subarray product ID, used to name the framework [%(default)s]')
    parser.add_argument('--image-tag',
                        metavar='TAG', help='Image tag to use')
    parser.add_argument('--s3-config', type=parse_s3_config, metavar='JSON',
                        help='Configuration for connecting services to S3')
    parser.add_argument('master_controller', type=endpoint_parser(None),
                        help='Master controller katcp endpoint')
    parser.add_argument('mesos_master',
                        help='Zookeeper URL for discovering Mesos master '
                             'e.g. zk://server.domain:2181/mesos')
    add_shared_options(parser)
    katsdpservices.add_aiomonitor_arguments(parser)
    parser.set_defaults(
        aiomonitor_port=os.environ.get('PORT2', aiomonitor.MONITOR_PORT),
        aioconsole_port=os.environ.get('PORT3', aiomonitor.CONSOLE_PORT))
    args = parser.parse_args()

    if args.localhost:
        args.host = '127.0.0.1'
        args.external_hostname = '127.0.0.1'

    if args.s3_config is None and not args.interface_mode:
        parser.error('--s3-config is required (unless --interface-mode is given)')

    if args.http_url is None:
        # When Singularity creates the port mapping, it puts the host ports
        # in PORT0 (katcp) and PORT1 (http).
        http_port = os.environ.get('PORT1', args.http_port)
        args.http_url = 'http://{}:{}/'.format(urllib.parse.quote(args.external_hostname),
                                               http_port)

    return parser, args


def prepare_env(args: argparse.Namespace) -> None:
    """Update os.environ with logging settings extracted from arguments"""
    if os.environ.get('KATSDP_LOG_GELF_ADDRESS'):
        extra = load_json_dict(os.environ.get('KATSDP_LOG_GELF_EXTRA', '{}'))
        extra['subarray_product_id'] = args.subarray_product_id
        os.environ['KATSDP_LOG_GELF_EXTRA'] = json.dumps(extra)
        if not os.environ.get('KATSDP_LOG_GELF_LOCALNAME'):
            os.environ['KATSDP_LOG_GELF_LOCALNAME'] = args.external_hostname


def main() -> None:
    parser, args = parse_args()
    prepare_env(args)
    katsdpservices.setup_logging()
    katsdpservices.setup_restart()
    if args.log_level is not None:
        logging.root.setLevel(args.log_level.upper())

    logger = logging.getLogger('katsdpcontroller')
    logger.info("Starting SDP product controller...")
    logger.info('katcp: %s:%d', args.host, args.port)
    logger.info('http: %s', args.http_url)

    master_controller = aiokatcp.Client(args.master_controller.host, args.master_controller.port)
    image_lookup = product_controller.KatcpImageLookup(master_controller)
    try:
        image_resolver_factory = make_image_resolver_factory(image_lookup, args)
    except ValueError as exc:
        parser.error(str(exc))

    framework_info = addict.Dict()
    framework_info.user = args.user
    framework_info.name = args.subarray_product_id
    framework_info.checkpoint = True
    framework_info.principal = args.principal
    framework_info.roles = [args.realtime_role, args.batch_role]
    framework_info.capabilities = [{'type': 'MULTI_ROLE'}, {'type': 'TASK_KILLING_STATE'}]

    loop = asyncio.get_event_loop()
    if args.interface_mode:
        sched = None
    else:
        sched = scheduler.Scheduler(args.realtime_role, args.host, args.http_port, args.http_url,
                                    task_stats=product_controller.TaskStats(),
                                    runner_kwargs=dict(access_log_class=web_utils.AccessLogger))
        sched.app.router.add_get('/metrics', web_utils.prometheus_handler)
        sched.app.router.add_get('/health', web_utils.health_handler)
        driver = pymesos.MesosSchedulerDriver(
            sched, framework_info, args.mesos_master, use_addict=True,
            implicit_acknowledgements=False)
        sched.set_driver(driver)
        driver.start()

    dashboard_path = f'/gui/{args.subarray_product_id}/product/dashboard/'
    dashboard_url: Optional[str] = args.dashboard_url
    if not args.interface_mode and args.dashboard_port != 0 and dashboard_url is None:
        dashboard_url = str(yarl.URL.build(scheme='http', host=args.external_hostname,
                                           port=args.dashboard_port, path=dashboard_path))

    server = product_controller.DeviceServer(
        args.host, args.port, master_controller, args.subarray_product_id, sched,
        batch_role=args.batch_role,
        interface_mode=args.interface_mode,
        localhost=args.localhost,
        image_resolver_factory=image_resolver_factory,
        s3_config=args.s3_config if args.s3_config is not None else {},
        graph_dir=args.write_graphs,
        dashboard_url=dashboard_url)
    if not args.interface_mode and args.dashboard_port != 0:
        init_dashboard(server, args, dashboard_path)

    with katsdpservices.start_aiomonitor(loop, args, locals()):
        loop.run_until_complete(run(sched, server))
    loop.close()


if __name__ == "__main__":
    main()
