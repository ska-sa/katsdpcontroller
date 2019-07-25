#!/usr/bin/env python3

"""Script for launching the Science Data Processor Master Controller."""

import argparse
import asyncio
import logging
import signal
import functools
import sys
from typing import List

import prometheus_async
import aiohttp.web
import katsdpservices

from katsdpcontroller import master_controller, web_utils


async def quiet_prometheus_stats(request: aiohttp.web.Request) -> aiohttp.web.Response:
    response = await prometheus_async.aio.web.server_stats(request)
    if response.status == 200:
        # Avoid spamming logs (feeds into web_utils.AccessLogger).
        response.log_level = logging.DEBUG
    return response


def handle_signal(server: master_controller.DeviceServer) -> None:
    # Disable the signal handlers, to avoid being unable to kill if there
    # is an exception in the shutdown path.
    loop = asyncio.get_event_loop()
    for sig in [signal.SIGINT, signal.SIGTERM]:
        loop.remove_signal_handler(sig)
    logging.info('Starting shutdown')
    server.halt()


async def setup_web(args: argparse.Namespace) -> aiohttp.web.AppRunner:
    app = aiohttp.web.Application()
    app.add_routes([aiohttp.web.get('/metrics', quiet_prometheus_stats)])
    runner = aiohttp.web.AppRunner(app, access_log_class=web_utils.AccessLogger)
    await runner.setup()
    site = aiohttp.web.TCPSite(runner, args.host, args.http_port)
    await site.start()
    return runner


async def async_main(server: master_controller.DeviceServer,
                     args: argparse.Namespace) -> None:
    runner = await setup_web(args)
    await server.start()
    await server.join()
    await runner.cleanup()
    logging.info('Server shut down')


def main(argv: List[str]) -> None:
    katsdpservices.setup_logging()
    katsdpservices.setup_restart()
    args = master_controller.parse_args(argv)
    if args.log_level is not None:
        logging.root.setLevel(args.log_level.upper())

    if args.interface_mode:
        logging.warning("Note: Running master controller in interface mode. "
                        "This allows testing of the interface only, "
                        "no actual command logic will be enacted.")

    loop = asyncio.get_event_loop()
    server = master_controller.DeviceServer(args)
    for sig in [signal.SIGINT, signal.SIGTERM]:
        loop.add_signal_handler(sig, functools.partial(handle_signal, server))
    with katsdpservices.start_aiomonitor(loop, args, locals()):
        loop.run_until_complete(async_main(server, args))
    loop.close()


if __name__ == '__main__':
    main(sys.argv[1:])
