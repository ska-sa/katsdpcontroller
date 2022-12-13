#!/usr/bin/env python3

"""Script for launching the Science Data Processor Master Controller."""

import argparse
import asyncio
import functools
import logging
import signal
import sys
from typing import Callable, List, Optional

import aiohttp.web
import aiokatcp
import katsdpservices

from katsdpcontroller import master_controller, web, web_utils


def handle_signal(server: master_controller.DeviceServer) -> None:
    # Disable the signal handlers, to avoid being unable to kill if there
    # is an exception in the shutdown path.
    loop = asyncio.get_event_loop()
    for sig in [signal.SIGINT, signal.SIGTERM]:
        loop.remove_signal_handler(sig)
    logging.info("Starting shutdown")
    server.halt()


async def setup_web(
    args: argparse.Namespace, server: master_controller.DeviceServer
) -> aiohttp.web.AppRunner:
    app = web.make_app(server, (args.host, args.http_port) if args.haproxy else None)
    runner = aiohttp.web.AppRunner(app, access_log_class=web_utils.AccessLogger)
    await runner.setup()
    if args.haproxy:
        site = aiohttp.web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        app["updater"].internal_port = runner.addresses[0][1]
    else:
        site = aiohttp.web.TCPSite(runner, args.host, args.http_port)
        await site.start()
    return runner


async def async_main(server: master_controller.DeviceServer, args: argparse.Namespace) -> None:
    runner = await setup_web(args, server)
    await server.start()
    await server.join()
    await runner.cleanup()
    logging.info("Server shut down")


def main(argv: List[str]) -> None:
    katsdpservices.setup_logging()
    katsdpservices.setup_restart()
    args = master_controller.parse_args(argv)
    if args.log_level is not None:
        logging.root.setLevel(args.log_level.upper())

    if args.interface_mode:
        logging.warning(
            "Note: Running master controller in interface mode. "
            "This allows testing of the interface only, "
            "no actual command logic will be enacted."
        )

    rewrite_gui_urls: Optional[Callable[[aiokatcp.Sensor], bytes]]
    if args.haproxy:
        rewrite_gui_urls = functools.partial(web.rewrite_gui_urls, args.external_url)
    else:
        rewrite_gui_urls = None

    loop = asyncio.get_event_loop()
    server = master_controller.DeviceServer(args, rewrite_gui_urls=rewrite_gui_urls)
    for sig in [signal.SIGINT, signal.SIGTERM]:
        loop.add_signal_handler(sig, functools.partial(handle_signal, server))
    with katsdpservices.start_aiomonitor(loop, args, locals()):
        loop.run_until_complete(async_main(server, args))
    loop.close()


if __name__ == "__main__":
    main(sys.argv[1:])
