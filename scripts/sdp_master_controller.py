#!/usr/bin/env python3

"""Script for launching the Science Data Processor Master Controller."""

import argparse
import asyncio
import logging
import signal
import functools
import socket
import os

import katsdpservices

from katsdpcontroller import master_controller
from katsdpcontroller.controller import add_shared_options


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


def parse_args() -> argparse.Namespace:
    usage = "%(prog)s [options] zk singularity"
    parser = argparse.ArgumentParser(usage=usage)
    parser.add_argument('-a', '--host', default="", metavar='HOST',
                        help='attach to server HOST [localhost]')
    parser.add_argument('-p', '--port', type=int, default=5001, metavar='N',
                        help='katcp listen port [%(default)s]')
    parser.add_argument('-l', '--log-level', metavar='LEVEL',
                        help='set the Python logging level [%(default)s]')
    parser.add_argument('--name', default='sdpmc',
                        help='name to use in Zookeeper and Singularity [%(default)s]')
    parser.add_argument('--external-hostname', metavar='FQDN', default=socket.getfqdn(),
                        help='Name by which others connect to this machine [%(default)s]')
    parser.add_argument('--dashboard-port', type=int, default=5006, metavar='PORT',
                        help='port for the Dash backend for the GUI [%(default)s]')
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
    add_shared_options(parser)
    katsdpservices.add_aiomonitor_arguments(parser)
    # TODO: support Zookeeper ensemble
    parser.add_argument('zk',
                        help='Endpoint for Zookeeper server e.g. server.domain:2181')
    parser.add_argument('singularity',
                        help='URL for Singularity server')
    args = parser.parse_args()

    if args.gui_urls is not None:
        try:
            if os.path.isdir(args.gui_urls):
                args.gui_urls = load_gui_urls_dir(args.gui_urls)
            else:
                args.gui_urls = load_gui_urls_file(args.gui_urls)
        except InvalidGuiUrlsError as exc:
            parser.error(str(exc))
        except Exception as exc:
            parser.error(f'Could not read {args.gui_urls}: {exc}')

    if args.s3_config_file is None and not args.interface_mode:
        parser.error('--s3-config-file is required (unless --interface-mode is given)')

    return args


def handle_signal(server: master_controller.DeviceServer) -> None:
    # Disable the signal handlers, to avoid being unable to kill if there
    # is an exception in the shutdown path.
    loop = asyncio.get_event_loop()
    for sig in [signal.SIGINT, signal.SIGTERM]:
        loop.remove_signal_handler(sig)
    logging.info('Starting shutdown')
    server.halt()


async def async_main(server: master_controller.DeviceServer, args: argparse.Namespace) -> None:
    await server.start()
    await server.join()
    logging.info('Server shut down')


def main() -> None:
    katsdpservices.setup_logging()
    katsdpservices.setup_restart()
    args = parse_args()
    if args.log_level is not None:
        logging.root.setLevel(args.log_level.upper())

    if args.interface_mode:
        logging.warning("Note: Running master controller in interface mode. "
                        "This allows testing of the interface only, "
                        "no actual command logic will be enacted.")

    server = master_controller.DeviceServer(args)
    loop = asyncio.get_event_loop()
    for sig in [signal.SIGINT, signal.SIGTERM]:
        loop.add_signal_handler(sig, functools.partial(handle_signal, server))
    with katsdpservices.start_aiomonitor(loop, args, locals()):
        loop.run_until_complete(async_main(server, args))
    loop.close()


if __name__ == '__main__':
    main()
