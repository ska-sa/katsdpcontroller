#!/usr/bin/env python3

"""Script for launching the Science Data Processor Master Controller."""

import argparse
import asyncio
import logging
import signal
import functools
import socket

import katsdpservices

from katsdpcontroller import master_controller


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
    # TODO: most of the below get passed straight through, so move them to
    # common code.
    parser.add_argument('--no-pull', action='store_true', default=False,
                        help='Skip pulling images from the registry if already present')
    parser.add_argument('--write-graphs', metavar='DIR',
                        help='Write visualisations of the processing graph to directory')
    parser.add_argument('--realtime-role', default='realtime',
                        help='Mesos role for realtime capture tasks [%(default)s]')
    parser.add_argument('--batch-role', default='batch',
                        help='Mesos role for batch processing tasks [%(default)s]')
    parser.add_argument('--principal', default='katsdpcontroller',
                        help='Mesos principal for the framework [%(default)s]')
    parser.add_argument('--user', default='root',
                        help='User to run as on the Mesos agents [%(default)s]')
    katsdpservices.add_aiomonitor_arguments(parser)
    # TODO: support Zookeeper ensemble
    parser.add_argument('zk',
                        help='Endpoint for Zookeeper server e.g. server.domain:2181')
    parser.add_argument('singularity',
                        help='URL for Singularity server')
    args = parser.parse_args()
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

    server = master_controller.DeviceServer(args)
    loop = asyncio.get_event_loop()
    for sig in [signal.SIGINT, signal.SIGTERM]:
        loop.add_signal_handler(sig, functools.partial(handle_signal, server))
    with katsdpservices.start_aiomonitor(loop, args, locals()):
        loop.run_until_complete(async_main(server, args))
    loop.close()


if __name__ == '__main__':
    main()
