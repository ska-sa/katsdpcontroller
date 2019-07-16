"""Code that is common to master and product controllers"""

import asyncio
import argparse
import logging
import functools
import json
import enum
from typing import List, Tuple, Callable, Union, Optional, AnyStr

import aiokatcp
from prometheus_client import Histogram

from . import scheduler


REQUEST_TIME = Histogram(
    'katsdpcontroller_request_time_seconds', 'Time to process katcp requests', ['request'],
    buckets=(0.001, 0.01, 0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0, 600.0))
logger = logging.getLogger(__name__)


def time_request(func: Callable) -> Callable:
    """Decorator to record request servicing time as a Prometheus histogram."""
    @functools.wraps(func)
    async def wrapper(self, ctx, *args, **kwargs):
        with REQUEST_TIME.labels(ctx.req.name).time():
            return await func(self, ctx, *args, **kwargs)
    return wrapper


def load_json_dict(text: AnyStr) -> dict:
    """Loads from JSON and checks that the result is a dict.

    Raises
    ------
    ValueError
        if `text` is not valid JSON or is valid JSON but not a dict
    """
    ans = json.loads(text)
    if not isinstance(ans, dict):
        raise ValueError('not a dict')
    return ans


def log_task_exceptions(task: asyncio.Future,
                        logger: Union[logging.Logger, logging.LoggerAdapter],
                        msg: str):
    """Add a done callback to a task that logs any exception it raised.

    Parameters
    ----------
    task
        Task (or any future) on which the callback will be added
    logger
        Logger to which the warning will be written
    msg
        Message that will be logged
    """
    def done_callback(task):
        if not task.cancelled():
            try:
                task.result()
            except Exception:
                logger.warning('%s', msg, exc_info=True)
    task.add_done_callback(done_callback)


def add_shared_options(parser: argparse.ArgumentParser) -> None:
    """Add command-line options that flow through master controller to the product controller."""
    # Keep these in sync with extract_shared_options
    parser.add_argument('--localhost', action='store_true',
                        help='Bind all ports to localhost (for security on dev systems)')
    parser.add_argument('-i', '--interface-mode', default=False,
                        action='store_true',
                        help='run the controller in interface only mode for testing '
                             'integration and ICD compliance. [%(default)s]')
    parser.add_argument('--image-override', action='append',
                        default=[], metavar='NAME:IMAGE',
                        help='Override an image name lookup [none]')
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


def extract_shared_options(args: argparse.Namespace) -> List[str]:
    """Turn the arguments provided by :func:`add_shared_options` into command-line arguments."""
    ret = [
        f'--realtime-role={args.realtime_role}',
        f'--batch-role={args.batch_role}',
        f'--principal={args.principal}',
        f'--user={args.user}'
    ]
    if args.localhost:
        ret.append('--localhost')
    if args.interface_mode:
        ret.append('--interface-mode')
    if args.write_graphs is not None:
        ret.append(f'--write-graphs={args.write_graphs}')
    for override in args.image_override:
        ret.append(f'--image-override={override}')
    return ret


def make_image_resolver_factory(lookup: scheduler.ImageLookup,
                                args: argparse.Namespace) -> scheduler.ImageResolverFactory:
    # The master controller only has --image-tag-file and the product
    # controller only has --image-tag, so we have to load them dynamically.
    tag: Optional[str] = vars(args).get('image_tag')
    tag_file: Optional[str] = vars(args).get('image_tag_file')
    factory = scheduler.ImageResolverFactory(lookup=lookup, tag=tag, tag_file=tag_file)
    for override in args.image_override:
        fields = override.split(':', 1)
        if len(fields) < 2:
            raise ValueError("--image-override option must have a colon")
        factory.override(fields[0], fields[1])
    return factory


class ProductState(scheduler.OrderedEnum):
    """State of a subarray.

    Only the following transitions can occur (TODO: make a picture):
    - CONFIGURING -> IDLE (via product-configure)
    - IDLE -> CAPTURING (via capture-init)
    - CAPTURING -> IDLE (via capture-done)
    - CONFIGURING/IDLE/CAPTURING/ERROR -> DECONFIGURING -> DEAD (via product-deconfigure)
    - IDLE/CAPTURING/DECONFIGURING -> ERROR (via an internal error)
    """
    CONFIGURING = 0
    IDLE = 1
    CAPTURING = 2
    DECONFIGURING = 3
    DEAD = 4
    ERROR = 5


class DeviceStatus(enum.Enum):
    OK = 1
    DEGRADED = 2
    FAIL = 3


def device_server_sockname(server: aiokatcp.DeviceServer) -> Tuple[str, int]:
    assert server.server and server.server.sockets, "Server is not yet started"
    return server.server.sockets[0].getsockname()[:2]
