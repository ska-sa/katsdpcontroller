"""Code that is common to master and product controllers"""

import asyncio
import logging
import functools
import json
import enum
from typing import Callable, Union, AnyStr

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
