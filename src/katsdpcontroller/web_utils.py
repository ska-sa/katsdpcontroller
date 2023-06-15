################################################################################
# Copyright (c) 2013-2023, National Research Foundation (SARAO)
#
# Licensed under the BSD 3-Clause License (the "License"); you may not use
# this file except in compliance with the License. You may obtain a copy
# of the License at
#
#   https://opensource.org/licenses/BSD-3-Clause
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

"""Utility code for interfacing with aiohttp"""

import logging

import aiohttp.web_log
import prometheus_async
from aiohttp import web


class _ReplaceLevel:
    """Wraps a logger to replace calls to .info with a chosen level."""

    def __init__(self, logger, level):
        self._logger = logger
        self._level = level

    def info(self, *args, **kwargs):
        self._logger.log(self._level, *args, **kwargs)

    def __getattr__(self, attr):
        return getattr(self._logger, attr)


async def prometheus_handler(request: web.Request) -> web.Response:
    response = await prometheus_async.aio.web.server_stats(request)
    if response.status == 200:
        # Avoid spamming logs (feeds into AccessLogger).
        response["log_level"] = logging.DEBUG
    return response


async def health_handler(request: web.Request) -> web.Response:
    response = web.Response(text="Health OK")
    response["log_level"] = logging.DEBUG  # avoid spamming logs
    return response


class AccessLogger(aiohttp.web_log.AccessLogger):
    """Access logger that with variable logging level.

    The request handler can assign to a ``log_level`` attribute of the
    response to specify the log level. If not set, it defaults to INFO.
    """

    def log(self, request, response, time):
        level = response.get("log_level", logging.INFO)
        # Callee calls self.logger.info. We temporarily swap out the logger
        # to override the log level. It's not thread-safe, but aiohttp runs
        # on the event loop anyway.
        orig_logger = self.logger
        try:
            self.logger = _ReplaceLevel(self.logger, level)
            super().log(request, response, time)
        finally:
            self.logger = orig_logger


@aiohttp.web.middleware
async def cache_control(request, handler):
    """Middleware that sets cache control headers.

    For resources in /static it allows caching for an hour. For everything
    else it disables caching.
    """
    if request.path.startswith("/static/"):

        def add_headers(obj):
            obj.headers["Cache-Control"] = "max-age=3600"

    else:

        def add_headers(obj):
            obj.headers["Cache-Control"] = "no-store"

    try:
        response = await handler(request)
        add_headers(response)
        return response
    except aiohttp.web.HTTPException as exc:
        add_headers(exc)
        raise
