"""Utility code for interfacing with aiohttp"""

import logging
import re

import aiohttp.helpers


class _ReplaceLevel(object):
    """Wraps a logger to replace calls to .info with a chosen level."""
    def __init__(self, logger, level):
        self._logger = logger
        self._level = level

    def info(self, *args, **kwargs):
        self._logger.log(self._level, *args, **kwargs)

    def __getattr__(self, attr):
        return getattr(self._logger, attr)


class AccessLogger(aiohttp.helpers.AccessLogger):
    """Access logger that with variable logging level.

    The request handler can assign to a ``log_level`` attribute of the
    response to specify the log level. If not set, it defaults to INFO.
    """
    def log(self, request, response, time):
        level = getattr(response, 'log_level', logging.INFO)
        # Callee calls self.logger.info. We temporarily swap out the logger
        # to override the log level. It's not thread-safe, but aiohttp runs
        # on the event loop anyway.
        orig_logger = self.logger
        try:
            self.logger = _ReplaceLevel(self.logger, level)
            super().log(request, response, time)
        finally:
            self.logger = orig_logger
