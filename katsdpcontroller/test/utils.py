"""Utilities for unit tests"""

import unittest
from typing import Any, Tuple

import aiokatcp
import asynctest


def create_patch(test_case: unittest.TestCase, *args, **kwargs) -> Any:
    """Wrap mock.patch such that it will be unpatched as part of test case cleanup"""
    patcher = asynctest.patch(*args, **kwargs)
    mock_obj = patcher.start()
    test_case.addCleanup(patcher.stop)
    return mock_obj


def device_server_sockname(server: aiokatcp.DeviceServer) -> Tuple[str, int]:
    assert server.server
    assert server.server.sockets
    return server.server.sockets[0].getsockname()[:2]
