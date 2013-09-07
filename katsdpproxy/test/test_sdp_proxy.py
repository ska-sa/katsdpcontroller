"""Tests for the sdp proxy module."""

import unittest2 as unittest

import os, time

from katcore.testutils import SimulatorTestMixin
from katcp import Message

from katsdpproxy.sdpproxy import SDPProxyServer

EXPECTED_SENSOR_LIST = [
    ('api-version', '', '', 'string'),
    ('build-state', '', '', 'string'),
]

EXPECTED_REQUEST_LIST = [
    'subarray-configure','sdp-status',
]

class TestSDPProxy(unittest.TestCase, SimulatorTestMixin):
    def setUp(self):
        self.proxy = SDPProxyServer('127.0.0.1',5000)
        self.proxy.start()
        time.sleep(0.5) # I know,I know, stop complaining...
        self.client = self.add_client(('127.0.0.1',5000))

    def tearDown(self):
        self.client.stop()
        self.proxy.stop()
        self.proxy.join()

    def test_configure_subarray(self):
        self.client.assert_request_fails("subarray-configure","1")
        self.client.assert_request_succeeds("subarray-configure")
        self.client.assert_request_succeeds("subarray-configure","1","64","16384","2.1","0","127.0.0.1:9000")
        self.client.assert_request_succeeds("subarray-configure","1")

        reply, informs = self.client.blocking_request(Message.request("subarray-configure"))
        self.assertEqual(repr(reply),repr(Message.reply("subarray-configure","ok",1)))

        self.client.assert_request_succeeds("subarray-configure","1","0")
        self.client.assert_request_fails("subarray-configure","1")

    def test_sensor_list(self):
        self.client.test_sensor_list(EXPECTED_SENSOR_LIST,ignore_descriptions=True)

    def test_help(self):
        self.client.test_help(EXPECTED_REQUEST_LIST)

