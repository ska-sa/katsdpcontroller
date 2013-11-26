"""Tests for the sdp proxy module."""

import unittest2 as unittest

import os, time

from katcp.testutils import BlockingTestClient
from katcp import Message

from katsdpproxy.sdpproxy import SDPProxyServer

SA_STATES = {0:'unconfigured',1:'idle',2:'init_wait',3:'capturing',4:'capture_complete',5:'done'}

ANTENNAS = 'm063,m064'

TEST_ID = "sub32_c16n400m1k"

EXPECTED_SENSOR_LIST = [
    ('api-version', '', '', 'string'),
    ('build-state', '', '', 'string'),
]

EXPECTED_REQUEST_LIST = [
    'data-product-configure',
    'sdp-status',
    'capture-done',
    'capture-init',
    'capture-status',
    'postproc-init',
]

class TestSDPProxy(unittest.TestCase):
    """Testing of the SDP proxy.

    Note: The proxy itself is only started once, so state remains between tests. For
    this reason unique subarray_ids are used by each test to try and avoid clashes.
    """
    def setUp(self):
        self.proxy = SDPProxyServer('127.0.0.1',5000)
        self.proxy.start()
        self.client = BlockingTestClient(self,'127.0.0.1',5000)
        self.client.start(timeout=1)
        self.client.wait_connected(timeout=1)

    def tearDown(self):
        self.client.stop()
        self.client.join()
        self.proxy.stop()
        self.proxy.join()

    def test_capture_init(self):
        self.client.assert_request_fails("capture-init",TEST_ID)
        self.client.assert_request_succeeds("data-product-configure",TEST_ID,ANTENNAS,"16384","2.1","0","127.0.0.1:9000")
        self.client.assert_request_succeeds("capture-init",TEST_ID)

        reply, informs = self.client.blocking_request(Message.request("capture-status",TEST_ID))
        self.assertEqual(repr(reply),repr(Message.reply("capture-status","ok",SA_STATES[2])))

    def test_capture_done(self):
        self.client.assert_request_fails("capture-done",TEST_ID)
        self.client.assert_request_succeeds("data-product-configure",TEST_ID,ANTENNAS,"16384","2.1","0","127.0.0.1:9000")
        self.client.assert_request_fails("capture-done",TEST_ID)

        self.client.assert_request_succeeds("capture-init",TEST_ID)
        self.client.assert_request_succeeds("capture-done",TEST_ID)

    def test_configure_data_product(self):
        self.client.assert_request_fails("data-product-configure","1")
        self.client.assert_request_succeeds("data-product-configure")
        self.client.assert_request_succeeds("data-product-configure","1",ANTENNAS,"16384","2.1","0","127.0.0.1:9000")
        self.client.assert_request_succeeds("data-product-configure","1")

        reply, informs = self.client.blocking_request(Message.request("data-product-configure"))
        self.assertEqual(repr(reply),repr(Message.reply("data-product-configure","ok",1)))

        self.client.assert_request_succeeds("data-product-configure","1","")
        self.client.assert_request_fails("data-product-configure","1")

    def test_sensor_list(self):
        self.client.test_sensor_list(EXPECTED_SENSOR_LIST,ignore_descriptions=True)

    def test_help(self):
        self.client.test_help(EXPECTED_REQUEST_LIST)

