"""Tests for the sdp controller module."""

import unittest2 as unittest

import os, time

from katcp.testutils import BlockingTestClient
from katcp import Message

from katsdpcontroller.sdpcontroller import SDPControllerServer

SA_STATES = {0:'unconfigured',1:'idle',2:'init_wait',3:'capturing',4:'capture_complete',5:'done'}

ANTENNAS = 'm063,m064'

TEST_ID = 'rts_wbc'

EXPECTED_SENSOR_LIST = [
    ('api-version', '', '', 'string'),
    ('build-state', '', '', 'string'),
    ('device-status', '', '', 'discrete', 'ok', 'degraded', 'fail'),
    ('fmeca.FD0001', '', '', 'boolean'),
    ('time-synchronised', '', '', 'boolean'),
]

EXPECTED_REQUEST_LIST = [
    'data-product-configure',
    'sdp-status',
    'capture-done',
    'capture-init',
    'capture-status',
    'postproc-init',
    'task-launch',
    'task-terminate'
]

class TestSDPController(unittest.TestCase):
    """Testing of the SDP controller.

    Note: The controller itself is only started once, so state remains between tests. For
    this reason unique subarray_ids are used by each test to try and avoid clashes.
    """
    def setUp(self):
        self.controller = SDPControllerServer('127.0.0.1',5000,True)
        self.controller.start()
        self.client = BlockingTestClient(self,'127.0.0.1',5000)
        self.client.start(timeout=1)
        self.client.wait_connected(timeout=1)

    def tearDown(self):
        self.client.stop()
        self.client.join()
        self.controller.stop()
        self.controller.join()

    def test_task_launch(self):
        self.client.assert_request_fails("task-launch","task1")
        self.client.assert_request_succeeds("task-launch","task1","/bin/sleep 5")
        reply, informs = self.client.blocking_request(Message.request("task-launch"))
        self.assertEqual(repr(reply),repr(Message.reply("task-launch","ok",1)))
        self.client.assert_request_succeeds("task-terminate","task1")

    def test_capture_init(self):
        self.client.assert_request_fails("capture-init",TEST_ID)
        self.client.assert_request_succeeds("data-product-configure",TEST_ID,ANTENNAS,"16384","2.1","0","127.0.0.1:9000","127.0.0.1:9001")
        self.client.assert_request_succeeds("capture-init",TEST_ID)

        reply, informs = self.client.blocking_request(Message.request("capture-status",TEST_ID))
        self.assertEqual(repr(reply),repr(Message.reply("capture-status","ok",SA_STATES[2])))

    def test_capture_done(self):
        self.client.assert_request_fails("capture-done",TEST_ID+'1')
        self.client.assert_request_succeeds("data-product-configure",TEST_ID+'1',ANTENNAS,"16384","2.1","0","127.0.0.1:9000","127.0.0.1:9001")
        self.client.assert_request_fails("capture-done",TEST_ID+'1')

        self.client.assert_request_succeeds("capture-init",TEST_ID+'1')
        self.client.assert_request_succeeds("capture-done",TEST_ID+'1')

    def test_deconfigure_data_product(self):
        self.client.assert_request_fails("data-product-configure",TEST_ID+'2',"")
        self.client.assert_request_succeeds("data-product-configure",TEST_ID+'2',ANTENNAS,"16384","2.1","0","127.0.0.1:9000","127.0.0.1:9001")
        self.client.assert_request_succeeds("capture-init",TEST_ID+'2')
        self.client.assert_request_fails("data-product-configure",TEST_ID+'2',"")
         # should not be able to deconfigure when not in idle state
        self.client.assert_request_succeeds("capture-done",TEST_ID+'2')
        self.client.assert_request_succeeds("data-product-configure",TEST_ID+'2',"")

    def test_configure_data_product(self):
        self.client.assert_request_fails("data-product-configure",TEST_ID+'3')
        self.client.assert_request_succeeds("data-product-configure")
        self.client.assert_request_succeeds("data-product-configure",TEST_ID+'3',ANTENNAS,"16384","2.1","0","127.0.0.1:9000","127.0.0.1:9001")
        self.client.assert_request_succeeds("data-product-configure",TEST_ID+'3')

        reply, informs = self.client.blocking_request(Message.request("data-product-configure"))
        self.assertEqual(repr(reply),repr(Message.reply("data-product-configure","ok",1)))

        self.client.assert_request_succeeds("data-product-configure",TEST_ID+'3',"")
        self.client.assert_request_fails("data-product-configure",TEST_ID+'3')

    def test_sensor_list(self):
        self.client.test_sensor_list(EXPECTED_SENSOR_LIST,ignore_descriptions=True)

    def test_help(self):
        self.client.test_help(EXPECTED_REQUEST_LIST)

