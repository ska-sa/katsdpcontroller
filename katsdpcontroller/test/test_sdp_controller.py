"""Tests for the sdp controller module."""

import threading
import concurrent.futures
import unittest2 as unittest

from katcp.testutils import BlockingTestClient
from katcp import Message
from tornado.platform.asyncio import AsyncIOLoop

from katsdpcontroller.sdpcontroller import SDPControllerServer, SDPCommonResources, SDPResources

ANTENNAS = 'm063,m064'

PRODUCT = 'c856M4k'
SUBARRAY_PRODUCT1 = 'array_1_' + PRODUCT
SUBARRAY_PRODUCT2 = 'array_2_' + PRODUCT
SUBARRAY_PRODUCT3 = 'array_3_' + PRODUCT
SUBARRAY_PRODUCT4 = 'array_4_' + PRODUCT

EXPECTED_SENSOR_LIST = [
    ('api-version', '', '', 'string'),
    ('build-state', '', '', 'string'),
    ('device-status', '', '', 'discrete', 'ok', 'degraded', 'fail'),
    ('fmeca.FD0001', '', '', 'boolean'),
    ('time-synchronised', '', '', 'boolean'),
]

EXPECTED_REQUEST_LIST = [
    'data-product-configure',
    'data-product-reconfigure',
    'sdp-status',
    'capture-done',
    'capture-init',
    'capture-status',
    'postproc-init',
    'task-launch',
    'task-terminate',
    'telstate-endpoint',
    'sdp-shutdown',
    'set-config-override'
]


class TestServerThread(threading.Thread):
    """Runs an SDPControllerServer in interface mode on a separate thread"""
    def __init__(self):
        super(TestServerThread, self).__init__(name='SDPControllerServer')
        self.controller_future = concurrent.futures.Future()

    def _initialise(self):
        self._controller = SDPControllerServer(
            '127.0.0.1', 0, {}, '',
            self._ioloop.asyncio_loop, simulate=True, interface_mode=True,
            safe_multicast_cidr="225.100.0.0/16")
        self._controller.start()
        self.controller_future.set_result(self._controller)

    def run(self):
        self._ioloop = AsyncIOLoop()
        self._ioloop.add_callback(self._initialise)
        self._ioloop.start()

    def stop(self):
        self._ioloop.add_callback(self._controller.stop)
        self._ioloop.add_callback(self._ioloop.stop)


class TestSDPController(unittest.TestCase):
    """Testing of the SDP controller.

    Note: The controller itself is only started once, so state remains between tests. For
    this reason unique subarray_ids are used by each test to try and avoid clashes.
    """
    def setUp(self):
        self.thread = TestServerThread()
        self.thread.start()
        try:
            self.controller = self.thread.controller_future.result()
            bind_address = self.controller.bind_address
            self.client = BlockingTestClient(self, *bind_address)
            self.client.start(timeout=1)
            self.client.wait_connected(timeout=1)
        except Exception:
            self.thread.stop()
            raise

    def tearDown(self):
        self.client.stop()
        self.client.join()
        self.thread.stop()
        self.thread.join()

    def test_task_launch(self):
        self.client.assert_request_fails("task-launch","task1")
        self.client.assert_request_succeeds("task-launch","task1","/bin/sleep 5")
        reply, informs = self.client.blocking_request(Message.request("task-launch"))
        self.assertEqual(repr(reply),repr(Message.reply("task-launch","ok",1)))
        self.client.assert_request_succeeds("task-terminate","task1")

    def test_capture_init(self):
        self.client.assert_request_fails("capture-init", SUBARRAY_PRODUCT1)
        self.client.assert_request_succeeds("data-product-configure",SUBARRAY_PRODUCT1,ANTENNAS,"16384","2.1","0","127.0.0.1:9000","127.0.0.1:9001")
        self.client.assert_request_succeeds("capture-init",SUBARRAY_PRODUCT1)

        reply, informs = self.client.blocking_request(Message.request("capture-status",SUBARRAY_PRODUCT1))
        self.assertEqual(repr(reply),repr(Message.reply("capture-status","ok","INIT_WAIT")))

    def test_capture_done(self):
        self.client.assert_request_fails("capture-done",SUBARRAY_PRODUCT2)
        self.client.assert_request_succeeds("data-product-configure",SUBARRAY_PRODUCT2,ANTENNAS,"16384","2.1","0","127.0.0.1:9000","127.0.0.1:9001")
        self.client.assert_request_fails("capture-done",SUBARRAY_PRODUCT2)

        self.client.assert_request_succeeds("capture-init",SUBARRAY_PRODUCT2)
        self.client.assert_request_succeeds("capture-done",SUBARRAY_PRODUCT2)

    def test_deconfigure_subarray_product(self):
        self.client.assert_request_fails("data-product-configure",SUBARRAY_PRODUCT3,"")
        self.client.assert_request_succeeds("data-product-configure",SUBARRAY_PRODUCT3,ANTENNAS,"16384","2.1","0","127.0.0.1:9000","127.0.0.1:9001")
        self.client.assert_request_succeeds("capture-init",SUBARRAY_PRODUCT3)
        self.client.assert_request_fails("data-product-configure",SUBARRAY_PRODUCT3,"")
         # should not be able to deconfigure when not in idle state
        self.client.assert_request_succeeds("capture-done",SUBARRAY_PRODUCT3)
        self.client.assert_request_succeeds("data-product-configure",SUBARRAY_PRODUCT3,"")

    def test_configure_subarray_product(self):
        self.client.assert_request_fails("data-product-configure",SUBARRAY_PRODUCT4)
        self.client.assert_request_succeeds("data-product-configure")
        self.client.assert_request_succeeds("data-product-configure",SUBARRAY_PRODUCT4,ANTENNAS,"16384","2.1","0","baseline-correlation-products:127.0.0.1:9000,CAM:127.0.0.1:9001,CAM_ws:ws://host.domain:port/path")
        self.client.assert_request_succeeds("data-product-configure",SUBARRAY_PRODUCT4)

        reply, informs = self.client.blocking_request(Message.request("data-product-configure"))
        self.assertEqual(repr(reply),repr(Message.reply("data-product-configure","ok",1)))

        self.client.assert_request_succeeds("data-product-configure",SUBARRAY_PRODUCT4,"")
        self.client.assert_request_fails("data-product-configure",SUBARRAY_PRODUCT4)

    def test_sensor_list(self):
        self.client.test_sensor_list(EXPECTED_SENSOR_LIST,ignore_descriptions=True)

    def test_help(self):
        self.client.test_help(EXPECTED_REQUEST_LIST)

class TestSDPResources(unittest.TestCase):
    """Test :class:`katsdpcontroller.sdpcontroller.SDPResources`."""
    def setUp(self):
        self.r = SDPCommonResources("225.100.0.0/16")
        self.r1 = SDPResources(self.r, 'array_1_c856M4k')
        self.r2 = SDPResources(self.r, 'array_1_bc856M4k')

    def test_multicast_ip(self):
        # Get assigned IP's from known host classes
        self.assertEqual('225.100.1.1', self.r1.get_multicast_ip('l0_spectral_spead'))
        self.assertEqual('225.100.4.1', self.r1.get_multicast_ip('l1_continuum_spead'))
        # Get assigned IP from unknown host class
        self.assertEqual('225.100.0.1', self.r1.get_multicast_ip('unknown'))
        # Check that assignments are remembered
        self.assertEqual('225.100.1.1', self.r1.get_multicast_ip('l0_spectral_spead'))
        self.assertEqual('225.100.4.1', self.r1.get_multicast_ip('l1_continuum_spead'))
        self.assertEqual('225.100.0.1', self.r1.get_multicast_ip('unknown'))
        # Override an assignment, check that this is remembered
        self.r1.set_multicast_ip('l0_spectral_spead', '239.1.2.3')
        self.assertEqual('239.1.2.3', self.r1.get_multicast_ip('l0_spectral_spead'))
        # Assign a value not previously seen
        self.r1.set_multicast_ip('CAM_spead', '239.4.5.6')
        self.assertEqual('239.4.5.6', self.r1.get_multicast_ip('CAM_spead'))

        # Now change to a different subarray-product, check that
        # new values are used.
        self.assertEqual('225.100.1.2', self.r2.get_multicast_ip('l0_spectral_spead'))
        self.assertEqual('225.100.0.2', self.r2.get_multicast_ip('CAM_spead'))
        self.assertEqual('225.100.0.3', self.r2.get_multicast_ip('unknown'))

    def test_url(self):
        self.assertEqual(None, self.r1.get_url('CAM_ws'))
        self.r1.set_url('CAM_ws', 'ws://host.domain:port/path')
        self.assertEqual('ws://host.domain:port/path', self.r1.get_url('CAM_ws'))
        # URLs should be unique per subarray-product
        self.assertIsNone(self.r2.get_url('CAM_ws'))
