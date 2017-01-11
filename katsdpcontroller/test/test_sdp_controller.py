"""Tests for the sdp controller module."""

import threading
import concurrent.futures
import unittest2 as unittest
import mock

from katcp.testutils import BlockingTestClient
from katcp import Message
import tornado.concurrent
import tornado.gen
from tornado.platform.asyncio import AsyncIOLoop
import trollius
from trollius import From
import katcp
import redis

from katsdpcontroller.sdpcontroller import (
        SDPControllerServer, SDPCommonResources, SDPResources, State)
from katsdpcontroller import scheduler
from katsdpcontroller.test.test_scheduler import AnyOrderList

ANTENNAS = 'm063,m064'

PRODUCT = 'c856M4k'
SUBARRAY_PRODUCT1 = 'array_1_' + PRODUCT
SUBARRAY_PRODUCT2 = 'array_2_' + PRODUCT
SUBARRAY_PRODUCT3 = 'array_3_' + PRODUCT
SUBARRAY_PRODUCT4 = 'array_4_' + PRODUCT
STREAM_SOURCES = "baseline-correlation-products:127.0.0.1:9000,CAM:127.0.0.1:9001,CAMDATA:ws://host.domain:port/path"

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
    """Runs an SDPControllerServer on a separate thread"""
    def __init__(self, *args, **kwargs):
        super(TestServerThread, self).__init__(name='SDPControllerServer')
        self.controller_future = concurrent.futures.Future()
        self._args = args
        self._kwargs = kwargs

    def _initialise(self):
        self.loop = self._ioloop.asyncio_loop
        self._kwargs['loop'] = self.loop
        self._controller = SDPControllerServer(*self._args, **self._kwargs)
        self._controller.start()
        self.controller_future.set_result(self._controller)

    def run(self):
        self._ioloop = AsyncIOLoop()
        self._ioloop.add_callback(self._initialise)
        self._ioloop.start()

    def stop(self):
        self._ioloop.add_callback(self._controller.stop)
        self._ioloop.add_callback(self._ioloop.stop)


class TestSDPControllerInterface(unittest.TestCase):
    """Testing of the SDP controller in interface mode."""
    def setUp(self):
        self.thread = TestServerThread(
            '127.0.0.1', 0, None, simulate=True, interface_mode=True,
            safe_multicast_cidr="225.100.0.0/16")
        self.thread.start()
        self.addCleanup(self.thread.join)
        self.addCleanup(self.thread.stop)
        self.controller = self.thread.controller_future.result()
        bind_address = self.controller.bind_address
        self.client = BlockingTestClient(self, *bind_address)
        self.client.start(timeout=1)
        self.addCleanup(self.client.join)
        self.addCleanup(self.client.stop)

    def test_task_launch(self):
        self.client.assert_request_fails("task-launch","task1")
        self.client.assert_request_succeeds("task-launch","task1","/bin/sleep 5")
        reply, informs = self.client.blocking_request(Message.request("task-launch"))
        self.assertEqual(repr(reply),repr(Message.reply("task-launch","ok",1)))
        self.client.assert_request_succeeds("task-terminate","task1")

    def test_capture_init(self):
        self.client.assert_request_fails("capture-init", SUBARRAY_PRODUCT1)
        self.client.assert_request_succeeds("data-product-configure",SUBARRAY_PRODUCT1,ANTENNAS,"4096","2.1","0","127.0.0.1:9000","127.0.0.1:9001")
        self.client.assert_request_succeeds("capture-init",SUBARRAY_PRODUCT1)

        reply, informs = self.client.blocking_request(Message.request("capture-status",SUBARRAY_PRODUCT1))
        self.assertEqual(repr(reply),repr(Message.reply("capture-status","ok","INIT_WAIT")))
        self.client.assert_request_fails("capture-init", SUBARRAY_PRODUCT1)

    def test_capture_done(self):
        self.client.assert_request_fails("capture-done",SUBARRAY_PRODUCT2)
        self.client.assert_request_succeeds("data-product-configure",SUBARRAY_PRODUCT2,ANTENNAS,"4096","2.1","0","127.0.0.1:9000","127.0.0.1:9001")
        self.client.assert_request_fails("capture-done",SUBARRAY_PRODUCT2)

        self.client.assert_request_succeeds("capture-init",SUBARRAY_PRODUCT2)
        self.client.assert_request_succeeds("capture-done",SUBARRAY_PRODUCT2)
        self.client.assert_request_fails("capture-done",SUBARRAY_PRODUCT2)

    def test_deconfigure_subarray_product(self):
        self.client.assert_request_fails("data-product-configure",SUBARRAY_PRODUCT3,"")
        self.client.assert_request_succeeds("data-product-configure",SUBARRAY_PRODUCT3,ANTENNAS,"4096","2.1","0","127.0.0.1:9000","127.0.0.1:9001")
        self.client.assert_request_succeeds("capture-init",SUBARRAY_PRODUCT3)
        self.client.assert_request_fails("data-product-configure",SUBARRAY_PRODUCT3,"")
         # should not be able to deconfigure when not in idle state
        self.client.assert_request_succeeds("capture-done",SUBARRAY_PRODUCT3)
        self.client.assert_request_succeeds("data-product-configure",SUBARRAY_PRODUCT3,"")

    def test_configure_subarray_product(self):
        self.client.assert_request_fails("data-product-configure",SUBARRAY_PRODUCT4)
        self.client.assert_request_succeeds("data-product-configure")
        self.client.assert_request_succeeds("data-product-configure",SUBARRAY_PRODUCT4,ANTENNAS,"4096","2.1","0",STREAM_SOURCES)
        self.client.assert_request_succeeds("data-product-configure",SUBARRAY_PRODUCT4)

        reply, informs = self.client.blocking_request(Message.request("data-product-configure"))
        self.assertEqual(repr(reply),repr(Message.reply("data-product-configure","ok",1)))

        self.client.assert_request_succeeds("data-product-configure",SUBARRAY_PRODUCT4,"")
        self.client.assert_request_fails("data-product-configure",SUBARRAY_PRODUCT4)

    def test_sensor_list(self):
        self.client.test_sensor_list(EXPECTED_SENSOR_LIST,ignore_descriptions=True)

    def test_help(self):
        self.client.test_help(EXPECTED_REQUEST_LIST)


class TestSDPController(unittest.TestCase):
    """Test :class:`katsdpcontroller.sdpcontroller.SDPController` using
    mocking of the scheduler.
    """
    def create_patch(self, *args, **kwargs):
        patcher = mock.patch(*args, **kwargs)
        mock_obj = patcher.start()
        self.addCleanup(patcher.stop)
        return mock_obj

    def setUp(self):
        done_future = tornado.concurrent.Future()
        done_future.set_result(None)
        ok_future = tornado.concurrent.Future()
        ok_future.set_result((katcp.Message.reply('dummy', 'ok'), []))
        self.telstate_class = self.create_patch('katsdptelstate.TelescopeState', autospec=True)
        self.sensor_proxy_client_class = self.create_patch('katsdpcontroller.sensor_proxy.SensorProxyClient', autospec=True)
        sensor_proxy_client = self.sensor_proxy_client_class.return_value
        sensor_proxy_client.start.return_value = done_future
        sensor_proxy_client.until_synced.return_value = done_future
        sensor_proxy_client.katcp_client = mock.create_autospec(katcp.AsyncClient, instance=True)
        sensor_proxy_client.katcp_client.future_request.return_value = ok_future
        self.create_patch(
            'katsdpcontroller.scheduler.poll_ports', autospec=True, return_value=None)
        self.sched = mock.create_autospec(spec=scheduler.Scheduler, instance=True)
        self.sched.launch.side_effect = self._launch
        self.sched.kill.side_effect = self._kill
        self.thread = TestServerThread(
            '127.0.0.1', 0, self.sched,
            safe_multicast_cidr="225.100.0.0/16")
        self.thread.start()
        self.addCleanup(self.thread.join)
        self.addCleanup(self.thread.stop)
        self.controller = self.thread.controller_future.result()
        bind_address = self.controller.bind_address
        self.client = BlockingTestClient(self, *bind_address)
        self.client.start(timeout=1)
        self.addCleanup(self.client.join)
        self.addCleanup(self.client.stop)
        self.loop = self.thread.loop
        # List of tasks which should be set to DEAD on launch
        self.fail_launches = []

    @trollius.coroutine
    def _launch(self, graph, resolver, nodes=None):
        """Mock implementation of Scheduler.launch."""
        if nodes is None:
            nodes = graph.nodes()
        for node in nodes:
            if node.state < scheduler.TaskState.RUNNING:
                node.set_state(scheduler.TaskState.STARTING)
        for node in nodes:
            if node.state < scheduler.TaskState.RUNNING:
                if hasattr(node.logical_node, 'ports'):
                    port_num = 20000
                    for port in node.logical_node.ports:
                        if port is not None:
                            node.ports[port] = port_num
                            port_num += 1
                node.allocation = mock.MagicMock()
                node.allocation.agent.host = 'host.' + node.name
        for node in nodes:
            if node.state < scheduler.TaskState.RUNNING:
                node.resolve(resolver, graph)
                if node.logical_node.name in self.fail_launches:
                    node.set_state(scheduler.TaskState.DEAD)
                else:
                    node.set_state(scheduler.TaskState.RUNNING)
        futures = []
        for node in nodes:
            futures.append(node.ready_event.wait())
        yield From(trollius.gather(*futures, loop=self.loop))


    @trollius.coroutine
    def _kill(self, graph, nodes=None):
        """Mock implementation of Scheduler.kill."""
        if nodes is not None:
            kill_graph = graph.subgraph(nodes)
        else:
            kill_graph = graph
        for node in kill_graph:
            node.set_state(scheduler.TaskState.DEAD)

    @trollius.coroutine
    def _poll_ports(self, host, ports, loop):
        pass

    def test_data_product_configure_success(self):
        """A ?data-product-configure request must wait for the tasks to come up, then indicate success."""
        self.client.assert_request_succeeds("data-product-configure",SUBARRAY_PRODUCT4,ANTENNAS,"4096","2.1","0",STREAM_SOURCES)
        self.telstate_class.assert_called_once_with('host.sdp.telstate:20000')

        # Verify the telescope state
        ts = self.telstate_class.return_value
        # Print the list so assist in debugging if the assert fails
        print(ts.add.call_args_list)
        # This is not a complete list of calls. It check that each category of stuff
        # is covered: additional_config, base_params, per node, per edge
        ts.add.assert_any_call('config', {
            'antenna_mask': ANTENNAS,
            'subarray_numeric_id': 4,
            'sd_int_time': 1.0 / 2.1,
            'output_int_time': 1.0 / 2.1,
            'stream_sources': STREAM_SOURCES
        }, immutable=True)
        ts.add.assert_any_call('sdp_cbf_channels', 4096, immutable=True)
        ts.add.assert_any_call('config.sdp.filewriter.1', {
            'file_base': '/var/kat/data',
            'port': 20000,
            'l0_spectral_spead': mock.ANY
        }, immutable=True)

        # Verify the state of the subarray
        self.assertIsNone(self.controller._conf_future)
        sa = self.controller.subarray_products[SUBARRAY_PRODUCT4]
        self.assertFalse(sa._async_busy)
        self.assertEqual(State.IDLE, sa.state)

    def test_data_product_configure_telstate_fail(self):
        """If the telstate task fails, data-product-configure must fail"""
        self.fail_launches.append('sdp.telstate')
        self.telstate_class.side_effect = redis.ConnectionError
        self.client.assert_request_fails("data-product-configure",SUBARRAY_PRODUCT4,ANTENNAS,"4096","2.1","0",STREAM_SOURCES)
        self.sched.launch.assert_called_with(mock.ANY, mock.ANY, mock.ANY)
        self.sched.kill.assert_called_with(mock.ANY)
        # Must not have created the subarray product internally
        self.assertEqual({}, self.controller.subarray_products)

    def test_capture_init(self):
        """Checks that capture-init succeeds and sets appropriate state"""
        self.client.assert_request_succeeds("data-product-configure",SUBARRAY_PRODUCT4,ANTENNAS,"4096","2.1","0",STREAM_SOURCES)
        self.client.assert_request_succeeds("capture-init", SUBARRAY_PRODUCT4)
        # check that the subarray is in an appropriate state
        sa = self.controller.subarray_products[SUBARRAY_PRODUCT4]
        self.assertFalse(sa._async_busy)
        self.assertEqual(State.INIT_WAIT, sa.state)
        # Check that the graph transitions succeeded
        katcp_client = self.sensor_proxy_client_class.return_value.katcp_client
        katcp_client.future_request.assert_called_with(
            katcp.Message.request('capture-init'), timeout=mock.ANY)

    def test_capture_init_failed_req(self):
        """Capture-init bumbles on even if a child request fails.

        TODO: that's probably not really the behaviour we want.
        """
        self.client.assert_request_succeeds("data-product-configure",SUBARRAY_PRODUCT4,ANTENNAS,"4096","2.1","0",STREAM_SOURCES)
        katcp_client = self.sensor_proxy_client_class.return_value.katcp_client
        fail_future = tornado.gen.Future()
        fail_future.set_result((katcp.Message.reply('fail', 'dummy failure'), []))
        katcp_client.future_request.return_value = fail_future
        self.client.assert_request_succeeds("capture-init", SUBARRAY_PRODUCT4)
        # check that the subarray is in an appropriate state
        sa = self.controller.subarray_products[SUBARRAY_PRODUCT4]
        self.assertEqual(State.INIT_WAIT, sa.state)

    def test_capture_init_dead_process(self):
        """Capture-init bumbles on even if a child process is dead.

        TODO: that's probably not really the behaviour we want.
        """
        self.client.assert_request_succeeds("data-product-configure",SUBARRAY_PRODUCT4,ANTENNAS,"4096","2.1","0",STREAM_SOURCES)
        sa = self.controller.subarray_products[SUBARRAY_PRODUCT4]
        for node in sa.graph.physical_graph:
            if node.logical_node.name == 'sdp.ingest.1':
                node.set_state(scheduler.TaskState.DEAD)
                break
        else:
            raise ValueError('Could not find ingest node')
        self.client.assert_request_succeeds("capture-init", SUBARRAY_PRODUCT4)
        # check that the subarray is in an appropriate state
        self.assertEqual(State.INIT_WAIT, sa.state)


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
        self.assertEqual(None, self.r1.get_url('CAMDATA'))
        self.r1.set_url('CAMDATA', 'ws://host.domain:port/path')
        self.assertEqual('ws://host.domain:port/path', self.r1.get_url('CAMDATA'))
        # URLs should be unique per subarray-product
        self.assertIsNone(self.r2.get_url('CAMDATA'))
