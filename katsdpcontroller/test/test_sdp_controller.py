"""Tests for the sdp controller module."""

import time
import threading
import contextlib
import concurrent.futures
import unittest2 as unittest
import mock

from addict import Dict
from katcp.testutils import BlockingTestClient
from katcp import Message
import tornado.concurrent
import tornado.gen
from tornado.platform.asyncio import AsyncIOLoop
import trollius
from trollius import From
import katcp
import redis
import pymesos
import netifaces
import requests

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

STREAMS = '{"cam.http": {"camdata": "http://127.0.0.1:8999"}, \
            "cbf.baseline_correlation_products": {"i0.baseline-correlation-products": "spead://127.0.0.1:9000"}, \
            "cbf.antenna_channelised_voltage": {"i0.antenna-channelised-voltage": "spead://127.0.0.1:9001"}}'

EXPECTED_SENSOR_LIST = (
    ('api-version', '', '', 'string'),
    ('build-state', '', '', 'string'),
    ('device-status', '', '', 'discrete', 'ok', 'degraded', 'fail'),
    ('fmeca.FD0001', '', '', 'boolean'),
    ('time-synchronised', '', '', 'boolean'),
    ('gui-urls', '', '', 'string')
)

EXPECTED_INTERFACE_SENSOR_LIST_1 = tuple(
    (SUBARRAY_PRODUCT1 + '.' + s[0],) + s[1:] for s in (
        ('sdp.bf_ingest.1.port', '', '', 'string'),
        ('sdp.filewriter.1.filename', '', '', 'string'),
        ('sdp.filewriter.1.input_rate', '', 'Bps', 'float'),
        ('sdp.ingest.1.capture-active', '', '', 'boolean'),
        ('sdp.timeplot.1.gui-urls', '', '', 'string'),
        ('sdp.timeplot.1.html_port', '', '', 'string'),
))

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
        self.client.wait_connected(timeout=1)
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
        self.client.assert_request_succeeds("data-product-configure",SUBARRAY_PRODUCT1,ANTENNAS,"4096","2.1","0",STREAMS)
        self.client.assert_request_succeeds("capture-init",SUBARRAY_PRODUCT1)

        reply, informs = self.client.blocking_request(Message.request("capture-status",SUBARRAY_PRODUCT1))
        self.assertEqual(repr(reply),repr(Message.reply("capture-status","ok","INITIALISED")))
        self.client.assert_request_fails("capture-init", SUBARRAY_PRODUCT1)

    def test_interface_sensors(self):
        self.client.test_sensor_list(EXPECTED_SENSOR_LIST,ignore_descriptions=True)
        recorder = self.client.message_recorder(('interface-changed',))
        self.client.assert_request_succeeds(
            "data-product-configure", SUBARRAY_PRODUCT1,
            ANTENNAS, "4096", "2.1", "0", STREAMS)
        self.assertEqual([str(m) for m in recorder()], ['#interface-changed'])
        self.client.test_sensor_list(
            EXPECTED_SENSOR_LIST + EXPECTED_INTERFACE_SENSOR_LIST_1,
            ignore_descriptions=True)
        # Deconfigure and check that the array sensors are gone
        self.client.assert_request_succeeds(
            "data-product-configure", SUBARRAY_PRODUCT1, "")
        self.assertEqual([str(m) for m in recorder()], ['#interface-changed'])
        self.client.test_sensor_list(
            EXPECTED_SENSOR_LIST, ignore_descriptions=True)

    def test_capture_done(self):
        self.client.assert_request_fails("capture-done",SUBARRAY_PRODUCT2)
        self.client.assert_request_succeeds("data-product-configure",SUBARRAY_PRODUCT2,ANTENNAS,"4096","2.1","0",STREAMS)
        self.client.assert_request_fails("capture-done",SUBARRAY_PRODUCT2)

        self.client.assert_request_succeeds("capture-init",SUBARRAY_PRODUCT2)
        self.client.assert_request_succeeds("capture-done",SUBARRAY_PRODUCT2)
        self.client.assert_request_fails("capture-done",SUBARRAY_PRODUCT2)

    def test_deconfigure_subarray_product(self):
        self.client.assert_request_fails("data-product-configure",SUBARRAY_PRODUCT3,"")
        self.client.assert_request_succeeds("data-product-configure",SUBARRAY_PRODUCT3,ANTENNAS,"4096","2.1","0",STREAMS)
        self.client.assert_request_succeeds("capture-init",SUBARRAY_PRODUCT3)
        self.client.assert_request_fails("data-product-configure",SUBARRAY_PRODUCT3,"")
         # should not be able to deconfigure when not in idle state
        self.client.assert_request_succeeds("capture-done",SUBARRAY_PRODUCT3)
        self.client.assert_request_succeeds("data-product-configure",SUBARRAY_PRODUCT3,"")

    def test_configure_subarray_product(self):
        self.client.assert_request_fails("data-product-configure",SUBARRAY_PRODUCT4)
        self.client.assert_request_succeeds("data-product-configure")
        self.client.assert_request_succeeds("data-product-configure",SUBARRAY_PRODUCT4,ANTENNAS,"4096","2.1","0",STREAMS)
        self.client.assert_request_succeeds("data-product-configure",SUBARRAY_PRODUCT4,ANTENNAS,"4096","2.1","0",STREAMS)
        self.client.assert_request_fails("data-product-configure",SUBARRAY_PRODUCT4,ANTENNAS,"4096","2.2","0",STREAMS)
        self.client.assert_request_succeeds("data-product-configure",SUBARRAY_PRODUCT4)

        reply, informs = self.client.blocking_request(Message.request("data-product-configure"))
        self.assertEqual(repr(reply),repr(Message.reply("data-product-configure","ok",1)))

        self.client.assert_request_succeeds("data-product-configure",SUBARRAY_PRODUCT4,"")
        self.client.assert_request_fails("data-product-configure",SUBARRAY_PRODUCT4)

    def test_reconfigure_subarray_product(self):
        self.client.assert_request_fails("data-product-reconfigure", SUBARRAY_PRODUCT4)
        self.client.assert_request_succeeds("data-product-configure",SUBARRAY_PRODUCT4,ANTENNAS,"4096","2.1","0",STREAMS)
        self.client.assert_request_succeeds("data-product-reconfigure", SUBARRAY_PRODUCT4)
        self.client.assert_request_succeeds("capture-init", SUBARRAY_PRODUCT4)
        self.client.assert_request_fails("data-product-reconfigure", SUBARRAY_PRODUCT4)

    def test_sensor_list(self):
        self.client.test_sensor_list(EXPECTED_SENSOR_LIST,ignore_descriptions=True)

    def test_help(self):
        self.client.test_help(EXPECTED_REQUEST_LIST)


class TestSDPController(unittest.TestCase):
    """Test :class:`katsdpcontroller.sdpcontroller.SDPController` using
    mocking of the scheduler.
    """
    def _create_patch(self, *args, **kwargs):
        patcher = mock.patch(*args, **kwargs)
        mock_obj = patcher.start()
        self.addCleanup(patcher.stop)
        return mock_obj

    def _delay_mock(self, mock):
        """Modify a callable mock so that we can make it block and run code
        while it is blocked. When run, it will set an empty result on one
        future, while returning another one to the caller.

        Parameters
        ----------
        mock : `mock.MagicMock`
            Callable mock

        Returns
        -------
        started : `concurrent.futures.Future`
            Future signalled when the mock is called
        release : callable
            Call with result to set on future returned from the mock
        """
        started = concurrent.futures.Future()
        result = tornado.gen.Future()
        def side_effect(*args, **kwargs):
            started.set_result(None)
            return result
        def release(value):
            self.controller.loop.call_soon_threadsafe(result.set_result, value)
        mock.side_effect = side_effect
        return started, release

    @contextlib.contextmanager
    def _capture_init_slow(self, subarray_product):
        """Context manager that runs its block with a capture-init in
        progress. The subarray product must already be configured."""
        # Set when the capture-init is blocked
        reply_future = concurrent.futures.Future()
        sensor_proxy_client = self.sensor_proxy_client_class.return_value
        started_future, release = self._delay_mock(sensor_proxy_client.katcp_client.future_request)
        self.client.callback_request(
            Message.request('capture-init', subarray_product),
            reply_cb=lambda msg: reply_future.set_result(msg))
        # Wait until the first command gets blocked
        started_future.result()
        # Do the test
        try:
            yield
        finally:
            # Unblock things
            release((Message.reply('capture-init', 'ok'), []))
        self.assertTrue(reply_future.result().reply_ok())

    @contextlib.contextmanager
    def _data_product_configure_slow(self, subarray_product, expect_ok=True):
        """Context manager that runs its block with a data-product-configure in
        progress."""
        # See comments in _capture_init_slow
        reply_future = concurrent.futures.Future()
        sensor_proxy_client = self.sensor_proxy_client_class.return_value
        started_future, release = self._delay_mock(sensor_proxy_client.until_synced)
        self.client.callback_request(
            Message.request(*self._configure_args(subarray_product)),
            reply_cb=lambda msg: reply_future.set_result(msg))
        started_future.result()
        try:
            yield
        finally:
            release(None)
        self.assertEqual(expect_ok, reply_future.result().reply_ok())

    def setUp(self):
        # Future that is already resolved with no return value
        done_future = tornado.concurrent.Future()
        done_future.set_result(None)
        self.telstate_class = self._create_patch('katsdptelstate.TelescopeState', autospec=True)
        self.sensor_proxy_client_class = self._create_patch('katsdpcontroller.sensor_proxy.SensorProxyClient', autospec=True)
        sensor_proxy_client = self.sensor_proxy_client_class.return_value
        sensor_proxy_client.start.return_value = done_future
        sensor_proxy_client.until_synced.return_value = done_future
        sensor_proxy_client.katcp_client = mock.create_autospec(katcp.AsyncClient, instance=True)
        sensor_proxy_client.katcp_client.future_request.side_effect = self._future_request
        self._create_patch(
            'katsdpcontroller.scheduler.poll_ports', autospec=True, return_value=None)
        self._create_patch('netifaces.interfaces', autospec=True, return_value=['lo', 'em1'])
        self._create_patch('netifaces.ifaddresses', autospec=True, side_effect=self._ifaddresses)
        self.sched = mock.create_autospec(spec=scheduler.Scheduler, instance=True)
        self.sched.launch.side_effect = self._launch
        self.sched.kill.side_effect = self._kill
        self.driver = mock.create_autospec(spec=pymesos.MesosSchedulerDriver, instance=True)
        self.thread = TestServerThread(
            '127.0.0.1', 0, self.sched, simulate=True,
            safe_multicast_cidr="225.100.0.0/16")
        self.thread.start()
        self.addCleanup(self.thread.join)
        self.addCleanup(self.thread.stop)
        self.controller = self.thread.controller_future.result()
        bind_address = self.controller.bind_address
        self.client = BlockingTestClient(self, *bind_address)
        self.client.start(timeout=1)
        self.client.wait_connected(timeout=1)
        self.addCleanup(self.client.join)
        self.addCleanup(self.client.stop)
        self.loop = self.thread.loop
        master_and_slaves_future = trollius.Future(loop=self.loop)
        master_and_slaves_future.set_result(('10.0.0.1', ['10.0.0.1', '10.0.0.2', '10.0.0.3', '10.0.0.4']))
        self.sched.get_master_and_slaves.return_value = master_and_slaves_future
        # Dict mapping task name to Mesos task status string
        self.fail_launches = {}
        # Set of katcp requests to return failures for
        self.fail_requests = set()

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
                node.allocation.agent.host = 'host.' + node.logical_node.name
                node.allocation.agent.gpus[0].name = 'GeForce GTX TITAN X'
        for node in nodes:
            if node.state < scheduler.TaskState.RUNNING:
                yield From(node.resolve(resolver, graph, self.loop))
                if node.logical_node.name in self.fail_launches:
                    node.set_state(scheduler.TaskState.DEAD)
                    # This may need to be fleshed out if sdp_controller looks
                    # at other fields.
                    node.status = Dict(state=self.fail_launches[node.logical_node.name])
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
            if scheduler.TaskState.STARTED <= node.state < scheduler.TaskState.KILLED:
                node.kill(self.driver)
            node.set_state(scheduler.TaskState.DEAD)

    def _future_request(self, msg, *args, **kwargs):
        """Mock implementation of katcp.DeviceClient.future_request"""
        if msg.name in self.fail_requests:
            reply = Message.reply(msg.name, 'fail', 'dummy failure')
        else:
            reply = Message.reply(msg.name, 'ok')
        future = tornado.concurrent.Future()
        future.set_result((reply, []))
        return future

    def _ifaddresses(self, interface):
        if interface == 'lo':
            return {
                netifaces.AF_INET: [{'addr': '127.0.0.1', 'netmask': '255.0.0.0', 'peer': '127.0.0.1'}],
                netifaces.AF_INET6: [{'addr': '::1', 'netmask': 'ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff/128'}],
            }
        elif interface == 'em1':
            return {
                netifaces.AF_INET: [{'addr': '10.0.0.2', 'broadcast': '10.255.255.255', 'netmask': '255.0.0.0'}],
            }
        else:
            raise ValueError('You must specify a valid interface name')

    def _configure_args(self, subarray_product):
        return ("data-product-configure", subarray_product, ANTENNAS, "4096",
                "2.1", "0", STREAMS)

    def _configure_subarray(self, subarray_product):
        self.client.assert_request_succeeds(*self._configure_args(subarray_product))

    def test_data_product_configure_success(self):
        """A ?data-product-configure request must wait for the tasks to come up, then indicate success."""
        self.client.assert_request_succeeds(
            'set-config-override', SUBARRAY_PRODUCT4, '{"override_key": ["override_value"]}')
        self._configure_subarray(SUBARRAY_PRODUCT4)
        self.telstate_class.assert_called_once_with('host.sdp.telstate:20000')

        # Verify the telescope state
        ts = self.telstate_class.return_value
        # Print the list so assist in debugging if the assert fails
        print(ts.add.call_args_list)
        # This is not a complete list of calls. It check that each category of stuff
        # is covered: overrides, additional_config, base_params, per node, per edge
        ts.add.assert_any_call('config', {
            'antenna_mask': ANTENNAS,
            'subarray_numeric_id': 4,
            'sd_int_time': 1.0 / 2.1,
            'output_int_time': 1.0 / 2.1,
            'stream_sources': STREAMS,
            'override_key': ['override_value']
        }, immutable=True)
        ts.add.assert_any_call('sdp_cbf_channels', 4096, immutable=True)
        ts.add.assert_any_call('config.sdp.filewriter.1', {
            'file_base': '/var/kat/data',
            'port': 20000,
            'l0_spectral_spead': mock.ANY
        }, immutable=True)

        # Verify the state of the subarray
        self.assertIsNone(self.controller._conf_future)
        self.assertEqual({}, self.controller.override_dicts)
        sa = self.controller.subarray_products[SUBARRAY_PRODUCT4]
        self.assertFalse(sa._async_busy)
        self.assertEqual(State.IDLE, sa.state)

    def test_data_product_configure_telstate_fail(self):
        """If the telstate task fails, data-product-configure must fail"""
        self.fail_launches['sdp.telstate'] = 'TASK_FAILED'
        self.telstate_class.side_effect = redis.ConnectionError
        self.client.assert_request_fails(*self._configure_args(SUBARRAY_PRODUCT4))
        self.sched.launch.assert_called_with(mock.ANY, mock.ANY, mock.ANY)
        self.sched.kill.assert_called_with(mock.ANY)
        # Must not have created the subarray product internally
        self.assertEqual({}, self.controller.subarray_products)
        self.assertEqual({}, self.controller.subarray_product_config)

    def test_data_product_configure_task_fail(self):
        """If a task other than telstate fails, data-product-configure must fail"""
        self.fail_launches['sdp.ingest.1'] = 'TASK_FAILED'
        self.client.assert_request_fails(*self._configure_args(SUBARRAY_PRODUCT4))
        self.telstate_class.assert_called_once_with('host.sdp.telstate:20000')
        self.sched.launch.assert_called_with(mock.ANY, mock.ANY)
        self.sched.kill.assert_called_with(mock.ANY)
        # Must not have created the subarray product internally
        self.assertEqual({}, self.controller.subarray_products)
        self.assertEqual({}, self.controller.subarray_product_config)

    def test_data_product_configure_busy(self):
        """Cannot have concurrent data-product-configure commands"""
        with self._data_product_configure_slow(SUBARRAY_PRODUCT1):
            # We use a different subarray product, which would otherwise be
            # legal.
            self.client.assert_request_fails(*self._configure_args(SUBARRAY_PRODUCT2))
        # Check that no state leaked through
        self.assertNotIn(SUBARRAY_PRODUCT2, self.controller.subarray_products)
        self.assertNotIn(SUBARRAY_PRODUCT2, self.controller.subarray_product_config)
        self.assertIsNone(self.controller._conf_future)

    def test_data_product_deconfigure(self):
        """Checks success path of data-product-configure for deconfiguration"""
        self._configure_subarray(SUBARRAY_PRODUCT1)
        self.client.assert_request_succeeds("data-product-configure", SUBARRAY_PRODUCT1, "0")
        # Check that the graph was shut down
        self.sched.kill.assert_called_with(mock.ANY)
        # Verify the state
        self.assertIsNone(self.controller._conf_future)
        self.assertEqual({}, self.controller.subarray_products)
        self.assertEqual({}, self.controller.subarray_product_config)

    def test_data_product_deconfigure_capturing(self):
        """data-product-configure for deconfigure must fail while capturing"""
        self._configure_subarray(SUBARRAY_PRODUCT1)
        self.client.assert_request_succeeds("capture-init", SUBARRAY_PRODUCT1)
        self.client.assert_request_fails("data-product-configure", SUBARRAY_PRODUCT1, "0")

    def test_data_product_deconfigure_busy(self):
        """data-product-configure for deconfigure cannot happen concurrently with capture-init"""
        self._configure_subarray(SUBARRAY_PRODUCT1)
        with self._capture_init_slow(SUBARRAY_PRODUCT1):
            self.client.assert_request_fails('data-product-configure', SUBARRAY_PRODUCT1, '0')
        # Check that the subarray still exists and has the right state
        sa = self.controller.subarray_products[SUBARRAY_PRODUCT1]
        self.assertFalse(sa._async_busy)
        self.assertEqual(State.INITIALISED, sa.state)

    def test_data_product_reconfigure(self):
        """Checks success path of data_product_reconfigure"""
        self._configure_subarray(SUBARRAY_PRODUCT1)
        self.sched.launch.reset_mock()
        self.sched.kill.reset_mock()

        self.client.assert_request_succeeds(
            'set-config-override', SUBARRAY_PRODUCT1, '{"override_key": ["override_value2"]}')
        self.client.assert_request_succeeds('data-product-reconfigure', SUBARRAY_PRODUCT1)
        # Check that the graph was killed and restarted
        self.sched.kill.assert_called_with(mock.ANY)
        self.sched.launch.assert_called_with(mock.ANY, mock.ANY)
        # Check that the override took effect
        ts = self.telstate_class.return_value
        print(ts.add.call_args_list)
        ts.add.assert_any_call('config', {
            'antenna_mask': ANTENNAS,
            'subarray_numeric_id': 1,
            'sd_int_time': 1.0 / 2.1,
            'output_int_time': 1.0 / 2.1,
            'stream_sources': STREAMS,
            'override_key': ['override_value2']
        }, immutable=True)

    def test_data_product_reconfigure_configure_busy(self):
        """Cannot run data-product-reconfigure concurrently with another
        data-product-configure"""
        self._configure_subarray(SUBARRAY_PRODUCT1)
        with self._data_product_configure_slow(SUBARRAY_PRODUCT2):
            self.client.assert_request_fails('data-product-reconfigure', SUBARRAY_PRODUCT1)

    def test_data_product_reconfigure_configure_fails(self):
        """Tests data-product-reconfigure when the new graph fails"""
        self._configure_subarray(SUBARRAY_PRODUCT1)
        self.fail_launches['sdp.telstate'] = 'TASK_FAILED'
        self.client.assert_request_fails('data-product-reconfigure', SUBARRAY_PRODUCT1)
        # Check that the subarray was deconfigured cleanly
        self.assertIsNone(self.controller._conf_future)
        self.assertEqual({}, self.controller.subarray_products)
        self.assertEqual({}, self.controller.subarray_product_config)

    # TODO: test that reconfigure with override dict picks up the override dict
    # TODO: test that reconfigure fails if another configuration is happening

    def test_capture_init(self):
        """Checks that capture-init succeeds and sets appropriate state"""
        self._configure_subarray(SUBARRAY_PRODUCT4)
        self.client.assert_request_succeeds("capture-init", SUBARRAY_PRODUCT4)
        # check that the subarray is in an appropriate state
        sa = self.controller.subarray_products[SUBARRAY_PRODUCT4]
        self.assertFalse(sa._async_busy)
        self.assertEqual(State.INITIALISED, sa.state)
        # Check that the graph transitions succeeded
        katcp_client = self.sensor_proxy_client_class.return_value.katcp_client
        katcp_client.future_request.assert_has_calls([
            mock.call(Message.request('configure-subarray-from-telstate')),
            mock.call(Message.request('capture-meta', 'i0.baseline-correlation-products')),
            mock.call(Message.request('capture-init'), timeout=mock.ANY),
            mock.call(Message.request('capture-init'), timeout=mock.ANY),
            mock.call(Message.request('capture-start', 'i0.baseline-correlation-products'))
        ])

    def test_capture_init_failed_req(self):
        """Capture-init bumbles on even if a child request fails.

        TODO: that's probably not really the behaviour we want.
        """
        self._configure_subarray(SUBARRAY_PRODUCT4)
        katcp_client = self.sensor_proxy_client_class.return_value.katcp_client
        self.fail_requests.add('capture-init')
        self.client.assert_request_succeeds("capture-init", SUBARRAY_PRODUCT4)
        # check that the subarray is in an appropriate state
        sa = self.controller.subarray_products[SUBARRAY_PRODUCT4]
        self.assertEqual(State.INITIALISED, sa.state)

    def _test_capture_busy(self, command, *args):
        """Test that a command fails if issued while a ?capture-init is in progress"""
        self._configure_subarray(SUBARRAY_PRODUCT1)
        with self._capture_init_slow(SUBARRAY_PRODUCT1):
            self.client.assert_request_fails(command, *args)

    def test_capture_init_busy(self):
        """Capture-init fails if an asynchronous operation is already in progress"""
        self._test_capture_busy("capture-init", SUBARRAY_PRODUCT1)

    def test_capture_init_dead_process(self):
        """Capture-init bumbles on even if a child process is dead.

        TODO: that's probably not really the behaviour we want.
        """
        self.client.assert_request_succeeds("data-product-configure",SUBARRAY_PRODUCT4,ANTENNAS,"4096","2.1","0",STREAMS)
        sa = self.controller.subarray_products[SUBARRAY_PRODUCT4]
        for node in sa.graph.physical_graph:
            if node.logical_node.name == 'sdp.ingest.1':
                node.set_state(scheduler.TaskState.DEAD)
                node.status = Dict(state='TASK_FAILED')
                break
        else:
            raise ValueError('Could not find ingest node')
        self.client.assert_request_succeeds("capture-init", SUBARRAY_PRODUCT4)
        # check that the subarray is in an appropriate state
        self.assertEqual(State.INITIALISED, sa.state)

    def test_capture_done(self):
        """Checks that capture-done succeeds and sets appropriate state"""
        self._configure_subarray(SUBARRAY_PRODUCT4)
        self.client.assert_request_succeeds("capture-init", SUBARRAY_PRODUCT4)
        self.client.assert_request_succeeds("capture-done", SUBARRAY_PRODUCT4)
        # check that the subarray is in an appropriate state
        sa = self.controller.subarray_products[SUBARRAY_PRODUCT4]
        self.assertFalse(sa._async_busy)
        self.assertEqual(State.IDLE, sa.state)
        # Check that the graph transitions succeeded
        katcp_client = self.sensor_proxy_client_class.return_value.katcp_client
        katcp_client.future_request.assert_called_with(
            Message.request('capture-done'), timeout=mock.ANY)

    def test_capture_done_busy(self):
        """Capture-done fails if an asynchronous operation is already in progress"""
        self._test_capture_busy("capture-done", SUBARRAY_PRODUCT1)

    def _async_deconfigure_on_exit(self):
        """Call deconfigure_on_exit from the IOLoop"""
        @trollius.coroutine
        def shutdown(future):
            yield From(self.controller.deconfigure_on_exit())
            future.set_result(None)
        deconfigured_future = concurrent.futures.Future()
        self.controller.loop.call_soon_threadsafe(
            trollius.ensure_future, shutdown(deconfigured_future), self.controller.loop)
        deconfigured_future.result()

    def test_deconfigure_on_exit(self):
        """Calling deconfigure_on_exit will force-deconfigure existing
        subarrays, even if capturing."""
        self._configure_subarray(SUBARRAY_PRODUCT1)
        self.client.assert_request_succeeds('capture-init', SUBARRAY_PRODUCT1)
        self._async_deconfigure_on_exit()

        sensor_proxy_client = self.sensor_proxy_client_class.return_value
        sensor_proxy_client.katcp_client.future_request.assert_called_with(
            Message.request('capture-done'), timeout=mock.ANY)
        self.sched.kill.assert_called_with(mock.ANY)
        self.assertEqual({}, self.controller.subarray_products)
        self.assertEqual({}, self.controller.subarray_product_config)

    def test_deconfigure_on_exit_busy(self):
        """Calling deconfigure_on_exit while a capture-init or capture-done
        is busy kills off the graph anyway."""
        self._configure_subarray(SUBARRAY_PRODUCT1)
        with self._capture_init_slow(SUBARRAY_PRODUCT1):
            self._async_deconfigure_on_exit()
        self.sched.kill.assert_called_with(mock.ANY)
        self.assertEqual({}, self.controller.subarray_products)
        self.assertEqual({}, self.controller.subarray_product_config)

    def test_deconfigure_on_exit_cancel(self):
        """Calling deconfigure_on_exit while a configure is in process cancels
        that configure and kills off the graph."""
        with self._data_product_configure_slow(SUBARRAY_PRODUCT1, expect_ok=False):
            self._async_deconfigure_on_exit()
        # We must have killed off the partially-launched graph
        self.sched.kill.assert_called_with(mock.ANY)

    def test_telstate_endpoint_all(self):
        """Test telstate-endpoint without a subarray_product_id argument"""
        self._configure_subarray(SUBARRAY_PRODUCT1)
        self._configure_subarray(SUBARRAY_PRODUCT2)
        reply, informs = self.client.blocking_request(Message.request('telstate-endpoint'))
        self.assertTrue(reply.reply_ok())
        # Need to compare just arguments, because the message objects have message IDs
        informs = [tuple(msg.arguments) for msg in informs]
        self.assertEqual(AnyOrderList([
            (SUBARRAY_PRODUCT1, 'host.sdp.telstate:20000'),
            (SUBARRAY_PRODUCT2, 'host.sdp.telstate:20000')
        ]), informs)

    def test_telstate_endpoint_one(self):
        """Test telstate-endpoint with a subarray_product_id argument"""
        self._configure_subarray(SUBARRAY_PRODUCT1)
        reply, informs = self.client.blocking_request(Message.request('telstate-endpoint', SUBARRAY_PRODUCT1))
        self.assertEqual(reply.arguments, ['ok', 'host.sdp.telstate:20000'])

    def test_telstate_endpoint_not_found(self):
        """Test telstate-endpoint with a subarray_product_id that does not exist"""
        self.client.assert_request_fails('telstate-endpoint', SUBARRAY_PRODUCT2)

    def test_capture_status_all(self):
        """Test capture-status without a subarray_product_id argument"""
        self._configure_subarray(SUBARRAY_PRODUCT1)
        self._configure_subarray(SUBARRAY_PRODUCT2)
        self.client.assert_request_succeeds('capture-init', SUBARRAY_PRODUCT2)
        reply, informs = self.client.blocking_request(Message.request('capture-status'))
        self.assertEqual(reply.arguments, ['ok', '2'])
        # Need to compare just arguments, because the message objects have message IDs
        informs = [tuple(msg.arguments) for msg in informs]
        self.assertEqual(AnyOrderList([
            (SUBARRAY_PRODUCT1, 'IDLE'),
            (SUBARRAY_PRODUCT2, 'INITIALISED')
        ]), informs)

    def test_capture_status_one(self):
        """Test capture-status with a subarray_product_id argument"""
        self._configure_subarray(SUBARRAY_PRODUCT1)
        reply, informs = self.client.blocking_request(
            Message.request('capture-status', SUBARRAY_PRODUCT1))
        self.assertEqual(reply.arguments, ['ok', 'IDLE'])
        self.assertEqual([], informs)
        self.client.assert_request_succeeds('capture-init', SUBARRAY_PRODUCT1)
        reply, informs = self.client.blocking_request(
            Message.request('capture-status', SUBARRAY_PRODUCT1))
        self.assertEqual(reply.arguments, ['ok', 'INITIALISED'])
        self.client.assert_request_succeeds('capture-done', SUBARRAY_PRODUCT1)
        reply, informs = self.client.blocking_request(
            Message.request('capture-status', SUBARRAY_PRODUCT1))
        self.assertEqual(reply.arguments, ['ok', 'IDLE'])

    def test_capture_status_not_found(self):
        """Test capture_status with a subarray_product_id that does not exist"""
        self.client.assert_request_fails('capture-status', SUBARRAY_PRODUCT2)

    def test_sdp_shutdown(self):
        """Tests success path of sdp-shutdown"""
        self._configure_subarray(SUBARRAY_PRODUCT1)
        self.client.assert_request_succeeds('capture-init', SUBARRAY_PRODUCT1)
        self.sched.launch.reset_mock()
        self.client.assert_request_succeeds('sdp-shutdown')
        # Check that the subarray was stopped then shut down
        sensor_proxy_client = self.sensor_proxy_client_class.return_value
        sensor_proxy_client.katcp_client.future_request.assert_called_with(
            Message.request('capture-done'), timeout=mock.ANY)
        self.sched.kill.assert_called_with(mock.ANY)
        # Check that the shutdown was launched in two phases, non-masters
        # first.
        calls = self.sched.launch.call_args_list
        self.assertEqual(2, len(calls))
        nodes = calls[0][0][2]   # First call, positional args, 3rd argument
        hosts = [node.logical_node.host for node in nodes]
        # 10.0.0.1-10.0.0.4 are slaves, but 10.0.0.1 is master and 10.0.0.2 is us
        self.assertEqual(AnyOrderList(['10.0.0.3', '10.0.0.4']), hosts)

    def test_sdp_shutdown_slaves_error(self):
        """Test sdp-shutdown when get_master_and_slaves fails"""
        future = trollius.Future(loop=self.loop)
        future.set_exception(requests.exceptions.Timeout())
        self.sched.get_master_and_slaves.return_value = future
        self.client.assert_request_fails('sdp-shutdown')


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
