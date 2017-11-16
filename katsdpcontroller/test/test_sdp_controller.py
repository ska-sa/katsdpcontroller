"""Tests for the sdp controller module."""

import time
import threading
import contextlib
import concurrent.futures
import unittest2 as unittest
import mock
import json

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
import networkx
import netifaces
import requests

from katsdpcontroller.sdpcontroller import (
    SDPControllerServer, SDPCommonResources, SDPResources, State)
from katsdpcontroller import scheduler
from katsdpcontroller.test.test_scheduler import AnyOrderList

ANTENNAS = 'm000,m001,m063,m064'

PRODUCT = 'bc856M4k'
SUBARRAY_PRODUCT1 = 'array_1_' + PRODUCT
SUBARRAY_PRODUCT2 = 'array_2_' + PRODUCT
SUBARRAY_PRODUCT3 = 'array_3_' + PRODUCT
SUBARRAY_PRODUCT4 = 'array_4_' + PRODUCT

STREAMS = '''{
    "cam.http": {"camdata": "http://127.0.0.1:8999"},
    "cbf.baseline_correlation_products": {
        "i0.baseline-correlation-products": "spead://239.102.255.0+15:7148"
    },
    "cbf.antenna_channelised_voltage": {
        "i0.antenna-channelised-voltage": "spead://239.102.252.0+15:7148"
    },
    "cbf.tied_array_channelised_voltage": {
        "i0.tied-array-channelised-voltage-0x": "spead://239.102.254.1+15:7148",
        "i0.tied-array-channelised-voltage-0y": "spead://239.102.253.1+15:7148"
    }
}'''

CONFIG = '''{
    "version": "1.0",
    "inputs": {
        "camdata": {
            "type": "cam.http",
            "url": "http://127.0.0.1:8999"
        },
        "i0_antenna_channelised_voltage": {
            "type": "cbf.antenna_channelised_voltage",
            "url": "spead://239.102.252.0+15:7148",
            "antennas": ["m000", "m001", "m063", "m064"],
            "n_chans": 4096,
            "n_pols": 2,
            "adc_sample_rate": 1712000000.0,
            "bandwidth": 856000000.0,
            "n_samples_between_spectra": 8192,
            "instrument_dev_name": "i0"
        },
        "i0_baseline_correlation_products": {
            "type": "cbf.baseline_correlation_products",
            "url": "spead://239.102.255.0+15:7148",
            "src_streams": ["i0_antenna_channelised_voltage"],
            "int_time": 0.499,
            "n_bls": 40,
            "xeng_out_bits_per_sample": 32,
            "n_chans_per_substream": 256,
            "instrument_dev_name": "i0",
            "simulate": true
        },
        "i0_tied_array_channelised_voltage_0x": {
            "type": "cbf.tied_array_channelised_voltage",
            "url": "spead://239.102.254.1+15:7148",
            "src_streams": ["i0_antenna_channelised_voltage"],
            "spectra_per_heap": 256,
            "n_chans_per_substream": 256,
            "beng_out_bits_per_sample": 8,
            "instrument_dev_name": "i0"
        },
        "i0_tied_array_channelised_voltage_0y": {
            "type": "cbf.tied_array_channelised_voltage",
            "url": "spead://239.102.253.1+15:7148",
            "src_streams": ["i0_antenna_channelised_voltage"],
            "spectra_per_heap": 256,
            "n_chans_per_substream": 256,
            "beng_out_bits_per_sample": 8,
            "instrument_dev_name": "i0"
        }
    },
    "outputs": {
        "sdp_l0": {
            "type": "sdp.l0",
            "src_streams": ["i0_baseline_correlation_products"],
            "output_int_time": 4.0,
            "continuum_factor": 1
        },
        "sdp_l0_continuum": {
            "type": "sdp.l0",
            "src_streams": ["i0_baseline_correlation_products"],
            "output_int_time": 4.0,
            "continuum_factor": 16
        },
        "sdp_l0_spectral_only": {
            "type": "sdp.l0",
            "src_streams": ["i0_baseline_correlation_products"],
            "output_int_time": 1.9,
            "continuum_factor": 1
        },
        "sdp_l0_continuum_only": {
            "type": "sdp.l0",
            "src_streams": ["i0_baseline_correlation_products"],
            "output_int_time": 2.1,
            "continuum_factor": 16
        },
        "sdp_beamformer": {
            "type": "sdp.beamformer",
            "src_streams": [
                "i0_tied_array_channelised_voltage_0x",
                "i0_tied_array_channelised_voltage_0y"
            ]
        },
        "sdp_beamformer_engineering_ssd": {
            "type": "sdp.beamformer_engineering",
            "src_streams": [
                "i0_tied_array_channelised_voltage_0x",
                "i0_tied_array_channelised_voltage_0y"
            ],
            "output_channels": [0, 4096],
            "store": "ssd"
        },
        "sdp_beamformer_engineering_ram": {
            "type": "sdp.beamformer_engineering",
            "src_streams": [
                "i0_tied_array_channelised_voltage_0x",
                "i0_tied_array_channelised_voltage_0y"
            ],
            "output_channels": [0, 4096],
            "store": "ram"
        }
    },
    "config": {}
}'''

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
        ('bf_ingest.beamformer.1.port', '', '', 'address'),
        ('filewriter.sdp_l0.1.filename', '', '', 'string'),
        ('ingest.sdp_l0.1.capture-active', '', '', 'boolean'),
        ('timeplot.sdp_l0.1.gui-urls', '', '', 'string'),
        ('timeplot.sdp_l0.1.html_port', '', '', 'address')
))

EXPECTED_REQUEST_LIST = [
    'data-product-configure',
    'data-product-reconfigure',
    'product-configure',
    'product-deconfigure',
    'product-reconfigure',
    'product-list',
    'sdp-status',
    'capture-done',
    'capture-init',
    'capture-status',
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

    def test_capture_init(self):
        self.client.assert_request_fails("capture-init", SUBARRAY_PRODUCT1)
        self.client.assert_request_succeeds("product-configure",SUBARRAY_PRODUCT1,CONFIG)
        self.client.assert_request_succeeds("capture-init",SUBARRAY_PRODUCT1)

        reply, informs = self.client.blocking_request(Message.request("capture-status",SUBARRAY_PRODUCT1))
        self.assertEqual(repr(reply),repr(Message.reply("capture-status","ok","CAPTURING")))
        self.client.assert_request_fails("capture-init", SUBARRAY_PRODUCT1)

    def test_interface_sensors(self):
        self.client.test_sensor_list(EXPECTED_SENSOR_LIST, ignore_descriptions=True)
        recorder = self.client.message_recorder(('interface-changed',))
        self.client.assert_request_succeeds("product-configure", SUBARRAY_PRODUCT1, CONFIG)
        self.assertEqual([str(m) for m in recorder()], ['#interface-changed sensor-list'])
        self.client.test_sensor_list(
            EXPECTED_SENSOR_LIST + EXPECTED_INTERFACE_SENSOR_LIST_1,
            ignore_descriptions=True)
        # Deconfigure and check that the array sensors are gone
        self.client.assert_request_succeeds(
            "product-deconfigure", SUBARRAY_PRODUCT1)
        self.assertEqual([str(m) for m in recorder()], ['#interface-changed sensor-list'])
        self.client.test_sensor_list(
            EXPECTED_SENSOR_LIST, ignore_descriptions=True)

    def test_capture_done(self):
        self.client.assert_request_fails("capture-done",SUBARRAY_PRODUCT2)
        self.client.assert_request_succeeds("product-configure",SUBARRAY_PRODUCT2,CONFIG)
        self.client.assert_request_fails("capture-done",SUBARRAY_PRODUCT2)

        self.client.assert_request_succeeds("capture-init",SUBARRAY_PRODUCT2)
        self.client.assert_request_succeeds("capture-done",SUBARRAY_PRODUCT2)
        self.client.assert_request_fails("capture-done",SUBARRAY_PRODUCT2)

    def test_deconfigure_subarray_product_legacy(self):
        self.client.assert_request_fails("data-product-configure",SUBARRAY_PRODUCT3,"")
        self.client.assert_request_succeeds("data-product-configure",SUBARRAY_PRODUCT3,ANTENNAS,"4096","2.1","0",STREAMS)
        self.client.assert_request_succeeds("capture-init",SUBARRAY_PRODUCT3)
        self.client.assert_request_fails("data-product-configure",SUBARRAY_PRODUCT3,"")
         # should not be able to deconfigure when not in idle state
        self.client.assert_request_succeeds("capture-done",SUBARRAY_PRODUCT3)
        self.client.assert_request_succeeds("data-product-configure",SUBARRAY_PRODUCT3,"")

    def test_deconfigure_subarray_product(self):
        self.client.assert_request_fails("product-configure",SUBARRAY_PRODUCT3)
        self.client.assert_request_succeeds("product-configure",SUBARRAY_PRODUCT3,CONFIG)
        self.client.assert_request_succeeds("capture-init",SUBARRAY_PRODUCT3)
        self.client.assert_request_fails("product-configure",SUBARRAY_PRODUCT3)
         # should not be able to deconfigure when not in idle state
        self.client.assert_request_succeeds("capture-done",SUBARRAY_PRODUCT3)
        self.client.assert_request_succeeds("product-list",SUBARRAY_PRODUCT3)

    def test_configure_subarray_product_legacy(self):
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

    def test_configure_subarray_product(self):
        self.client.assert_request_fails("product-deconfigure",SUBARRAY_PRODUCT4)
        self.client.assert_request_succeeds("product-list")
        self.client.assert_request_succeeds("product-configure",SUBARRAY_PRODUCT4,CONFIG)
        # Same config again is okay
        self.client.assert_request_succeeds("product-configure",SUBARRAY_PRODUCT4,CONFIG)
        # Changing the config without deconfiguring is not okay
        config2 = json.loads(CONFIG)
        config2['outputs']['sdp_l0_continuum']['continuum_factor'] = 8
        config2 = json.dumps(config2)
        self.client.assert_request_fails("product-configure",SUBARRAY_PRODUCT4,config2)
        self.client.assert_request_succeeds("product-deconfigure",SUBARRAY_PRODUCT4)
        # Check that config2 is valid - otherwise the above test is testing the wrong thing
        self.client.assert_request_succeeds("product-configure",SUBARRAY_PRODUCT4,config2)

        reply, informs = self.client.blocking_request(Message.request("product-list"))
        self.assertEqual(repr(reply),repr(Message.reply("product-list","ok",1)))

        self.client.assert_request_succeeds("product-deconfigure",SUBARRAY_PRODUCT4)
        self.client.assert_request_fails("product-list",SUBARRAY_PRODUCT4)

    def test_reconfigure_subarray_product_legacy(self):
        self.client.assert_request_fails("data-product-reconfigure", SUBARRAY_PRODUCT4)
        self.client.assert_request_succeeds("data-product-configure",SUBARRAY_PRODUCT4,ANTENNAS,"4096","2.1","0",STREAMS)
        self.client.assert_request_succeeds("data-product-reconfigure", SUBARRAY_PRODUCT4)
        self.client.assert_request_succeeds("capture-init", SUBARRAY_PRODUCT4)
        self.client.assert_request_fails("data-product-reconfigure", SUBARRAY_PRODUCT4)

    def test_reconfigure_subarray_product(self):
        self.client.assert_request_fails("product-reconfigure", SUBARRAY_PRODUCT4)
        self.client.assert_request_succeeds("product-configure",SUBARRAY_PRODUCT4,CONFIG)
        self.client.assert_request_succeeds("product-reconfigure", SUBARRAY_PRODUCT4)
        self.client.assert_request_succeeds("capture-init", SUBARRAY_PRODUCT4)
        self.client.assert_request_fails("product-reconfigure", SUBARRAY_PRODUCT4)

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
        old_side_effect = mock.side_effect
        def side_effect(*args, **kwargs):
            started.set_result(None)
            mock.side_effect = old_side_effect
            return result
        def release(value):
            self.controller.loop.call_soon_threadsafe(result.set_result, value)
        mock.side_effect = side_effect
        return started, release

    @contextlib.contextmanager
    def _slow(self, request, mock, return_value, cancelled):
        """Context manager that runs its block with a message in progress.

        The `mock` is modified to return a future that only resolves to
        `return_value` after the with block has completed (the first time it is
        called).

        If `cancelled` is true, the request is expected to fail with a message
        about being cancelled, otherwise it is expected to succeed.
        """
        # Set when the capture-init is blocked
        reply_future = concurrent.futures.Future()
        started_future, release = self._delay_mock(mock)
        self.client.callback_request(request, reply_cb=lambda msg: reply_future.set_result(msg))
        # Wait until the first command gets blocked
        started_future.result()
        # Do the test
        try:
            yield
        finally:
            # Unblock things
            release(return_value)
        reply = reply_future.result()
        if cancelled:
            self.assertFalse(reply.reply_ok())
            self.assertEqual('request was cancelled', reply.arguments[1])
        else:
            self.assertTrue(reply.reply_ok())

    def _capture_init_slow(self, subarray_product, cancelled=False):
        """Context manager that runs its block with a capture-init in
        progress. The subarray product must already be configured.

        If `cancelled` is true, the capture-init is expected to have been
        cancelled, otherwise it is expected to succeed.
        """
        sensor_proxy_client = self.sensor_proxy_client_class.return_value
        return self._slow(
            Message.request('capture-init', subarray_product),
            sensor_proxy_client.katcp_client.future_request,
            (Message.reply('capture-init', 'ok'), []),
            cancelled)

    def _data_product_configure_slow(self, subarray_product, cancelled=False):
        """Context manager that runs its block with a product-configure in
        progress."""
        sensor_proxy_client = self.sensor_proxy_client_class.return_value
        return self._slow(
            Message.request(*self._configure_args(subarray_product)),
            sensor_proxy_client.until_synced,
            None, cancelled)

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
        self.sched.http_url = 'http://scheduler:8080/'
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
                if hasattr(node.logical_node, 'cores'):
                    core_num = 0
                    for core in node.logical_node.cores:
                        if core is not None:
                            node.cores[core] = core_num
                            core_num += 1
                if isinstance(node.logical_node, scheduler.LogicalTask):
                    node.allocation = mock.MagicMock()
                    node.allocation.agent.host = 'host.' + node.logical_node.name
                    node.allocation.agent.gpus[0].name = 'GeForce GTX TITAN X'
                    for request in node.logical_node.interfaces:
                        interface = mock.Mock()
                        interface.name = 'em1'
                        node.interfaces[request.network] = interface
        order_graph = scheduler.subgraph(graph, scheduler.DEPENDS_RESOLVE, nodes)
        for node in reversed(list(networkx.topological_sort(order_graph))):
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
        return ("product-configure", subarray_product, CONFIG)

    def _configure_subarray(self, subarray_product):
        return self.client.assert_request_succeeds(*self._configure_args(subarray_product))

    def test_product_configure_success(self):
        """A ?product-configure request must wait for the tasks to come up, then indicate success."""
        self.client.assert_request_succeeds(
            'set-config-override', SUBARRAY_PRODUCT4,
            '''
            {
                "config": {
                    "develop": true,
                    "service_overrides": {
                        "filewriter.sdp_l0": {
                            "config": {
                                "override_test": "value"
                            }
                        }
                    }
                }
            }''')
        name = self._configure_subarray(SUBARRAY_PRODUCT4)[0]
        self.assertEqual(SUBARRAY_PRODUCT4, name)
        self.telstate_class.assert_called_once_with('host.telstate:20000')

        # Verify the telescope state
        ts = self.telstate_class.return_value
        # Print the list to assist in debugging if the assert fails
        print(ts.add.call_args_list)
        # This is not a complete list of calls. It check that each category of stuff
        # is covered: base_params, per node, per edge
        ts.add.assert_any_call('subarray_product_id', SUBARRAY_PRODUCT4, immutable=True)
        ts.add.assert_any_call('cal_bp_solint', 10, immutable=True)
        ts.add.assert_any_call('config.filewriter.sdp_l0', {
            'file_base': '/var/kat/data',
            'port': 20000,
            'l0_spead': mock.ANY,
            'l0_interface': 'em1',
            'l0_name': 'sdp_l0',
            'override_test': 'value'
        }, immutable=True)

        # Verify the state of the subarray
        self.assertEqual({}, self.controller.override_dicts)
        sa = self.controller.subarray_products[SUBARRAY_PRODUCT4]
        self.assertFalse(sa.async_busy)
        self.assertEqual(State.IDLE, sa.state)

    def test_product_configure_generate_names(self):
        """Name with trailing * must generate lowest-numbered name"""
        name = self._configure_subarray('prefix_*')[0]
        self.assertEqual('prefix_0', name)
        name = self._configure_subarray('prefix_*')[0]
        self.assertEqual('prefix_1', name)
        self.client.assert_request_succeeds('product-deconfigure', 'prefix_0')
        name = self._configure_subarray('prefix_*')[0]
        self.assertEqual('prefix_0', name)

    def test_product_configure_telstate_fail(self):
        """If the telstate task fails, product-configure must fail"""
        self.fail_launches['telstate'] = 'TASK_FAILED'
        self.telstate_class.side_effect = redis.ConnectionError
        self.client.assert_request_fails(*self._configure_args(SUBARRAY_PRODUCT4))
        self.sched.launch.assert_called_with(mock.ANY, mock.ANY, mock.ANY)
        self.sched.kill.assert_called_with(mock.ANY)
        # Must not have created the subarray product internally
        self.assertEqual({}, self.controller.subarray_products)

    def test_product_configure_task_fail(self):
        """If a task other than telstate fails, product-configure must fail"""
        self.fail_launches['ingest.sdp_l0.1'] = 'TASK_FAILED'
        self.client.assert_request_fails(*self._configure_args(SUBARRAY_PRODUCT4))
        self.telstate_class.assert_called_once_with('host.telstate:20000')
        self.sched.launch.assert_called_with(mock.ANY, mock.ANY)
        self.sched.kill.assert_called_with(mock.ANY)
        # Must have cleaned up the subarray product internally
        self.assertEqual({}, self.controller.subarray_products)

    def test_product_configure_parallel(self):
        """Can configure two subarray products at the same time"""
        with self._data_product_configure_slow(SUBARRAY_PRODUCT1):
            # We use a different subarray product
            self.client.assert_request_succeeds(*self._configure_args(SUBARRAY_PRODUCT2))
        # Check that both products are in the right state
        self.assertIn(SUBARRAY_PRODUCT1, self.controller.subarray_products)
        self.assertEqual(State.IDLE, self.controller.subarray_products[SUBARRAY_PRODUCT1].state)
        self.assertIn(SUBARRAY_PRODUCT2, self.controller.subarray_products)
        self.assertEqual(State.IDLE, self.controller.subarray_products[SUBARRAY_PRODUCT2].state)

    def test_product_deconfigure(self):
        """Checks success path of product-deconfigure"""
        self._configure_subarray(SUBARRAY_PRODUCT1)
        self.client.assert_request_succeeds("product-deconfigure", SUBARRAY_PRODUCT1)
        # Check that the graph was shut down
        self.sched.kill.assert_called_with(mock.ANY)
        # Verify the state
        self.assertEqual({}, self.controller.subarray_products)

    def test_product_deconfigure_capturing(self):
        """product-deconfigure must fail while capturing"""
        self._configure_subarray(SUBARRAY_PRODUCT1)
        self.client.assert_request_succeeds("capture-init", SUBARRAY_PRODUCT1)
        self.client.assert_request_fails("product-deconfigure", SUBARRAY_PRODUCT1)

    def test_product_deconfigure_capturing_force(self):
        """forced product-deconfigure must succeed while capturing"""
        self._configure_subarray(SUBARRAY_PRODUCT1)
        self.client.assert_request_succeeds("capture-init", SUBARRAY_PRODUCT1)
        self.client.assert_request_succeeds("product-deconfigure", SUBARRAY_PRODUCT1, '1')

    def test_product_deconfigure_busy(self):
        """product-deconfigure cannot happen concurrently with capture-init"""
        self._configure_subarray(SUBARRAY_PRODUCT1)
        with self._capture_init_slow(SUBARRAY_PRODUCT1):
            self.client.assert_request_fails('product-deconfigure', SUBARRAY_PRODUCT1, '0')
        # Check that the subarray still exists and has the right state
        sa = self.controller.subarray_products[SUBARRAY_PRODUCT1]
        self.assertFalse(sa.async_busy)
        self.assertEqual(State.CAPTURING, sa.state)

    def test_product_deconfigure_busy_force(self):
        """forced product-deconfigure must succeed while in capture-init"""
        self._configure_subarray(SUBARRAY_PRODUCT1)
        with self._capture_init_slow(SUBARRAY_PRODUCT1, cancelled=True):
            self.client.assert_request_succeeds("product-deconfigure", SUBARRAY_PRODUCT1, '1')
        # Check that the graph was shut down
        self.sched.kill.assert_called_with(mock.ANY)
        # Verify the state
        self.assertEqual({}, self.controller.subarray_products)

    def test_product_deconfigure_while_configuring_force(self):
        """forced product-deconfigure must succeed while in product-configure"""
        with self._data_product_configure_slow(SUBARRAY_PRODUCT1, cancelled=True):
            self.client.assert_request_succeeds("product-deconfigure", SUBARRAY_PRODUCT1, '1')
        # Check that the graph was shut down
        self.sched.kill.assert_called_with(mock.ANY)
        # Verify the state
        self.assertEqual({}, self.controller.subarray_products)

    def test_product_reconfigure(self):
        """Checks success path of product_reconfigure"""
        self._configure_subarray(SUBARRAY_PRODUCT1)
        self.sched.launch.reset_mock()
        self.sched.kill.reset_mock()

        self.client.assert_request_succeeds(
            'set-config-override', SUBARRAY_PRODUCT1,
            '''
            {
                "inputs": {
                    "i0_baseline_correlation_products": {
                        "simulate": true
                    }
                },
                "config": {
                    "service_overrides": {
                        "sim.i0_baseline_correlation_products": {
                            "taskinfo": {
                                "command": {
                                    "shell": true
                                }
                            }
                        }
                    }
                }
            }''')
        self.client.assert_request_succeeds('product-reconfigure', SUBARRAY_PRODUCT1)
        # Check that the graph was killed and restarted
        self.sched.kill.assert_called_with(mock.ANY)
        self.sched.launch.assert_called_with(mock.ANY, mock.ANY)
        # Check that the override took effect
        ts = self.telstate_class.return_value
        print(ts.add.call_args_list)
        ts.add.assert_any_call('config.sim.i0_baseline_correlation_products', {
            'antenna_mask': ANTENNAS,
            'cbf_ibv': True,
            'cbf_channels': 4096,
            'cbf_substreams': 16,
            'cbf_adc_sample_rate': 1712000000.0,
            'cbf_bandwidth': 856000000.0,
            'cbf_int_time': 0.499,
            'cbf_interface': 'em1',
            'port': 20000,
            'cbf_spead': '239.102.255.0+15:7148'
        }, immutable=True)
        # Check that the taskinfo override worked
        ts.add.assert_any_call('sdp_task_details', mock.ANY, immutable=True)
        for call in ts.add.call_args_list:
            if call[0][0] == 'sdp_task_details':
                task_details = call[0][1]
        self.assertTrue(task_details['sim.i0_baseline_correlation_products']['taskinfo']['command']['shell'])

    def test_product_reconfigure_configure_busy(self):
        """Can run product-reconfigure concurrently with another product-configure"""
        self._configure_subarray(SUBARRAY_PRODUCT1)
        with self._data_product_configure_slow(SUBARRAY_PRODUCT2):
            self.client.assert_request_succeeds('product-reconfigure', SUBARRAY_PRODUCT1)

    def test_product_reconfigure_configure_fails(self):
        """Tests product-reconfigure when the new graph fails"""
        self._configure_subarray(SUBARRAY_PRODUCT1)
        self.fail_launches['telstate'] = 'TASK_FAILED'
        self.client.assert_request_fails('product-reconfigure', SUBARRAY_PRODUCT1)
        # Check that the subarray was deconfigured cleanly
        self.assertEqual({}, self.controller.subarray_products)

    # TODO: test that reconfigure with override dict picks up the override dict

    def test_capture_init(self):
        """Checks that capture-init succeeds and sets appropriate state"""
        self._configure_subarray(SUBARRAY_PRODUCT4)
        self.client.assert_request_succeeds("capture-init", SUBARRAY_PRODUCT4)
        # check that the subarray is in an appropriate state
        sa = self.controller.subarray_products[SUBARRAY_PRODUCT4]
        self.assertFalse(sa.async_busy)
        self.assertEqual(State.CAPTURING, sa.state)
        # Check that the graph transitions succeeded
        katcp_client = self.sensor_proxy_client_class.return_value.katcp_client
        expected_calls = []
        expected_calls.append(mock.call(Message.request('configure-subarray-from-telstate')))
        # 12x ingest, 2x filewriter, beamformer, 4x engineering beamformer, 2x cal
        expected_calls.extend([
            mock.call(Message.request('capture-init'), timeout=mock.ANY)] * 21)
        expected_calls.append(mock.call(Message.request(
            'capture-start', 'i0_baseline_correlation_products'), timeout=mock.ANY))
        katcp_client.future_request.assert_has_calls(expected_calls)

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
        self.assertEqual(State.CAPTURING, sa.state)

    def _test_busy(self, command, *args):
        """Test that a command fails if issued while ?capture-init or ?product-configure is in progress"""
        with self._data_product_configure_slow(SUBARRAY_PRODUCT1):
            self.client.assert_request_fails(command, *args)
        with self._capture_init_slow(SUBARRAY_PRODUCT1):
            self.client.assert_request_fails(command, *args)

    def test_capture_init_busy(self):
        """Capture-init fails if an asynchronous operation is already in progress"""
        self._test_busy("capture-init", SUBARRAY_PRODUCT1)

    def test_capture_init_dead_process(self):
        """Capture-init bumbles on even if a child process is dead.

        TODO: that's probably not really the behaviour we want.
        """
        self.client.assert_request_succeeds("product-configure",SUBARRAY_PRODUCT4,CONFIG)
        sa = self.controller.subarray_products[SUBARRAY_PRODUCT4]
        for node in sa.physical_graph:
            if node.logical_node.name == 'ingest.sdp_l0.1':
                node.set_state(scheduler.TaskState.DEAD)
                node.status = Dict(state='TASK_FAILED')
                break
        else:
            raise ValueError('Could not find ingest node')
        self.client.assert_request_succeeds("capture-init", SUBARRAY_PRODUCT4)
        # check that the subarray is in an appropriate state
        self.assertEqual(State.CAPTURING, sa.state)

    def test_capture_done(self):
        """Checks that capture-done succeeds and sets appropriate state"""
        self._configure_subarray(SUBARRAY_PRODUCT4)
        self.client.assert_request_succeeds("capture-init", SUBARRAY_PRODUCT4)
        self.client.assert_request_succeeds("capture-done", SUBARRAY_PRODUCT4)
        # check that the subarray is in an appropriate state
        sa = self.controller.subarray_products[SUBARRAY_PRODUCT4]
        self.assertFalse(sa.async_busy)
        self.assertEqual(State.IDLE, sa.state)
        # Check that the graph transitions succeeded
        katcp_client = self.sensor_proxy_client_class.return_value.katcp_client
        katcp_client.future_request.assert_called_with(
            Message.request('capture-done'), timeout=mock.ANY)

    def test_capture_done_busy(self):
        """Capture-done fails if an asynchronous operation is already in progress"""
        self._test_busy("capture-done", SUBARRAY_PRODUCT1)

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

    def test_deconfigure_on_exit_busy(self):
        """Calling deconfigure_on_exit while a capture-init or capture-done
        is busy kills off the graph anyway."""
        self._configure_subarray(SUBARRAY_PRODUCT1)
        with self._capture_init_slow(SUBARRAY_PRODUCT1, cancelled=True):
            self._async_deconfigure_on_exit()
        self.sched.kill.assert_called_with(mock.ANY)
        self.assertEqual({}, self.controller.subarray_products)

    def test_deconfigure_on_exit_cancel(self):
        """Calling deconfigure_on_exit while a configure is in process cancels
        that configure and kills off the graph."""
        with self._data_product_configure_slow(SUBARRAY_PRODUCT1, cancelled=True):
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
            (SUBARRAY_PRODUCT1, 'host.telstate:20000'),
            (SUBARRAY_PRODUCT2, 'host.telstate:20000')
        ]), informs)

    def test_telstate_endpoint_one(self):
        """Test telstate-endpoint with a subarray_product_id argument"""
        self._configure_subarray(SUBARRAY_PRODUCT1)
        reply, informs = self.client.blocking_request(Message.request('telstate-endpoint', SUBARRAY_PRODUCT1))
        self.assertEqual(reply.arguments, ['ok', 'host.telstate:20000'])

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
            (SUBARRAY_PRODUCT2, 'CAPTURING')
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
        self.assertEqual(reply.arguments, ['ok', 'CAPTURING'])
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
        self.r = SDPCommonResources("225.100.0.0/23")
        self.r1 = SDPResources(self.r, 'array_1_c856M4k')
        self.r2 = SDPResources(self.r, 'array_1_bc856M4k')

    def test_multicast_ip(self):
        """Get assigned IP's from different subarrays"""
        self.assertEqual('225.100.0.1+3', self.r1.get_multicast_ip(4))
        self.assertEqual('225.100.1.1', self.r2.get_multicast_ip(1))
        self.assertEqual('225.100.0.5+7', self.r1.get_multicast_ip(8))

    def test_multicast_ip_exhausted(self):
        """Error raised when a single subnet is used up"""
        self.r1.get_multicast_ip(4)
        with self.assertRaises(RuntimeError):
            self.r1.get_multicast_ip(255)

    def test_multicast_ip_exhausted_subnets(self):
        """Error raised when too many subnets are requested"""
        with self.assertRaises(RuntimeError):
            SDPResources(self.r, 'array_2_c856M4k')

    def test_recycle_subnets(self):
        """Closing a subnet makes it available again"""
        self.r1.close()
        r3 = SDPResources(self.r, 'array_3_c856M4k')
        self.assertEqual('225.100.0.1+3', r3.get_multicast_ip(4))
