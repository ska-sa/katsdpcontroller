"""Tests for the sdp controller module."""

import unittest
from unittest import mock
import json
import itertools
import asyncio
# Needs to be imported this way so that it is unaffected by mocking of socket.getaddrinfo
from socket import getaddrinfo

import asynctest
from nose.tools import assert_raises, assert_equal
from addict import Dict
import aiokatcp
from aiokatcp import Message, FailReply, Sensor
from aiokatcp.test.test_utils import timelimit
import pymesos
import networkx
import netifaces
import open_file_mock
import katsdptelstate
import katpoint

from katsdpcontroller.sdpcontroller import (
    SDPControllerServer, SDPCommonResources, SDPResources, ProductState, DeviceStatus,
    _capture_block_names, _redact_keys)
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
    "version": "2.4",
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
            "simulate": {
                "center_freq": 1284000000.0,
                "sources": ["PKS 1934-638, radec, 19:39:25.03, -63:42:45.63"]
            }
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
            "type": "sdp.vis",
            "src_streams": ["i0_baseline_correlation_products"],
            "output_int_time": 4.0,
            "continuum_factor": 1,
            "archive": true
        },
        "sdp_l0_continuum": {
            "type": "sdp.vis",
            "src_streams": ["i0_baseline_correlation_products"],
            "output_int_time": 4.0,
            "continuum_factor": 16,
            "archive": true
        },
        "sdp_l0_spectral_only": {
            "type": "sdp.vis",
            "src_streams": ["i0_baseline_correlation_products"],
            "output_int_time": 1.9,
            "continuum_factor": 1,
            "archive": true
        },
        "sdp_l0_continuum_only": {
            "type": "sdp.vis",
            "src_streams": ["i0_baseline_correlation_products"],
            "output_int_time": 2.1,
            "continuum_factor": 16,
            "output_channels": [117, 3472],
            "archive": true
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
        },
        "cal": {
            "type": "sdp.cal",
            "src_streams": ["sdp_l0"],
            "buffer_time": 1800.0
        },
        "sdp_l1_flags": {
            "type": "sdp.flags",
            "src_streams": ["sdp_l0"],
            "calibration": ["cal"],
            "archive": true
        },
        "sdp_l1_flags_continuum": {
            "type": "sdp.flags",
            "src_streams": ["sdp_l0_continuum"],
            "calibration": ["cal"],
            "archive": true
        },
        "continuum_image": {
            "type": "sdp.continuum_image",
            "src_streams": ["sdp_l1_flags_continuum"]
        },
        "spectral_image": {
            "type": "sdp.spectral_image",
            "src_streams": ["sdp_l1_flags"],
            "output_channels": [60, 70]
        }
    },
    "config": {}
}'''

EXPECTED_SENSOR_LIST = (
    (b'api-version', b'', b'string'),
    (b'build-state', b'', b'string'),
    (b'device-status', b'', b'discrete', b'ok', b'degraded', b'fail'),
    (b'fmeca.FD0001', b'', b'boolean'),
    (b'time-synchronised', b'', b'boolean'),
    (b'gui-urls', b'', b'string'),
    (b'products', b'', b'string')
)

EXPECTED_INTERFACE_SENSOR_LIST_1 = tuple(
    (SUBARRAY_PRODUCT1.encode('ascii') + b'.' + s[0],) + s[1:] for s in (
        (b'bf_ingest.beamformer.1.port', b'', b'address'),
        (b'ingest.sdp_l0.1.capture-active', b'', b'boolean'),
        (b'timeplot.sdp_l0.1.gui-urls', b'', b'string'),
        (b'timeplot.sdp_l0.1.html_port', b'', b'address'),
        (b'cal.1.capture-block-state', b'', b'string'),
        (b'state', b'', b'discrete',
         b'configuring', b'idle', b'capturing', b'deconfiguring', b'dead', b'error'),
        (b'capture-block-state', b'', b'string')
    ))

EXPECTED_REQUEST_LIST = [
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
    'set-config-override',
    # Standard katcp commands
    'client-list', 'halt', 'help', 'log-level', 'sensor-list',
    'sensor-sampling', 'sensor-value', 'watchdog', 'version-list'
]


def get_metric(metric):
    """Get the current value of a Prometheus metric"""
    return metric.collect()[0].samples[0][2]


class TestRedactKeys(unittest.TestCase):
    def setUp(self):
        self.s3_config = {
            'archive': {
                'read': {
                    'access_key': 'ACCESS_KEY',
                    'secret_key': 'tellno1'
                },
                'write': {
                    'access_key': 's3cr3t',
                    'secret_key': 'mores3cr3t'
                },
                'url': 'http://invalid/'
            }
        }

    def test_no_command(self):
        taskinfo = Dict()
        result = _redact_keys(taskinfo, self.s3_config)
        self.assertEqual(result, taskinfo)

    def run_it(self, arguments):
        taskinfo = Dict()
        taskinfo.command.arguments = arguments
        result = _redact_keys(taskinfo, self.s3_config)
        return result.command.arguments

    def test_with_equals(self):
        result = self.run_it(['--secret=mores3cr3t', '--key=ACCESS_KEY', '--other=safe'])
        self.assertEqual(result, ['--secret=REDACTED', '--key=REDACTED', '--other=safe'])

    def test_without_equals(self):
        result = self.run_it(['--secret', 's3cr3t', '--key', 'tellno1', '--other', 'safe'])
        self.assertEqual(result, ['--secret', 'REDACTED', '--key', 'REDACTED', '--other', 'safe'])


class BaseTestSDPController(asynctest.TestCase):
    """Utilities for test classes"""
    async def setup_server(self, *server_args, **server_kwargs):
        self.server = SDPControllerServer(*server_args, **server_kwargs)
        await self.server.start()
        self.addCleanup(self.server.stop)
        bind_address = self.server.server.sockets[0].getsockname()
        self.client = await aiokatcp.Client.connect(bind_address[0], bind_address[1])
        self.addCleanup(self.client.wait_closed)
        self.addCleanup(self.client.close)

    async def assert_request_fails(self, name, *args):
        with self.assertRaises(aiokatcp.FailReply):
            await self.client.request(name, *args)

    async def assert_sensors(self, expected_list):
        expected = {item[0]: item[1:] for item in expected_list}
        reply, informs = await self.client.request("sensor-list")
        actual = {}
        for inform in informs:
            # Skip the description
            actual[inform.arguments[0]] = tuple(inform.arguments[2:])
        self.assertEqual(expected, actual)

    async def assert_sensor_value(self, name, value):
        encoded = aiokatcp.encode(value)
        reply, informs = await self.client.request("sensor-value", name)
        self.assertEqual(informs[0].arguments[4], encoded)

    def create_patch(self, *args, **kwargs):
        patcher = mock.patch(*args, **kwargs)
        mock_obj = patcher.start()
        self.addCleanup(patcher.stop)
        return mock_obj


@timelimit
class TestSDPControllerInterface(BaseTestSDPController):
    """Testing of the SDP controller in interface mode."""
    async def setUp(self):
        await self.setup_server('127.0.0.1', 0, None, interface_mode=True,
                                safe_multicast_cidr="225.100.0.0/16",
                                batch_role='batch',
                                loop=self.loop)
        self.create_patch('time.time', return_value=123456789.5)
        # Isolate tests from each other by resetting this
        _capture_block_names.clear()

    async def test_capture_init(self):
        await self.assert_request_fails("capture-init", SUBARRAY_PRODUCT1)
        await self.client.request("product-configure", SUBARRAY_PRODUCT1, CONFIG)
        reply, informs = await self.client.request("capture-init", SUBARRAY_PRODUCT1)
        self.assertEqual(reply, [b"123456789"])

        reply, informs = await self.client.request("capture-status", SUBARRAY_PRODUCT1)
        self.assertEqual(reply, [b"capturing"])
        await self.assert_request_fails("capture-init", SUBARRAY_PRODUCT1)

    async def test_interface_sensors(self):
        await self.assert_sensors(EXPECTED_SENSOR_LIST)
        await self.assert_sensor_value('products', '[]')
        interface_changed_msg = Message.inform('interface-changed', b'sensor-list')
        # Constructor checks that message ID is valid and ANY isn't, so
        # have to hack it in after constructor.
        interface_changed_msg.mid = mock.ANY
        with mock.patch.object(aiokatcp.Client, 'unhandled_inform', spec=True) as unhandled_inform:
            await self.client.request("product-configure", SUBARRAY_PRODUCT1, CONFIG)
            unhandled_inform.assert_called_once_with(self.client, interface_changed_msg)
        await self.assert_sensors(EXPECTED_SENSOR_LIST + EXPECTED_INTERFACE_SENSOR_LIST_1)
        await self.assert_sensor_value('products', json.dumps([SUBARRAY_PRODUCT1]))
        # Deconfigure and check that the array sensors are gone
        with mock.patch.object(aiokatcp.Client, 'unhandled_inform', spec=True) as unhandled_inform:
            await self.client.request("product-deconfigure", SUBARRAY_PRODUCT1)
            unhandled_inform.assert_called_with(self.client, interface_changed_msg)
        await self.assert_sensors(EXPECTED_SENSOR_LIST)
        await self.assert_sensor_value('products', '[]')

    async def test_capture_done(self):
        await self.assert_request_fails("capture-done", SUBARRAY_PRODUCT2)
        await self.client.request("product-configure", SUBARRAY_PRODUCT2, CONFIG)
        await self.assert_request_fails("capture-done", SUBARRAY_PRODUCT2)

        await self.client.request("capture-init", SUBARRAY_PRODUCT2)
        reply, informs = await self.client.request("capture-done", SUBARRAY_PRODUCT2)
        self.assertEqual(reply, [b"123456789"])
        await self.assert_request_fails("capture-done", SUBARRAY_PRODUCT2)

    async def test_deconfigure_subarray_product(self):
        await self.assert_request_fails("product-configure", SUBARRAY_PRODUCT3)
        await self.client.request("product-configure", SUBARRAY_PRODUCT3, CONFIG)
        await self.client.request("capture-init", SUBARRAY_PRODUCT3)
        # should not be able to deconfigure when not in idle state
        await self.assert_request_fails("product-deconfigure", SUBARRAY_PRODUCT3)
        await self.client.request("capture-done", SUBARRAY_PRODUCT3)
        await self.client.request("product-deconfigure", SUBARRAY_PRODUCT3)

    async def test_configure_subarray_product(self):
        await self.assert_request_fails("product-deconfigure", SUBARRAY_PRODUCT4)
        await self.client.request("product-list")
        await self.client.request("product-configure", SUBARRAY_PRODUCT4, CONFIG)
        # Same config again is okay
        await self.client.request("product-configure", SUBARRAY_PRODUCT4, CONFIG)
        # Changing the config without deconfiguring is not okay
        config2 = json.loads(CONFIG)
        config2['outputs']['sdp_l0_continuum']['continuum_factor'] = 8
        config2 = json.dumps(config2)
        await self.assert_request_fails("product-configure", SUBARRAY_PRODUCT4, config2)
        await self.client.request("product-deconfigure", SUBARRAY_PRODUCT4)
        # Check that config2 is valid - otherwise the above test is testing the wrong thing
        await self.client.request("product-configure", SUBARRAY_PRODUCT4, config2)

        reply, informs = await self.client.request('product-list')
        self.assertEqual(reply, [b'1'])

        await self.client.request("product-deconfigure", SUBARRAY_PRODUCT4)
        await self.assert_request_fails("product-list", SUBARRAY_PRODUCT4)

    async def test_reconfigure_subarray_product(self):
        await self.assert_request_fails("product-reconfigure", SUBARRAY_PRODUCT4)
        await self.client.request("product-configure", SUBARRAY_PRODUCT4, CONFIG)
        await self.client.request("product-reconfigure", SUBARRAY_PRODUCT4)
        await self.client.request("capture-init", SUBARRAY_PRODUCT4)
        await self.assert_request_fails("product-reconfigure", SUBARRAY_PRODUCT4)

    async def test_help(self):
        reply, informs = await self.client.request('help')
        requests = [inform.arguments[0].decode('utf-8') for inform in informs]
        self.assertEqual(set(EXPECTED_REQUEST_LIST), set(requests))


class DelayedManager:
    """Asynchronous context manager that runs its block with a task in progress.

    The `mock` is modified to return a future that only resolves to
    `return_value` after exiting the context manager completed (the first
    time it is called).

    If `cancelled` is true, the request is expected to fail with a message
    about being cancelled, otherwise it is expected to succeed.
    """
    def __init__(self, coro, mock, return_value, cancelled, *, loop):
        # Set when the call to the mock is made
        self._started = asyncio.Future(loop=loop)
        self.mock = mock
        self.return_value = return_value
        self.cancelled = cancelled
        # Set to return_value when exiting the manager
        self._result = asyncio.Future(loop=loop)
        self._old_side_effect = mock.side_effect
        mock.side_effect = self._side_effect
        self._request_task = loop.create_task(coro)

    def _side_effect(self, *args, **kwargs):
        self._started.set_result(None)
        self.mock.side_effect = self._old_side_effect
        return self._result

    async def __aenter__(self):
        await self._started
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        # Unblock the mock call
        if not self._result.cancelled():
            self._result.set_result(self.return_value)
        if exc_type:
            # If we already have an exception, don't make more assertions
            self._request_task.cancel()
            return
        if self.cancelled:
            with assert_raises(FailReply) as cm:
                await self._request_task
            assert_equal('request cancelled', str(cm.exception))
        else:
            await self._request_task     # Will raise if it failed


@timelimit
class TestSDPController(BaseTestSDPController):
    """Test :class:`katsdpcontroller.sdpcontroller.SDPController` using
    mocking of the scheduler.
    """
    def _request_slow(self, name, *args, cancelled=False):
        """Asynchronous context manager that runs its block with a request in progress.

        The request must operate by issuing requests to the tasks, as this is
        used to block it from completing.
        """
        sensor_proxy_client = self.sensor_proxy_client_class.return_value
        return DelayedManager(
            self.client.request(name, *args),
            sensor_proxy_client.request,
            ([], []),
            cancelled, loop=self.loop)

    def _capture_init_slow(self, subarray_product, cancelled=False):
        """Asynchronous context manager that runs its block with a capture-init
        in progress. The subarray product must already be configured.
        """
        return self._request_slow('capture-init', subarray_product, cancelled=cancelled)

    def _capture_done_slow(self, subarray_product, cancelled=False):
        """Asynchronous context manager that runs its block with a capture-done
        in progress. The subarray product must already be configured and
        capturing.
        """
        return self._request_slow('capture-done', subarray_product, cancelled=cancelled)

    def _product_configure_slow(self, subarray_product, cancelled=False):
        """Asynchronous context manager that runs its block with a
        product-configure in progress.
        """
        sensor_proxy_client = self.sensor_proxy_client_class.return_value
        return DelayedManager(
            self.client.request(*self._configure_args(subarray_product)),
            sensor_proxy_client.wait_synced,
            None, cancelled, loop=self.loop)

    def _getaddrinfo(self, host, *args, **kwargs):
        """Mock getaddrinfo that replaces all hosts with a dummy IP address"""
        if host.startswith('host'):
            host = '127.0.0.2'
        return getaddrinfo(host, *args, **kwargs)

    async def setUp(self):
        # Future that is already resolved with no return value
        done_future = asyncio.Future()
        done_future.set_result(None)
        self.create_patch('time.time', return_value=123456789.5)
        mock_getaddrinfo = self.create_patch('socket.getaddrinfo', side_effect=self._getaddrinfo)
        # Workaround for Python bug that makes it think mocks are coroutines
        mock_getaddrinfo._is_coroutine = False
        # Mock TelescopeState's constructor to create an in-memory telstate
        orig_telstate_init = katsdptelstate.TelescopeState.__init__
        self.telstate = None

        def _telstate_init(obj, *args, **kwargs):
            self.telstate = obj
            orig_telstate_init(obj)
        self.create_patch('katsdptelstate.TelescopeState.__init__', side_effect=_telstate_init,
                          autospec=True)
        self.sensor_proxy_client_class = self.create_patch(
            'katsdpcontroller.sensor_proxy.SensorProxyClient', autospec=True)
        sensor_proxy_client = self.sensor_proxy_client_class.return_value
        sensor_proxy_client.wait_connected.return_value = done_future
        sensor_proxy_client.wait_synced.return_value = done_future
        sensor_proxy_client.wait_closed.return_value = done_future
        sensor_proxy_client.request.side_effect = self._request
        self.create_patch(
            'katsdpcontroller.scheduler.poll_ports', autospec=True, return_value=done_future)
        self.create_patch('netifaces.interfaces', autospec=True, return_value=['lo', 'em1'])
        self.create_patch('netifaces.ifaddresses', autospec=True, side_effect=self._ifaddresses)
        self.sched = mock.create_autospec(spec=scheduler.Scheduler, instance=True)
        self.sched.launch.side_effect = self._launch
        self.sched.kill.side_effect = self._kill
        self.sched.batch_run.side_effect = self._batch_run
        self.sched.close.return_value = done_future
        self.sched.http_url = 'http://scheduler:8080/'
        self.driver = mock.create_autospec(spec=pymesos.MesosSchedulerDriver, instance=True)
        self.open_mock = self.create_patch('builtins.open', new_callable=open_file_mock.MockOpen)
        self.open_mock.set_read_data_for('s3_config.json', '''
            {
                "continuum": {
                    "read": {
                        "access_key": "not-really-an-access-key",
                        "secret_key": "tellno1"
                    },
                    "write": {
                        "access_key": "another-fake-key",
                        "secret_key": "s3cr3t"
                    },
                    "url": "http://continuum.s3.invalid/",
                    "expiry_days": 7
                },
                "spectral": {
                    "read": {
                        "access_key": "not-really-an-access-key",
                        "secret_key": "tellno1"
                    },
                    "write": {
                        "access_key": "another-fake-key",
                        "secret_key": "s3cr3t"
                    },
                    "url": "http://spectral.s3.invalid/",
                    "expiry_days": 7
                },
                "archive": {
                    "read": {
                        "access_key": "not-really-an-access-key",
                        "secret_key": "tellno1"
                    },
                    "write": {
                        "access_key": "another-fake-key",
                        "secret_key": "s3cr3t"
                    },
                    "url": "http://archive.s3.invalid/"
                }
            }''')
        await self.setup_server(
            '127.0.0.1', 0, self.sched, s3_config_file='s3_config.json',
            safe_multicast_cidr="225.100.0.0/16", loop=self.loop,
            batch_role='batch')
        for product in [SUBARRAY_PRODUCT1, SUBARRAY_PRODUCT2,
                        SUBARRAY_PRODUCT3, SUBARRAY_PRODUCT4]:
            # Creating the sensor here isn't quite accurate (it is a dynamic sensor
            # created on subarray activation), but it shouldn't matter.
            self.server.sensors.add(Sensor(
                bytes, product + '.cal.1.capture-block-state',
                'Dummy implementation of sensor', default=b'{}',
                initial_status=Sensor.Status.NOMINAL))
            self.server.sensors.add(Sensor(
                DeviceStatus, product + '.ingest.sdp_l0.1.device-status',
                'Dummy implementation of sensor',
                initial_status=Sensor.Status.NOMINAL))
        master_and_slaves_future = asyncio.Future(loop=self.loop)
        master_and_slaves_future.set_result(
            ('10.0.0.1', ['10.0.0.1', '10.0.0.2', '10.0.0.3', '10.0.0.4']))
        self.sched.get_master_and_slaves.return_value = master_and_slaves_future
        # Dict mapping task name to Mesos task status string
        self.fail_launches = {}
        # Set of katcp requests to return failures for
        self.fail_requests = set()
        # Isolate tests from each other by resetting this
        _capture_block_names.clear()

        # Mock out use of katdal to get the targets
        catalogue = katpoint.Catalogue()
        catalogue.add('PKS 1934-63, radec target, 19:39:25.03, -63:42:45.7')
        catalogue.add('3C286, radec, 13:31:08.29, +30:30:33.0,(800.0 43200.0 0.956 0.584 -0.1644)')
        # Two targets with the same name, to check disambiguation
        catalogue.add('My Target, radec target, 0:00:00.00, -10:00:00.0')
        catalogue.add('My Target, radec target, 0:00:00.00, -20:00:00.0')
        self.create_patch('katsdpcontroller.generator._get_targets', return_value=catalogue)

    async def _launch(self, graph, resolver, nodes=None, *, queue=None, resources_timeout=None):
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
                    node.allocation.agent.agent_id = 'agent-id.' + node.logical_node.name
                    node.allocation.agent.gpus[0].name = 'GeForce GTX TITAN X'
                    for request in node.logical_node.interfaces:
                        interface = mock.Mock()
                        interface.name = 'em1'
                        node.interfaces[request.network] = interface
        order_graph = scheduler.subgraph(graph, scheduler.DEPENDS_RESOLVE, nodes)
        for node in reversed(list(networkx.topological_sort(order_graph))):
            if node.state < scheduler.TaskState.RUNNING:
                await node.resolve(resolver, graph, self.loop)
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
        await asyncio.gather(*futures, loop=self.loop)

    async def _batch_run(self, graph, resolver, nodes=None, *,
                         queue=None, resources_timeout=None,
                         attempts=1):
        """Mock implementation of Scheduler.batch_run.

        For now this is a much lighter-weight emulation than :meth:`_launch`,
        and does not model the internal state machine of the nodes. It may
        need to be improved later.
        """
        if nodes is None:
            nodes = list(graph.nodes())
        for node in nodes:
            # Batch tasks die on their own
            node.death_expected = True
        for node in nodes:
            node.set_state(scheduler.TaskState.READY)
            node.set_state(scheduler.TaskState.DEAD)
        scheduler.BATCH_TASKS_CREATED.inc(len(nodes))
        scheduler.BATCH_TASKS_STARTED.inc(len(nodes))
        scheduler.BATCH_TASKS_DONE.inc(len(nodes))

    async def _kill(self, graph, nodes=None, **kwargs):
        """Mock implementation of Scheduler.kill."""
        if nodes is not None:
            kill_graph = graph.subgraph(nodes)
        else:
            kill_graph = graph
        for node in kill_graph:
            if scheduler.TaskState.STARTED <= node.state <= scheduler.TaskState.KILLING:
                if hasattr(node, 'graceful_kill') and not kwargs.get('force'):
                    await node.graceful_kill(self.driver, **kwargs)
                else:
                    node.kill(self.driver, **kwargs)
            node.set_state(scheduler.TaskState.DEAD)

    async def _request(self, msg, *args, **kwargs):
        """Mock implementation of aiokatcp.Client.request"""
        if msg in self.fail_requests:
            raise FailReply('dummy failure')
        else:
            return [], []

    def _ifaddresses(self, interface):
        if interface == 'lo':
            return {
                netifaces.AF_INET: [{
                    'addr': '127.0.0.1', 'netmask': '255.0.0.0', 'peer': '127.0.0.1'}],
                netifaces.AF_INET6: [{
                    'addr': '::1', 'netmask': 'ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff/128'}],
            }
        elif interface == 'em1':
            return {
                netifaces.AF_INET: [{
                    'addr': '10.0.0.2', 'broadcast': '10.255.255.255', 'netmask': '255.0.0.0'}],
            }
        else:
            raise ValueError('You must specify a valid interface name')

    def _configure_args(self, subarray_product):
        return ("product-configure", subarray_product, CONFIG)

    async def _configure_subarray(self, subarray_product):
        reply, informs = await self.client.request(*self._configure_args(subarray_product))
        return reply[0]

    def assert_immutable(self, key, value):
        """Check the value of a telstate key and also that it is immutable"""
        self.assertEqual(self.telstate[key], value)
        self.assertTrue(self.telstate.is_immutable(key))

    async def test_product_configure_success(self):
        """A ?product-configure request must wait for the tasks to come up,
        then indicate success.
        """
        await self.client.request(
            'set-config-override', SUBARRAY_PRODUCT4,
            '''
            {
                "config": {
                    "develop": true,
                    "service_overrides": {
                        "vis_writer.sdp_l0": {
                            "config": {
                                "override_test": "value"
                            }
                        }
                    }
                }
            }''')
        name = await self._configure_subarray(SUBARRAY_PRODUCT4)
        self.assertEqual(SUBARRAY_PRODUCT4.encode('utf-8'), name)
        katsdptelstate.TelescopeState.__init__.assert_called_once_with(
            mock.ANY, 'host.telstate:20000')

        # Verify the telescope state.
        # This is not a complete list of calls. It checks that each category of stuff
        # is covered: base_params, per node, per edge
        self.assert_immutable('subarray_product_id', SUBARRAY_PRODUCT4)
        self.assert_immutable('config.vis_writer.sdp_l0', {
            'external_hostname': 'host.vis_writer.sdp_l0',
            'npy_path': '/var/kat/data',
            'obj_size_mb': mock.ANY,
            'port': 20000,
            'aiomonitor_port': 20001,
            'aioconsole_port': 20002,
            'aiomonitor': True,
            'l0_spead': mock.ANY,
            'l0_interface': 'em1',
            'l0_name': 'sdp_l0',
            's3_endpoint_url': 'http://archive.s3.invalid/',
            's3_expiry_days': None,
            'workers': mock.ANY,
            'buffer_dumps': mock.ANY,
            'obj_max_dumps': mock.ANY,
            'direct_write': True,
            'override_test': 'value'
        })
        # Test that the output channel rounding was done correctly
        self.assert_immutable('config.ingest.sdp_l0_continuum_only', {
            'antenna_mask': mock.ANY,
            'cbf_spead': mock.ANY,
            'cbf_ibv': False,
            'cbf_name': 'i0_baseline_correlation_products',
            'continuum_factor': 16,
            'l0_continuum_spead': mock.ANY,
            'l0_continuum_name': 'sdp_l0_continuum_only',
            'l0_spectral_name': None,
            'sd_continuum_factor': 16,
            'sd_spead_rate': mock.ANY,
            'sd_output_channels': '96:3488',
            'sd_int_time': 1.996,
            'output_int_time': 1.996,
            'output_channels': '96:3488',
            'servers': 2,
            'clock_ratio': 1.0,
            'aiomonitor': True
        })

        # Verify the state of the subarray
        self.assertEqual({}, self.server.override_dicts)
        sa = self.server.subarray_products[SUBARRAY_PRODUCT4]
        self.assertFalse(sa.async_busy)
        self.assertEqual(ProductState.IDLE, sa.state)

    async def test_product_configure_generate_names(self):
        """Name with trailing * must generate lowest-numbered name"""
        name = await self._configure_subarray('prefix_*')
        self.assertEqual(b'prefix_0', name)
        name = await self._configure_subarray('prefix_*')
        self.assertEqual(b'prefix_1', name)
        await self.client.request('product-deconfigure', 'prefix_0')
        name = await self._configure_subarray('prefix_*')
        self.assertEqual(b'prefix_0', name)

    async def test_product_configure_s3_config_missing(self):
        self.open_mock.unregister_path('s3_config.json')
        await self.assert_request_fails(*self._configure_args(SUBARRAY_PRODUCT1))
        # Must not have created the subarray product internally
        self.assertEqual({}, self.server.subarray_products)

    async def test_product_configure_s3_config_bad_json(self):
        self.open_mock.unregister_path('s3_config.json')
        self.open_mock.set_read_data_for('s3_config.json', '{not json')
        await self.assert_request_fails(*self._configure_args(SUBARRAY_PRODUCT1))

    async def test_product_configure_s3_config_schema_fail(self):
        self.open_mock.unregister_path('s3_config.json')
        self.open_mock.set_read_data_for('s3_config.json', '{}')
        await self.assert_request_fails(*self._configure_args(SUBARRAY_PRODUCT1))

    async def test_product_configure_telstate_fail(self):
        """If the telstate task fails, product-configure must fail"""
        self.fail_launches['telstate'] = 'TASK_FAILED'
        katsdptelstate.TelescopeState.__init__.side_effect = katsdptelstate.ConnectionError
        await self.assert_request_fails(*self._configure_args(SUBARRAY_PRODUCT4))
        self.sched.launch.assert_called_with(mock.ANY, mock.ANY, mock.ANY)
        self.sched.kill.assert_called_with(mock.ANY, capture_blocks=mock.ANY, force=True)
        # Must not have created the subarray product internally
        self.assertEqual({}, self.server.subarray_products)

    async def test_product_configure_task_fail(self):
        """If a task other than telstate fails, product-configure must fail"""
        self.fail_launches['ingest.sdp_l0.1'] = 'TASK_FAILED'
        await self.assert_request_fails(*self._configure_args(SUBARRAY_PRODUCT4))
        katsdptelstate.TelescopeState.__init__.assert_called_once_with(
            mock.ANY, 'host.telstate:20000')
        self.sched.launch.assert_called_with(mock.ANY, mock.ANY)
        self.sched.kill.assert_called_with(mock.ANY, capture_blocks=mock.ANY, force=True)
        # Must have cleaned up the subarray product internally
        self.assertEqual({}, self.server.subarray_products)

    async def test_product_configure_parallel(self):
        """Can configure two subarray products at the same time"""
        async with self._product_configure_slow(SUBARRAY_PRODUCT1):
            # We use a different subarray product
            await self.client.request(*self._configure_args(SUBARRAY_PRODUCT2))
        # Check that both products are in the right state
        self.assertIn(SUBARRAY_PRODUCT1, self.server.subarray_products)
        self.assertEqual(ProductState.IDLE, self.server.subarray_products[SUBARRAY_PRODUCT1].state)
        self.assertIn(SUBARRAY_PRODUCT2, self.server.subarray_products)
        self.assertEqual(ProductState.IDLE, self.server.subarray_products[SUBARRAY_PRODUCT2].state)

    async def test_product_deconfigure(self):
        """Checks success path of product-deconfigure"""
        await self._configure_subarray(SUBARRAY_PRODUCT1)
        await self.client.request("product-deconfigure", SUBARRAY_PRODUCT1)
        # Check that the graph was shut down
        self.sched.kill.assert_called_with(mock.ANY, capture_blocks=mock.ANY, force=False)
        # Verify the state
        self.assertEqual({}, self.server.subarray_products)

    async def test_product_deconfigure_capturing(self):
        """product-deconfigure must fail while capturing"""
        await self._configure_subarray(SUBARRAY_PRODUCT1)
        await self.client.request("capture-init", SUBARRAY_PRODUCT1)
        await self.assert_request_fails("product-deconfigure", SUBARRAY_PRODUCT1)

    async def test_product_deconfigure_capturing_force(self):
        """forced product-deconfigure must succeed while capturing"""
        await self._configure_subarray(SUBARRAY_PRODUCT1)
        await self.client.request("capture-init", SUBARRAY_PRODUCT1)
        await self.client.request("product-deconfigure", SUBARRAY_PRODUCT1, '1')

    async def test_product_deconfigure_busy(self):
        """product-deconfigure cannot happen concurrently with capture-init"""
        await self._configure_subarray(SUBARRAY_PRODUCT1)
        async with self._capture_init_slow(SUBARRAY_PRODUCT1):
            await self.assert_request_fails('product-deconfigure', SUBARRAY_PRODUCT1, '0')
        # Check that the subarray still exists and has the right state
        sa = self.server.subarray_products[SUBARRAY_PRODUCT1]
        self.assertFalse(sa.async_busy)
        self.assertEqual(ProductState.CAPTURING, sa.state)

    async def test_product_deconfigure_busy_force(self):
        """forced product-deconfigure must succeed while in capture-init"""
        await self._configure_subarray(SUBARRAY_PRODUCT1)
        async with self._capture_init_slow(SUBARRAY_PRODUCT1, cancelled=True):
            await self.client.request("product-deconfigure", SUBARRAY_PRODUCT1, '1')
        # Check that the graph was shut down
        self.sched.kill.assert_called_with(mock.ANY, capture_blocks=mock.ANY, force=True)
        # Verify the state
        self.assertEqual({}, self.server.subarray_products)

    async def test_product_deconfigure_while_configuring_force(self):
        """forced product-deconfigure must succeed while in product-configure"""
        async with self._product_configure_slow(SUBARRAY_PRODUCT1, cancelled=True):
            await self.client.request("product-deconfigure", SUBARRAY_PRODUCT1, '1')
        # Check that the graph was shut down
        self.sched.kill.assert_called_with(mock.ANY, capture_blocks=mock.ANY, force=True)
        # Verify the state
        self.assertEqual({}, self.server.subarray_products)

    async def test_product_reconfigure(self):
        """Checks success path of product_reconfigure"""
        await self._configure_subarray(SUBARRAY_PRODUCT1)
        self.sched.launch.reset_mock()
        self.sched.kill.reset_mock()

        await self.client.request(
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
                        "sim.i0_baseline_correlation_products.1": {
                            "taskinfo": {
                                "command": {
                                    "shell": true
                                }
                            }
                        }
                    }
                }
            }''')
        await self.client.request('product-reconfigure', SUBARRAY_PRODUCT1)
        # Check that the graph was killed and restarted
        self.sched.kill.assert_called_with(mock.ANY, capture_blocks=mock.ANY, force=False)
        self.sched.launch.assert_called_with(mock.ANY, mock.ANY)
        # Check that the override took effect
        self.assert_immutable('config.sim.i0_baseline_correlation_products', {
            'antenna_mask': ANTENNAS,
            'cbf_ibv': True,
            'cbf_channels': 4096,
            'cbf_substreams': 16,
            'cbf_adc_sample_rate': 1712000000.0,
            'cbf_bandwidth': 856000000.0,
            'cbf_int_time': 0.499,
            'cbf_sim_clock_ratio': 1.0,
            'cbf_sync_time': mock.ANY,
            'cbf_spead': '239.102.255.0+15:7148',
            'max_packet_size': mock.ANY,
            'servers': mock.ANY
        })
        # Check that the taskinfo override worked
        self.assert_immutable('sdp_task_details', mock.ANY)
        task_details = self.telstate['sdp_task_details']
        self.assertTrue(
            task_details['sim.i0_baseline_correlation_products.1']['taskinfo']['command']['shell'])

    async def test_product_reconfigure_configure_busy(self):
        """Can run product-reconfigure concurrently with another product-configure"""
        await self._configure_subarray(SUBARRAY_PRODUCT1)
        async with self._product_configure_slow(SUBARRAY_PRODUCT2):
            await self.client.request('product-reconfigure', SUBARRAY_PRODUCT1)

    async def test_product_reconfigure_configure_fails(self):
        """Tests product-reconfigure when the new graph fails"""
        await self._configure_subarray(SUBARRAY_PRODUCT1)
        self.fail_launches['telstate'] = 'TASK_FAILED'
        await self.assert_request_fails('product-reconfigure', SUBARRAY_PRODUCT1)
        # Check that the subarray was deconfigured cleanly
        self.assertEqual({}, self.server.subarray_products)

    # TODO: test that reconfigure with override dict picks up the override dict

    async def test_capture_init(self):
        """Checks that capture-init succeeds and sets appropriate state"""
        await self._configure_subarray(SUBARRAY_PRODUCT4)
        await self.client.request("capture-init", SUBARRAY_PRODUCT4)
        # check that the subarray is in an appropriate state
        sa = self.server.subarray_products[SUBARRAY_PRODUCT4]
        self.assertFalse(sa.async_busy)
        self.assertEqual(ProductState.CAPTURING, sa.state)
        # Check that the graph transitions were called. Each call may be made
        # multiple times, depending on the number of instances of each child.
        # We thus collapse them into groups of equal calls and don't worry
        # about the number, which would otherwise make the test fragile.
        katcp_client = self.sensor_proxy_client_class.return_value
        grouped_calls = [k for k, g in itertools.groupby(katcp_client.request.mock_calls)]
        expected_calls = [
            mock.call('capture-init', '123456789'),
            mock.call('capture-start', 'i0_baseline_correlation_products', mock.ANY)
        ]
        self.assertEqual(grouped_calls, expected_calls)

    async def test_capture_init_failed_req(self):
        """Capture-init fails on some task"""
        await self._configure_subarray(SUBARRAY_PRODUCT4)
        self.fail_requests.add('capture-init')
        await self.assert_request_fails("capture-init", SUBARRAY_PRODUCT4)
        # check that the subarray is in an appropriate state
        sa = self.server.subarray_products[SUBARRAY_PRODUCT4]
        self.assertEqual(ProductState.ERROR, sa.state)
        self.assertEqual({}, sa.capture_blocks)
        # check that the subarray can be safely deconfigured, and that it
        # goes via DECONFIGURING state. Rather than trying to directly
        # observe the internal state during deconfigure (e.g. with
        # DelayedManager), we'll just observe the sensor
        state_observer = mock.Mock()
        self.server.sensors[SUBARRAY_PRODUCT4 + '.state'].attach(state_observer)
        await self.client.request('product-deconfigure', SUBARRAY_PRODUCT4)
        # call 0, arguments, argument 1
        self.assertEqual(state_observer.mock_calls[0][1][1].value, ProductState.DECONFIGURING)
        self.assertEqual(ProductState.DEAD, sa.state)

    async def test_capture_done_failed_req(self):
        """Capture-done fails on some task"""
        await self._configure_subarray(SUBARRAY_PRODUCT4)
        self.fail_requests.add('capture-done')
        reply, informs = await self.client.request("capture-init", SUBARRAY_PRODUCT4)
        await self.assert_request_fails("capture-done", SUBARRAY_PRODUCT4)
        # check that the subsequent transitions still run
        katcp_client = self.sensor_proxy_client_class.return_value
        katcp_client.request.assert_called_with('write-meta', '123456789', True)
        # check that the subarray is in an appropriate state
        sa = self.server.subarray_products[SUBARRAY_PRODUCT4]
        self.assertEqual(ProductState.ERROR, sa.state)
        self.assertEqual({}, sa.capture_blocks)
        # check that the subarray can be safely deconfigured
        await self.client.request('product-deconfigure', SUBARRAY_PRODUCT4)
        self.assertEqual(ProductState.DEAD, sa.state)

    async def _test_busy(self, command, *args):
        """Test that a command fails if issued while ?capture-init or
        ?product-configure is in progress
        """
        async with self._product_configure_slow(SUBARRAY_PRODUCT1):
            await self.assert_request_fails(command, *args)
        async with self._capture_init_slow(SUBARRAY_PRODUCT1):
            await self.assert_request_fails(command, *args)

    async def test_capture_init_busy(self):
        """Capture-init fails if an asynchronous operation is already in progress"""
        await self._test_busy("capture-init", SUBARRAY_PRODUCT1)

    def _ingest_died(self, subarray_product):
        """Mark an ingest process as having died"""
        for node in subarray_product.physical_graph:
            if node.logical_node.name == 'ingest.sdp_l0.1':
                node.set_state(scheduler.TaskState.DEAD)
                node.status = Dict(state='TASK_FAILED')
                break
        else:
            raise ValueError('Could not find ingest node')

    def _ingest_bad_device_status(self, subarray_product):
        """Mark an ingest process as having bad status"""
        sensor_name = subarray_product.subarray_product_id + '.ingest.sdp_l0.1.device-status'
        sensor = self.server.sensors[sensor_name]
        sensor.set_value(DeviceStatus.FAIL, Sensor.Status.ERROR)

    async def test_capture_init_dead_process(self):
        """Capture-init fails if a child process is dead."""
        await self.client.request("product-configure", SUBARRAY_PRODUCT4, CONFIG)
        sa = self.server.subarray_products[SUBARRAY_PRODUCT4]
        self._ingest_died(sa)
        self.assertEqual(ProductState.ERROR, sa.state)
        await self.assert_request_fails("capture-init", SUBARRAY_PRODUCT4)
        # check that the subarray is in an appropriate state
        self.assertEqual(ProductState.ERROR, sa.state)
        self.assertEqual({}, sa.capture_blocks)

    async def test_capture_init_process_dies(self):
        """Capture-init fails if a child dies half-way through."""
        await self.client.request("product-configure", SUBARRAY_PRODUCT1, CONFIG)
        sa = self.server.subarray_products[SUBARRAY_PRODUCT1]
        with self.assertRaises(FailReply):
            async with self._capture_init_slow(SUBARRAY_PRODUCT1):
                self._ingest_died(sa)
        # check that the subarray is in an appropriate state
        self.assertEqual(ProductState.ERROR, sa.state)
        self.assertEqual({}, sa.capture_blocks)

    async def test_capture_done(self):
        """Checks that capture-done succeeds and sets appropriate state"""
        init_batch_started = get_metric(scheduler.BATCH_TASKS_STARTED)
        init_batch_done = get_metric(scheduler.BATCH_TASKS_DONE)
        init_batch_failed = get_metric(scheduler.BATCH_TASKS_FAILED)
        init_batch_skipped = get_metric(scheduler.BATCH_TASKS_SKIPPED)

        await self._configure_subarray(SUBARRAY_PRODUCT4)
        await self.client.request("capture-init", SUBARRAY_PRODUCT4)
        cal_sensor = self.server.sensors[SUBARRAY_PRODUCT4 + '.cal.1.capture-block-state']
        cal_sensor.value = b'{"123456789": "capturing"}'
        await self.client.request("capture-done", SUBARRAY_PRODUCT4)
        # check that the subarray is in an appropriate state
        sa = self.server.subarray_products[SUBARRAY_PRODUCT4]
        self.assertFalse(sa.async_busy)
        self.assertEqual(ProductState.IDLE, sa.state)
        # Check that the graph transitions succeeded
        katcp_client = self.sensor_proxy_client_class.return_value
        katcp_client.request.assert_any_call('capture-done')
        katcp_client.request.assert_called_with('write-meta', '123456789', True)
        # Now simulate cal finishing with the capture block
        cal_sensor.value = b'{}'
        await asynctest.exhaust_callbacks(self.loop)
        # write-meta full dump must be last, hence assert_called_with not assert_any_call
        katcp_client.request.assert_called_with('write-meta', '123456789', False)

        # Check that postprocessing ran and didn't fail
        self.assertEqual(sa.capture_blocks, {})
        started = get_metric(scheduler.BATCH_TASKS_STARTED) - init_batch_started
        done = get_metric(scheduler.BATCH_TASKS_DONE) - init_batch_done
        failed = get_metric(scheduler.BATCH_TASKS_FAILED) - init_batch_failed
        skipped = get_metric(scheduler.BATCH_TASKS_SKIPPED) - init_batch_skipped
        self.assertEqual(started, 7)    # One continuum, 3x2 spectral
        self.assertEqual(done, started)
        self.assertEqual(failed, 0)
        self.assertEqual(skipped, 0)

    async def test_capture_done_busy(self):
        """Capture-done fails if an asynchronous operation is already in progress"""
        await self._test_busy("capture-done", SUBARRAY_PRODUCT1)

    async def _test_failure_while_capturing(self, failfunc):
        await self.client.request("product-configure", SUBARRAY_PRODUCT1, CONFIG)
        reply, _ = await self.client.request("capture-init", SUBARRAY_PRODUCT1)
        cbid = reply[0].decode()
        sa = self.server.subarray_products[SUBARRAY_PRODUCT1]
        self.assertEqual(ProductState.CAPTURING, sa.state)
        failfunc(sa)
        self.assertEqual(ProductState.ERROR, sa.state)
        self.assertEqual({cbid: mock.ANY}, sa.capture_blocks)
        # In the background it will terminate the capture block
        await asynctest.exhaust_callbacks(self.loop)
        self.assertEqual({}, sa.capture_blocks)
        katcp_client = self.sensor_proxy_client_class.return_value
        katcp_client.request.assert_called_with('write-meta', cbid, False)

    async def test_process_dies_while_capturing(self):
        await self._test_failure_while_capturing(self._ingest_died)

    async def test_bad_device_status_while_capturing(self):
        await self._test_failure_while_capturing(self._ingest_bad_device_status)

    async def test_capture_done_process_dies(self):
        """Capture-done fails if a child dies half-way through."""
        await self.client.request("product-configure", SUBARRAY_PRODUCT1, CONFIG)
        reply, _ = await self.client.request("capture-init", SUBARRAY_PRODUCT1)
        cbid = reply[0].decode()
        sa = self.server.subarray_products[SUBARRAY_PRODUCT1]
        self.assertEqual(ProductState.CAPTURING, sa.state)
        self.assertEqual({cbid: mock.ANY}, sa.capture_blocks)
        with self.assertRaises(FailReply):
            async with self._capture_done_slow(SUBARRAY_PRODUCT1):
                self._ingest_died(sa)
        # check that the subarray is in an appropriate state
        self.assertEqual(ProductState.ERROR, sa.state)
        self.assertEqual({}, sa.capture_blocks)

    async def test_deconfigure_on_exit(self):
        """Calling deconfigure_on_exit will force-deconfigure existing
        subarrays, even if capturing."""
        await self._configure_subarray(SUBARRAY_PRODUCT1)
        await self.client.request('capture-init', SUBARRAY_PRODUCT1)
        await self.server.deconfigure_on_exit()

        sensor_proxy_client = self.sensor_proxy_client_class.return_value
        sensor_proxy_client.request.assert_any_call('capture-done')
        # Forced deconfigure, so we only get the light dump
        sensor_proxy_client.request.assert_called_with('write-meta', mock.ANY, True)
        self.sched.kill.assert_called_with(mock.ANY, capture_blocks=mock.ANY, force=True)
        self.assertEqual({}, self.server.subarray_products)

    async def test_deconfigure_on_exit_busy(self):
        """Calling deconfigure_on_exit while a capture-init or capture-done
        is busy kills off the graph anyway."""
        await self._configure_subarray(SUBARRAY_PRODUCT1)
        async with self._capture_init_slow(SUBARRAY_PRODUCT1, cancelled=True):
            await self.server.deconfigure_on_exit()
        self.sched.kill.assert_called_with(mock.ANY, capture_blocks=mock.ANY, force=True)
        self.assertEqual({}, self.server.subarray_products)

    async def test_deconfigure_on_exit_cancel(self):
        """Calling deconfigure_on_exit while a configure is in process cancels
        that configure and kills off the graph."""
        async with self._product_configure_slow(SUBARRAY_PRODUCT1, cancelled=True):
            await self.server.deconfigure_on_exit()
        # We must have killed off the partially-launched graph
        self.sched.kill.assert_called_with(mock.ANY, capture_blocks=mock.ANY, force=True)

    async def test_telstate_endpoint_all(self):
        """Test telstate-endpoint without a subarray_product_id argument"""
        await self._configure_subarray(SUBARRAY_PRODUCT1)
        await self._configure_subarray(SUBARRAY_PRODUCT2)
        reply, informs = await self.client.request('telstate-endpoint')
        # Need to compare just arguments, because the message objects have message IDs
        informs = [tuple(msg.arguments) for msg in informs]
        self.assertEqual(AnyOrderList([
            (SUBARRAY_PRODUCT1.encode('utf-8'), b'host.telstate:20000'),
            (SUBARRAY_PRODUCT2.encode('utf-8'), b'host.telstate:20000')
        ]), informs)

    async def test_telstate_endpoint_one(self):
        """Test telstate-endpoint with a subarray_product_id argument"""
        await self._configure_subarray(SUBARRAY_PRODUCT1)
        reply, informs = await self.client.request('telstate-endpoint', SUBARRAY_PRODUCT1)
        self.assertEqual(reply, [b'host.telstate:20000'])

    async def test_telstate_endpoint_not_found(self):
        """Test telstate-endpoint with a subarray_product_id that does not exist"""
        await self.assert_request_fails('telstate-endpoint', SUBARRAY_PRODUCT2)

    async def test_capture_status_all(self):
        """Test capture-status without a subarray_product_id argument"""
        await self._configure_subarray(SUBARRAY_PRODUCT1)
        await self._configure_subarray(SUBARRAY_PRODUCT2)
        await self.client.request('capture-init', SUBARRAY_PRODUCT2)
        reply, informs = await self.client.request('capture-status')
        self.assertEqual(reply, [b'2'])
        # Need to compare just arguments, because the message objects have message IDs
        informs = [tuple(msg.arguments) for msg in informs]
        self.assertEqual(AnyOrderList([
            (SUBARRAY_PRODUCT1.encode('utf-8'), b'idle'),
            (SUBARRAY_PRODUCT2.encode('utf-8'), b'capturing')
        ]), informs)

    async def test_capture_status_one(self):
        """Test capture-status with a subarray_product_id argument"""
        await self._configure_subarray(SUBARRAY_PRODUCT1)
        reply, informs = await self.client.request('capture-status', SUBARRAY_PRODUCT1)
        self.assertEqual(reply, [b'idle'])
        self.assertEqual([], informs)
        await self.client.request('capture-init', SUBARRAY_PRODUCT1)
        reply, informs = await self.client.request('capture-status', SUBARRAY_PRODUCT1)
        self.assertEqual(reply, [b'capturing'])
        await self.client.request('capture-done', SUBARRAY_PRODUCT1)
        reply, informs = await self.client.request('capture-status', SUBARRAY_PRODUCT1)
        self.assertEqual(reply, [b'idle'])

    async def test_capture_status_not_found(self):
        """Test capture_status with a subarray_product_id that does not exist"""
        await self.assert_request_fails('capture-status', SUBARRAY_PRODUCT2)

    async def test_sdp_shutdown(self):
        """Tests success path of sdp-shutdown"""
        await self._configure_subarray(SUBARRAY_PRODUCT1)
        await self.client.request('capture-init', SUBARRAY_PRODUCT1)
        self.sched.launch.reset_mock()
        await self.client.request('sdp-shutdown')
        # Check that the subarray was stopped then shut down
        sensor_proxy_client = self.sensor_proxy_client_class.return_value
        sensor_proxy_client.request.assert_any_call('capture-done')
        # Forced deconfigure, so we only get the light dump
        sensor_proxy_client.request.assert_called_with('write-meta', mock.ANY, True)
        self.sched.kill.assert_called_with(mock.ANY, capture_blocks=mock.ANY, force=True)
        # Check that the shutdown was launched in two phases, non-masters
        # first.
        calls = self.sched.launch.call_args_list
        self.assertEqual(2, len(calls))
        nodes = calls[0][0][2]   # First call, positional args, 3rd argument
        hosts = [node.logical_node.host for node in nodes]
        # 10.0.0.1-10.0.0.4 are slaves, but 10.0.0.1 is master and 10.0.0.2 is us
        self.assertEqual(AnyOrderList(['10.0.0.3', '10.0.0.4']), hosts)

    async def test_sdp_shutdown_slaves_error(self):
        """Test sdp-shutdown when get_master_and_slaves fails"""
        future = asyncio.Future(loop=self.loop)
        future.set_exception(asyncio.TimeoutError())
        self.sched.get_master_and_slaves.return_value = future
        await self.assert_request_fails('sdp-shutdown')


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
