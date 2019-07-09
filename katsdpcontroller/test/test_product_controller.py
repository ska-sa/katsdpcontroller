"""Tests for :mod:`katsdpcontroller.product_controller."""

import unittest
from unittest import mock
import itertools
import asyncio
# Needs to be imported this way so that it is unaffected by mocking of socket.getaddrinfo
from socket import getaddrinfo
import ipaddress
import typing
from typing import (List, Tuple, Set, Type, Callable, Coroutine,
                    Iterable, Sequence, Mapping, Any, Optional)
from types import TracebackType

import asynctest
from nose.tools import assert_raises, assert_equal
from addict import Dict
import aiokatcp
from aiokatcp import Message, FailReply, Sensor
from aiokatcp.test.test_utils import timelimit
import pymesos
import networkx
import netifaces
import katsdptelstate
import katpoint

from katsdpcontroller.product_controller import (
    DeviceServer, SDPSubarrayProductBase, SDPSubarrayProduct, SDPResources,
    ProductState, DeviceStatus, _redact_keys)
from katsdpcontroller import scheduler
from . import fake_katportalclient

ANTENNAS = 'm000,m001,m063,m064'

SUBARRAY_PRODUCT = 'array_1_0'
CAPTURE_BLOCK = '1122334455'

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

EXPECTED_SENSOR_LIST: Tuple[Tuple[bytes, ...], ...] = (
    (b'api-version', b'', b'string'),
    (b'build-state', b'', b'string'),
    (b'device-status', b'', b'discrete', b'ok', b'degraded', b'fail'),
    (b'fmeca.FD0001', b'', b'boolean'),
    (b'time-synchronised', b'', b'boolean'),
    (b'gui-urls', b'', b'string'),
    (b'products', b'', b'string')
)

EXPECTED_INTERFACE_SENSOR_LIST: Tuple[Tuple[bytes, ...], ...] = (
    (b'bf_ingest.beamformer.1.port', b'', b'address'),
    (b'ingest.sdp_l0.1.capture-active', b'', b'boolean'),
    (b'timeplot.sdp_l0.1.gui-urls', b'', b'string'),
    (b'timeplot.sdp_l0.1.html_port', b'', b'address'),
    (b'cal.1.capture-block-state', b'', b'string'),
    (b'state', b'', b'discrete',
     b'configuring', b'idle', b'capturing', b'deconfiguring', b'dead', b'error'),
    (b'capture-block-state', b'', b'string')
)

EXPECTED_REQUEST_LIST = [
    'product-configure',
    'product-deconfigure',
    'capture-done',
    'capture-init',
    'capture-status',
    'telstate-endpoint',
    # Standard katcp commands
    'client-list', 'halt', 'help', 'log-level', 'sensor-list',
    'sensor-sampling', 'sensor-value', 'watchdog', 'version-list'
]


class DummyMasterController(aiokatcp.DeviceServer):
    VERSION = 'dummy-1.0'
    BUILD_STATE = 'dummy-1.0'

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._network = ipaddress.IPv4Network('225.100.0.0/16')
        self._next = self._network.network_address + 1

    async def request_get_multicast_groups(self, ctx: aiokatcp.RequestContext,
                                           subarray_product_id: str, n_addresses: int) -> str:
        """Dummy docstring"""
        ans = str(self._next)
        if n_addresses > 1:
            ans += '+{}'.format(n_addresses - 1)
        self._next += n_addresses
        return ans;


def get_metric(metric):
    """Get the current value of a Prometheus metric"""
    return metric.collect()[0].samples[0][2]


class TestRedactKeys(unittest.TestCase):
    def setUp(self) -> None:
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

    def test_no_command(self) -> None:
        taskinfo = Dict()
        result = _redact_keys(taskinfo, self.s3_config)
        self.assertEqual(result, taskinfo)

    def run_it(self, arguments: List[str]) -> List[str]:
        taskinfo = Dict()
        taskinfo.command.arguments = arguments
        result = _redact_keys(taskinfo, self.s3_config)
        return result.command.arguments

    def test_with_equals(self) -> None:
        result = self.run_it(['--secret=mores3cr3t', '--key=ACCESS_KEY', '--other=safe'])
        self.assertEqual(result, ['--secret=REDACTED', '--key=REDACTED', '--other=safe'])

    def test_without_equals(self) -> None:
        result = self.run_it(['--secret', 's3cr3t', '--key', 'tellno1', '--other', 'safe'])
        self.assertEqual(result, ['--secret', 'REDACTED', '--key', 'REDACTED', '--other', 'safe'])


class BaseTestSDPController(asynctest.TestCase):
    """Utilities for test classes"""

    async def setup_server(self, **server_kwargs) -> None:
        mc_server = DummyMasterController('127.0.0.1', 0)
        await mc_server.start()
        self.addCleanup(mc_server.stop)
        assert mc_server.server and mc_server.server.sockets
        mc_address = mc_server.server.sockets[0].getsockname()
        mc_client = await aiokatcp.Client.connect(mc_address[0], mc_address[1])
        self.addCleanup(mc_client.wait_closed)
        self.addCleanup(mc_client.close)

        self.server = DeviceServer(master_controller=mc_client, **server_kwargs)
        await self.server.start()
        self.addCleanup(self.server.stop)
        assert self.server.server and self.server.server.sockets
        bind_address = self.server.server.sockets[0].getsockname()
        self.client = await aiokatcp.Client.connect(bind_address[0], bind_address[1])
        self.addCleanup(self.client.wait_closed)
        self.addCleanup(self.client.close)

    async def assert_request_fails(self, name: str, *args: Any) -> None:
        with self.assertRaises(aiokatcp.FailReply):
            await self.client.request(name, *args)

    async def assert_sensors(self, expected_list: Iterable[Tuple[bytes, ...]]) -> None:
        expected = {item[0]: item[1:] for item in expected_list}
        reply, informs = await self.client.request("sensor-list")
        actual = {}
        for inform in informs:
            # Skip the description
            actual[inform.arguments[0]] = tuple(inform.arguments[2:])
        self.assertEqual(expected, actual)

    async def assert_sensor_value(self, name: str, value: Any) -> None:
        encoded = aiokatcp.encode(value)
        reply, informs = await self.client.request("sensor-value", name)
        self.assertEqual(informs[0].arguments[4], encoded)

    def create_patch(self, *args, **kwargs) -> Any:
        patcher = mock.patch(*args, **kwargs)
        mock_obj = patcher.start()
        self.addCleanup(patcher.stop)
        return mock_obj

    async def setUp(self) -> None:
        # Mock the CBF sensors
        dummy_client = fake_katportalclient.KATPortalClient(
            components={'cbf': 'cbf_1', 'sub': 'subarray_1'},
            sensors={
                'cbf_1_i0_antenna_channelised_voltage_n_chans': 4096,
                'cbf_1_i0_adc_sample_rate': 1712e6,
                'cbf_1_i0_antenna_channelised_voltage_n_samples_between_spectra': 8192,
                'subarray_1_streams_i0_antenna_channelised_voltage_bandwidth': 856e6,
                'cbf_1_i0_baseline_correlation_products_int_time': 0.499,
                'cbf_1_i0_baseline_correlation_products_n_bls': 40,
                'cbf_1_i0_baseline_correlation_products_xeng_out_bits_per_sample': 32,
                'cbf_1_i0_baseline_correlation_products_n_chans_per_substream': 256,
                'cbf_1_i0_tied_array_channelised_voltage_0x_spectra_per_heap': 256,
                'cbf_1_i0_tied_array_channelised_voltage_0x_n_chans_per_substream': 256,
                'cbf_1_i0_tied_array_channelised_voltage_0x_beng_out_bits_per_sample': 8,
                'cbf_1_i0_tied_array_channelised_voltage_0y_spectra_per_heap': 256,
                'cbf_1_i0_tied_array_channelised_voltage_0y_n_chans_per_substream': 256,
                'cbf_1_i0_tied_array_channelised_voltage_0y_beng_out_bits_per_sample': 8
            })
        self.create_patch('katportalclient.KATPortalClient', return_value=dummy_client)


@timelimit
class TestSDPControllerInterface(BaseTestSDPController):
    """Testing of the SDP controller in interface mode."""
    async def setUp(self) -> None:
        await super().setUp()
        image_resolver_factory = scheduler.ImageResolverFactory(scheduler.SimpleImageLookup('sdp'))
        await self.setup_server(host='127.0.0.1', port=0, sched=None,
                                batch_role='batch',
                                interface_mode=True,
                                image_resolver_factory=image_resolver_factory,
                                s3_config=None)
        self.create_patch('time.time', return_value=123456789.5)

    async def test_capture_init(self) -> None:
        await self.assert_request_fails("capture-init", CAPTURE_BLOCK)
        await self.client.request("product-configure", SUBARRAY_PRODUCT, CONFIG)
        await self.client.request("capture-init", CAPTURE_BLOCK)

        reply, informs = await self.client.request("capture-status")
        self.assertEqual(reply, [b"capturing"])
        await self.assert_request_fails("capture-init", CAPTURE_BLOCK)

    async def test_interface_sensors(self) -> None:
        await self.assert_sensors(EXPECTED_SENSOR_LIST)
        interface_changed_callback = mock.MagicMock()
        self.client.add_inform_callback('interface-changed', interface_changed_callback)
        await self.client.request("product-configure", SUBARRAY_PRODUCT, CONFIG)
        interface_changed_callback.assert_called_once_with([b'sensor-list'])
        await self.assert_sensors(EXPECTED_SENSOR_LIST + EXPECTED_INTERFACE_SENSOR_LIST)

        # Deconfigure and check that the server shuts down
        interface_changed_callback.reset_mock()
        await self.client.request("product-deconfigure")
        interface_changed_callback.assert_called_once_with([b'sensor-list'])
        await self.client.wait_disconnected()
        self.client.remove_inform_callback('interface-changed', interface_changed_callback)

    async def test_capture_done(self) -> None:
        await self.assert_request_fails("capture-done")
        await self.client.request("product-configure", SUBARRAY_PRODUCT, CONFIG)
        await self.assert_request_fails("capture-done")

        await self.client.request("capture-init", CAPTURE_BLOCK)
        reply, informs = await self.client.request("capture-done")
        self.assertEqual(reply, [CAPTURE_BLOCK.encode()])
        await self.assert_request_fails("capture-done")

    async def test_deconfigure_subarray_product(self) -> None:
        await self.assert_request_fails("product-configure")
        await self.client.request("product-configure", SUBARRAY_PRODUCT, CONFIG)
        await self.client.request("capture-init", CAPTURE_BLOCK)
        # should not be able to deconfigure when not in idle state
        await self.assert_request_fails("product-deconfigure")
        await self.client.request("capture-done")
        await self.client.request("product-deconfigure")
        # server should now shut itself down
        await self.client.wait_disconnected()

    async def test_configure_subarray_product(self) -> None:
        await self.assert_request_fails("product-deconfigure")
        await self.client.request("product-configure", SUBARRAY_PRODUCT, CONFIG)
        # Can't reconfigure when already configured
        await self.assert_request_fails("product-configure", SUBARRAY_PRODUCT, CONFIG)
        await self.client.request("product-deconfigure")

    async def test_help(self) -> None:
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
    def __init__(self, coro: Coroutine, mock: mock.Mock, return_value: Any,
                 cancelled: bool) -> None:
        # Set when the call to the mock is made
        self._started: asyncio.Future[None] = asyncio.Future()
        self.mock = mock
        self.return_value = return_value
        self.cancelled = cancelled
        # Set to return_value when exiting the manager
        self._result: asyncio.Future[Any] = asyncio.Future()
        self._old_side_effect = mock.side_effect
        mock.side_effect = self._side_effect
        self._request_task = asyncio.get_event_loop().create_task(coro)

    def _side_effect(self, *args, **kwargs) -> asyncio.Future:
        self._started.set_result(None)
        self.mock.side_effect = self._old_side_effect
        return self._result

    async def __aenter__(self) -> 'DelayedManager':
        await self._started
        return self

    async def __aexit__(self, exc_type: Optional[Type[BaseException]],
                        exc_value: Optional[BaseException],
                        traceback: Optional[TracebackType]) -> None:
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
    def _request_slow(self, name: str, *args: Any, cancelled: bool = False) -> DelayedManager:
        """Asynchronous context manager that runs its block with a request in progress.

        The request must operate by issuing requests to the tasks, as this is
        used to block it from completing.
        """
        sensor_proxy_client = self.sensor_proxy_client_class.return_value
        return DelayedManager(
            self.client.request(name, *args),
            sensor_proxy_client.request,
            ([], []),
            cancelled)

    def _capture_init_slow(self, capture_block: str, cancelled: bool = False) -> DelayedManager:
        """Asynchronous context manager that runs its block with a capture-init
        in progress. The subarray product must already be configured.
        """
        return self._request_slow('capture-init', capture_block, cancelled=cancelled)

    def _capture_done_slow(self, cancelled: bool = False) -> DelayedManager:
        """Asynchronous context manager that runs its block with a capture-done
        in progress. The subarray product must already be configured and
        capturing.
        """
        return self._request_slow('capture-done', cancelled=cancelled)

    def _product_configure_slow(self, subarray_product: str,
                                cancelled: bool = False) -> DelayedManager:
        """Asynchronous context manager that runs its block with a
        product-configure in progress.
        """
        sensor_proxy_client = self.sensor_proxy_client_class.return_value
        return DelayedManager(
            self.client.request(*self._configure_args(subarray_product)),
            sensor_proxy_client.wait_synced,
            None, cancelled)

    def _getaddrinfo(self, host: str, *args, **kwargs) -> List[Tuple[int, int, int, str, tuple]]:
        """Mock getaddrinfo that replaces all hosts with a dummy IP address"""
        if host.startswith('host'):
            host = '127.0.0.2'
        return getaddrinfo(host, *args, **kwargs)

    async def setUp(self) -> None:
        await super().setUp()
        # Future that is already resolved with no return value
        done_future: asyncio.Future[None] = asyncio.Future()
        done_future.set_result(None)
        self.create_patch('time.time', return_value=123456789.5)
        self.create_patch('socket.getaddrinfo', side_effect=self._getaddrinfo)
        # Mock TelescopeState's constructor to create an in-memory telstate
        orig_telstate_init = katsdptelstate.TelescopeState.__init__
        self.telstate: Optional[katsdptelstate.TelescopeState] = None

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
        s3_config = {
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
        }
        await self.setup_server(
            host='127.0.0.1', port=0, sched=self.sched,
            s3_config=s3_config,
            image_resolver_factory=scheduler.ImageResolverFactory(
                scheduler.SimpleImageLookup('sdp')),
            interface_mode=False,
            batch_role='batch')
        # Creating the sensor here isn't quite accurate (it is a dynamic sensor
        # created on subarray activation), but it shouldn't matter.
        self.server.sensors.add(Sensor(
            bytes, 'cal.1.capture-block-state',
            'Dummy implementation of sensor', default=b'{}',
            initial_status=Sensor.Status.NOMINAL))
        self.server.sensors.add(Sensor(
            DeviceStatus, 'ingest.sdp_l0.1.device-status',
            'Dummy implementation of sensor',
            initial_status=Sensor.Status.NOMINAL))
        # Dict mapping task name to Mesos task status string
        self.fail_launches: typing.Dict[str, str] = {}
        # Set of katcp requests to return failures for
        self.fail_requests: Set[str] = set()

        # Mock out use of katdal to get the targets
        catalogue = katpoint.Catalogue()
        catalogue.add('PKS 1934-63, radec target, 19:39:25.03, -63:42:45.7')
        catalogue.add('3C286, radec, 13:31:08.29, +30:30:33.0,(800.0 43200.0 0.956 0.584 -0.1644)')
        # Two targets with the same name, to check disambiguation
        catalogue.add('My Target, radec target, 0:00:00.00, -10:00:00.0')
        catalogue.add('My Target, radec target, 0:00:00.00, -20:00:00.0')
        self.create_patch('katsdpcontroller.generator._get_targets', return_value=catalogue)

    async def _launch(self, graph: networkx.MultiDiGraph,
                      resolver: scheduler.Resolver,
                      nodes: Sequence[scheduler.PhysicalNode] = None, *,
                      queue: scheduler.LaunchQueue = None,
                      resources_timeout: float = None) -> None:
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
                            node.cores[core] = core_num   # type: ignore
                            core_num += 1
                if isinstance(node.logical_node, scheduler.LogicalTask):
                    assert isinstance(node, scheduler.PhysicalTask)
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
                await node.resolve(resolver, graph)
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
        await asyncio.gather(*futures)

    async def _batch_run(self, graph: networkx.MultiDiGraph,
                         resolver: scheduler.Resolver,
                         nodes: Sequence[scheduler.PhysicalNode] = None, *,
                         queue: scheduler.LaunchQueue = None,
                         resources_timeout: float = None,
                         attempts: int = 1) -> None:
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

    async def _kill(self, graph: networkx.MultiDiGraph,
                    nodes: Sequence[scheduler.PhysicalNode] = None,
                    **kwargs) -> None:
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

    async def _request(self, msg: str, *args: Any, **kwargs: Any) \
            -> Tuple[List[bytes], List[Message]]:
        """Mock implementation of aiokatcp.Client.request"""
        if msg in self.fail_requests:
            raise FailReply('dummy failure')
        else:
            return [], []

    def _ifaddresses(self, interface: str) -> Mapping[int, Sequence[Mapping[str, str]]]:
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

    def _configure_args(self, subarray_product: str) -> Tuple[str, ...]:
        return ("product-configure", subarray_product, CONFIG)

    async def _configure_subarray(self, subarray_product: str) -> None:
        reply, informs = await self.client.request(*self._configure_args(subarray_product))

    def assert_immutable(self, key: str, value: Any) -> None:
        """Check the value of a telstate key and also that it is immutable"""
        assert self.telstate is not None
        # Uncomment for debugging
        # import json
        # with open('expected.json', 'w') as f:
        #     json.dump(value, f, indent=2, default=str, sort_keys=True)
        # with open('actual.json', 'w') as f:
        #     json.dump(self.telstate[key], f, indent=2, default=str, sort_keys=True)
        self.assertEqual(self.telstate[key], value)
        self.assertTrue(self.telstate.is_immutable(key))

    async def test_product_configure_success(self) -> None:
        """A ?product-configure request must wait for the tasks to come up,
        then indicate success.
        """
        await self._configure_subarray(SUBARRAY_PRODUCT)
        katsdptelstate.TelescopeState.__init__.assert_called_once_with(
            mock.ANY, 'host.telstate:20000')

        # Verify the telescope state.
        # This is not a complete list of calls. It checks that each category of stuff
        # is covered: base_params, per node, per edge
        self.assert_immutable('subarray_product_id', SUBARRAY_PRODUCT)
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
            'direct_write': True
        })
        # Test that the output channel rounding was done correctly
        self.assert_immutable('config.ingest.sdp_l0_continuum_only', {
            'antenna_mask': mock.ANY,
            'cbf_spead': mock.ANY,
            'cbf_ibv': True,
            'cbf_name': 'i0_baseline_correlation_products',
            'continuum_factor': 16,
            'l0_continuum_spead': mock.ANY,
            'l0_continuum_name': 'sdp_l0_continuum_only',
            'l0_spectral_name': None,
            'sd_continuum_factor': 16,
            'sd_spead_rate': mock.ANY,
            'sd_output_channels': '64:3520',
            'sd_int_time': 1.996,
            'output_int_time': 1.996,
            'output_channels': '64:3520',
            'servers': 4,
            'clock_ratio': 1.0,
            'aiomonitor': True
        })

        # Verify the state of the subarray
        product = self.server.product
        assert product is not None       # mypy doesn't understand self.assertIsNotNone
        self.assertFalse(product.async_busy)
        self.assertEqual(ProductState.IDLE, product.state)

    async def test_product_configure_telstate_fail(self) -> None:
        """If the telstate task fails, product-configure must fail"""
        self.fail_launches['telstate'] = 'TASK_FAILED'
        katsdptelstate.TelescopeState.__init__.side_effect = katsdptelstate.ConnectionError
        await self.assert_request_fails(*self._configure_args(SUBARRAY_PRODUCT))
        self.sched.launch.assert_called_with(mock.ANY, mock.ANY, mock.ANY)
        self.sched.kill.assert_called_with(mock.ANY, capture_blocks=mock.ANY, force=True)
        # Must not have created the subarray product internally
        self.assertIsNone(self.server.product)

    async def test_product_configure_task_fail(self) -> None:
        """If a task other than telstate fails, product-configure must fail"""
        self.fail_launches['ingest.sdp_l0.1'] = 'TASK_FAILED'
        await self.assert_request_fails(*self._configure_args(SUBARRAY_PRODUCT))
        katsdptelstate.TelescopeState.__init__.assert_called_once_with(
            mock.ANY, 'host.telstate:20000')
        self.sched.launch.assert_called_with(mock.ANY, mock.ANY)
        self.sched.kill.assert_called_with(mock.ANY, capture_blocks=mock.ANY, force=True)
        # Must not have created the subarray product internally
        self.assertIsNone(self.server.product)

    async def test_product_deconfigure(self) -> None:
        """Checks success path of product-deconfigure"""
        await self._configure_subarray(SUBARRAY_PRODUCT)
        await self.client.request("product-deconfigure")
        # Check that the graph was shut down
        self.sched.kill.assert_called_with(mock.ANY, capture_blocks=mock.ANY, force=False)
        # The server must shut itself down
        await self.server.join()

    async def test_product_deconfigure_capturing(self) -> None:
        """product-deconfigure must fail while capturing"""
        await self._configure_subarray(SUBARRAY_PRODUCT)
        await self.client.request("capture-init", CAPTURE_BLOCK)
        await self.assert_request_fails("product-deconfigure")

    async def test_product_deconfigure_capturing_force(self) -> None:
        """forced product-deconfigure must succeed while capturing"""
        await self._configure_subarray(SUBARRAY_PRODUCT)
        await self.client.request("capture-init", CAPTURE_BLOCK)
        await self.client.request("product-deconfigure", True)
        # The server must shut itself down
        await self.server.join()

    async def test_product_deconfigure_busy(self) -> None:
        """product-deconfigure cannot happen concurrently with capture-init"""
        await self._configure_subarray(SUBARRAY_PRODUCT)
        async with self._capture_init_slow(CAPTURE_BLOCK):
            await self.assert_request_fails('product-deconfigure', False)
        # Check that the subarray still exists and has the right state
        product = self.server.product
        assert product is not None       # mypy doesn't understand self.assertIsNotNone
        self.assertFalse(product.async_busy)
        self.assertEqual(ProductState.CAPTURING, product.state)

    async def test_product_deconfigure_busy_force(self) -> None:
        """forced product-deconfigure must succeed while in capture-init"""
        await self._configure_subarray(SUBARRAY_PRODUCT)
        async with self._capture_init_slow(CAPTURE_BLOCK, cancelled=True):
            await self.client.request("product-deconfigure", True)
        # Check that the graph was shut down
        self.sched.kill.assert_called_with(mock.ANY, capture_blocks=mock.ANY, force=True)
        # The server must shut itself down
        await self.server.join()

    async def test_product_deconfigure_while_configuring_force(self) -> None:
        """forced product-deconfigure must succeed while in product-configure"""
        async with self._product_configure_slow(SUBARRAY_PRODUCT, cancelled=True):
            await self.client.request("product-deconfigure", True)
        # Check that the graph was shut down
        self.sched.kill.assert_called_with(mock.ANY, capture_blocks=mock.ANY, force=True)
        # Verify the state
        self.assertIsNone(self.server.product)

    async def test_capture_init(self) -> None:
        """Checks that capture-init succeeds and sets appropriate state"""
        await self._configure_subarray(SUBARRAY_PRODUCT)
        await self.client.request("capture-init", CAPTURE_BLOCK)
        # check that the subarray is in an appropriate state
        product = self.server.product
        assert product is not None       # mypy doesn't understand self.assertIsNotNone
        self.assertFalse(product.async_busy)
        self.assertEqual(ProductState.CAPTURING, product.state)
        # Check that the graph transitions were called. Each call may be made
        # multiple times, depending on the number of instances of each child.
        # We thus collapse them into groups of equal calls and don't worry
        # about the number, which would otherwise make the test fragile.
        katcp_client = self.sensor_proxy_client_class.return_value
        grouped_calls = [k for k, g in itertools.groupby(katcp_client.request.mock_calls)]
        expected_calls = [
            mock.call('capture-init', CAPTURE_BLOCK),
            mock.call('capture-start', 'i0_baseline_correlation_products', mock.ANY)
        ]
        self.assertEqual(grouped_calls, expected_calls)

    async def test_capture_init_bad_json(self) -> None:
        """Check that capture-init fails if an override is illegal"""
        await self._configure_subarray(SUBARRAY_PRODUCT)
        with assert_raises(FailReply):
            await self.client.request("capture-init", CAPTURE_BLOCK, 'not json')

    async def test_capture_init_bad_override(self) -> None:
        """Check that capture-init fails if an override makes the config illegal"""
        await self._configure_subarray(SUBARRAY_PRODUCT)
        with assert_raises(FailReply):
            await self.client.request("capture-init", CAPTURE_BLOCK, '{"inputs": null}')

    async def test_capture_init_bad_override_change(self) -> None:
        """Check that capture-init fails if an override makes an invalid change"""
        await self._configure_subarray(SUBARRAY_PRODUCT)
        with assert_raises(FailReply):
            await self.client.request(
                "capture-init", CAPTURE_BLOCK,
                '{"inputs": {"camdata": {"url": "http://127.0.0.1:8888"}}}')

    async def test_capture_init_failed_req(self) -> None:
        """Capture-init fails on some task"""
        await self._configure_subarray(SUBARRAY_PRODUCT)
        self.fail_requests.add('capture-init')
        await self.assert_request_fails("capture-init", CAPTURE_BLOCK)
        # check that the subarray is in an appropriate state
        product = self.server.product
        assert product is not None       # mypy doesn't understand self.assertIsNotNone
        self.assertEqual(ProductState.ERROR, product.state)
        self.assertEqual({}, product.capture_blocks)
        # check that the subarray can be safely deconfigured, and that it
        # goes via DECONFIGURING state. Rather than trying to directly
        # observe the internal state during deconfigure (e.g. with
        # DelayedManager), we'll just observe the sensor
        state_observer = mock.Mock()
        self.server.sensors['state'].attach(state_observer)
        await self.client.request('product-deconfigure')
        # call 0, arguments, argument 1
        self.assertEqual(state_observer.mock_calls[0][1][1].value, ProductState.DECONFIGURING)
        self.assertEqual(ProductState.DEAD, product.state)

    async def test_capture_done_failed_req(self) -> None:
        """Capture-done fails on some task"""
        await self._configure_subarray(SUBARRAY_PRODUCT)
        self.fail_requests.add('capture-done')
        reply, informs = await self.client.request("capture-init", CAPTURE_BLOCK)
        await self.assert_request_fails("capture-done")
        # check that the subsequent transitions still run
        katcp_client = self.sensor_proxy_client_class.return_value
        katcp_client.request.assert_called_with('write-meta', CAPTURE_BLOCK, True)
        # check that the subarray is in an appropriate state
        product = self.server.product
        assert product is not None       # mypy doesn't understand self.assertIsNotNone
        self.assertEqual(ProductState.ERROR, product.state)
        self.assertEqual({}, product.capture_blocks)
        # check that the subarray can be safely deconfigured
        await self.client.request('product-deconfigure')
        self.assertEqual(ProductState.DEAD, product.state)

    async def _test_busy(self, command: str, *args: Any) -> None:
        """Test that a command fails if issued while ?capture-init or
        ?product-configure is in progress
        """
        async with self._product_configure_slow(SUBARRAY_PRODUCT):
            await self.assert_request_fails(command, *args)
        async with self._capture_init_slow(CAPTURE_BLOCK):
            await self.assert_request_fails(command, *args)

    async def test_capture_init_busy(self) -> None:
        """Capture-init fails if an asynchronous operation is already in progress"""
        await self._test_busy("capture-init", CAPTURE_BLOCK)

    def _ingest_died(self, subarray_product: SDPSubarrayProductBase) -> None:
        """Mark an ingest process as having died"""
        # The type signature uses SDPSubarrayProductBase so that the callers don't
        # all have to explicitly check that self.server.product is an
        # instance of SDPSubarrayProduct.
        assert isinstance(subarray_product, SDPSubarrayProduct)
        for node in subarray_product.physical_graph:
            if node.logical_node.name == 'ingest.sdp_l0.1':
                node.set_state(scheduler.TaskState.DEAD)
                node.status = Dict(state='TASK_FAILED')
                break
        else:
            raise ValueError('Could not find ingest node')

    def _ingest_bad_device_status(self, subarray_product: SDPSubarrayProductBase) -> None:
        """Mark an ingest process as having bad status"""
        sensor = self.server.sensors['ingest.sdp_l0.1.device-status']
        sensor.set_value(DeviceStatus.FAIL, Sensor.Status.ERROR)

    async def test_capture_init_dead_process(self) -> None:
        """Capture-init fails if a child process is dead."""
        await self.client.request("product-configure", SUBARRAY_PRODUCT, CONFIG)
        product = self.server.product
        assert product is not None       # mypy doesn't understand self.assertIsNotNone
        self._ingest_died(product)
        self.assertEqual(ProductState.ERROR, product.state)
        await self.assert_request_fails("capture-init", CAPTURE_BLOCK)
        # check that the subarray is in an appropriate state
        self.assertEqual(ProductState.ERROR, product.state)
        self.assertEqual({}, product.capture_blocks)

    async def test_capture_init_process_dies(self) -> None:
        """Capture-init fails if a child dies half-way through."""
        await self.client.request("product-configure", SUBARRAY_PRODUCT, CONFIG)
        product = self.server.product
        assert product is not None       # mypy doesn't understand self.assertIsNotNone
        with self.assertRaises(FailReply):
            async with self._capture_init_slow(CAPTURE_BLOCK):
                self._ingest_died(product)
        # check that the subarray is in an appropriate state
        self.assertEqual(ProductState.ERROR, product.state)
        self.assertEqual({}, product.capture_blocks)

    async def test_capture_done(self) -> None:
        """Checks that capture-done succeeds and sets appropriate state"""
        init_batch_started = get_metric(scheduler.BATCH_TASKS_STARTED)
        init_batch_done = get_metric(scheduler.BATCH_TASKS_DONE)
        init_batch_failed = get_metric(scheduler.BATCH_TASKS_FAILED)
        init_batch_skipped = get_metric(scheduler.BATCH_TASKS_SKIPPED)

        await self._configure_subarray(SUBARRAY_PRODUCT)
        await self.client.request("capture-init", CAPTURE_BLOCK)
        cal_sensor = self.server.sensors['cal.1.capture-block-state']
        cal_sensor.value = b'{"1122334455: "capturing"}'
        await self.client.request("capture-done")
        # check that the subarray is in an appropriate state
        product = self.server.product
        assert product is not None       # mypy doesn't understand self.assertIsNotNone
        self.assertFalse(product.async_busy)
        self.assertEqual(ProductState.IDLE, product.state)
        # Check that the graph transitions succeeded
        katcp_client = self.sensor_proxy_client_class.return_value
        katcp_client.request.assert_any_call('capture-done')
        katcp_client.request.assert_called_with('write-meta', CAPTURE_BLOCK, True)
        # Now simulate cal finishing with the capture block
        cal_sensor.value = b'{}'
        await asynctest.exhaust_callbacks(self.loop)
        # write-meta full dump must be last, hence assert_called_with not assert_any_call
        katcp_client.request.assert_called_with('write-meta', CAPTURE_BLOCK, False)

        # Check that postprocessing ran and didn't fail
        self.assertEqual(product.capture_blocks, {})
        started = get_metric(scheduler.BATCH_TASKS_STARTED) - init_batch_started
        done = get_metric(scheduler.BATCH_TASKS_DONE) - init_batch_done
        failed = get_metric(scheduler.BATCH_TASKS_FAILED) - init_batch_failed
        skipped = get_metric(scheduler.BATCH_TASKS_SKIPPED) - init_batch_skipped
        self.assertEqual(started, 9)    # 3 continuum, 3x2 spectral
        self.assertEqual(done, started)
        self.assertEqual(failed, 0)
        self.assertEqual(skipped, 0)

    async def test_capture_done_disable_batch(self) -> None:
        """Checks that capture-init with override takes effect"""
        init_batch_done = get_metric(scheduler.BATCH_TASKS_DONE)
        await self._configure_subarray(SUBARRAY_PRODUCT)
        await self.client.request(
            "capture-init", CAPTURE_BLOCK, '{"outputs": {"spectral_image": null}}')
        cal_sensor = self.server.sensors['cal.1.capture-block-state']
        cal_sensor.value = b'{"1122334455": "capturing"}'
        await self.client.request("capture-done")
        cal_sensor.value = b'{}'
        await asynctest.exhaust_callbacks(self.loop)
        done = get_metric(scheduler.BATCH_TASKS_DONE) - init_batch_done
        self.assertEqual(done, 3)    # 3 continuum, no spectral

    async def test_capture_done_busy(self):
        """Capture-done fails if an asynchronous operation is already in progress"""
        await self._test_busy("capture-done")

    async def _test_failure_while_capturing(
            self, failfunc: Callable[[SDPSubarrayProductBase], None]) -> None:
        await self.client.request("product-configure", SUBARRAY_PRODUCT, CONFIG)
        await self.client.request("capture-init", CAPTURE_BLOCK)
        product = self.server.product
        assert product is not None       # mypy doesn't understand self.assertIsNotNone
        self.assertEqual(ProductState.CAPTURING, product.state)
        failfunc(product)
        self.assertEqual(ProductState.ERROR, product.state)
        self.assertEqual({CAPTURE_BLOCK: mock.ANY}, product.capture_blocks)
        # In the background it will terminate the capture block
        await asynctest.exhaust_callbacks(self.loop)
        self.assertEqual({}, product.capture_blocks)
        katcp_client = self.sensor_proxy_client_class.return_value
        katcp_client.request.assert_called_with('write-meta', CAPTURE_BLOCK, False)

    async def test_process_dies_while_capturing(self) -> None:
        await self._test_failure_while_capturing(self._ingest_died)

    async def test_bad_device_status_while_capturing(self) -> None:
        await self._test_failure_while_capturing(self._ingest_bad_device_status)

    async def test_capture_done_process_dies(self) -> None:
        """Capture-done fails if a child dies half-way through."""
        await self.client.request("product-configure", SUBARRAY_PRODUCT, CONFIG)
        await self.client.request("capture-init", CAPTURE_BLOCK)
        product = self.server.product
        assert product is not None       # mypy doesn't understand self.assertIsNotNone
        self.assertEqual(ProductState.CAPTURING, product.state)
        self.assertEqual({CAPTURE_BLOCK: mock.ANY}, product.capture_blocks)
        with self.assertRaises(FailReply):
            async with self._capture_done_slow():
                self._ingest_died(product)
        # check that the subarray is in an appropriate state
        self.assertEqual(ProductState.ERROR, product.state)
        self.assertEqual({}, product.capture_blocks)

    async def test_deconfigure_on_stop(self) -> None:
        """Calling stop will force-deconfigure existing subarrays, even if capturing."""
        await self._configure_subarray(SUBARRAY_PRODUCT)
        await self.client.request('capture-init', CAPTURE_BLOCK)
        await self.server.stop()

        sensor_proxy_client = self.sensor_proxy_client_class.return_value
        sensor_proxy_client.request.assert_any_call('capture-done')
        # Forced deconfigure, so we only get the light dump
        sensor_proxy_client.request.assert_called_with('write-meta', mock.ANY, True)
        self.sched.kill.assert_called_with(mock.ANY, capture_blocks=mock.ANY, force=True)

    async def test_deconfigure_on_stop_busy(self) -> None:
        """Calling deconfigure_on_exit while a capture-init or capture-done is busy
        kills off the graph anyway.
        """
        await self._configure_subarray(SUBARRAY_PRODUCT)
        async with self._capture_init_slow(CAPTURE_BLOCK, cancelled=True):
            await self.server.stop()
        self.sched.kill.assert_called_with(mock.ANY, capture_blocks=mock.ANY, force=True)

    async def test_deconfigure_on_stop_cancel(self) -> None:
        """Calling deconfigure_on_exit while a configure is in process cancels
        that configure and kills off the graph."""
        async with self._product_configure_slow(SUBARRAY_PRODUCT, cancelled=True):
            await self.server.stop()
        # We must have killed off the partially-launched graph
        self.sched.kill.assert_called_with(mock.ANY, capture_blocks=mock.ANY, force=True)

    async def test_telstate_endpoint(self) -> None:
        """Test telstate-endpoint"""
        await self.assert_request_fails('telstate-endpoint')
        await self._configure_subarray(SUBARRAY_PRODUCT)
        reply, _ = await self.client.request('telstate-endpoint')
        self.assertEqual(reply, [b'host.telstate:20000'])

    async def test_capture_status(self) -> None:
        """Test capture-status"""
        await self.assert_request_fails('capture-status')
        await self._configure_subarray(SUBARRAY_PRODUCT)
        reply, _ = await self.client.request('capture-status')
        self.assertEqual(reply, [b'idle'])
        await self.client.request('capture-init', CAPTURE_BLOCK)
        reply, _ = await self.client.request('capture-status')
        self.assertEqual(reply, [b'capturing'])


class TestSDPResources(asynctest.TestCase):
    """Test :class:`katsdpcontroller.product_controller.SDPResources`."""
    async def setUp(self):
        mc_server = DummyMasterController('127.0.0.1', 0)
        await mc_server.start()
        self.addCleanup(mc_server.stop)
        assert mc_server.server and mc_server.server.sockets
        mc_address = mc_server.server.sockets[0].getsockname()
        mc_client = await aiokatcp.Client.connect(mc_address[0], mc_address[1])
        self.resources = SDPResources(mc_client, SUBARRAY_PRODUCT)

    async def test_get_multicast_groups(self):
        self.assertEqual('225.100.0.1', await self.resources.get_multicast_groups(1))
        self.assertEqual('225.100.0.2+3', await self.resources.get_multicast_groups(4))
