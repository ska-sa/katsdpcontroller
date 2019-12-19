"""Tests for :mod:`katsdpcontroller.product_controller."""

import unittest
from unittest import mock
import itertools
import json
import asyncio
# Needs to be imported this way so that it is unaffected by mocking of socket.getaddrinfo
from socket import getaddrinfo
import ipaddress
from urllib.parse import urljoin
import typing
from typing import List, Tuple, Set, Callable, Sequence, Mapping, Any, Optional

import asynctest
from aioresponses import aioresponses
from nose.tools import assert_raises
import numpy as np
from addict import Dict
import aiokatcp
from aiokatcp import Message, FailReply, Sensor
from aiokatcp.test.test_utils import timelimit
from prometheus_client import CollectorRegistry
import pymesos
import networkx
import netifaces
import katsdptelstate
import katpoint

from ..controller import device_server_sockname
from ..product_controller import (
    DeviceServer, SDPSubarrayProductBase, SDPSubarrayProduct, SDPResources,
    ProductState, DeviceStatus, _redact_keys, CONSUL_URL)
from .. import scheduler
from . import fake_katportalclient
from .utils import (create_patch, assert_request_fails, assert_sensors, DelayedManager,
                    CONFIG, S3_CONFIG, EXPECTED_INTERFACE_SENSOR_LIST,
                    EXPECTED_PRODUCT_CONTROLLER_SENSOR_LIST)


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
        return ans


class DummyDataSet:
    """Stub replacement for :class:`katdal.DataSet`."""

    def __init__(self):
        catalogue = katpoint.Catalogue()
        catalogue.add('PKS 1934-63, radec target, 19:39:25.03, -63:42:45.7')
        catalogue.add('3C286, radec, 13:31:08.29, +30:30:33.0,(800.0 43200.0 0.956 0.584 -0.1644)')
        # Two targets with the same name, to check disambiguation
        catalogue.add('My Target, radec target, 0:00:00.00, -10:00:00.0')
        catalogue.add('My Target, radec target, 0:00:00.00, -20:00:00.0')
        # Target that is only observed for 20 minutes: will produce continuum
        # but not spectral image.
        catalogue.add('Continuum, radec target, 0:00:00.00, -30:00:00.0')
        # Target that is set but never tracked
        catalogue.add('notrack, radec target, 0:00:00.00, -30:00:00.0')

        self.catalogue = catalogue
        self.dump_period = 8.0
        scan_state = []
        target_index = []
        for i in range(4):
            for j in range(1000):
                scan_state.append('track')
                target_index.append(i)
        for i in range(150):
            scan_state.append('track')
            target_index.append(4)
        for i in range(4, 6):
            for j in range(1000):
                scan_state.append('scan')
                target_index.append(i)
        self.sensor = {
            'Observation/scan_state': np.array(scan_state),
            'Observation/target_index': np.array(target_index)
        }


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
        mc_address = device_server_sockname(mc_server)
        mc_client = await aiokatcp.Client.connect(mc_address[0], mc_address[1])
        self.addCleanup(mc_client.wait_closed)
        self.addCleanup(mc_client.close)
        self.prometheus_registry = CollectorRegistry()

        self.server = DeviceServer(
            master_controller=mc_client, subarray_product_id=SUBARRAY_PRODUCT,
            prometheus_registry=self.prometheus_registry,
            **server_kwargs)
        # server.start will try to register with consul. Mock that out, and make sure it
        # fails (which will prevent it from trying to deregister on stop).
        with aioresponses() as m:
            m.put(urljoin(CONSUL_URL, '/v1/agent/service/register?replace-existing-checks=1'),
                  status=500)
            await self.server.start()
        self.addCleanup(self.server.stop)
        bind_address = device_server_sockname(self.server)
        self.client = await aiokatcp.Client.connect(bind_address[0], bind_address[1])
        self.addCleanup(self.client.wait_closed)
        self.addCleanup(self.client.close)

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
        create_patch(self, 'katportalclient.KATPortalClient', return_value=dummy_client)


@timelimit
class TestSDPControllerInterface(BaseTestSDPController):
    """Testing of the SDP controller in interface mode."""

    async def setUp(self) -> None:
        await super().setUp()
        image_resolver_factory = scheduler.ImageResolverFactory(scheduler.SimpleImageLookup('sdp'))
        await self.setup_server(host='127.0.0.1', port=0, sched=None,
                                batch_role='batch',
                                interface_mode=True,
                                localhost=True,
                                image_resolver_factory=image_resolver_factory,
                                s3_config=None)
        create_patch(self, 'time.time', return_value=123456789.5)

    async def test_capture_init(self) -> None:
        await assert_request_fails(self.client, "capture-init", CAPTURE_BLOCK)
        await self.client.request("product-configure", SUBARRAY_PRODUCT, CONFIG)
        await self.client.request("capture-init", CAPTURE_BLOCK)

        reply, informs = await self.client.request("capture-status")
        self.assertEqual(reply, [b"capturing"])
        await assert_request_fails(self.client, "capture-init", CAPTURE_BLOCK)

    async def test_interface_sensors(self) -> None:
        await assert_sensors(self.client, EXPECTED_PRODUCT_CONTROLLER_SENSOR_LIST)
        interface_changed_callback = mock.MagicMock()
        self.client.add_inform_callback('interface-changed', interface_changed_callback)
        await self.client.request("product-configure", SUBARRAY_PRODUCT, CONFIG)
        interface_changed_callback.assert_called_once_with(b'sensor-list')
        await assert_sensors(
            self.client,
            EXPECTED_PRODUCT_CONTROLLER_SENSOR_LIST + EXPECTED_INTERFACE_SENSOR_LIST)

        # Deconfigure and check that the server shuts down
        interface_changed_callback.reset_mock()
        await self.client.request("product-deconfigure")
        interface_changed_callback.assert_called_once_with(b'sensor-list')
        await self.client.wait_disconnected()
        self.client.remove_inform_callback('interface-changed', interface_changed_callback)

    async def test_capture_done(self) -> None:
        await assert_request_fails(self.client, "capture-done")
        await self.client.request("product-configure", SUBARRAY_PRODUCT, CONFIG)
        await assert_request_fails(self.client, "capture-done")

        await self.client.request("capture-init", CAPTURE_BLOCK)
        reply, informs = await self.client.request("capture-done")
        self.assertEqual(reply, [CAPTURE_BLOCK.encode()])
        await assert_request_fails(self.client, "capture-done")

    async def test_deconfigure_subarray_product(self) -> None:
        await assert_request_fails(self.client, "product-configure")
        await self.client.request("product-configure", SUBARRAY_PRODUCT, CONFIG)
        await self.client.request("capture-init", CAPTURE_BLOCK)
        # should not be able to deconfigure when not in idle state
        await assert_request_fails(self.client, "product-deconfigure")
        await self.client.request("capture-done")
        await self.client.request("product-deconfigure")
        # server should now shut itself down
        await self.client.wait_disconnected()

    async def test_configure_subarray_product(self) -> None:
        await assert_request_fails(self.client, "product-deconfigure")
        await self.client.request("product-configure", SUBARRAY_PRODUCT, CONFIG)
        # Can't reconfigure when already configured
        await assert_request_fails(self.client, "product-configure", SUBARRAY_PRODUCT, CONFIG)
        await self.client.request("product-deconfigure")

    async def test_help(self) -> None:
        reply, informs = await self.client.request('help')
        requests = [inform.arguments[0].decode('utf-8') for inform in informs]
        self.assertEqual(set(EXPECTED_REQUEST_LIST), set(requests))


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

    # The return annotation is deliberately vague because typeshed changed
    # its annotation at some point and so any more specific annotation will
    # error out on *some* version of mypy.
    def _getaddrinfo(self, host: str, *args, **kwargs) -> List[Any]:
        """Mock getaddrinfo that replaces all hosts with a dummy IP address"""
        if host.startswith('host'):
            host = '127.0.0.2'
        return getaddrinfo(host, *args, **kwargs)

    async def setUp(self) -> None:
        await super().setUp()
        # Future that is already resolved with no return value
        done_future: asyncio.Future[None] = asyncio.Future()
        done_future.set_result(None)
        create_patch(self, 'time.time', return_value=123456789.5)
        create_patch(self, 'socket.getaddrinfo', side_effect=self._getaddrinfo)
        # Mock TelescopeState's constructor to create an in-memory telstate
        orig_telstate_init = katsdptelstate.TelescopeState.__init__
        self.telstate: Optional[katsdptelstate.TelescopeState] = None

        def _telstate_init(obj, *args, **kwargs):
            self.telstate = obj
            orig_telstate_init(obj)
        create_patch(self, 'katsdptelstate.TelescopeState.__init__', side_effect=_telstate_init,
                     autospec=True)
        self.sensor_proxy_client_class = create_patch(
            self, 'katsdpcontroller.sensor_proxy.SensorProxyClient', autospec=True)
        sensor_proxy_client = self.sensor_proxy_client_class.return_value
        sensor_proxy_client.wait_connected.return_value = done_future
        sensor_proxy_client.wait_synced.return_value = done_future
        sensor_proxy_client.wait_closed.return_value = done_future
        sensor_proxy_client.request.side_effect = self._request
        create_patch(
            self, 'katsdpcontroller.scheduler.poll_ports', autospec=True, return_value=done_future)
        create_patch(self, 'netifaces.interfaces', autospec=True, return_value=['lo', 'em1'])
        create_patch(self, 'netifaces.ifaddresses', autospec=True, side_effect=self._ifaddresses)
        self.sched = mock.create_autospec(spec=scheduler.Scheduler, instance=True)
        self.sched.launch.side_effect = self._launch
        self.sched.kill.side_effect = self._kill
        self.sched.batch_run.side_effect = self._batch_run
        self.n_batch_tasks = 0
        self.sched.close.return_value = done_future
        self.sched.http_url = 'http://scheduler:8080/'
        self.sched.http_port = 8080
        self.sched.task_stats = scheduler.TaskStats()
        self.driver = mock.create_autospec(spec=pymesos.MesosSchedulerDriver, instance=True)
        await self.setup_server(
            host='127.0.0.1', port=0, sched=self.sched,
            s3_config=json.loads(S3_CONFIG),
            image_resolver_factory=scheduler.ImageResolverFactory(
                scheduler.SimpleImageLookup('sdp')),
            interface_mode=False,
            localhost=True,
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
        create_patch(self, 'katsdpcontroller.generator._get_data_set',
                     return_value=DummyDataSet())

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
        self.n_batch_tasks += len(nodes)

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
            'use_data_suspect': True,
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
        await assert_request_fails(self.client, *self._configure_args(SUBARRAY_PRODUCT))
        self.sched.launch.assert_called_with(mock.ANY, mock.ANY, mock.ANY)
        self.sched.kill.assert_called_with(mock.ANY, capture_blocks=mock.ANY, force=True)
        # Must not have created the subarray product internally
        self.assertIsNone(self.server.product)

    async def test_product_configure_task_fail(self) -> None:
        """If a task other than telstate fails, product-configure must fail"""
        self.fail_launches['ingest.sdp_l0.1'] = 'TASK_FAILED'
        await assert_request_fails(self.client, *self._configure_args(SUBARRAY_PRODUCT))
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
        await assert_request_fails(self.client, "product-deconfigure")

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
            await assert_request_fails(self.client, 'product-deconfigure', False)
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
        await assert_request_fails(self.client, "capture-init", CAPTURE_BLOCK)
        # check that the subarray is in an appropriate state
        product = self.server.product
        assert product is not None       # mypy doesn't understand self.assertIsNotNone
        self.assertEqual(ProductState.ERROR, product.state)
        self.assertEqual(ProductState.ERROR, self.server.sensors['state'].value)
        self.assertEqual(DeviceStatus.FAIL, self.server.sensors['device-status'].value)
        self.assertEqual(Sensor.Status.ERROR, self.server.sensors['device-status'].status)
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
        await assert_request_fails(self.client, "capture-done")
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
            await assert_request_fails(self.client, command, *args)
        async with self._capture_init_slow(CAPTURE_BLOCK):
            await assert_request_fails(self.client, command, *args)

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
        await assert_request_fails(self.client, "capture-init", CAPTURE_BLOCK)
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
        self.assertEqual(self.n_batch_tasks, 10)    # 4 continuum, 3x2 spectral

    async def test_capture_done_disable_batch(self) -> None:
        """Checks that capture-init with override takes effect"""
        await self._configure_subarray(SUBARRAY_PRODUCT)
        await self.client.request(
            "capture-init", CAPTURE_BLOCK, '{"outputs": {"spectral_image": null}}')
        cal_sensor = self.server.sensors['cal.1.capture-block-state']
        cal_sensor.value = b'{"1122334455": "capturing"}'
        await self.client.request("capture-done")
        cal_sensor.value = b'{}'
        await asynctest.exhaust_callbacks(self.loop)
        self.assertEqual(self.n_batch_tasks, 4)    # 4 continuum, no spectral

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
        await assert_request_fails(self.client, 'telstate-endpoint')
        await self._configure_subarray(SUBARRAY_PRODUCT)
        reply, _ = await self.client.request('telstate-endpoint')
        self.assertEqual(reply, [b'host.telstate:20000'])

    async def test_capture_status(self) -> None:
        """Test capture-status"""
        await assert_request_fails(self.client, 'capture-status')
        await self._configure_subarray(SUBARRAY_PRODUCT)
        reply, _ = await self.client.request('capture-status')
        self.assertEqual(reply, [b'idle'])
        await self.client.request('capture-init', CAPTURE_BLOCK)
        reply, _ = await self.client.request('capture-status')
        self.assertEqual(reply, [b'capturing'])

    def _check_prom(self, name: str, service: str, type: str, value: float,
                    sample_name: str = None, extra_labels: Mapping[str, str] = None) -> None:
        name = 'katsdpcontroller_' + name
        registry = self.prometheus_registry
        for metric in registry.collect():
            if metric.name == name:
                break
        else:
            raise KeyError(f'Metric {name} not found')
        self.assertEqual(metric.type, type)
        labels = {'subarray_product_id': SUBARRAY_PRODUCT, 'service': service}
        if sample_name is None:
            sample_name = name
        else:
            sample_name = 'katsdpcontroller_' + sample_name
        if extra_labels is not None:
            labels.update(extra_labels)
        self.assertEqual(registry.get_sample_value(sample_name, labels), value)

    async def test_prom_sensors(self) -> None:
        """Test that sensors are mirrored into Prometheus."""
        self.server.sensors.add(aiokatcp.Sensor(
            float, 'foo.gauge', '(prometheus: gauge)', default=1.5,
            initial_status=Sensor.Status.NOMINAL))
        self.server.sensors.add(aiokatcp.Sensor(
            float, 'foo.cheese.labelled-gauge', '(prometheus: gauge labels: type)', default=1,
            initial_status=Sensor.Status.NOMINAL))
        self.server.sensors.add(aiokatcp.Sensor(
            int, 'foo.histogram', '(prometheus: histogram(1, 10, 100))'))
        self._check_prom('gauge', 'foo', 'gauge', 1.5)
        self._check_prom('labelled_gauge', 'foo', 'gauge', 1, extra_labels={'type': 'cheese'})
        self._check_prom('histogram', 'foo', 'histogram', 0, 'histogram_bucket', {'le': '10.0'})


class TestSDPResources(asynctest.TestCase):
    """Test :class:`katsdpcontroller.product_controller.SDPResources`."""

    async def setUp(self):
        mc_server = DummyMasterController('127.0.0.1', 0)
        await mc_server.start()
        self.addCleanup(mc_server.stop)
        mc_address = device_server_sockname(mc_server)
        mc_client = await aiokatcp.Client.connect(mc_address[0], mc_address[1])
        self.resources = SDPResources(mc_client, SUBARRAY_PRODUCT)

    async def test_get_multicast_groups(self):
        self.assertEqual('225.100.0.1', await self.resources.get_multicast_groups(1))
        self.assertEqual('225.100.0.2+3', await self.resources.get_multicast_groups(4))
