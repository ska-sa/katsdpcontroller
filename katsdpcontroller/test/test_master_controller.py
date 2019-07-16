"""Tests for :mod:`katsdpcontroller.master_controller."""

import asyncio
import logging
import json
import functools
import ipaddress
import os
from unittest import mock
from typing import Tuple, Set, List, Mapping

import aiokatcp
import prometheus_client
import asynctest
import open_file_mock

from .. import master_controller
from ..controller import ProductState, device_server_sockname
from ..master_controller import (ProductFailed, Product, SingularityProduct,
                                 SingularityProductManager, NoAddressesError,
                                 DeviceServer, parse_args)
from ..sensor_proxy import SensorProxyClient
from . import fake_zk, fake_singularity
from .utils import (create_patch, assert_request_fails, assert_sensors, assert_sensor_value,
                    CONFIG, S3_CONFIG, EXPECTED_INTERFACE_SENSOR_LIST)


EXPECTED_SENSOR_LIST: Tuple[Tuple[bytes, ...], ...] = (
    (b'api-version', b'', b'string'),
    (b'build-state', b'', b'string'),
    (b'device-status', b'', b'discrete', b'ok', b'degraded', b'fail'),
    (b'fmeca.FD0001', b'', b'boolean'),
    (b'time-synchronised', b'', b'boolean'),
    (b'gui-urls', b'', b'string'),
    (b'products', b'', b'string')
)

EXPECTED_REQUEST_LIST = [
    'product-configure',
    'product-deconfigure',
    'product-reconfigure',
    'product-list',
    'set-config-override',
    'capture-done',
    'capture-init',
    'capture-status',
    'telstate-endpoint',
    # Internal commands
    'get-multicast-groups',
    'image-lookup',
    # Standard katcp commands
    'client-list', 'halt', 'help', 'log-level', 'sensor-list',
    'sensor-sampling', 'sensor-value', 'watchdog', 'version-list'
]


class DummyServer(aiokatcp.DeviceServer):
    VERSION = 'dummy-1.0'
    BUILD_STATE = VERSION


class DummyProductController(aiokatcp.DeviceServer):
    VERSION = 'dummy-product-controller-1.0'
    BUILD_STATE = VERSION

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.sensors.add(aiokatcp.Sensor(
            int, 'ingest.sdp_l0.1.input-bytes-total',
            'Total input bytes (prometheus: counter)',
            default=42,
            initial_status=aiokatcp.Sensor.Status.NOMINAL))
        self.sensors.add(aiokatcp.Sensor(
            float, 'foo.gauge', '(prometheus: gauge)', default=1.5,
            initial_status=aiokatcp.Sensor.Status.NOMINAL))
        self.sensors.add(aiokatcp.Sensor(
            int, 'foo.histogram', '(prometheus: histogram(1, 10, 100))'))
        self.requests: List[aiokatcp.Message] = []

    async def unhandled_request(self, ctx: aiokatcp.RequestContext, req: aiokatcp.Message) -> None:
        self.requests.append(req)

    async def request_capture_status(self, ctx: aiokatcp.RequestContext) -> str:
        """Get product status"""
        return 'idle'

    async def request_telstate_endpoint(self, ctx: aiokatcp.RequestContext) -> str:
        """Get the telescope state endpoint"""
        return 'telstate.invalid:31000'


async def quick_death_lifecycle(task: fake_singularity.Task) -> None:
    """Task dies instantly"""
    task.state = fake_singularity.TaskState.DEAD


async def death_after_task_id_lifecycle(init_wait: float, task: fake_singularity.Task) -> None:
    """Task dies as soon as the client sees the task ID."""
    await asyncio.sleep(init_wait)
    task.state = fake_singularity.TaskState.NOT_YET_HEALTHY
    await task.task_id_known.wait()
    task.state = fake_singularity.TaskState.DEAD


async def spontaneous_death_lifecycle(task: fake_singularity.Task) -> None:
    """Task dies after 1000s of life"""
    await fake_singularity.default_lifecycle(
        task, times={fake_singularity.TaskState.HEALTHY: 1000.0})


async def long_pending_lifecycle(task: fake_singularity.Task) -> None:
    """Task takes longer than the timeout to make it out of pending state"""
    await fake_singularity.default_lifecycle(
        task, times={fake_singularity.TaskState.PENDING: 1000.0})


async def katcp_server_lifecycle(task: fake_singularity.Task) -> None:
    """The default lifecycle, but creates a real katcp port"""
    server = DummyProductController('127.0.0.1', 0)
    await server.start()
    try:
        task.host, task.ports[0] = device_server_sockname(server)
        await fake_singularity.default_lifecycle(task)
    finally:
        await server.stop()


class TestSingularityProductManager(asynctest.ClockedTestCase):
    async def setUp(self) -> None:
        self.singularity_server = fake_singularity.SingularityServer()
        await self.singularity_server.start()
        self.addCleanup(self.singularity_server.close)
        self.server = DummyServer('127.0.0.1', 0)
        await self.server.start()
        self.addCleanup(self.server.stop)
        self.args = parse_args([
            '--host', '127.0.0.1',
            '--port', str(device_server_sockname(self.server)[1]),
            '--name', 'sdpmc_test',
            '--external-hostname', 'me.invalid',
            '--s3-config-file', 's3_config.json',
            '--safe-multicast-cidr', '225.100.0.0/24',
            'zk.invalid:2181', self.singularity_server.root_url
        ])
        self.prometheus_registry = prometheus_client.CollectorRegistry()
        with mock.patch('aiozk.ZKClient', fake_zk.ZKClient):
            self.manager = SingularityProductManager(self.args, self.server,
                                                     self.prometheus_registry)
        self.sensor_proxy_client_mock = \
            create_patch(self, 'katsdpcontroller.sensor_proxy.SensorProxyClient', autospec=True)
        self.open_mock = create_patch(self, 'builtins.open', new_callable=open_file_mock.MockOpen)
        self.open_mock.default_behavior = open_file_mock.DEFAULTS_ORIGINAL
        self.open_mock.set_read_data_for('s3_config.json', S3_CONFIG)

    async def start_manager(self) -> None:
        """Start the manager and arrange for it to be stopped"""
        await self.manager.start()
        self.addCleanup(self.stop_manager)

    async def stop_manager(self) -> None:
        """Stop the manager.

        This is used as a cleanup function rather than ``self.manager.stop``
        directly, because it binds to the manager set at the time of call
        whereas ``self.manager.stop`` binds immediately.
        """
        await self.manager.stop()

    async def get_zk_state(self) -> dict:
        payload = (await self.manager._zk.get('/state'))[0]
        return json.loads(payload)

    async def test_start_clean(self) -> None:
        await self.start_manager()
        await self.advance(30)     # Runs reconciliation a few times

    async def test_create_product(self) -> None:
        await self.start_manager()
        task = self.loop.create_task(self.manager.create_product('foo'))
        await self.advance(100)
        self.assertTrue(task.done())
        product = await task
        self.assertEqual(product.task_state, Product.TaskState.STARTING)
        self.assertEqual(product.host, 'slave.invalid')
        self.assertEqual(product.port, 12345)
        self.sensor_proxy_client_mock.assert_called_with(
            self.server, 'foo.', {'subarray_product_id': 'foo'}, mock.ANY,
            host='slave.invalid', port=12345)

        await self.manager.product_active(product)
        self.assertEqual(product.task_state, Product.TaskState.ACTIVE)

    async def test_create_product_dies_fast(self) -> None:
        """Task dies before we observe it running"""
        await self.start_manager()
        self.singularity_server.lifecycles.append(quick_death_lifecycle)
        task = self.loop.create_task(self.manager.create_product('foo'))
        await self.advance(100)
        self.assertTrue(task.done())
        with self.assertRaises(ProductFailed):
            await task
        self.assertEqual(self.manager.products, {})

    async def _test_create_product_dies_after_task_id(self, init_wait: float) -> None:
        """Task dies immediately after we learn its task ID

        This test is parametrised so that we can control whether the task ID is
        learnt during polling for the new task or during task reconciliation.
        """
        await self.start_manager()
        self.singularity_server.lifecycles.append(
            functools.partial(death_after_task_id_lifecycle, init_wait))
        task = self.loop.create_task(self.manager.create_product('foo'))
        await self.advance(100)
        self.assertTrue(task.done())
        with self.assertRaises(ProductFailed):
            await task
        self.assertEqual(self.manager.products, {})

    async def test_create_product_dies_after_task_id_reconciliation(self) -> None:
        """Task dies immediately after we learn its task ID during reconciliation"""
        await self._test_create_product_dies_after_task_id(self.manager.reconciliation_interval)

    async def test_create_product_dies_after_task_id_poll(self) -> None:
        """Task dies immediately after we learn its task ID during polling"""
        await self._test_create_product_dies_after_task_id(self.manager.new_task_poll_interval)

    async def start_product(
            self, name: str = 'foo',
            lifecycle: fake_singularity.Lifecycle = None) -> SingularityProduct:
        if lifecycle:
            self.singularity_server.lifecycles.append(lifecycle)
        task = self.loop.create_task(self.manager.create_product(name))
        await self.advance(100)
        self.assertTrue(task.done())
        product = await task
        await self.manager.product_active(product)
        return product

    async def reset_manager(self) -> None:
        """Throw away the manager and create a new one"""
        zk = self.manager._zk
        await self.manager.stop()
        # We don't model ephemeral nodes in fake_zk, so have to delete manually
        await zk.delete('/running')
        with mock.patch('aiozk.ZKClient', return_value=zk):
            self.manager = SingularityProductManager(self.args, self.server)
        await self.manager.start()

    async def test_persist(self) -> None:
        await self.start_manager()
        product = await self.start_product()
        await self.reset_manager()
        self.assertEqual(self.manager.products, {'foo': mock.ANY})

        product2 = self.manager.products['foo']
        self.assertEqual(product.task_state, product2.task_state)
        self.assertEqual(product.run_id, product2.run_id)
        self.assertEqual(product.task_id, product2.task_id)
        self.assertEqual(product.multicast_groups, product2.multicast_groups)
        self.assertEqual(product.host, product2.host)
        self.assertEqual(product.port, product2.port)
        self.assertIsNotNone(product2.katcp_conn)
        self.assertIsNot(product, product2)   # Must be reconstituted from state

    async def test_spontaneous_death(self) -> None:
        """Product must be cleaned up if it dies on its own"""
        await self.start_manager()
        product = await self.start_product(lifecycle=spontaneous_death_lifecycle)
        # Check that Zookeeper initially knows about the product
        self.assertEqual((await self.get_zk_state())['products'], {'foo': mock.ANY})

        await self.advance(1000)   # Task will die during this time
        self.assertEqual(product.task_state, Product.TaskState.DEAD)
        self.assertEqual(self.manager.products, {})
        # Check that Zookeeper was updated
        self.assertEqual((await self.get_zk_state())['products'], {})

    async def test_stuck_pending(self) -> None:
        """Task takes a long time to be launched.

        The configure gets cancelled before then, and reconciliation must
        clean up the task.
        """
        await self.start_manager()
        self.singularity_server.lifecycles.append(long_pending_lifecycle)
        task = self.loop.create_task(self.manager.create_product('foo'))
        await self.advance(500)
        self.assertFalse(task.done())
        task.cancel()

        await self.advance(1000)
        self.assertTrue(task.done())
        with self.assertRaises(asyncio.CancelledError):
            await task

    async def test_reuse_deploy(self) -> None:
        await self.start_manager()
        product = await self.start_product()
        deploy_id = list(self.singularity_server.requests.values())[0].active_deploy.deploy_id

        # Reuse, without restarting the manager
        await self.manager.kill_product(product)
        # Give it time to die
        await self.advance(100)
        product = await self.start_product()
        new_deploy_id = list(self.singularity_server.requests.values())[0].active_deploy.deploy_id
        self.assertEqual(new_deploy_id, deploy_id)

        # Reuse, after a restart
        await self.manager.kill_product(product)
        # Give it time to die
        await self.advance(100)
        await self.reset_manager()
        product = await self.start_product()
        new_deploy_id = list(self.singularity_server.requests.values())[0].active_deploy.deploy_id
        self.assertEqual(new_deploy_id, deploy_id)

        # Alter the necessary state to ensure that a new deploy is used
        await self.manager.kill_product(product)
        await self.advance(100)
        with mock.patch.dict(os.environ, {'KATSDP_LOG_LEVEL': 'test'}):
            product = await self.start_product()
        new_deploy_id = list(self.singularity_server.requests.values())[0].active_deploy.deploy_id
        self.assertNotEqual(new_deploy_id, deploy_id)

    async def _test_bad_zk(self, payload: bytes) -> None:
        """Existing state data in Zookeeper is not valid"""
        await self.manager._zk.create('/state', payload)
        with self.assertLogs(master_controller.logger, logging.WARNING) as cm:
            await self.start_manager()
        self.assertRegex(cm.output[0], '.*:Could not load existing state')

    async def test_bad_zk_version(self) -> None:
        """Wrong version in state stored in Zookeeper"""
        await self._test_bad_zk(json.dumps({"version": 200000}).encode())

    async def test_bad_zk_json(self) -> None:
        """Data in Zookeeper is not valid JSON"""
        await self._test_bad_zk(b'I am not JSON')

    async def test_bad_zk_utf8(self) -> None:
        """Data in Zookeeper is not valid UTF-8"""
        await self._test_bad_zk(b'\xff')

    async def test_bad_zk_schema(self) -> None:
        """Data in Zookeeper does not conform to schema"""
        await self._test_bad_zk(json.dumps({"version": 1}).encode())

    async def test_bad_s3_config(self) -> None:
        await self.start_manager()
        self.open_mock.unregister_path('s3_config.json')
        self.open_mock.set_read_data_for('s3_config.json', 'I am not JSON')
        task = self.loop.create_task(self.manager.create_product('foo'))
        await self.advance(100)
        self.assertTrue(task.done())
        with self.assertRaises(ProductFailed):
            await task

    async def test_get_multicast_groups(self) -> None:
        await self.start_manager()
        product1 = await self.start_product('product1')
        product2 = await self.start_product('product2')
        self.assertEqual(await self.manager.get_multicast_groups(product1, 1), '225.100.0.1')
        self.assertEqual(await self.manager.get_multicast_groups(product1, 4), '225.100.0.2+3')
        with self.assertRaises(NoAddressesError):
            await self.manager.get_multicast_groups(product1, 1000)

        self.assertEqual(await self.manager.get_multicast_groups(product2, 128), '225.100.0.6+127')
        # Now product1 owns .1-.5, product2 owns .6-.133.
        await self.manager.kill_product(product2)
        await self.advance(100)   # Give it time to clean up

        # Allocations should continue from where they left off, then cycle
        # around to reuse the space freed by product2.
        self.assertEqual(await self.manager.get_multicast_groups(product1, 100), '225.100.0.134+99')
        self.assertEqual(await self.manager.get_multicast_groups(product1, 100), '225.100.0.6+99')

    async def test_get_multicast_groups_persist(self) -> None:
        await self.test_get_multicast_groups()
        await self.reset_manager()
        product1 = self.manager.products['product1']
        expected: Set[ipaddress.IPv4Address] = set()
        for i in range(105):
            expected.add(ipaddress.IPv4Address('225.100.0.1') + i)
        for i in range(100):
            expected.add(ipaddress.IPv4Address('225.100.0.134') + i)
        self.assertEqual(product1.multicast_groups, expected)
        self.assertEqual(self.manager._next_multicast_group,
                         ipaddress.IPv4Address('225.100.0.106'))

    async def test_get_multicast_groups_negative(self) -> None:
        await self.start_manager()
        product = await self.start_product()
        with self.assertRaises(ValueError):
            await self.manager.get_multicast_groups(product, -1)
        with self.assertRaises(ValueError):
            await self.manager.get_multicast_groups(product, 0)

    @asynctest.patch('time.time')
    async def test_capture_block_id(self, mock_time) -> None:
        await self.start_manager()
        mock_time.return_value = 1122334455.123
        self.assertEqual(await self.manager.get_capture_block_id(), '1122334455')
        self.assertEqual(await self.manager.get_capture_block_id(), '1122334456')
        # Must still be monotonic, even if time.time goes backwards
        mock_time.return_value -= 10
        self.assertEqual(await self.manager.get_capture_block_id(), '1122334457')
        # Once time.time goes past next, must use that again
        mock_time.return_value = 1122334460.987
        self.assertEqual(await self.manager.get_capture_block_id(), '1122334460')

        # Must persist state over restarts
        await self.reset_manager()
        self.assertEqual(await self.manager.get_capture_block_id(), '1122334461')

    async def test_katcp(self) -> None:
        def check_prom(name: str, service: str, type: str, value: float,
                       sample_name: str = None, extra_labels: Mapping[str, str] = None) -> None:
            name = 'katsdpcontroller_' + name
            registry = self.prometheus_registry
            for metric in registry.collect():
                if metric.name == name:
                    break
            else:
                raise KeyError(f'Metric {name} not found')
            self.assertEqual(metric.type, type)
            labels = {'subarray_product_id': 'product1', 'service': service}
            if sample_name is None:
                sample_name = name
            else:
                sample_name = 'katsdpcontroller_' + sample_name
            if extra_labels is not None:
                labels.update(extra_labels)
            self.assertEqual(registry.get_sample_value(sample_name, labels), value)

        await self.start_manager()
        # Disable the mocking by making the real version the side effect
        self.sensor_proxy_client_mock.side_effect = SensorProxyClient
        self.singularity_server.lifecycles.append(katcp_server_lifecycle)
        task = self.loop.create_task(self.manager.create_product('product1'))
        await self.advance(100)
        self.assertTrue(task.done())
        product = await task

        # We haven't called product_active yet, so it should still be CONFIGURING
        self.assertEqual(await product.get_state(), ProductState.CONFIGURING)
        self.assertEqual(await product.get_telstate_endpoint(), '')

        await self.manager.product_active(product)
        self.assertEqual(await product.get_state(), ProductState.IDLE)
        self.assertEqual(await product.get_telstate_endpoint(), 'telstate.invalid:31000')
        self.assertEqual(self.server.sensors['product1.ingest.sdp_l0.1.input-bytes-total'].value,
                         42)
        check_prom('input_bytes_total', 'ingest.sdp_l0.1', 'counter', 42)
        check_prom('gauge', 'foo', 'gauge', 1.5)
        check_prom('histogram', 'foo', 'histogram', 0, 'histogram_bucket', {'le': '10.0'})

        # Have the remote katcp server tell us it is going away. This also
        # provides test coverage of this shutdown path.
        await product.katcp_conn.request('halt')
        await product.dead_event.wait()
        self.assertEqual(await product.get_state(), ProductState.DEAD)
        self.assertEqual(await product.get_telstate_endpoint(), '')


class TestDeviceServer(asynctest.TestCase):
    """Tests for :class:`.master_controller.DeviceServer`.

    The tests use interface mode, because that avoids the complications of
    emulating Singularity and Zookeeper, and allows interaction with a mostly
    real product controller.
    """
    async def setUp(self):
        self.args = parse_args([
            '--localhost',
            '--interface-mode',
            '--port', '0',
            '--name', 'sdpmc_test',
            '--safe-multicast-cidr', '225.100.0.0/24',
            'unused argument (zk)', 'unused argument (Singularity)'
        ])
        self.server = DeviceServer(self.args)
        await self.server.start()
        self.addCleanup(self.server.stop)
        host, port = device_server_sockname(self.server)
        self.client = aiokatcp.Client(host, port)
        await self.client.wait_connected()
        self.addCleanup(self.client.wait_closed)
        self.addCleanup(self.client.close)

    async def test_capture_init(self):
        await assert_request_fails(self.client, "capture-init", "product")
        await self.client.request("product-configure", "product", CONFIG)
        reply, informs = await self.client.request("capture-init", "product")
        self.assertEqual(reply, [b"0000000002"])

        reply, informs = await self.client.request("capture-status", "product")
        self.assertEqual(reply, [b"capturing"])
        await assert_request_fails(self.client, "capture-init", "product")

    async def test_interface_sensors(self):
        await assert_sensors(self.client, EXPECTED_SENSOR_LIST)
        await assert_sensor_value(self.client, 'products', '[]')
        interface_changed_callback = mock.MagicMock()
        self.client.add_inform_callback('interface-changed', interface_changed_callback)
        await self.client.request('product-configure', 'product', CONFIG)
        interface_changed_callback.assert_called_once_with([b'sensor-list'])
        # Prepend the subarray product ID to the names
        expected_interface_sensors = tuple(('product.' + s[0]) + s[1:]
                                           for s in EXPECTED_INTERFACE_SENSOR_LIST)
        await assert_sensors(self.client, EXPECTED_SENSOR_LIST + expected_interface_sensors)
        await assert_sensor_value(self.client, 'products', '["product"]')

        # Deconfigure and check that the array sensors are gone
        interface_changed_callback.reset_mock()
        await self.client.request('product-deconfigure', 'product')
        interface_changed_callback.assert_called_once_with([b'sensor-list'])
        await assert_sensors(self.client, EXPECTED_SENSOR_LIST)
        await assert_sensor_value(self.client, 'products', '[]')

    async def test_capture_done(self):
        await assert_request_fails(self.client, "capture-done", "product")
        await self.client.request("product-configure", "product", CONFIG)
        await assert_request_fails(self.client, "capture-done", "product")

        await self.client.request("capture-init", "product")
        reply, informs = await self.client.request("capture-done", "product")
        self.assertEqual(reply, [b"0000000001"])
        await assert_request_fails(self.client, "capture-done", "product")

    async def test_deconfigure_subarray_product(self):
        await assert_request_fails(self.client, "product-configure", "product")
        await self.client.request("product-configure", "product", CONFIG)
        await self.client.request("capture-init", "product")
        # should not be able to deconfigure when not in idle state
        await assert_request_fails(self.client, "product-deconfigure", "product")
        await self.client.request("capture-done", "product")
        await self.client.request("product-deconfigure", "product")

    async def test_configure_subarray_product(self):
        await assert_request_fails(self.client, "product-deconfigure", "product")
        await self.client.request("product-list")
        await self.client.request("product-configure", "product", CONFIG)
        # Cannot configure an already-configured array
        await assert_request_fails(self.client, "product-configure", "product", CONFIG)

        reply, informs = await self.client.request('product-list')
        self.assertEqual(reply, [b'1'])

    async def test_reconfigure_subarray_product(self):
        await assert_request_fails(self.client, "product-reconfigure", "product")
        await self.client.request("product-configure", "product", CONFIG)
        await self.client.request("product-reconfigure", "product")
        await self.client.request("capture-init", "product")
        await assert_request_fails(self.client, "product-reconfigure", "product")

    async def test_help(self):
        reply, informs = await self.client.request('help')
        requests = [inform.arguments[0].decode('utf-8') for inform in informs]
        self.assertEqual(set(EXPECTED_REQUEST_LIST), set(requests))
