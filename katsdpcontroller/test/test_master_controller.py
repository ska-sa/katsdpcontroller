"""Tests for :mod:`katsdpcontroller.master_controller."""

import asyncio
import logging
import json
import functools
import ipaddress
from unittest import mock
from typing import Set, List

import aiokatcp
import prometheus_client
import asynctest
import open_file_mock

from .. import master_controller
from ..controller import ProductState
from ..master_controller import (ProductFailed, Product, SingularityProduct,
                                 SingularityProductManager, NoAddressesError,
                                 parse_args)
from ..sensor_proxy import SensorProxyClient
from . import fake_zk, fake_singularity
from .utils import create_patch, device_server_sockname


S3_CONFIG_JSON = '''
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
}'''


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
            'Total input bytes (prometheus: gauge)',
            default=42,
            initial_status=aiokatcp.Sensor.Status.NOMINAL))
        self.requests: List[aiokatcp.Message] = []

    async def unhandled_request(self, ctx: aiokatcp.RequestContext, req: aiokatcp.Message) -> None:
        self.requests.append(req)

    async def request_capture_status(self, ctx: aiokatcp.RequestContext) -> str:
        """Get product status"""
        return 'idle'


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
        with mock.patch('aiozk.ZKClient', fake_zk.ZKClient):
            self.manager = SingularityProductManager(self.args, self.server)
        self.sensor_proxy_client_mock = \
            create_patch(self, 'katsdpcontroller.sensor_proxy.SensorProxyClient', autospec=True)
        self.open_mock = create_patch(self, 'builtins.open', new_callable=open_file_mock.MockOpen)
        self.open_mock.default_behavior = open_file_mock.DEFAULTS_ORIGINAL
        self.open_mock.set_read_data_for('s3_config.json', S3_CONFIG_JSON)

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
        await self.start_manager()
        # Disable the mocking by making the real version the side effect
        self.sensor_proxy_client_mock.side_effect = SensorProxyClient
        product = await self.start_product(name='product1', lifecycle=katcp_server_lifecycle)
        self.assertEqual(await product.get_state(), ProductState.IDLE)
        self.assertEqual(self.server.sensors['product1.ingest.sdp_l0.1.input-bytes-total'].value,
                         42)
        prom_value = prometheus_client.REGISTRY.get_sample_value(
            'katsdpcontroller_input_bytes_total',
            {'subarray_product_id': 'product1', 'service': 'ingest.sdp_l0.1'})
        self.assertEqual(prom_value, 42)

