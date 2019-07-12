"""Tests for :mod:`katsdpcontroller.master_controller."""

import logging
import json
from unittest import mock

import aiokatcp
import asynctest
import open_file_mock

from .. import master_controller
from ..master_controller import (ProductFailed, Product, SingularityProduct,
                                 SingularityProductManager, parse_args)
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
    BUILD_STATE = 'dummy-1.0'


async def quick_death_lifecycle(task: fake_singularity.Task) -> None:
    """Task dies instantly"""
    task.state = fake_singularity.TaskState.DEAD


async def death_after_task_id_lifecycle(task: fake_singularity.Task) -> None:
    """Task dies as soon as the client sees the task ID."""
    task.state = fake_singularity.TaskState.NOT_YET_HEALTHY
    await task.task_id_known.wait()
    task.state = fake_singularity.TaskState.DEAD


async def spontaneous_death_lifecycle(task: fake_singularity.Task) -> None:
    """Task dies after 1000s of life"""
    await fake_singularity.default_lifecycle(
        task, times={fake_singularity.TaskState.HEALTHY: 1000.0})


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
            'zk.invalid:2181', self.singularity_server.root_url
        ])
        with mock.patch('aiozk.ZKClient', fake_zk.ZKClient):
            self.manager = SingularityProductManager(self.args, self.server)
        self.sensor_proxy_client_mock = \
            create_patch(self, 'katsdpcontroller.sensor_proxy.SensorProxyClient', autospec=True)
        self.open_mock = create_patch(self, 'builtins.open', new_callable=open_file_mock.MockOpen)
        self.open_mock.set_read_data_for('s3_config.json', S3_CONFIG_JSON)

    async def start_manager(self) -> None:
        """Start the manager and arrange for it to be stopped"""
        await self.manager.start()
        self.addCleanup(self.manager.stop)

    async def get_zk_state(self) -> None:
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

    async def test_create_product_dies_after_task_id(self) -> None:
        """Task dies immediately after we learn its task ID"""
        await self.start_manager()
        self.singularity_server.lifecycles.append(death_after_task_id_lifecycle)
        task = self.loop.create_task(self.manager.create_product('foo'))
        await self.advance(100)
        self.assertTrue(task.done())
        with self.assertRaises(ProductFailed):
            await task
        self.assertEqual(self.manager.products, {})

    async def start_product(
            self, lifecycle: fake_singularity.Lifecycle = None) -> SingularityProduct:
        if lifecycle:
            self.singularity_server.lifecycles.append(lifecycle)
        task = self.loop.create_task(self.manager.create_product('foo'))
        await self.advance(100)
        self.assertTrue(task.done())
        product = await task
        await self.manager.product_active(product)
        return product

    async def test_persist(self) -> None:
        await self.manager.start()
        product = await self.start_product()

        # Throw away the manager, create a new one
        zk = self.manager._zk
        await self.manager.stop()
        # We don't model ephemeral nodes in fake_zk, so have to delete manually
        await zk.delete('/running')
        with mock.patch('aiozk.ZKClient', return_value=zk):
            self.manager = SingularityProductManager(self.args, self.server)
        await self.manager.start()
        self.addCleanup(self.manager.stop)
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

    async def test_spontaneous_death(self):
        """Product must be cleaned up if it dies on its own"""
        await self.start_manager()
        product = await self.start_product(spontaneous_death_lifecycle)
        # Check that Zookeeper initially knows about the product
        self.assertEqual((await self.get_zk_state())['products'], {'foo': mock.ANY})

        await self.advance(1000)   # Task will die during this time
        self.assertEqual(product.task_state, Product.TaskState.DEAD)
        self.assertEqual(self.manager.products, {})
        # Check that Zookeeper was updated
        self.assertEqual((await self.get_zk_state())['products'], {})

    async def test_bad_zk_version(self):
        """Wrong version in state stored in Zookeeper"""
        payload = json.dumps({"version": 200000}).encode()
        await self.manager._zk.create('/state', payload)
        with self.assertLogs(master_controller.logger, logging.WARNING) as cm:
            await self.start_manager()
        self.assertEqual(
            cm.output[0],
            'WARNING:katsdpcontroller.master_controller:'
            'Could not load existing state (version mismatch), so starting fresh')
