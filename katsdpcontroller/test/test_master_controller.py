"""Tests for :mod:`katsdpcontroller.master_controller."""

from unittest import mock

import aiokatcp
import asynctest
import open_file_mock

from ..master_controller import ProductFailed, Product, SingularityProductManager, parse_args
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


class TestSingularityProductManager(asynctest.ClockedTestCase):
    async def setUp(self) -> None:
        self.singularity_server = fake_singularity.SingularityServer()
        await self.singularity_server.start()
        self.addCleanup(self.singularity_server.close)
        self.server = DummyServer('127.0.0.1', 0)
        await self.server.start()
        self.addCleanup(self.server.stop)
        args = parse_args([
            '--host', '127.0.0.1',
            '--port', str(device_server_sockname(self.server)[1]),
            '--name', 'sdpmc_test',
            '--external-hostname', 'me.invalid',
            '--s3-config-file', 's3_config.json',
            'zk.invalid:2181', self.singularity_server.root_url
        ])
        with mock.patch('aiozk.ZKClient', fake_zk.ZKClient):
            self.manager = SingularityProductManager(args, self.server)
        self.sensor_proxy_client_mock = \
            create_patch(self, 'katsdpcontroller.sensor_proxy.SensorProxyClient', autospec=True)
        self.open_mock = create_patch(self, 'builtins.open', new_callable=open_file_mock.MockOpen)
        self.open_mock.set_read_data_for('s3_config.json', S3_CONFIG_JSON)

    async def clean_start(self) -> None:
        """Start the manager with no existing state in Singularity or Zookeeper"""
        await self.manager.start()
        self.addCleanup(self.manager.stop)

    async def test_start_clean(self) -> None:
        await self.clean_start()
        await self.advance(30)     # Runs reconciliation a few times

    async def test_create_product(self) -> None:
        await self.clean_start()
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
        await self.clean_start()
        self.singularity_server.lifecycles.append(quick_death_lifecycle)
        task = self.loop.create_task(self.manager.create_product('foo'))
        await self.advance(100)
        self.assertTrue(task.done())
        with self.assertRaises(ProductFailed):
            await task
        self.assertEqual(self.manager.products, {})

    async def test_create_product_dies_after_task_id(self) -> None:
        """Task dies immediately after we learn its task ID"""
        await self.clean_start()
        self.singularity_server.lifecycles.append(death_after_task_id_lifecycle)
        task = self.loop.create_task(self.manager.create_product('foo'))
        await self.advance(100)
        self.assertTrue(task.done())
        with self.assertRaises(ProductFailed):
            await task
        self.assertEqual(self.manager.products, {})
