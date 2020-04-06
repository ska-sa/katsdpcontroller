"""Tests for :mod:`katsdpcontroller.master_controller."""

import asyncio
import logging
import json
import functools
import ipaddress
import os
import socket
import unittest
from unittest import mock
from typing import Tuple, Set, List, Optional, Any

import aiokatcp
from aiokatcp import Sensor, Client
import asynctest
import aiohttp.client
import open_file_mock
import aioresponses
import yarl

from .. import master_controller, scheduler
from ..controller import (DeviceStatus, ProductState, device_server_sockname,
                          make_image_resolver_factory, device_status_to_sensor_status)
from ..master_controller import (ProductFailed, Product, SingularityProduct,
                                 SingularityProductManager, NoAddressesError,
                                 DeviceServer, parse_args)
from . import fake_zk, fake_singularity
from .utils import (create_patch, assert_request_fails, assert_sensors, assert_sensor_value,
                    DelayedManager, Background, run_clocked,
                    CONFIG, S3_CONFIG, EXPECTED_INTERFACE_SENSOR_LIST,
                    EXPECTED_PRODUCT_CONTROLLER_SENSOR_LIST)


EXPECTED_SENSOR_LIST: List[Tuple[bytes, ...]] = [
    (b'device-status', b'', b'discrete', b'ok', b'degraded', b'fail'),
    (b'gui-urls', b'', b'string'),
    (b'products', b'', b'string')
]

# Sensors created per-product by the master controller
EXPECTED_PRODUCT_SENSOR_LIST: List[Tuple[bytes, ...]] = [
    (b'katcp-address', b'', b'address')
]

EXPECTED_REQUEST_LIST = [
    'product-configure',
    'product-deconfigure',
    'product-reconfigure',
    'product-list',
    'set-config-override',
    'sdp-shutdown',
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

# Adapted from an actual query to consul
CONSUL_POWEROFF_SERVERS = [
    {
        "ID": "67aaaddc-6b24-5c93-e114-6b4eb6201843",
        "Node": "testhost",
        "Address": "127.0.0.42",
        "Datacenter": "dc1",
        "TaggedAddresses": {
            "lan": "127.0.0.43",
            "wan": "127.0.0.44"
        },
        "NodeMeta": {
            "consul-network-segment": ""
        },
        "ServiceKind": "",
        "ServiceID": "poweroff",
        "ServiceName": "poweroff",
        "ServiceTags": [],
        "ServiceAddress": "",
        "ServiceWeights": {
            "Passing": 1,
            "Warning": 1
        },
        "ServiceMeta": {},
        "ServicePort": 9118,
        "ServiceEnableTagOverride": False,
        "ServiceProxyDestination": "",
        "ServiceProxy": {},
        "ServiceConnect": {},
        "CreateIndex": 7,
        "ModifyIndex": 7
    },
    {
        "ID": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
        "Node": "testhost2",
        "Address": "127.0.0.142",
        "Datacenter": "dc1",
        "TaggedAddresses": {
            "lan": "127.0.0.143",
            "wan": "127.0.0.144"
        },
        "NodeMeta": {
            "consul-network-segment": ""
        },
        "ServiceKind": "",
        "ServiceID": "poweroff",
        "ServiceName": "poweroff",
        "ServiceTags": [],
        "ServiceAddress": "127.0.0.144",
        "ServiceWeights": {
            "Passing": 1,
            "Warning": 1
        },
        "ServiceMeta": {},
        "ServicePort": 9118,
        "ServiceEnableTagOverride": False,
        "ServiceProxyDestination": "",
        "ServiceProxy": {},
        "ServiceConnect": {},
        "CreateIndex": 7,
        "ModifyIndex": 7
    }
]
CONSUL_POWEROFF_URL = 'http://127.0.0.1:8500/v1/catalog/service/poweroff?near=_agent'


class DummyServer(aiokatcp.DeviceServer):
    VERSION = 'dummy-1.0'
    BUILD_STATE = VERSION

    def __init__(self, host: str, port: int) -> None:
        super().__init__(host, port)
        self.sensors.add(Sensor(str, "products", "JSON list of subarray products",
                                default="[]", initial_status=Sensor.Status.NOMINAL))
        self.sensors.add(Sensor(DeviceStatus, "device-status",
                                "Devices status of the SDP Master Controller",
                                default=DeviceStatus.OK,
                                status_func=device_status_to_sensor_status))


class DummyProductController(aiokatcp.DeviceServer):
    VERSION = 'dummy-product-controller-1.0'
    BUILD_STATE = VERSION

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.sensors.add(aiokatcp.Sensor(
            int, 'ingest.sdp_l0.1.input-bytes-total',
            'Total input bytes',
            default=42,
            initial_status=aiokatcp.Sensor.Status.NOMINAL))
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
            '--image-tag-file', 'sdp_image_tag',
            '--image-override', 'katsdptelstate:branch',
            '--external-hostname', 'me.invalid',
            '--s3-config-file', 's3_config.json',
            '--safe-multicast-cidr', '225.100.0.0/24',
            'zk.invalid:2181', self.singularity_server.root_url
        ])
        image_lookup = scheduler.SimpleImageLookup('registry.invalid:5000')
        self.image_resolver_factory = make_image_resolver_factory(image_lookup, self.args)
        with mock.patch('aiozk.ZKClient', fake_zk.ZKClient):
            self.manager = SingularityProductManager(self.args, self.server,
                                                     self.image_resolver_factory)
        self.client_mock = create_patch(self, 'aiokatcp.Client', autospec=True)
        self.client_mock.return_value.loop = self.loop
        self.client_mock.return_value.logger = mock.MagicMock()
        self.open_mock = create_patch(self, 'builtins.open', new_callable=open_file_mock.MockOpen)
        self.open_mock.set_read_data_for('s3_config.json', S3_CONFIG)
        self.open_mock.set_read_data_for('sdp_image_tag', 'a_tag')
        create_patch(self, 'socket.getaddrinfo',
                     return_value=[(socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP, '',
                                    ('192.0.2.0', 0))])

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
        product = await run_clocked(self, 100, self.manager.create_product('foo', {}))
        self.assertEqual(product.task_state, Product.TaskState.STARTING)
        self.assertEqual(product.host, ipaddress.ip_address('192.0.2.0'))
        self.assertEqual(product.ports['katcp'], 12345)
        self.assertEqual(product.ports['http'], 12346)
        self.assertEqual(product.ports['aiomonitor'], 12347)
        self.assertEqual(product.ports['aioconsole'], 12348)
        self.assertEqual(product.ports['dashboard'], 12349)
        self.client_mock.assert_called_with('192.0.2.0', 12345)

        await self.manager.product_active(product)
        self.assertEqual(product.task_state, Product.TaskState.ACTIVE)
        # Check that the right image selection options were passed to the task
        task = list(self.singularity_server.tasks.values())[0]
        arguments = task.arguments()
        self.assertIn('--image-tag=a_tag', arguments)
        self.assertIn('--image-override=katsdptelstate:branch', arguments)
        self.assertEqual(task.deploy.config['containerInfo']['docker']['image'],
                         'registry.invalid:5000/katsdpcontroller:a_tag')

    async def test_create_product_dies_fast(self) -> None:
        """Task dies before we observe it running"""
        await self.start_manager()
        self.singularity_server.lifecycles.append(quick_death_lifecycle)
        with self.assertRaises(ProductFailed):
            await run_clocked(self, 100, self.manager.create_product('foo', {}))
        self.assertEqual(self.manager.products, {})

    async def test_create_product_parallel(self) -> None:
        """Can configure two subarray products at the same time"""
        await self.start_manager()
        with Background(self.manager.create_product('product1', {})) as cm1, \
                Background(self.manager.create_product('product2', {})) as cm2:
            await self.advance(100)
        product1 = cm1.result
        product2 = cm2.result
        self.assertEqual(product1.name, 'product1')
        self.assertEqual(product2.name, 'product2')
        self.assertEqual(product1.task_state, Product.TaskState.STARTING)
        self.assertEqual(product2.task_state, Product.TaskState.STARTING)

    async def _test_create_product_dies_after_task_id(self, init_wait: float) -> None:
        """Task dies immediately after we learn its task ID

        This test is parametrised so that we can control whether the task ID is
        learnt during polling for the new task or during task reconciliation.
        """
        await self.start_manager()
        self.singularity_server.lifecycles.append(
            functools.partial(death_after_task_id_lifecycle, init_wait))
        with self.assertRaises(ProductFailed):
            await run_clocked(self, 100, self.manager.create_product('foo', {}))
        self.assertEqual(self.manager.products, {})

    async def test_create_product_dies_after_task_id_reconciliation(self) -> None:
        """Task dies immediately after we learn its task ID during reconciliation"""
        await self._test_create_product_dies_after_task_id(self.manager.reconciliation_interval)

    async def test_create_product_dies_after_task_id_poll(self) -> None:
        """Task dies immediately after we learn its task ID during polling"""
        await self._test_create_product_dies_after_task_id(self.manager.new_task_poll_interval)

    async def test_singularity_down(self) -> None:
        await self.start_manager()
        with asynctest.patch('aiohttp.ClientSession._request',
                             side_effect=aiohttp.client.ClientConnectionError):
            with self.assertRaises(ProductFailed):
                await run_clocked(self, 10, self.manager.create_product('foo', {}))
        # Product must be cleared
        self.assertEqual(self.manager.products, {})

    async def start_product(
            self, name: str = 'foo',
            lifecycle: fake_singularity.Lifecycle = None) -> SingularityProduct:
        if lifecycle:
            self.singularity_server.lifecycles.append(lifecycle)
        product = await run_clocked(self, 100, self.manager.create_product(name, {}))
        await self.manager.product_active(product)
        return product

    async def reset_manager(self) -> None:
        """Throw away the manager and create a new one"""
        zk = self.manager._zk
        await self.manager.stop()
        # We don't model ephemeral nodes in fake_zk, so have to delete manually
        await zk.delete('/running')
        with mock.patch('aiozk.ZKClient', return_value=zk):
            self.manager = SingularityProductManager(self.args, self.server,
                                                     self.image_resolver_factory)
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
        self.assertEqual(product.ports, product2.ports)
        self.assertEqual(product.start_time, product2.start_time)
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
        task = self.loop.create_task(self.manager.create_product('foo', {}))
        await self.advance(500)
        self.assertFalse(task.done())
        task.cancel()

        await self.advance(1000)
        self.assertTrue(task.done())
        with self.assertRaises(asyncio.CancelledError):
            await task

    async def test_reuse_deploy(self) -> None:
        def get_deploy_id() -> str:
            request = list(self.singularity_server.requests.values())[0]
            assert request.active_deploy is not None
            return request.active_deploy.deploy_id

        await self.start_manager()
        product = await self.start_product()
        deploy_id = get_deploy_id()

        # Reuse, without restarting the manager
        await self.manager.kill_product(product)
        # Give it time to die
        await self.advance(100)
        product = await self.start_product()
        self.assertEqual(get_deploy_id(), deploy_id)

        # Reuse, after a restart
        await self.manager.kill_product(product)
        # Give it time to die
        await self.advance(100)
        await self.reset_manager()
        product = await self.start_product()
        self.assertEqual(get_deploy_id(), deploy_id)

        # Alter the necessary state to ensure that a new deploy is used
        await self.manager.kill_product(product)
        await self.advance(100)
        with mock.patch.dict(os.environ, {'KATSDP_LOG_LEVEL': 'test'}):
            product = await self.start_product()
        self.assertNotEqual(get_deploy_id(), deploy_id)

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

    async def _test_bad_s3_config(self, content: Optional[str]) -> None:
        await self.start_manager()
        self.open_mock.unregister_path('s3_config.json')
        if content is not None:
            self.open_mock.set_read_data_for('s3_config.json', 'I am not JSON')
        with self.assertRaises(ProductFailed):
            await run_clocked(self, 100, self.manager.create_product('foo', {}))

    async def test_s3_config_bad_json(self) -> None:
        await self._test_bad_s3_config('I am not JSON')

    async def test_s3_config_missing(self) -> None:
        await self._test_bad_s3_config(None)

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

    async def test_multicast_group_out_of_range(self) -> None:
        await self.test_get_multicast_groups()
        self.args.safe_multicast_cidr = '225.101.0.0/24'
        with self.assertLogs('katsdpcontroller.master_controller', 'WARNING') as cm:
            await self.reset_manager()
        self.assertEqual(cm.output, [
            "WARNING:katsdpcontroller.master_controller:Product 'product1' contains "
            "multicast group(s) outside the defined range 225.101.0.0/24"])

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
        create_patch(self, 'aiokatcp.Client', Client)
        self.singularity_server.lifecycles.append(katcp_server_lifecycle)
        product = await run_clocked(self, 100, self.manager.create_product('product1', {}))

        # We haven't called product_active yet, so it should still be CONFIGURING
        self.assertEqual(await product.get_state(), ProductState.CONFIGURING)
        self.assertEqual(await product.get_telstate_endpoint(), '')

        await self.manager.product_active(product)
        self.assertEqual(await product.get_state(), ProductState.IDLE)
        self.assertEqual(await product.get_telstate_endpoint(), 'telstate.invalid:31000')
        self.assertEqual(self.server.sensors['product1.ingest.sdp_l0.1.input-bytes-total'].value,
                         42)

        # Have the remote katcp server tell us it is going away. This also
        # provides test coverage of this shutdown path.
        assert product.katcp_conn is not None
        await product.katcp_conn.request('halt')
        await product.dead_event.wait()
        self.assertEqual(await product.get_state(), ProductState.DEAD)
        self.assertEqual(await product.get_telstate_endpoint(), '')


class TestDeviceServer(asynctest.ClockedTestCase):
    """Tests for :class:`.master_controller.DeviceServer`.

    The tests use interface mode, because that avoids the complications of
    emulating Singularity and Zookeeper, and allows interaction with a mostly
    real product controller.
    """
    async def setUp(self) -> None:
        self.args = parse_args([
            '--localhost',
            '--interface-mode',
            '--port', '0',
            '--name', 'sdpmc_test',
            '--registry', 'registry.invalid:5000',
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

    async def test_capture_init(self) -> None:
        await assert_request_fails(self.client, "capture-init", "product")
        await self.client.request("product-configure", "product", CONFIG)
        reply, informs = await self.client.request("capture-init", "product")
        self.assertEqual(reply, [b"0000000002"])

        reply, informs = await self.client.request("capture-status", "product")
        self.assertEqual(reply, [b"capturing"])
        await assert_request_fails(self.client, "capture-init", "product")

    async def test_capture_init_while_configuring(self) -> None:
        async with self._product_configure_slow('product'):
            await assert_request_fails(self.client, 'capture-init', 'product')

    async def test_interface_sensors(self) -> None:
        await assert_sensors(self.client, EXPECTED_SENSOR_LIST)
        await assert_sensor_value(self.client, 'products', '[]')
        interface_changed_callback = mock.MagicMock()
        self.client.add_inform_callback('interface-changed', interface_changed_callback)
        await self.client.request('product-configure', 'product', CONFIG)
        await asynctest.exhaust_callbacks(self.loop)
        interface_changed_callback.assert_called_once_with(b'sensor-list')
        # Prepend the subarray product ID to the names
        expected_product_sensors = [
            (b'product.' + s[0],) + s[1:]
            for s in (EXPECTED_INTERFACE_SENSOR_LIST
                      + EXPECTED_PRODUCT_CONTROLLER_SENSOR_LIST
                      + EXPECTED_PRODUCT_SENSOR_LIST)]
        await assert_sensors(self.client, EXPECTED_SENSOR_LIST + expected_product_sensors)
        await assert_sensor_value(self.client, 'products', '["product"]')
        product = self.server._manager.products['product']
        await assert_sensor_value(
            self.client, 'product.katcp-address', f'127.0.0.1:{product.ports["katcp"]}')

        # Change the product's device-status to FAIL and check that the top-level sensor
        # is updated.
        product.server.sensors['device-status'].value = DeviceStatus.FAIL
        # Do a round trip to the product server to give time for the change to propagate
        await self.client.request('capture-status', 'product')
        await assert_sensor_value(self.client, 'device-status', 'fail', Sensor.Status.ERROR)

        # Deconfigure and check that the array sensors are gone
        interface_changed_callback.reset_mock()
        await self.client.request('product-deconfigure', 'product')
        await asynctest.exhaust_callbacks(self.loop)
        interface_changed_callback.assert_called_with(b'sensor-list')
        await assert_sensors(self.client, EXPECTED_SENSOR_LIST)
        await assert_sensor_value(self.client, 'products', '[]')
        # With the product gone, the device status must go back to OK
        await assert_sensor_value(self.client, 'device-status', 'ok')

    async def test_capture_done(self) -> None:
        await assert_request_fails(self.client, "capture-done", "product")
        await self.client.request("product-configure", "product", CONFIG)
        await assert_request_fails(self.client, "capture-done", "product")

        await self.client.request("capture-init", "product")
        reply, informs = await self.client.request("capture-done", "product")
        self.assertEqual(reply, [b"0000000001"])
        await assert_request_fails(self.client, "capture-done", "product")

    async def test_set_config_override_bad(self) -> None:
        await assert_request_fails(self.client, "set-config-override", "product", "not json")
        await assert_request_fails(self.client, "set-config-override", "product", "[]")

    async def test_product_configure(self) -> None:
        await assert_request_fails(self.client, "product-deconfigure", "product")
        await self.client.request("product-list")
        await self.client.request("product-configure", "product", CONFIG)
        # Cannot configure an already-configured array
        await assert_request_fails(self.client, "product-configure", "product", CONFIG)

    async def test_product_configure_bad_name(self) -> None:
        await assert_request_fails(self.client, 'product-configure', '!$@#', CONFIG)

    async def test_product_configure_bad_json(self) -> None:
        await assert_request_fails(self.client, 'product-configure', 'product', 'not JSON')

    async def test_product_configure_generate_names(self) -> None:
        """Name with trailing * must generate lowest-numbered name"""
        async def product_configure():
            return (await self.client.request('product-configure', 'prefix_*', CONFIG))[0][0]

        self.assertEqual(b'prefix_0', await product_configure())
        self.assertEqual(b'prefix_1', await product_configure())
        # Deconfigure the product, then check that the name is recycled
        await self.client.request('product-deconfigure', 'prefix_0')
        # Interface mode has some small sleeps, which we have to get past for
        # it to properly die.
        await self.advance(1)
        self.assertEqual(b'prefix_0', await product_configure())

    def _product_configure_slow(self, subarray_product: str,
                                cancelled: bool = False) -> DelayedManager:
        create_patch(self, 'aiokatcp.Client.wait_connected',
                     side_effect=aiokatcp.Client.wait_connected, autospec=True)
        return DelayedManager(
            self.client.request('product-configure', subarray_product, CONFIG),
            aiokatcp.Client.wait_connected,
            None, cancelled)

    async def test_product_deconfigure(self) -> None:
        await assert_request_fails(self.client, "product-configure", "product")
        await self.client.request("product-configure", "product", CONFIG)
        await self.client.request("capture-init", "product")
        # should not be able to deconfigure when not in idle state
        await assert_request_fails(self.client, "product-deconfigure", "product")
        await self.client.request("capture-done", "product")
        await self.advance(1)    # interface mode has some sleeps in capture-done
        await self.client.request("product-deconfigure", "product")

    async def test_product_deconfigure_while_configuring_force(self) -> None:
        """Forced product-deconfigure must succeed while in product-configure"""
        async with self._product_configure_slow('product', cancelled=True):
            await self.client.request("product-deconfigure", 'product', True)
        # Verify that it's gone
        self.assertEqual({}, self.server._manager.products)

    async def test_product_deconfigure_capturing_force(self) -> None:
        """forced product-deconfigure must succeed while capturing"""
        await self.client.request("product-configure", "product", CONFIG)
        await self.client.request("capture-init", "product")
        await self.client.request("product-deconfigure", "product", True)

    async def test_product_reconfigure(self) -> None:
        await assert_request_fails(self.client, "product-reconfigure", "product")
        await self.client.request("product-configure", "product", CONFIG)
        await self.client.request("product-reconfigure", "product")
        await self.client.request("capture-init", "product")
        await assert_request_fails(self.client, "product-reconfigure", "product")

    async def test_product_reconfigure_override(self) -> None:
        """?product-reconfigure must pick up config overrides"""
        await self.client.request("product-configure", "product", CONFIG)
        await self.client.request("set-config-override", "product", '{"config": {"develop": true}}')
        await self.client.request("product-reconfigure", "product")
        config = self.server._manager.products['product'].config
        self.assertEqual(config['config'].get('develop'), True)

    async def test_product_reconfigure_configure_busy(self) -> None:
        """Can run product-reconfigure concurrently with another product-configure"""
        await self.client.request('product-configure', 'product1', CONFIG)
        async with self._product_configure_slow('product2'):
            await self.client.request('product-reconfigure', 'product1')

    async def test_product_reconfigure_configure_fails(self) -> None:
        """Tests product-reconfigure when the new graph fails"""
        async def request(self, name: str,
                          *args: Any) -> Tuple[List[bytes], List[aiokatcp.Message]]:
            # Mock implementation of request that fails product-configure
            if name == 'product-configure':
                raise aiokatcp.FailReply('Fault injected into product-configure')
            else:
                return await orig_request(self, name, *args)

        await self.client.request('product-configure', 'product', CONFIG)
        orig_request = aiokatcp.Client.request
        with mock.patch.object(aiokatcp.Client, 'request', new=request):
            with self.assertRaises(aiokatcp.FailReply):
                await self.client.request('product-reconfigure', 'product')
        # Check that the subarray was deconfigured cleanly
        self.assertEqual({}, self.server._manager.products)

    async def test_help(self) -> None:
        reply, informs = await self.client.request('help')
        requests = [inform.arguments[0].decode('utf-8') for inform in informs]
        self.assertEqual(set(EXPECTED_REQUEST_LIST), set(requests))

    async def test_telstate_endpoint_all(self) -> None:
        """Test telstate-endpoint without a subarray_product_id argument"""
        await self.client.request('product-configure', 'product1', CONFIG)
        await self.client.request('product-configure', 'product2', CONFIG)
        reply, informs = await self.client.request('telstate-endpoint')
        self.assertEqual(reply, [b'2'])
        # Need to compare just arguments, because the message objects have message IDs
        inform_args = [tuple(msg.arguments) for msg in informs]
        self.assertEqual([
            (b'product1', b''),
            (b'product2', b'')
        ], inform_args)

    async def test_telstate_endpoint_one(self) -> None:
        """Test telstate-endpoint with a subarray_product_id argument"""
        await self.client.request('product-configure', 'product', CONFIG)
        reply, informs = await self.client.request('telstate-endpoint', 'product')
        self.assertEqual(reply, [b''])

    async def test_telstate_endpoint_not_found(self) -> None:
        """Test telstate-endpoint with a subarray_product_id that does not exist"""
        await assert_request_fails(self.client, 'telstate-endpoint', 'product')

    async def test_capture_status_all(self) -> None:
        """Test capture-status without a subarray_product_id argument"""
        await self.client.request('product-configure', 'product1', CONFIG)
        await self.client.request('product-configure', 'product2', CONFIG)
        await self.client.request('capture-init', 'product2')
        reply, informs = await self.client.request('capture-status')
        self.assertEqual(reply, [b'2'])
        # Need to compare just arguments, because the message objects have message IDs
        inform_args = [tuple(msg.arguments) for msg in informs]
        self.assertEqual([
            (b'product1', b'idle'),
            (b'product2', b'capturing')
        ], inform_args)

    async def test_capture_status_one(self) -> None:
        """Test capture-status with a subarray_product_id argument"""
        await self.client.request('product-configure', 'product', CONFIG)
        reply, informs = await self.client.request('capture-status', 'product')
        self.assertEqual(reply, [b'idle'])
        self.assertEqual([], informs)
        await self.client.request('capture-init', 'product')
        reply, informs = await self.client.request('capture-status', 'product')
        self.assertEqual(reply, [b'capturing'])
        await self.client.request('capture-done', 'product')
        reply, informs = await self.client.request('capture-status', 'product')
        self.assertEqual(reply, [b'idle'])

    async def test_capture_status_not_found(self) -> None:
        """Test capture-status with a subarray_product_id that does not exist"""
        await assert_request_fails(self.client, 'capture-status', 'product')

    @asynctest.patch('time.time')
    async def test_product_list_all(self, time_mock) -> None:
        """Test product-list without a subarray_product_id argument"""
        time_mock.return_value = 1122334455.123
        await self.client.request('product-configure', 'product1', CONFIG)
        time_mock.return_value = 1234567890.987
        await self.client.request('product-configure', 'product2', CONFIG)
        reply, informs = await self.client.request('product-list')
        self.assertEqual(reply, [b'2'])
        # Need to compare just arguments, because the message objects have message IDs
        inform_args = [tuple(msg.arguments) for msg in informs]
        self.assertEqual([
            (b'product1', b'idle, started at 2005-07-25T23:34:15Z'),
            (b'product2', b'idle, started at 2009-02-13T23:31:30Z')
        ], inform_args)

    @asynctest.patch('time.time', return_value=1122334455.123)
    async def test_product_list_one(self, time_mock) -> None:
        """Test product-list with a subarray_product_id argument"""
        await self.client.request('product-configure', 'product', CONFIG)
        reply, informs = await self.client.request('product-list', 'product')
        self.assertEqual(reply, [b'1'])
        # Need to compare just arguments, because the message objects have message IDs
        inform_args = [tuple(msg.arguments) for msg in informs]
        self.assertEqual([(b'product', b'idle, started at 2005-07-25T23:34:15Z')], inform_args)

    async def test_product_list_not_found(self) -> None:
        """Test product-list with a subarray_product_id that does not exist"""
        await assert_request_fails(self.client, 'product-list', 'product')

    async def test_get_multicast_groups(self) -> None:
        await self.client.request('product-configure', 'product', CONFIG)
        reply, informs = await self.client.request('get-multicast-groups', 'product', 10)
        self.assertEqual(reply, [b'225.100.0.1+9'])
        reply, informs = await self.client.request('get-multicast-groups', 'product', 1)
        self.assertEqual(reply, [b'225.100.0.11'])
        await assert_request_fails(self.client, 'get-multicast-groups', 'product', 0)
        await assert_request_fails(self.client, 'get-multicast-groups', 'wrong-product', 1)
        await assert_request_fails(self.client, 'get-multicast-groups', 'product', 1000000)

    async def test_image_lookup(self) -> None:
        reply, informs = await self.client.request('image-lookup', 'foo', 'tag')
        self.assertEqual(reply, [b'registry.invalid:5000/foo:tag'])

    @aioresponses.aioresponses()
    async def test_sdp_shutdown(self, rmock: aioresponses) -> None:
        rmock.get(CONSUL_POWEROFF_URL, payload=CONSUL_POWEROFF_SERVERS)
        poweroff_mock = mock.MagicMock(return_value=aioresponses.CallbackResult(
            status=202, payload={"stdout": "", "stderr": ""}))
        url1 = yarl.URL('http://127.0.0.42:9118/poweroff')
        url2 = yarl.URL('http://127.0.0.144:9118/poweroff')
        rmock.post(url1, callback=poweroff_mock)
        rmock.post(url2, callback=poweroff_mock)
        await self.client.request('product-configure', 'product', CONFIG)
        await self.client.request('capture-init', 'product')
        reply, informs = await self.client.request('sdp-shutdown')
        self.assertEqual(reply[0], b'127.0.0.144,127.0.0.42')
        poweroff_mock.assert_any_call(url1, data=None)
        poweroff_mock.assert_any_call(url2, data=None)
        # The product should have been forcibly deconfigured
        self.assertEqual(self.server._manager.products, {})

    @aioresponses.aioresponses()
    async def test_sdp_shutdown_no_consul(self, rmock: aioresponses) -> None:
        await self.client.request('product-configure', 'product', CONFIG)
        await self.client.request('capture-init', 'product')
        with self.assertRaisesRegex(aiokatcp.FailReply,
                                    'Could not retrieve list of nodes running poweroff service'):
            await self.client.request('sdp-shutdown')
        # The product should still have been forcibly deconfigured
        self.assertEqual(self.server._manager.products, {})

    @aioresponses.aioresponses()
    async def test_sdp_shutdown_failure(self, rmock: aioresponses) -> None:
        rmock.get(CONSUL_POWEROFF_URL, payload=CONSUL_POWEROFF_SERVERS)
        poweroff_mock = mock.MagicMock(return_value=aioresponses.CallbackResult(
            status=202, payload={"stdout": "", "stderr": ""}))
        url1 = yarl.URL('http://127.0.0.42:9118/poweroff')
        url2 = yarl.URL('http://127.0.0.144:9118/poweroff')
        rmock.post(url1, callback=poweroff_mock)
        rmock.post(url2, status=500, payload={"stdout": "", "stderr": "Simulated failure"})
        await self.client.request('product-configure', 'product', CONFIG)
        await self.client.request('capture-init', 'product')
        with self.assertRaisesRegex(aiokatcp.FailReply,
                                    r'^Success: 127\.0\.0\.42 Failed: 127.0.0.144$'):
            await self.client.request('sdp-shutdown')
        # Other machine must still have been powered off
        poweroff_mock.assert_any_call(url1, data=None)
        # The product should have been forcibly deconfigured
        self.assertEqual(self.server._manager.products, {})


class _ParserError(Exception):
    """Exception substituted for parser.error, which normally raises SystemExit"""


class TestParseArgs(unittest.TestCase):
    @staticmethod
    def _error(message: str) -> None:
        raise _ParserError(message)

    def setUp(self) -> None:
        self.open_mock = create_patch(self, 'builtins.open', new_callable=open_file_mock.MockOpen)
        create_patch(self, 'argparse.ArgumentParser.error', side_effect=self._error)
        self.content1 = '''
            [
                {
                    "title": "Logtrail",
                    "description": "Logtrail (live logs)",
                    "href": "http://kibana.invalid:5601/app/logtrail/",
                    "category": "Log"
                },
                {
                    "title": "Kibana",
                    "description": "Kibana (log exploration)",
                    "href": "http://kibana.invalid:5601/",
                    "category": "Log"
                }
            ]
        '''
        self.content2 = '''
            [
                {
                    "title": "Grafana",
                    "description": "Grafana dashboard",
                    "href": "http://grafana.invalid:3000/",
                    "category": "Dashboard"
                }
            ]
        '''

    def test_gui_urls_file(self) -> None:
        self.open_mock.set_read_data_for('gui-urls.json', self.content1)
        args = parse_args(['--gui-urls=gui-urls.json', '--interface-mode', '', ''])
        self.assertEqual(args.gui_urls, json.loads(self.content1))

    def test_gui_urls_bad_json(self) -> None:
        self.open_mock.set_read_data_for('gui-urls.json', 'not json')
        with self.assertRaisesRegex(_ParserError, 'Invalid JSON'):
            parse_args(['--gui-urls=gui-urls.json', '--interface-mode', '', ''])

    def test_gui_urls_not_list(self) -> None:
        self.open_mock.set_read_data_for('gui-urls.json', '{}')
        with self.assertRaisesRegex(_ParserError, r'gui-urls\.json does not contain a list'):
            parse_args(['--gui-urls=gui-urls.json', '--interface-mode', '', ''])

    def test_gui_urls_missing_file(self) -> None:
        with self.assertRaisesRegex(_ParserError, r'Cannot read gui-urls\.json: File .* not found'):
            parse_args(['--gui-urls=gui-urls.json', '--interface-mode', '', ''])

    def test_gui_urls_dir(self) -> None:
        self.open_mock.set_read_data_for('./file1.json', self.content1)
        self.open_mock.set_read_data_for('./file2.json', self.content2)
        # This is a bit fragile, because open_file_mock doesn't emulate all
        # the os functions to simulate a filesystem.
        with mock.patch('os.listdir', return_value=['file1.json', 'file2.json', 'notjson.txt']), \
                mock.patch('os.path.isfile', return_value=True):
            args = parse_args(['--gui-urls=.', '--interface-mode', '', ''])
        self.assertEqual(args.gui_urls, json.loads(self.content1) + json.loads(self.content2))

    def test_gui_urls_bad_dir(self) -> None:
        with mock.patch('os.listdir', side_effect=IOError):
            with self.assertRaisesRegex(_ParserError, r'Cannot read .:'):
                parse_args(['--gui-urls=.', '--interface-mode', '', ''])

    def test_no_s3_config(self) -> None:
        # Mostly just to get test coverage
        with self.assertRaisesRegex(_ParserError, r'--s3-config-file is required'):
            parse_args(['', ''])
