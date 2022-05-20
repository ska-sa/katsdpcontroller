"""Tests for :mod:`katsdpcontroller.master_controller."""

import argparse
import asyncio
import logging
import json
import functools
import ipaddress
import os
import socket
from unittest import mock
from typing import AsyncGenerator, Callable, Generator, Tuple, Set, List, Optional, Any

import aiokatcp
from aiokatcp import Sensor, Client
import asynctest
import async_solipsism
import aiohttp.client
import open_file_mock
import aioresponses
import yarl
import pytest

from .. import scheduler
from ..controller import (DeviceStatus, ProductState, device_server_sockname,
                          make_image_resolver_factory, device_status_to_sensor_status)
from ..master_controller import (ProductFailed, Product, SingularityProduct,
                                 ProductManagerBase, SingularityProductManager, NoAddressesError,
                                 DeviceServer, parse_args)
from . import fake_zk, fake_singularity
from .utils import (create_patch, assert_request_fails, assert_sensors, assert_sensor_value,
                    DelayedManager, Background, exhaust_callbacks,
                    CONFIG, CONFIG_CBF_ONLY, S3_CONFIG, EXPECTED_INTERFACE_SENSOR_LIST,
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
                                "Devices status of the Master Controller",
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


class TestSingularityProductManager:
    class Manager:
        """Handles starting and stopping a :class:`SingularityProductManager`."""

        def __init__(self, factory: Callable[[], SingularityProductManager]) -> None:
            self.factory = factory
            with mock.patch('aiozk.ZKClient', fake_zk.ZKClient):
                self.manager = factory()

        async def start(self) -> None:
            await self.manager.start()

        async def stop(self) -> None:
            await self.manager.stop()

        async def reset(self) -> None:
            """Throw away the manager and create a new one"""
            zk = self.manager._zk
            await self.manager.stop()
            # We don't model ephemeral nodes in fake_zk, so have to delete manually
            await zk.delete('/running')
            with mock.patch('aiozk.ZKClient', return_value=zk):
                self.manager = self.factory()
            await self.manager.start()

        async def get_zk_state(self) -> dict:
            payload = (await self.manager._zk.get('/state'))[0]
            return json.loads(payload)

    @pytest.fixture
    def event_loop(self) -> Generator[async_solipsism.EventLoop, None, None]:
        loop = async_solipsism.EventLoop()
        yield loop
        loop.close()

    @pytest.fixture
    async def singularity_server(self) -> AsyncGenerator[fake_singularity.SingularityServer, None]:
        def socket_factory(host: str, port: int, family: socket.AddressFamily):
            return async_solipsism.ListenSocket((host, port))

        singularity_server = fake_singularity.SingularityServer(
            aiohttp_server_kwargs=dict(socket_factory=socket_factory, port=80))
        await singularity_server.start()
        yield singularity_server
        await singularity_server.close()

    @pytest.fixture
    async def server(self) -> AsyncGenerator[DummyServer, None]:
        server = DummyServer('127.0.0.1', 1234)
        await server.start()
        yield server
        await server.stop()

    @pytest.fixture(autouse=True)
    def client_mock(self, mocker, event_loop) -> mock.MagicMock:
        client_mock = mocker.patch('aiokatcp.Client', autospec=True)
        client_mock.return_value.loop = event_loop
        client_mock.return_value.logger = mock.MagicMock()
        return client_mock

    @pytest.fixture(autouse=True)
    def open_mock(self, mocker) -> open_file_mock.MockOpen:
        open_mock = mocker.patch('builtins.open', new_callable=open_file_mock.MockOpen)
        open_mock.set_read_data_for('s3_config.json', S3_CONFIG)
        open_mock.set_read_data_for('sdp_image_tag', 'a_tag')
        return open_mock

    @pytest.fixture(autouse=True)
    def mock_getaddrinfo(self, mocker, event_loop) -> None:
        return_value = [
            (socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP, '', ('192.0.2.0', 0))
        ]
        mocker.patch('socket.getaddrinfo', return_value=return_value)
        mocker.patch.object(event_loop, 'getaddrinfo', return_value=return_value)

    @pytest.fixture
    def args(self, server, singularity_server) -> argparse.Namespace:
        return parse_args([
            '--host', '127.0.0.1',
            '--port', str(device_server_sockname(server)[1]),
            '--name', 'sdpmc_test',
            '--image-tag-file', 'sdp_image_tag',
            '--image-override', 'katsdptelstate:branch',
            '--external-hostname', 'me.invalid',
            '--s3-config-file', 's3_config.json',
            '--safe-multicast-cidr', '239.192.0.0/24',
            'zk.invalid:2181', singularity_server.root_url
        ])

    @pytest.fixture
    def image_resolver_factory(self, args: argparse.Namespace) -> scheduler.ImageResolverFactory:
        image_lookup = scheduler.SimpleImageLookup('registry.invalid:5000')
        return make_image_resolver_factory(image_lookup, args)

    # Note: needs to be an async method just so that the event loop is running.
    @pytest.fixture
    async def manager_unstarted(
            self,
            args: argparse.Namespace,
            server: DummyServer,
            image_resolver_factory: scheduler.ImageResolverFactory) \
            -> 'TestSingularityProductManager.Manager':
        def factory() -> SingularityProductManager:
            return SingularityProductManager(args, server, image_resolver_factory)

        return self.Manager(factory)

    @pytest.fixture
    async def manager(
            self, manager_unstarted: 'TestSingularityProductManager.Manager') \
            -> AsyncGenerator['TestSingularityProductManager.Manager', None]:
        await manager_unstarted.start()
        yield manager_unstarted
        await manager_unstarted.stop()

    async def test_start_clean(self, manager) -> None:
        await asyncio.sleep(30)     # Runs reconciliation a few times

    async def test_create_product(self, manager, client_mock, singularity_server) -> None:
        product = await manager.manager.create_product('foo', {})
        assert product.task_state == Product.TaskState.STARTING
        assert product.host == ipaddress.ip_address('192.0.2.0')
        assert product.ports['katcp'] == 12345
        assert product.ports['http'] == 12346
        assert product.ports['aiomonitor'] == 12347
        assert product.ports['aioconsole'] == 12348
        assert product.ports['dashboard'] == 12349
        client_mock.assert_called_with('192.0.2.0', 12345)

        await manager.manager.product_active(product)
        assert product.task_state == Product.TaskState.ACTIVE
        # Check that the right image selection options were passed to the task
        task = list(singularity_server.tasks.values())[0]
        arguments = task.arguments()
        assert '--image-tag=a_tag' in arguments
        assert '--image-override=katsdptelstate:branch' in arguments
        assert (
            task.deploy.config['containerInfo']['docker']['image']
            == 'registry.invalid:5000/katsdpcontroller:a_tag'
        )

    async def test_config_tag_override(self, manager, singularity_server) -> None:
        """Image tag in the config dict overrides tag file."""
        config = {
            'config': {'image_tag': 'custom_tag'}
        }
        product = await manager.manager.create_product('foo', config)
        await manager.manager.product_active(product)
        task = list(singularity_server.tasks.values())[0]
        # Check that the right image was used
        assert (
            task.deploy.config['containerInfo']['docker']['image']
            == 'registry.invalid:5000/katsdpcontroller:custom_tag'
        )
        # Check that it was given the right arguments (although it doesn't
        # really matter because the product controller will pick up the
        # override in the config dict).
        arguments = task.arguments()
        assert '--image-tag=custom_tag' in arguments

    async def test_config_image_override(self, manager, singularity_server) -> None:
        """Image override in the config dict overrides the product controller image."""
        config = {
            'config': {'image_overrides': {'katsdpcontroller': 'katsdpcontroller:custom_tag'}}
        }
        product = await manager.manager.create_product('foo', config)
        await manager.manager.product_active(product)
        task = list(singularity_server.tasks.values())[0]
        # Check that the right image was used
        assert (
            task.deploy.config['containerInfo']['docker']['image']
            == 'registry.invalid:5000/katsdpcontroller:custom_tag'
        )

    async def test_create_product_dies_fast(self, manager, singularity_server) -> None:
        """Task dies before we observe it running"""
        singularity_server.lifecycles.append(quick_death_lifecycle)
        with pytest.raises(ProductFailed):
            await manager.manager.create_product('foo', {})
        assert manager.manager.products == {}

    async def test_create_product_parallel(self, manager) -> None:
        """Can configure two subarray products at the same time"""
        with Background(manager.manager.create_product('product1', {})) as cm1, \
                Background(manager.manager.create_product('product2', {})) as cm2:
            await asyncio.sleep(100)
        product1 = cm1.result
        product2 = cm2.result
        assert product1.name == 'product1'
        assert product2.name == 'product2'
        assert product1.task_state == Product.TaskState.STARTING
        assert product2.task_state == Product.TaskState.STARTING

    @pytest.mark.parametrize(
        'init_wait',
        [
            # Task dies immediately after we learn its task ID during reconciliation
            SingularityProductManager.reconciliation_interval,
            # Task dies immediately after we learn its task ID during polling
            SingularityProductManager.new_task_poll_interval
        ]
    )
    async def test_create_product_dies_after_task_id(
            self, init_wait: float, manager, singularity_server) -> None:
        """Task dies immediately after we learn its task ID.

        This test is parametrised so that we can control whether the task ID is
        learnt during polling for the new task or during task reconciliation.
        """
        singularity_server.lifecycles.append(
            functools.partial(death_after_task_id_lifecycle, init_wait))
        with pytest.raises(ProductFailed):
            await manager.manager.create_product('foo', {})
        assert manager.manager.products == {}

    async def test_singularity_down(self, manager) -> None:
        with mock.patch('aiohttp.ClientSession._request',
                        side_effect=aiohttp.client.ClientConnectionError):
            with pytest.raises(ProductFailed):
                await manager.manager.create_product('foo', {})
        # Product must be cleared
        assert manager.manager.products == {}

    @pytest.fixture
    def start_product(
            self,
            manager: 'TestSingularityProductManager.Manager',
            singularity_server: fake_singularity.SingularityServer):
        async def _start_product(
                name: str = 'foo',
                lifecycle: fake_singularity.Lifecycle = None) -> SingularityProduct:
            if lifecycle:
                singularity_server.lifecycles.append(lifecycle)
            product = await asyncio.shield(manager.manager.create_product(name, {}))
            await manager.manager.product_active(product)
            return product

        return _start_product

    async def test_persist(self, manager, start_product) -> None:
        product = await start_product()
        await manager.reset()
        assert manager.manager.products == {'foo': mock.ANY}

        product2 = manager.manager.products['foo']
        assert product.task_state == product2.task_state
        assert product.run_id == product2.run_id
        assert product.task_id == product2.task_id
        assert product.multicast_groups == product2.multicast_groups
        assert product.host == product2.host
        assert product.ports == product2.ports
        assert product.start_time == product2.start_time
        assert product2.katcp_conn is not None
        assert product is not product2   # Must be reconstituted from state

    async def test_spontaneous_death(self, manager, start_product) -> None:
        """Product must be cleaned up if it dies on its own"""
        product = await start_product(lifecycle=spontaneous_death_lifecycle)
        # Check that Zookeeper initially knows about the product
        assert (await manager.get_zk_state())['products'] == {'foo': mock.ANY}

        await asyncio.sleep(1100)   # Task will die during this time
        assert product.task_state == Product.TaskState.DEAD
        assert manager.manager.products == {}
        # Check that Zookeeper was updated
        assert (await manager.get_zk_state())['products'] == {}

    async def test_stuck_pending(self, manager, singularity_server) -> None:
        """Task takes a long time to be launched.

        The configure gets cancelled before then, and reconciliation must
        clean up the task.
        """
        singularity_server.lifecycles.append(long_pending_lifecycle)
        task = asyncio.create_task(manager.manager.create_product('foo', {}))
        await asyncio.sleep(500)
        assert not task.done()
        task.cancel()

        await asyncio.sleep(1000)
        assert task.done()
        with pytest.raises(asyncio.CancelledError):
            await task

    async def test_reuse_deploy(self, manager, singularity_server, start_product) -> None:
        def get_deploy_id() -> str:
            request = list(singularity_server.requests.values())[0]
            assert request.active_deploy is not None
            return request.active_deploy.deploy_id

        product = await start_product()
        deploy_id = get_deploy_id()

        # Reuse, without restarting the manager
        await manager.manager.kill_product(product)
        # Give it time to die
        await asyncio.sleep(100)
        product = await start_product()
        assert get_deploy_id() == deploy_id

        # Reuse, after a restart
        await manager.manager.kill_product(product)
        # Give it time to die
        await asyncio.sleep(100)
        await manager.reset()
        product = await start_product()
        assert get_deploy_id() == deploy_id

        # Alter the necessary state to ensure that a new deploy is used
        await manager.manager.kill_product(product)
        await asyncio.sleep(100)
        with mock.patch.dict(os.environ, {'KATSDP_LOG_LEVEL': 'test'}):
            product = await start_product()
        assert get_deploy_id() != deploy_id

    @pytest.mark.parametrize(
        'payload',
        [
            # Wrong version in state stored in Zookeeper
            json.dumps({"version": 200000}).encode(),
            # Data in Zookeeper is not valid JSON
            b'I am not JSON',
            # Data in Zookeeper is not valid UTF-8
            b'\xff',
            # Data in Zookeeper does not conform to schema
            json.dumps({"version": 1}).encode()
        ]
    )
    async def test_bad_zk(self, payload: bytes, manager_unstarted, caplog) -> None:
        """Existing state data in Zookeeper is not valid"""
        await manager_unstarted.manager._zk.create('/state', payload)
        with caplog.at_level(logging.WARNING):
            await manager_unstarted.start()
        assert 'Could not load existing state' in caplog.records[0].message
        await manager_unstarted.stop()

    @pytest.mark.parametrize(
        'content',
        [
            'I am not JSON',
            None
        ]
    )
    async def test_bad_s3_config(self, content: Optional[str], manager, open_mock) -> None:
        open_mock.unregister_path('s3_config.json')
        if content is not None:
            open_mock.set_read_data_for('s3_config.json', content)
        with pytest.raises(ProductFailed):
            await manager.manager.create_product('foo', {})

    async def test_get_multicast_groups(self, manager, start_product) -> None:
        product1 = await start_product('product1')
        product2 = await start_product('product2')
        assert await manager.manager.get_multicast_groups(product1, 1) == '239.192.0.1'
        assert await manager.manager.get_multicast_groups(product1, 4) == '239.192.0.2+3'
        with pytest.raises(NoAddressesError):
            await manager.manager.get_multicast_groups(product1, 1000)

        assert await manager.manager.get_multicast_groups(product2, 128) == '239.192.0.6+127'
        # Now product1 owns .1-.5, product2 owns .6-.133.
        await manager.manager.kill_product(product2)
        await asyncio.sleep(100)   # Give it time to clean up

        # Allocations should continue from where they left off, then cycle
        # around to reuse the space freed by product2.
        assert await manager.manager.get_multicast_groups(product1, 100) == '239.192.0.134+99'
        assert await manager.manager.get_multicast_groups(product1, 100) == '239.192.0.6+99'

    async def test_get_multicast_groups_persist(self, manager, start_product) -> None:
        await self.test_get_multicast_groups(manager, start_product)
        await manager.reset()
        product1 = manager.manager.products['product1']
        expected: Set[ipaddress.IPv4Address] = set()
        for i in range(105):
            expected.add(ipaddress.IPv4Address('239.192.0.1') + i)
        for i in range(100):
            expected.add(ipaddress.IPv4Address('239.192.0.134') + i)
        assert product1.multicast_groups == expected
        assert manager.manager._next_multicast_group == ipaddress.IPv4Address('239.192.0.106')

    async def test_multicast_group_out_of_range(self, manager, start_product, args, caplog) -> None:
        await self.test_get_multicast_groups(manager, start_product)
        args.safe_multicast_cidr = '225.101.0.0/24'
        with caplog.at_level(logging.WARNING, 'katsdpcontroller.master_controller'):
            await manager.reset()
        assert caplog.record_tuples == [
            (
                'katsdpcontroller.master_controller',
                logging.WARNING,
                "Product 'product1' contains multicast group(s) outside the "
                "defined range 225.101.0.0/24"
            )
        ]

    async def test_get_multicast_groups_negative(self, manager, start_product) -> None:
        product = await start_product()
        with pytest.raises(ValueError):
            await manager.manager.get_multicast_groups(product, -1)
        with pytest.raises(ValueError):
            await manager.manager.get_multicast_groups(product, 0)

    async def test_capture_block_id(self, manager, mocker) -> None:
        mock_time = mocker.patch('time.time')
        mock_time.return_value = 1122334455.123
        assert await manager.manager.get_capture_block_id() == '1122334455'
        assert await manager.manager.get_capture_block_id() == '1122334456'
        # Must still be monotonic, even if time.time goes backwards
        mock_time.return_value -= 10
        assert await manager.manager.get_capture_block_id() == '1122334457'
        # Once time.time goes past next, must use that again
        mock_time.return_value = 1122334460.987
        assert await manager.manager.get_capture_block_id() == '1122334460'

        # Must persist state over restarts
        await manager.reset()
        assert await manager.manager.get_capture_block_id() == '1122334461'

    async def test_katcp(self, server, manager, singularity_server, mocker) -> None:
        # Disable the mocking by making the real version the side effect
        mocker.patch('aiokatcp.Client', Client)
        singularity_server.lifecycles.append(katcp_server_lifecycle)
        product = await manager.manager.create_product('product1', {})

        # We haven't called product_active yet, so it should still be CONFIGURING
        assert await product.get_state() == ProductState.CONFIGURING
        assert await product.get_telstate_endpoint() == ''

        await manager.manager.product_active(product)
        assert await product.get_state() == ProductState.IDLE
        assert await product.get_telstate_endpoint() == 'telstate.invalid:31000'
        assert server.sensors['product1.ingest.sdp_l0.1.input-bytes-total'].value == 42

        # Have the remote katcp server tell us it is going away. This also
        # provides test coverage of this shutdown path.
        assert product.katcp_conn is not None
        await product.katcp_conn.request('halt')
        await product.dead_event.wait()
        assert await product.get_state() == ProductState.DEAD
        assert await product.get_telstate_endpoint() == ''


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
            '--safe-multicast-cidr', '239.192.0.0/24',
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
        assert reply == [b"0000000002"]

        reply, informs = await self.client.request("capture-status", "product")
        assert reply == [b"capturing"]
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
        await exhaust_callbacks()
        interface_changed_callback.assert_called_with(b'sensor-list')
        # Prepend the subarray product ID to the names
        expected_product_sensors = [
            (b'product.' + s[0],) + s[1:]
            for s in (EXPECTED_INTERFACE_SENSOR_LIST
                      + EXPECTED_PRODUCT_CONTROLLER_SENSOR_LIST
                      + EXPECTED_PRODUCT_SENSOR_LIST)]
        await assert_sensors(self.client, EXPECTED_SENSOR_LIST + expected_product_sensors,
                             subset=True)
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
        await exhaust_callbacks()
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
        assert reply == [b"0000000001"]
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

        assert b'prefix_0' == await product_configure()
        assert b'prefix_1' == await product_configure()
        # Deconfigure the product, then check that the name is recycled
        await self.client.request('product-deconfigure', 'prefix_0')
        # Interface mode has some small sleeps, which we have to get past for
        # it to properly die.
        await asyncio.sleep(1)
        assert b'prefix_0' == await product_configure()

    def _product_configure_slow(self, subarray_product: str,
                                cancelled: bool = False) -> DelayedManager:
        create_patch(self, 'aiokatcp.Client.wait_connected',
                     side_effect=aiokatcp.Client.wait_connected, autospec=True)
        return DelayedManager(
            self.client.request('product-configure', subarray_product, CONFIG),
            aiokatcp.Client.wait_connected,         # type: ignore
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
        assert self.server._manager.products == {}

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
        assert config['config'].get('develop') is True

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
            with pytest.raises(aiokatcp.FailReply):
                await self.client.request('product-reconfigure', 'product')
        # Check that the subarray was deconfigured cleanly
        assert self.server._manager.products == {}

    async def test_product_configure_reuse_name(self) -> None:
        await self.client.request('product-configure', 'product', CONFIG_CBF_ONLY)
        await self.client.request('product-deconfigure', 'product')
        await self.client.request('product-configure', 'product', CONFIG)

    async def test_help(self) -> None:
        reply, informs = await self.client.request('help')
        requests = [inform.arguments[0].decode('utf-8') for inform in informs]
        assert set(requests) == set(EXPECTED_REQUEST_LIST)

    async def test_telstate_endpoint_all(self) -> None:
        """Test telstate-endpoint without a subarray_product_id argument"""
        await self.client.request('product-configure', 'product1', CONFIG)
        await self.client.request('product-configure', 'product2', CONFIG)
        reply, informs = await self.client.request('telstate-endpoint')
        assert reply == [b'2']
        # Need to compare just arguments, because the message objects have message IDs
        inform_args = [tuple(msg.arguments) for msg in informs]
        assert inform_args == [
            (b'product1', b''),
            (b'product2', b'')
        ]

    async def test_telstate_endpoint_one(self) -> None:
        """Test telstate-endpoint with a subarray_product_id argument"""
        await self.client.request('product-configure', 'product', CONFIG)
        reply, informs = await self.client.request('telstate-endpoint', 'product')
        assert reply == [b'']

    async def test_telstate_endpoint_not_found(self) -> None:
        """Test telstate-endpoint with a subarray_product_id that does not exist"""
        await assert_request_fails(self.client, 'telstate-endpoint', 'product')

    async def test_capture_status_all(self) -> None:
        """Test capture-status without a subarray_product_id argument"""
        await self.client.request('product-configure', 'product1', CONFIG)
        await self.client.request('product-configure', 'product2', CONFIG)
        await self.client.request('capture-init', 'product2')
        reply, informs = await self.client.request('capture-status')
        assert reply == [b'2']
        # Need to compare just arguments, because the message objects have message IDs
        inform_args = [tuple(msg.arguments) for msg in informs]
        assert inform_args == [
            (b'product1', b'idle'),
            (b'product2', b'capturing')
        ]

    async def test_capture_status_one(self) -> None:
        """Test capture-status with a subarray_product_id argument"""
        await self.client.request('product-configure', 'product', CONFIG)
        reply, informs = await self.client.request('capture-status', 'product')
        assert reply == [b'idle']
        assert informs == []
        await self.client.request('capture-init', 'product')
        reply, informs = await self.client.request('capture-status', 'product')
        assert reply == [b'capturing']
        await self.client.request('capture-done', 'product')
        reply, informs = await self.client.request('capture-status', 'product')
        assert reply == [b'idle']

    async def test_capture_status_not_found(self) -> None:
        """Test capture-status with a subarray_product_id that does not exist"""
        await assert_request_fails(self.client, 'capture-status', 'product')

    @mock.patch('time.time')
    async def test_product_list_all(self, time_mock) -> None:
        """Test product-list without a subarray_product_id argument"""
        time_mock.return_value = 1122334455.123
        await self.client.request('product-configure', 'product1', CONFIG)
        time_mock.return_value = 1234567890.987
        await self.client.request('product-configure', 'product2', CONFIG)
        reply, informs = await self.client.request('product-list')
        assert reply == [b'2']
        # Need to compare just arguments, because the message objects have message IDs
        inform_args = [tuple(msg.arguments) for msg in informs]
        assert inform_args == [
            (b'product1', b'idle, started at 2005-07-25T23:34:15Z'),
            (b'product2', b'idle, started at 2009-02-13T23:31:30Z')
        ]

    @mock.patch('time.time', return_value=1122334455.123)
    async def test_product_list_one(self, time_mock) -> None:
        """Test product-list with a subarray_product_id argument"""
        await self.client.request('product-configure', 'product', CONFIG)
        reply, informs = await self.client.request('product-list', 'product')
        assert reply == [b'1']
        # Need to compare just arguments, because the message objects have message IDs
        inform_args = [tuple(msg.arguments) for msg in informs]
        assert inform_args == [(b'product', b'idle, started at 2005-07-25T23:34:15Z')]

    async def test_product_list_not_found(self) -> None:
        """Test product-list with a subarray_product_id that does not exist"""
        await assert_request_fails(self.client, 'product-list', 'product')

    async def test_get_multicast_groups(self) -> None:
        # Prevent product-configure from actually allocating multicast groups,
        # since that would throw off the expected values.
        with mock.patch.object(ProductManagerBase, 'get_multicast_groups', return_value='0.0.0.0'):
            await self.client.request('product-configure', 'product', CONFIG)
        reply, informs = await self.client.request('get-multicast-groups', 'product', 10)
        assert reply == [b'239.192.0.1+9']
        reply, informs = await self.client.request('get-multicast-groups', 'product', 1)
        assert reply == [b'239.192.0.11']
        await assert_request_fails(self.client, 'get-multicast-groups', 'product', 0)
        await assert_request_fails(self.client, 'get-multicast-groups', 'wrong-product', 1)
        await assert_request_fails(self.client, 'get-multicast-groups', 'product', 1000000)

    async def test_image_lookup(self) -> None:
        reply, informs = await self.client.request('image-lookup', 'foo', 'tag')
        assert reply == [b'registry.invalid:5000/foo:tag']

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
        assert reply[0] == b'127.0.0.144,127.0.0.42'
        poweroff_mock.assert_any_call(
            url1, headers={'X-Poweroff-Server': '1'}, allow_redirects=True, data=None)
        poweroff_mock.assert_any_call(
            url2, headers={'X-Poweroff-Server': '1'}, allow_redirects=True, data=None)
        # The product should have been forcibly deconfigured
        assert self.server._manager.products == {}

    @aioresponses.aioresponses()
    async def test_sdp_shutdown_no_consul(self, rmock: aioresponses) -> None:
        await self.client.request('product-configure', 'product', CONFIG)
        await self.client.request('capture-init', 'product')
        with pytest.raises(aiokatcp.FailReply,
                           match='Could not retrieve list of nodes running poweroff service'):
            await self.client.request('sdp-shutdown')
        # The product should still have been forcibly deconfigured
        assert self.server._manager.products == {}

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
        with pytest.raises(aiokatcp.FailReply,
                           match=r'^Success: 127\.0\.0\.42 Failed: 127.0.0.144$'):
            await self.client.request('sdp-shutdown')
        # Other machine must still have been powered off
        poweroff_mock.assert_any_call(
            url1, headers={'X-Poweroff-Server': '1'}, allow_redirects=True, data=None)
        # The product should have been forcibly deconfigured
        assert self.server._manager.products == {}


class _ParserError(Exception):
    """Exception substituted for parser.error, which normally raises SystemExit"""


class TestParseArgs:
    @staticmethod
    def _error(message: str) -> None:
        raise _ParserError(message)

    @pytest.fixture(autouse=True)
    def catch_argparse_error(self, mocker) -> None:
        mocker.patch('argparse.ArgumentParser.error', side_effect=self._error)

    @pytest.fixture(autouse=True)
    def open_mock(self, mocker) -> open_file_mock.MockOpen:
        return mocker.patch('builtins.open', new_callable=open_file_mock.MockOpen)

    def setup(self) -> None:
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

    def test_gui_urls_file(self, open_mock) -> None:
        open_mock.set_read_data_for('gui-urls.json', self.content1)
        args = parse_args(['--gui-urls=gui-urls.json', '--interface-mode', '', ''])
        assert args.gui_urls == json.loads(self.content1)

    def test_gui_urls_bad_json(self, open_mock) -> None:
        open_mock.set_read_data_for('gui-urls.json', 'not json')
        with pytest.raises(_ParserError, match='Invalid JSON'):
            parse_args(['--gui-urls=gui-urls.json', '--interface-mode', '', ''])

    def test_gui_urls_not_list(self, open_mock) -> None:
        open_mock.set_read_data_for('gui-urls.json', '{}')
        with pytest.raises(_ParserError, match=r'gui-urls\.json does not contain a list'):
            parse_args(['--gui-urls=gui-urls.json', '--interface-mode', '', ''])

    def test_gui_urls_missing_file(self) -> None:
        with pytest.raises(_ParserError, match=r'Cannot read gui-urls\.json: File .* not found'):
            parse_args(['--gui-urls=gui-urls.json', '--interface-mode', '', ''])

    def test_gui_urls_dir(self, open_mock) -> None:
        open_mock.set_read_data_for('./file1.json', self.content1)
        open_mock.set_read_data_for('./file2.json', self.content2)
        # This is a bit fragile, because open_file_mock doesn't emulate all
        # the os functions to simulate a filesystem.
        with mock.patch('os.listdir', return_value=['file1.json', 'file2.json', 'notjson.txt']), \
                mock.patch('os.path.isfile', return_value=True):
            args = parse_args(['--gui-urls=.', '--interface-mode', '', ''])
        assert args.gui_urls == json.loads(self.content1) + json.loads(self.content2)

    def test_gui_urls_bad_dir(self) -> None:
        with mock.patch('os.listdir', side_effect=IOError):
            with pytest.raises(_ParserError, match=r'Cannot read .:'):
                parse_args(['--gui-urls=.', '--interface-mode', '', ''])

    def test_no_s3_config(self) -> None:
        # Mostly just to get test coverage
        with pytest.raises(_ParserError, match=r'--s3-config-file is required'):
            parse_args(['', ''])
