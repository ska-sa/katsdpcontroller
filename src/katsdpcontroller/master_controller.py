"""Science Data Processor Master Controller"""

# TODO:
# - fill in remaining requests
# - possibly add some barriers to ensure that existing state is saved before
#   handling new katcp requests.
# - possibly add wait_connected to _katcp_request

import asyncio
import logging
import argparse
import enum
import uuid
import ipaddress
import json
import itertools
import re
import time
from datetime import datetime
import os
import socket
from abc import abstractmethod
from typing import (Type, TypeVar, Generic, Dict, Tuple, Set, List, Union,
                    Iterable, Mapping, Sequence, Optional, Callable, Awaitable, Any)

import aiozk
import aiokatcp
from aiokatcp import FailReply, Sensor, SensorSet, Address
import async_timeout
import jsonschema
import yarl
import aiohttp
import katsdpservices

import katsdpcontroller
from . import singularity, product_config, product_controller, scheduler, sensor_proxy
from .defaults import LOCALHOST
from .scheduler import decode_json_base64
from .schemas import ZK_STATE, SUBSYSTEMS       # type: ignore
from .controller import (time_request, load_json_dict, log_task_exceptions, device_server_sockname,
                         add_shared_options, extract_shared_options, make_image_resolver_factory,
                         ProductState, DeviceStatus, device_status_to_sensor_status)


ZK_STATE_VERSION = 4
logger = logging.getLogger(__name__)
_T = TypeVar('_T')
_P = TypeVar('_P', bound='Product')
_IPAddress = Union[ipaddress.IPv4Address, ipaddress.IPv6Address]
CONSUL_POWEROFF_PATH = 'v1/catalog/service/poweroff'


class NoAddressesError(Exception):
    """Insufficient multicast addresses available"""


class ProductFailed(Exception):
    """Attempt to create a subarray product was unsuccessful"""


def _load_s3_config(filename: str) -> dict:
    with open(filename, 'r') as f:
        config = json.load(f)
    return config


async def _resolve_host(host: str) -> _IPAddress:
    """Resolve host name or address to an IP address if necessary.

    If the address is already an IP address, this will not block.
    """
    try:
        return ipaddress.ip_address(host)
    except ValueError:
        addrs = await asyncio.get_event_loop().getaddrinfo(
            host, 0, family=socket.AF_INET, type=socket.SOCK_STREAM, proto=socket.IPPROTO_TCP)
        return ipaddress.ip_address(addrs[0][4][0])


class DeviceStatusWatcher(aiokatcp.AbstractSensorWatcher):
    """Call a callback whenever the client's device-status sensor changes."""

    def __init__(self, callback: Callable[[], None]) -> None:
        self.callback = callback

    def sensor_updated(self, name: str, value: bytes, status: aiokatcp.Sensor.Status,
                       timestamp: float) -> None:
        if name == 'device-status':
            self.callback()


class Product:
    """A single subarray product

    Parameters
    ----------
    name
        Subarray product ID
    config
        Product configuration to pass to product controller
    configure_task
        The asyncio task corresponding to the product-configure command. If the
        process dies, this task is cancelled to notify it.
    """

    class TaskState(enum.Enum):
        """State of the product controller process"""
        CREATED = 1
        STARTING = 2      # The task is running, but we haven't finished product-configure on it
        ACTIVE = 3
        DEAD = 4

    def __init__(self, name: str, config: dict, configure_task: Optional[asyncio.Task]) -> None:
        self.name = name
        self.config = config
        self.configure_task = configure_task
        self.katcp_conn: Optional[aiokatcp.Client] = None
        self.task_state = Product.TaskState.CREATED
        self.host: Optional[_IPAddress] = None
        self.hostname: Optional[str] = None  # Human-friendly form of .host
        self.ports: Dict[str, int] = {}
        self.multicast_groups: Set[ipaddress.IPv4Address] = set()
        self.logger = logging.LoggerAdapter(logger, dict(subarray_product_id=name))
        self.dead_event = asyncio.Event()
        self._dead_callbacks: List[Callable[['Product'], None]] = []
        self.start_time = time.time()
        self.sensors = SensorSet()     # Sensors created internally - not proxied
        self.sensors.add(Sensor(Address, f'{self.name}.katcp-address',
                                'Address of the katcp server for the product controller'))
        self.sensors.add(Sensor(str, f'{self.name}.host',
                                'Name of the host running the product controller'))

    def connect(self, server: aiokatcp.DeviceServer,
                rewrite_gui_urls: Optional[Callable[[Sensor], bytes]],
                hostname: str, host: _IPAddress, ports: Dict[str, int]) -> None:
        """Notify product of the location of the katcp interface.

        After calling this, :attr:`host`, :attr:`hostname`, :attr:`ports` and :attr:`katcp_conn`
        are all ready to use.
        """
        self.hostname = hostname
        self.host = host
        self.ports = ports
        self.sensors[f'{self.name}.host'].value = hostname
        for (port_name, port_value) in ports.items():
            sensor_name = f'{self.name}.{port_name}-address'
            try:
                self.sensors[sensor_name].value = Address(host, port_value)
            except KeyError:
                self.logger.warning('Sensor %s does not exist', sensor_name)
        self.katcp_conn = aiokatcp.Client(str(host), ports['katcp'])
        self.katcp_conn.add_sensor_watcher(sensor_proxy.SensorWatcher(
            self.katcp_conn, server, f'{self.name}.',
            rewrite_gui_urls=rewrite_gui_urls,
            enum_types=(DeviceStatus,)))
        self.katcp_conn.add_inform_callback('disconnect', self._disconnect_callback)
        # TODO: start a watchdog

    def _disconnect_callback(self, *args: bytes) -> None:
        """Called when the remote katcp server tells us it is shutting down.

        In normal use, this indicates that the product has been fully deconfigured.
        """
        self.died()

    def died(self) -> None:
        """The remote server has died or the product is otherwise dead."""
        self.task_state = Product.TaskState.DEAD
        self.dead_event.set()
        if self.configure_task is not None:
            self.configure_task.cancel()
            self.configure_task = None
        if self.katcp_conn is not None:
            self.katcp_conn.remove_inform_callback('disconnect', self._disconnect_callback)
            self.katcp_conn.close()
            self.katcp_conn = None
            # TODO: should this be async and await it being closed?
        for callback in self._dead_callbacks:
            callback(self)

    async def _katcp_request(self, type_: Type[_T], configuring: _T, dead: _T,
                             message: str, *args: Any) -> _T:
        """Query a single value from the product using katcp

        Parameters
        ----------
        type_
            The type of the value (used to decode the raw bytes from katcp)
        configuring
            Value to return if the product has not yet become active.
        dead
            Value to return if the product has died.
        message
            katcp request name
        args
            katcp arguments
        """
        if self.task_state == Product.TaskState.ACTIVE:
            assert self.katcp_conn is not None
            reply, informs = await self.katcp_conn.request(message, *args)
            return aiokatcp.decode(type_, reply[0])
        elif self.task_state == Product.TaskState.DEAD:
            return dead
        else:
            return configuring

    async def get_state(self) -> ProductState:
        return await self._katcp_request(
            ProductState, ProductState.CONFIGURING, ProductState.DEAD, 'capture-status')

    async def get_telstate_endpoint(self) -> str:
        return await self._katcp_request(str, '', '', 'telstate-endpoint')

    def add_dead_callback(self, callback: Callable[['Product'], None]) -> None:
        """Add a function to call when :meth:`died` is called."""
        self._dead_callbacks.append(callback)

    async def close(self) -> None:
        """Disconnect from the product controller.

        For a "real" product, this should *not* try to shut down the remote
        side. This is called if the master controller is shut down, not when
        ``product-deconfigure`` is called.
        """
        self.logger.info('Disconnecting from product %s during shutdown', self.name)
        if self.katcp_conn is not None:
            self.katcp_conn.remove_inform_callback('disconnect', self._disconnect_callback)
            self.katcp_conn.close()
            await self.katcp_conn.wait_closed()
            self.katcp_conn.close()

    async def description(self) -> str:
        start_dt = datetime.utcfromtimestamp(self.start_time)
        start_str = start_dt.isoformat(timespec='seconds')
        state = (await self.get_state()).name.lower()
        return f'{state}, started at {start_str}Z'


class ProductManagerBase(Generic[_P]):
    """Abstract base class for launching and managing subarray products.

    It also handles allocation of multicast groups and capture block IDs.
    """

    def __init__(self, args: argparse.Namespace, server: aiokatcp.DeviceServer,
                 image_resolver_factory: scheduler.ImageResolverFactory,
                 rewrite_gui_urls: Callable[[Sensor], bytes] = None) -> None:
        self._args = args
        self._multicast_network = ipaddress.IPv4Network(args.safe_multicast_cidr)
        self._next_multicast_group = self._multicast_network.network_address + 1
        self._server = server
        self._products: Dict[str, _P] = {}
        self._next_capture_block_id = 1
        self._image_resolver_factory = image_resolver_factory
        self._rewrite_gui_urls = rewrite_gui_urls

    @abstractmethod
    async def start(self) -> None: pass

    @abstractmethod
    async def stop(self) -> None:
        for product in self.products.values():
            await product.close()

    async def _save_state(self) -> None:
        """Persist the state of the manager."""

    def _save_state_bg(self) -> None:
        """Persist current state in the background"""
        task = asyncio.get_event_loop().create_task(self._save_state())
        log_task_exceptions(task, logger, '_save_state')

    def valid_multicast_group(self, group: ipaddress.IPv4Address) -> bool:
        return (group in self._multicast_network
                and group != self._multicast_network.network_address
                and group != self._multicast_network.broadcast_address)

    def _init_state(self, products: Iterable[_P], next_capture_block_id: int,
                    next_multicast_group: ipaddress.IPv4Address) -> None:
        """Set the initial state from persistent storage. This is intended to
        be called by subclasses.

        Parameters
        ----------
        products
            Subarray products. They must be in state :const:`~Product.TaskState.ACTIVE`
            and have katcp information filled in, but the _product_died callback should
            not have been added.
        next_capture_block_id
            Lower bound for the next capture block ID.
        next_multicast_group
            Hint for the next multicast group to allocate. If it is outside the allowed
            range, it will be ignored.
        """
        assert not self._products
        for product in products:
            if any(not self.valid_multicast_group(group) for group in product.multicast_groups):
                product.logger.warning(
                    'Product %r contains multicast group(s) outside the defined range %s',
                    product.name, self._multicast_network)
            self._add_product(product)
        self._next_capture_block_id = next_capture_block_id

        if (next_multicast_group not in self._multicast_network
                or next_multicast_group == self._multicast_network.network_address):
            if next_multicast_group != ipaddress.IPv4Address('0.0.0.0'):
                logger.info('Resetting next multicast group from out-of-range %s to %s',
                            next_multicast_group, self._multicast_network.network_address + 1)
            next_multicast_group = self._multicast_network.network_address + 1
        self._next_multicast_group = next_multicast_group

    @property
    def products(self) -> Mapping[str, _P]:
        """Get a list of all defined products, indexed by name.

        The caller should treat this as read-only, using the methods on this
        class to add/remove products or change their state.
        """
        return self._products

    @abstractmethod
    async def create_product(self, name: str, config: dict) -> _P:
        """Create a new product called `name`.

        The product is brought to state :const:`~Product.TaskState.STARTING`
        before return.

        The implementation *must not* yield control before calling
        :meth:`add_product`.
        """

    def _update_device_status(self) -> None:
        """Recompute the top-level device-status from the per-product device-status sensors."""
        status = DeviceStatus.OK
        for name in self.products.keys():
            sensor = self._server.sensors.get(f'{name}.device-status')
            if sensor is not None and sensor.status.valid_value() and sensor.value > status:
                status = sensor.value
        self._server.sensors['device-status'].value = status

    def _update_products_sensor(self) -> None:
        self._server.sensors['products'].value = json.dumps(sorted(self._products.keys()))
        self._update_device_status()

    def _add_product(self, product: _P) -> None:
        """Used by subclasses to add a newly-created product."""
        assert product.name not in self._products
        self._products[product.name] = product
        product.add_dead_callback(self._product_died)
        for sensor in product.sensors.values():
            self._server.sensors.add(sensor)
        self._update_products_sensor()

    def _remove_product(self, product: Product) -> None:
        """Removes a product. Should not be used by subclasses."""
        assert self._products.get(product.name) is product
        del self._products[product.name]
        for sensor in product.sensors.values():
            self._server.sensors.remove(sensor)
        self._update_products_sensor()

    async def product_active(self, product: _P) -> None:
        """Move a product to state :const:`~Product.TaskState.ACTIVE`"""
        product.task_state = Product.TaskState.ACTIVE
        await self._save_state()

    @abstractmethod
    async def kill_product(self, product: _P) -> None:
        """Move a product to state :const:`~Product.TaskState.DEAD`.

        Subclasses should override this to actually kill off the product.
        """
        if self._products.get(product.name) is product:
            self._remove_product(product)
            await self._save_state()

    def _product_died(self, product: Product) -> None:
        """Called by product.died"""
        if self._products.get(product.name) is product:
            self._remove_product(product)
            self._save_state_bg()

    async def get_multicast_groups(self, product: _P, n_addresses: int) -> str:
        """Allocate `n_addresses` consecutive multicast groups.

        Returns
        -------
        addresses
            The single IP address if n_addresses = 1, or :samp:`{IP}+{n}` where
            n = n_addresses-1.

        Raises
        ------
        NoAddressesError
            if there are no enough free multicast addresses
        """
        if n_addresses <= 0:
            raise ValueError('n_addresses must be positive')
        products = self.products.values()
        wrapped = False
        # It might be the broadcast address, but must always be inside the network
        assert self._next_multicast_group in self._multicast_network
        start = self._next_multicast_group
        while True:
            # Check if there is enough space
            if int(self._multicast_network.broadcast_address) - int(start) < n_addresses:
                if wrapped:
                    # Wrapped for the second time - give up
                    raise NoAddressesError
                wrapped = True
                start = self._multicast_network.network_address + 1
                continue
            for i in range(n_addresses):
                cur = start + i
                if any(cur in prod.multicast_groups for prod in products):
                    # We've hit an already-allocated address. Go back around
                    # the while loop, starting from the following address.
                    start = cur + 1
                    break
            else:
                # We've found a usable range
                self._next_multicast_group = start + n_addresses
                for i in range(n_addresses):
                    product.multicast_groups.add(start + i)
                ans = str(start)
                if n_addresses > 1:
                    ans += '+{}'.format(n_addresses - 1)
                await self._save_state()
                return ans

    @abstractmethod
    def _gen_capture_block_id(self, minimum: int) -> int:
        """Create a new capture block ID that must be at least `minimum`"""

    def _connect(self, product: _P, hostname: str, host: _IPAddress, ports: Dict[str, int]) -> None:
        """Establish a connection to a product's katcp server.

        Subclasses should always call this method rather than directly using
        ``product.connect``.

        `ports` must contain at least ``katcp``, but subclasses may include
        additional ports.
        """
        product.connect(self._server, self._rewrite_gui_urls, hostname, host, ports)
        assert product.katcp_conn is not None
        product.katcp_conn.add_sensor_watcher(DeviceStatusWatcher(self._update_device_status))

    async def get_capture_block_id(self) -> str:
        """Generate a unique capture block ID"""
        cbid = self._gen_capture_block_id(self._next_capture_block_id)
        self._next_capture_block_id = cbid + 1
        await self._save_state()
        return f'{cbid:010}'


class InternalProduct(Product):
    """Product with internal :class:`.product_controller.DeviceServer`"""
    def __init__(self, name: str, config: dict, configure_task: Optional[asyncio.Task],
                 server: aiokatcp.DeviceServer) -> None:
        super().__init__(name, config, configure_task)
        self.server = server

    async def close(self) -> None:
        await super().close()
        await self.server.stop()


class InternalProductManager(ProductManagerBase[InternalProduct]):
    """Run subarray products as asyncio tasks within the process.

    This is intended **only** for integration testing.
    """

    async def start(self) -> None:
        await super().start()

    async def stop(self) -> None:
        await super().stop()
        for product in self.products.values():
            if product.server is not None:
                await product.server.stop()

    async def create_product(self, name: str, config: dict) -> InternalProduct:
        mc_client = aiokatcp.Client(*device_server_sockname(self._server))
        sched = scheduler.SchedulerBase(self._args.realtime_role, LOCALHOST, 0)
        server = product_controller.DeviceServer(
            '127.0.0.1', 0, mc_client, name, sched, self._args.batch_role,
            True, self._args.localhost, self._image_resolver_factory,
            s3_config={}, shutdown_delay=0)
        product = InternalProduct(name, config, asyncio.current_task(), server)
        self._add_product(product)
        await product.server.start()
        host, port = device_server_sockname(product.server)
        product.task_state = Product.TaskState.STARTING
        self._connect(product, host, ipaddress.ip_address(host), {'katcp': port})
        return product

    async def kill_product(self, product: InternalProduct) -> None:
        await super().kill_product(product)
        await product.server.stop()
        product.died()

    def _gen_capture_block_id(self, minimum: int) -> int:
        return minimum


class SingularityProduct(Product):
    """Subarray product launched as a task within Hubspot Singularity"""

    def __init__(self, name: str, config: dict, configure_task: Optional[asyncio.Task]) -> None:
        super().__init__(name, config, configure_task)
        self.run_id = name + '-' + uuid.uuid4().hex
        self.task_id: Optional[str] = None
        self._image: Optional[str] = None
        self.sensors.add(Sensor(
            Address, f'{name}.http-address',
            'Address of internal HTTP server (which is NOT the dashboard)'))
        self.sensors.add(Sensor(
            Address, f'{name}.aiomonitor-address',
            'Address of aiomonitor debugging port (only accessible from the host)'))
        self.sensors.add(Sensor(
            Address, f'{name}.aioconsole-address',
            'Address of aioconsole debugging port (only accessible from the host)'))
        self.sensors.add(Sensor(
            Address, f'{name}.dashboard-address',
            'Address of product controller dashboard'))
        self.sensors.add(Sensor(
            str, f'{self.name}.version',
            'Docker image running the product controller'))

    @property
    def image(self) -> Optional[str]:
        return self._image

    @image.setter
    def image(self, value: Optional[str]) -> None:
        self._image = value
        sensor = self.sensors[f'{self.name}.version']
        if value is not None:
            sensor.value = value
        else:
            sensor.set_value('', status=Sensor.Status.UNKNOWN)


class SingularityProductManager(ProductManagerBase[SingularityProduct]):
    """Run subarray products as tasks within Hubspot Singularity."""

    # Interval between polling Singularity for data on tasks
    reconciliation_interval = 10.0
    # Once we create a new singularity run, this is the interval at which
    # we poll to try to find the task ID. It is deliberately chosen not to
    # divide into the reconciliation interval, which is needed for some of
    # the clocked unit tests so that it's possible to control which event
    # happens next.
    new_task_poll_interval = 0.3
    assert (reconciliation_interval + 1e-6) % new_task_poll_interval > 1e-5, \
        "reconciliation_interval must not be a multiple of new_task_poll_interval"

    def __init__(self, args: argparse.Namespace,
                 server: aiokatcp.DeviceServer,
                 image_resolver_factory: scheduler.ImageResolverFactory,
                 rewrite_gui_urls: Callable[[Sensor], bytes] = None) -> None:
        super().__init__(args, server, image_resolver_factory, rewrite_gui_urls)
        self._request_id_prefix = args.name + '_product_'
        self._task_cache: Dict[str, dict] = {}  # Maps Singularity Task IDs to their info
        # Task IDs that we didn't expect to see, but have been seen once.
        # They might be tasks we killed which haven't quite died yet, so they get
        # one reconciliation cycle to disappear on their own.
        self._probation: Set[str] = set()
        self._reconciliation_task: Optional[asyncio.Task] = None
        self._sing = singularity.Singularity(yarl.URL(args.singularity) / 'api')
        self._zk = aiozk.ZKClient(args.zk, chroot=args.name)
        self._zk_state_lock = asyncio.Lock()

    async def _zk_set(self, key: str, payload: bytes) -> None:
        """Set content for a Zookeeper node, creating it if necessary"""
        try:
            await self._zk.create(key, payload)
        except aiozk.exc.NodeExists:
            await self._zk.set(key, payload, -1)

    async def _try_kill_task(self, task_id: str) -> None:
        """Kill a task, but ignore 404 errors which can occur if the task is already dead"""
        try:
            await self._sing.kill_task(task_id)
        except singularity.NotFoundError:
            pass

    async def _get_task_info(self, task_id: str) -> Optional[dict]:
        """Get the information about a task.

        This uses a cache, so is cheap to use more than once. If the task was
        not cached and no longer exists, returns ``None``.
        """
        if task_id in self._task_cache:
            return self._task_cache[task_id]
        else:
            try:
                data = await self._sing.get_task(task_id)
                self._task_cache[task_id] = data
                return data
            except singularity.NotFoundError:
                return None

    async def _mark_running(self) -> None:
        """Create an ephemeral Zookeeper node as a lock"""
        try:
            await self._zk.create('/running', data=b'', ephemeral=True)
        except aiozk.exc.NodeExists as exc:
            raise RuntimeError('Another instance is already running - kill it first') from exc

    async def _ensure_request(self, product_name: str) -> str:
        """Create or update a Singularity request object

        Parameters
        ----------
        product_name
            Name of the subarray product

        Returns
        -------
        request_id
            Singularity request ID
        """
        # Singularity allows requests to be updated with POST, so just do it unconditionally
        request_id = self._request_id_prefix + product_name
        await self._sing.create_request({
            "id": request_id,
            "requestType": "ON_DEMAND"
        })
        return request_id

    async def _ensure_deploy(self, product_name: str, image: str) -> str:
        """Create or update the Singularity deploy object

        Parameters
        ----------
        product_name
            Subarray product being configured (determines the request name)
        image
            Docker image to run

        Returns
        -------
        deploy_id
            Singularity deploy ID
        """
        request_id = self._request_id_prefix + product_name
        environ = {'LOGSPOUT': 'ignore'}
        for key in ['KATSDP_LOG_ONELINE', 'KATSDP_LOG_LEVEL', 'KATSDP_LOG_GELF_ADDRESS']:
            if key in os.environ:
                environ[key] = os.environ[key]
        extra = {
            **json.loads(os.environ.get('KATSDP_LOG_GELF_EXTRA', '{}')),
            'task': 'product_controller',
            'docker.image': image
        }
        environ['KATSDP_LOG_GELF_EXTRA'] = json.dumps(extra)
        labels = {
            'za.ac.kat.sdp.katsdpcontroller.task': 'product_controller',
            'za.ac.kat.sdp.katsdpcontroller.subarray_product_id': product_name
        }
        docker_parameters = [
            {'key': 'label', 'value': f'{key}={value}'} for (key, value) in labels.items()
        ]
        deploy = {
            "requestId": request_id,
            "command": "sdp_product_controller.py",
            "arguments": ["--aiomonitor"],
            "env": environ,
            "containerInfo": {
                "type": "DOCKER",
                "docker": {
                    "image": image,
                    "forcePullImage": False,
                    "network": "HOST",
                    "dockerParameters": docker_parameters
                }
            },
            "resources": {
                "cpus": 0.2,
                "memoryMb": 2048,
                "numPorts": 5         # katcp, http, aiomonitor, aioconsole, dashboard
            }
        }

        # Singularity can sometimes get stuck in a state where the last deploy
        # is "pending" and then one cannot create a new deploy. To reduce the
        # risk of that, only update if necessary.
        #
        # Unfortunately when querying the deploy from Singularity we get it
        # with defaults filled in, so it is non-trivial to determine if the
        # current deploy is up-to-date. We thus use Zookeeper to remember the
        # previous request.
        try:
            (old_deploy_raw, stat), current_request = await asyncio.gather(
                self._zk.get(f'/deploys/{request_id}'), self._sing.get_request(request_id))
            old_deploy = json.loads(old_deploy_raw)
            old_id = old_deploy['id']
            current_id = current_request['activeDeploy']['id']
            if old_id == current_id:
                deploy['id'] = current_id
                if deploy == old_deploy:
                    timestamp = datetime.utcfromtimestamp(stat.modified / 1000.0)
                    timestamp_str = timestamp.isoformat(timespec='seconds')
                    logger.info('Reusing deploy ID %s, created at %sZ', current_id, timestamp_str)
                    return current_id
        except (aiozk.exc.NoNode, KeyError, TypeError, ValueError):
            logger.debug('Cannot reuse deploy', exc_info=True)
        deploy['id'] = uuid.uuid4().hex
        logger.info('Creating new deploy with ID %s', deploy['id'])
        await self._sing.create_deploy({"deploy": deploy})
        await self._zk.ensure_path('/deploys')
        await self._zk_set(f'/deploys/{request_id}', json.dumps(deploy).encode())
        return deploy['id']

    async def _load_state(self) -> None:
        """Load existing subarray product state from Zookeeper"""
        async with self._zk_state_lock:
            default = {
                "version": ZK_STATE_VERSION,
                "products": {},
                "next_multicast_group": "0.0.0.0",
                "next_capture_block_id": 0
            }
            try:
                payload = (await self._zk.get('/state'))[0]
                data = json.loads(payload)
                ZK_STATE.validate(data)
            except aiozk.exc.NoNode:
                logger.info('No existing state found')
                data = default
            except (ValueError, jsonschema.ValidationError) as exc:
                logger.warning('Could not load existing state (%s), so starting fresh', exc)
                data = default
            products = []
            for name, info in data['products'].items():
                prod = SingularityProduct(name, info['config'], None)
                prod.run_id = info['run_id']
                prod.task_id = info['task_id']
                prod.image = info.get('image')  # Only introduced in version 4
                try:
                    prod.start_time = info['start_time']
                except KeyError:
                    pass     # Version 1 didn't have start_time
                try:
                    ports = info['ports']
                except KeyError:
                    # Version 2 only had the katcp port
                    ports = {'katcp': info['port']}
                prod.task_state = Product.TaskState.ACTIVE
                prod.multicast_groups = {ipaddress.ip_address(addr)
                                         for addr in info['multicast_groups']}
                prod.logger.info('Reconnecting to existing product %s at %s:%d',
                                 prod.name, info['host'], ports['katcp'])
                self._connect(prod, info['host'], await _resolve_host(info['host']), ports)
                products.append(prod)
            self._init_state(products, data['next_capture_block_id'],
                             ipaddress.IPv4Address(data['next_multicast_group']))

    async def _save_state(self) -> None:
        """Save the current state to Zookeeper"""
        async with self._zk_state_lock:
            data = {
                'version': ZK_STATE_VERSION,
                'products': {
                    prod.name: {
                        'config': prod.config,
                        'run_id': prod.run_id,
                        'task_id': prod.task_id,
                        'image': prod.image,
                        'host': prod.hostname,
                        'ports': prod.ports,
                        'multicast_groups': [str(group) for group in prod.multicast_groups],
                        'start_time': prod.start_time
                    } for prod in self.products.values()
                    if prod.task_state == Product.TaskState.ACTIVE
                },
                'next_multicast_group': str(self._next_multicast_group),
                'next_capture_block_id': self._next_capture_block_id
            }
            payload = json.dumps(data).encode()
            await self._zk_set('/state', payload)

    async def _reconcile_once(self) -> None:
        """Reconcile internal state with state reported by Singularity.

        Tasks that we didn't expect to see are killed if they don't promptly
        kill themselves. Tasks that we expected to see but are missing cause
        products to be marked as dead.
        """
        logger.debug('Starting reconciliation')
        new_probation: Set[str] = set()
        requests = await self._sing.get_requests(request_type=['ON_DEMAND'])
        # Start by assuming everything died and remove tasks as they're seen
        dead_tasks = set(self._task_cache.keys())
        expected_run_ids = {product.run_id for product in self.products.values()}
        for request in requests:
            request_id = request['request']['id']
            if not request_id.startswith(self._request_id_prefix):
                continue
            data = await self._sing.get_request_tasks(request_id)
            for state, tasks in data.items():
                if state in {'pending', 'cleaning'}:
                    continue      # TODO: cancel unwanted pending tasks before they launch
                for task in tasks:
                    task_id: str = task['id']
                    task_info = await self._get_task_info(task_id)
                    if task_info is None:
                        continue       # Died before we could query it
                    dead_tasks.discard(task_id)
                    logger.debug('Reconciliation: %s is in state %s', task_id, state)
                    run_id: str = task_info['taskRequest']['pendingTask']['runId']
                    if run_id not in expected_run_ids:
                        if task_id in self._probation:
                            logger.info('Killing task %s with unknown run_id %s', task_id, run_id)
                            await self._try_kill_task(task_id)
                        else:
                            logger.debug('Task %s with unknown run_id %s will be killed next time',
                                         task_id, run_id)
                            new_probation.add(task_id)
        for task_id in dead_tasks:
            logger.debug('Task %s died', task_id)
            del self._task_cache[task_id]
        n_died = 0
        for product in list(self.products.values()):
            if product.task_id is not None and product.task_id not in self._task_cache:
                product.logger.info('Task %s for %s died', product.task_id, product.name)
                product.died()
                n_died += 1
        if n_died > 0:
            await self._save_state()
        self._probation = new_probation
        logging.debug('Reconciliation finished')

    async def _reconcile_repeat(self) -> None:
        """Run :meth:_reconcile_once regularly"""
        while True:
            await asyncio.sleep(self.reconciliation_interval)
            try:
                await self._reconcile_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception('Exception in reconciliation')

    async def start(self) -> None:
        await self._zk.start()
        await self._mark_running()
        await self._load_state()
        await self._reconcile_once()
        self._reconciliation_task = asyncio.create_task(self._reconcile_repeat(), name="reconcile")
        log_task_exceptions(self._reconciliation_task, logger, 'reconciliation')
        await super().start()

    async def stop(self) -> None:
        if self._reconciliation_task is not None:
            self._reconciliation_task.cancel()
            # Waits but does not raise exceptions - there is a done callback to log them
            await asyncio.wait([self._reconciliation_task])
        await self._save_state()   # Should all be saved already, but belt-and-braces
        await self._sing.close()
        await self._zk.close()
        await super().stop()

    def _gen_capture_block_id(self, minimum: int) -> int:
        return max(int(time.time()), minimum)

    async def create_product(self, name: str, config: dict) -> SingularityProduct:
        try:
            s3_config = _load_s3_config(self._args.s3_config_file)
        except Exception as exc:
            raise ProductFailed(
                f'Could not load S3 credentials from {self._args.s3_config_file}: {exc}') from exc
        args = extract_shared_options(self._args)
        # If the config specifies an image tag, use it to override the tag.
        image_resolver_kwargs = {}
        try:
            image_resolver_kwargs['tag'] = config['config']['image_tag']
        except KeyError:
            pass

        # Creates a temporary ImageResolver so that we read the tag file now
        # and throw away the cache immediately after this function.
        try:
            image_resolver = self._image_resolver_factory(**image_resolver_kwargs)
            for image_name, image in config.get('config', {}).get('image_overrides', {}).items():
                image_resolver.override(image_name, image)
        except Exception as exc:
            raise ProductFailed(f'Could not load image tag file: {exc}')
        port = device_server_sockname(self._server)[1]
        args.extend([
            '--s3-config=' + json.dumps(s3_config),
            f'--image-tag={image_resolver.tag}',
            f'--subarray-product-id={name}',
            f'{self._args.external_hostname}:{port}',
            f'zk://{self._args.zk}/mesos'
        ])

        product = SingularityProduct(name, config, asyncio.current_task())
        self._add_product(product)
        success = False
        task_id: Optional[str] = None
        try:
            image = (await image_resolver('katsdpcontroller')).path
            product.image = image
            request_id = await self._ensure_request(name)
            await self._ensure_deploy(name, image)
            await self._sing.create_run(request_id, {
                "runId": product.run_id,
                "commandLineArgs": args
            })
            # Wait until the task is running or dead
            loop = asyncio.get_event_loop()
            while True:
                logger.debug('Checking if task is running yet')
                try:
                    data = await self._sing.track_run(request_id, product.run_id)
                except singularity.NotFoundError:
                    # Singularity is asynchronous, so it doesn't immediately recognise run_id
                    pass
                else:
                    try:
                        task_id = data['taskId']['id']
                        state = data['currentState']
                        if state in {'TASK_CLEANING', 'TASK_KILLED'}:
                            raise ProductFailed('Task died immediately')
                        elif state == 'TASK_RUNNING':
                            break
                    except KeyError:
                        # Happens if the task hasn't been launched yet
                        pass
                await asyncio.sleep(self.new_task_poll_interval)

            # Update task cache
            assert task_id is not None
            task_info = await self._get_task_info(task_id)
            if task_info is None:
                raise ProductFailed('Task died immediately')

            product.task_id = task_id
            product.task_state = Product.TaskState.STARTING
            # From this point, reconciliation will kill the product if the task dies
            env_list = task_info['mesosTask']['command']['environment']['variables']
            env: Dict[str, str] = {item['name']: item['value'] for item in env_list}
            host = await _resolve_host(env['TASK_HOST'])
            ports = {
                'katcp': int(env['PORT0']),
                'http': int(env['PORT1']),
                'aiomonitor': int(env['PORT2']),
                'aioconsole': int(env['PORT3']),
                'dashboard': int(env['PORT4'])
            }
            self._connect(product, env['TASK_HOST'], host, ports)
            success = True
        except (scheduler.ImageError, aiohttp.ClientError, singularity.SingularityError) as exc:
            raise ProductFailed(f'Failed to start product controller: {exc}') from exc
        finally:
            if success:
                product.configure_task = None   # Stops died() from trying to cancel us
            else:
                if task_id is not None:
                    # Make best effort to kill it; it might be dead already though
                    kill_task = loop.create_task(self._try_kill_task(task_id))
                    log_task_exceptions(kill_task, product.logger, f'kill task {task_id}')
                product.configure_task = None   # Stops died() from trying to cancel us
                product.died()
                # No need for self._save_state, because the product never had a chance to
                # reach ACTIVE state and hence wasn't stored
        return product

    async def kill_product(self, product: SingularityProduct) -> None:
        await super().kill_product(product)
        if product.task_id is not None:
            await self._try_kill_task(product.task_id)
        # TODO: wait until it's actually dead?
        product.died()


class DeviceServer(aiokatcp.DeviceServer):
    """katcp server around a :class:`ProductManagerBase`."""

    VERSION = "sdpcontroller-3.3"
    BUILD_STATE = "katsdpcontroller-" + katsdpcontroller.__version__

    _manager: ProductManagerBase
    _override_dicts: Dict[str, dict]
    _image_lookup: scheduler.ImageLookup
    _interface_changed_callbacks: List[Callable[[], None]]

    def __init__(self, args: argparse.Namespace,
                 rewrite_gui_urls: Callable[[Sensor], bytes] = None) -> None:
        if args.no_pull or args.interface_mode:
            self._image_lookup = scheduler.SimpleImageLookup(args.registry)
        else:
            self._image_lookup = scheduler.HTTPImageLookup(args.registry)
        image_resolver_factory = make_image_resolver_factory(self._image_lookup, args)

        manager_cls: Type[ProductManagerBase]
        if args.interface_mode:
            manager_cls = InternalProductManager
        else:
            manager_cls = SingularityProductManager
        self._manager = manager_cls(args, self, image_resolver_factory,
                                    rewrite_gui_urls if args.haproxy else None)
        self._consul_url = args.consul_url
        self._override_dicts = {}
        self._interface_changed_callbacks = []
        self._args = args
        super().__init__(args.host, args.port)
        self.sensors.add(Sensor(DeviceStatus, "device-status",
                                "Combined (worst) status of all subarray product controllers",
                                default=DeviceStatus.OK,
                                status_func=device_status_to_sensor_status))
        self.sensors.add(Sensor(str, "gui-urls", "Links to associated GUIs",
                                default=json.dumps(args.gui_urls),
                                initial_status=Sensor.Status.NOMINAL))
        self.sensors.add(Sensor(str, "products", "JSON list of subarray products",
                                default="[]", initial_status=Sensor.Status.NOMINAL))
        self.sensors.add(Sensor(int, "cbf-resources-total",
                                "Total number of devices (e.g. servers) that are known "
                                "to the master controller and suitable for running CBF "
                                "tasks, including those reserved for maintenance."))
        self.sensors.add(Sensor(int, "cbf-resources-maintenance",
                                "Total number of devices that have been flagged for "
                                "maintenance and which will not be used to run any "
                                "new subarray products."))
        self.sensors.add(Sensor(int, "cbf-resources-free",
                                "Number of devices that are not in maintenance "
                                "and are not currently running any tasks."))

        # Updated by SensorProxyClient with sensor values prior to gui-url rewriting
        self.orig_sensors = SensorSet()
        for sensor in self.sensors.values():
            self.orig_sensors.add(sensor)
        # Send all logs over katcp connection to clients
        logging.getLogger().addHandler(self.LogHandler(self))

    async def start(self) -> None:
        await self._manager.start()
        if not self._args.interface_mode:
            self.add_service_task(asyncio.create_task(
                self._update_resource_sensors_repeat(), name="update_resource_sensors"))
        await super().start()

    async def on_stop(self) -> None:
        await self._manager.stop()

    def add_interface_changed_callback(self, callback: Callable[[], None]) -> None:
        self._interface_changed_callbacks.append(callback)

    def mass_inform(self, name: str, *args: Any) -> None:
        super().mass_inform(name, *args)
        # Triggered by SensorWatcher
        if name == 'interface-changed':
            for callback in self._interface_changed_callbacks:
                callback()

    async def _mesos_master_url(self, zk: aiozk.ZKClient) -> yarl.URL:
        """Get the address of the leading Mesos master.

        This can fail and raise an exception in lots of ways. Callers should
        be prepared to treat any exception as a failure.
        """
        while True:
            children = await zk.get_children('/')
            # Limit to names involved in leadership election.
            children = [child for child in children if child.startswith('json.info_')]
            if not children:
                raise RuntimeError('no Mesos masters found')
            child = min(children)
            try:
                raw_data = await zk.get_data(f'/{child}')
            except aiozk.exc.NoNode:
                # The node vanished before we were able to query it. Check
                # for the new leader.
                continue
            data = json.loads(raw_data)
            ip_addr = data['address']['ip']
            port = data['address']['port']
            return yarl.URL.build(scheme='http', host=ip_addr, port=port)

    async def _update_resource_sensors(self, zk: aiozk.ZKClient) -> None:
        try:
            cbf_resources_total = 0
            cbf_resources_maintenance = 0
            cbf_resources_free = 0
            url = await self._mesos_master_url(zk)
            timeout = aiohttp.ClientTimeout(total=5)
            draining_machines = set()
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(url / 'master/maintenance/status') as resp:
                    data = await resp.json()
                    draining = data.get('draining_machines', [])
                    for machine in draining:
                        draining_machines.add(machine['id']['hostname'])
                async with session.get(url / 'master/slaves') as resp:
                    data = await resp.json()
                    for agent in data['slaves']:
                        attributes = agent.get('attributes', {})
                        try:
                            subsystems_raw = attributes['katsdpcontroller.subsystems']
                            subsystems = decode_json_base64(subsystems_raw)
                            SUBSYSTEMS.validate(subsystems)
                            is_cbf = 'cbf' in subsystems
                        except Exception:
                            # Lots of possible exceptions: missing attributes, bad base64,
                            # bad JSON, schema error...
                            is_cbf = False
                        if not is_cbf:
                            continue
                        cbf_resources_total += 1
                        if agent['hostname'] in draining_machines:
                            cbf_resources_maintenance += 1
                        elif agent['used_resources']['cpus'] == 0:
                            cbf_resources_free += 1
        except Exception as exc:
            logger.warning('Failed to get resource information from Mesos: %s', exc)
            self.sensors['cbf-resources-total'].set_value(0, status=Sensor.Status.FAILURE)
            self.sensors['cbf-resources-maintenance'].set_value(0, status=Sensor.Status.FAILURE)
            self.sensors['cbf-resources-free'].set_value(0, status=Sensor.Status.FAILURE)
        else:
            self.sensors['cbf-resources-total'].value = cbf_resources_total
            self.sensors['cbf-resources-maintenance'].value = cbf_resources_maintenance
            self.sensors['cbf-resources-free'].value = cbf_resources_free

    async def _update_resource_sensors_repeat(self) -> None:
        zk = aiozk.ZKClient(self._args.zk, chroot='mesos', allow_read_only=True)
        await zk.start()
        try:
            while True:
                await self._update_resource_sensors(zk)
                await asyncio.sleep(5)
        finally:
            await zk.close()

    def _unique_name(self, prefix: str) -> str:
        """Find first unused name with the given prefix"""
        for i in itertools.count():
            name = prefix + str(i)
            if name not in self._manager.products:
                return name
        # itertools.count never finishes, but to keep mypy happy:
        assert False     # pragma: nocover

    def _get_katcp(self, subarray_product_id: str) -> aiokatcp.Client:
        """Get the katcp connection to an active subarray product.

        Raises
        ------
        FailReply
            if `subarray_product_id` is not the name of an active product
        """
        product = self._manager.products.get(subarray_product_id)
        if product is None:
            raise FailReply(f'There is no subarray product with ID {subarray_product_id}')
        if product.task_state != Product.TaskState.ACTIVE:
            raise FailReply(f'Subarray product {subarray_product_id} is still being configured')
        assert product.katcp_conn is not None    # Guaranteed by task_state == ACTIVE
        return product.katcp_conn

    @time_request
    async def request_get_multicast_groups(
            self, ctx, name: str, n_addresses: int) -> str:
        """Allocate multicast groups for a subarray product.

        This should only be used by product controllers.

        Parameters
        ----------
        name : str
            Subarray product
        n_addresses : int
            Number of multicast addresses to allocate
        """
        try:
            product = self._manager.products[name]
        except KeyError as exc:
            raise FailReply(f'No product named {name}') from exc
        if n_addresses <= 0:
            raise FailReply('n_addresses must be positive')
        try:
            return await self._manager.get_multicast_groups(product, n_addresses)
        except NoAddressesError as exc:
            raise FailReply('Insufficient multicast addresses available') from exc

    @time_request
    async def request_image_lookup(self, ctx, repo: str, tag: str) -> str:
        """Look up the full name to use for an image.

        This should only be used by product controllers.

        Parameters
        ----------
        repo : str
            Image repository name
        tag : str
            Docker image tag

        Returns
        -------
        str
            Full image name
        """
        return (await self._image_lookup(repo, tag)).path

    @time_request
    async def request_image_lookup_v2(self, ctx, repo: str, tag: str) -> str:
        """Look up information about an image.

        This should only be used by product controllers.

        Parameters
        ----------
        repo : str
            Image repository name
        tag : str
            Docker image tag

        Returns
        -------
        str
            JSON serialisation of scheduler.Image class
        """
        image = await self._image_lookup(repo, tag)
        return json.dumps(image.__dict__)

    @time_request
    async def request_set_config_override(self, ctx, name: str, override_dict_json: str) -> str:
        """Override internal configuration parameters for the next configure of the
        specified subarray product.

        Any existing override for this subarry product will be completely overwritten.

        The override will only persist until a configure has been called on the
        subarray product.

        Parameters
        ----------
        name : str
            The ID of the subarray product to set overrides for.
        override_dict_json : str
            A JSON string containing a dict of config key:value overrides to use.
        """
        product_logger = logging.LoggerAdapter(
            logger, dict(subarray_product_id=name))
        product_logger.info("?set-config-override called on %s with %s",
                            name, override_dict_json)
        try:
            override = load_json_dict(override_dict_json)
            product_logger.info("Set override for subarray product %s to the following: %s",
                                name, override_dict_json)
            self._override_dicts[name] = override
        except ValueError as exc:
            msg = (f"The supplied override string {override_dict_json} does not appear to "
                   f"be a valid json string containing a dict. {exc}")
            product_logger.error(msg)
            raise FailReply(msg)
        n = len(override)
        return f"Set {n} override keys for subarray product {name}"

    async def product_configure(self, name: str, config: dict) -> Product:
        """Implementation of :meth:`request_product_configure`."""
        orig_name = name
        if name.endswith('*'):
            name = self._unique_name(name[:-1])
        # NB: do not await between _unique_name and create_product below, as
        # doing so would introduce a race condition where the name could get
        # taken before we create it.
        if name in self._manager.products:
            raise FailReply(f'Subarray product {name} already exists')

        product_logger = logging.LoggerAdapter(logger, dict(subarray_product_id=name))
        if not re.match(r'^[A-Za-z0-9_]+\*?$', name):
            raise FailReply('Subarray product ID contains illegal characters')
        if orig_name in self._override_dicts:
            # this is a use-once set of overrides
            odict = self._override_dicts.pop(orig_name)
            product_logger.warning("Setting overrides on %s for the following: %s",
                                   name, odict)
            config = product_config.override(config, odict)
        product_logger.info("Configuring %s with '%s'", name, json.dumps(config))

        success = False
        product: Optional[Product] = None
        timeout = 300
        try:
            async with async_timeout.timeout(timeout):
                product = await self._manager.create_product(name, config)
                assert product is not None
                assert product.katcp_conn is not None
                assert product.host is not None and product.ports

                await product.katcp_conn.wait_connected()
                await product.katcp_conn.request('product-configure', name,
                                                 json.dumps(config))
                await self._manager.product_active(product)
                success = True
                return product
        except asyncio.TimeoutError:
            raise FailReply(f'Subarray product did not configure within {timeout}s') from None
        except asyncio.CancelledError:
            # Could be because the request itself was cancelled (e.g. server
            # going down) or because reconciliation noticed the task had
            # died. We can distinguish them by checking the product state.
            if product is not None and product.task_state == Product.TaskState.DEAD:
                raise FailReply(
                    f'Task for {name} died before the subarray was configured') from None
            else:
                raise
        except ProductFailed as exc:
            raise FailReply(f'Failed to configure {name}: {exc}') from exc
        finally:
            if product is not None:
                product.configure_task = None
                if not success:
                    await self._manager.kill_product(product)

    @time_request
    async def request_product_configure(self, ctx, name: str, config: str) -> Tuple[str, str, int]:
        """Configure a subarray product instance.

        A subarray product instance is comprised of an optional telescope
        state, a collection of containers running required services, and
        a networking configuration appropriate for the required data
        movement.

        On configuring a new product, several steps occur:
         * Build initial static configuration. Includes elements such as IP
           addresses of deployment machines, multicast subscription details etc
         * Launch a new Telescope State Repository (redis instance) for this
           product and copy in static config (if the subarray product contains
           any SDP components).
         * Launch service containers as described in the static configuration.
         * Verify all services are running and reachable.

        Parameters
        ----------
        name : str
            The ID to use for this product (an arbitrary string, with
            characters A-Z, a-z, 0-9 and _). It may optionally be
            suffixed with a "*" to request that a unique name is generated
            by replacing the "*" with a suffix.
        config : str
            A JSON-encoded dictionary of configuration data.

        Returns
        -------
        name : str
            Actual subarray-product-id
        host : str
            Host of product controller katcp interface
        port : int
            Port of product controller katcp interface
        """
        try:
            config_dict = load_json_dict(config)
        except ValueError as exc:
            raise FailReply(f'Config is not valid JSON: {exc}') from None
        product = await self.product_configure(name, config_dict)
        assert product.host is not None and product.ports
        return product.name, str(product.host), product.ports['katcp']

    async def product_deconfigure(self, product: Product, force: bool = False) -> None:
        """Implementation of meth:`request_product_deconfigure`."""
        timeout = 15 if force else None
        try:
            async with async_timeout.timeout(timeout):
                if product.task_state != Product.TaskState.ACTIVE:
                    raise FailReply(f"Product {product.name} is not yet configured")
                if not product.katcp_conn or not product.katcp_conn.is_connected:
                    raise FailReply('Not connected to product controller (try with force=True)')
                reply = (await product.katcp_conn.request('product-deconfigure', force))[0]
                # reply will be empty if this is an older product controller.
                # Otherwise a return of False (encoded as b'0') tells us that
                # the product controller will die promptly.
                if force or (reply and reply[0] == b'0'):
                    # Make sure it actually dies; if not we'll force it when we
                    # time out.
                    await product.dead_event.wait()
        except Exception as exc:
            if not force:
                raise
            product.logger.warning(f'Graceful kill failed, force-killing: {exc}')
            # Mesos will eventually kill the slave tasks when it sees that
            # the framework has disappeared.
            await self._manager.kill_product(product)

    @time_request
    async def request_product_deconfigure(self, ctx, name: str, force: bool = False) -> None:
        """Deconfigure an existing subarray product.

        Parameters
        ----------
        subarray_product_id : string
            Subarray product to deconfigure
        force : bool, optional
            Take down the subarray immediately, even if it is still capturing.
            If it does not complete within 15 seconds it is taken down anyway.
        """
        product = self._manager.products.get(name)
        if product is None:
            raise FailReply(f"Deconfiguration of subarray product {name} requested, "
                            "but no configuration found.")
        await self.product_deconfigure(product, force)

    @time_request
    async def request_product_reconfigure(self, ctx, name: str) -> None:
        """
        Reconfigure the specified subarray product instance.

        The primary use of this command is to restart the components for a particular
        subarray product without having to reconfigure the rest of the system.

        Essentially this runs a deconfigure() followed by a configure() with
        the same parameters as originally specified via the
        product-configure katcp request.

        Parameters
        ----------
        subarray_product_id : string
            The ID of the subarray product to reconfigure.
        """
        product_logger = logging.LoggerAdapter(logger, dict(subarray_product_id=name))
        product_logger.info("?product-reconfigure called on %s", name)
        product = self._manager.products.get(name)
        if product is None:
            raise FailReply(f"The specified subarray product id {name} has no existing "
                            f"configuration and thus cannot be reconfigured.")
        config = product.config

        product_logger.info("Deconfiguring %s as part of a reconfigure request", name)
        try:
            await self.product_deconfigure(product)
        except Exception as error:
            msg = "Unable to deconfigure as part of reconfigure"
            product_logger.exception(msg)
            raise FailReply(f"{msg}: {error}")

        product_logger.info("Waiting for %s to disappear", name)
        await product.dead_event.wait()

        product_logger.info("Issuing new configure for %s as part of reconfigure request.",
                            name)
        try:
            await self.product_configure(name, config)
        except Exception as error:
            msg = "Unable to configure as part of reconfigure, original array deconfigured"
            product_logger.exception(msg)
            raise FailReply(f"{msg}: {error}")

    @time_request
    async def request_product_list(self, ctx, name: str = None) -> None:
        """List existing subarray products

        Parameters
        ----------
        subarray_product_id : str, optional
            If specified, report on only this subarray product ID

        Returns
        -------
        num_informs : int
            Number of subarray products listed
        """
        if name is None:
            products = list(self._manager.products.values())
        elif name in self._manager.products:
            products = [self._manager.products[name]]
        else:
            raise FailReply("This product id has no current configuration.")
        ctx.informs([(s.name, await s.description()) for s in products])

    @time_request
    async def request_capture_init(self, ctx, subarray_product_id: str,
                                   override_dict_json: str = '{}') -> str:
        """Request capture of the specified subarray product to start.

        Note: This command is used to prepare the SDP for reception of data as
        specified by the subarray product provided. It is necessary to call this
        command before issuing a start command to the CBF. Once this command
        has returned `OK`, SDP is ready to receive data from CBF.

        Upon capture-init the subarray product starts a new capture block which
        lasts until the next capture-done command. This corresponds to the
        notion of a "file". The capture-init command returns an ID string that
        uniquely identifies the capture block and can be used to link various
        output products and data sets produced during the capture block.

        Parameters
        ----------
        subarray_product_id : str
            The ID of the subarray product to initialise. This must have
            already been configured via the product-configure command.
        override_dict_json : str, optional
            Configuration dictionary to merge with the subarray config.

        Returns
        -------
        capture_block_id : str
            ID of the new capture block
        """
        capture_block_id = await self._manager.get_capture_block_id()
        katcp_conn = self._get_katcp(subarray_product_id)
        await katcp_conn.request('capture-init', capture_block_id, override_dict_json)
        return capture_block_id

    @time_request
    async def request_telstate_endpoint(
            self, ctx, subarray_product_id: str = None) -> Optional[str]:
        """Returns the endpoint for the telescope state of the specified subarray product.

        If no subarray product is specified, generates an inform for each
        active subarray product.

        Parameters
        ----------
        subarray_product_id : str, optional
            The id of the subarray product whose state we wish to return.

        Returns
        -------
        state : str
        """
        if subarray_product_id is None:
            ctx.informs([(product_id, await product.get_telstate_endpoint())
                         for (product_id, product) in self._manager.products.items()])
            return None   # ctx.informs sends the reply
        product = self._manager.products.get(subarray_product_id)
        if product is None:
            raise FailReply('No existing subarray product configuration with this id found')
        return await product.get_telstate_endpoint()

    @time_request
    async def request_capture_status(
            self, ctx, subarray_product_id: str = None) -> Optional[ProductState]:
        """Returns the status of the specified subarray product.

        If no subarray product is specified, generates an inform per subarray
        product and return the number of such informs.

        Request Arguments
        -----------------
        subarray_product_id : string
            The id of the subarray product whose state we wish to return.

        Returns
        -------
        state : str
        """
        if subarray_product_id is None:
            products = list(self._manager.products.values())
            ctx.informs([(s.name, await s.get_state()) for s in products])
            return None     # ctx.informs sends the reply
        else:
            product = self._manager.products.get(subarray_product_id)
            if product is None:
                raise FailReply('No existing subarray product configuration with this id found')
            return await product.get_state()

    @time_request
    async def request_capture_done(self, ctx, subarray_product_id: str) -> str:
        """Halts the currently specified subarray product

        Parameters
        ----------
        subarray_product_id : str
            The id of the subarray product whose state we wish to halt.

        Returns
        -------
        cbid : str
            Capture-block ID that was stopped
        """
        katcp_conn = self._get_katcp(subarray_product_id)
        reply, informs = await katcp_conn.request('capture-done')
        return reply[0].decode()

    async def _deconfigure_all(self) -> None:
        """Forcibly deconfigure all products.

        Any errors are simply logged. This is only intended to be used as part
        of ``sdp-shutdown``.
        """
        futures: List[Awaitable[None]] = []
        names: List[str] = []
        for product in list(self._manager.products.values()):
            names.append(product.name)
            futures.append(self.product_deconfigure(product, force=True))
        results = await asyncio.gather(*futures, return_exceptions=True)
        for name, result in zip(names, results):
            if isinstance(result, BaseException):
                logger.warning("Failed to deconfigure product %s during sdp-shutdown. "
                               "Forging ahead...", name, exc_info=result,
                               extra=dict(subarray_product_id=name))

    @staticmethod
    async def _poweroff_endpoints(consul_url: yarl.URL) -> Sequence[Tuple[str, int]]:
        """Get URLs for the poweroff service."""
        url = (consul_url / CONSUL_POWEROFF_PATH).with_query(near='_agent')
        try:
            async with aiohttp.ClientSession(raise_for_status=True) as session:
                async with session.get(url) as resp:
                    nodes = await resp.json()
        except (OSError, ValueError, aiohttp.ClientError) as exc:
            msg = ('Could not retrieve list of nodes running poweroff service from consul. '
                   'No nodes will be powered off.')
            logger.exception(msg)
            raise FailReply(f'{msg} {exc}')
        endpoints: List[Tuple[str, int]] = []
        for node in nodes:
            address: str = node.get('ServiceAddress') or node.get('Address')
            port: int = node.get('ServicePort')
            endpoints.append((address, port))
        # Put the nodes closest to localhost (which likely includes localhost
        # itself) at the end.
        endpoints.reverse()
        return endpoints

    @time_request
    async def request_sdp_shutdown(self, ctx) -> str:
        """Shut down the master controller and all controlled nodes.

        Note that despite the name, this will shut down all nodes that
        have registered a poweroff-server service with consul, regardless of
        which subsystem they belong to.

        Returns
        -------
        success : {'ok', 'fail'}
            Whether the shutdown sequence of all other nodes succeeded.
        hosts : str
            On success, a comma-separated lists of hosts that have been shutdown; on
            failure a human-readable message listing success and failure machines.
        """
        logger.warning("Master Controller interrupted by sdp-shutdown request - "
                       "deconfiguring existing products (may lead to data loss!).")
        await self._deconfigure_all()
        endpoints = await self._poweroff_endpoints(self._consul_url)
        urls = [yarl.URL.build(scheme='http', host=host, port=port, path='/poweroff')
                for (host, port) in endpoints]
        successful: List[str] = []
        failed: List[str] = []
        async with aiohttp.ClientSession() as session:
            async def post1(url):
                async with session.post(url, headers={'X-Poweroff-Server': '1'}) as resp:
                    resp.raise_for_status()

            futures = [post1(url) for url in urls]
            results = await asyncio.gather(*futures, return_exceptions=True)
            for endpoint, result in zip(endpoints, results):
                if isinstance(result, BaseException):
                    logger.warning('Shutting down %s:%d failed: %s',
                                   endpoint[0], endpoint[1], result)
                    failed.append(endpoint[0])
                else:
                    successful.append(endpoint[0])
        success_str = ','.join(successful)
        if failed:
            failed_str = ','.join(failed)
            raise FailReply(f'Success: {success_str} Failed: {failed_str}')
        else:
            return success_str


class _InvalidGuiUrlsError(RuntimeError):
    pass


def _load_gui_urls_file(filename: str) -> List[Dict[str, str]]:
    try:
        with open(filename) as gui_urls_file:
            gui_urls = json.load(gui_urls_file)
    except OSError as error:
        raise _InvalidGuiUrlsError('Cannot read {}: {}'.format(filename, error))
    except ValueError as error:
        raise _InvalidGuiUrlsError('Invalid JSON in {}: {}'.format(filename, error))
    if not isinstance(gui_urls, list):
        raise _InvalidGuiUrlsError('{} does not contain a list'.format(filename))
    return gui_urls


def _load_gui_urls_dir(dirname: str) -> List[Dict[str, str]]:
    try:
        gui_urls = []
        for name in sorted(os.listdir(dirname)):
            filename = os.path.join(dirname, name)
            if filename.endswith('.json') and os.path.isfile(filename):
                gui_urls.extend(_load_gui_urls_file(filename))
    except OSError as error:
        raise _InvalidGuiUrlsError('Cannot read {}: {}'.format(dirname, error))
    return gui_urls


def _load_gui_urls(path: str) -> List[Dict[str, str]]:
    if os.path.isdir(path):
        return _load_gui_urls_dir(path)
    else:
        return _load_gui_urls_file(path)


def parse_args(argv: List[str]) -> argparse.Namespace:
    usage = "%(prog)s [options] zk singularity"
    parser = argparse.ArgumentParser(usage=usage)
    parser.add_argument('-a', '--host', default="", metavar='HOST',
                        help='attach to server HOST [localhost]')
    parser.add_argument('-i', '--interface-mode', default=False,
                        action='store_true',
                        help='run the controller in interface only mode for testing '
                             'integration and ICD compliance. [%(default)s]')
    parser.add_argument('-p', '--port', type=int, default=5001, metavar='N',
                        help='katcp listen port [%(default)s]')
    parser.add_argument('-l', '--log-level', metavar='LEVEL',
                        help='set the Python logging level [%(default)s]')
    parser.add_argument('--name', default='sdpmc',
                        help='name to use in Zookeeper and Singularity [%(default)s]')
    parser.add_argument('--external-hostname', metavar='FQDN', default=socket.getfqdn(),
                        help='Name by which others connect to this machine [%(default)s]')
    parser.add_argument('--http-port', type=int, default=8080, metavar='PORT',
                        help='port for the web server [%(default)s]')
    parser.add_argument('--haproxy', action='store_true',
                        help='Run haproxy to provide frontend to GUIs [no]')
    parser.add_argument('--external-url', metavar='URL', type=yarl.URL,
                        help='External URL for clients to browse the GUI')
    parser.add_argument('--consul-url', type=yarl.URL, default='http://127.0.0.1:8500/',
                        help='base URL for local consul agent [%(default)s]')
    parser.add_argument('--image-tag-file',
                        metavar='FILE', help='Load image tag to run from file (on each configure)')
    parser.add_argument('--s3-config-file',
                        metavar='FILE',
                        help='configuration for connecting services to S3 '
                             '(loaded on each configure)')
    parser.add_argument('--safe-multicast-cidr', default='239.192.0.0/18',
                        metavar='MULTICAST-CIDR',
                        help='block of multicast addresses from which to draw internal allocation. '
                             'Needs to be at least /16. [%(default)s]')
    parser.add_argument('--gui-urls', metavar='FILE-OR-DIR',
                        help='file containing JSON describing related GUIs, '
                             'or directory with .json files [none]')
    parser.add_argument('--registry',
                        default='sdp-docker-registry.kat.ac.za:5000', metavar='HOST:PORT',
                        help='registry from which to pull images [%(default)s]')
    parser.add_argument('--no-pull', action='store_true', default=False,
                        help='skip pulling images from the registry if already present')
    add_shared_options(parser)
    katsdpservices.add_aiomonitor_arguments(parser)
    # TODO: support Zookeeper ensemble
    parser.add_argument('zk',
                        help='endpoint for Zookeeper server e.g. server.domain:2181')
    parser.add_argument('singularity',
                        help='URL for Singularity server')
    args = parser.parse_args(argv)

    if args.localhost:
        args.host = '127.0.0.1'
        args.external_hostname = '127.0.0.1'

    if args.gui_urls is not None:
        try:
            args.gui_urls = _load_gui_urls(args.gui_urls)
        except _InvalidGuiUrlsError as exc:
            parser.error(str(exc))
        except Exception as exc:
            parser.error(f'Could not read {args.gui_urls}: {exc}')
    else:
        args.gui_urls = []

    if args.external_url is None:
        args.external_url = yarl.URL.build(
            scheme='http', host=args.external_hostname, port=args.http_port, path='/')

    if args.s3_config_file is None and not args.interface_mode:
        parser.error('--s3-config-file is required (unless --interface-mode is given)')

    return args
