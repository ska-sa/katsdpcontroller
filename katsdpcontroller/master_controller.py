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
import functools
import re
import time
import os
import socket
from abc import abstractmethod
from typing import Type, TypeVar, Dict, Tuple, Set, List, Mapping, Optional, Callable, Any

import aiozk
import aiokatcp
from aiokatcp import FailReply
import async_timeout
import jsonschema
import yarl
from prometheus_client import Gauge, Counter, Histogram
import katsdpservices

import katsdpcontroller
from . import singularity, product_config, scheduler, sensor_proxy
from .schemas import ZK_STATE       # type: ignore
from .controller import (time_request, load_json_dict, log_task_exceptions,
                         extract_shared_options, ProductState, add_shared_options)


_HINT_RE = re.compile(r'\bprometheus: *(?P<type>[a-z]+)(?:\((?P<args>[^)]*)\)|\b)',
                      re.IGNORECASE)
logger = logging.getLogger(__name__)
_T = TypeVar('_T')


class NoAddressesError(Exception):
    """Insufficient multicast addresses available"""


class ProductFailed(Exception):
    """Attempt to create a subarray product was unsuccessful"""


def _prometheus_factory(name: str,
                        sensor: aiokatcp.Sensor) -> Optional[sensor_proxy.PrometheusInfo]:
    assert sensor.description is not None
    match = _HINT_RE.search(sensor.description)
    if not match:
        return None
    type_ = match.group('type').lower()
    args_ = match.group('args')
    if type_ == 'counter':
        class_ = Counter
        if args_ is not None:
            logger.warning('Arguments are not supported for counters (%s)', sensor.name)
    elif type_ == 'gauge':
        class_ = Gauge
        if args_ is not None:
            logger.warning('Arguments are not supported for gauges (%s)', sensor.name)
    elif type_ == 'histogram':
        class_ = Histogram
        if args_ is not None:
            try:
                buckets = [float(x.strip()) for x in args_.split(',')]
                class_ = functools.partial(Histogram, buckets=buckets)
            except ValueError as exc:
                logger.warning('Could not parse histogram buckets (%s): %s', sensor.name, exc)
    else:
        logger.warning('Unknown Prometheus metric type %s for %s', type_, sensor.name)
        return None
    service, base = name.rsplit('.', 1)
    normalised_name = 'katsdpcontroller_' + base.replace('-', '_')
    return sensor_proxy.PrometheusInfo(class_, normalised_name, sensor.description,
                                       {'service': service})


def _load_s3_config(filename):
    with open(filename, 'r') as f:
        config = json.load(f)
    return config


class Product:
    """A single subarray product

    Parameters
    ----------
    name
        Subarray product ID
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

    def __init__(self, name: str, configure_task: Optional[asyncio.Task]) -> None:
        self.name = name
        self.configure_task = configure_task
        self.katcp_conn: Optional[aiokatcp.Client] = None
        self.task_state = Product.TaskState.CREATED
        self.host: Optional[str] = None
        self.port: Optional[int] = None
        self.multicast_groups: Set[ipaddress.IPv4Address] = set()
        self.logger = logging.LoggerAdapter(logger, dict(subarray_product_id=name))
        self.dead_event = asyncio.Event()
        self._dead_callbacks: List[Callable[['Product'], None]] = []

    def connect(self, server: aiokatcp.DeviceServer, host: str, port: int) -> None:
        """Notify product of the location of the katcp interface.

        After calling this, :attr:`host`, :attr:`port` and :attr:`katcp_conn`
        are all ready to use.
        """
        self.host = host
        self.port = port
        self.katcp_conn = sensor_proxy.SensorProxyClient(
            server, f'{self.name}.', {'subarray_product_id': self.name}, _prometheus_factory,
            host=host, port=port)
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
            # TODO: should this be async and await it being closed
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

    def add_dead_callback(self, callback: Callable[['Product'], None]):
        """Add a function to call when :meth:`died` is called."""
        self._dead_callbacks.append(callback)


class ProductManagerBase:
    """Abstract base class for launching and managing subarray products."""

    def __init__(self, args: argparse.Namespace, server: aiokatcp.DeviceServer) -> None:
        self._multicast_network = ipaddress.IPv4Network(args.safe_multicast_cidr)
        self._next_multicast_group = self._multicast_network.network_address + 1
        self._server = server

    @abstractmethod
    async def start(self) -> None: pass

    @abstractmethod
    async def stop(self) -> None: pass

    @property
    @abstractmethod
    def products(self) -> Mapping[str, Product]:
        """Get a list of all defined products, indexed by name.

        The caller should treat this as read-only, using the methods on this
        class to add/remove products or change their state.
        """

    @abstractmethod
    async def create_product(self, name: str) -> Product:
        """Create a new product called `name`.

        The product is brought to state :const:`~Product.TaskState.STARTING`
        before return.
        """

    @abstractmethod
    async def product_active(self, product: Product) -> None:
        """Move a product to state :const:`~Product.TaskState.ACTIVE`"""

    @abstractmethod
    async def kill_product(self, product: Product) -> None:
        """Move a product to state :const:`~Product.TaskState.DEAD`"""

    def _set_next_multicast_group(self, value: ipaddress.IPv4Address) -> None:
        """Control where to start looking in :meth:`get_multicast_groups`.

        This is intended for use by subclasses. Any `value` is accepted, but
        if it is out of range, it will be replaced by the first host address in
        the network.
        """
        if value not in self._multicast_network or value == self._multicast_network.network_address:
            value = self._multicast_network.network_address + 1
        self._next_multicast_group = value

    async def get_multicast_groups(self, product: Product, n_addresses: int) -> str:
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
                return ans

    @abstractmethod
    async def get_capture_block_id(self) -> str:
        """Generate a unique capture block ID"""


class InternalProductManager(ProductManagerBase):
    """Run subarray products as asyncio tasks within the process.

    This is intended **only** for integration testing.
    """
    pass


class SingularityProduct(Product):
    """Subarray product launched as a task within Hubspot Singularity"""

    def __init__(self, name: str, configure_task: Optional[asyncio.Task]) -> None:
        super().__init__(name, configure_task)
        self.run_id = name + '-' + uuid.uuid4().hex
        self.task_id: Optional[str] = None


class SingularityProductManager(ProductManagerBase):
    """Run subarray products as tasks within Hubspot Singularity."""

    # Interval between polling Singularity for data on tasks
    reconciliation_interval = 10.0
    # Once we create a new singularity run, this is the interval at which
    # we poll to try to find the task ID. It is deliberately chosen not to
    # divide into the reconciliation interval, which is needed for some of
    # the clocked unit tests so that it's possible to control which event
    # happens next.
    new_task_poll_interval = 0.3

    def __init__(self, args: argparse.Namespace, server: aiokatcp.DeviceServer) -> None:
        super().__init__(args, server)
        self._request_id_prefix = args.name + '_product_'
        self._task_cache: Dict[str, dict] = {}  # Maps Singularity Task IDs to their info
        # Task IDs that we didn't expect to see, but have been seen once.
        # They might be tasks we killed which haven't quite died yet, so they get
        # one reconciliation cycle to disappear on their own.
        self._probation: Set[str] = set()
        self._products: Dict[str, SingularityProduct] = {}
        self._reconciliation_task: Optional[asyncio.Task] = None
        self._sing = singularity.Singularity(yarl.URL(args.singularity) / 'api')
        self._zk = aiozk.ZKClient(args.zk, chroot=args.name)
        self._zk_state_lock = asyncio.Lock()
        self._args = args
        self._next_capture_block_id = 0

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
        environ = {}
        for key in ['KATSDP_LOG_ONELINE', 'KATSDP_LOG_LEVEL', 'KATSDP_LOG_GELF_ADDRESS']:
            if key in os.environ:
                environ[key] = os.environ[key]
        extra = {
            **json.loads(os.environ.get('KATSDP_LOG_GELF_EXTRA', '{}')),
            'task': 'product_controller',
            'docker.image': image
        }
        environ['KATSDP_LOG_GELF_EXTRA'] = json.dumps(extra)
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
                    "network": "HOST"
                }
            },
            "resources": {
                "cpus": 0.2,
                "memoryMb": 128,
                "numPorts": 4         # katcp, http, aiomonitor and aioconsole
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
                    logger.info('Reusing deploy ID %s', current_id)
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
                "version": 1,
                "products": {},
                "next_multicast_group": "0.0.0.0",
                "next_capture_block_id": 0
            }
            try:
                payload = (await self._zk.get('/state'))[0]
                data = json.loads(payload)
                if data.get('version') != 1:
                    raise ValueError('version mismatch')
                ZK_STATE.validate(data)
            except aiozk.exc.NoNode:
                logger.info('No existing state found')
                data = default
            except (ValueError, jsonschema.ValidationError) as exc:
                logger.warning('Could not load existing state (%s), so starting fresh', exc)
                data = default
            for name, info in data['products'].items():
                prod = SingularityProduct(name, None)
                prod.run_id = info['run_id']
                prod.task_id = info['task_id']
                prod.task_state = Product.TaskState.ACTIVE
                prod.multicast_groups = {ipaddress.ip_address(addr)
                                         for addr in info['multicast_groups']}
                prod.connect(self._server, info['host'], info['port'])
                self._products[name] = prod
                prod.add_dead_callback(self._product_died)
            # The implementation overrides it if it doesn't match the current
            # _multicast_network.
            self._set_next_multicast_group(ipaddress.IPv4Address(data['next_multicast_group']))
            self._next_capture_block_id = data['next_capture_block_id']

    async def _save_state(self) -> None:
        """Save the current state to Zookeeper"""
        async with self._zk_state_lock:
            data = {
                'version': 1,
                'products': {
                    prod.name: {
                        'run_id': prod.run_id,
                        'task_id': prod.task_id,
                        'host': prod.host,
                        'port': prod.port,
                        'multicast_groups': [str(group) for group in prod.multicast_groups]
                    } for prod in self._products.values()
                    if prod.task_state == Product.TaskState.ACTIVE
                },
                'next_multicast_group': str(self._next_multicast_group),
                'next_capture_block_id': self._next_capture_block_id
            }
            payload = json.dumps(data).encode()
            await self._zk_set('/state', payload)

    def _save_state_bg(self) -> None:
        """Save current state to Zookeeper in the background"""
        task = asyncio.get_event_loop().create_task(self._save_state())
        log_task_exceptions(task, logger, '_save_state')

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
        expected_run_ids = {product.run_id for product in self._products.values()}
        for request in requests:
            request_id = request['request']['id']
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
        for product in list(self._products.values()):
            if product.task_id is not None and product.task_id not in self._task_cache:
                product.logger.info('Task for %s died', product.name)
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
            except Exception:
                logger.warning('Exception in reconciliation', exc_info=True)

    def _product_died(self, product: Product) -> None:
        """Called by product.died"""
        if self._products.get(product.name) is product:
            del self._products[product.name]
            self._save_state_bg()

    async def start(self) -> None:
        await self._zk.start()
        await self._mark_running()
        await self._load_state()
        await self._reconcile_once()
        self._reconciliation_task = asyncio.get_event_loop().create_task(self._reconcile_repeat())
        log_task_exceptions(self._reconciliation_task, logger, 'reconciliation')

    async def stop(self) -> None:
        if self._reconciliation_task is not None:
            self._reconciliation_task.cancel()
            # Waits but does not raise exceptions - there is a done callback to log them
            await asyncio.wait([self._reconciliation_task])
        await self._save_state()   # Should all be saved already, but belt-and-braces
        await self._sing.close()
        await self._zk.close()
        for product in self._products.values():
            if product.katcp_conn is not None:
                product.katcp_conn.close()

    async def get_multicast_groups(self, product: Product, n_addresses: int) -> str:
        ans = await super().get_multicast_groups(product, n_addresses)
        await self._save_state()
        return ans

    async def get_capture_block_id(self) -> str:
        cbid = max(int(time.time()), self._next_capture_block_id)
        self._next_capture_block_id = cbid + 1
        await self._save_state()
        return str(cbid)

    @property
    def products(self) -> Mapping[str, SingularityProduct]:
        return self._products

    async def create_product(self, name: str) -> Product:
        assert name not in self._products
        s3_config = _load_s3_config(self._args.s3_config_file)
        args = extract_shared_options(self._args)
        # TODO: append --image-tag
        args.extend([
            '--s3-config=' + json.dumps(s3_config),
            f'--subarray-product-id={name}',
            f'{self._args.external_hostname}:{self._args.port}',
            f'zk://{self._args.zk}/mesos'
        ])

        product = SingularityProduct(name, asyncio.Task.current_task())
        self._products[name] = product
        product.add_dead_callback(self._product_died)
        request_id = await self._ensure_request(name)
        # TODO: use image resolver
        await self._ensure_deploy(name, 'katsdpcontroller')
        await self._sing.create_run(request_id, {
            "runId": product.run_id,
            "commandLineArgs": args
        })
        # Wait until the task is running or dead
        task_id: Optional[str] = None
        success = False
        loop = asyncio.get_event_loop()
        try:
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
            host = env['TASK_HOST']
            port = int(env['PORT0'])
            product.connect(self._server, host, port)
            success = True
            await self._save_state()

        finally:
            if not success:
                if task_id is not None:
                    # Make best effort to kill it; it might be dead already though
                    kill_task = loop.create_task(self._try_kill_task(task_id))
                    log_task_exceptions(kill_task, product.logger, f'kill {task_id}')
                product.configure_task = None   # Stops died() from trying to cancel us
                product.died()
                # No need for self._save_state, because the product never had a chance to
                # reach ACTIVE state and hence wasn't stored
        return product

    async def product_active(self, product: Product) -> None:
        product.task_state = Product.TaskState.ACTIVE
        await self._save_state()

    async def kill_product(self, product: Product) -> None:
        assert isinstance(product, SingularityProduct)
        if self._products.get(product.name) is product:
            del self._products[product.name]
            await self._save_state()
        if product.task_id is not None:
            await self._try_kill_task(product.task_id)
        # TODO: wait until it's actually dead?
        product.died()


class DeviceServer(aiokatcp.DeviceServer):
    """katcp server around a :class:`ProductManagerBase`."""

    VERSION = "sdpcontroller-3.2"
    BUILD_STATE = "katsdpcontroller-" + katsdpcontroller.__version__

    _manager: ProductManagerBase
    _override_dicts: Dict[str, dict]
    _image_lookup: scheduler.ImageLookup

    def __init__(self, args: argparse.Namespace) -> None:
        if args.interface_mode:
            self._manager = InternalProductManager(args, self)
        else:
            self._manager = SingularityProductManager(args, self)
        self._manager_stopped = asyncio.Event()
        self._override_dicts = {}
        if args.no_pull:
            self._image_lookup = scheduler.SimpleImageLookup(args.registry)
        else:
            self._image_lookup = scheduler.HTTPImageLookup(args.registry)
        super().__init__(args.host, args.port)

    async def start(self) -> None:
        await self._manager.start()
        await super().start()

    async def stop(self, cancel: bool = True) -> None:
        await super().stop(cancel)
        await self._manager.stop()
        self._manager_stopped.set()

    async def join(self) -> None:
        # The base version returns as soon as the superclass is stopped, but
        # we want to wait for _manager to be stopped too. We can't stop the
        # manager before stopping the server because that would introduce a
        # race where new requests could arrive and be processed after stopping
        # the manager.
        await super().join()
        await self._manager_stopped.wait()

    def _unique_name(self, prefix: str) -> str:
        """Find first unused name with the given prefix"""
        for i in itertools.count():
            name = prefix + str(i)
            if name not in self._manager.products:
                return name
        # itertools.count never finishes, but to keep mypy happy:
        assert False     # pragma: noqa

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
        return await self._image_lookup(repo, tag)

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
            odict = load_json_dict(override_dict_json)
            product_logger.info("Set override for subarray product %s to the following: %s",
                                name, odict)
            override = json.loads(override_dict_json)
            self._override_dicts[name] = override
        except ValueError as exc:
            msg = (f"The supplied override string {override_dict_json} does not appear to "
                   f"be a valid json string containing a dict. {exc}")
            product_logger.error(msg)
            raise FailReply(msg)
        n = len(override)
        return f"Set {n} override keys for subarray product {name}"

    @time_request
    async def request_product_configure(self, ctx, name: str, config: str) -> Tuple[str, str, int]:
        """Configure a SDP subarray product instance.

        A subarray product instance is comprised of a telescope state, a
        collection of containers running required SDP services, and a
        networking configuration appropriate for the required data movement.

        On configuring a new product, several steps occur:
         * Build initial static configuration. Includes elements such as IP
           addresses of deployment machines, multicast subscription details etc
         * Launch a new Telescope State Repository (redis instance) for this
           product and copy in static config.
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
        product_logger = logging.LoggerAdapter(
            logger, dict(subarray_product_id=name))
        product_logger.info("?product-configure called with: %s", ctx.req)
        if not re.match(r'^[A-Za-z0-9_]+\*?$', name):
            raise FailReply('Subarray product ID contains illegal characters')
        try:
            config_dict = load_json_dict(config)
        except ValueError as exc:
            raise FailReply(f'Config is not valid JSON: {exc}') from None

        if name in self._override_dicts:
            # this is a use-once set of overrides
            odict = self._override_dicts.pop(name)
            product_logger.warning("Setting overrides on %s for the following: %s",
                                   name, odict)
            config_dict = product_config.override(config_dict, odict)

        success = False
        product: Optional[Product] = None
        timeout = 60       # TODO: will need to be higher
        try:
            async with async_timeout.timeout(timeout):
                # This needs to be as close as possible to create_product
                # (specifically, no intervening awaits) so that other events
                # can't affect the set of valid names.
                if name in self._manager.products:
                    raise FailReply(f'Subarray product {name} already exists')
                if name.endswith('*'):
                    name = self._unique_name(name[:-1])
                product = await self._manager.create_product(name)
                assert product.katcp_conn is not None
                assert product.host is not None and product.port is not None

                await product.katcp_conn.wait_connected()
                await product.katcp_conn.request('product-configure', name,
                                                 json.dumps(config_dict))
                await self._manager.product_active(product)
                success = True
                return name, product.host, product.port
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
    async def request_product_deconfigure(self, ctx, name: str, force: bool = False) -> None:
        """Deconfigure an existing subarray product.

        Parameters
        ----------
        subarray_product_id : string
            Subarray product to deconfigure
        force : bool, optional
            Take down the subarray immediately, even if it is still capturing,
            and without waiting for completion.
        """
        product = self._manager.products.get(name)
        if product is None:
            raise FailReply(f"Deconfiguration of subarray product {name} requested, "
                            "but no configuration found.")
        timeout = 15 if force else None
        try:
            async with async_timeout.timeout(timeout):
                if product.task_state != Product.TaskState.ACTIVE:
                    raise FailReply(f"Product {name} is not yet configured")
                if not product.katcp_conn or not product.katcp_conn.is_connected:
                    raise FailReply('Not connected to product controller (try with force=True)')
                await product.katcp_conn.request('product-deconfigure', force)
                if force:
                    # Make sure it actually dies; if not we'll force it when we
                    # time out.
                    await product.dead_event.wait()
        except Exception as exc:
            if not force:
                raise
            product.logger.info(f'Graceful kill failed, force-killing: {exc}')
            # Mesos will eventually kill the slave tasks when it sees that
            # the framework has disappeared.
            await self._manager.kill_product(product)

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
        ctx.informs([(s.name, await s.get_state()) for s in products])

    @time_request
    async def request_capture_init(self, ctx, subarray_product_id: str,
                                   override_dict_json: str = '{}') -> str:
        """Request capture of the specified subarray product to start.

        Note: This command is used to prepare the SDP for reception of data as
        specified by the subarray product provided. It is necessary to call this
        command before issuing a start command to the CBF. Essentially the SDP
        will, once this command has returned 'OK', be in a wait state until
        reception of the stream control start packet.

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
            return await self.request_product_list(ctx, None)

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


class _InvalidGuiUrlsError(RuntimeError):
    pass


def _load_gui_urls_file(filename):
    try:
        with open(filename) as gui_urls_file:
            gui_urls = json.load(gui_urls_file)
    except (IOError, OSError) as error:
        raise _InvalidGuiUrlsError('Cannot read {}: {}'.format(filename, error))
    except ValueError as error:
        raise _InvalidGuiUrlsError('Invalid JSON in {}: {}'.format(filename, error))
    if not isinstance(gui_urls, list):
        raise _InvalidGuiUrlsError('{} does not contain a list'.format(filename))
    return gui_urls


def _load_gui_urls_dir(dirname):
    try:
        gui_urls = []
        for name in sorted(os.listdir(dirname)):
            filename = os.path.join(dirname, name)
            if filename.endswith('.json') and os.path.isfile(filename):
                gui_urls.extend(_load_gui_urls_file(filename))
    except (IOError, OSError) as error:
        raise _InvalidGuiUrlsError('Cannot read {}: {}'.format(dirname, error))
    return gui_urls


def parse_args(argv: List[str]) -> argparse.Namespace:
    usage = "%(prog)s [options] zk singularity"
    parser = argparse.ArgumentParser(usage=usage)
    parser.add_argument('-a', '--host', default="", metavar='HOST',
                        help='attach to server HOST [localhost]')
    parser.add_argument('-p', '--port', type=int, default=5001, metavar='N',
                        help='katcp listen port [%(default)s]')
    parser.add_argument('-l', '--log-level', metavar='LEVEL',
                        help='set the Python logging level [%(default)s]')
    parser.add_argument('--name', default='sdpmc',
                        help='name to use in Zookeeper and Singularity [%(default)s]')
    parser.add_argument('--external-hostname', metavar='FQDN', default=socket.getfqdn(),
                        help='Name by which others connect to this machine [%(default)s]')
    parser.add_argument('--dashboard-port', type=int, default=5006, metavar='PORT',
                        help='port for the Dash backend for the GUI [%(default)s]')
    parser.add_argument('--http-port', type=int, default=8080, metavar='PORT',
                        help='port for Prometheus metrics [%(default)s]')
    parser.add_argument('--image-tag-file',
                        metavar='FILE', help='Load image tag to run from file (on each configure)')
    parser.add_argument('--s3-config-file',
                        metavar='FILE',
                        help='Configuration for connecting services to S3 '
                             '(loaded on each configure)')
    parser.add_argument('--safe-multicast-cidr', default='225.100.0.0/16',
                        metavar='MULTICAST-CIDR',
                        help='Block of multicast addresses from which to draw internal allocation. '
                             'Needs to be at least /16. [%(default)s]')
    parser.add_argument('--gui-urls', metavar='FILE-OR-DIR',
                        help='File containing JSON describing related GUIs, '
                             'or directory with .json files [none]')
    parser.add_argument('--registry',
                        default='sdp-docker-registry.kat.ac.za:5000', metavar='HOST:PORT',
                        help='registry from which to pull images [%(default)s]')
    parser.add_argument('--no-pull', action='store_true', default=False,
                        help='Skip pulling images from the registry if already present')
    add_shared_options(parser)
    katsdpservices.add_aiomonitor_arguments(parser)
    # TODO: support Zookeeper ensemble
    parser.add_argument('zk',
                        help='Endpoint for Zookeeper server e.g. server.domain:2181')
    parser.add_argument('singularity',
                        help='URL for Singularity server')
    args = parser.parse_args(argv)

    if args.localhost:
        args.host = '127.0.0.1'
        args.external_hostname = '127.0.0.1'

    if args.gui_urls is not None:
        try:
            if os.path.isdir(args.gui_urls):
                args.gui_urls = _load_gui_urls_dir(args.gui_urls)
            else:
                args.gui_urls = _load_gui_urls_file(args.gui_urls)
        except _InvalidGuiUrlsError as exc:
            parser.error(str(exc))
        except Exception as exc:
            parser.error(f'Could not read {args.gui_urls}: {exc}')

    if args.s3_config_file is None and not args.interface_mode:
        parser.error('--s3-config-file is required (unless --interface-mode is given)')

    return args
