"""Science Data Processor Master Controller"""

import asyncio
import logging
import argparse
import enum
import uuid
import ipaddress
import json
import itertools
import re
from abc import abstractmethod
from typing import Dict, Mapping, Optional, Tuple, Set

import aiozk
import aiokatcp
from aiokatcp import FailReply
import async_timeout
import yarl

import katsdpcontroller
from . import singularity, product_config
from .controller import time_request, load_json_dict, ProductState


logger = logging.getLogger(__name__)


class NoAddressesError(Exception):
    """Insufficient multicast addresses available"""


class ProductFailed(Exception):
    """Attempt to create a subarray product was unsuccessful"""


def _log_exceptions(task: asyncio.Task, name: str, *, logger=logger) -> None:
    def callback(future):
        try:
            future.result()
        except asyncio.CancelledError:
            pass
        except Exception:
            logger.exception('Exception in %s', name)

    task.add_done_callback(callback)


class Product:
    class TaskState(enum.Enum):
        CREATED = 1
        STARTING = 2
        ACTIVE = 3
        DEAD = 4

    def __init__(self, name: str, configure_task: Optional[asyncio.Task]):
        self.name = name
        self.configure_task = configure_task
        self.katcp_conn: Optional[aiokatcp.Client] = None
        self.task_state = Product.TaskState.CREATED
        self.host: Optional[str] = None
        self.port: Optional[int] = None
        self.multicast_groups: Set[ipaddress.IPv4Address] = set()
        self.logger = logging.LoggerAdapter(logger, dict(subarray_product_id=name))
        self.dead_event = asyncio.Event()

    def connect(self, host: str, port: int) -> None:
        self.host = host
        self.port = port
        self.katcp_conn = aiokatcp.Client(host, port)
        # TODO: start a watchdog
        # TODO: set up sensor proxying

    def died(self) -> None:
        self.task_state = Product.TaskState.DEAD
        self.dead_event.set()
        if self.configure_task is not None:
            self.configure_task.cancel()
            self.configure_task = None
        if self.katcp_conn is not None:
            self.katcp_conn.close()
            self.katcp_conn = None
            # TODO: should this be async and await it being closed

    async def get_state(self) -> ProductState:
        if self.task_state == Product.TaskState.ACTIVE:
            # TODO: get actual state from katcp query
            return ProductState.IDLE
        elif self.task_state == Product.TaskState.DEAD:
            return ProductState.DEAD
        else:
            return ProductState.CONFIGURING


class ProductManagerBase:
    """Abstract base class for launching and managing subarray products."""

    def __init__(self, args: argparse.Namespace) -> None:
        self._multicast_network = ipaddress.IPv4Network(args.safe_multicast_cidr)
        self._next_multicast_group = self._multicast_network.network_address + 1

    @abstractmethod
    async def start(self) -> None: pass

    @abstractmethod
    async def stop(self) -> None: pass

    @property
    @abstractmethod
    def products(self) -> Mapping[str, Product]: pass

    @abstractmethod
    async def create_product(self, name: str) -> Product: pass

    @abstractmethod
    async def product_active(self, product: Product) -> None: pass

    @abstractmethod
    async def kill_product(self, product: Product) -> None: pass

    def _set_next_multicast_group(self, value: ipaddress.IPv4Address) -> None:
        """Control where to start looking in :meth:`get_multicast_groups`.

        Any `value` is accepted, but if it is out of range, it will be replaced
        by the first host address in the network.
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
        products = self.products.values()
        wrapped = False
        assert n_addresses >= 1
        # It might be the broadcast address, but must always be inside the network
        assert self._next_multicast_group in self._multicast_network
        while True:
            start = self._next_multicast_group
            # Check if there is enough space
            if int(self._multicast_network.broadcast_address) - int(start) < n_addresses:
                if wrapped:
                    # Wrapped for the second time - give up
                    raise NoAddressesError
                wrapped = True
                start = self._multicast_network.network_address + 1
                continue
            for i in range(n_addresses):
                if any(self._next_multicast_group in prod.multicast_groups for prod in products):
                    self._next_multicast_group = start + (i + 1)
                    break
            else:
                # We've found a usable range
                self._next_multicast_group += n_addresses
                for i in range(n_addresses):
                    product.multicast_groups.add(start + i)
                ans = str(start)
                if n_addresses > 1:
                    ans += '+{}'.format(n_addresses - 1)
                return ans


class InternalProductManager(ProductManagerBase):
    """Run subarray products as asyncio tasks within the process.

    This is intended **only** for integration testing.
    """
    pass


class SingularityProduct(Product):
    def __init__(self, name: str, configure_task: Optional[asyncio.Task]) -> None:
        super().__init__(name, configure_task)
        self.run_id = name + '-' + uuid.uuid4().hex
        self.task_id: Optional[str] = None


class SingularityProductManager(ProductManagerBase):
    """Run subarray products as tasks within Hubspot Singularity."""

    def __init__(self, args) -> None:
        super().__init__(args)
        self._request_id = args.name + '_product'
        self._deploy_id = uuid.uuid4().hex
        self._task_cache: Dict[str, dict] = {}  # Maps Singularity Task IDs to their info
        self._products: Dict[str, SingularityProduct] = {}
        self._reconciliation_task: Optional[asyncio.Task] = None
        self._sing = singularity.Singularity(yarl.URL(args.singularity) / 'api')
        self._zk = aiozk.ZKClient(args.zk, chroot=args.name)

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

    async def _ensure_request(self) -> None:
        """Create or update the Singularity request object"""
        # Singularity allows requests to be updated with POST, so just do it unconditionally
        await self._sing.create_request({
            "id": self._request_id,
            "requestType": "ON_DEMAND"
        })

    async def _ensure_deploy(self) -> None:
        """Create or update the Singularity deploy object"""
        deploy = {
            "id": self._deploy_id,
            "requestId": self._request_id,
            # TODO: pass through options from self
            # TODO: change to sdp_product_controller
            "command": "sdp_master_controller.py",
            "arguments": [
                "--port", "5101",
                "--http-port", "5102",
                "--user", "kat",
                "--s3-config-file", "dummy.json",
                "--interface-mode",             # TODO
                "zk://172.17.0.1:2181/mesos"    # TODO
            ],
            "containerInfo": {
                "type": "DOCKER",
                "docker": {
                    # TODO: use image resolver
                    "image": "sdp-docker-registry.kat.ac.za:5000/katsdpcontroller",
                    "forcePullImage": True,
                    "network": "BRIDGE",
                    "portMappings": [
                        {
                            "containerPortType": "LITERAL",
                            "containerPort": 5101,
                            "hostPortType": "FROM_OFFER",
                            "hostPort": 0
                        },
                        {
                            "containerPortType": "LITERAL",
                            "containerPort": 5102,
                            "hostPortType": "FROM_OFFER",
                            "hostPort": 1
                        }
                    ]
                }
            },
            "resources": {
                "cpus": 0.2,
                "memoryMb": 128,
                "numPorts": 2
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
                self._zk.get('/deploy'), self._sing.get_request(self._request_id))
            old_deploy = json.loads(old_deploy_raw)
            old_id = old_deploy['id']
            current_id = current_request['activeDeploy']['id']
            if old_id == current_id:
                deploy['id'] = current_id
                if deploy == old_deploy:
                    logger.info('Reusing deploy ID %s', current_id)
                    self._deploy_id = current_id
                    return
        except (aiozk.exc.NoNode, KeyError, TypeError, ValueError):
            logger.debug('Cannot reuse deploy', exc_info=True)
        logger.info('Creating new deploy with ID %s', self._deploy_id)
        deploy['id'] = self._deploy_id
        await self._sing.create_deploy({"deploy": deploy})
        await self._zk_set('/deploy', json.dumps(deploy).encode())

    async def _load_state(self) -> None:
        """Load existing subarray product state from Zookeeper"""
        default = {"version": 1, "products": {}, "next_multicast_group": "0.0.0.0"}
        try:
            payload = (await self._zk.get('/state'))[0]
            data = json.loads(payload)
            if data['version'] != 1:
                raise ValueError('Version mismatch')
            # TODO: apply a JSON schema check?
        except aiozk.exc.NoNode:
            logger.info('No existing state found')
            data = default
        except (KeyError, ValueError) as exc:
            logger.warning('Could not load existing state (%s), so starting fresh', exc)
            data = default
        for name, info in data['products'].items():
            prod = SingularityProduct(name, None)
            prod.run_id = info['run_id']
            prod.task_id = info['task_id']
            prod.task_state = Product.TaskState.ACTIVE
            prod.multicast_groups = {ipaddress.ip_address(addr)
                                     for addr in info['multicast_groups']}
            prod.connect(info['host'], info['port'])
            self._products[name] = prod
        # The implementation overrides it if it doesn't match the current
        # _multicast_network.
        self._set_next_multicast_group(ipaddress.IPv4Address(data['next_multicast_group']))

    async def _save_state(self) -> None:
        data = {
            'version': 1,
            'products': {
                prod.name: {
                    'run_id': prod.run_id,
                    'task_id': prod.task_id,
                    'host': prod.host,
                    'port': prod.port,
                    'multicast_groups': [str(group) for group in prod.multicast_groups]
                } for prod in self._products.values() if prod.task_state == Product.TaskState.ACTIVE
            },
            'next_multicast_group': str(self._next_multicast_group)
        }
        payload = json.dumps(data).encode()
        await self._zk_set('/state', payload)

    async def _reconcile_once(self) -> None:
        logger.debug('Starting reconciliation')
        data = await self._sing.get_request_tasks(self._request_id)
        # Start by assuming everything died and remove tasks as they're seen
        dead_tasks = set(self._task_cache.keys())
        expected_run_ids = {product.run_id for product in self._products.values()}
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
                    logger.info('Killing task %s with unknown run_id %s', task_id, run_id)
                    await self._try_kill_task(task_id)
        for task_id in dead_tasks:
            logger.debug('Task %s died', task_id)
            del self._task_cache[task_id]
        n_died = 0
        for product in list(self._products.values()):
            if product.task_id is not None and product.task_id not in self._task_cache:
                product.logger.info('Task for %s died', product.name)
                product.died()
                del self._products[product.name]
                n_died += 1
        if n_died > 0:
            await self._save_state()
        logging.debug('Reconciliation finished')

    async def _reconcile_repeat(self):
        while True:
            await asyncio.sleep(10)
            try:
                await self._reconcile_once()
            except Exception:
                logger.warning('Exception in reconciliation', exc_info=True)

    async def start(self) -> None:
        await self._zk.start()
        await self._mark_running()
        await self._load_state()
        await self._ensure_request()
        await self._ensure_deploy()
        await self._reconcile_once()
        self._reconciliation_task = asyncio.get_event_loop().create_task(self._reconcile_repeat())
        _log_exceptions(self._reconciliation_task, 'reconciliation')

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

    @property
    def products(self) -> Mapping[str, Product]:
        return self._products

    async def create_product(self, name: str) -> Product:
        assert name not in self._products
        product = SingularityProduct(name, asyncio.Task.current_task())
        self._products[name] = product
        await self._sing.create_run(self._request_id, {"runId": product.run_id})
        # Wait until the task is running or dead
        task_id: Optional[str] = None
        success = False
        loop = asyncio.get_event_loop()
        try:
            while True:
                try:
                    data = await self._sing.track_run(self._request_id, product.run_id)
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
                await asyncio.sleep(0.2)

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
            product.connect(host, port)
            success = True
            await self._save_state()

        finally:
            if not success:
                if task_id is not None:
                    # Make best effort to kill it; it might be dead already though
                    kill_task = loop.create_task(self._try_kill_task(task_id))
                    _log_exceptions(kill_task, f'kill {task_id}', logger=product.logger)
                if self._products.get(name) is product:
                    del self._products[name]
                product.configure_task = None   # Stops died() from trying to cancel us
                product.died()
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
    VERSION = "sdpcontroller-3.2"
    BUILD_STATE = "katsdpcontroller-" + katsdpcontroller.__version__

    _manager: ProductManagerBase

    def __init__(self, args: argparse.Namespace) -> None:
        if args.interface_mode:
            self._manager = InternalProductManager(args)
        else:
            self._manager = SingularityProductManager(args)
        self._manager_stopped = asyncio.Event()
        self._override_dicts: Dict[str, dict] = {}
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

    @time_request
    async def request_set_config_override(
            self, ctx, name: str, override_dict_json: str) -> str:
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
        success : {'ok', 'fail'}
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

        Returns
        -------
        success : {'ok', 'fail'}
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
                await product.katcp_conn.request('product-deconfigure', name, force)
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
            # TODO: product controller needs to self-terminate when finished.
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
        success : {'ok', 'fail'}

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
