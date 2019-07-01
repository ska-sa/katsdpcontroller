"""Control of a single subarray product"""

import logging
import json
from typing import Optional

import jsonschema
import aiokatcp
from aiokatcp import FailReply, Sensor
import katsdptelstate

import katsdpcontroller
from . import scheduler, product_config
from .controller import time_request, load_json_dict, DeviceStatus


logger = logging.getLogger(__name__)


class Resolver(scheduler.Resolver):
    """Resolver with some extra fields"""
    def __init__(self,
                 image_resolver: scheduler.ImageResolver,
                 task_id_allocator: scheduler.TaskIDAllocator,
                 http_url: Optional[str],
                 service_overrides: dict,
                 s3_config: dict) -> None:
        super().__init__(image_resolver, task_id_allocator, http_url)
        self.service_overrides = service_overrides
        self.s3_config = s3_config
        self.telstate: Optional[katsdptelstate.TelescopeState] = None


class DeviceServer(aiokatcp.DeviceServer):
    VERSION = 'product-controller-1.0'
    BUILD_STATE = "katsdpcontroller-" + katsdpcontroller.__version__

    def __init__(self, host: str, port: int,
                 parent_host: str, parent_port: int,
                 sched: scheduler.Scheduler,
                 batch_role: str,
                 interface_mode: bool,
                 image_resolver_factory: scheduler.ImageResolverFactory,
                 s3_config: dict,
                 graph_dir: str = None) -> None:
        self.sched = sched
        self.batch_role = batch_role
        self.interface_mode = interface_mode
        self.image_resolver_factory = image_resolver_factory
        self.s3_config = s3_config
        self.graph_dir = graph_dir
        self.parent = aiokatcp.Client(parent_host, parent_port)
        self.product: Optional[SDPSubarrayProductBase] = None

        super().__init__(host, port)
        # setup sensors
        self.sensors.add(Sensor(DeviceStatus, "device-status",
                                "Devices status of the subarray product controller",
                                default=DeviceStatus.OK, initial_status=Sensor.Status.NOMINAL))

    async def configure_product(self, name: str, config: dict) -> None:
        """Configure a subarray product in response to a request.

        Raises
        ------
        FailReply
            if a configure/deconfigure is in progress
        FailReply
            If any of the following occur
            - The specified subarray product id already exists, but the config
              differs from that specified
            - If docker python libraries are not installed and we are not using interface mode
            - There are insufficient resources to launch
            - A docker image could not be found
            - If one or more nodes fail to launch (e.g. container not found)
            - If one or more nodes fail to become alive
            - If we fail to establish katcp connection to all nodes requiring them.

        Returns
        -------
        str
            Final name of the subarray-product.
        """

        logger.debug('config is %s', json.dumps(config, indent=2, sort_keys=True))
        logger.info("Launching subarray product.")

        image_tag = config['config'].get('image_tag')
        if image_tag is not None:
            resolver_factory_args = dict(tag=image_tag)
        else:
            resolver_factory_args = {}
        resolver = Resolver(
            self.image_resolver_factory(**resolver_factory_args),
            scheduler.TaskIDAllocator(name + '-'),
            self.sched.http_url if self.sched else '',
            config['config'].get('service_overrides', {}),
            self.s3_config)

        # create graph object and build physical graph from specified resources
        if self.interface_mode:
            product_cls = SDPSubarrayProductInterface
        else:
            product_cls = SDPSubarrayProduct
        product = product_cls(self.sched, config, resolver, name, self)
        if self.graph_dir is not None:
            product.write_graphs(self.graph_dir)
        self.product = product   # Prevents another attempt to configure
        await product.configure()

    @time_request
    async def request_product_configure(self, ctx, name: str, config: str) -> None:
        """Configure a SDP subarray product instance.

        Parameters
        ----------
        name : str
            Name of the subarray product.
        config : str
            A JSON-encoded dictionary of configuration data.
        """
        logger.info("?product-configure called with: %s", ctx.req)

        if self.product is not None:
            raise FailReply('Already configured or configuring')
        try:
            config_dict = load_json_dict(config)
            product_config.validate(config_dict)
            config_dict = product_config.normalise(config_dict)
        except (ValueError, jsonschema.ValidationError) as exc:
            retmsg = f"Failed to process config: {exc}"
            logger.error(retmsg)
            raise FailReply(retmsg)

        await self.configure_product(name, config_dict)

    @time_request
    async def request_product_deconfigure(self, ctx, force: bool = False) -> None:
        """Deconfigure the product and shut down the server."""
        if self.product is None:
            raise FailReply('Have not yet configured')
        await self.product.deconfigure(force=force)
        self.halt()
