################################################################################
# Copyright (c) 2013-2023, 2025, National Research Foundation (SARAO)
#
# Licensed under the BSD 3-Clause License (the "License"); you may not use
# this file except in compliance with the License. You may obtain a copy
# of the License at
#
#   https://opensource.org/licenses/BSD-3-Clause
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

"""Dynamically register and deregister services with consul."""

import asyncio
import logging
import uuid
from typing import Optional

import aiohttp
import yarl

from .defaults import LOCALHOST

CONSUL_PORT = 8500
CONSUL_URL = yarl.URL.build(scheme="http", host=LOCALHOST, port=CONSUL_PORT)
# Usually needs must less than this, but registering a service requires
# storing the record on disk in the consul catalogue, and things like VM
# backups can make that unusually slow.
CONSUL_TIMEOUT = 20
logger = logging.getLogger(__name__)


class ConsulService:
    def __init__(self, service_id: Optional[str] = None, base_url: yarl.URL = CONSUL_URL) -> None:
        self.service_id = service_id
        self.base_url = base_url

    async def deregister(self) -> bool:
        """Deregister the service from Consul, if currently registered.

        If it fails, no exception is thrown, but a warning message is logged
        (and it is safe to try again). Returns true if the service is no
        longer registered, whether because it was already unregistered or
        because it was successfully deregistered.
        """
        if self.service_id is None:
            return True
        timeout = aiohttp.ClientTimeout(total=CONSUL_TIMEOUT)
        try:
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.put(
                    self.base_url / f"v1/agent/service/deregister/{self.service_id}"
                ) as resp:
                    resp.raise_for_status()
                    self.service_id = None
                    logging.info("Deregistered from consul (ID %s)", self.service_id)
                    return True
        except aiohttp.ClientError as exc:
            logger.warning("Could not deregister from consul: %s", exc)
            return False
        except asyncio.TimeoutError:
            logger.warning("Timed out registering from consul")
            return False

    @classmethod
    async def register(cls, service: dict, base_url: yarl.URL = CONSUL_URL) -> "ConsulService":
        """Register a service with Consul.

        If registration fails, an instance of the class is still returned,
        but its deregistration will be a no-op.

        Parameters
        ----------
        service
            A JSON-serializable dictionary to pass to Consul. It should exclude
            the ``ID`` member, which will be generated automatically.
        base_url
            Base URL of the Consul service. The default is to register with
            the consul agent on localhost.
        """
        service_id = str(uuid.uuid4())
        timeout = aiohttp.ClientTimeout(total=CONSUL_TIMEOUT)
        service = {**service, "ID": service_id}
        try:
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.put(
                    base_url / "v1/agent/service/register",
                    params={"replace-existing-checks": "1"},
                    json=service,
                ) as resp:
                    resp.raise_for_status()
                    logging.info("Registered with consul as ID %s", service_id)
                    return ConsulService(service_id, base_url)
        except aiohttp.ClientError as exc:
            logger.warning("Could not register with consul: %s", exc)
            return ConsulService(base_url=base_url)
        except asyncio.TimeoutError:
            logger.warning("Timed out registering with consul")
            return ConsulService(base_url=base_url)
