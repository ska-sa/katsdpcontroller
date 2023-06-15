################################################################################
# Copyright (c) 2013-2023, National Research Foundation (SARAO)
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

"""Tests for :mod:`katsdpcontroller.consul`."""

from typing import Generator

import pytest
import yarl
from aioresponses import aioresponses

from katsdpcontroller.consul import CONSUL_URL, ConsulService


class TestConsulService:
    def setup_method(self) -> None:
        self.service_data = {
            "Name": "product-controller",
            "Tags": ["prometheus-metrics"],
            "Port": 12345,
        }
        self.register_url = (CONSUL_URL / "v1/agent/service/register").with_query(
            {"replace-existing-checks": 1}
        )

    @pytest.fixture
    def m(self) -> Generator[aioresponses, None, None]:
        with aioresponses() as m:
            yield m

    async def test_register_failure(self, m) -> None:
        m.put(self.register_url, status=500)
        service = await ConsulService.register(self.service_data)
        assert service.service_id is None

    async def test_register_success(self, m) -> None:
        m.put(self.register_url)
        service = await ConsulService.register(self.service_data)
        assert service.service_id is not None

    async def test_deregister_none(self) -> None:
        service = ConsulService()
        assert await service.deregister()

    async def test_deregister_success(self, m) -> None:
        service_id = "test-id"
        service = ConsulService(service_id)
        m.put(CONSUL_URL / f"v1/agent/service/deregister/{service_id}")
        assert await service.deregister()
        assert service.service_id is None

    async def test_deregister_failure(self, m) -> None:
        service_id = "test-id"
        service = ConsulService(service_id)
        m.put(CONSUL_URL / f"v1/agent/service/deregister/{service_id}", status=500)
        assert not await service.deregister()
        assert service.service_id == service_id

    async def test_external_host(self, m) -> None:
        base_url = yarl.URL("http://foo.invalid:8500")
        register_url = (base_url / "v1/agent/service/register").with_query(
            {"replace-existing-checks": 1}
        )
        m.put(register_url)
        service = await ConsulService.register(self.service_data, base_url)
        m.put(base_url / f"v1/agent/service/deregister/{service.service_id}")
        assert service.base_url == base_url
        assert service.service_id is not None
        assert await service.deregister()
