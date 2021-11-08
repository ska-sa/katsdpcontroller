"""Tests for :mod:`katsdpcontroller.consul`."""

from urllib.parse import urljoin

import asynctest
from aioresponses import aioresponses

from ..consul import ConsulService, CONSUL_URL


class TestConsulService(asynctest.TestCase):
    def setUp(self) -> None:
        self.service_data = {
            'Name': 'product-controller',
            'Tags': ['prometheus-metrics'],
            'Port': 12345
        }

    @aioresponses()
    async def test_register_failure(self, m) -> None:
        m.put(
            urljoin(CONSUL_URL, '/v1/agent/service/register?replace-existing-checks=1'),
            status=500
        )
        service = await ConsulService.register(self.service_data)
        self.assertIsNone(service.service_id)

    @aioresponses()
    async def test_register_success(self, m) -> None:
        m.put(urljoin(CONSUL_URL, '/v1/agent/service/register?replace-existing-checks=1'))
        service = await ConsulService.register(self.service_data)
        self.assertIsNotNone(service.service_id)

    async def test_deregister_none(self) -> None:
        service = ConsulService()
        self.assertTrue(await service.deregister())

    @aioresponses()
    async def test_deregister_success(self, m) -> None:
        service_id = 'test-id'
        service = ConsulService(service_id)
        m.put(urljoin(CONSUL_URL, f'/v1/agent/service/deregister/{service_id}'))
        self.assertTrue(await service.deregister())
        self.assertIsNone(service.service_id)

    @aioresponses()
    async def test_deregister_failure(self, m) -> None:
        service_id = 'test-id'
        service = ConsulService(service_id)
        m.put(urljoin(CONSUL_URL, f'/v1/agent/service/deregister/{service_id}'), status=500)
        self.assertFalse(await service.deregister())
        self.assertEqual(service.service_id, service_id)
