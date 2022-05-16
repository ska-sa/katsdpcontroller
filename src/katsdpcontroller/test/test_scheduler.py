import json
import base64
import socket
import logging
import uuid
import asyncio
import ipaddress
import unittest
from unittest import mock
import time
from decimal import Decimal
from collections import Counter
from typing import Optional, Callable

from nose.tools import (assert_equal, assert_false, assert_true, assert_in,
                        assert_is, assert_is_not, assert_is_none, assert_is_instance,
                        assert_count_equal, assert_not_equal)
import networkx
import pymesos
from addict import Dict
import asynctest
import aioresponses
import open_file_mock
import aiohttp
import pytest
from yarl import URL

from .. import scheduler
from ..scheduler import TaskState
from .utils import create_patch, future_return


class AnyOrderList(list):
    """Used for asserting that a list is present in a call, but without
    constraining the order. It does not require the elements to be hashable.
    """
    def __eq__(self, other):
        if isinstance(other, list):
            if len(self) != len(other):
                return False
            tmp = list(other)
            for item in self:
                try:
                    tmp.remove(item)
                except ValueError:
                    return False
            return True
        return NotImplemented

    def __ne__(self, other):
        if isinstance(other, list):
            return not (self == other)
        return NotImplemented


class SimpleTaskStats(scheduler.TaskStats):
    def __init__(self):
        self.batch_created = 0
        self.batch_started = 0
        self.batch_done = 0
        self.batch_retried = 0
        self.batch_failed = 0
        self.batch_skipped = 0
        self._state_counts = Counter()

    def task_state_changes(self, changes):
        for deltas in changes.values():
            for state, delta in deltas.items():
                self._state_counts[state] += delta

    def batch_tasks_created(self, n_tasks):
        self.batch_created += n_tasks

    def batch_tasks_started(self, n_tasks):
        self.batch_started += n_tasks

    def batch_tasks_done(self, n_tasks):
        self.batch_done += n_tasks

    def batch_tasks_retried(self, n_tasks):
        self.batch_retried += n_tasks

    def batch_tasks_failed(self, n_tasks):
        self.batch_failed += n_tasks

    def batch_tasks_skipped(self, n_tasks):
        self.batch_skipped += n_tasks

    @property
    def state_counts(self):
        """Return state counts as a normal dict with zero counts omitted"""
        return {key: value for (key, value) in self._state_counts.items() if value != 0}


class TestScalarResource:
    """Tests for :class:`katsdpcontroller.scheduler.ScalarResource`"""
    def setup(self):
        self.resource = scheduler.ScalarResource('cpus')
        self.parts = [self._make_part('foo', 3.6), self._make_part('*', 2.2)]

    def _make_part(self, role, value):
        return Dict({
            'name': 'cpus',
            'type': 'SCALAR',
            'role': role,
            'scalar': {'value': value},
            'allocation_info': {'role': 'foo/bar'}
        })

    def test_empty(self):
        assert_equal('cpus', self.resource.name)
        assert_is_instance(self.resource.available, Decimal)
        assert_equal(0, self.resource.available)
        assert_false(self.resource)
        assert_count_equal([], self.resource.info())

    def test_construct(self):
        for part in self.parts:
            self.resource.add(part)
        assert_equal(Decimal('5.8'), self.resource.available)
        assert_true(self.resource)
        assert_count_equal(self.parts, self.resource.info())

    def test_construct_other_order(self):
        for part in reversed(self.parts):
            self.resource.add(part)
        assert_equal(Decimal('5.8'), self.resource.available)
        assert_true(self.resource)
        assert_count_equal(self.parts, self.resource.info())

    def test_add_wrong_name(self):
        self.parts[0].name = 'mem'
        with pytest.raises(ValueError):
            self.resource.add(self.parts[0])

    def test_add_wrong_type(self):
        self.parts[0].type = 'RANGES'
        with pytest.raises(TypeError):
            self.resource.add(self.parts[0])

    def test_allocate(self):
        for part in self.parts:
            self.resource.add(part)
        alloced = self.resource.allocate(Decimal('3.9'))
        assert_count_equal(
            [self._make_part('foo', 3.6), self._make_part('*', 0.3)],
            alloced.info())
        assert_equal(Decimal('3.9'), alloced.available)
        assert_count_equal([self._make_part('*', 1.9)], self.resource.info())
        assert_equal(Decimal('1.9'), self.resource.available)

    def test_allocate_all(self):
        for part in self.parts:
            self.resource.add(part)
        alloced = self.resource.allocate(Decimal('5.8'))
        assert_count_equal(self.parts, alloced.info())
        assert_equal(Decimal('5.8'), alloced.available)
        assert_count_equal([], self.resource.info())
        assert_equal(Decimal('0.0'), self.resource.available)

    def test_over_allocate(self):
        with pytest.raises(ValueError):
            self.resource.allocate(Decimal('3.99'))

    def test_empty_request(self):
        request = self.resource.empty_request()
        assert_is_instance(request, scheduler.ScalarResourceRequest)
        assert_equal(Decimal('0.000'), request.amount)


class TestRangeResource:
    """Tests for :class:`katsdpcontroller.scheduler.RangeResource`"""
    def setup(self):
        self.resource = scheduler.RangeResource('ports')
        self.parts = [
            self._make_part('foo', [(5, 6), (20, 22)]),
            self._make_part('*', [(10, 12), (30, 30)])
        ]

    @classmethod
    def _make_part(cls, role, ranges):
        return Dict({
            'name': 'ports',
            'type': 'RANGES',
            'role': role,
            'allocation_info': {'role': 'foo/bar'},
            'ranges': {'range': [{'begin': r[0], 'end': r[1]} for r in ranges]}
        })

    def test_empty(self):
        assert_equal('ports', self.resource.name)
        assert_is_instance(self.resource.available, int)
        assert_equal(0, self.resource.available)
        assert_false(self.resource)
        assert_equal([], list(self.resource.info()))

    def test_construct(self):
        for part in self.parts:
            self.resource.add(part)
        assert_equal(9, len(self.resource))
        assert_equal(9, self.resource.available)
        assert_equal([5, 6, 20, 21, 22, 10, 11, 12, 30], list(self.resource))
        assert_count_equal(self.parts, list(self.resource.info()))

    def test_add_wrong_type(self):
        self.parts[0].type = 'SCALAR'
        with pytest.raises(TypeError):
            self.resource.add(self.parts[0])

    def test_allocate(self):
        for part in self.parts:
            self.resource.add(part)
        alloced = self.resource.allocate(6)
        assert_equal(6, alloced.available)
        assert_count_equal(
            [
                self._make_part('foo', [(5, 6), (20, 22)]),
                self._make_part('*', [(10, 10)])
            ], alloced.info())
        assert_equal([5, 6, 20, 21, 22, 10], list(alloced))
        assert_equal(3, self.resource.available)
        assert_count_equal(
            [self._make_part('*', [(11, 12), (30, 30)])],
            self.resource.info())

    def test_allocate_random(self):
        for part in self.parts:
            self.resource.add(part)
        alloced = self.resource.allocate(5, use_random=True)
        assert_equal(5, alloced.available)
        # It should have allocated exactly the foo resources
        assert_count_equal(
            [
                self._make_part('foo', [(5, 5), (6, 6), (20, 20), (21, 21), (22, 22)]),
            ], alloced.info())
        assert_equal([5, 6, 20, 21, 22], list(alloced))
        assert_equal(4, self.resource.available)
        assert_count_equal(
            [
                self._make_part('*', [(10, 12), (30, 30)])
            ], self.resource.info())

    def test_subset(self):
        for part in self.parts:
            self.resource.add(part)
        sub = self.resource.subset([5, 6, 10, 12, 40])
        assert_equal(4, sub.available)
        assert_count_equal(
            [
                self._make_part('foo', [(5, 5), (6, 6)]),
                self._make_part('*', [(10, 10), (12, 12)])
            ], sub.info())

    def test_subset_empty(self):
        for part in self.parts:
            self.resource.add(part)
        sub = self.resource.subset([40])
        assert_equal(0, sub.available)
        assert_false(sub)
        assert_count_equal([], sub.info())


class TestPollPorts(asynctest.TestCase):
    """Tests for poll_ports"""
    def setUp(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.bind(('127.0.0.1', 0))
        self.addCleanup(self.sock.close)
        self.port = self.sock.getsockname()[1]

    async def test_normal(self):
        future = asyncio.ensure_future(scheduler.poll_ports('127.0.0.1', [self.port]))
        # Sleep for while, give poll_ports time to poll a few times
        await asyncio.sleep(1)
        assert_false(future.done())
        self.sock.listen(1)
        await asyncio.wait_for(future, timeout=5)

    async def test_cancel(self):
        """poll_ports must be able to be cancelled gracefully"""
        future = asyncio.ensure_future(scheduler.poll_ports('127.0.0.1', [self.port]))
        await asyncio.sleep(0.2)
        future.cancel()
        with pytest.raises(asyncio.CancelledError):
            await future

    async def test_temporary_dns_failure(self):
        """Test poll ports against a temporary DNS failure."""
        with mock.patch.object(self.loop, 'getaddrinfo', autospec=True) as getaddrinfo:
            test_address = socket.getaddrinfo('127.0.0.1', self.port)
            # create a legitimate return future for getaddrinfo
            legit_future = asyncio.Future()
            legit_future.set_result(test_address)

            # sequential calls to getaddrinfo produce failure and success
            getaddrinfo.side_effect = [socket.gaierror("Failed to resolve"), legit_future]

            self.sock.listen(1)
            future = asyncio.ensure_future(scheduler.poll_ports('127.0.0.1', [self.port]))
            await asyncio.sleep(1)
            # temporary DNS failure
            assert_false(future.done())
            # wait for retry loop (currently 5s)
            # Note: it's tempting to try asynctest.ClockedTestCase, but that
            # only works if ALL interactions with the outside world are mocked
            # to be instantaneous.
            await asyncio.sleep(6)
            assert_true(future.done())


class TestTaskState:
    """Tests that TaskState ordering works as expected"""
    def test_compare(self):
        assert_true(TaskState.NOT_READY <= TaskState.RUNNING)
        assert_true(TaskState.NOT_READY < TaskState.RUNNING)
        assert_false(TaskState.NOT_READY > TaskState.RUNNING)
        assert_false(TaskState.NOT_READY >= TaskState.RUNNING)
        assert_false(TaskState.NOT_READY == TaskState.RUNNING)
        assert_true(TaskState.NOT_READY != TaskState.RUNNING)

        assert_false(TaskState.DEAD <= TaskState.RUNNING)
        assert_false(TaskState.DEAD < TaskState.RUNNING)
        assert_true(TaskState.DEAD > TaskState.RUNNING)
        assert_true(TaskState.DEAD >= TaskState.RUNNING)
        assert_false(TaskState.DEAD == TaskState.RUNNING)
        assert_true(TaskState.DEAD != TaskState.RUNNING)

        assert_true(TaskState.RUNNING <= TaskState.RUNNING)
        assert_false(TaskState.RUNNING < TaskState.RUNNING)
        assert_false(TaskState.RUNNING > TaskState.RUNNING)
        assert_true(TaskState.RUNNING >= TaskState.RUNNING)
        assert_true(TaskState.RUNNING == TaskState.RUNNING)
        assert_false(TaskState.RUNNING != TaskState.RUNNING)

    def test_compare_other(self):
        with pytest.raises(TypeError):
            TaskState.RUNNING < 3
        with pytest.raises(TypeError):
            TaskState.RUNNING > 3
        with pytest.raises(TypeError):
            TaskState.RUNNING <= 3
        with pytest.raises(TypeError):
            TaskState.RUNNING >= 3


class TestSimpleImageLookup(asynctest.TestCase):
    async def test(self) -> None:
        lookup = scheduler.SimpleImageLookup('registry.invalid:5000')
        assert_equal('registry.invalid:5000/foo:latest', await lookup('foo', 'latest'))


class TestHTTPImageLookup(asynctest.TestCase):
    def setUp(self) -> None:
        self.digest1 = "sha256:1234567812345678123456781234567812345678123456781234567812345678"
        self.digest2 = "sha256:2345678123456781234567812345678123456781234567812345678123456781"
        self.auth1 = aiohttp.BasicAuth('myuser', 'mypassword')
        self.auth2 = aiohttp.BasicAuth('myuser2', 'mypassword2')
        patcher = mock.patch('docker.auth.load_config', autospec=True)
        # This format isn't documented, but inferred from examining the real value
        load_config_mock = patcher.start()
        self.addCleanup(patcher.stop)
        load_config_mock.return_value = {
            'auths': {
                'registry.invalid:5000': {
                    'email': None,
                    'username': self.auth1.login,
                    'password': self.auth1.password,
                    'serveraddress': 'registry.invalid:5000'
                },
                'registry2.invalid:5000': {
                    'email': None,
                    'username': self.auth2.login,
                    'password': self.auth2.password,
                    'serveraddress': 'registry2.invalid:5000'
                }
            }
        }

    def _prepare_image(self, rmock, url, digest, **kwargs) -> None:
        # Response headers are modelled on some actual registry responses
        rmock.head(
            url,
            content_type='application/vnd.docker.distribution.manifest.v2+json',
            headers={
                'Content-Length': '1234',
                'Docker-Content-Digest': digest,
                'Docker-Distribution-Api-Version': 'registry/2.0',
                'Etag': f'"{url}"',
                'X-Content-Type-Options': 'nosniff',
                'Date': 'Thu, 26 Jan 2017 11:31:22 GMT'
            },
            **kwargs
        )

    def _prepare_image_auth_required(self, rmock, url, realm, scope, **kwargs) -> None:
        # Response headers are loosely based on Harbor 1.8
        rmock.head(
            url,
            status=401,
            content_type='application/json; charset=utf-8',
            headers={
                'WWW-Authenticate': (
                    'Bearer '
                    f'realm="{realm}",'
                    'service="harbor-registry",'
                    f'scope="{scope}"'
                )
            },
            **kwargs)

    @staticmethod
    def _check_basic(auth: aiohttp.BasicAuth) -> Callable:
        """Create aioresponses callback to ensure that basic auth credentials were provided."""
        def check(url, **kwargs) -> Optional[aioresponses.CallbackResult]:
            # We get the raw parameters to aiohttp rather than what it puts on
            # the wire, so we have to cater for different possible ways to
            # pass authentication.
            # Returning None tells aioresponses to use its normal mechanisms
            # to formulate the result.
            if kwargs.get('auth') == auth:
                return None
            header = kwargs.get('headers', {}).get(aiohttp.hdrs.AUTHORIZATION, '')
            if header == auth.encode():
                return None
            return aioresponses.CallbackResult(status=401, reason='Basic auth failed')

        return check

    @staticmethod
    def _check_token(url, headers, **kwargs):
        """aioresponses callback to check for bearer token."""
        if headers.get(aiohttp.hdrs.AUTHORIZATION) != 'Bearer helloiamatoken':
            return aioresponses.CallbackResult(status=401, reason='Token not found')
        return None  # Tells aioresponses to use its normal mechanisms

    async def test_relative(self) -> None:
        """Resolve an image without a registry, using the default registry."""
        with aioresponses.aioresponses() as rmock:
            self._prepare_image(
                rmock,
                'https://registry.invalid:5000/v2/myimage/manifests/latest',
                self.digest1,
                callback=self._check_basic(self.auth1))
            lookup = scheduler.HTTPImageLookup('registry.invalid:5000')
            image = await lookup('myimage', 'latest')
        assert_equal('registry.invalid:5000/myimage@' + self.digest1, image)

    async def test_absolute(self) -> None:
        """Resolve an image with an explicit registry."""
        with aioresponses.aioresponses() as rmock:
            self._prepare_image(
                rmock,
                'https://registry2.invalid:5000/v2/anotherimage/manifests/custom',
                self.digest2,
                callback=self._check_basic(self.auth2))
            lookup = scheduler.HTTPImageLookup('registry.invalid:5000')
            image = await lookup('registry2.invalid:5000/anotherimage', 'custom')
        assert_equal('registry2.invalid:5000/anotherimage@' + self.digest2, image)

    async def test_anonymous(self) -> None:
        """Resolve an image with a registry having no authentication information."""
        with aioresponses.aioresponses() as rmock:
            self._prepare_image(
                rmock,
                'https://anon.invalid:5000/v2/myimage/manifests/latest',
                self.digest2)
            lookup = scheduler.HTTPImageLookup('anon.invalid:5000')
            image = await lookup('myimage', 'latest')
        assert_equal('anon.invalid:5000/myimage@' + self.digest2, image)

    async def test_token_service(self) -> None:
        """Test redirection via a token service."""

        with aioresponses.aioresponses() as rmock:
            self._prepare_image_auth_required(
                rmock,
                'https://registry.invalid:5000/v2/myimage/manifests/latest',
                'https://tokenservice.invalid/service/token',
                'repository:myimage:pull')
            rmock.get(
                URL('https://tokenservice.invalid/service/token').with_query({
                    'client_id': 'katsdpcontroller',
                    'scope': 'repository:myimage:pull',
                    'service': 'harbor-registry'
                }),
                content_type='application/json; charset=utf-8',
                payload={
                    'token': 'helloiamatoken',
                    'expires_in': 1800,
                    'issued_at': '2021-09-29T11:01:59Z'
                },
                callback=self._check_basic(self.auth1))
            self._prepare_image(
                rmock,
                'https://registry.invalid:5000/v2/myimage/manifests/latest',
                self.digest1,
                callback=self._check_token)
            lookup = scheduler.HTTPImageLookup('registry.invalid:5000')
            image = await lookup('myimage', 'latest')
        assert_equal('registry.invalid:5000/myimage@' + self.digest1, image)

    async def test_http_fail(self) -> None:
        """Test that appropriate error is raised if bad HTTP status is returned."""
        with aioresponses.aioresponses() as rmock:
            self._prepare_image(
                rmock,
                'https://registry.invalid:5000/v2/myimage/manifests/latest',
                self.digest1,
                status=403)  # unauthorized
            lookup = scheduler.HTTPImageLookup('registry.invalid:5000')
            with pytest.raises(scheduler.ImageError):
                await lookup('myimage', 'latest')

    async def test_no_token(self):
        """Test that appropriate error is raised if token service doesn't return a token."""
        with aioresponses.aioresponses() as rmock:
            self._prepare_image_auth_required(
                rmock,
                'https://registry.invalid:5000/v2/myimage/manifests/latest',
                'https://tokenservice.invalid/service/token',
                'repository:myimage:pull')
            rmock.get(
                URL('https://tokenservice.invalid/service/token').with_query({
                    'client_id': 'katsdpcontroller',
                    'scope': 'repository:myimage:pull',
                    'service': 'harbor-registry'
                }),
                content_type='application/json; charset=utf-8',
                payload={},
                callback=self._check_basic(self.auth1))
            lookup = scheduler.HTTPImageLookup('registry.invalid:5000')
            with pytest.raises(scheduler.ImageError):
                await lookup('myimage', 'latest')

    async def test_invalid_token(self):
        """Test that appropriate error is raised if token isn't valid base64."""
        with aioresponses.aioresponses() as rmock:
            self._prepare_image_auth_required(
                rmock,
                'https://registry.invalid:5000/v2/myimage/manifests/latest',
                'https://tokenservice.invalid/service/token',
                'repository:myimage:pull')
            rmock.get(
                URL('https://tokenservice.invalid/service/token').with_query({
                    'client_id': 'katsdpcontroller',
                    'scope': 'repository:myimage:pull',
                    'service': 'harbor-registry'
                }),
                content_type='application/json; charset=utf-8',
                payload={
                    'token': 'this is not a valid token\n',
                    'expires_in': 1800,
                    'issued_at': '2021-09-29T11:01:59Z'
                },
                callback=self._check_basic(self.auth1))
            lookup = scheduler.HTTPImageLookup('registry.invalid:5000')
            with pytest.raises(scheduler.ImageError):
                await lookup('myimage', 'latest')

    async def test_missing_authenticate_fields(self):
        """Test error if WWW-Authenticate header is missing required fields."""
        with aioresponses.aioresponses() as rmock:
            rmock.head(
                'https://registry.invalid:5000/v2/myimage/manifests/latest',
                status=401,
                content_type='application/json; charset=utf-8',
                headers={
                    'WWW-Authenticate': 'Bearer service="harbor-registry"'
                }
            )
            lookup = scheduler.HTTPImageLookup('registry.invalid:5000')
            with pytest.raises(scheduler.ImageError):
                await lookup('myimage', 'latest')


class TestImageResolver(asynctest.TestCase):
    """Tests for :class:`katsdpcontroller.scheduler.ImageResolver`."""
    def setUp(self) -> None:
        self._open_mock = create_patch(self, 'builtins.open', new_callable=open_file_mock.MockOpen)
        self.lookup = scheduler.SimpleImageLookup('registry.invalid:5000')

    async def test_simple(self) -> None:
        """Test the base case"""
        resolver = scheduler.ImageResolver(self.lookup)
        resolver.override('foo', 'my-registry:5000/bar:custom')
        resolver.override('baz', 'baz:mytag')
        assert_equal('registry.invalid:5000/test1:latest', await resolver('test1'))
        assert_equal('registry.invalid:5000/test1:tagged', await resolver('test1:tagged'))
        assert_equal('my-registry:5000/bar:custom', await(resolver('foo')))
        assert_equal('registry.invalid:5000/baz:mytag', await(resolver('baz')))

    async def test_tag_file(self) -> None:
        """Test with a tag file"""
        self._open_mock.set_read_data_for('tag_file', 'tag1\n')
        resolver = scheduler.ImageResolver(self.lookup, tag_file='tag_file')
        resolver.override('foo', 'my-registry:5000/bar:custom')
        resolver.override('baz', 'baz:mytag')
        assert_equal('registry.invalid:5000/test1:tag1', await resolver('test1'))
        assert_equal('registry.invalid:5000/test1:tagged', await resolver('test1:tagged'))
        assert_equal('my-registry:5000/bar:custom', await resolver('foo'))
        assert_equal('registry.invalid:5000/baz:mytag', await(resolver('baz')))

    async def test_bad_tag_file(self) -> None:
        """A ValueError is raised if the tag file contains illegal content"""
        self._open_mock.set_read_data_for('tag_file', 'not a good :tag\n')
        with pytest.raises(ValueError):
            scheduler.ImageResolver(self.lookup, tag_file='tag_file')

    async def test_tag(self) -> None:
        """Test with an explicit tag"""
        resolver = scheduler.ImageResolver(self.lookup, tag_file='tag_file', tag='mytag')
        assert_equal('registry.invalid:5000/test1:mytag', await resolver('test1'))


class TestTaskIDAllocator:
    """Tests for :class:`katsdpcontroller.scheduler.TaskIDAllocator`."""
    def test_singleton(self):
        """Allocators with the same prefix are the same object"""
        a = scheduler.TaskIDAllocator('test-foo-')
        b = scheduler.TaskIDAllocator('test-bar-')
        c = scheduler.TaskIDAllocator('test-foo-')
        assert_is(a, c)
        assert_is_not(a, b)

    def test_call(self):
        a = scheduler.TaskIDAllocator('test-baz-')
        tid0 = a()
        tid1 = a()
        assert_equal('test-baz-00000000', tid0)
        assert_equal('test-baz-00000001', tid1)


def _make_resources(resources, role='default'):
    out = AnyOrderList()
    for name, value in resources.items():
        resource = Dict()
        resource.name = name
        resource.allocation_info.role = role
        if isinstance(value, (int, float)):
            resource.type = 'SCALAR'
            resource.scalar.value = float(value)
        else:
            resource.type = 'RANGES'
            resource.ranges.range = []
            for start, stop in value:
                resource.ranges.range.append(Dict(begin=start, end=stop - 1))
        out.append(resource)
    return out


def _make_text_attr(name, value):
    attr = Dict()
    attr.name = name
    attr.type = 'TEXT'
    attr.text.value = value
    return attr


def _make_json_attr(name, value):
    return _make_text_attr(name, base64.urlsafe_b64encode(json.dumps(value).encode('utf-8')))


def _make_offer(framework_id, agent_id, host, resources, attrs=(), role='default'):
    offer = Dict()
    offer.id.value = uuid.uuid4().hex
    offer.framework_id.value = framework_id
    offer.agent_id.value = agent_id
    offer.allocation_info.role = role
    offer.hostname = host
    offer.resources = _make_resources(resources, role)
    offer.attributes = attrs
    return offer


def _make_status(task_id, state):
    status = Dict()
    status.task_id.value = task_id
    status.state = state
    return status


class TestAgent(unittest.TestCase):
    """Tests for :class:`katsdpcontroller.scheduler.Agent`.

    This imports from :class:`unittest.TestCase` so that we can use
    ``assertLogs``, which has not been ported to :mod:`nose.tools` yet."""
    def _make_offer(self, resources, attrs=()):
        return _make_offer(self.framework_id, self.agent_id, self.host, resources, attrs)

    def setUp(self):
        self.agent_id = 'agentid'
        self.host = 'agenthostname'
        self.framework_id = 'framework'
        self.if_attr = _make_json_attr(
            'katsdpcontroller.interfaces',
            [{
                'name': 'eth0',
                'network': 'net0',
                'ipv4_address': '192.168.254.254',
                'numa_node': 1,
                'infiniband_devices': ['/dev/infiniband/foo'],
                'infiniband_multicast_loopback': False
            }])
        # Same as if_attr but without infiniband_multicast_loopback
        self.if_attr_loopback = _make_json_attr(
            'katsdpcontroller.interfaces',
            [{
                'name': 'eth0',
                'network': 'net0',
                'ipv4_address': '192.168.254.254',
                'numa_node': 1,
                'infiniband_devices': ['/dev/infiniband/foo']
            }])
        self.if_attr_bad_json = _make_text_attr(
            'katsdpcontroller.interfaces',
            base64.urlsafe_b64encode(b'{not valid json'))
        self.if_attr_bad_schema = _make_json_attr(
            'katsdpcontroller.interfaces',
            [{'name': 'eth1'}])
        self.volume_attr = _make_json_attr(
            'katsdpcontroller.volumes',
            [{'name': 'vol1', 'host_path': '/host1'},
             {'name': 'vol2', 'host_path': '/host2', 'numa_node': 1}])
        self.gpu_attr = _make_json_attr(
            'katsdpcontroller.gpus',
            [{'name': 'Dummy GPU', 'device_attributes': {},
              'compute_capability': (5, 2), 'numa_node': 1, 'uuid': 'GPU-123'},
             {'name': 'Dummy GPU', 'device_attributes': {},
              'compute_capability': (5, 2), 'numa_node': 0, 'uuid': 'GPU-456'}])
        self.numa_attr = _make_json_attr(
            'katsdpcontroller.numa', [[0, 2, 4, 6], [1, 3, 5, 7]])
        self.subsystem_attr = _make_json_attr(
            'katsdpcontroller.subsystems', ['subsystem1', 'subsystem2'])
        self.priority_attr = Dict()
        self.priority_attr.name = 'katsdpcontroller.priority'
        self.priority_attr.type = 'SCALAR'
        self.priority_attr.scalar.value = 8.5

    def test_construct(self):
        """Construct an agent from some offers"""
        attrs = [self.if_attr, self.volume_attr, self.gpu_attr, self.numa_attr, self.priority_attr]
        offers = [
            self._make_offer({'cpus': 4.0, 'mem': 1024.0,
                              'ports': [(100, 200), (300, 350)], 'cores': [(0, 7)]}, attrs),
            self._make_offer({'cpus': 0.5, 'mem': 123.5, 'disk': 1024.5,
                              'katsdpcontroller.gpu.0.compute': 0.25,
                              'katsdpcontroller.gpu.0.mem': 256.0,
                              'katsdpcontroller.interface.0.bandwidth_in': 1e9,
                              'katsdpcontroller.interface.0.bandwidth_out': 1e9,
                              'cores': [(7, 8)]}, attrs),
            self._make_offer({'katsdpcontroller.gpu.0.compute': 0.5,
                              'katsdpcontroller.gpu.0.mem': 1024.0,
                              'katsdpcontroller.gpu.1.compute': 0.125,
                              'katsdpcontroller.gpu.1.mem': 2048.0,
                              'katsdpcontroller.interface.0.bandwidth_in': 1e8,
                              'katsdpcontroller.interface.0.bandwidth_out': 2e8})
        ]
        agent = scheduler.Agent(offers)
        assert_equal(self.agent_id, agent.agent_id)
        assert_equal(self.host, agent.host)
        assert_equal(len(attrs), len(agent.attributes))
        for attr, agent_attr in zip(attrs, agent.attributes):
            assert_equal(attr, agent_attr)
        assert_equal(4.5, agent.resources['cpus'].available)
        assert_equal(1147.5, agent.resources['mem'].available)
        assert_equal(1024.5, agent.resources['disk'].available)
        assert_equal(2, len(agent.gpus))
        assert_equal(0.75, agent.gpus[0].resources['compute'].available)
        assert_equal(1280.0, agent.gpus[0].resources['mem'].available)
        assert_equal(0.125, agent.gpus[1].resources['compute'].available)
        assert_equal(2048.0, agent.gpus[1].resources['mem'].available)
        assert_equal([0, 2, 4, 6], list(agent.numa_cores[0]))
        assert_equal([1, 3, 5, 7], list(agent.numa_cores[1]))
        assert_equal(list(range(100, 200)) + list(range(300, 350)), list(agent.resources['ports']))
        assert_equal(1, len(agent.interfaces))
        assert_equal('eth0', agent.interfaces[0].name)
        assert_equal({'net0'}, agent.interfaces[0].networks)
        assert_equal(ipaddress.IPv4Address('192.168.254.254'), agent.interfaces[0].ipv4_address)
        assert_equal(1, agent.interfaces[0].numa_node)
        assert_equal(11e8, agent.interfaces[0].resources['bandwidth_in'].available)
        assert_equal(12e8, agent.interfaces[0].resources['bandwidth_out'].available)
        assert_equal(['/dev/infiniband/foo'], agent.interfaces[0].infiniband_devices)
        assert_equal(False, agent.interfaces[0].infiniband_multicast_loopback)
        assert_equal([scheduler.Volume(name='vol1', host_path='/host1', numa_node=None),
                      scheduler.Volume(name='vol2', host_path='/host2', numa_node=1)],
                     agent.volumes)
        assert_equal([[0, 2, 4, 6], [1, 3, 5, 7]], agent.numa)
        assert_equal(8.5, agent.priority)

    def test_construct_implicit_priority(self):
        """Test computation of priority when none is given"""
        attrs = [self.if_attr, self.volume_attr, self.gpu_attr, self.numa_attr]
        offers = [
            self._make_offer({'cpus': 0.5, 'mem': 123.5, 'disk': 1024.5}, attrs)
        ]
        agent = scheduler.Agent(offers)
        assert_equal(5, agent.priority)

    def test_no_offers(self):
        """ValueError is raised if zero offers are passed"""
        with pytest.raises(ValueError):
            scheduler.Agent([])

    def test_special_disk_resource(self):
        """A resource for a non-root disk is ignored"""
        offers = [self._make_offer({'disk': 1024})]
        # Example from https://mesos.apache.org/documentation/latest/multiple-disk/
        offers[0].resources[0].disk.source.type = 'PATH'
        offers[0].resources[0].disk.source.path.root = '/mnt/data'
        agent = scheduler.Agent(offers)
        assert_equal(0, agent.resources['disk'].available)

    def test_bad_json(self):
        """A warning must be printed if an interface description is not valid JSON"""
        offers = [self._make_offer({}, [self.if_attr_bad_json])]
        with self.assertLogs('katsdpcontroller.scheduler', logging.WARN):
            agent = scheduler.Agent(offers)
        assert_equal([], agent.interfaces)

    def test_bad_schema(self):
        """A warning must be printed if an interface description does not conform to the schema"""
        offers = [self._make_offer({}, [self.if_attr_bad_schema])]
        with self.assertLogs('katsdpcontroller.scheduler', logging.WARN):
            agent = scheduler.Agent(offers)
        assert_equal([], agent.interfaces)

    def test_allocate_not_valid(self):
        """allocate raises if the task does not accept the agent"""
        task = scheduler.LogicalTask('task')
        task.valid_agent = lambda x: False
        agent = scheduler.Agent([self._make_offer({}, [])])
        with pytest.raises(scheduler.InsufficientResourcesError):
            agent.allocate(task)

    def test_allocate_subsystem_mismatch(self):
        """allocate raises if the task is owned by a subsystem not valid on the agent"""
        task = scheduler.LogicalTask('task')
        task.subsystem = 'foo'
        agent = scheduler.Agent([self._make_offer({}, [self.subsystem_attr])])
        with pytest.raises(scheduler.InsufficientResourcesError):
            agent.allocate(task)

    def test_allocate_insufficient_scalar(self):
        """allocate raises if the task requires too much of a scalar resource"""
        task = scheduler.LogicalTask('task')
        task.cpus = 4.0
        agent = scheduler.Agent([self._make_offer({'cpus': 2.0}, [])])
        with pytest.raises(scheduler.InsufficientResourcesError):
            agent.allocate(task)

    def test_allocate_insufficient_range(self):
        """allocate raises if the task requires too much of a range resource"""
        task = scheduler.LogicalTask('task')
        task.cores = [None] * 3
        agent = scheduler.Agent([self._make_offer({'cores': [(4, 6)]}, [])])
        with pytest.raises(scheduler.InsufficientResourcesError):
            agent.allocate(task)

    def test_allocate_missing_interface(self):
        """allocate raises if the task requires a network that is not present"""
        task = scheduler.LogicalTask('task')
        task.interfaces = [scheduler.InterfaceRequest('net0'), scheduler.InterfaceRequest('net1')]
        agent = scheduler.Agent([self._make_offer({}, [self.if_attr])])
        with pytest.raises(scheduler.InsufficientResourcesError):
            agent.allocate(task)

    def test_allocate_missing_volume(self):
        """allocate raises if the task requires a volume that is not present"""
        task = scheduler.LogicalTask('task')
        task.volumes = [scheduler.VolumeRequest('vol-missing', '/container-path', 'RW')]
        agent = scheduler.Agent([self._make_offer({}, [self.volume_attr])])
        with pytest.raises(scheduler.InsufficientResourcesError):
            agent.allocate(task)

    def test_allocate_insufficient_gpu(self):
        """allocate raises if the task requires more GPU resources than available"""
        task = scheduler.LogicalTask('task')
        task.gpus.append(scheduler.GPURequest())
        task.gpus[-1].compute = 0.5
        task.gpus[-1].mem = 3000.0
        agent = scheduler.Agent([self._make_offer({
            'katsdpcontroller.gpu.0.compute': 1.0,
            'katsdpcontroller.gpu.0.mem': 2048.0,
            'katsdpcontroller.gpu.1.compute': 1.0,
            'katsdpcontroller.gpu.1.mem': 2048.0}, [self.gpu_attr])])
        with pytest.raises(scheduler.InsufficientResourcesError):
            agent.allocate(task)

    def test_allocate_insufficient_interface(self):
        """allocate raises if the task requires more interface resources than available"""
        task = scheduler.LogicalTask('task')
        task.interfaces.append(scheduler.InterfaceRequest('net0'))
        task.interfaces[-1].bandwidth_in = 1200e6
        agent = scheduler.Agent([self._make_offer({
            'katsdpcontroller.interface.0.bandwidth_in': 1000e6,
            'katsdpcontroller.interface.0.bandwidth_out': 2000e6}, [self.if_attr])])
        with pytest.raises(scheduler.InsufficientResourcesError):
            agent.allocate(task)

    def test_allocate_conflicting_multicast_out(self):
        """allocate raises if there is a multicast group conflict on the interface.

        This tests the case where the task is sending using ibverbs.
        """
        task = scheduler.LogicalTask('task')
        task.interfaces.append(scheduler.InterfaceRequest('net0', infiniband=True))
        task.interfaces[-1].multicast_out |= {'mc'}
        agent = scheduler.Agent([self._make_offer({
            'katsdpcontroller.interface.0.bandwidth_in': 1000e6,
            'katsdpcontroller.interface.0.bandwidth_out': 2000e6}, [self.if_attr])])
        agent.interfaces[0].multicast_in |= {'mc'}
        with pytest.raises(scheduler.InsufficientResourcesError):
            agent.allocate(task)

    def test_allocate_conflicting_multicast_in(self):
        """allocate raises if there is a multicast group conflict on the interface.

        This tests the case where the task is receiving.
        """
        task = scheduler.LogicalTask('task')
        task.interfaces.append(scheduler.InterfaceRequest('net0'))
        task.interfaces[-1].multicast_in |= {'mc'}
        agent = scheduler.Agent([self._make_offer({
            'katsdpcontroller.interface.0.bandwidth_in': 1000e6,
            'katsdpcontroller.interface.0.bandwidth_out': 2000e6}, [self.if_attr])])
        agent.interfaces[0].infiniband_multicast_out |= {'mc'}
        with pytest.raises(scheduler.InsufficientResourcesError):
            agent.allocate(task)

    def test_allocate_safe_multicast_loopback(self):
        """allocate does not raise on multicast conflict if loopback is available."""
        task = scheduler.LogicalTask('task')
        task.interfaces.append(scheduler.InterfaceRequest('net0', infiniband=True))
        task.interfaces[-1].multicast_out |= {'mc'}
        agent = scheduler.Agent([self._make_offer({
            'katsdpcontroller.interface.0.bandwidth_in': 1000e6,
            'katsdpcontroller.interface.0.bandwidth_out': 2000e6}, [self.if_attr_loopback])])
        agent.interfaces[0].multicast_in |= {'mc'}
        agent.allocate(task)
        assert_equal({'mc'}, agent.interfaces[0].infiniband_multicast_out)

    def test_allocate_safe_no_infiniband(self):
        """allocate does not raise on multicast conflict if infiniband is not used."""
        task = scheduler.LogicalTask('task')
        task.interfaces.append(scheduler.InterfaceRequest('net0'))
        task.interfaces[-1].multicast_out |= {'mc'}
        agent = scheduler.Agent([self._make_offer({
            'katsdpcontroller.interface.0.bandwidth_in': 1000e6,
            'katsdpcontroller.interface.0.bandwidth_out': 2000e6}, [self.if_attr])])
        agent.interfaces[0].multicast_in |= {'mc'}
        agent.allocate(task)
        assert_equal(set(), agent.interfaces[0].infiniband_multicast_out)

    def test_allocate_no_numa_cores(self):
        """allocate raises if no NUMA node has enough cores on its own"""
        task = scheduler.LogicalTask('task')
        task.cpus = 3.0
        task.mem = 128.0
        task.cores = ['a', 'b', None]
        agent = scheduler.Agent([
            self._make_offer({
                'cpus': 5.0, 'mem': 200.0, 'cores': [(4, 8)],
            }, [self.numa_attr])])
        with pytest.raises(scheduler.InsufficientResourcesError):
            agent.allocate(task)

    def test_allocate_no_numa_gpu(self):
        """allocate raises if no NUMA node has enough cores and GPUs together,
        and GPU affinity is requested"""
        task = scheduler.LogicalTask('task')
        task.cpus = 3.0
        task.mem = 128.0
        task.cores = ['a', 'b', None]
        task.gpus.append(scheduler.GPURequest())
        task.gpus[-1].compute = 0.5
        task.gpus[-1].mem = 1024.0
        task.gpus[-1].affinity = True
        agent = scheduler.Agent([self._make_offer({
            'cpus': 5.0, 'mem': 200.0, 'cores': [(0, 5)],
            'katsdpcontroller.gpu.0.compute': 1.0,
            'katsdpcontroller.gpu.0.mem': 2048.0,
            'katsdpcontroller.gpu.1.compute': 1.0,
            'katsdpcontroller.gpu.1.mem': 512.0}, [self.gpu_attr, self.numa_attr])])
        with pytest.raises(scheduler.InsufficientResourcesError):
            agent.allocate(task)

    def test_allocate_no_numa_interface(self):
        """allocate raises if no NUMA node has enough cores and interfaces together,
        and affinity is requested"""
        task = scheduler.LogicalTask('task')
        task.cpus = 3.0
        task.mem = 128.0
        task.cores = ['a', 'b', None]
        task.interfaces.append(scheduler.InterfaceRequest('net0', affinity=True))
        agent = scheduler.Agent([self._make_offer(
            {'cpus': 5.0, 'mem': 200.0, 'cores': [(0, 5)]},
            [self.if_attr, self.numa_attr])])
        with pytest.raises(scheduler.InsufficientResourcesError):
            agent.allocate(task)

    def test_allocate_no_infiniband_interface(self):
        """allocate raises if no interface is Infiniband-capable and Infiniband
        was requested"""
        task = scheduler.LogicalTask('task')
        task.cpus = 1.0
        task.mem = 128.0
        task.interfaces.append(scheduler.InterfaceRequest('net0', infiniband=True))
        if_attr = _make_json_attr(
            'katsdpcontroller.interfaces',
            [{'name': 'eth0', 'network': 'net0', 'ipv4_address': '192.168.254.254',
              'numa_node': 1, 'infiniband_devices': []}])
        agent = scheduler.Agent([self._make_offer(
            {'cpus': 5.0, 'mem': 200.0, 'cores': [(0, 5)]},
            [if_attr])])
        with pytest.raises(scheduler.InsufficientResourcesError):
            agent.allocate(task)

    def test_allocate_no_numa_volume(self):
        """allocate raises if no NUMA node has enough cores and volumes together,
        and affinity is requested"""
        task = scheduler.LogicalTask('task')
        task.cpus = 3.0
        task.mem = 128.0
        task.cores = ['a', 'b', None]
        task.volumes.append(scheduler.VolumeRequest('vol2', '/container-path', 'RW', affinity=True))
        agent = scheduler.Agent([self._make_offer(
            {'cpus': 5.0, 'mem': 200.0, 'cores': [(0, 5)]},
            [self.volume_attr, self.numa_attr])])
        with pytest.raises(scheduler.InsufficientResourcesError):
            agent.allocate(task)

    def test_allocate_success(self):
        """Tests allocate in the success case"""
        task = scheduler.LogicalTask('task')
        task.cpus = 4.0
        task.mem = 128.0
        task.cores = ['a', 'b', 'c']
        task.interfaces = [scheduler.InterfaceRequest('net0')]
        task.interfaces[0].bandwidth_in = 1000e6
        task.interfaces[0].bandwidth_out = 500e6
        task.volumes = [scheduler.VolumeRequest('vol2', '/container-path', 'RW')]
        task.gpus = [scheduler.GPURequest(), scheduler.GPURequest()]
        task.gpus[0].compute = 0.5
        task.gpus[0].mem = 1024.0
        task.gpus[1].compute = 0.5
        task.gpus[1].mem = 256.0
        task.gpus[1].affinity = True
        agent = scheduler.Agent([
            self._make_offer({
                'cpus': 4.0, 'mem': 200.0, 'cores': [(3, 8)],
                'katsdpcontroller.gpu.1.compute': 0.75,
                'katsdpcontroller.gpu.1.mem': 2048.0,
                'katsdpcontroller.gpu.0.compute': 0.75,
                'katsdpcontroller.gpu.0.mem': 256.0,
                'katsdpcontroller.interface.0.bandwidth_in': 2000e6,
                'katsdpcontroller.interface.0.bandwidth_out': 2100e6
            }, [self.if_attr, self.volume_attr, self.gpu_attr, self.numa_attr])])
        ra = agent.allocate(task)
        assert_equal(4.0, ra.resources['cpus'].available)
        assert_equal(128.0, ra.resources['mem'].available)
        assert_equal(1, len(ra.interfaces))
        assert_equal(1000e6, ra.interfaces[0].resources['bandwidth_in'].available)
        assert_equal(500e6, ra.interfaces[0].resources['bandwidth_out'].available)
        assert_equal(0, ra.interfaces[0].index)
        assert_equal([scheduler.Volume(name='vol2', host_path='/host2', numa_node=1)],
                     ra.volumes)
        assert_equal(2, len(ra.gpus))
        assert_equal(0.5, ra.gpus[0].resources['compute'].available)
        assert_equal(1024.0, ra.gpus[0].resources['mem'].available)
        assert_equal(1, ra.gpus[0].index)
        assert_equal(0.5, ra.gpus[1].resources['compute'].available)
        assert_equal(256.0, ra.gpus[1].resources['mem'].available)
        assert_equal(0, ra.gpus[1].index)
        assert_equal([3, 5, 7], list(ra.resources['cores']))
        # Check that the resources were subtracted
        assert_equal(0.0, agent.resources['cpus'].available)
        assert_equal(72.0, agent.resources['mem'].available)
        assert_equal([4, 6], list(agent.numa_cores[0]))
        assert_equal([], list(agent.numa_cores[1]))
        assert_equal(1000e6, agent.interfaces[0].resources['bandwidth_in'].available)
        assert_equal(1600e6, agent.interfaces[0].resources['bandwidth_out'].available)
        assert_equal(0.25, agent.gpus[0].resources['compute'].available)
        assert_equal(0.0, agent.gpus[0].resources['mem'].available)
        assert_equal(0.25, agent.gpus[1].resources['compute'].available)
        assert_equal(1024.0, agent.gpus[1].resources['mem'].available)


class TestPhysicalTask:
    """Tests for :class:`katsdpcontroller.scheduler.PhysicalTask`"""

    def setup(self):
        self.logical_task = scheduler.LogicalTask('task')
        self.logical_task.cpus = 4.0
        self.logical_task.mem = 256.0
        self.logical_task.ports = ['port1', 'port2']
        self.logical_task.cores = ['core1', 'core2', 'core3']
        self.logical_task.interfaces = [
            scheduler.InterfaceRequest('net1'),
            scheduler.InterfaceRequest('net0')]
        self.logical_task.volumes = [scheduler.VolumeRequest('vol0', '/container-path', 'RW')]
        self.eth0 = scheduler.InterfaceResources(0)
        self.eth0.bandwidth_in = 500e6
        self.eth0.bandwidth_out = 600e6
        self.eth1 = scheduler.InterfaceResources(1)
        self.eth1.bandwidth_in = 300e6
        self.eth1.bandwidth_out = 200e6
        self.vol0 = scheduler.Volume('vol0', '/host0', numa_node=1)
        attributes = [
            _make_json_attr('katsdpcontroller.interfaces', [
                {"name": "eth0", "network": "net0", "ipv4_address": "192.168.1.1"},
                {"name": "eth1", "network": "net1", "ipv4_address": "192.168.2.1"}
            ]),
            _make_json_attr('katsdpcontroller.volumes',
                            [{"name": "vol0", "host_path": "/host0", "numa_node": 1}]),
            _make_json_attr('katsdpcontroller.numa', [[0, 2, 4, 6], [1, 3, 5, 7]])
        ]
        offers = [_make_offer('framework', 'agentid', 'agenthost',
                              {'cpus': 8.0, 'mem': 256.0,
                               'ports': [(30000, 31000)], 'cores': [(1, 8)],
                               'katsdpcontroller.interface.0.bandwidth_in': 1000e6,
                               'katsdpcontroller.interface.0.bandwidth_out': 1000e6,
                               'katsdpcontroller.interface.1.bandwidth_in': 1000e6,
                               'katsdpcontroller.interface.1.bandwidth_out': 1000e6},
                              attributes)]
        agent = scheduler.Agent(offers)
        self.allocation = agent.allocate(self.logical_task)

    def test_properties_init(self):
        """Resolved properties are ``None`` on construction"""
        physical_task = scheduler.PhysicalTask(self.logical_task)
        assert_is_none(physical_task.agent)
        assert_is_none(physical_task.host)
        assert_is_none(physical_task.agent_id)
        assert_is_none(physical_task.taskinfo)
        assert_is_none(physical_task.allocation)
        assert_equal({}, physical_task.interfaces)
        assert_equal({}, physical_task.endpoints)
        assert_equal({}, physical_task.ports)
        assert_equal({}, physical_task.cores)

    def test_allocate(self):
        physical_task = scheduler.PhysicalTask(self.logical_task)
        physical_task.allocate(self.allocation)
        assert_is(self.allocation.agent, physical_task.agent)
        assert_equal('agenthost', physical_task.host)
        assert_equal('agentid', physical_task.agent_id)
        assert_is(self.allocation, physical_task.allocation)
        assert_in('net0', physical_task.interfaces)
        assert_equal('eth0', physical_task.interfaces['net0'].name)
        assert_equal(ipaddress.IPv4Address('192.168.1.1'),
                     physical_task.interfaces['net0'].ipv4_address)
        assert_in('net1', physical_task.interfaces)
        assert_equal('eth1', physical_task.interfaces['net1'].name)
        assert_equal(ipaddress.IPv4Address('192.168.2.1'),
                     physical_task.interfaces['net1'].ipv4_address)
        assert_equal({}, physical_task.endpoints)
        assert_equal({'port1': mock.ANY, 'port2': mock.ANY}, physical_task.ports)
        assert_in(physical_task.ports['port1'], range(30000, 31001))
        assert_in(physical_task.ports['port2'], range(30000, 31001))
        assert_not_equal(physical_task.ports['port1'], physical_task.ports['port2'])
        assert_equal({'core1': 2, 'core2': 4, 'core3': 6}, physical_task.cores)


class TestDiagnoseInsufficient(unittest.TestCase):
    """Test :class:`katsdpcontroller.scheduler.Scheduler._diagnose_insufficient.

    This is split out from TestScheduler to make it easier to set up fixtures.
    """
    def _make_offer(self, resources, agent_num=0, attrs=()):
        return _make_offer('frameworkid', 'agentid{}'.format(agent_num),
                           'agenthost{}'.format(agent_num), resources, attrs)

    def setUp(self):
        # Create a number of agents, each of which has a large quantity of
        # some resource but not much of others. This makes it easier to
        # control which resources are plentiful in the simulated cluster.
        numa_attr = _make_json_attr('katsdpcontroller.numa', [[0, 2, 4, 6], [1, 3, 5, 7]])
        gpu_attr = _make_json_attr(
            'katsdpcontroller.gpus',
            [{'name': 'Dummy GPU', 'device_attributes': {},
              'compute_capability': (5, 2), 'numa_node': 1, 'uuid': 'GPU-123'}])
        interface_attr = _make_json_attr(
            'katsdpcontroller.interfaces',
            [{'name': 'eth0', 'network': 'net0', 'ipv4_address': '192.168.1.1',
              'infiniband_devices': ['/dev/infiniband/rdma_cm', '/dev/infiniband/uverbs0']},
             {'name': 'eth1', 'network': 'net1', 'ipv4_address': '192.168.1.2'}])
        volume_attr = _make_json_attr(
            'katsdpcontroller.volumes',
            [{'name': 'vol0', 'host_path': '/host0'}])

        self.cpus_agent = scheduler.Agent([self._make_offer(
            {'cpus': 32, 'mem': 2, 'disk': 7}, 0)])
        self.mem_agent = scheduler.Agent([self._make_offer(
            {'cpus': 1.25, 'mem': 256, 'disk': 8}, 1)])
        self.disk_agent = scheduler.Agent([self._make_offer(
            {'cpus': 1.5, 'mem': 3, 'disk': 1024}, 2)])
        self.ports_agent = scheduler.Agent([self._make_offer(
            {'cpus': 1.75, 'mem': 4, 'disk': 9, 'ports': [(30000, 30005)]}, 3)])
        self.cores_agent = scheduler.Agent([self._make_offer(
            {'cpus': 6, 'mem': 5, 'disk': 10, 'cores': [(0, 6)]}, 4, [numa_attr])])
        self.gpu_compute_agent = scheduler.Agent([self._make_offer(
            {'cpus': 0.75, 'mem': 6, 'disk': 11,
             'katsdpcontroller.gpu.0.compute': 1.0,
             'katsdpcontroller.gpu.0.mem': 2.25}, 5,
            [numa_attr, gpu_attr])])
        self.gpu_mem_agent = scheduler.Agent([self._make_offer(
            {'cpus': 1.0, 'mem': 7, 'disk': 11,
             'katsdpcontroller.gpu.0.compute': 0.125,
             'katsdpcontroller.gpu.0.mem': 256.0}, 6,
            [numa_attr, gpu_attr])])
        self.interface_agent = scheduler.Agent([self._make_offer(
            {'cpus': 1.0, 'mem': 1, 'disk': 1,
             'katsdpcontroller.interface.0.bandwidth_in': 1e9,
             'katsdpcontroller.interface.0.bandwidth_out': 1e9,
             'katsdpcontroller.interface.1.bandwidth_in': 1e9,
             'katsdpcontroller.interface.1.bandwidth_out': 1e9}, 7,
            [interface_attr])])
        self.volume_agent = scheduler.Agent([self._make_offer(
            {'cpus': 1.0, 'mem': 1, 'disk': 1}, 8,
            [volume_attr])])
        # Create a template logical and physical task
        self.logical_task = scheduler.LogicalTask('logical')
        self.physical_task = self.logical_task.physical_factory(self.logical_task)
        self.logical_task2 = scheduler.LogicalTask('logical2')
        self.physical_task2 = self.logical_task2.physical_factory(self.logical_task2)

    def test_task_insufficient_scalar_resource(self):
        """A task requests more of a scalar resource than any agent has"""
        self.logical_task.cpus = 4
        with pytest.raises(scheduler.TaskInsufficientResourcesError) as cm:
            scheduler.Scheduler._diagnose_insufficient(
                [self.mem_agent, self.disk_agent], [self.physical_task])
        assert cm.value.node is self.physical_task
        assert cm.value.resource == 'cpus'
        assert cm.value.needed == 4
        assert cm.value.available == 1.5

    def test_task_insufficient_range_resource(self):
        """A task requests more of a range resource than any agent has"""
        self.logical_task.ports = ['a', 'b', 'c']
        with pytest.raises(scheduler.TaskInsufficientResourcesError) as cm:
            scheduler.Scheduler._diagnose_insufficient(
                [self.mem_agent, self.disk_agent], [self.physical_task])
        assert cm.value.node is self.physical_task
        assert cm.value.resource == 'ports'
        assert cm.value.needed == 3
        assert cm.value.available == 0

    def test_task_insufficient_cores(self):
        """A task requests more cores than are available on a single NUMA node"""
        self.logical_task.cores = ['a', 'b', 'c', 'd']
        with pytest.raises(scheduler.TaskInsufficientResourcesError) as cm:
            scheduler.Scheduler._diagnose_insufficient(
                [self.mem_agent, self.cores_agent], [self.physical_task])
        assert cm.value.node is self.physical_task
        assert cm.value.resource == 'cores'
        assert cm.value.needed == 4
        assert cm.value.available == 3

    def test_task_insufficient_gpu_scalar_resource(self):
        """A task requests more of a GPU scalar resource than any agent has"""
        req = scheduler.GPURequest()
        req.mem = 2048
        self.logical_task.gpus = [req]
        with pytest.raises(scheduler.TaskInsufficientGPUResourcesError) as cm:
            scheduler.Scheduler._diagnose_insufficient(
                [self.mem_agent, self.gpu_compute_agent], [self.physical_task])
        assert cm.value.node is self.physical_task
        assert cm.value.request_index == 0
        assert cm.value.resource == 'mem'
        assert cm.value.needed == 2048
        assert cm.value.available == 2.25

    def test_task_insufficient_interface_scalar_resources(self):
        """A task requests more of an interface scalar resource than any agent has"""
        req = scheduler.InterfaceRequest('net0')
        req.bandwidth_in = 5e9
        self.logical_task.interfaces = [req]
        with pytest.raises(scheduler.TaskInsufficientInterfaceResourcesError) as cm:
            scheduler.Scheduler._diagnose_insufficient(
                [self.mem_agent, self.interface_agent], [self.physical_task])
        assert cm.value.node is self.physical_task
        assert cm.value.request == req
        assert cm.value.resource == 'bandwidth_in'
        assert cm.value.needed == 5e9
        assert cm.value.available == 1e9

    def test_task_no_interface(self):
        """A task requests a network interface that is not available on any agent"""
        self.logical_task.interfaces = [
            scheduler.InterfaceRequest('net0'),
            scheduler.InterfaceRequest('badnet')
        ]
        with pytest.raises(scheduler.TaskNoInterfaceError) as cm:
            scheduler.Scheduler._diagnose_insufficient(
                [self.mem_agent, self.interface_agent], [self.physical_task])
        assert cm.value.node is self.physical_task
        assert cm.value.request is self.logical_task.interfaces[1]

    def test_task_no_volume(self):
        """A task requests a volume that is not available on any agent"""
        self.logical_task.volumes = [
            scheduler.VolumeRequest('vol0', '/vol0', 'RW'),
            scheduler.VolumeRequest('badvol', '/badvol', 'RO')
        ]
        with pytest.raises(scheduler.TaskNoVolumeError) as cm:
            scheduler.Scheduler._diagnose_insufficient(
                [self.mem_agent, self.volume_agent], [self.physical_task])
        assert cm.value.node is self.physical_task
        assert cm.value.request is self.logical_task.volumes[1]

    def test_task_no_gpu(self):
        """A task requests a GPU that is not available on any agent"""
        req = scheduler.GPURequest()
        req.name = 'GPU that does not exist'
        self.logical_task.gpus = [req]
        with pytest.raises(scheduler.TaskNoGPUError) as cm:
            scheduler.Scheduler._diagnose_insufficient(
                [self.mem_agent, self.gpu_compute_agent], [self.physical_task])
        assert cm.value.node == self.physical_task
        assert cm.value.request_index == 0

    def test_task_no_agent(self):
        """A task does not fit on any agent, but not due to a single reason"""
        # Ask for more combined cpu+ports than is available on one agent
        self.logical_task.cpus = 8
        self.logical_task.ports = ['a', 'b', 'c']
        with pytest.raises(scheduler.TaskNoAgentError) as cm:
            scheduler.Scheduler._diagnose_insufficient(
                [self.cpus_agent, self.ports_agent], [self.physical_task])
        assert cm.value.node is self.physical_task
        # Make sure that it didn't incorrectly return a subclass
        assert cm.type == scheduler.TaskNoAgentError

    def test_group_insufficient_scalar_resource(self):
        """A group of tasks require more of a scalar resource than available"""
        self.logical_task.cpus = 24
        self.logical_task2.cpus = 16
        with pytest.raises(scheduler.GroupInsufficientResourcesError) as cm:
            scheduler.Scheduler._diagnose_insufficient(
                [self.cpus_agent, self.mem_agent], [self.physical_task, self.physical_task2])
        assert cm.value.resource == 'cpus'
        assert cm.value.needed == 40
        assert cm.value.available == 33.25

    def test_group_insufficient_range_resource(self):
        """A group of tasks require more of a range resource than available"""
        self.logical_task.ports = ['a', 'b', 'c']
        self.logical_task2.ports = ['d', 'e', 'f']
        with pytest.raises(scheduler.GroupInsufficientResourcesError) as cm:
            scheduler.Scheduler._diagnose_insufficient(
                [self.ports_agent], [self.physical_task, self.physical_task2])
        assert cm.value.resource == 'ports'
        assert cm.value.needed == 6
        assert cm.value.available == 5

    def test_group_insufficient_gpu_scalar_resources(self):
        """A group of tasks require more of a GPU scalar resource than available"""
        self.logical_task.gpus = [scheduler.GPURequest()]
        self.logical_task.gpus[-1].compute = 0.75
        self.logical_task2.gpus = [scheduler.GPURequest()]
        self.logical_task2.gpus[-1].compute = 0.5
        with pytest.raises(scheduler.GroupInsufficientGPUResourcesError) as cm:
            scheduler.Scheduler._diagnose_insufficient(
                [self.gpu_compute_agent, self.gpu_mem_agent],
                [self.physical_task, self.physical_task2])
        assert cm.value.resource == 'compute'
        assert cm.value.needed == 1.25
        assert cm.value.available == 1.125

    def test_group_insufficient_interface_scalar_resources(self):
        """A group of tasks require more of a network resource than available"""
        self.logical_task.interfaces = [
            scheduler.InterfaceRequest('net0'),
            scheduler.InterfaceRequest('net1')
        ]
        self.logical_task.interfaces[0].bandwidth_in = 800e6
        # An amount that must not be added to the needed value reported
        self.logical_task.interfaces[1].bandwidth_in = 50e6
        self.logical_task2.interfaces = [scheduler.InterfaceRequest('net0')]
        self.logical_task2.interfaces[0].bandwidth_in = 700e6
        with pytest.raises(scheduler.GroupInsufficientInterfaceResourcesError) as cm:
            scheduler.Scheduler._diagnose_insufficient(
                [self.interface_agent],
                [self.physical_task, self.physical_task2])
        assert cm.value.network == 'net0'
        assert cm.value.resource == 'bandwidth_in'
        assert cm.value.needed == 1500e6
        assert cm.value.available == 1000e6

    def test_subsystem(self):
        """A subsystem has insufficient resources, although the system has enough."""
        self.logical_task.subsystem = 'sdp'
        self.logical_task.cpus = 32  # Consumes all of cpus_agent
        self.logical_task2.subsystem = 'sdp'
        self.logical_task2.cpus = 1
        self.cpus_agent.subsystems = {'sdp'}
        self.cores_agent.subsystems = {'cbf', 'other'}
        with pytest.raises(scheduler.GroupInsufficientResourcesError) as cm:
            scheduler.Scheduler._diagnose_insufficient(
                [self.cpus_agent, self.cores_agent], [self.physical_task, self.physical_task2])
        assert cm.value.resource == 'cpus'
        assert cm.value.needed == 33
        assert cm.value.available == 32
        assert cm.value.subsystem == 'sdp'

    def test_generic(self):
        """A group of tasks can't fit, but no simpler explanation is available"""
        # Create a task that uses just too much memory for the
        # low-memory agents, forcing them to consume memory from the
        # big-memory agent and not leaving enough for the big-memory task.
        self.logical_task.mem = 5
        self.logical_task2.mem = 251
        with pytest.raises(scheduler.InsufficientResourcesError) as cm:
            scheduler.Scheduler._diagnose_insufficient(
                [self.cpus_agent, self.mem_agent, self.disk_agent],
                [self.physical_task, self.physical_task2])
        # Check that it wasn't a subclass raised
        assert cm.type == scheduler.InsufficientResourcesError


class TestSubgraph:
    """Tests for :func:`katsdpcontroller.scheduler.subgraph`"""
    def setup(self):
        g = networkx.MultiDiGraph()
        g.add_nodes_from(['a', 'b', 'c'])
        g.add_edge('a', 'b', key1=True, key2=False)
        g.add_edge('a', 'c')
        g.add_edge('b', 'c', key1=False)
        g.add_edge('c', 'a', key2=123)
        g.add_edge('c', 'b', key1=True, key2=True)
        self.g = g

    def test_simple(self):
        """Test with string filter and all nodes"""
        out = scheduler.subgraph(self.g, 'key2')
        assert_equal({('c', 'a'), ('c', 'b')}, set(out.edges()))
        assert_equal({'a', 'b', 'c'}, set(out.nodes()))

    def test_restrict_nodes(self):
        out = scheduler.subgraph(self.g, 'key2', {'a', 'c'})
        assert_equal({('c', 'a')}, set(out.edges()))
        assert_equal({'a', 'c'}, set(out.nodes()))

    def test_callable_filter(self):
        out = scheduler.subgraph(self.g, lambda data: 'key1' in data)
        assert_equal({('a', 'b'), ('b', 'c'), ('c', 'b')}, set(out.edges()))
        assert_equal({'a', 'b', 'c'}, set(out.nodes()))


class TestScheduler(asynctest.ClockedTestCase):
    """Tests for :class:`katsdpcontroller.scheduler.Scheduler`."""
    def _make_offer(self, resources, agent_num=0, attrs=()):
        return _make_offer(self.framework_id, 'agentid{}'.format(agent_num),
                           'agenthost{}'.format(agent_num), resources, attrs)

    def _make_offers(self, ports=None):
        if ports is None:
            ports = [(30000, 31000)]
        return [
            # Suitable for node0
            self._make_offer({
                'cpus': 2.0, 'mem': 1024.0, 'ports': ports,
                'katsdpcontroller.gpu.0.compute': 0.25,
                'katsdpcontroller.gpu.0.mem': 2048.0,
                'katsdpcontroller.gpu.1.compute': 1.0,
                'katsdpcontroller.gpu.1.mem': 1024.0,
                'katsdpcontroller.interface.0.bandwidth_in': 1e9,
                'katsdpcontroller.interface.0.bandwidth_out': 1e9
            }, 0, self.agent0_attrs),
            # Suitable for node1
            self._make_offer({
                'cpus': 0.5, 'mem': 128.0, 'ports': [(31000, 32000)],
                'cores': [(0, 8)]
            }, 1, [self.numa_attr])
        ]

    def _status_update(self, task_id, state):
        status = _make_status(task_id, state)
        self.sched.statusUpdate(self.driver, status)
        return status

    def _make_physical(self):
        self.physical_graph = scheduler.instantiate(self.logical_graph)
        self.physical_batch_graph = scheduler.instantiate(self.logical_batch_graph)
        self.nodes = []
        for i in range(3):
            self.nodes.append(next(node for node in self.physical_graph
                                   if node.name == 'node{}'.format(i)))
        self.nodes[2].host = 'remotehost'
        self.nodes[2].ports['foo'] = 10000
        self.batch_nodes = []
        for i in range(2):
            self.batch_nodes.append(next(node for node in self.physical_batch_graph
                                         if node.name == 'batch{}'.format(i)))

    async def _wait_request(self, task_id):
        async with aiohttp.ClientSession() as session:
            async with session.get('http://127.0.0.1:{}/tasks/{}/wait_start'.format(
                    self.sched.http_port, task_id)) as resp:
                resp.raise_for_status()
                await resp.read()

    @staticmethod
    def _dummy_random():
        return 0.0

    @staticmethod
    def _dummy_randint(a, b):
        return a

    def _driver_calls(self):
        """self.driver.mock_calls, with reconcileTasks filtered out."""
        return [call for call in self.driver.mock_calls if call[0] != 'reconcileTasks']

    async def setUp(self):
        self.framework_id = 'frameworkid'
        # Normally TaskIDAllocator's constructor returns a singleton to keep
        # IDs globally unique, but we want the test to be isolated. Bypass its
        # __new__.
        self.image_resolver = scheduler.ImageResolver(scheduler.SimpleImageLookup('sdp'))
        self.task_id_allocator = object.__new__(scheduler.TaskIDAllocator)
        self.task_id_allocator._prefix = 'test-'
        self.task_id_allocator._next_id = 0
        node0 = scheduler.LogicalTask('node0')
        node0.cpus = 1.0
        node0.command = ['hello', '--port={ports[port]}']
        node0.ports = ['port']
        node0.image = 'image0'
        node0.gpus.append(scheduler.GPURequest())
        node0.gpus[-1].compute = 0.5
        node0.gpus[-1].mem = 256.0
        node0.interfaces = [
            scheduler.InterfaceRequest('net0', infiniband=True, multicast_out={'mc'})
        ]
        node0.interfaces[-1].bandwidth_in = 500e6
        node0.interfaces[-1].bandwidth_out = 200e6
        node0.volumes = [scheduler.VolumeRequest('vol0', '/container-path', 'RW')]
        node1 = scheduler.LogicalTask('node1')
        node1.cpus = 0.5
        node1.command = ['test', '--host={host}', '--remote={endpoints[node0_port]}',
                         '--another={endpoints[node2_foo]}']
        node1.image = 'image1'
        node1.cores = ['core0', 'core1']
        node2 = scheduler.LogicalExternal('node2')
        node2.wait_ports = []
        batch_nodes = []
        for i in range(2):
            batch_node = scheduler.LogicalTask('batch{}'.format(i))
            batch_node.image = 'batch_image'
            batch_node.cpus = 0.5
            batch_node.mem = 256
            batch_node.max_run_time = 10
            batch_node.command = ['/bin/echo', 'Hello']
            batch_nodes.append(batch_node)
        # The order is determined by the lexicographical_topological_sort done in _launch_group
        self.task_ids = ['test-00000000', 'test-00000001', None]
        self.logical_graph = networkx.MultiDiGraph()
        self.logical_graph.add_nodes_from([node0, node1, node2])
        self.logical_graph.add_edge(node1, node0, port='port',
                                    depends_ready=True, depends_kill=True)
        self.logical_graph.add_edge(node1, node2, port='foo', depends_ready=True, depends_kill=True)
        self.logical_batch_graph = networkx.MultiDiGraph()
        self.logical_batch_graph.add_node(batch_nodes[0])
        self.logical_batch_graph.add_node(batch_nodes[1])
        self.logical_batch_graph.add_edge(batch_nodes[1], batch_nodes[0], depends_finished=True)
        self.numa_attr = _make_json_attr('katsdpcontroller.numa', [[0, 2, 4, 6], [1, 3, 5, 7]])
        self.agent0_attrs = [
            _make_json_attr('katsdpcontroller.gpus', [
                {'uuid': 'GPU-123',
                 'name': 'Dummy GPU', 'device_attributes': {}, 'compute_capability': (5, 2)},
                {'uuid': 'GPU-456',
                 'name': 'Dummy GPU', 'device_attributes': {}, 'compute_capability': (5, 2)}
            ]),
            _make_json_attr('katsdpcontroller.volumes', [
                {'name': 'vol0', 'host_path': '/host0'}
            ]),
            _make_json_attr('katsdpcontroller.interfaces', [
                {'name': 'eth0', 'network': 'net0', 'ipv4_address': '192.168.1.1',
                 'infiniband_devices': ['/dev/infiniband/rdma_cm', '/dev/infiniband/uverbs0'],
                 'infiniband_multicast_loopback': False}]),
            self.numa_attr
        ]
        self._make_physical()
        self.task_stats = SimpleTaskStats()
        self.sched = scheduler.Scheduler('default', '127.0.0.1', 0, 'http://scheduler/',
                                         task_stats=self.task_stats)
        self.resolver = scheduler.Resolver(self.image_resolver, self.task_id_allocator,
                                           self.sched.http_url)
        self.driver = mock.create_autospec(pymesos.MesosSchedulerDriver,
                                           spec_set=True, instance=True)
        self.sched.set_driver(self.driver)
        self.sched.registered(self.driver, 'framework', mock.sentinel.master_info)
        # Mock out the random generator so that port allocations will be
        # predictable.
        create_patch(self, 'katsdpcontroller.scheduler.Agent._random.random', self._dummy_random)
        create_patch(self, 'katsdpcontroller.scheduler.Agent._random.randint', self._dummy_randint)
        await self.sched.start()

    async def test_initial_offers(self):
        """Offers passed to resourcesOffers in initial state are declined"""
        offers = [
            self._make_offer({'cpus': 2.0}, 0),
            self._make_offer({'cpus': 1.0}, 1),
            self._make_offer({'cpus': 1.5}, 0)
        ]
        self.sched.resourceOffers(self.driver, offers)
        await asynctest.exhaust_callbacks(self.loop)
        assert_equal(
            AnyOrderList([
                mock.call.declineOffer(AnyOrderList([offers[0].id, offers[1].id, offers[2].id])),
                mock.call.suppressOffers({'default'})
            ]), self._driver_calls())

    async def test_launch_cycle(self):
        """Launch raises CycleError if there is a cycle of depends_ready edges"""
        nodes = [scheduler.LogicalExternal('node{}'.format(i)) for i in range(4)]
        logical_graph = networkx.MultiDiGraph()
        logical_graph.add_nodes_from(nodes)
        logical_graph.add_edge(nodes[1], nodes[0], depends_ready=True)
        logical_graph.add_edge(nodes[1], nodes[2], depends_ready=True)
        logical_graph.add_edge(nodes[2], nodes[3], depends_ready=True)
        logical_graph.add_edge(nodes[3], nodes[1], depends_ready=True)
        physical_graph = scheduler.instantiate(logical_graph)
        with pytest.raises(scheduler.CycleError):
            await self.sched.launch(physical_graph, self.resolver)

    async def test_launch_omit_dependency(self):
        """Launch raises DependencyError if launching a subset of nodes that
        depends on a node that is outside the set and not running.
        """
        nodes = [scheduler.LogicalExternal('node{}'.format(i)) for i in range(2)]
        logical_graph = networkx.MultiDiGraph()
        logical_graph.add_nodes_from(nodes)
        logical_graph.add_edge(nodes[1], nodes[0], depends_resources=True)
        physical_graph = scheduler.instantiate(logical_graph)
        target = [node for node in physical_graph if node.name == 'node1']
        with pytest.raises(scheduler.DependencyError):
            await self.sched.launch(physical_graph, self.resolver, target)

    async def test_add_queue_twice(self):
        queue = scheduler.LaunchQueue('default')
        self.sched.add_queue(queue)
        with pytest.raises(ValueError):
            self.sched.add_queue(queue)

    async def test_remove_nonexistent_queue(self):
        with pytest.raises(ValueError):
            self.sched.remove_queue(scheduler.LaunchQueue('default'))

    async def test_launch_bad_queue(self):
        """Launch raises ValueError if queue has been added"""
        queue = scheduler.LaunchQueue('default')
        with pytest.raises(ValueError):
            await self.sched.launch(self.physical_graph, self.resolver, queue=queue)

    async def test_launch_closing(self):
        """Launch raises asyncio.InvalidStateError if close has been called"""
        await self.sched.close()
        with pytest.raises(asyncio.InvalidStateError):
            # Timeout is just to ensure the test won't hang
            await asyncio.wait_for(self.sched.launch(self.physical_graph, self.resolver),
                                   timeout=1)

    async def test_launch_serial(self):
        """Test launch on the success path, with no concurrent calls."""
        # TODO: still need to extend this to test:
        # - custom wait_ports
        offer0, offer1 = self._make_offers()
        expected_taskinfo0 = Dict()
        expected_taskinfo0.name = 'node0'
        expected_taskinfo0.task_id.value = self.task_ids[0]
        expected_taskinfo0.agent_id.value = 'agentid0'
        expected_taskinfo0.command.shell = False
        expected_taskinfo0.command.value = 'hello'
        expected_taskinfo0.command.arguments = ['--port=30000']
        expected_taskinfo0.command.environment.variables = [
            Dict(name='NVIDIA_VISIBLE_DEVICES', value='GPU-456')]
        expected_taskinfo0.container.type = 'DOCKER'
        expected_taskinfo0.container.docker.image = 'sdp/image0:latest'
        expected_taskinfo0.container.docker.parameters = AnyOrderList([
            Dict(key='ulimit', value='memlock=-1'),
            Dict(key='device', value='/dev/infiniband/rdma_cm'),
            Dict(key='device', value='/dev/infiniband/uverbs0'),
            Dict(key='runtime', value='nvidia')
        ])
        volume = Dict()
        volume.mode = 'RW'
        volume.host_path = '/host0'
        volume.container_path = '/container-path'
        expected_taskinfo0.container.volumes = [volume]
        expected_taskinfo0.resources = _make_resources({
            'cpus': 1.0, 'ports': [(30000, 30001)],
            'katsdpcontroller.gpu.1.compute': 0.5,
            'katsdpcontroller.gpu.1.mem': 256.0,
            'katsdpcontroller.interface.0.bandwidth_in': 500e6,
            'katsdpcontroller.interface.0.bandwidth_out': 200e6
        })
        expected_taskinfo0.discovery.visibility = 'EXTERNAL'
        expected_taskinfo0.discovery.name = 'node0'
        expected_taskinfo0.discovery.ports.ports = [Dict(number=30000, name='port', protocol='tcp')]
        expected_taskinfo1 = Dict()
        expected_taskinfo1.name = 'node1'
        expected_taskinfo1.task_id.value = self.task_ids[1]
        expected_taskinfo1.agent_id.value = 'agentid1'
        expected_taskinfo1.command.shell = False
        uri = Dict()
        uri.executable = True
        uri.value = 'http://scheduler/static/delay_run.sh'
        expected_taskinfo1.command.uris = [uri]
        expected_taskinfo1.command.value = '/mnt/mesos/sandbox/delay_run.sh'
        expected_taskinfo1.command.arguments = [
            'http://scheduler/tasks/{}/wait_start'.format(self.task_ids[1]),
            'test', '--host=agenthost1', '--remote=agenthost0:30000',
            '--another=remotehost:10000']
        expected_taskinfo1.container.type = 'DOCKER'
        expected_taskinfo1.container.docker.image = 'sdp/image1:latest'
        expected_taskinfo1.container.docker.parameters = [{'key': 'cpuset-cpus', 'value': '0,2'}]
        expected_taskinfo1.resources = _make_resources(
            {'cpus': 0.5, 'cores': [(0, 1), (2, 3)]})
        expected_taskinfo1.discovery.visibility = 'EXTERNAL'
        expected_taskinfo1.discovery.name = 'node1'
        expected_taskinfo1.discovery.ports.ports = []

        launch = asyncio.ensure_future(self.sched.launch(
            self.physical_graph, self.resolver))
        await asynctest.exhaust_callbacks(self.loop)
        # The tasks must be in state STARTING, but not yet RUNNING because
        # there are no offers.
        for node in self.nodes:
            assert_equal(TaskState.STARTING, node.state)
            assert_false(node.ready_event.is_set())
            assert_false(node.dead_event.is_set())
        assert_equal({TaskState.STARTING: 2}, self.task_stats.state_counts)
        assert_equal([mock.call.reviveOffers({'default'})], self._driver_calls())
        self.driver.reset_mock()
        # Now provide an offer that is suitable for node1 but not node0.
        # Nothing should happen, because we don't yet have enough resources.
        self.sched.resourceOffers(self.driver, [offer1])
        await asynctest.exhaust_callbacks(self.loop)
        assert_equal([], self._driver_calls())
        for node in self.nodes:
            assert_equal(TaskState.STARTING, node.state)
            assert_false(node.ready_event.is_set())
            assert_false(node.dead_event.is_set())
        # Provide offer suitable for launching node0. At this point all nodes
        # should launch.
        self.sched.resourceOffers(self.driver, [offer0])
        await self.advance(60)   # For the benefit of test_launch_slow_resolve

        assert_equal(TaskState.STARTED, self.nodes[0].state)
        assert_equal(expected_taskinfo0.resources, self.nodes[0].taskinfo.resources)
        assert_equal(expected_taskinfo0, self.nodes[0].taskinfo)
        assert_equal('agenthost0', self.nodes[0].host)
        assert_equal('agentid0', self.nodes[0].agent_id)
        assert_equal({'port': 30000}, self.nodes[0].ports)
        assert_equal({}, self.nodes[0].cores)
        assert_is_none(self.nodes[0].status)

        assert_equal(expected_taskinfo1, self.nodes[1].taskinfo)
        assert_equal('agentid1', self.nodes[1].agent_id)
        assert_equal(TaskState.STARTED, self.nodes[1].state)

        assert_equal(TaskState.READY, self.nodes[2].state)
        assert_equal(AnyOrderList([
            mock.call.launchTasks([offer0.id], [expected_taskinfo0]),
            mock.call.launchTasks([offer1.id], [expected_taskinfo1]),
            mock.call.suppressOffers({'default'})
        ]), self._driver_calls())
        self.driver.reset_mock()
        assert_equal({TaskState.STARTED: 2}, self.task_stats.state_counts)

        # Tell scheduler that node1 is now running. It should go to RUNNING
        # but not READY, because node0 isn't ready yet.
        status = self._status_update(self.task_ids[1], 'TASK_RUNNING')
        await asynctest.exhaust_callbacks(self.loop)
        assert_equal(TaskState.RUNNING, self.nodes[1].state)
        assert_equal(status, self.nodes[1].status)
        assert_equal([mock.call.acknowledgeStatusUpdate(status)],
                     self._driver_calls())
        self.driver.reset_mock()
        assert_equal({TaskState.STARTED: 1, TaskState.RUNNING: 1}, self.task_stats.state_counts)

        # A real node1 would issue an HTTP request back to the scheduler. Fake
        # it instead, to check the timing.
        wait_request = self.loop.create_task(self._wait_request(self.task_ids[1]))
        await asynctest.exhaust_callbacks(self.loop)
        assert_false(wait_request.done())

        # Tell scheduler that node0 is now running. This will start up the
        # the waiter, so we need to mock poll_ports.
        with mock.patch.object(scheduler, 'poll_ports', autospec=True) as poll_ports:
            poll_future = future_return(poll_ports)
            status = self._status_update(self.task_ids[0], 'TASK_RUNNING')
            await asynctest.exhaust_callbacks(self.loop)
            assert_equal(TaskState.RUNNING, self.nodes[0].state)
            assert_equal(status, self.nodes[0].status)
            assert_equal([mock.call.acknowledgeStatusUpdate(status)],
                         self._driver_calls())
            self.driver.reset_mock()
            poll_ports.assert_called_once_with('agenthost0', [30000])
        assert_equal({TaskState.RUNNING: 2}, self.task_stats.state_counts)

        # Make poll_ports ready. Node 0 should now become ready, which will
        # make node 1 ready too.
        poll_future.set_result(None)
        await asynctest.exhaust_callbacks(self.loop)
        assert_equal(TaskState.READY, self.nodes[0].state)
        await wait_request
        assert_equal(TaskState.READY, self.nodes[1].state)
        assert_true(launch.done())
        assert_equal({TaskState.READY: 2}, self.task_stats.state_counts)
        await launch

    async def test_launch_slow_resolve(self):
        """Like test_launch_serial, but where a task has a slow resolve call.

        This is a regression test for SR-1093.
        """
        class SlowResolve(scheduler.PhysicalTask):
            async def resolve(self, resolver, graph, image_path=None):
                await asyncio.sleep(30)
                await super().resolve(resolver, graph, image_path=None)

        for node in self.logical_graph.nodes():
            if node.name == 'node0':
                node.physical_factory = SlowResolve
                break
        else:
            raise KeyError('Could not find node0')
        self._make_physical()
        await self.test_launch_serial()

    async def _transition_node0(self, target_state, nodes=None, ports=None):
        """Launch the graph and proceed until node0 is in `target_state`.

        This is intended to be used in test setup. It is assumed that this
        functionality is more fully tested in test_launch_serial, so minimal
        assertions are made.

        Returns
        -------
        launch, kill : :class:`asyncio.Task`
            Asynchronous tasks for launching and killing the graph. If
            `target_state` is not :const:`TaskState.KILLING` or
            :const:`TaskState.DEAD`, then `kill` is ``None``
        """
        assert target_state > TaskState.NOT_READY
        offers = self._make_offers(ports)
        launch = asyncio.ensure_future(
            self.sched.launch(self.physical_graph, self.resolver, nodes))
        kill = None
        await asynctest.exhaust_callbacks(self.loop)
        assert_equal(TaskState.STARTING, self.nodes[0].state)
        with mock.patch.object(scheduler, 'poll_ports', autospec=True) as poll_ports:
            poll_future = future_return(poll_ports)
            if target_state > TaskState.STARTING:
                self.sched.resourceOffers(self.driver, offers)
                await asynctest.exhaust_callbacks(self.loop)
                assert_equal(TaskState.STARTED, self.nodes[0].state)
                task_id = self.nodes[0].taskinfo.task_id.value
                if target_state > TaskState.STARTED:
                    self._status_update(task_id, 'TASK_RUNNING')
                    await asynctest.exhaust_callbacks(self.loop)
                    assert_equal(TaskState.RUNNING, self.nodes[0].state)
                    if target_state > TaskState.RUNNING:
                        poll_future.set_result(None)   # Mark ports as ready
                        await asynctest.exhaust_callbacks(self.loop)
                        assert_equal(TaskState.READY, self.nodes[0].state)
                        if target_state > TaskState.READY:
                            kill = asyncio.ensure_future(
                                self.sched.kill(self.physical_graph, nodes))
                            await asynctest.exhaust_callbacks(self.loop)
                            assert_equal(TaskState.KILLING, self.nodes[0].state)
                            if target_state > TaskState.KILLING:
                                self._status_update(task_id, 'TASK_KILLED')
                            await asynctest.exhaust_callbacks(self.loop)
        self.driver.reset_mock()
        assert_equal(target_state, self.nodes[0].state)
        return (launch, kill)

    async def _ready_graph(self):
        """Gets the whole graph to READY state"""
        launch, kill = await self._transition_node0(TaskState.READY)
        self._status_update(self.nodes[1].taskinfo.task_id.value, 'TASK_RUNNING')
        await asynctest.exhaust_callbacks(self.loop)
        assert_true(launch.done())  # Ensures the next line won't hang the test
        await launch
        self.driver.reset_mock()

    async def test_multiple_queues(self):
        # Remove the dependency between nodes so that they can be launched
        # independently
        self.physical_graph.remove_edge(self.nodes[1], self.nodes[0])
        self.nodes[1].logical_node.command = [
            'test', '--host={host}', '--another={endpoints[node2_foo]}']
        # Schedule the nodes separately on separate queues
        queue = scheduler.LaunchQueue('default')
        self.sched.add_queue(queue)
        launch0, kill0 = await self._transition_node0(TaskState.STARTING, [self.nodes[0]])
        launch1 = asyncio.ensure_future(
            self.sched.launch(self.physical_graph, self.resolver,
                              self.nodes[1:], queue=queue))
        # Make an offer so that node1 can start
        offers = self._make_offers()
        self.sched.resourceOffers(self.driver, [offers[1]])
        await asynctest.exhaust_callbacks(self.loop)
        assert_equal(TaskState.STARTED, self.nodes[1].state)
        self._status_update(self.nodes[1].taskinfo.task_id.value, 'TASK_RUNNING')
        await asynctest.exhaust_callbacks(self.loop)
        assert_true(launch1.done())

        # Now unblock node0
        assert_equal(TaskState.STARTING, self.nodes[0].state)
        self.sched.resourceOffers(self.driver, [offers[0]])
        await asynctest.exhaust_callbacks(self.loop)
        assert_equal(TaskState.STARTED, self.nodes[0].state)
        with mock.patch.object(scheduler, 'poll_ports', autospec=True) as poll_ports:
            poll_future = future_return(poll_ports)
            poll_future.set_result(None)   # Mark ports as ready
            self._status_update(self.nodes[0].taskinfo.task_id.value, 'TASK_RUNNING')
            await asynctest.exhaust_callbacks(self.loop)
        assert_equal(TaskState.READY, self.nodes[0].state)
        assert_true(launch0.done())

    async def _test_launch_cancel(self, target_state):
        launch, kill = await self._transition_node0(target_state)
        assert_false(launch.done())
        # Now cancel and check that nodes go back to NOT_READY if they were
        # in STARTING, otherwise keep their state.
        launch.cancel()
        await asynctest.exhaust_callbacks(self.loop)
        if target_state == TaskState.STARTING:
            for node in self.nodes:
                assert_equal(TaskState.NOT_READY, node.state)
        else:
            assert_equal(target_state, self.nodes[0].state)

    async def test_launch_cancel_wait_task(self):
        """Test cancelling a launch while waiting for a task to become READY"""
        await self._test_launch_cancel(TaskState.RUNNING)

    async def test_launch_cancel_wait_resource(self):
        """Test cancelling a launch while waiting for resources"""
        await self._test_launch_cancel(TaskState.STARTING)

    async def test_launch_resources_timeout(self):
        """Test a launch failing due to insufficient resources within the timeout"""
        launch, kill = await self._transition_node0(TaskState.STARTING)
        await self.advance(30)
        with pytest.raises(scheduler.InsufficientResourcesError):
            await launch
        assert_equal(TaskState.NOT_READY, self.nodes[0].state)
        assert_equal(TaskState.NOT_READY, self.nodes[1].state)
        assert_equal(TaskState.NOT_READY, self.nodes[2].state)
        # Once we abort, we should no longer be interested in offers
        assert_equal([mock.call.suppressOffers({'default'})], self._driver_calls())

    async def test_launch_queue_busy(self):
        """Test a launch failing due to tasks ahead of it blocking the queue."""
        launch, kill = await self._transition_node0(TaskState.STARTING, nodes=[self.nodes[0]])
        launch1 = asyncio.ensure_future(
            self.sched.launch(self.physical_graph, self.resolver, [self.nodes[2]],
                              resources_timeout=2))
        await self.advance(3)
        with pytest.raises(scheduler.QueueBusyError, match='(2s)') as cm:
            await launch1
        assert cm.value.timeout == 2
        assert_false(launch.done())
        await self.advance(30)
        with pytest.raises(scheduler.InsufficientResourcesError):
            await launch

    async def test_launch_force_host(self):
        """Like test_launch_serial, but tests forcing a logical task to a node."""
        for node in self.logical_graph.nodes():
            if node.name == 'node0':
                node.host = 'agenthost0'
        self._make_physical()
        await self.test_launch_serial()

    async def test_launch_bad_host(self):
        """Force a host which doesn't have sufficient resources"""
        for node in self.logical_graph.nodes():
            if node.name == 'node0':
                node.host = 'agenthost1'
        self._make_physical()
        launch, kill = await self._transition_node0(TaskState.STARTING, [self.nodes[0]])
        offers = self._make_offers()
        self.sched.resourceOffers(self.driver, offers)
        await self.advance(30)
        with pytest.raises(scheduler.InsufficientResourcesError):
            await launch

    async def test_launch_multicast_conflict(self):
        """Test launching when an interface can't be used due to multicast loopback limitations."""
        node3 = scheduler.LogicalTask('node3')
        node3.command = ['hello']
        node3.image = 'image0'
        # Add a GPU just to force it to run on agent0
        node3.gpus.append(scheduler.GPURequest())
        node3.gpus[-1].compute = 0.5
        node3.gpus[-1].mem = 256.0
        node3.interfaces = [
            scheduler.InterfaceRequest('net0', multicast_in={'mc'})
        ]
        node3.interfaces[-1].bandwidth_in = 1e6
        node3.interfaces[-1].bandwidth_out = 1e6
        self.logical_graph.add_node(node3)

        self._make_physical()
        launch, kill = await self._transition_node0(TaskState.STARTING)
        offers = self._make_offers()
        self.sched.resourceOffers(self.driver, offers)
        await self.advance(30)
        with pytest.raises(scheduler.InsufficientResourcesError):
            await launch

    async def test_launch_resolve_raises(self):
        async def resolve_raise(resolver, graph, image_path=None):
            raise ValueError('Testing')

        self.nodes[0].resolve = resolve_raise
        launch, kill = await self._transition_node0(TaskState.STARTING)
        offers = self._make_offers()
        self.sched.resourceOffers(self.driver, offers)
        with pytest.raises(ValueError, match='Testing'):
            await launch
        # The offers must be returned to Mesos
        assert_equal(AnyOrderList([
            mock.call.declineOffer(AnyOrderList([offers[0].id, offers[1].id])),
            mock.call.suppressOffers({'default'})]), self._driver_calls())

    async def test_offer_rescinded(self):
        """Test offerRescinded"""
        launch, kill = await self._transition_node0(TaskState.STARTING)
        offers = self._make_offers()
        # Provide an offer that is sufficient only for node 0
        self.sched.resourceOffers(self.driver, [offers[0]])
        await asynctest.exhaust_callbacks(self.loop)
        assert_equal(TaskState.STARTING, self.nodes[0].state)
        # Rescind the offer
        self.sched.offerRescinded(self.driver, offers[0].id)
        # Make a new offer, which is also insufficient, but which with the
        # original one would have been sufficient.
        self.sched.resourceOffers(self.driver, [offers[1]])
        await asynctest.exhaust_callbacks(self.loop)
        assert_equal(TaskState.STARTING, self.nodes[0].state)
        # Rescind an unknown offer. This can happen if an offer was accepted at
        # the same time as it was rescinded.
        offer2 = self._make_offer({'cpus': 0.8, 'mem': 128.0, 'ports': [(31000, 32000)]}, 1)
        self.sched.offerRescinded(self.driver, offer2.id)
        await asynctest.exhaust_callbacks(self.loop)
        assert_equal([], self._driver_calls())
        launch.cancel()

    async def test_decline_unneeded_offers(self):
        """Test that useless offers are not hoarded."""
        launch, kill = await self._transition_node0(TaskState.STARTING)
        offers = self._make_offers()
        # Replace offer 0 with a useless offer
        offers[0] = self._make_offer({'cpus': 0.1})
        self.sched.resourceOffers(self.driver, offers)
        await asynctest.exhaust_callbacks(self.loop)
        assert_equal(TaskState.STARTING, self.nodes[0].state)
        assert_equal([mock.call.declineOffer([offers[0].id])], self._driver_calls())
        launch.cancel()

    async def test_unavailability(self):
        """Test offers with unavailability information"""
        launch, kill = await self._transition_node0(TaskState.STARTING, [self.nodes[0]])
        # Provide an offer that would be sufficient if not for unavailability
        offer0 = self._make_offers()[0]
        offer0.unavailability.start.nanoseconds = int(time.time() * 1e9)
        offer0.unavailability.duration.nanoseconds = int(3600e9)
        self.sched.resourceOffers(self.driver, [offer0])
        await asynctest.exhaust_callbacks(self.loop)
        assert_equal(TaskState.STARTING, self.nodes[0].state)
        assert_equal([mock.call.declineOffer([offer0.id])], self._driver_calls())
        launch.cancel()

    async def test_unavailability_forever(self):
        """Test offers with unavailability and no end time."""
        # TODO: once we've switched to pytest, parametrise this test with the
        # previous one.
        launch, kill = await self._transition_node0(TaskState.STARTING, [self.nodes[0]])
        # Provide an offer that would be sufficient if not for unavailability
        offer0 = self._make_offers()[0]
        offer0.unavailability.start.nanoseconds = int(time.time() * 1e9)
        self.sched.resourceOffers(self.driver, [offer0])
        await asynctest.exhaust_callbacks(self.loop)
        assert_equal(TaskState.STARTING, self.nodes[0].state)
        assert_equal([mock.call.declineOffer([offer0.id])], self._driver_calls())
        launch.cancel()

    async def test_unavailability_past(self):
        """Test offers with unavailability information in the past"""
        launch, kill = await self._transition_node0(TaskState.STARTING, [self.nodes[0]])
        # Provide an offer that would be sufficient if not for unavailability
        offer0 = self._make_offers()[0]
        offer0.unavailability.start.nanoseconds = int(time.time() * 1e9 - 7200e9)
        offer0.unavailability.duration.nanoseconds = int(3600e9)
        self.sched.resourceOffers(self.driver, [offer0])
        await asynctest.exhaust_callbacks(self.loop)
        assert_equal(TaskState.STARTED, self.nodes[0].state)
        assert_equal([
            mock.call.launchTasks([offer0.id], mock.ANY),
            mock.call.suppressOffers({'default'})
        ], self._driver_calls())
        launch.cancel()

    async def _test_kill_in_state(self, state):
        """Test killing a node while it is in the given state"""
        launch, kill = await self._transition_node0(state, [self.nodes[0]])
        kill = asyncio.ensure_future(self.sched.kill(self.physical_graph, [self.nodes[0]]))
        await asynctest.exhaust_callbacks(self.loop)
        if state > TaskState.STARTING:
            assert_equal(TaskState.KILLING, self.nodes[0].state)
            status = self._status_update(self.nodes[0].taskinfo.task_id.value, 'TASK_KILLED')
            await asynctest.exhaust_callbacks(self.loop)
            assert_is(status, self.nodes[0].status)
            assert_equal({TaskState.DEAD: 1}, self.task_stats.state_counts)
        assert_equal(TaskState.DEAD, self.nodes[0].state)
        await launch
        await kill

    async def test_kill_while_starting(self):
        """Test killing a node while in state STARTING"""
        await self._test_kill_in_state(TaskState.STARTING)

    async def test_kill_while_started(self):
        """Test killing a node while in state STARTED"""
        await self._test_kill_in_state(TaskState.STARTED)

    async def test_kill_while_running(self):
        """Test killing a node while in state RUNNING"""
        await self._test_kill_in_state(TaskState.RUNNING)

    async def test_kill_while_ready(self):
        """Test killing a node while in state READY"""
        await self._test_kill_in_state(TaskState.READY)

    async def test_kill_while_killed(self):
        """Test killing a node while in state KILLING"""
        await self._test_kill_in_state(TaskState.KILLING)

    async def _test_die_in_state(self, state):
        """Test a node dying on its own while it is in the given state"""
        launch, kill = await self._transition_node0(state, [self.nodes[0]])
        status = self._status_update(self.nodes[0].taskinfo.task_id.value, 'TASK_FINISHED')
        await asynctest.exhaust_callbacks(self.loop)
        assert_is(status, self.nodes[0].status)
        assert_equal(TaskState.DEAD, self.nodes[0].state)
        assert_true(self.nodes[0].ready_event.is_set())
        assert_true(self.nodes[0].dead_event.is_set())
        await launch

    async def test_die_while_started(self):
        """Test a process dying on its own while in state STARTED"""
        await self._test_die_in_state(TaskState.STARTED)

    async def test_die_while_running(self):
        """Test a process dying on its own while in state RUNNING"""
        await self._test_die_in_state(TaskState.RUNNING)

    async def test_die_while_ready(self):
        """Test a process dying on its own while in state READY"""
        await self._test_die_in_state(TaskState.READY)

    async def test_kill_order(self):
        """Kill must respect dependency ordering"""
        await self._ready_graph()
        # Now kill it. node1 must be dead before node0, node2 get killed
        kill = asyncio.ensure_future(self.sched.kill(self.physical_graph))
        await asynctest.exhaust_callbacks(self.loop)
        assert_equal([mock.call.killTask(self.nodes[1].taskinfo.task_id)],
                     self._driver_calls())
        assert_equal(TaskState.READY, self.nodes[0].state)
        assert_equal(TaskState.KILLING, self.nodes[1].state)
        assert_equal(TaskState.READY, self.nodes[2].state)
        self.driver.reset_mock()
        # node1 now dies, and node0 and node2 should be killed
        status = self._status_update(self.nodes[1].taskinfo.task_id.value, 'TASK_KILLED')
        await asynctest.exhaust_callbacks(self.loop)
        assert_equal(
            AnyOrderList([
                mock.call.killTask(self.nodes[0].taskinfo.task_id),
                mock.call.acknowledgeStatusUpdate(status)
            ]),
            self._driver_calls())
        assert_equal(TaskState.KILLING, self.nodes[0].state)
        assert_equal(TaskState.DEAD, self.nodes[1].state)
        assert_equal(TaskState.DEAD, self.nodes[2].state)
        assert_false(kill.done())
        # node0 now dies, to finish the cleanup
        self._status_update(self.nodes[0].taskinfo.task_id.value, 'TASK_KILLED')
        await asynctest.exhaust_callbacks(self.loop)
        assert_equal(TaskState.DEAD, self.nodes[0].state)
        assert_equal(TaskState.DEAD, self.nodes[1].state)
        assert_equal(TaskState.DEAD, self.nodes[2].state)
        assert_true(kill.done())
        await kill

    async def _transition_batch_run(self, node, state):
        """Interact with scheduler to get batch task to state `state`.

        It is assumed that it has already been launched.
        """
        if state >= TaskState.STARTED:
            await asynctest.exhaust_callbacks(self.loop)
            self.sched.resourceOffers(self.driver, self._make_offers())
            await asynctest.exhaust_callbacks(self.loop)
            assert_equal(TaskState.STARTED, node.state)
            if state >= TaskState.READY:
                task_id = node.taskinfo.task_id.value
                self._status_update(task_id, 'TASK_RUNNING')
                await asynctest.exhaust_callbacks(self.loop)
                assert_equal(TaskState.READY, node.state)
                if state >= TaskState.DEAD:
                    self._status_update(task_id, 'TASK_FINISHED')
                    await asynctest.exhaust_callbacks(self.loop)
                    assert_equal(TaskState.DEAD, node.state)

    async def test_batch_run_success(self):
        """batch_run for the case of a successful run"""
        task = asyncio.ensure_future(self.sched.batch_run(
            self.physical_batch_graph, self.resolver, [self.batch_nodes[0]]))
        await self._transition_batch_run(self.batch_nodes[0], TaskState.DEAD)
        results = await task
        assert_equal(results, {self.batch_nodes[0]: None})
        assert_equal(self.task_stats.batch_created, 1)
        assert_equal(self.task_stats.batch_started, 1)
        assert_equal(self.task_stats.batch_done, 1)
        assert_equal(self.task_stats.batch_retried, 0)
        assert_equal(self.task_stats.batch_failed, 0)
        assert_equal(self.task_stats.batch_skipped, 0)

    async def test_batch_run_failure(self):
        """batch_run with a failing task"""
        task = asyncio.ensure_future(self.sched.batch_run(
            self.physical_batch_graph, self.resolver, [self.batch_nodes[0]]))
        await self._transition_batch_run(self.batch_nodes[0], TaskState.READY)
        task_id = self.batch_nodes[0].taskinfo.task_id.value
        self._status_update(task_id, 'TASK_FAILED')
        results = await task
        with pytest.raises(scheduler.TaskError):
            raise next(iter(results.values()))
        assert_equal(self.task_stats.batch_created, 1)
        assert_equal(self.task_stats.batch_started, 1)
        assert_equal(self.task_stats.batch_done, 1)
        assert_equal(self.task_stats.batch_retried, 0)
        assert_equal(self.task_stats.batch_failed, 1)
        assert_equal(self.task_stats.batch_skipped, 0)

    async def test_batch_run_resources_timeout(self):
        """batch_run with a task that doesn't get resources in time"""
        task = asyncio.ensure_future(self.sched.batch_run(
            self.physical_batch_graph, self.resolver, [self.batch_nodes[0]]))
        await self.advance(30)
        results = await task
        with pytest.raises(scheduler.TaskInsufficientResourcesError):
            raise next(iter(results.values()))
        assert_equal(self.task_stats.batch_created, 1)
        assert_equal(self.task_stats.batch_started, 1)
        assert_equal(self.task_stats.batch_done, 1)
        assert_equal(self.task_stats.batch_retried, 0)
        assert_equal(self.task_stats.batch_failed, 1)
        assert_equal(self.task_stats.batch_skipped, 0)

    async def _batch_run_retry_second(self, task):
        """Do the retry on a test_batch_run_retry_* test."""
        # The graph should now have been modified in place, so we need to
        # get the new physical node
        self.batch_nodes[0] = next(node for node in self.physical_batch_graph
                                   if node.name == 'batch0')
        await self._transition_batch_run(self.batch_nodes[0], TaskState.DEAD)
        await task
        assert_equal(self.task_stats.batch_created, 1)
        assert_equal(self.task_stats.batch_started, 1)
        assert_equal(self.task_stats.batch_done, 1)
        assert_equal(self.task_stats.batch_retried, 1)
        assert_equal(self.task_stats.batch_failed, 0)
        assert_equal(self.task_stats.batch_skipped, 0)

    async def test_batch_run_retry(self):
        """batch_run where first attempt fails, later attempt succeeds."""
        task = asyncio.ensure_future(self.sched.batch_run(
            self.physical_batch_graph, self.resolver, [self.batch_nodes[0]], attempts=2))
        await self._transition_batch_run(self.batch_nodes[0], TaskState.READY)
        task_id = self.batch_nodes[0].taskinfo.task_id.value
        self._status_update(task_id, 'TASK_FAILED')
        await asynctest.exhaust_callbacks(self.loop)
        await self._batch_run_retry_second(task)

    async def test_batch_run_depends(self):
        """Batch launch with one task depending on another"""
        task = asyncio.ensure_future(self.sched.batch_run(self.physical_batch_graph, self.resolver))
        await self._transition_batch_run(self.batch_nodes[0], TaskState.READY)
        # Ensure that we haven't even tried to launch the second one
        assert_equal(TaskState.NOT_READY, self.batch_nodes[1].state)
        # Kill it, the next one should start up
        self._status_update(self.batch_nodes[0].taskinfo.task_id.value, 'TASK_FINISHED')
        await asynctest.exhaust_callbacks(self.loop)
        assert_equal(TaskState.DEAD, self.batch_nodes[0].state)
        assert_equal(TaskState.STARTING, self.batch_nodes[1].state)

        await self._transition_batch_run(self.batch_nodes[1], TaskState.DEAD)
        results = await task
        assert_equal(results, {self.batch_nodes[0]: None, self.batch_nodes[1]: None})

    async def test_batch_run_skip(self):
        """If a dependency fails, the dependent task is skipped"""
        task = asyncio.ensure_future(self.sched.batch_run(self.physical_batch_graph, self.resolver))
        await self._transition_batch_run(self.batch_nodes[0], TaskState.READY)
        # Kill it
        self._status_update(self.batch_nodes[0].taskinfo.task_id.value, 'TASK_FAILED')
        await asynctest.exhaust_callbacks(self.loop)
        assert_equal(TaskState.DEAD, self.batch_nodes[0].state)
        # Next task shouldn't even try to start
        assert_equal(TaskState.NOT_READY, self.batch_nodes[1].state)
        results = await task
        with pytest.raises(scheduler.TaskError):
            raise results[self.batch_nodes[0]]
        with pytest.raises(scheduler.TaskSkipped):
            raise results[self.batch_nodes[1]]
        assert_equal(self.task_stats.batch_created, 2)
        assert_equal(self.task_stats.batch_started, 1)
        assert_equal(self.task_stats.batch_done, 2)
        assert_equal(self.task_stats.batch_retried, 0)
        assert_equal(self.task_stats.batch_failed, 1)
        assert_equal(self.task_stats.batch_skipped, 1)

    async def test_batch_run_non_critical_failure(self):
        """If a non-critical dependency fails, the dependent task runs anyway."""
        # Modify graph to make dependency non-critical
        for u, v, data in self.logical_batch_graph.edges(data=True):
            if data.get('depends_finished', False):
                data['depends_finished_critical'] = False
        for u, v, data in self.logical_batch_graph.edges(data=True):
            print(u, v, data)
        self._make_physical()

        task = asyncio.ensure_future(self.sched.batch_run(self.physical_batch_graph, self.resolver))
        await self._transition_batch_run(self.batch_nodes[0], TaskState.READY)
        assert_equal(TaskState.NOT_READY, self.batch_nodes[1].state)
        # Kill it
        self._status_update(self.batch_nodes[0].taskinfo.task_id.value, 'TASK_FAILED')
        await asynctest.exhaust_callbacks(self.loop)
        assert_equal(TaskState.DEAD, self.batch_nodes[0].state)
        # Next task should now start
        assert_equal(TaskState.STARTING, self.batch_nodes[1].state)
        await self._transition_batch_run(self.batch_nodes[1], TaskState.DEAD)
        results = await task
        with pytest.raises(scheduler.TaskError):
            raise results[self.batch_nodes[0]]
        assert_is_none(results[self.batch_nodes[1]])
        assert_equal(self.task_stats.batch_created, 2)
        assert_equal(self.task_stats.batch_started, 2)
        assert_equal(self.task_stats.batch_done, 2)
        assert_equal(self.task_stats.batch_retried, 0)
        assert_equal(self.task_stats.batch_failed, 1)
        assert_equal(self.task_stats.batch_skipped, 0)

    async def test_batch_run_depends_retry(self):
        """If a dependencies fails once, wait until it's retried."""
        task = asyncio.ensure_future(self.sched.batch_run(
            self.physical_batch_graph, self.resolver, attempts=3))
        await self._transition_batch_run(self.batch_nodes[0], TaskState.READY)
        task_id = self.batch_nodes[0].taskinfo.task_id.value
        self._status_update(task_id, 'TASK_FAILED')
        await asynctest.exhaust_callbacks(self.loop)
        # The graph should now have been modified in place, so we need to
        # get the new physical node
        self.batch_nodes[0] = next(node for node in self.physical_batch_graph
                                   if node.name == 'batch0')
        # Retry the first task. The next task must not have started yet.
        await self._transition_batch_run(self.batch_nodes[0], TaskState.READY)
        await asynctest.exhaust_callbacks(self.loop)
        assert_equal(TaskState.NOT_READY, self.batch_nodes[1].state)
        # Finish the retried first task.
        self._status_update(self.batch_nodes[0].taskinfo.task_id.value, 'TASK_FINISHED')
        await asynctest.exhaust_callbacks(self.loop)
        assert_equal(TaskState.DEAD, self.batch_nodes[0].state)
        assert_equal(TaskState.STARTING, self.batch_nodes[1].state)
        # Finish the second task
        await self._transition_batch_run(self.batch_nodes[1], TaskState.DEAD)
        results = await task
        assert_equal(list(results.values()), [None, None])

    async def test_close(self):
        """Close must kill off all remaining tasks and abort any pending launches"""
        await self._ready_graph()
        # Start launching a second graph, but do not give it resources
        physical_graph2 = scheduler.instantiate(self.logical_graph)
        launch = asyncio.ensure_future(self.sched.launch(physical_graph2, self.resolver))
        await asynctest.exhaust_callbacks(self.loop)
        close = asyncio.ensure_future(self.sched.close())
        await asynctest.exhaust_callbacks(self.loop)
        status1 = self._status_update(self.nodes[1].taskinfo.task_id.value, 'TASK_KILLED')
        await asynctest.exhaust_callbacks(self.loop)
        status0 = self._status_update(self.nodes[0].taskinfo.task_id.value, 'TASK_KILLED')
        # defer is insufficient here, because close() uses run_in_executor to
        # join the driver thread. Wait up to 5 seconds for that to happen.
        await asyncio.wait_for(close, 5)
        for node in self.physical_graph:
            assert_equal(TaskState.DEAD, node.state)
        for node in physical_graph2:
            assert_equal(TaskState.DEAD, node.state)
        # The timing of suppressOffers is undefined, because it depends on the
        # order in which the graphs are killed. However, it must occur
        # after the initial reviveOffers and before stopping the driver.
        driver_calls = self._driver_calls()
        assert_in(mock.call.suppressOffers({'default'}), driver_calls)
        pos = driver_calls.index(mock.call.suppressOffers({'default'}))
        assert_true(1 <= pos < len(driver_calls) - 2)
        del driver_calls[pos]
        assert_equal([
            mock.call.reviveOffers({'default'}),
            mock.call.killTask(self.nodes[1].taskinfo.task_id),
            mock.call.acknowledgeStatusUpdate(status1),
            mock.call.killTask(self.nodes[0].taskinfo.task_id),
            mock.call.acknowledgeStatusUpdate(status0),
            mock.call.stop(),
            mock.call.join()
            ], driver_calls)
        assert_true(launch.done())
        await launch

    async def test_status_unknown_task_id(self):
        """statusUpdate must correctly handle an unknown task ID.

        It must also kill it if it's not already dead.
        """
        self._status_update('test-01234567', 'TASK_LOST')
        self._status_update('test-12345678', 'TASK_RUNNING')
        await asynctest.exhaust_callbacks(self.loop)
        self.driver.killTask.assert_called_once_with({'value': 'test-12345678'})

    async def test_retry_kill(self):
        """Killing a task must be retried after a timeout."""
        await self._ready_graph()
        kill = asyncio.ensure_future(self.sched.kill(self.physical_graph, [self.nodes[0]]))
        await asynctest.exhaust_callbacks(self.loop)
        self.driver.killTask.assert_called_once_with(self.nodes[0].taskinfo.task_id)
        self.driver.killTask.reset_mock()
        # Send a task status update in less than the kill timeout. It must not
        # lead to a retry.
        await self.advance(1.0)
        self._status_update(self.task_ids[0], 'TASK_RUNNING')
        await asynctest.exhaust_callbacks(self.loop)
        self.driver.killTask.assert_not_called()

        # Give time for reconciliation requests to occur
        await self.advance(70.0)
        self.driver.reconcileTasks.assert_called()
        self._status_update(self.task_ids[0], 'TASK_RUNNING')
        await asynctest.exhaust_callbacks(self.loop)
        self.driver.killTask.assert_called_once_with(self.nodes[0].taskinfo.task_id)

        # Send an update for TASK_KILLING state: must not trigger another attempt
        self.driver.killTask.reset_mock()
        self._status_update(self.task_ids[0], 'TASK_KILLING')
        await asynctest.exhaust_callbacks(self.loop)
        self.driver.killTask.assert_not_called()

        # Let it die so that we can clean up the async task
        self._status_update(self.task_ids[0], 'TASK_FAILED')
        await kill
