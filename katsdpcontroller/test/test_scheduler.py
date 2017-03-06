from __future__ import print_function, division, absolute_import
import json
import base64
import socket
import contextlib
import logging
import uuid
import threading
import BaseHTTPServer
import mock
import requests_mock
from nose.tools import *
from katsdpcontroller import scheduler
from katsdpcontroller.scheduler import TaskState
import ipaddress
import six
import requests
import trollius
import networkx
import pymesos
from addict import Dict
from trollius import From, Return
try:
    import unittest2 as unittest
except ImportError:
    import unittest


def run_with_event_loop(func):
    """Decorator to mark a function as a coroutine. When the wrapper is called,
    it creates an event loop and runs the function on it.
    """
    func = trollius.coroutine(func)

    @six.wraps(func)
    def wrapper(*args, **kwargs):
        loop = trollius.new_event_loop()
        with contextlib.closing(loop):
            args2 = args + (loop,)
            loop.run_until_complete(func(*args2, **kwargs))
    return wrapper


def run_with_self_event_loop(func):
    """Like :meth:`run_with_event_loop`, but instead of creating an event loop,
    it uses one provided by the containing object.
    """
    func = trollius.coroutine(func)

    @six.wraps(func)
    def wrapper(self, *args, **kwargs):
        self.loop.run_until_complete(func(self, *args, **kwargs))
    return wrapper


def defer(depth=20, loop=None):
    """Returns a future which is signalled in the very near future.

    Specifically, it tries to wait until the event loop is otherwise idle. It
    does that by using :meth:`trollius.BaseEventLoop.call_soon` to defer
    completing the future, and does this `depth` times.
    """
    def callback(future, depth):
        if depth == 0:
            if not future.cancelled():
                future.set_result(None)
        else:
            loop.call_soon(callback, future, depth - 1)
    future = trollius.Future(loop=loop)
    loop.call_soon(callback, future, depth - 1)
    return future


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


class TestRangeResource(object):
    """Tests for :class:`katsdpcontroller.scheduler.RangeResource`"""
    def test_len(self):
        rr = scheduler.RangeResource()
        assert_equal(0, len(rr))
        rr.add_range(5, 8)
        rr.add_range(20, 30)
        assert_equal(13, len(rr))
        rr.popleft()
        rr.pop()
        assert_equal(11, len(rr))
        rr.add_range(20, 10)
        assert_equal(11, len(rr))

    def test_add_resource(self):
        rr = scheduler.RangeResource()
        resource = Dict()
        resource.ranges.range = [
            Dict(begin=5, end=7),
            Dict(begin=20, end=29)
        ]
        rr.add_resource(resource)
        assert_equal([(5, 8), (20, 30)], list(rr._ranges))
        assert_equal(13, len(rr))

    def test_iter(self):
        rr = scheduler.RangeResource()
        rr.add_range(9, 10)
        rr.add_range(5, 8)
        assert_equal([9, 5, 6, 7], list(iter(rr)))

    def test_remove(self):
        rr = scheduler.RangeResource()
        rr.add_range(9, 10)
        rr.add_range(5, 8)
        rr.add_range(20, 25)
        with assert_raises(ValueError):
            rr.remove(8)
        with assert_raises(ValueError):
            rr.remove(10)
        rr.remove(9)
        assert_equal([5, 6, 7, 20, 21, 22, 23, 24], list(rr))
        rr.remove(22)
        assert_equal([5, 6, 7, 20, 21, 23, 24], list(rr))
        rr.remove(24)
        assert_equal([5, 6, 7, 20, 21, 23], list(rr))

    def test_popleft(self):
        rr = scheduler.RangeResource()
        rr.add_range(9, 10)
        rr.add_range(5, 8)
        out = []
        for i in range(4):
            out.append(rr.popleft())
        assert_false(rr)
        assert_equal([9, 5, 6, 7], out)
        assert_raises(IndexError, rr.popleft)

    def test_pop(self):
        rr = scheduler.RangeResource()
        rr.add_range(9, 10)
        rr.add_range(5, 8)
        out = []
        for i in range(4):
            out.append(rr.pop())
        assert_false(rr)
        assert_equal([7, 6, 5, 9], out)
        assert_raises(IndexError, rr.pop)

    def test_popleft_min(self):
        rr = scheduler.RangeResource()
        rr.add_range(4, 7)
        rr.add_range(9, 10)
        rr.add_range(17, 20)
        assert_raises(IndexError, rr.popleft_min, 20)
        assert_equal(9, rr.popleft_min(8))
        assert_equal(18, rr.popleft_min(18))
        assert_equal([4, 5, 6, 17, 19], list(rr))

    def test_str(self):
        rr = scheduler.RangeResource()
        assert_equal('', str(rr))
        rr.add_range(9, 10)
        rr.add_range(5, 8)
        assert_equal('9,5-7', str(rr))


class TestPollPorts(object):
    """Tests for poll_ports"""
    def setup(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.bind(('127.0.0.1', 0))
        self.port = self.sock.getsockname()[1]

    def teardown(self):
        self.sock.close()

    @run_with_event_loop
    def test_normal(self, loop):
        future = trollius.async(scheduler.poll_ports('127.0.0.1', [self.port], loop), loop=loop)
        # Sleep for while, give poll_ports time to poll a few times
        yield From(trollius.sleep(1, loop=loop))
        assert_false(future.done())
        self.sock.listen(1)
        yield From(trollius.wait_for(future, timeout=5, loop=loop))

    @run_with_event_loop
    def test_cancel(self, loop):
        """poll_ports must be able to be cancelled gracefully"""
        future = trollius.async(scheduler.poll_ports('127.0.0.1', [self.port], loop), loop=loop)
        yield From(trollius.sleep(0.2, loop=loop))
        future.cancel()
        with assert_raises(trollius.CancelledError):
            yield From(future)

    @run_with_event_loop
    def test_bad_host(self, loop):
        """poll_ports raises :exc:`socket.gaierror` if given a bad address"""
        with assert_raises(socket.gaierror):
            yield From(scheduler.poll_ports('not a hostname', [self.port], loop))


class TestTaskState(object):
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
        # Python 2 allows ordering comparisons between any objects, so
        # only test this on Python 3
        if six.PY3:
            with assert_raises(TypeError):
                TaskState.RUNNING < 3
            with assert_raises(TypeError):
                TaskState.RUNNING > 3
            with assert_raises(TypeError):
                TaskState.RUNNING <= 3
            with assert_raises(TypeError):
                TaskState.RUNNING >= 3


class TestImageResolver(object):
    """Tests for :class:`katsdpcontroller.scheduler.ImageResolver`."""
    @run_with_event_loop
    def test_simple(self, loop):
        """Test the base case"""
        resolver = scheduler.ImageResolver()
        resolver.override('foo', 'my-registry:5000/bar:custom')
        assert_equal('sdp/test1:latest', (yield From(resolver('test1', loop))))
        assert_equal('sdp/test1:tagged', (yield From(resolver('test1:tagged', loop))))
        assert_equal('my-registry:5000/bar:custom', (yield From(resolver('foo', loop))))

    @run_with_event_loop
    def test_private_registry(self, loop):
        """Test with a private registry"""
        resolver = scheduler.ImageResolver(private_registry='my-registry:5000', use_digests=False)
        resolver.override('foo', 'my-registry:5000/bar:custom')
        assert_equal('my-registry:5000/test1:latest', (yield From(resolver('test1', loop))))
        assert_equal('my-registry:5000/test1:tagged', (yield From(resolver('test1:tagged', loop))))
        assert_equal('my-registry:5000/bar:custom', (yield From(resolver('foo', loop))))

    @mock.patch('docker.auth.load_config', autospec=True)
    @run_with_event_loop
    def test_private_registry_digests(self, load_config_mock, loop):
        """Test with a private registry, looking up a digest"""
        digest = "sha256:1234567812345678123456781234567812345678123456781234567812345678"""
        # Based on an actual registry response
        headers = {
            '/v2/myimage/manifests/latest': {
                'Content-Length': '1234',
                'Content-Type': 'application/vnd.docker.distribution.manifest.v2+json',
                'Docker-Content-Digest': digest,
                'Docker-Distribution-Api-Version': 'registry/2.0',
                'Etag': '"{}"'.format(digest),
                'X-Content-Type-Options': 'nosniff',
                'Date': 'Thu, 26 Jan 2017 11:31:22 GMT'
            }
        }
        # Response headers are modelled on an actual registry response
        with requests_mock.mock(case_sensitive=True) as rmock:
            rmock.head(
                'https://registry.invalid:5000/v2/myimage/manifests/latest',
                request_headers={
                    'Authorization': 'Basic ' + base64.urlsafe_b64encode('myuser:mypassword'),
                    'Accept': 'application/vnd.docker.distribution.manifest.v2+json'
                },
                headers={
                    'Content-Length': '1234',
                    'Content-Type': 'application/vnd.docker.distribution.manifest.v2+json',
                    'Docker-Content-Digest': digest,
                    'Docker-Distribution-Api-Version': 'registry/2.0',
                    'Etag': '"{}"'.format(digest),
                    'X-Content-Type-Options': 'nosniff',
                    'Date': 'Thu, 26 Jan 2017 11:31:22 GMT'
                })
            # This format isn't documented, but inferred from examining the real value
            load_config_mock.return_value = {
                u'registry.invalid:5000' : {
                    'email': None,
                    'username': u'myuser',
                    'password': u'mypassword',
                    'serveraddress': u'registry.invalid:5000'
                }
            }
            resolver = scheduler.ImageResolver(private_registry='registry.invalid:5000')
            image = yield From(resolver('myimage', loop))
        assert_equal('registry.invalid:5000/myimage@' + digest, image)

    @mock.patch('__builtin__.open', autospec=file)
    @run_with_event_loop
    def test_tag_file(self, open_mock, loop):
        """Test with a tag file"""
        open_mock.return_value.__enter__.return_value.read.return_value = b'tag1\n'
        resolver = scheduler.ImageResolver(private_registry='my-registry:5000',
                                           tag_file='tag_file', use_digests=False)
        resolver.override('foo', 'my-registry:5000/bar:custom')
        open_mock.assert_called_once_with('tag_file', 'r')
        assert_equal('my-registry:5000/test1:tag1', (yield From(resolver('test1', loop))))
        assert_equal('my-registry:5000/test1:tagged', (yield From(resolver('test1:tagged', loop))))
        assert_equal('my-registry:5000/bar:custom', (yield From(resolver('foo', loop))))

    @mock.patch('__builtin__.open', autospec=file)
    def test_bad_tag_file(self, open_mock):
        """A ValueError is raised if the tag file contains illegal content"""
        open_mock.return_value.__enter__.return_value.read.return_value = b'not a good :tag\n'
        with assert_raises(ValueError):
            scheduler.ImageResolver(private_registry='my-registry:5000', tag_file='tag_file')


class TestTaskIDAllocator(object):
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


def _make_resources(resources):
    out = AnyOrderList()
    for name, value in six.iteritems(resources):
        resource = Dict()
        resource.name = name
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
    return _make_text_attr(name, base64.urlsafe_b64encode(json.dumps(value)))


def _make_offer(framework_id, agent_id, host, resources, attrs=()):
    offer = Dict()
    offer.id.value = uuid.uuid4().hex
    offer.framework_id.value = framework_id
    offer.agent_id.value = agent_id
    offer.hostname = host
    offer.resources = _make_resources(resources)
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
            [{'name': 'eth0', 'network': 'net0', 'ipv4_address': '192.168.254.254',
                'numa_node': 1, 'infiniband_devices': ['/dev/infiniband/foo']}])
        self.if_attr_bad_json = _make_text_attr(
            'katsdpcontroller.interfaces',
            base64.urlsafe_b64encode('{not valid json'))
        self.if_attr_bad_schema = _make_json_attr(
            'katsdpcontroller.interfaces',
            [{'name': 'eth1'}])
        self.volume_attr = _make_json_attr(
            'katsdpcontroller.volumes',
            [{'name': 'vol1', 'host_path': '/host1'},
             {'name': 'vol2', 'host_path': '/host2', 'numa_node': 1}])
        self.gpu_attr = _make_json_attr(
            'katsdpcontroller.gpus',
            [{'devices': ['/dev/nvidia0', '/dev/nvidiactl', '/dev/nvidia-uvm'], 'driver_version': '123.45', 'name': 'Dummy GPU', 'device_attributes': {}, 'compute_capability': (5, 2), 'numa_node': 1},
             {'devices': ['/dev/nvidia1', '/dev/nvidiactl', '/dev/nvidia-uvm'], 'driver_version': '123.45', 'name': 'Dummy GPU', 'device_attributes': {}, 'compute_capability': (5, 2), 'numa_node': 0}])
        self.numa_attr = _make_json_attr(
            'katsdpcontroller.numa', [[0, 2, 4, 6], [1, 3, 5, 7]])
        self.priority_attr = Dict()
        self.priority_attr.name = 'katsdpcontroller.priority'
        self.priority_attr.type = 'SCALAR'
        self.priority_attr.scalar.value = 8.5

    def test_construct(self):
        """Construct an agent from some offers"""
        attrs = [self.if_attr, self.volume_attr, self.gpu_attr, self.numa_attr, self.priority_attr]
        offers = [
            self._make_offer({'cpus': 4.0, 'mem': 1024.0,
                              'ports': [(100, 200), (300, 350)], 'cores': [(0, 8)]}, attrs),
            self._make_offer({'cpus': 0.5, 'mem': 123.5, 'disk': 1024.5,
                              'katsdpcontroller.gpu.0.compute': 0.25,
                              'katsdpcontroller.gpu.0.mem': 256.0,
                              'katsdpcontroller.interface.0.bandwidth_in': 1e9,
                              'katsdpcontroller.interface.0.bandwidth_out': 1e9,
                              'cores': [(8, 9)]}, attrs),
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
        assert_equal(4.5, agent.cpus)
        assert_equal(1147.5, agent.mem)
        assert_equal(1024.5, agent.disk)
        assert_equal(2, len(agent.gpus))
        assert_equal(0.75, agent.gpus[0].compute)
        assert_equal(1280.0, agent.gpus[0].mem)
        assert_equal(0.125, agent.gpus[1].compute)
        assert_equal(2048.0, agent.gpus[1].mem)
        assert_equal(list(range(0, 9)), list(agent.cores))
        assert_equal(list(range(100, 200)) + list(range(300, 350)), list(agent.ports))
        assert_equal(1, len(agent.interfaces))
        assert_equal('eth0', agent.interfaces[0].name)
        assert_equal('net0', agent.interfaces[0].network)
        assert_equal(ipaddress.IPv4Address(u'192.168.254.254'), agent.interfaces[0].ipv4_address)
        assert_equal(1, agent.interfaces[0].numa_node)
        assert_equal(11e8, agent.interfaces[0].bandwidth_in)
        assert_equal(12e8, agent.interfaces[0].bandwidth_out)
        assert_equal(['/dev/infiniband/foo'], agent.interfaces[0].infiniband_devices)
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
        with assert_raises(ValueError):
            scheduler.Agent([])

    def test_special_disk_resource(self):
        """A resource for a non-root disk is ignored"""
        offers = [self._make_offer({'disk': 1024})]
        # Example from https://mesos.apache.org/documentation/latest/multiple-disk/
        offers[0].resources[0].disk.source.type = 'PATH'
        offers[0].resources[0].disk.source.path.root = '/mnt/data'
        agent = scheduler.Agent(offers)
        assert_equal(0, agent.disk)

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
        with assert_raises(scheduler.InsufficientResourcesError):
            agent.allocate(task)

    def test_allocate_insufficient_scalar(self):
        """allocate raises if the task requires too much of a scalar resource"""
        task = scheduler.LogicalTask('task')
        task.cpus = 4.0
        agent = scheduler.Agent([self._make_offer({'cpus': 2.0}, [])])
        with assert_raises(scheduler.InsufficientResourcesError):
            agent.allocate(task)

    def test_allocate_insufficient_range(self):
        """allocate raises if the task requires too much of a range resource"""
        task = scheduler.LogicalTask('task')
        task.cores = [None] * 3
        agent = scheduler.Agent([self._make_offer({'cores': [(4, 6)]}, [])])
        with assert_raises(scheduler.InsufficientResourcesError):
            agent.allocate(task)

    def test_allocate_missing_interface(self):
        """allocate raises if the task requires a network that is not present"""
        task = scheduler.LogicalTask('task')
        task.interfaces = [scheduler.InterfaceRequest('net0'), scheduler.InterfaceRequest('net1')]
        agent = scheduler.Agent([self._make_offer({}, [self.if_attr])])
        with assert_raises(scheduler.InsufficientResourcesError):
            agent.allocate(task)

    def test_allocate_missing_volume(self):
        """allocate raises if the task requires a volume that is not present"""
        task = scheduler.LogicalTask('task')
        task.volumes = [scheduler.VolumeRequest('vol-missing', '/container-path', 'RW')]
        agent = scheduler.Agent([self._make_offer({}, [self.volume_attr])])
        with assert_raises(scheduler.InsufficientResourcesError):
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
        with assert_raises(scheduler.InsufficientResourcesError):
            agent.allocate(task)

    def test_allocate_insufficient_interface(self):
        """allocate raises if the task requires more interface resources than available"""
        task = scheduler.LogicalTask('task')
        task.interfaces.append(scheduler.InterfaceRequest('net0'))
        task.interfaces[-1].bandwidth_in = 1200e6
        agent = scheduler.Agent([self._make_offer({
            'katsdpcontroller.interface.0.bandwidth_in': 1000e6,
            'katsdpcontroller.interface.0.bandwidth_out': 2000e6}, [self.if_attr])])
        with assert_raises(scheduler.InsufficientResourcesError):
            agent.allocate(task)

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
        with assert_raises(scheduler.InsufficientResourcesError):
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
        with assert_raises(scheduler.InsufficientResourcesError):
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
        with assert_raises(scheduler.InsufficientResourcesError):
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
        with assert_raises(scheduler.InsufficientResourcesError):
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
        with assert_raises(scheduler.InsufficientResourcesError):
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
        assert_equal(4.0, ra.cpus)
        assert_equal(128.0, ra.mem)
        assert_equal(1, len(ra.interfaces))
        assert_equal(1000e6, ra.interfaces[0].bandwidth_in)
        assert_equal(500e6, ra.interfaces[0].bandwidth_out)
        assert_equal(0, ra.interfaces[0].index)
        assert_equal([scheduler.Volume(name='vol2', host_path='/host2', numa_node=1)],
                     ra.volumes)
        assert_equal(2, len(ra.gpus))
        assert_equal(0.5, ra.gpus[0].compute)
        assert_equal(1024.0, ra.gpus[0].mem)
        assert_equal(1, ra.gpus[0].index)
        assert_equal(0.5, ra.gpus[1].compute)
        assert_equal(256.0, ra.gpus[1].mem)
        assert_equal(0, ra.gpus[1].index)
        assert_equal([3, 5, 7], ra.cores)
        # Check that the resources were subtracted
        assert_equal(0.0, agent.cpus)
        assert_equal(72.0, agent.mem)
        assert_equal([4, 6], list(agent.cores))
        assert_equal(1000e6, agent.interfaces[0].bandwidth_in)
        assert_equal(1600e6, agent.interfaces[0].bandwidth_out)
        assert_equal(0.25, agent.gpus[0].compute)
        assert_equal(0.0, agent.gpus[0].mem)
        assert_equal(0.25, agent.gpus[1].compute)
        assert_equal(1024.0, agent.gpus[1].mem)


class TestPhysicalTask(object):
    """Tests for :class:`katsdpcontroller.scheduler.PhysicalTask`"""

    def setUp(self):
        self.logical_task = scheduler.LogicalTask('task')
        self.logical_task.cpus = 4.0
        self.logical_task.mem = 256.0
        self.logical_task.ports = ['port1', 'port2']
        self.logical_task.cores = ['core1', 'core2', 'core3']
        self.logical_task.interfaces = [
            scheduler.InterfaceRequest('net1'),
            scheduler.InterfaceRequest('net0')]
        self.logical_task.volumes = [scheduler.VolumeRequest('vol0', '/container-path', 'RW')]
        self.eth0 = scheduler.InterfaceResourceAllocation(0)
        self.eth0.bandwidth_in = 500e6
        self.eth0.bandwidth_out = 600e6
        self.eth1 = scheduler.InterfaceResourceAllocation(1)
        self.eth1.bandwidth_in = 300e6
        self.eth1.bandwidth_out = 200e6
        self.vol0 = scheduler.Volume('vol0', '/host0', numa_node=1)
        attributes = [
            _make_json_attr('katsdpcontroller.interfaces', [
                {"name": "eth0", "network": "net0", "ipv4_address": "192.168.1.1"},
                {"name": "eth1", "network": "net1", "ipv4_address": "192.168.2.1"}
            ]),
            _make_json_attr('katsdpcontroller.volumes',
                [{"name": "vol0", "host_path": "/host0", "numa_node": 1}])
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
        self.allocation = scheduler.ResourceAllocation(agent)
        self.allocation.cpus = self.logical_task.cpus
        self.allocation.mem = self.logical_task.mem
        self.allocation.ports = [30000, 30001]
        self.allocation.cores = [1, 2, 3]
        self.allocation.interfaces = [self.eth1, self.eth0]
        self.allocation.volumes = [self.vol0]

    def test_properties_init(self):
        """Resolved properties are ``None`` on construction"""
        physical_task = scheduler.PhysicalTask(self.logical_task, mock.sentinel.loop)
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
        physical_task = scheduler.PhysicalTask(self.logical_task, mock.sentinel.loop)
        physical_task.allocate(self.allocation)
        assert_is(self.allocation.agent, physical_task.agent)
        assert_equal('agenthost', physical_task.host)
        assert_equal('agentid', physical_task.agent_id)
        assert_is(self.allocation, physical_task.allocation)
        assert_in('net0', physical_task.interfaces)
        assert_equal('eth0', physical_task.interfaces['net0'].name)
        assert_equal(ipaddress.IPv4Address(u'192.168.1.1'),
                     physical_task.interfaces['net0'].ipv4_address)
        assert_in('net1', physical_task.interfaces)
        assert_equal('eth1', physical_task.interfaces['net1'].name)
        assert_equal(ipaddress.IPv4Address(u'192.168.2.1'),
                     physical_task.interfaces['net1'].ipv4_address)
        assert_equal({}, physical_task.endpoints)
        assert_equal({'port1': 30000, 'port2': 30001}, physical_task.ports)
        assert_equal({'core1': 1, 'core2': 2, 'core3': 3}, physical_task.cores)


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
        framework_id = 'frameworkid'
        numa_attr = _make_json_attr('katsdpcontroller.numa', [[0, 2, 4, 6], [1, 3, 5, 7]])
        gpu_attr = _make_json_attr(
            'katsdpcontroller.gpus',
            [{'devices': ['/dev/nvidia0', '/dev/nvidiactl', '/dev/nvidia-uvm'], 'driver_version': '123.45', 'name': 'Dummy GPU', 'device_attributes': {}, 'compute_capability': (5, 2), 'numa_node': 1}])
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
        self.physical_task = self.logical_task.physical_factory(
            self.logical_task, mock.sentinel.loop)
        self.logical_task2 = scheduler.LogicalTask('logical2')
        self.physical_task2 = self.logical_task2.physical_factory(
            self.logical_task2, mock.sentinel.loop)

    def test_task_insufficient_scalar_resource(self):
        """A task requests more of a scalar resource than any agent has"""
        self.logical_task.cpus = 4
        with self.assertRaises(scheduler.TaskInsufficientResourcesError) as cm:
            scheduler.Scheduler._diagnose_insufficient(
                [self.mem_agent, self.disk_agent], [self.physical_task])
        self.assertIs(self.physical_task, cm.exception.node)
        self.assertEqual('cpus', cm.exception.resource)
        self.assertEqual(4, cm.exception.needed)
        self.assertEqual(1.5, cm.exception.available)

    def test_task_insufficient_range_resource(self):
        """A task requests more of a range resource than any agent has"""
        self.logical_task.ports = ['a', 'b', 'c']
        with self.assertRaises(scheduler.TaskInsufficientResourcesError) as cm:
            scheduler.Scheduler._diagnose_insufficient(
                [self.mem_agent, self.disk_agent], [self.physical_task])
        self.assertIs(self.physical_task, cm.exception.node)
        self.assertEqual('ports', cm.exception.resource)
        self.assertEqual(3, cm.exception.needed)
        self.assertEqual(0, cm.exception.available)

    def test_task_insufficient_cores(self):
        """A task requests more cores than are available on a single NUMA node"""
        self.logical_task.cores = ['a', 'b', 'c', 'd']
        with self.assertRaises(scheduler.TaskInsufficientResourcesError) as cm:
            scheduler.Scheduler._diagnose_insufficient(
                [self.mem_agent, self.cores_agent], [self.physical_task])
        self.assertIs(self.physical_task, cm.exception.node)
        self.assertEqual('cores', cm.exception.resource)
        self.assertEqual(4, cm.exception.needed)
        self.assertEqual(3, cm.exception.available)

    def test_task_insufficient_gpu_scalar_resource(self):
        """A task requests more of a GPU scalar resource than any agent has"""
        req = scheduler.GPURequest()
        req.mem = 2048
        self.logical_task.gpus = [req]
        with self.assertRaises(scheduler.TaskInsufficientGPUResourcesError) as cm:
            scheduler.Scheduler._diagnose_insufficient(
                [self.mem_agent, self.gpu_compute_agent], [self.physical_task])
        self.assertIs(self.physical_task, cm.exception.node)
        self.assertEqual(0, cm.exception.request_index)
        self.assertEqual('mem', cm.exception.resource)
        self.assertEqual(2048, cm.exception.needed)
        self.assertEqual(2.25, cm.exception.available)

    def test_task_insufficient_interface_scalar_resources(self):
        """A task requests more of an interface scalar resource than any agent has"""
        req = scheduler.InterfaceRequest('net0')
        req.bandwidth_in = 5e9
        self.logical_task.interfaces = [req]
        with self.assertRaises(scheduler.TaskInsufficientInterfaceResourcesError) as cm:
            scheduler.Scheduler._diagnose_insufficient(
                [self.mem_agent, self.interface_agent], [self.physical_task])
        self.assertIs(self.physical_task, cm.exception.node)
        self.assertEqual(req, cm.exception.request)
        self.assertEqual('bandwidth_in', cm.exception.resource)
        self.assertEqual(5e9, cm.exception.needed)
        self.assertEqual(1e9, cm.exception.available)

    def test_task_no_interface(self):
        """A task requests a network interface that is not available on any agent"""
        self.logical_task.interfaces = [
            scheduler.InterfaceRequest('net0'),
            scheduler.InterfaceRequest('badnet')
        ]
        with self.assertRaises(scheduler.TaskNoInterfaceError) as cm:
            scheduler.Scheduler._diagnose_insufficient(
                [self.mem_agent, self.interface_agent], [self.physical_task])
        self.assertIs(self.physical_task, cm.exception.node)
        self.assertIs(self.logical_task.interfaces[1], cm.exception.request)

    def test_task_no_volume(self):
        """A task requests a volume that is not available on any agent"""
        self.logical_task.volumes = [
            scheduler.VolumeRequest('vol0', '/vol0', 'RW'),
            scheduler.VolumeRequest('badvol', '/badvol', 'RO')
        ]
        with self.assertRaises(scheduler.TaskNoVolumeError) as cm:
            scheduler.Scheduler._diagnose_insufficient(
                [self.mem_agent, self.volume_agent], [self.physical_task])
        self.assertIs(self.physical_task, cm.exception.node)
        self.assertIs(self.logical_task.volumes[1], cm.exception.request)

    def test_task_no_gpu(self):
        """A task requests a GPU that is not available on any agent"""
        req = scheduler.GPURequest()
        req.name = 'GPU that does not exist'
        self.logical_task.gpus = [req]
        with self.assertRaises(scheduler.TaskNoGPUError) as cm:
            scheduler.Scheduler._diagnose_insufficient(
                [self.mem_agent, self.gpu_compute_agent], [self.physical_task])
        self.assertIs(self.physical_task, cm.exception.node)
        self.assertEqual(0, cm.exception.request_index)

    def test_task_no_agent(self):
        """A task does not fit on any agent, but not due to a single reason"""
        # Ask for more combined cpu+ports than is available on one agent
        self.logical_task.cpus = 8
        self.logical_task.ports = ['a', 'b', 'c']
        with self.assertRaises(scheduler.TaskNoAgentError) as cm:
            scheduler.Scheduler._diagnose_insufficient(
                [self.cpus_agent, self.ports_agent], [self.physical_task])
        self.assertIs(self.physical_task, cm.exception.node)
        # Make sure that it didn't incorrectly return a subclass
        self.assertEqual(scheduler.TaskNoAgentError, type(cm.exception))

    def test_group_insufficient_scalar_resource(self):
        """A group of tasks require more of a scalar resource than available"""
        self.logical_task.cpus = 24
        self.logical_task2.cpus = 16
        with self.assertRaises(scheduler.GroupInsufficientResourcesError) as cm:
            scheduler.Scheduler._diagnose_insufficient(
                [self.cpus_agent, self.mem_agent], [self.physical_task, self.physical_task2])
        self.assertEqual('cpus', cm.exception.resource)
        self.assertEqual(40, cm.exception.needed)
        self.assertEqual(33.25, cm.exception.available)

    def test_group_insufficient_range_resource(self):
        """A group of tasks require more of a range resource than available"""
        self.logical_task.ports = ['a', 'b', 'c']
        self.logical_task2.ports = ['d', 'e', 'f']
        with self.assertRaises(scheduler.GroupInsufficientResourcesError) as cm:
            scheduler.Scheduler._diagnose_insufficient(
                [self.ports_agent], [self.physical_task, self.physical_task2])
        self.assertEqual('ports', cm.exception.resource)
        self.assertEqual(6, cm.exception.needed)
        self.assertEqual(5, cm.exception.available)

    def test_group_insufficient_gpu_scalar_resources(self):
        """A group of tasks require more of a GPU scalar resource than available"""
        self.logical_task.gpus = [scheduler.GPURequest()]
        self.logical_task.gpus[-1].compute = 0.75
        self.logical_task2.gpus = [scheduler.GPURequest()]
        self.logical_task2.gpus[-1].compute = 0.5
        with self.assertRaises(scheduler.GroupInsufficientGPUResourcesError) as cm:
            scheduler.Scheduler._diagnose_insufficient(
                [self.gpu_compute_agent, self.gpu_mem_agent],
                [self.physical_task, self.physical_task2])
        self.assertEqual('compute', cm.exception.resource)
        self.assertEqual(1.25, cm.exception.needed)
        self.assertEqual(1.125, cm.exception.available)

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
        with self.assertRaises(scheduler.GroupInsufficientInterfaceResourcesError) as cm:
            scheduler.Scheduler._diagnose_insufficient(
                [self.interface_agent],
                [self.physical_task, self.physical_task2])
        self.assertEqual('net0', cm.exception.network)
        self.assertEqual('bandwidth_in', cm.exception.resource)
        self.assertEqual(1500e6, cm.exception.needed)
        self.assertEqual(1000e6, cm.exception.available)

    def test_generic(self):
        """A group of tasks can't fit, but on simpler explanation is available"""
        # Create a tasks that uses just too much memory for the
        # low-memory agents, forcing them to consume memory from the
        # big-memory agent and not leaving enough for the big-memory task.
        self.logical_task.mem = 5
        self.logical_task2.mem = 251
        with self.assertRaises(scheduler.InsufficientResourcesError) as cm:
            scheduler.Scheduler._diagnose_insufficient(
                [self.cpus_agent, self.mem_agent, self.disk_agent],
                [self.physical_task, self.physical_task2])
        # Check that it wasn't a subclass raised
        self.assertEqual(scheduler.InsufficientResourcesError, type(cm.exception))


class TestScheduler(object):
    """Tests for :class:`katsdpcontroller.scheduler.Scheduler`."""
    def _make_offer(self, resources, agent_num=0, attrs=()):
        return _make_offer(self.framework_id, 'agentid{}'.format(agent_num),
                           'agenthost{}'.format(agent_num), resources, attrs)

    def _status_update(self, task_id, state):
        status = _make_status(task_id, state)
        self.sched.statusUpdate(self.driver, status)
        return status

    def _make_physical(self):
        self.physical_graph = scheduler.instantiate(self.logical_graph, self.loop)
        self.nodes = []
        for i in range(3):
            self.nodes.append(next(node for node in self.physical_graph
                                   if node.name == 'node{}'.format(i)))
        self.nodes[2].host = 'remotehost'
        self.nodes[2].ports['foo'] = 10000

    def setup(self):
        self.framework_id = 'frameworkid'
        # Normally TaskIDAllocator's constructor returns a singleton to keep
        # IDs globally unique, but we want the test to be isolated. Bypass its
        # __new__.
        self.image_resolver = scheduler.ImageResolver()
        self.task_id_allocator = object.__new__(scheduler.TaskIDAllocator)
        self.task_id_allocator._prefix = 'test-'
        self.task_id_allocator._next_id = 0
        self.resolver = scheduler.Resolver(self.image_resolver, self.task_id_allocator)
        node0 = scheduler.LogicalTask('node0')
        node0.cpus = 1.0
        node0.command = ['hello', '--port={ports[port]}']
        node0.ports = ['port']
        node0.image = 'image0'
        node0.gpus.append(scheduler.GPURequest())
        node0.gpus[-1].compute = 0.5
        node0.gpus[-1].mem = 256.0
        node0.interfaces = [scheduler.InterfaceRequest('net0', infiniband=True)]
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
        self.logical_graph = networkx.MultiDiGraph()
        self.logical_graph.add_nodes_from([node0, node1, node2])
        self.logical_graph.add_edge(node1, node0, port='port', order='strong')
        self.logical_graph.add_edge(node1, node2, port='foo', order='strong')
        self.numa_attr = _make_json_attr('katsdpcontroller.numa', [[0, 2, 4, 6], [1, 3, 5, 7]])
        self.agent0_attrs = [
            _make_json_attr('katsdpcontroller.gpus', [
                {'driver_version': '123.45',
                 'devices': ['/dev/nvidia0', '/dev/nvidiactl', '/dev/nvidia-uvm'],
                 'name': 'Dummy GPU', 'device_attributes': {}, 'compute_capability': (5, 2)},
                {'driver_version': '123.45',
                 'devices': ['/dev/nvidia1', '/dev/nvidiactl', '/dev/nvidia-uvm'],
                 'name': 'Dummy GPU', 'device_attributes': {}, 'compute_capability': (5, 2)}
            ]),
            _make_json_attr('katsdpcontroller.volumes', [
                {'name': 'vol0', 'host_path': '/host0'}
            ]),
            _make_json_attr('katsdpcontroller.interfaces', [
                {'name': 'eth0', 'network': 'net0', 'ipv4_address': '192.168.1.1',
                 'infiniband_devices': ['/dev/infiniband/rdma_cm', '/dev/infiniband/uverbs0']}]),
            self.numa_attr
        ]
        self.loop = trollius.new_event_loop()
        try:
            self._make_physical()
            self.sched = scheduler.Scheduler(self.loop)
            self.driver = mock.create_autospec(pymesos.MesosSchedulerDriver,
                                               spec_set=True, instance=True)
            self.sched.set_driver(self.driver)
            self.sched.registered(self.driver, 'framework', mock.sentinel.master_info)
        except Exception:
            self.loop.close()
            raise

    def teardown(self):
        self.loop.close()

    @run_with_self_event_loop
    def test_initial_offers(self):
        """Offers passed to resourcesOffers in initial state are declined"""
        offers = [
            self._make_offer({'cpus': 2.0}, 0),
            self._make_offer({'cpus': 1.0}, 1),
            self._make_offer({'cpus': 1.5}, 0)
        ]
        self.sched.resourceOffers(self.driver, offers)
        yield From(defer(loop=self.loop))
        assert_equal(
            AnyOrderList([
                mock.call.acceptOffers(AnyOrderList([offers[0].id, offers[2].id]), []),
                mock.call.acceptOffers([offers[1].id], []),
                mock.call.suppressOffers()
            ]), self.driver.mock_calls)

    @run_with_self_event_loop
    def test_launch_cycle(self):
        """Launch raises CycleError if there is a cycle with a strong edge in the graph"""
        nodes = [scheduler.LogicalExternal('node{}'.format(i)) for i in range(4)]
        logical_graph = networkx.MultiDiGraph()
        logical_graph.add_nodes_from(nodes)
        logical_graph.add_edge(nodes[1], nodes[0], order='strong')
        logical_graph.add_edge(nodes[1], nodes[2], order='strong')
        logical_graph.add_edge(nodes[2], nodes[3])
        logical_graph.add_edge(nodes[3], nodes[1])
        physical_graph = scheduler.instantiate(logical_graph, self.loop)
        with assert_raises(scheduler.CycleError):
            yield From(self.sched.launch(physical_graph, self.resolver))

    @run_with_self_event_loop
    def test_launch_omit_dependency(self):
        """Launch raises DependencyError if launching a subset of nodes that
        depends on a node that is outside the set and not running.
        """
        nodes = [scheduler.LogicalExternal('node{}'.format(i)) for i in range(2)]
        logical_graph = networkx.MultiDiGraph()
        logical_graph.add_nodes_from(nodes)
        logical_graph.add_edge(nodes[1], nodes[0])
        physical_graph = scheduler.instantiate(logical_graph, self.loop)
        target = [node for node in physical_graph if node.name == 'node1']
        with assert_raises(scheduler.DependencyError):
            yield From(self.sched.launch(physical_graph, self.resolver, target))

    @run_with_self_event_loop
    def test_launch_closing(self):
        """Launch raises trollius.InvalidStateError if close has been called"""
        yield From(self.sched.close())
        with assert_raises(trollius.InvalidStateError):
            # Timeout is just to ensure the test won't hang
            yield From(trollius.wait_for(self.sched.launch(self.physical_graph, self.resolver),
                                         timeout=1, loop=self.loop))

    @run_with_self_event_loop
    def test_launch_serial(self):
        """Test launch on the success path, with no concurrent calls."""
        # TODO: still need to extend this to test:
        # - custom wait_ports
        numa_attr = _make_json_attr('katsdpcontroller.numa', [[0, 2, 4, 6], [1, 3, 5, 7]])
        offer0 = self._make_offer({
            'cpus': 2.0, 'mem': 1024.0, 'ports': [(30000, 31000)],
            'katsdpcontroller.gpu.0.compute': 0.25,
            'katsdpcontroller.gpu.0.mem': 2048.0,
            'katsdpcontroller.gpu.1.compute': 1.0,
            'katsdpcontroller.gpu.1.mem': 1024.0,
            'katsdpcontroller.interface.0.bandwidth_in': 1e9,
            'katsdpcontroller.interface.0.bandwidth_out': 1e9
        }, 0, self.agent0_attrs)
        offer1 = self._make_offer({
            'cpus': 0.5, 'mem': 128.0, 'ports': [(31000, 32000)],
            'cores': [(0, 8)]
        }, 1, [self.numa_attr])
        expected_taskinfo0 = Dict()
        expected_taskinfo0.name = 'node0'
        expected_taskinfo0.task_id.value = 'test-00000000'
        expected_taskinfo0.agent_id.value = 'agentid0'
        expected_taskinfo0.command.shell = False
        expected_taskinfo0.command.value = 'hello'
        expected_taskinfo0.command.arguments = ['--port=30000']
        expected_taskinfo0.container.type = 'DOCKER'
        expected_taskinfo0.container.docker.image = 'sdp/image0:latest'
        expected_taskinfo0.container.docker.parameters = AnyOrderList([
            Dict(key='device', value='/dev/nvidia1'),
            Dict(key='device', value='/dev/nvidiactl'),
            Dict(key='device', value='/dev/nvidia-uvm'),
            Dict(key='ulimit', value='memlock=-1'),
            Dict(key='device', value='/dev/infiniband/rdma_cm'),
            Dict(key='device', value='/dev/infiniband/uverbs0')
        ])
        volume_gpu = Dict()
        volume_gpu.mode = 'RO'
        volume_gpu.container_path = '/usr/local/nvidia'
        volume_gpu.source.type = 'DOCKER_VOLUME'
        volume_gpu.source.docker_volume.driver = 'nvidia-docker'
        volume_gpu.source.docker_volume.name = 'nvidia_driver_123.45'
        volume = Dict()
        volume.mode = 'RW'
        volume.host_path = '/host0'
        volume.container_path = '/container-path'
        expected_taskinfo0.container.volumes = AnyOrderList([volume_gpu, volume])
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
        expected_taskinfo1.task_id.value = 'test-00000001'
        expected_taskinfo1.agent_id.value = 'agentid1'
        expected_taskinfo1.command.shell = False
        expected_taskinfo1.command.value = 'test'
        expected_taskinfo1.command.arguments = [
            '--host=agenthost1', '--remote=agenthost0:30000',
            '--another=remotehost:10000']
        expected_taskinfo1.container.type = 'DOCKER'
        expected_taskinfo1.container.docker.image = 'sdp/image1:latest'
        expected_taskinfo1.container.docker.parameters = [{'key': 'cpuset-cpus', 'value': '0,2'}]
        expected_taskinfo1.resources = _make_resources({'cpus': 0.5, 'cores': [(0, 1), (2, 3)]})
        expected_taskinfo1.discovery.visibility = 'EXTERNAL'
        expected_taskinfo1.discovery.name = 'node1'
        expected_taskinfo1.discovery.ports.ports = []

        launch = trollius.async(self.sched.launch(
            self.physical_graph, self.resolver), loop=self.loop)
        yield From(defer(loop=self.loop))
        # The tasks must be in state STARTING, but not yet RUNNING because
        # there are no offers.
        for node in self.nodes:
            assert_equal(TaskState.STARTING, node.state)
            assert_false(node.ready_event.is_set())
            assert_false(node.dead_event.is_set())
        assert_equal([mock.call.reviveOffers()], self.driver.mock_calls)
        self.driver.reset_mock()
        # Now provide an offer that is suitable for node1 but not node0.
        # Nothing should happen
        self.sched.resourceOffers(self.driver, [offer1])
        yield From(defer(loop=self.loop))
        assert_equal([], self.driver.mock_calls)
        self.driver.reset_mock()
        # Provide offer suitable for launching node0. At this point nodes 0 and 2
        # should launch. The offers are suppressed while we wait for node 0 to come
        # up.
        self.sched.resourceOffers(self.driver, [offer0])
        yield From(defer(loop=self.loop))
        assert_equal(expected_taskinfo0, self.nodes[0].taskinfo)
        assert_equal('agenthost0', self.nodes[0].host)
        assert_equal('agentid0', self.nodes[0].agent_id)
        assert_equal({'port': 30000}, self.nodes[0].ports)
        assert_equal({}, self.nodes[0].cores)
        assert_is_none(self.nodes[0].status)
        assert_equal(TaskState.STARTED, self.nodes[0].state)
        assert_equal(TaskState.READY, self.nodes[2].state)
        assert_equal(AnyOrderList([
            mock.call.launchTasks([offer0.id], [expected_taskinfo0]),
            mock.call.launchTasks([offer1.id], []),
            mock.call.suppressOffers()
        ]), self.driver.mock_calls)
        self.driver.reset_mock()
        # Tell scheduler that node0 is now running. This will start up the
        # the waiter, so we need to mock poll_ports.
        with mock.patch.object(scheduler, 'poll_ports', autospec=True) as poll_ports:
            poll_future = trollius.Future(self.loop)
            poll_ports.return_value = poll_future
            status = self._status_update('test-00000000', 'TASK_RUNNING')
            yield From(defer(loop=self.loop))
            assert_equal(TaskState.RUNNING, self.nodes[0].state)
            assert_equal(status, self.nodes[0].status)
            assert_equal([mock.call.acknowledgeStatusUpdate(status)],
                         self.driver.mock_calls)
            self.driver.reset_mock()
            poll_ports.assert_called_once_with('agenthost0', [30000], self.loop)
        # Make poll_ports ready. Node 0 should now become ready, and offers
        # should be revived to get resources for node 1.
        poll_future.set_result(None)
        yield From(defer(loop=self.loop))
        assert_equal(TaskState.READY, self.nodes[0].state)
        self.driver.reset_mock()
        # Now provide an offer suitable for node 1.
        self.sched.resourceOffers(self.driver, [offer1])
        yield From(defer(loop=self.loop))
        assert_equal(TaskState.STARTED, self.nodes[1].state)
        assert_equal(expected_taskinfo1, self.nodes[1].taskinfo)
        assert_equal('agentid1', self.nodes[1].agent_id)
        assert_equal([
            mock.call.launchTasks([offer1.id], [expected_taskinfo1]),
            mock.call.suppressOffers()], self.driver.mock_calls)
        self.driver.reset_mock()
        # Finally, tell the scheduler that node 1 is running. There are no
        # ports, so it will go straight to READY.
        status = self._status_update('test-00000001', 'TASK_RUNNING')
        yield From(defer(loop=self.loop))
        assert_equal(TaskState.READY, self.nodes[1].state)
        assert_equal(status, self.nodes[1].status)
        assert_equal([mock.call.acknowledgeStatusUpdate(status)],
                     self.driver.mock_calls)
        self.driver.reset_mock()
        assert_true(launch.done())
        yield From(launch)

    @trollius.coroutine
    def _transition_node0(self, target_state, nodes=None, ports=None, task_id='test-00000000'):
        """Launch the graph and proceed until node0 is in `target_state`.

        This is intended to be used in test setup. It is assumed that this
        functionality is more fully tested test_launch_serial, so minimal
        assertions are made.

        Returns
        -------
        launch, kill : :class:`trollius.Task`
            Asynchronous tasks for launching and killing the graph. If
            `target_state` is not :const:`TaskState.KILLED` or
            :const:`TaskState.DEAD`, then `kill` is ``None``
        """
        assert target_state > TaskState.NOT_READY
        if ports is None:
            ports = [(30000, 31000)]
        offer = self._make_offer({
            'cpus': 2.0, 'mem': 1024.0, 'ports': ports,
            'katsdpcontroller.gpu.0.compute': 0.25,
            'katsdpcontroller.gpu.0.mem': 2048.0,
            'katsdpcontroller.gpu.1.compute': 1.0,
            'katsdpcontroller.gpu.1.mem': 1024.0,
            'katsdpcontroller.interface.0.bandwidth_in': 1e9,
            'katsdpcontroller.interface.0.bandwidth_out': 1e9
        }, 0, self.agent0_attrs)
        launch = trollius.async(self.sched.launch(self.physical_graph, self.resolver, nodes),
                                loop=self.loop)
        kill = None
        yield From(defer(loop=self.loop))
        assert_equal(TaskState.STARTING, self.nodes[0].state)
        with mock.patch.object(scheduler, 'poll_ports', autospec=True) as poll_ports:
            poll_future = trollius.Future(self.loop)
            poll_ports.return_value = poll_future
            if target_state > TaskState.STARTING:
                self.sched.resourceOffers(self.driver, [offer])
                yield From(defer(loop=self.loop))
                assert_equal(TaskState.STARTED, self.nodes[0].state)
                if target_state > TaskState.STARTED:
                    self._status_update(task_id, 'TASK_RUNNING')
                    yield From(defer(loop=self.loop))
                    assert_equal(TaskState.RUNNING, self.nodes[0].state)
                    if target_state > TaskState.RUNNING:
                        poll_future.set_result(None)   # Mark ports as ready
                        yield From(defer(loop=self.loop))
                        assert_equal(TaskState.READY, self.nodes[0].state)
                        if target_state > TaskState.READY:
                            kill = trollius.async(self.sched.kill(
                                self.physical_graph, nodes), loop=self.loop)
                            yield From(defer(loop=self.loop))
                            assert_equal(TaskState.KILLED, self.nodes[0].state)
                            if target_state > TaskState.KILLED:
                                self._status_update(task_id, 'TASK_KILLED')
                            yield From(defer(loop=self.loop))
        self.driver.reset_mock()
        assert_equal(target_state, self.nodes[0].state)
        raise Return((launch, kill))

    @trollius.coroutine
    def _ready_graph(self):
        """Gets the whole graph to READY state"""
        launch, kill = yield From(self._transition_node0(TaskState.READY))
        offer = self._make_offer(
            {'cpus': 0.5, 'mem': 128.0, 'ports': [(31000, 32000)], 'cores': [(0, 8)]}, 1,
            [_make_json_attr('katsdpcontroller.numa', [[0, 2, 4, 6], [1, 3, 5, 7]])])
        self.sched.resourceOffers(self.driver, [offer])
        yield From(defer(loop=self.loop))
        self._status_update('test-00000001', 'TASK_RUNNING')
        yield From(defer(loop=self.loop))
        assert_true(launch.done())  # Ensures the next line won't hang the test
        yield From(launch)
        self.driver.reset_mock()

    @run_with_self_event_loop
    def test_launch_port_recycle(self):
        """Tests that ports are recycled only when necessary"""
        ports = [(30000, 30002)]
        yield From(self._transition_node0(TaskState.DEAD, [self.nodes[0]], ports=ports))
        assert_equal(30000, self.nodes[0].ports['port'])
        # Build a new physical graph
        self._make_physical()
        yield From(self._transition_node0(TaskState.DEAD, [self.nodes[0]], ports=ports,
                                          task_id='test-00000001'))
        assert_equal(30001, self.nodes[0].ports['port'])
        # Do it again, check that it cycles back to the start
        self._make_physical()
        yield From(self._transition_node0(TaskState.DEAD, [self.nodes[0]], ports=ports,
                                          task_id='test-00000002'))
        assert_equal(30000, self.nodes[0].ports['port'])

    @trollius.coroutine
    def _test_launch_cancel(self, target_state):
        launch, kill = yield From(self._transition_node0(target_state))
        assert_equal(TaskState.STARTING, self.nodes[1].state)
        assert_false(launch.done())
        # Now cancel and check that node1 goes back to NOT_READY while
        # the others retain their state.
        launch.cancel()
        yield From(defer(loop=self.loop))
        assert_equal(target_state, self.nodes[0].state)
        assert_equal(TaskState.NOT_READY, self.nodes[1].state)
        assert_equal(TaskState.READY, self.nodes[2].state)
        if target_state == TaskState.READY:
            assert_equal([mock.call.suppressOffers()], self.driver.mock_calls)
        else:
            assert_equal([], self.driver.mock_calls)

    @run_with_self_event_loop
    def test_launch_cancel_wait_task(self):
        """Test cancelling a launch while waiting for a task to become READY"""
        yield From(self._test_launch_cancel(TaskState.RUNNING))

    @run_with_self_event_loop
    def test_launch_cancel_wait_resource(self):
        """Test cancelling a launch while waiting for resources"""
        yield From(self._test_launch_cancel(TaskState.READY))

    @run_with_self_event_loop
    def test_launch_resources_timeout(self):
        """Test a launch failing due to insufficient resources within the timeout"""
        self.sched.resources_timeout = 0.001
        launch, kill = yield From(self._transition_node0(TaskState.STARTING))
        with assert_raises(scheduler.InsufficientResourcesError):
            yield From(launch)
        assert_equal(TaskState.NOT_READY, self.nodes[0].state)
        assert_equal(TaskState.NOT_READY, self.nodes[1].state)
        assert_equal(TaskState.NOT_READY, self.nodes[2].state)
        # Once we abort, we should no longer be interested in offers
        assert_equal([mock.call.suppressOffers()], self.driver.mock_calls)

    @run_with_self_event_loop
    def test_offer_rescinded(self):
        """Test offerRescinded"""
        launch, kill = yield From(self._transition_node0(TaskState.STARTING, [self.nodes[0]]))
        # Provide an offer that is insufficient
        offer0 = self._make_offer({'cpus': 0.5, 'mem': 128.0, 'ports': [(31000, 32000)]}, 1)
        self.sched.resourceOffers(self.driver, [offer0])
        yield From(defer(loop=self.loop))
        assert_equal(TaskState.STARTING, self.nodes[0].state)
        # Rescind the offer
        self.sched.offerRescinded(self.driver, offer0.id)
        # Make a new offer, which is also insufficient, but which with the
        # original one would have been sufficient.
        offer1 = self._make_offer({'cpus': 0.8, 'mem': 128.0, 'ports': [(31000, 32000)]}, 1)
        self.sched.resourceOffers(self.driver, [offer1])
        yield From(defer(loop=self.loop))
        assert_equal(TaskState.STARTING, self.nodes[0].state)
        # Rescind an unknown offer. This can happen if an offer was accepted at
        # the same time as it was rescinded.
        offer2 = self._make_offer({'cpus': 0.8, 'mem': 128.0, 'ports': [(31000, 32000)]}, 1)
        self.sched.offerRescinded(self.driver, offer2.id)
        yield From(defer(loop=self.loop))
        assert_equal([], self.driver.mock_calls)
        launch.cancel()

    @trollius.coroutine
    def _test_kill_in_state(self, state):
        """Test killing a node while it is in the given state"""
        launch, kill = yield From(self._transition_node0(state, [self.nodes[0]]))
        kill = trollius.async(self.sched.kill(self.physical_graph, [self.nodes[0]]),
                              loop=self.loop)
        yield From(defer(loop=self.loop))
        if state > TaskState.STARTING:
            assert_equal(TaskState.KILLED, self.nodes[0].state)
            status = self._status_update('test-00000000', 'TASK_KILLED')
            yield From(defer(loop=self.loop))
            assert_is(status, self.nodes[0].status)
        assert_equal(TaskState.DEAD, self.nodes[0].state)
        yield From(launch)
        yield From(kill)

    @run_with_self_event_loop
    def test_kill_while_starting(self):
        """Test killing a node while in state STARTING"""
        yield From(self._test_kill_in_state(TaskState.STARTING))

    @run_with_self_event_loop
    def test_kill_while_started(self):
        """Test killing a node while in state STARTED"""
        yield From(self._test_kill_in_state(TaskState.STARTED))

    @run_with_self_event_loop
    def test_kill_while_running(self):
        """Test killing a node while in state RUNNING"""
        yield From(self._test_kill_in_state(TaskState.RUNNING))

    @run_with_self_event_loop
    def test_kill_while_ready(self):
        """Test killing a node while in state READY"""
        yield From(self._test_kill_in_state(TaskState.READY))

    @run_with_self_event_loop
    def test_kill_while_killed(self):
        """Test killing a node while in state KILLED"""
        yield From(self._test_kill_in_state(TaskState.KILLED))

    @trollius.coroutine
    def _test_die_in_state(self, state):
        """Test a node dying on its own while it is in the given state"""
        launch, kill = yield From(self._transition_node0(state, [self.nodes[0]]))
        status = self._status_update('test-00000000', 'TASK_FINISHED')
        yield From(defer(loop=self.loop))
        assert_is(status, self.nodes[0].status)
        assert_equal(TaskState.DEAD, self.nodes[0].state)
        assert_true(self.nodes[0].ready_event.is_set())
        assert_true(self.nodes[0].dead_event.is_set())
        yield From(launch)

    @run_with_self_event_loop
    def test_die_while_started(self):
        """Test a process dying on its own while in state STARTED"""
        yield From(self._test_die_in_state(TaskState.STARTED))

    @run_with_self_event_loop
    def test_die_while_running(self):
        """Test a process dying on its own while in state RUNNING"""
        yield From(self._test_die_in_state(TaskState.RUNNING))

    @run_with_self_event_loop
    def test_die_while_ready(self):
        """Test a process dying on its own while in state READY"""
        yield From(self._test_die_in_state(TaskState.READY))

    @run_with_self_event_loop
    def test_kill_order(self):
        """Kill must respect dependency ordering"""
        yield From(self._ready_graph())
        # Now kill it. node1 must be dead before node0, node2 get killed
        kill = trollius.async(self.sched.kill(self.physical_graph), loop=self.loop)
        yield From(defer(loop=self.loop))
        assert_equal([mock.call.killTask(self.nodes[1].taskinfo.task_id)],
                     self.driver.mock_calls)
        assert_equal(TaskState.READY, self.nodes[0].state)
        assert_equal(TaskState.KILLED, self.nodes[1].state)
        assert_equal(TaskState.READY, self.nodes[2].state)
        self.driver.reset_mock()
        # node1 now dies, and node0 and node2 should be killed
        status = self._status_update('test-00000001', 'TASK_KILLED')
        yield From(defer(loop=self.loop))
        assert_equal(AnyOrderList([
            mock.call.killTask(self.nodes[0].taskinfo.task_id),
            mock.call.acknowledgeStatusUpdate(status)]),
            self.driver.mock_calls)
        assert_equal(TaskState.KILLED, self.nodes[0].state)
        assert_equal(TaskState.DEAD, self.nodes[1].state)
        assert_equal(TaskState.DEAD, self.nodes[2].state)
        assert_false(kill.done())
        # node0 now dies, to finish the cleanup
        self._status_update('test-00000000', 'TASK_KILLED')
        yield From(defer(loop=self.loop))
        assert_equal(TaskState.DEAD, self.nodes[0].state)
        assert_equal(TaskState.DEAD, self.nodes[1].state)
        assert_equal(TaskState.DEAD, self.nodes[2].state)
        assert_true(kill.done())
        yield From(kill)

    @run_with_self_event_loop
    def test_close(self):
        """Close must kill off all remaining tasks and abort any pending launches"""
        yield From(self._ready_graph())
        # Start launching a second graph, but do not give it resources
        physical_graph2 = scheduler.instantiate(self.logical_graph, self.loop)
        launch = trollius.async(self.sched.launch(physical_graph2, self.resolver), loop=self.loop)
        yield From(defer(loop=self.loop))
        close = trollius.async(self.sched.close(), loop=self.loop)
        yield From(defer(loop=self.loop))
        status1 = self._status_update('test-00000001', 'TASK_KILLED')
        yield From(defer(loop=self.loop))
        status0 = self._status_update('test-00000000', 'TASK_KILLED')
        # defer is insufficient here, because close() uses run_in_executor to
        # join the driver thread. Wait up to 5 seconds for that to happen.
        yield From(trollius.wait_for(close, 5, loop=self.loop))
        for node in self.physical_graph:
            assert_equal(TaskState.DEAD, node.state)
        for node in physical_graph2:
            assert_equal(TaskState.DEAD, node.state)
        # The timing of suppressOffers is undefined, because it depends on the
        # order in which the graphs are killed. However, it must occur
        # after the initial reviveOffers and before stopping the driver.
        assert_in(mock.call.suppressOffers(), self.driver.mock_calls)
        pos = self.driver.mock_calls.index(mock.call.suppressOffers())
        assert_true(1 <= pos < len(self.driver.mock_calls) - 2)
        del self.driver.mock_calls[pos]
        assert_equal([
            mock.call.reviveOffers(),
            mock.call.killTask(self.nodes[1].taskinfo.task_id),
            mock.call.acknowledgeStatusUpdate(status1),
            mock.call.killTask(self.nodes[0].taskinfo.task_id),
            mock.call.acknowledgeStatusUpdate(status0),
            mock.call.stop(),
            mock.call.join()
            ], self.driver.mock_calls)
        assert_true(launch.done())
        yield From(launch)

    @run_with_self_event_loop
    def test_status_unknown_task_id(self):
        """statusUpdate must correctly handle an unknown task ID"""
        self._status_update('test-01234567', 'TASK_LOST')

    @run_with_self_event_loop
    def test_get_master_and_slaves(self):
        with requests_mock.mock(case_sensitive=True) as rmock:
            # An actual response scraped from a real Mesos server
            rmock.get('http://master.invalid:5050/slaves',
                      text=r'{"slaves":[{"id":"001fe2cf-cd21-464e-9b38-e043535aa29e-S13","pid":"slave(1)@192.168.6.198:5051","hostname":"192.168.6.198","registered_time":1485252612.46216,"resources":{"disk":34080.0,"mem":15023.0,"gpus":0.0,"cpus":4.0,"ports":"[31000-32000]"},"used_resources":{"disk":0.0,"mem":0.0,"gpus":0.0,"cpus":0.0},"offered_resources":{"disk":0.0,"mem":0.0,"gpus":0.0,"cpus":0.0},"reserved_resources":{},"unreserved_resources":{"disk":34080.0,"mem":15023.0,"gpus":0.0,"cpus":4.0,"ports":"[31000-32000]"},"attributes":{},"active":true,"version":"1.1.0","reserved_resources_full":{},"used_resources_full":[],"offered_resources_full":[]},{"id":"001fe2cf-cd21-464e-9b38-e043535aa29e-S12","pid":"slave(1)@192.168.6.188:5051","hostname":"192.168.6.188","registered_time":1485252591.10345,"resources":{"disk":34080.0,"mem":15023.0,"gpus":0.0,"cpus":4.0,"ports":"[31000-32000]"},"used_resources":{"disk":0.0,"mem":0.0,"gpus":0.0,"cpus":0.0},"offered_resources":{"disk":0.0,"mem":0.0,"gpus":0.0,"cpus":0.0},"reserved_resources":{},"unreserved_resources":{"disk":34080.0,"mem":15023.0,"gpus":0.0,"cpus":4.0,"ports":"[31000-32000]"},"attributes":{},"active":true,"version":"1.1.0","reserved_resources_full":{},"used_resources_full":[],"offered_resources_full":[]},{"id":"001fe2cf-cd21-464e-9b38-e043535aa29e-S11","pid":"slave(1)@192.168.6.206:5051","hostname":"192.168.6.206","registered_time":1485252564.45196,"resources":{"disk":34080.0,"mem":15023.0,"gpus":0.0,"cpus":4.0,"ports":"[31000-32000]"},"used_resources":{"disk":0.0,"mem":0.0,"gpus":0.0,"cpus":0.0},"offered_resources":{"disk":0.0,"mem":0.0,"gpus":0.0,"cpus":0.0},"reserved_resources":{},"unreserved_resources":{"disk":34080.0,"mem":15023.0,"gpus":0.0,"cpus":4.0,"ports":"[31000-32000]"},"attributes":{},"active":true,"version":"1.1.0","reserved_resources_full":{},"used_resources_full":[],"offered_resources_full":[]}]}')
            self.driver.master = 'master.invalid:5050'
            master, slaves = yield From(self.sched.get_master_and_slaves())
            assert_equal('master.invalid', master)
            assert_equal(AnyOrderList(['192.168.6.198', '192.168.6.188', '192.168.6.206']), slaves)

    @run_with_self_event_loop
    def test_get_master_and_slaves_connect_failed(self):
        # Guaranteed not to be a valid domain name (RFC2606)
        self.driver.master = 'example.invalid:5050'
        with assert_raises(requests.exceptions.RequestException):
            yield From(self.sched.get_master_and_slaves())

    @run_with_self_event_loop
    def test_get_master_and_slaves_bad_response(self):
        with requests_mock.mock(case_sensitive=True) as rmock:
            rmock.get('http://master.invalid:5050/slaves', text='', status_code=404)
            self.driver.master = 'master.invalid:5050'
            with assert_raises(requests.exceptions.RequestException):
                yield From(self.sched.get_master_and_slaves())

    @run_with_self_event_loop
    def test_get_master_and_slaves_bad_json(self):
        responses = [
            '{not valid json',
            '["not a dict"]',
            '{"no_slaves": 4}',
            '{"slaves": "not an array"}']
        for response in responses:
            with requests_mock.mock(case_sensitive=True) as rmock:
                rmock.get('http://master.invalid:5050/slaves', text=response)
                self.driver.master = 'master.invalid:5050'
                with assert_raises(ValueError):
                    yield From(self.sched.get_master_and_slaves())
