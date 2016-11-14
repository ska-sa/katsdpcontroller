from __future__ import print_function, division, absolute_import
import mock
from nose.tools import *
from katsdpcontroller import scheduler
from katsdpcontroller.scheduler import TaskState
from mesos.interface import mesos_pb2
import uuid
import ipaddress
import logging
import six
import contextlib
import functools
import socket
import trollius
from trollius import From
try:
    import unittest2 as unittest
except ImportError:
    import unittest


def run_with_event_loop(func):
    func = trollius.coroutine(func)
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        loop = trollius.new_event_loop()
        with contextlib.closing(loop):
            args2 = args + (loop,)
            loop.run_until_complete(func(*args2, **kwargs))
    return wrapper


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
        resource = mesos_pb2.Resource()
        range1 = resource.ranges.range.add()
        range1.begin = 5
        range1.end = 7
        range2 = resource.ranges.range.add()
        range2.begin = 20
        range2.end = 29
        rr.add_resource(resource)
        assert_equal([(5, 8), (20, 30)], list(rr._ranges))
        assert_equal(13, len(rr))

    def test_iter(self):
        rr = scheduler.RangeResource()
        rr.add_range(9, 10)
        rr.add_range(5, 8)
        assert_equal([9, 5, 6, 7], list(iter(rr)))

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
            with assert_raises(NotImplementedError):
                TaskState.RUNNING < 3
            with assert_raises(NotImplementedError):
                TaskState.RUNNING > 3
            with assert_raises(NotImplementedError):
                TaskState.RUNNING <= 3
            with assert_raises(NotImplementedError):
                TaskState.RUNNING >= 3


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


class TestAgent(unittest.TestCase):
    """Tests for :class:`katsdpcontroller.scheduler.Agent`.

    This imports from :class:`unittest.TestCase` so that we can use
    ``assertLogs``, which has not been ported to :mod:`nose.tools` yet."""
    @classmethod
    def _make_text_attr(cls, name, value):
        attr = mesos_pb2.Attribute()
        attr.name = name
        attr.type = mesos_pb2.Value.SCALAR
        attr.text.value = value
        return attr

    def _make_offer(self, resources, attrs):
        offer = mesos_pb2.Offer()
        offer.id.value = uuid.uuid4().hex
        offer.framework_id.value = self.framework_id
        offer.slave_id.value = self.slave_id
        offer.hostname = self.host
        for name, value in six.iteritems(resources):
            resource = offer.resources.add()
            resource.name = name
            if isinstance(value, float):
                resource.type = mesos_pb2.Value.SCALAR
                resource.scalar.value = value
            else:
                resource.type = mesos_pb2.Value.RANGES
                for start, stop in value:
                    rng = resource.ranges.range.add()
                    rng.begin = start
                    rng.end = stop - 1
        offer.attributes.extend(attrs)
        return offer

    def setUp(self):
        self.slave_id = 'slaveid'
        self.host = 'slavehostname'
        self.framework_id = 'framework'
        self.if_attr = self._make_text_attr(
            'interface_eth0',
            '{"name": "eth0", "network": "net0", "ipv4_address": "192.168.254.254"}')
        self.if_attr_bad_json = self._make_text_attr(
            'interface_bad_json',
            '{not valid json')
        self.if_attr_bad_schema = self._make_text_attr(
            'interface_bad_schema',
            '{"name": "eth1"}')


    def test_construct(self):
        """Construct an agent from some offers"""
        attrs = [self.if_attr]
        offers = [
            self._make_offer({'cpus': 4.0, 'mem': 1024.0,
                         'ports': [(100, 200), (300, 350)], 'cores': [(0, 8)]}, attrs),
            self._make_offer({'cpus': 0.5, 'mem': 123.5, 'gpus': 1.0,
                              'cores': [(8, 9)]}, attrs)
        ]
        agent = scheduler.Agent(offers)
        assert_equal(self.slave_id, agent.slave_id)
        assert_equal(self.host, agent.host)
        assert_equal(len(attrs), len(agent.attributes))
        for attr, agent_attr in zip(attrs, agent.attributes):
            assert_equal(attr, agent_attr)
        assert_equal(4.5, agent.cpus)
        assert_equal(1147.5, agent.mem)
        assert_equal(1.0, agent.gpus)
        assert_equal(list(range(0, 9)), list(agent.cores))
        assert_equal(list(range(100, 200)) + list(range(300, 350)), list(agent.ports))
        assert_equal([scheduler.Interface(name='eth0',
                                          network='net0',
                                          ipv4_address=ipaddress.IPv4Address(u'192.168.254.254'),
                                          numa_node=None)],
                     agent.interfaces)

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

    def test_allocate_missing_network(self):
        """allocate raises if the task requires a network that is not present"""
        task = scheduler.LogicalTask('task')
        task.networks = ['net0', 'net1']
        agent = scheduler.Agent([self._make_offer({}, [self.if_attr])])
        with assert_raises(scheduler.InsufficientResourcesError):
            agent.allocate(task)

    def test_allocate_success(self):
        """Tests allocate in the success case"""
        task = scheduler.LogicalTask('task')
        task.cpus = 4.0
        task.mem = 128.0
        task.cores = ['a', 'b', 'c']
        task.networks = ['net0']
        agent = scheduler.Agent([
            self._make_offer({'cpus': 4.0, 'mem': 200.0, 'cores': [(4, 8)]}, [self.if_attr])])
        ra = agent.allocate(task)
        assert_equal(0.0, ra.gpus)
        assert_equal(4.0, ra.cpus)
        assert_equal(128.0, ra.mem)
        assert_equal([4, 5, 6], ra.cores)
        assert_equal(0.0, agent.cpus)
        assert_equal(72.0, agent.mem)
        assert_equal(0.0, agent.gpus)
        assert_equal([7], list(agent.cores))
