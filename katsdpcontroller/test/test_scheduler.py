from __future__ import print_function, division, absolute_import
import mock
from nose.tools import *
from katsdpcontroller import scheduler
from katsdpcontroller.scheduler import TaskState
from mesos.interface import mesos_pb2
import mesos.scheduler
import uuid
import ipaddress
import logging
import six
import contextlib
import functools
import socket
import trollius
import networkx
from trollius import From
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


def _make_resources(resources):
    out = []
    for name, value in six.iteritems(resources):
        resource = mesos_pb2.Resource()
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
        out.append(resource)
    return out


def _make_offer(framework_id, slave_id, host, resources, attrs=()):
    offer = mesos_pb2.Offer()
    offer.id.value = uuid.uuid4().hex
    offer.framework_id.value = framework_id
    offer.slave_id.value = slave_id
    offer.hostname = host
    offer.resources.extend(_make_resources(resources))
    offer.attributes.extend(attrs)
    return offer


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

    def _make_offer(self, resources, attrs=()):
        return _make_offer(self.framework_id, self.slave_id, self.host, resources, attrs)

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


class TestScheduler(object):
    """Tests for :class:`katsdpcontroller.scheduler.Scheduler`."""
    def _make_offer(self, resources, slave_num=0, attrs=()):
        return _make_offer(self.framework_id, 'slaveid{}'.format(slave_num),
                           'slavehost{}'.format(slave_num), resources, attrs)

    def setup(self):
        self.framework_id = 'frameworkid'
        # Normally TaskIDAllocator's constructor returns a singleton to keep
        # IDs globally unique, but we want the test to be isolated. Bypass its
        # __new__.
        self.image_resolver = scheduler.ImageResolver()
        self.task_id_allocator = object.__new__(scheduler.TaskIDAllocator, 'test-')
        self.task_id_allocator._prefix = 'test-'
        self.task_id_allocator._next_id = 0
        self.resolver = scheduler.Resolver(self.image_resolver, self.task_id_allocator)
        self.loop = trollius.new_event_loop()
        self.sched = scheduler.Scheduler(self.loop)
        self.driver = mock.create_autospec(mesos.scheduler.MesosSchedulerDriver,
                                           spec_set=True, instance=True)
        self.sched.set_driver(self.driver)

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
        pass   # TODO

    @run_with_self_event_loop
    def test_launch_serial(self):
        """Test launch on the success path, with no concurrent calls."""
        # TODO: still need to extend this to test:
        # - core affinity
        # - NUMA awareness
        # - network interfaces
        # - custom wait_ports
        logical_graph = networkx.MultiDiGraph()
        node0 = scheduler.LogicalTask('node0')
        node0.cpus = 1.0
        node0.command = ['hello', '--port={ports[port]}']
        node0.ports = ['port']
        node0.image = 'image0'
        node1 = scheduler.LogicalTask('node1')
        node1.cpus = 0.5
        node1.command = ['test', '--host={host}', '--remote={endpoints[node0_port]}',
                         '--another={endpoints[node2_foo]}']
        node1.image = 'image1'
        node2 = scheduler.LogicalExternal('node2')
        logical_graph.add_nodes_from([node0, node1, node2])
        logical_graph.add_edge(node1, node0, port='port', order='strong')
        logical_graph.add_edge(node1, node2, port='foo')
        # The launch groups should be [node0, node2], [node1]
        offer0 = self._make_offer({'cpus': 2.0, 'mem': 1024.0, 'ports': [(30000, 31000)]}, 0)
        offer1 = self._make_offer({'cpus': 0.5, 'mem': 128.0, 'ports': [(31000, 32000)]}, 1)
        expected_taskinfo0 = mesos_pb2.TaskInfo()
        expected_taskinfo0.name = 'node0'
        expected_taskinfo0.task_id.value = 'test-00000000'
        expected_taskinfo0.slave_id.value = 'slaveid0'
        expected_taskinfo0.command.shell = False
        expected_taskinfo0.command.value = 'hello'
        expected_taskinfo0.command.arguments.extend(['--port=30000'])
        expected_taskinfo0.container.type = mesos_pb2.ContainerInfo.DOCKER
        expected_taskinfo0.container.docker.image = 'sdp/image0:latest'
        expected_taskinfo0.container.docker.force_pull_image = False
        expected_taskinfo0.resources.extend(
            _make_resources({'cpus': 1.0, 'ports': [(30000, 30001)]}))
        expected_taskinfo0.discovery.visibility = mesos_pb2.DiscoveryInfo.EXTERNAL
        expected_taskinfo0.discovery.name = 'node0'
        port = expected_taskinfo0.discovery.ports.ports.add()
        port.number = 30000
        port.name = 'port'
        port.protocol = 'tcp'
        expected_taskinfo1 = mesos_pb2.TaskInfo()
        expected_taskinfo1.name = 'node1'
        expected_taskinfo1.task_id.value = 'test-00000001'
        expected_taskinfo1.slave_id.value = 'slaveid1'
        expected_taskinfo1.command.shell = False
        expected_taskinfo1.command.value = 'test'
        expected_taskinfo1.command.arguments.extend([
            '--host=slavehost1', '--remote=slavehost0:30000',
            '--another=remotehost:10000'])
        expected_taskinfo1.container.type = mesos_pb2.ContainerInfo.DOCKER
        expected_taskinfo1.container.docker.image = 'sdp/image1:latest'
        expected_taskinfo1.container.docker.force_pull_image = False
        expected_taskinfo1.resources.extend(
            _make_resources({'cpus': 0.5}))
        expected_taskinfo1.discovery.visibility = mesos_pb2.DiscoveryInfo.EXTERNAL
        expected_taskinfo1.discovery.name = 'node1'

        physical_graph = scheduler.instantiate(logical_graph, self.loop)
        pnode0 = next(node for node in physical_graph if node.logical_node is node0)
        pnode1 = next(node for node in physical_graph if node.logical_node is node1)
        pnode2 = next(node for node in physical_graph if node.logical_node is node2)
        pnode2.host = 'remotehost'
        pnode2.ports['foo'] = 10000
        launch = trollius.async(self.sched.launch(physical_graph, self.resolver), loop=self.loop)
        yield From(defer(loop=self.loop))
        # The tasks must be in state STARTING, but not yet RUNNING because
        # there are no offers.
        for node in physical_graph:
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
        assert_equal(expected_taskinfo0, pnode0.taskinfo)
        assert_equal('slavehost0', pnode0.host)
        assert_equal('slaveid0', pnode0.slave_id)
        assert_equal({'port': 30000}, pnode0.ports)
        assert_equal({}, pnode0.cores)
        assert_is_none(pnode0.status)
        assert_equal(TaskState.STARTED, pnode0.state)
        assert_equal(TaskState.READY, pnode2.state)
        assert_equal(AnyOrderList([
            mock.call.launchTasks([offer0.id], [expected_taskinfo0]),
            mock.call.launchTasks([offer1.id], []),
            mock.call.suppressOffers()
        ]), self.driver.mock_calls)
        self.driver.reset_mock()
        # Tell scheduler that node0 is now running. This will start up the
        # the poller, so we need to mock poll_ports.
        with mock.patch.object(scheduler, 'poll_ports', autospec=True) as poll_ports:
            poll_future = trollius.Future(self.loop)
            poll_ports.return_value = poll_future
            status = mesos_pb2.TaskStatus()
            status.task_id.value = 'test-00000000'
            status.state = mesos_pb2.TASK_RUNNING
            self.sched.statusUpdate(self.driver, status)
            yield From(defer(loop=self.loop))
            assert_equal(TaskState.RUNNING, pnode0.state)
            assert_equal(status, pnode0.status)
            assert_equal([mock.call.acknowledgeStatusUpdate(status)],
                         self.driver.mock_calls)
            self.driver.reset_mock()
            poll_ports.assert_called_once_with('slavehost0', [30000], self.loop)
        # Make poll_ports ready. Node 0 should now become ready, and offers
        # should be revived to get resources for node 1.
        poll_future.set_result(None)
        yield From(defer(loop=self.loop))
        assert_equal(TaskState.READY, pnode0.state)
        assert_equal([mock.call.reviveOffers()], self.driver.mock_calls)
        self.driver.reset_mock()
        # Now provide an offer suitable for node 1.
        self.sched.resourceOffers(self.driver, [offer1])
        yield From(defer(loop=self.loop))
        assert_equal(TaskState.STARTED, pnode1.state)
        assert_equal(expected_taskinfo1, pnode1.taskinfo)
        assert_equal('slaveid1', pnode1.slave_id)
        assert_equal([
            mock.call.launchTasks([offer1.id], [expected_taskinfo1]),
            mock.call.suppressOffers()], self.driver.mock_calls)
        self.driver.reset_mock()
        # Finally, tell the scheduler that node 1 is running. There are no
        # ports, so it will go straight to READY.
        status = mesos_pb2.TaskStatus()
        status.task_id.value = 'test-00000001'
        status.state = mesos_pb2.TASK_RUNNING
        self.sched.statusUpdate(self.driver, status)
        yield From(defer(loop=self.loop))
        assert_equal(TaskState.READY, pnode1.state)
        assert_equal(status, pnode1.status)
        assert_equal([mock.call.acknowledgeStatusUpdate(status)],
                     self.driver.mock_calls)
        self.driver.reset_mock()
        assert_true(launch.done())
        yield From(launch)

    @run_with_self_event_loop
    def test_launch_cancel(self):
        """Test cancelling a launch after partial success"""
        pass   # TODO

    @run_with_self_event_loop
    def test_kill_while_starting(self):
        """Test killing a process while in state STARTING"""
        pass   # TODO

    @run_with_self_event_loop
    def test_kill_while_started(self):
        """Test killing a process while in state STARTED"""
        pass   # TODO

    @run_with_self_event_loop
    def test_kill_while_running(self):
        """Test killing a process while in state RUNNING"""
        pass   # TODO

    @run_with_self_event_loop
    def test_kill_while_ready(self):
        """Test killing a process while in state READY"""
        pass   # TODO

    @run_with_self_event_loop
    def test_kill_while_killing(self):
        """Test killing a process while in state KILLING"""
        pass   # TODO

    @run_with_self_event_loop
    def test_die_while_started(self):
        """Test a process dying on its own while in state STARTED"""
        pass   # TODO

    @run_with_self_event_loop
    def test_die_while_running(self):
        """Test a process dying on its own while in state RUNNING"""
        pass   # TODO

    @run_with_self_event_loop
    def test_die_while_ready(self):
        """Test a process dying on its own while in state READY"""
        pass   # TODO

    @run_with_self_event_loop
    def test_kill_order(self):
        """Kill must respect dependency ordering"""
        pass   # TODO

    @run_with_self_event_loop
    def test_close(self):
        """Close must kill off all remaining tasks and abort any pending launches"""
        pass   # TODO
