from __future__ import print_function, division, absolute_import
import networkx
import ipaddress
import logging
import json
import jsonschema
import itertools
import six
import re
from enum import Enum
from katsdptelstate.endpoint import Endpoint
from collections import namedtuple, deque
from decorator import decorator
import mesos.interface
from mesos.interface import mesos_pb2
import mesos.scheduler
import trollius
from trollius import From, Return


SCALAR_RESOURCES = ['cpus', 'gpus', 'mem']
RANGE_RESOURCES = ['ports', 'cores']
TERMINAL_STATES = frozenset([
    mesos_pb2.TASK_FINISHED,
    mesos_pb2.TASK_FAILED,
    mesos_pb2.TASK_KILLED,
    mesos_pb2.TASK_LOST,
    mesos_pb2.TASK_ERROR])
INTERFACE_SCHEMA = {
    'type': 'object',
    'properties': {
        'name': {
            'type': 'string',
            'minLength': 1
        },
        'network': {
            'type': 'string',
            'minLength': 1
        },
        'ipv4_address': {
            'type': 'string',
            'formt': 'ipv4'
        },
        'numa_node': {
            'type': 'integer',
            'minimum': 0
        }
    },
    'required': ['name', 'network', 'ipv4_address']
}
logger = logging.getLogger(__name__)


Interface = namedtuple('Interface', ['name', 'network', 'ipv4_address', 'numa_node'])
"""Abstraction of a network interface on a machine

Attributes
----------
name : str
    Kernel interface name
network : str
    Logical name for the network to which the interface is attached
ipv4_address : :class:`ipaddress.IPv4Address`
    IPv4 local address of the interface
numa_node : int
    Index of the NUMA socket to which the NIC is connected
"""


class RangeResource(object):
    """More convenient wrapper over Mesos' presentation of a range resource
    (list of contiguous ranges). It provides Pythonic idioms to allow it
    to be treated similarly to a list, while keeping storage compact.
    """
    def __init__(self):
        self._ranges = deque()
        self._len = 0

    def __len__(self):
        return self._len

    def add_range(self, start, stop):
        if stop > start:
            self._ranges.append((start, stop))
            self._len += stop - start

    def add_resource(self, resource):
        for r in resource.ranges.range:
            self.add_range(r.begin, r.end + 1)   # Mesos has inclusive ranges

    def __iter__(self):
        return itertools.chain(*[xrange(start, stop) for (start, stop) in self._ranges])

    def popleft(self):
        if not self._ranges:
            raise IndexError('pop from empty list')
        start, stop = self._ranges[0]
        ans = start
        if stop - start > 1:
            self._ranges[0] = (start + 1, stop)
        else:
            self._ranges.popleft()
        self._len -= 1
        return ans

    def pop(self):
        if not self._ranges:
            raise IndexError('pop from empty list')
        start, stop = self._ranges[-1]
        ans = stop - 1
        if stop - start > 1:
            self._ranges[-1] = (start, stop - 1)
        else:
            self._ranges.pop()
        self._len -= 1
        return ans

    def __str__(self):
        def format_range(rng):
            start, stop = rng
            if stop - start == 1:
                return '{}'.format(start)
            else:
                return '{}-{}'.format(start, stop - 1)
        return ','.join(format_range(rng) for rng in self._ranges)


class ResourceAllocation(object):
    """Collection of specific resources allocated from a collection of offers.
    """
    def __init__(self, agent):
        for r in SCALAR_RESOURCES:
            setattr(self, r, 0.0)
        for r in RANGE_RESOURCES:
            setattr(self, r, [])
        self.agent = agent


class ImageResolver(object):
    """Class to map an abstract Docker image name to a fully-qualified name.
    If no private registry is specified, it looks up names in the `sdp/`
    namespace, otherwise in the private registry. One can also override
    individual entries.

    Parameters
    ----------
    private_registry : str, optional
        Address (hostname and port) for a private registry
    tag_file : str, optional
        If specified, the file will be read to determine the image tag to use.
        It can be re-read by calling :meth:`reread_tag_file`.
        It does not affect overrides, to allow them to specify their own tags.
    pull : bool, optional
        Whether to pull images from the `private_registry`.
    """
    def __init__(self, private_registry=None, tag_file=None, pull=True):
        if private_registry is None:
            self._prefix = 'sdp/'
        else:
            self._prefix = private_registry + '/'
        self._tag_file = tag_file
        self._tag = None
        self._private_registry = private_registry
        self._overrides = {}
        self.pull = pull
        self.reread_tag_file()

    def reread_tag_file(self):
        if self._tag_file is None:
            self._tag = 'latest'
        else:
            with open(self._tag_file, 'r') as f:
                tag = f.read().strip()
                # This is a regex that appeared in older versions of Docker
                # (see https://github.com/docker/docker/pull/8447/files).
                # It's probably a reasonable constraint so that we don't allow
                # whitespace, / and other nonsense, even if Docker itself no
                # longer enforces it.
                if not re.match('^[\w][\w.-]{0,127}$', tag):
                    raise ValueError('Invalid tag {} in {}'.format(repr(tag), self._tag_file))
                if self._tag is not None and tag != self._tag:
                    logger.warn("Image tag changed: %s -> %s", self._tag, tag)
                self._tag = tag

    def override(self, name, path):
        self._overrides[name] = path

    def pullable(self, image):
        """Determine whether a fully-qualified image name should be pulled. At
        present, this is done for images in the explicitly specified private
        registry, but images in other registries and specified by override are
        not pulled.
        """
        return self._private_registry is not None and image.startswith(self._prefix) and self.pull

    def __call__(self, name):
        try:
            return self._overrides[name]
        except KeyError:
            if ':' in name:
                # A tag was already specified in the graph
                logger.warning("Image %s has a predefined tag, ignoring tag %s", name, self._tag)
                return self._prefix + name
            else:
                return self._prefix + name + ':' + self._tag


class TaskIDAllocator(object):
    """Allocates unique task IDs."""

    # There may only be a single allocator for each prefix, to avoid
    # collisions.
    _by_prefix = {}

    def __init__(self, prefix=''):
        pass   # Initialised by new

    def __new__(cls, prefix=''):
        # Obtain the singleton
        try:
            return TaskIDAllocator._by_prefix[prefix]
        except KeyError:
            alloc = super(TaskIDAllocator, cls).__new__(cls, prefix)
            alloc._prefix = prefix
            alloc._next_id = 0
            TaskIDAllocator._by_prefix[prefix] = alloc
            return alloc

    def __call__(self):
        ret = self._prefix + str(self._next_id).zfill(8)
        self._next_id += 1
        return ret


class Resolver(object):
    """General-purpose base class to connect extra resources to a graph. The
    base implementation contains an :class:`ImageResolver` and a
    :class:`TaskIDAllocator`.  However, other resources can be connected to
    tasks by subclassing both this class and :class:`PhysicalTask`.
    """
    def __init__(self, image_resolver, task_id_allocator):
        self.image_resolver = image_resolver
        self.task_id_allocator = task_id_allocator


class InsufficientResourcesError(RuntimeError):
    pass


class LogicalNode(object):
    """A node in a :class:`LogicalGraph`. This is a base class. For nodes that
    execute code and use Mesos resources, see :class:`LogicalTask`.

    Attributes
    ----------
    name : str
        Node name
    physical_factory : callable
        Creates the physical task (must return :class:`PhysicalNode`
        or subclass). It is passed the logical task and the event loop.
    """
    def __init__(self, name):
        self.name = name
        self.physical_factory = PhysicalNode


class LogicalTask(LogicalNode):
    """A node in a :class:`LogicalGraph`. It indicates how to run a task
    and what resources it requires, but does not correspond to any specific
    running task.

    The command is processed by :meth:`str.format` to substitute dynamic
    values. The following named values are provided:

    - `ports` : dictionary of port numbers
    - `cores` : dictionary of reserved core IDs
    - `interfaces` : dictionary mapping requested networks to :class:`Interface` objects
    - `endpoints` : dictionary of remote endpoints. Keys are of the form
      :samp:`{service}_{port}`, and values are :class:`Endpoint` objects

    Attributes
    ----------
    cpus : float
        Mesos CPU shares.
    gpus : float
        GPU resources required
    cores : list of str
        Reserved CPU cores. If this is empty, then the task will not be pinned.
        If it is non-empty, then the task will be pinned to the assigned cores,
        and no other tasks will be pinned to those cores. In addition, the
        cores will all be taken from the same NUMA socket, and network
        interfaces in :attr:`networks` will also come from the same socket.
        The strings in the list become keys in :attr:`Task.cores`. You can use
        ``None`` if you don't need to use the core numbers in the command to
        pin individual threads.
    mem : float
        Mesos memory reservation (megabytes)
    ports : list of str
        Network service ports. Each element in the list is a logical name for
        the port.
    networks : list
        Abstract names of networks for which network interfaces are required.
        At present duplicating a name in the list is not supported, but in
        future it may allow requesting multiple interfaces on the same
        network.
    image : str
        Base name of the Docker image (without registry or tag)
    container : `mesos_pb2.ContainerInfo`
        Modify this to override properties of the container. The `image` and
        `force_pull_image` fields are ignored.
    command : list of str
        Command to run inside the image (formatted)
    """
    def __init__(self, name):
        super(LogicalTask, self).__init__(name)
        for r in SCALAR_RESOURCES:
            setattr(self, r, 0.0)
        for r in RANGE_RESOURCES:
            setattr(self, r, [])
        self.networks = []
        self.image = None
        self.command = []
        self.container = mesos_pb2.ContainerInfo()
        self.container.type = mesos_pb2.ContainerInfo.DOCKER
        self.physical_factory = PhysicalTask

    def valid_agent(self, attributes):
        """Checks whether the attributes of an agent are suitable for running
        this task. Subclasses may override this to enforce constraints e.g.,
        requiring a special type of hardware."""
        # TODO: enforce x86-64, if we ever introduce ARM or other hardware
        return True


class Agent(object):
    """Collects multiple offers for a single Mesos agent and allows
    :class:`ResourceAllocation`s to be made from it.
    """
    def __init__(self, offers):
        if not offers:
            raise ValueError('At least one offer must be specified')
        self.offers = offers
        self.slave_id = offers[0].slave_id.value
        self.host = offers[0].hostname
        self.attributes = offers[0].attributes
        self.interfaces = []
        for attribute in offers[0].attributes:
            if attribute.name.startswith('interface_') and \
                    attribute.type == mesos_pb2.Value.SCALAR:
                try:
                    value = json.loads(attribute.text.value)
                    jsonschema.validate(value, INTERFACE_SCHEMA)
                    interface = Interface(
                        name=value['name'],
                        value=value['network'],
                        ipv4_address=ipaddress.IPv4Address(value['ipv4_address']),
                        numa_node=value.get('numa_node'))
                except (ValueError, KeyError, TypeError, ipaddress.AddressValueError):
                    logger.warn('Could not parse {} ({})'.format(
                        attribute.name, attribute.text.value))
                except jsonschema.ValidationError as e:
                    logger.warn('Validation error parsing {}: {}'.format(value, e))
                else:
                    self.interfaces.append(interface)
        # These resources all represent resources not yet allocated
        for r in SCALAR_RESOURCES:
            setattr(self, r, 0.0)
        for r in RANGE_RESOURCES:
            setattr(self, r, RangeResource())
        for offer in offers:
            for resource in offer.resources:
                if resource.name in SCALAR_RESOURCES:
                    self._inc_attr(resource.name, resource.scalar.value)
                elif resource.name in RANGE_RESOURCES:
                    self._add_range_attr(resource.name, resource)

    def _inc_attr(self, key, delta):
        setattr(self, key, getattr(self, key) + delta)

    def _add_range_attr(self, key, resource):
        getattr(self, key).add_resource(resource)

    def allocate(self, logical_task):
        """Allocate the resources for a logical task, is possible.

        Returns
        -------
        ResourceAllocation

        Raises
        ------
        InsufficientResourcesError
            if there are not enough resources to add the task
        """
        if not self.offers:
            raise InsufficientResourcesError('No offers have been received for this agent')
        if not logical_task.valid_agent(self.attributes):
            raise InsufficientResourcesError('Task does not match this agent')
        # TODO: add NUMA awareness
        for r in SCALAR_RESOURCES:
            need = getattr(logical_task, r)
            have = getattr(self, r)
            if have < need:
                raise InsufficientResourcesError('Not enough {} ({} < {})'.format(r, have, need))
        for r in RANGE_RESOURCES:
            need = len(getattr(logical_task, r))
            have = len(getattr(self, r))
            if have < need:
                raise InsufficientResourcesError('Not enough {} ({} < {})'.format(r, have, need))
        for network in logical_task.networks:
            if not any(interface.network == network for interface in self.interfaces):
                raise InsufficientResourcesError('Network {} not present'.format(network))

        # Have now verified that the task fits. Create the resources for it
        alloc = ResourceAllocation(self)
        for r in SCALAR_RESOURCES:
            need = getattr(logical_task, r)
            self._inc_attr(r, -need)
            setattr(alloc, r, need)
        for r in RANGE_RESOURCES:
            for name in getattr(logical_task, r):
                value = getattr(self, r).popleft()
                getattr(alloc, r).append(value)
        return alloc


class TaskState(Enum):
    NOT_READY = 0            #: We have not yet been asked to start
    START_BLOCKED = 1        #: Waiting for dependencies to start
    WAITING = 2              #: Ready to launch once resources are available
    STAGING = 3              #: Launch issued, waiting for task status message
    STARTING = 4             #: Task has started, waiting for ports to be open
    RUNNING = 5              #: Task is fully ready
    STOP_BLOCKED = 6         #: Ask to stop, but waiting for dependencies to die
    STOPPING = 7             #: Kill has been issued, waiting for status message
    DEAD = 8                 #: Have received terminal status message


class PhysicalNode(object):
    """Base class for physical nodes.

    Parameters
    ----------
    logical_node : :class:`LogicalNode`
        The logical node from which this physical node is constructed
    loop : :class:`trollius.BaseEventLoop`
        The event loop used for constructing futures etc
    """
    def __init__(self, logical_node, loop):
        self.logical_node = logical_node
        #: Current state
        self.state = TaskState.NOT_READY
        #: Future signalled when task is either fully running or has died
        self.running_future = trollius.Future(loop=loop)
        #: Future signalled when the task dies
        self.dead_future = trollius.Future(loop=loop)

    def resolve(self, resolver, graph):
        pass


class PhysicalTask(PhysicalNode):
    """Running task (or one that is being prepared to run). Unlike
    a :class:`LogicalTask`, it has a specific agent, ports, etc assigned
    (during it's life cycle, not at construction).

    Parameters
    ----------
    logical_task : :class:`LogicalTask`
        Logical task forming the template for this physical task
    loop : :class:`trollius.BaseEventLoop`
        The event loop used for constructing futures etc
    """
    def __init__(self, logical_task, loop):
        super(PhysicalTask, self).__init__(logical_task, loop)
        self.interfaces = {}
        self.endpoints = {}
        self.taskinfo = None
        self.allocation = None
        for r in RANGE_RESOURCES:
            setattr(self, r, {})   # Map name to resource, if name was given

    @property
    def agent(self):
        return self.allocation.agent

    @property
    def host(self):
        return self.allocation.agent.host

    @property
    def slave_id(self):
        return self.allocation.agent.slave_id

    def allocate(self, allocation):
        """Assign resources. This is called just before moving to
        :const:`TaskState.STAGING`, and before :meth:`resolve`.

        Parameters
        ----------
        allocation : :class:`ResourceAllocation`
            Specific resources assigned to the task
        """
        self.allocation = allocation
        for network in self.logical_node.networks:
            for interface in self.agent.interfaces:
                if interface.network == network:
                    self.interfaces[network] = interface
                    break
        for r in RANGE_RESOURCES:
            d = {}
            for name, value in zip(getattr(self.logical_node, r), getattr(self.allocation, r)):
                if name is not None:
                    d[name] = value
            setattr(self, r, d)

    def resolve(self, resolver, graph):
        """Do final preparation before moving to :const:`TaskState.STAGING`.
        At this point all dependencies are guaranteed to have resources allocated.

        Parameters
        ----------
        resolver : :class:`Resolver`
            Resolver to allocate resources like task IDs
        graph : :class:`networkx.MultiDiGraph`
            Physical graph
        """

        for src, trg, port in graph.out_edges_iter([self], data='port'):
            endpoint_name = '{}_{}'.format(trg.logical_node.name, port)
            self.endpoints[endpoint_name] = Endpoint(trg.host, trg.ports[port])

        taskinfo = mesos_pb2.TaskInfo()
        taskinfo.name = self.logical_node.name
        taskinfo.task_id.value = resolver.task_id_allocator()
        args = self.subst_args()
        command = [x.format(**args) for x in self.logical_node.command]
        if self.allocation.cores:
            # TODO: see if this can be done through Docker instead of with taskset
            command = ['taskset', '-c', ','.join(str(core) for core in self.allocation.cores)] + command
        if command:
            taskinfo.command.value = command[0]
            taskinfo.command.arguments.extend(command[1:])
        taskinfo.command.shell = False
        taskinfo.container.CopyFrom(self.logical_node.container)
        image_path = resolver.image_resolver(self.logical_node.image)
        taskinfo.container.docker.image = image_path
        taskinfo.container.docker.force_pull_image = \
                resolver.image_resolver.pullable(image_path)
        taskinfo.slave_id.value = self.slave_id
        for r in SCALAR_RESOURCES:
            value = getattr(self.logical_node, r)
            if value > 0:
                resource = taskinfo.resources.add()
                resource.name = r
                resource.type = mesos_pb2.Value.SCALAR
                resource.scalar.value = value
        for r in RANGE_RESOURCES:
            value = getattr(self.allocation, r)
            if value:
                resource = taskinfo.resources.add()
                resource.name = r
                resource.type = mesos_pb2.Value.RANGES
                for item in value:
                    rng = resource.ranges.range.add()
                    rng.begin = item
                    rng.end = item
        self.taskinfo = taskinfo

    def subst_args(self):
        """Returns a dictionary that is passed when formatting the command
        from the logical task.
        """
        args = {}
        for r in RANGE_RESOURCES:
            args[r] = getattr(self, r)
        args['interfaces'] = self.interfaces
        args['endpoints'] = self.endpoints
        args['host'] = self.host
        return args


class LogicalGraph(networkx.MultiDiGraph):
    """Collection of :class:`LogicalNode`s.

    If node A provides a service on a port and node B connects to it, add an
    edge from B to A, and set the `port` attribute of the edge to the logical
    name of the port. The endpoint of this port on A will be provided to B
    in the `endpoints` dictionary, under the name `A_port`.

    If there is an edge from A to B, then it is assumed that B must be started
    before or at the same time as A (because A needs to know where B will be
    running, and that is only known when starting B). If B needs to be running
    strictly before A is started, then set the property ``order=strong``. This
    will also ensure that B is killed after A.
    """
    pass


class PhysicalGraph(networkx.MultiDiGraph):
    """Collection of :class:`PhysicalNode`s.

    The constructor does not actually launch the tasks; it merely creates
    them.

    Parameters
    ----------
    logical_graph : :class:`LogicalGraph`
        Logical graph to instantiate
    loop : :class:`trollius.BaseEventLoop`
        Event loop used to create futures

    Raises
    ------
    InsufficientResourcesError
        if `offers` does not contain sufficient resources for the graph
    """

    def __init__(self, logical_graph, loop):
        super(PhysicalGraph, self).__init__()
        self.loop = loop
        # Transcribe nodes
        trans = {}
        for logical in logical_graph.nodes_iter():
            physical = logical.physical_factory(logical, loop)
            self.add_node(physical)
            trans[logical] = physical
        # Transcribe edges
        for u, v, attr in logical_graph.edges_iter(data=True):
            self.add_edge(trans[u], trans[v], attr_dict=attr)


@decorator
def run_in_event_loop(func, *args, **kw):
    args[0]._loop.call_soon_threadsafe(func, *args, **kw)


class Scheduler(mesos.interface.Scheduler):
    def __init__(self, loop):
        self._loop = loop
        self._drivers = None
        self._offers = {}           #: offers keyed by slave ID then offer ID
        self._pending = deque()     #: graphs for which we do not yet have resources
        self._running = set()       #: graphs that have been launched
        self._closing = False

    def _start_stop_offers(self):
        if self._pending:
            self._driver.reviveOffers()
        else:
            self._driver.suppressOffers()
        # If we have offers but nothing is pending, decline them
        if not self._pending and self._offers:
            for offers in six.itervalues(self._offers):
                self._driver.acceptOffers([offer.id.value for offer in offers], [])
            self._offers = {}

    def _try_launch(self):
        # TODO: handle strong ordering
        if self._pending and self._offers:
            try:
                (graph, resolver, future) = self._pending[0]
                agents = [Agent(list(six.itervalues(offers))) for offers in six.itervalues(self._offers)]
                # Sort agents by GPUs then free memory. This makes it less likely that a
                # low-memory process will hog all the other resources on a high-memory
                # box. Similarly, non-GPU tasks will only use GPU-equipped agents if
                # there is no other choice. Should eventually look into smarter
                # algorithms e.g. Dominant Resource Fairness
                # (http://mesos.apache.org/documentation/latest/allocation-module/)
                agents.sort(key=lambda agent: (agent.gpus, agent.mem))
                allocations = []
                for node in graph.nodes_iter():
                    logical_task = node.logical_node
                    allocation = None
                    if isinstance(logical_task, LogicalTask):
                        for agent in agents:
                            try:
                                allocation = agent.allocate(logical_task)
                                break
                            except InsufficientResourcesError as e:
                                logger.debug('Cannot add %s to %s: %s',
                                             logical_task.name, agent.slave_id, e)
                        if allocation is None:
                            raise InsufficientResourcesError('No agent found for {}'.format(logical_task.name))
                        allocations.append((node, allocation))
            except InsufficientResourcesError:
                logger.debug('Could not launch %s due to insufficient resources', physical_name)
            else:
                # Two-phase resolving
                for (node, allocation) in allocations:
                    node.allocate(allocation)
                for node in graph.nodes_iter():
                    node.resolve(resolver, graph)
                # Launch the tasks
                taskinfos = {agent: [] for agent in agents}
                for (node, _) in allocations:
                    taskinfos[node.agent].append(node.taskinfo)
                for agent in agents:
                    offer_ids = [offer.id for offer in agent.offers]
                    # TODO: does this need to use run_in_executor? Is it
                    # thread-safe to do so?
                    # TODO: update the status of tasks
                    self._driver.launchTasks(offer_ids, taskinfos[agent])
                    logger.info('Launched %d tasks on %s', len(taskinfos[agent]), agent.slave_id)
                self._offers = {}
                self._running.add(graph)
                self._pending.popleft()
                future.set_result(None)
                self._start_stop_offers()

    def set_driver(self, driver):
        self._driver = driver

    def registered(self, driver, framework_id, master_info):
        pass

    @run_in_event_loop
    def resourceOffers(self, driver, offers):
        for offer in offers:
            self._offers.setdefault(offer.slave_id.value, {})[offer.id.value] = offer
        self._try_launch()

    @run_in_event_loop
    def offerRescinded(self, offer_id):
        for slave_id, offers in six.iteritems(self._offers):
            try:
                del offers[offer_id]
                if not offers:
                    del self._offers[slave_id]
                break
            except KeyError:
                pass
        self._start_stop_offers()

    @run_in_event_loop
    def statusUpdate(self, driver, status):
        logging.debug('Update: task %s in state %s (%s)',
            status.task_id.value, mesos_pb2.TaskState.Name(status.state),
            status.message)
        # TODO: use a lookup to more easily find tasks from IDs
        # TODO: update task status
        for physical in self._running:
            for task in physical.nodes():
                if task.taskinfo.task_id.value == status.task_id.value and not task.dead_future.done():
                    if status.state in TERMINAL_STATES:
                        task.dead_future.set_result(status.state)
        self._driver.acknowledgeStatusUpdate(status)

    @trollius.coroutine
    def launch(self, graph, resolver):
        if self._closing:
            raise RuntimeError('Cannot launch graphs while shutting down')
        # TODO: use requestResources to ask the allocator for resources?
        future = trollius.Future(loop=self._loop)
        pending = (graph, resolver, future)
        self._pending.append(pending)
        self._start_stop_offers()
        self._try_launch()
        try:
            physical = yield From(future)
            raise Return(physical)
        finally:
            # In particular, if the user cancels the launch, we need to
            # do this.
            if pending in self._pending:
                self._pending.remove(pending)

    @trollius.coroutine
    def kill(self, graph):
        if graph not in self._running:
            raise ValueError('Graph is not running')
        futures = []
        for task in graph.nodes():
            # TODO: according to the Mesos docs, killing a task is not reliable,
            # and may need to be attempted again.
            if not task.dead_future.done():
                self._driver.killTask(task.taskinfo.task_id)
            futures.append(task.dead_future)
        yield From(trollius.gather(*futures, loop=self._loop))
        try:
            self._running.remove(graph)
        except KeyError:
            # Can happen if another caller also killed the graph when we yielded
            pass

    @trollius.coroutine
    def close(self):
        self._closing = True
        # TODO: do we need to explicitly decline outstanding offers?
        for (graph, resolver, future) in self._pending:
            future.cancel()
        self._pending = []
        while self._running:
            yield From(self.kill(next(iter(self._running))))
        self._driver.stop()
        status = yield From(self._loop.run_in_executor(None, self._driver.join))
        raise Return(status)


__all__ = [
    'LogicalTask', 'PhysicalTask',
    'Interface', 'InsufficientResourcesError',
    'LogicalGraph', 'PhysicalGraph',
    'ImageResolver', 'TaskIDAllocator', 'Resolver',
    'Scheduler']
