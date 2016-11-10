"""
A logical graph is a :class:`networkx.MultiDiGraph` containing
:class:`LogicalNode`s.

If node A provides a service on a port and node B connects to it, add an
edge from B to A, and set the `port` attribute of the edge to the logical
name of the port. The endpoint of this port on A will be provided to B
in the `endpoints` dictionary, under the name `A_port`.

If there is an edge from A to B, then it is assumed that B must be started
before or at the same time as A (because A needs to know where B will be
executing, and that is only known when starting B). If B needs to be ready
strictly before A is started, then set the property ``order=strong``. This
will also ensure that B is killed after A.
"""


from __future__ import print_function, division, absolute_import
import networkx
import ipaddress
import logging
import json
import jsonschema
import itertools
import six
import re
import socket
import contextlib
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
TERMINAL_STATUSES = frozenset([
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


class OrderedEnum(Enum):
    """Ordered enumeration from Python 3.x Enum documentation"""
    def __ge__(self, other):
        if self.__class__ is other.__class__:
            return self.value >= other.value
        return NotImplemented

    def __gt__(self, other):
        if self.__class__ is other.__class__:
            return self.value > other.value
        return NotImplemented

    def __le__(self, other):
        if self.__class__ is other.__class__:
            return self.value <= other.value
        return NotImplemented

    def __lt__(self, other):
        if self.__class__ is other.__class__:
            return self.value < other.value
        return NotImplemented


@trollius.coroutine
def poll_ports(host, ports, loop):
    """Waits until a set of TCP ports are accepting connections on a host."""
    try:
        addrs = yield From(loop.getaddrinfo(
            host=host, port=None,
            type=socket.SOCK_STREAM,
            proto=socket.IPPROTO_TCP,
            flags=socket.AI_ADDRCONFIG | socket.AI_V4MAPPED))
        if not addrs:
            raise ValueError('no address found for {}'.format(host))
        (family, type_, proto, canonname, sockaddr) = addrs[0]
        for port in ports:
            while True:
                sock = socket.socket(family=family, type=type_, proto=proto)
                sock.settimeout(5)
                with contextlib.closing(sock):
                    try:
                        yield From(loop.sock_connect(sock, (sockaddr[0], port)))
                    except OSError as e:
                        logging.debug('Port %d on %s not ready: %s', port, host, e)
                        yield From(trollius.sleep(1, loop=loop))
                        pass
                    else:
                        break
            logging.debug('Port %d on %s ready', port, host)
    except trollius.CancelledError:
        pass
    except Exception as e:
        logging.warn('Unexpected exception waiting for %s', host, exc_info=True)


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
    """A node in a logical graph. This is a base class. For nodes that
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


class LogicalExternal(LogicalNode):
    """An external service. It is assumed to be ready as soon as it starts.

    The host and port must be set manually on the physical node."""
    def __init__(self, name):
        super(LogicalExternal, self).__init__(name)
        self.physical_factory = PhysicalExternal


class LogicalTask(LogicalNode):
    """A node in a logical graph that indicates how to run a task
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
    wait_ports : list of str
        Subset of `ports` which must be open before the task is considered
        ready. If set to `None`, defaults to `ports`.
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
        self.wait_ports = None
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


class TaskState(OrderedEnum):
    NOT_READY = 0            #: We have not yet started it
    STARTING = 1             #: We have been asked to start, but have not yet asked Mesos
    STARTED = 2              #: We have asked Mesos to start it, but it is not yet running
    RUNNING = 3              #: Process is running, but we're waiting for ports to open
    READY = 4                #: Node is completely ready
    KILLED = 5               #: We have asked Mesos to kill it, but do not yet have confirmation
    DEAD = 6                 #: Have received terminal status message


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
        #: Event signalled when task is either fully ready or has died
        self.ready_event = trollius.Event(loop=loop)
        #: Event signalled when the task dies
        self.dead_event = trollius.Event(loop=loop)
        self.loop = loop

    @property
    def name(self):
        return self.logical_node.name

    def resolve(self, resolver, graph):
        pass

    def set_state(self, state):
        if state < self.state:
            # STARTING -> NOT_READY is permitted, for the case where a
            # launch is cancelled before the tasks are actually launched.
            if state != TaskState.NOT_READY or self.state != TaskState.STARTING:
                logging.warn('Ignoring state change that went backwards (%s -> %s) on task %s',
                             self.state, state, self.name)
                return
        self.state = state
        if state == TaskState.DEAD:
            self.dead_event.set()
            self.ready_event.set()
        if state == TaskState.READY:
            self.ready_event.set()

    def kill(self, driver):
        """Start killing off the node. This is guaranteed to be called only if
        the task made it to at least :const:`TaskState.STARTED` and it is not
        in :const:`TaskState.DEAD`.

        This function should not manipulate the task state; the caller will
        set the state to :const:`TaskState.KILLED`.
        """
        raise NotImplementedError()


class PhysicalExternal(PhysicalNode):
    """External service with a hostname and ports. The service moves
    automatically from :const:`TaskState.STARTED` to
    :const:`TaskState.READY`, and from :const:`TaskState.KILLED` to
    :const:`TaskState.DEAD`.
    """
    def __init__(self, logical_node, loop):
        super(PhysicalExternal, self).__init__(logical_node, loop)
        self.host = None
        self.ports = {}

    def set_state(self, state):
        if state >= TaskState.STARTED and state < TaskState.READY:
            state = TaskState.READY
        elif state == TaskState.KILLED:
            state = TaskState.DEAD
        super(PhysicalExternal, self).set_state(state)

    def kill(self, driver):
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
        self.status = None
        for r in RANGE_RESOURCES:
            setattr(self, r, {})   # Map name to resource, if name was given
        self._poller = None

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
            endpoint_name = '{}_{}'.format(trg.name, port)
            self.endpoints[endpoint_name] = Endpoint(trg.host, trg.ports[port])

        taskinfo = mesos_pb2.TaskInfo()
        taskinfo.name = self.name
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
        taskinfo.discovery.visibility = mesos_pb2.DiscoveryInfo.EXTERNAL
        taskinfo.discovery.name = self.name
        for port_name, port_number in six.iteritems(self.ports):
            port = taskinfo.discovery.ports.ports.add()
            port.number = port_number
            port.name = port_name
            port.protocol = 'tcp'    # TODO: need a way to indicate this
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

    def kill(self, driver):
        # TODO: according to the Mesos docs, killing a task is not reliable,
        # and may need to be attempted again.
        if self._poller:
            self._poller.cancel()
            self._poller = None
        driver.killTask(self.taskinfo.task_id)

    def set_state(self, state):
        def callback(future):
            self._poller = None
            if not future.cancelled():
                self.set_state(TaskState.READY)

        super(PhysicalTask, self).set_state(state)
        if state == TaskState.RUNNING and self._poller is None:
            if self.logical_node.wait_ports is not None:
                wait_ports = [self.ports[port] for port in wait_ports]
            else:
                wait_ports = six.viewvalues(self.ports)
            if wait_ports:
                self._poller = trollius.async(poll_ports(self.host, wait_ports, self.loop))
                self._poller.add_done_callback(callback)
            else:
                self.set_state(TaskState.READY)


def instantiate(logical_graph, loop):
    """Create a physical graph from a logical one.

    Parameters
    ----------
    logical_graph : :class:`networkx.MultiDiGraph`
        Logical graph to instantiate
    loop : :class:`trollius.BaseEventLoop`
        Event loop used to create futures
    """

    # Create physical nodes
    mapping = {logical: logical.physical_factory(logical, loop) for logical in logical_graph}
    return networkx.relabel_nodes(logical_graph, mapping)


@decorator
def run_in_event_loop(func, *args, **kw):
    args[0]._loop.call_soon_threadsafe(func, *args, **kw)


class Scheduler(mesos.interface.Scheduler):
    def __init__(self, loop):
        self._loop = loop
        self._drivers = None
        self._offers = {}           #: offers keyed by slave ID then offer ID
        self._pending = deque()     #: graphs for which we do not yet have resources
        self._active = {}           #: tasks that have been launched (STARTED to KILLED), indexed by task ID
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
        if self._pending and self._offers:
            try:
                (graph, nodes, resolver) = self._pending[0]
                # Due to concurrency, another coroutine may have altered
                # the state of the tasks since they were put onto the
                # pending list (e.g. by killing them). Filter those out.
                nodes = [node for node in nodes if node.state == TaskState.STARTING]
                agents = [Agent(list(six.itervalues(offers))) for offers in six.itervalues(self._offers)]
                # Sort agents by GPUs then free memory. This makes it less likely that a
                # low-memory process will hog all the other resources on a high-memory
                # box. Similarly, non-GPU tasks will only use GPU-equipped agents if
                # there is no other choice. Should eventually look into smarter
                # algorithms e.g. Dominant Resource Fairness
                # (http://mesos.apache.org/documentation/latest/allocation-module/)
                agents.sort(key=lambda agent: (agent.gpus, agent.mem))
                allocations = []
                for node in nodes:
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
                            raise InsufficientResourcesError(
                                'No agent found for {}'.format(logical_task.name))
                        allocations.append((node, allocation))
            except InsufficientResourcesError:
                logger.debug('Could not launch graph due to insufficient resources')
            else:
                # Two-phase resolving
                for (node, allocation) in allocations:
                    node.allocate(allocation)
                for node in nodes:
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
                self._pending.popleft()
                self._start_stop_offers()
                for node in nodes:
                    node.set_state(TaskState.STARTED)
                for (node, _) in allocations:
                    self._active[node.taskinfo.task_id.value] = node

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
        logging.debug(
            'Update: task %s in state %s (%s)',
            status.task_id.value, mesos_pb2.TaskState.Name(status.state),
            status.message)
        task = self._active.get(status.task_id.value)
        if task:
            if status.state in TERMINAL_STATUSES:
                task.set_state(TaskState.DEAD)
                del self._active[status.task_id.value]
            elif status.state == mesos_pb2.TASK_RUNNING:
                if task.status < TaskState.RUNNING:
                    task.set_state(TaskState.RUNNING)
                    # The task itself is responsible for advancing to
                    # READY
            task.status = status
        self._driver.acknowledgeStatusUpdate(status)

    @trollius.coroutine
    def launch(self, graph, resolver, nodes=None):
        """Launch a physical graph, or a subset of nodes from a graph. This is
        a coroutine that returns only once the nodes are ready.

        It is legal to pass nodes that have already been passed to
        :meth:`launch`. They will not be re-launched, but this call will wait
        until they are ready.

        It is safe to run this coroutine as a trollius task and cancel it.
        However, some of the nodes may have been started already. If the
        launch is cancelled, it is recommended to kill the nodes to reach a
        known state.

        Running two launches with the same graph concurrently is possible but
        not recommended. In particular, cancelling the first launch can cause
        the second to stall indefinitely if it was waiting for nodes from the
        first launch to be ready.

        .. note::

            When waiting for a node to be ready, it is also considered to be
            ready if it dies (either due to failing to start, or due to
            :meth:`kill`.

        Parameters
        ----------
        graph : :class:`networkx.MultiDiGraph`
            Physical graph to launch
        resolver : :class:`Resolver`
            Resolver to allocate resources like task IDs
        nodes : list, optional
            If specified, lists a subset of the nodes to launch. Otherwise,
            all nodes in the graph are launched.

        Raises
        ------
        RuntimeError
            If :meth:`close` has been called.
        ValueError
            If any nodes in `nodes` is in state :const:`TaskState.NOT_READY`
            and has a strong dependency on a node that is not in `nodes` and
            is also in state :const:`TaskState.NOT_READY`.
        """
        if self._closing:
            raise RuntimeError('Cannot launch tasks while shutting down')
        if nodes is None:
            nodes = graph.nodes()
        # Create a startup schedule. The nodes are partitioned into groups that can
        # be started at the same time.

        # Check that we don't depend on some non-ready task outside the set.
        remaining = set(node for node in nodes if node.state == TaskState.NOT_READY)
        for src, trg in graph.out_edges_iter(remaining):
            if trg not in remaining and trg.state == TaskState.NOT_READY:
                raise ValueError('{} depends on {} but it is neither ready not scheduled'.format(
                    src.name, trg.name))
        groups = []
        while remaining:
            # nodes that have a (possibly indirect) strong dependency on a non-ready task
            blocked = set()
            for src, trg, order in graph.out_edges_iter(remaining, data='order'):
                if order == 'strong' and trg.state == TaskState.NOT_READY:
                    blocked.add(src)
                    for ancestor in networkx.ancestors(graph, src):
                        blocked.add(ancestor)
            group = remaining - blocked
            if not group:
                raise ValueError('strong cyclic dependency in graph')
            groups.append(list(group))
            for node in groups[-1]:
                node.state = TaskState.STARTING
            remaining = blocked

        for group in groups:
            pending = (graph, group, resolver)
            # TODO: use requestResources to ask the allocator for resources?
            # TODO: check if any dependencies have died, and if so, bail out?
            self._pending.append(pending)
            self._start_stop_offers()
            self._try_launch()
            futures = []
            for node in group:
                futures.append(node.ready_event.wait())
            try:
                yield From(trollius.gather(*futures, loop=self._loop))
            except trollius.CancelledError:
                if pending in self._pending:
                    for node in group:
                        if node.state == TaskState.STARTING:
                            node.set_state(TaskState.NOT_READY)
                    self._pending.remove(pending)

    @trollius.coroutine
    def kill(self, graph, nodes=None):
        """Kill a graph or set of nodes from a graph. It is safe to kill nodes
        from any state. Strong dependencies will be honoured, leaving nodes
        alive until all their dependencies are killed (provided the
        dependencies are included in `nodes`).

        Parameters
        ----------
        graph : :class:`networkx.MultiDiGraph`
            Physical graph to kill
        nodes : list, optional
            If specified, the nodes to kill. The default is to kill all nodes
            in the graph.
        """
        @trollius.coroutine
        def kill_one(node, graph):
            if node.state <= TaskState.STARTING:
                node.set_state(TaskState.DEAD)
            elif node.state < TaskState.KILLED:
                for src, trg, data in graph.in_edges_iter([node], data=True):
                    if data.get('order') == 'strong':
                        yield From(src.dead_event.wait())
                # Re-check state because it might have changed while waiting
                if node.state < TaskState.KILLED:
                    logger.debug('Killing %s', node.name)
                    node.kill(self._driver)
                    node.set_state(TaskState.KILLED)
            yield From(node.dead_event.wait())

        futures = []
        # TODO: order the shutdown using the strong dependencies
        if nodes is not None:
            kill_graph = graph.subgraph(nodes)
        else:
            kill_graph = graph
        for node in kill_graph:
            futures.append(trollius.async(kill_one(node, kill_graph), loop=self._loop))
        yield From(trollius.gather(*futures, loop=self._loop))

    @trollius.coroutine
    def close(self):
        self._closing = True
        # TODO: do we need to explicitly decline outstanding offers?
        self._pending = []
        futures = []
        # TODO: shut down in the proper order
        for task in six.itervalues(self._active):
            task.kill(self._driver)
            futures.append(task.dead_event.wait())
        if futures:
            yield From(trollius.gather(*futures, loop=self._loop))
        self._driver.stop()
        status = yield From(self._loop.run_in_executor(None, self._driver.join))
        raise Return(status)


__all__ = [
    'LogicalNode', 'PhysicalNode',
    'LogicalExternal', 'PhysicalExternal',
    'LogicalTask', 'PhysicalTask', 'TaskState',
    'Interface', 'InsufficientResourcesError', 'ResourceAllocation',
    'ImageResolver', 'TaskIDAllocator', 'Resolver',
    'Scheduler', 'instantiate']
