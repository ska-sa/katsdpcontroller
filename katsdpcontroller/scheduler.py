"""
Mesos-based scheduler for launching collections of inter-dependent tasks. It
uses trollius for asynchronous execution. It is **not** thread-safe.

Graphs
------
The scheduler primary operates on *graphs*. There are two types of graphs:
logical and physical. A logical graph defines *how* to run things, but is not
assigned with any actually running tasks. In many cases, a logical graph can
be described statically, and should not contain any hostnames, addresses, port
numbers etc. The graph itself is simply a :class:`networkx.MultiDiGraph`, and
the nodes are instances of :class:`LogicalNode` (or subclasses).

A physical graph corresponds to a specific execution. The nodes in a physical
graph have a lifecycle where they start, run and are killed; they cannot come
back from the dead. To re-run all the processes, a new graph must be created.
A physical graph is generally created from a logical graph by
:func:`instantiate`, but it is also possible to build up a physical graph
manually (by creating :class:`PhysicalNode`s from :class:`LogicalNode`s and
assembling them into a graph). It is even possible to add to a physical graph
later (although some care is needed, as this is not async-safe relative to all
operations).

Nodes
-----
The base class for logical nodes is :class:`LogicalNode`, but it is an
abstract base class. Typically one will use :class:`LogicalTask` for tasks
that are executed as Docker images. For services managed outside the graph,
use :class:`LogicalExternal`. This can also be used for multicast endpoints.

Resolution
----------
Due to limitations of Mesos, the full information about a task (such as which
machine it is running on and which ports it is using) are not available until
immediately before the task is launched. The process of filling in this
information is called *resolution* and is performed by
:meth:`PhysicalNode.resolve`. If the user needs to access or modify this
information before the task is actually launched, it should make a subclass
and override this method.

If only the command needs to be adapted to the dynamic configuration, it might
not be necessary to write any extra code. The command specified in the logical
task is passed through :meth:`str.format` together with a dictionary of
dynamic information. See :attr:`LogicalTask.command` for details.

To aid in resolution, a :class:`Resolver` is provided that is able to allocate
task IDs, image paths, etc. It can be augmented or subclassed to provide extra
tools for subclasses of :class:`PhysicalNode` to use during resolution.

Edges
-----
Edges are used for several distinct but overlapping purposes.

1. If task A needs to connect to a service provided by task B, then add a
   graph edge from A to B, and set the `port` edge attribute to the name of
   the port on B. The endpoint on B is provided to A as part of resolution.

2. If there is an edge from A to B, then B must be launched before or at the
   same time as A. This is necessary for the resolution of port numbers above
   to work, but applies even if no `port` attribute is set on the edge. It is
   thus guaranteed that B's resources are assigned before A is resolved (but
   not necessary before B is resolved).

3. If the `order` edge attribute is set to `strong` on an edge from A to B,
   then B must be *ready* (running and listening on its ports) before A is
   launched, and B will be killed only after A is dead. This should be used if
   A will connect to B immediately on startup (and will fail if B is not yet
   running), rather than in response to some later trigger. At present, no
   other values are defined or should be used for the `order` attribute.

It is permitted for the graph to have cycles, as long as no cycle contains a
strong edge.
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
"""Abstraction of a network interface on a machine.

An interface is defined by setting a Mesos attribute of the form
:samp:`interface_{name}`, whose value is a JSON string that adheres to the
schema in :const:`INTERFACE_SCHEMA`.

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
    """Waits until a set of TCP ports are accepting connections on a host.

    It repeatedly tries to connect to each port until a connection is
    successful.

    It is safe to cancel this coroutine to give up waiting.

    Parameters
    ----------
    host : str
        Hostname or IP address
    ports : list
        Port numbers to connect to
    loop : :class:`trollius.BaseEventLoop`
        The event loop used for socket operations

    Raises
    ------
    socket.gaierror
        for any error in resolving `host`
    OSError
        on any socket operation errors other than the connect
    """
    addrs = yield From(loop.getaddrinfo(
        host=host, port=None,
        type=socket.SOCK_STREAM,
        proto=socket.IPPROTO_TCP,
        flags=socket.AI_ADDRCONFIG | socket.AI_V4MAPPED))
    # getaddrinfo always returns at least 1 (it is an error if there are no
    # matches), so we do not need to check for the empty case
    (family, type_, proto, canonname, sockaddr) = addrs[0]
    for port in ports:
        while True:
            sock = socket.socket(family=family, type=type_, proto=proto)
            with contextlib.closing(sock):
                sock.setblocking(False)
                try:
                    yield From(loop.sock_connect(sock, (sockaddr[0], port)))
                except OSError as e:
                    logging.debug('Port %d on %s not ready: %s', port, host, e)
                    yield From(trollius.sleep(1, loop=loop))
                    pass
                else:
                    break
        logging.debug('Port %d on %s ready', port, host)


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
        """Append the half-open range `[start, stop)`."""
        if stop > start:
            self._ranges.append((start, stop))
            self._len += stop - start

    def add_resource(self, resource):
        """Append a :class:`mesos_pb2.Resource`."""
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
    """Allocates unique task IDs, with a custom prefix on the name.

    Because IDs must be globally unique (within the framework), the
    ``__new__`` method is overridden to return a per-prefix singleton.
    """

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
    tasks by subclassing both this class and :class:`PhysicalNode`.
    """
    def __init__(self, image_resolver, task_id_allocator):
        self.image_resolver = image_resolver
        self.task_id_allocator = task_id_allocator


class InsufficientResourcesError(RuntimeError):
    """Internal error used when there are currently insufficient resources for
    an operation. Users should not see this error.
    """
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

    Attributes
    ----------
    cpus : float
        Mesos CPU shares.
    gpus : float
        GPU resources required
    mem : float
        Mesos memory reservation (megabytes)
    cores : list of str
        Reserved CPU cores. If this is empty, then the task will not be pinned.
        If it is non-empty, then the task will be pinned to the assigned cores,
        and no other tasks will be pinned to those cores. In addition, the
        cores will all be taken from the same NUMA socket, and network
        interfaces in :attr:`networks` will also come from the same socket.
        The strings in the list become keys in :attr:`Task.cores`. You can use
        ``None`` if you don't need to use the core numbers in the command to
        pin individual threads.
    ports : list of str
        Network service ports. Each element in the list is a logical name for
        the port.
    wait_ports : list of str
        Subset of `ports` which must be open before the task is considered
        ready. If set to `None`, defaults to `ports`.
    networks : list
        Abstract names of networks for which network interfaces are required.
    image : str
        Base name of the Docker image (without registry or tag).
    container : `mesos_pb2.ContainerInfo`
        Modify this to override properties of the container. The `image` and
        `force_pull_image` fields are set by the :class:`ImageResolver`,
        overriding anything set here.
    command : list of str
        Command to run inside the image. Each element is passed through
        :meth:`str.format` to generate a command for the physical node. The
        following keys are available for substitution.
        - `ports` : dictionary of port numbers
        - `cores` : dictionary of reserved core IDs
        - `interfaces` : dictionary mapping requested networks to :class:`Interface` objects
        - `endpoints` : dictionary of remote endpoints. Keys are of the form
          :samp:`{service}_{port}`, and values are :class:`Endpoint` objects.
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
        """Allocate the resources for a logical task, if possible.

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
    """States in a task's lifecycle. In almost all cases, tasks only ever move
    forward through the states, not backwards. An exception is that a launch
    may be cancelled while still waiting for resources for the task, in which
    case it moves from :const:`STARTING` to :const:`NOT_READY`.
    """
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

    Attributes
    ----------
    logical_node : :class:`LogicalNode`
        The logical node passed to the constructor
    loop : :class:`trollius.BaseEventLoop`
        The event loop used for constructing futures etc
    state : :class:`TaskState`
        The current state of the task (see :class:`TaskState`). Do not
        modify this directly; use :meth:`set_state` instead.
    ready_event : :class:`trollius.Event`
        An event that becomes set once the task reaches either
        :class:`~TaskState.READY` or :class:`~TaskState.DEAD`. It is never
        unset.
    dead_event : :class:`trollius.Event`
        An event that becomes set once the task reaches
        :class:`~TaskState.DEAD`.
    """
    def __init__(self, logical_node, loop):
        self.logical_node = logical_node
        self.name = logical_node.name
        self.state = TaskState.NOT_READY
        self.ready_event = trollius.Event(loop=loop)
        self.dead_event = trollius.Event(loop=loop)
        self.loop = loop

    def resolve(self, resolver, graph):
        """Make final preparations immediately before starting.

        Parameters
        ----------
        resolver : :class:`Resolver`
            Resolver for images etc.
        graph : :class:`networkx.MultiDiGraph`
            Physical graph containing the task
        """
        pass

    def set_state(self, state):
        """Update :attr:`state`.

        This checks for invalid transitions, and sets the events when
        appropriate.

        Subclasses may override this to take special actions on particular
        transitions, but should chain to the base class.
        """
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

    Attributes
    ----------
    host : str
        Host on which this node is operating (if any).
    ports : dict
        Dictionary mapping logical port names to port numbers.
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
    (during its life cycle, not at construction).

    Most of the attributes of this class only contain meaningful information
    after either :meth:`allocate` or :meth:`resolve` has been called.

    Parameters
    ----------
    logical_task : :class:`LogicalTask`
        Logical task forming the template for this physical task
    loop : :class:`trollius.BaseEventLoop`
        The event loop used for constructing futures etc

    Attributes
    ----------
    interfaces : dict
        Map from logical interface names requested by
        :class:`LogicalTask.interfaces` to :class:`Interface`s.
    taskinfo : :class:`mesos_pb2.TaskInfo`
        Task info for Mesos to run the task
    allocation : class:`ResourceAllocation`
        Resources allocated to this task.
    status : class:`mesos_pb2.TaskStatus`
        Last Mesos status allocated to this task. This should agree with with
        :attr:`state`, but if we receive notifications out-of-order from Mesos
        then it might not. If :attr:`state` is :const:`TaskState.DEAD` then it
        is guaranteed to be in sync.
    ports : dict
        Maps port names given in the logical task to port numbers.
    cores : dict
        Maps core names given in the logical task to core numbers.
    agent : :class:`Agent`
        Information about the agent on which this task is running.
    host : str
        Host on which this task is running
    slave_id : str
        Slave ID of the agent on which this task is running
    _poller : :class:`trollius.Task`
        Task which asynchronously waits for the task's ports to open up. It is
        started on reaching :class:`~TaskState.RUNNING`.
    """
    def __init__(self, logical_task, loop):
        super(PhysicalTask, self).__init__(logical_task, loop)
        self.interfaces = {}
        self.endpoints = {}
        self.taskinfo = None
        self.allocation = None
        self.status = None
        for r in RANGE_RESOURCES:
            setattr(self, r, {})
        self._poller = None

    @property
    def agent(self):
        if self.allocation:
            return self.allocation.agent
        else:
            return None

    @property
    def host(self):
        if self.allocation:
            return self.allocation.agent.host
        else:
            return None

    @property
    def slave_id(self):
        if self.allocation:
            return self.allocation.agent.slave_id
        else:
            return None

    def allocate(self, allocation):
        """Assign resources. This is called just before moving to
        :const:`TaskState.STARTED`, and before :meth:`resolve`.

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
        for src, trg, attr in graph.out_edges_iter([self], data=True):
            if 'port' in attr:
                port = attr['port']
                endpoint_name = '{}_{}'.format(trg.logical_node.name, port)
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
        # The poller is stopped by set_state, so we do not need to do it here.
        driver.killTask(self.taskinfo.task_id)

    def set_state(self, state):
        def callback(future):
            # This callback is called when the poller is either finished or
            # cancelled. If it is finished, we would normally not have
            # advanced beyond READY, because set_state will cancel the
            # poller if we do. However, there is a race condition if
            # set_state is called between the poller completing and the
            # call to this callback.
            self._poller = None
            try:
                future.result()  # throw the exception, if any
                if self.state < TaskState.READY:
                    self.set_state(TaskState.READY)
            except trollius.CancelledError:
                pass
            except Exception:
                logging.error('exception while polling for open ports', exc_info=True)

        super(PhysicalTask, self).set_state(state)
        if state == TaskState.RUNNING and self._poller is None:
            if self.logical_node.wait_ports is not None:
                wait_ports = [self.ports[port] for port in self.logical_node.wait_ports]
            else:
                wait_ports = six.viewvalues(self.ports)
            if wait_ports:
                self._poller = trollius.async(poll_ports(self.host, wait_ports, self.loop))
                self._poller.add_done_callback(callback)
            else:
                self.set_state(TaskState.READY)
        elif state > TaskState.READY and self._poller is not None:
            self._poller.cancel()
            self._poller = None


def instantiate(logical_graph, loop):
    """Create a physical graph from a logical one. Each physical node is
    created by calling :attr:`LogicalNode.physical_factory` on the
    corresponding logical node. Edges, and graph, node and edge attributes are
    transferred as-is.

    Parameters
    ----------
    logical_graph : :class:`networkx.MultiDiGraph`
        Logical graph to instantiate
    loop : :class:`trollius.BaseEventLoop`
        Event loop used to create futures
    """
    # Create physical nodes
    mapping = {logical: logical.physical_factory(logical, loop)
               for logical in logical_graph}
    return networkx.relabel_nodes(logical_graph, mapping)


@decorator
def run_in_event_loop(func, *args, **kw):
    args[0]._loop.call_soon_threadsafe(func, *args, **kw)


class Scheduler(mesos.interface.Scheduler):
    """Top-level scheduler implementing the Mesos Scheduler API.

    Mesos calls the callbacks provided in this class from another thread. To
    ensure thread safety, they are all posted to the event loop via a
    decorator.

    The following invariants are maintained at each yield point:
    - if :attr:`_pending` is empty, then so is :attr:`_offers`
    - if :attr:`_pending` is empty, then offers are suppressed, otherwise they
      are active (this is not true in the initial state, but becomes true as
      soon as an offer is received).
    - each dictionary within :attr:`_offers` is non-empty
    """
    def __init__(self, loop):
        self._loop = loop
        self._driver = None
        self._offers = {}           #: offers keyed by slave ID then offer ID
        self._pending = deque()     #: node groups for which we do not yet have resources
        self._active = {}           #: (task, graph) for tasks that have been launched (STARTED to KILLED), indexed by task ID
        self._closing = False       #: set to ``True`` when :meth:`close` is called

    def _clear_offers(self):
        """Declines all current offers and suppresses future offers. This is
        called when there are no more pending task groups.
        """
        for offers in six.itervalues(self._offers):
            self._driver.acceptOffers([offer.id for offer in six.itervalues(offers)], [])
        self._offers = {}
        self._driver.suppressOffers()

    def _try_launch(self):
        """Check whether it is possible to launch the next task group."""
        if self._offers:
            assert self._pending
            try:
                (graph, nodes, resolver) = self._pending[0]
                # Due to concurrency, another coroutine may have altered
                # the state of the tasks since they were put onto the
                # pending list (e.g. by killing them). Filter those out.
                nodes = [node for node in nodes if node.state == TaskState.STARTING]
                agents = [Agent(list(six.itervalues(offers)))
                          for offers in six.itervalues(self._offers)]
                # Sort agents by GPUs then free memory. This makes it less likely that a
                # low-memory process will hog all the other resources on a high-memory
                # box. Similarly, non-GPU tasks will only use GPU-equipped agents if
                # there is no other choice. Should eventually look into smarter
                # algorithms e.g. Dominant Resource Fairness
                # (http://mesos.apache.org/documentation/latest/allocation-module/)
                agents.sort(key=lambda agent: (agent.gpus, agent.mem))
                allocations = []
                for node in nodes:
                    allocation = None
                    if isinstance(node, PhysicalTask):
                        for agent in agents:
                            try:
                                allocation = agent.allocate(node.logical_node)
                                break
                            except InsufficientResourcesError as e:
                                logger.debug('Cannot add %s to %s: %s',
                                             node.name, agent.slave_id, e)
                        else:
                            raise InsufficientResourcesError(
                                'No agent found for {}'.format(node.name))
                        allocations.append((node, allocation))
            except InsufficientResourcesError:
                logger.debug('Could not launch graph due to insufficient resources')
            else:
                # Two-phase resolving
                logger.debug('Allocating resources to tasks')
                for (node, allocation) in allocations:
                    node.allocate(allocation)
                logger.debug('Performing resolution')
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
                    self._driver.launchTasks(offer_ids, taskinfos[agent])
                    logger.info('Launched %d tasks on %s', len(taskinfos[agent]), agent.slave_id)
                self._offers = {}
                self._pending.popleft()
                if not self._pending:
                    self._clear_offers()
                for node in nodes:
                    node.set_state(TaskState.STARTED)
                for (node, _) in allocations:
                    self._active[node.taskinfo.task_id.value] = (node, graph)

    def set_driver(self, driver):
        self._driver = driver

    def registered(self, driver, framework_id, master_info):
        pass

    @run_in_event_loop
    def resourceOffers(self, driver, offers):
        for offer in offers:
            self._offers.setdefault(offer.slave_id.value, {})[offer.id.value] = offer
        if self._pending:
            self._try_launch()
        else:
            self._clear_offers()

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

    @run_in_event_loop
    def statusUpdate(self, driver, status):
        logging.debug(
            'Update: task %s in state %s (%s)',
            status.task_id.value, mesos_pb2.TaskState.Name(status.state),
            status.message)
        task = self._active.get(status.task_id.value)[0]
        if task:
            if status.state in TERMINAL_STATUSES:
                task.set_state(TaskState.DEAD)
                del self._active[status.task_id.value]
            elif status.state == mesos_pb2.TASK_RUNNING:
                if task.state < TaskState.RUNNING:
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
            if not self._pending:
                self._driver.reviveOffers()
            self._pending.append(pending)
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
                    if not self._pending:
                        self._clear_offers()
                raise

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
        # Find the graphs that are still running
        graphs = set()
        for (task, graph) in six.itervalues(self._active):
            graphs.add(graph)
        for graph in graphs:
            yield From(self.kill(graph))
        self._driver.stop()
        status = yield From(self._loop.run_in_executor(None, self._driver.join))
        raise Return(status)


__all__ = [
    'LogicalNode', 'PhysicalNode',
    'LogicalExternal', 'PhysicalExternal',
    'LogicalTask', 'PhysicalTask', 'TaskState',
    'Interface', 'ResourceAllocation',
    'ImageResolver', 'TaskIDAllocator', 'Resolver',
    'Scheduler', 'instantiate']
