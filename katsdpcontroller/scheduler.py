"""
Mesos-based scheduler for launching collections of inter-dependent tasks. It
uses asyncio for asynchronous execution. It is **not** thread-safe.

Graphs
------
The scheduler primarily operates on *graphs*. There are two types of graphs:
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
Edges carry information about dependencies, which are indicated by different
edge attributes. An edge with no attributes carries no semantic information for
the scheduler. Users of the scheduler can also define extra attributes, which
are similarly ignored. The defined attributes on an edge from A to B are:

depends_resources
   Task A needs knowledge of the resources assigned to task B. This will
   ensure that B is started either before or at the same time as A. It is
   guaranteed that B's resources are assigned before A is resolved (but not
   necessarily before B is resolved). It is legal for these edges to form a
   cycle.

depends_resolve
   Task A must be resolved after task B. This implies `depends_resources`.
   These edges must not form a cycle.

port
   Task A needs to connect to a service provided by task B. Set the `port`
   edge attribute to the name of the port on B. The endpoint on B is provided
   to A as part of resolution. This automatically implies
   `depends_resources`.

depends_ready
   Task B needs to be *ready* (running and listening on its ports) before task
   A starts. Note that this only determines startup ordering, and has no
   effect on shutdown. For example, task B might put some information in a
   database before it comes ready, which is consumed by task A, but task A
   does not require B to remain alive. It is an error for these edges to form
   a cycle.

depends_kill
   Task A must be killed before task B. This is the shutdown counterpart to
   `depends_ready`. It is an error for these edges to form a cycle.

Queues
------
The scheduler supports multiple queues, but these aren't quite the same as the
queues in a classic HPC batch system. Each call to :meth:`Scheduler.launch`
targets a particular queue. Concurrent launch requests within a queue are
serviced strictly in order, while launch requests to separate queues may be
serviced out-of-order, depending on availability of resources. Note that jobs
in a queue *can* run in parallel: queues are not a tool to enforce
serialisation of dependent jobs.

The scheduler does not currently scale well with large numbers of queues. Thus,
it is recommended that batch jobs with the same resource requirements are
grouped into a single queue, since there is no benefit to launching out of
order. Batch jobs with very different requirements (e.g. non-overlapping)
should go into different queues to avoid head-of-line blocking.

GPU support
-----------
GPUs are supported independently of Mesos' built-in GPU support, which is not
very flexible. Mesos allows only a whole number of GPUs to be allocated to a
task, and they are reserved exclusively for that task. katsdpcontroller allows
tasks to share a GPU, and provides resources for compute and memory. There is
no isolation of GPU memory.

To use this support, the agent must be configured with attributes and
resources. The attribute :code:`katsdpcontroller.gpus` must be a
base64url-encoded JSON string, describing a list of GPUs, and conforming to
:const:`katsdpcontroller.schemas.GPUS`. For the ith GPU in this list (counting
from 0), there must be corresponding resources
:samp:`katsdpcontroller.gpu.{i}.compute` (generally with capacity 1.0) and
:samp:`katsdpcontroller.gpu.{i}.mem` (in MiB).

The node must also provide nvidia-docker-plugin so that driver volumes can be
loaded.

Agent prioritisation
--------------------
Each agent is assigned a 'priority', and tasks are assigned to the
lowest-priority agent where they fit (so that specialised tasks that can only
run on one agent are not blocked by more generic tasks being assigned to that
agent). By default, the priority is the total number of volumes, interfaces and
GPUs it has. This can be overridden by assigning a `katsdpcontroller.priority`
scalar attribute. Ties are broken by amount of available memory.

Setting up agents
-----------------
The previous sections list a number of resources and attributes that need to be
configured on each agent. To simplify this, a script
(:file:`scripts/agent_mkconfig.py`) is provided that will interrogate most of
the necessary information from the system. It assumes that Mesos is run with
system wrappers that source arguments from :file:`/etc/mesos-slave/attributes`
and :file:`/etc/mesos-slave/resources` (this is the case for the Ubuntu
packages; untested for other operating systems).

Note that Mesos slaves typically require a manual `recovery`_ step after
changing resources or attributes and restarting.

.. _recovery: http://mesos.apache.org/documentation/latest/agent-recovery/
"""

import os.path
import logging
import json
import itertools
import re
import base64
import socket
import contextlib
import copy
from collections import namedtuple, deque
from enum import Enum
import asyncio
import urllib
import functools
import ssl
import ipaddress

import pkg_resources
import docker
import networkx
import jsonschema
from decorator import decorator
from addict import Dict
import pymesos

import aiohttp.web

from katsdptelstate.endpoint import Endpoint

from . import schemas


SCALAR_RESOURCES = ['cpus', 'mem', 'disk']
GPU_SCALAR_RESOURCES = ['compute', 'mem']
INTERFACE_SCALAR_RESOURCES = ['bandwidth_in', 'bandwidth_out']
RANGE_RESOURCES = ['ports', 'cores']
#: Mesos task states that indicate that the task is dead (see https://github.com/apache/mesos/blob/1.0.1/include/mesos/mesos.proto#L1374)
TERMINAL_STATUSES = frozenset([
    'TASK_FINISHED',
    'TASK_FAILED',
    'TASK_KILLED',
    'TASK_LOST',
    'TASK_ERROR'])
# Names for standard edge attributes, to give some protection against typos
DEPENDS_READY = 'depends_ready'
DEPENDS_RESOURCES = 'depends_resources'
DEPENDS_RESOLVE = 'depends_resolve'
DEPENDS_KILL = 'depends_kill'
logger = logging.getLogger(__name__)


Volume = namedtuple('Volume', ['name', 'host_path', 'numa_node'])
"""Abstraction of a host path offered by an agent.

Volumes are defined by setting the Mesos attribute
:code:`katsdpcontroller.volumes`, whose value is a JSON string that adheres
to the schema in :const:`katsdpcontroller.schemas.VOLUMES`.

Attributes
----------
name : str
    A logical name that indicates the purpose of the path
host_path : str
    Path on the host machine
numa_node : int, optional
    Index of the NUMA socket to which the storage is connected
"""


class GPURequest:
    """Request for resources on a single GPU. These resources are not isolated,
    so the request functions purely to ensure that the scheduler does not try
    to over-allocate a GPU.

    Attributes
    ----------
    compute : float
        Fraction of GPU's compute resource consumed
    mem : float
        Memory usage (megabytes)
    affinity : bool
        If true, the GPU must be on the same NUMA node as the chosen CPU
        cores (ignored if no CPU cores are reserved).
    name : str, optional
        If specified, the name of the GPU must match this.
    """
    def __init__(self):
        for r in GPU_SCALAR_RESOURCES:
            setattr(self, r, 0.0)
        self.affinity = False
        self.name = None

    def matches(self, agent_gpu, numa_node):
        if self.name is not None and self.name != agent_gpu.name:
            return False
        return numa_node is None or not self.affinity or agent_gpu.numa_node == numa_node


class InterfaceRequest:
    """Request for resources on a network interface. At the moment only
    a logical network name can be specified, but this may be augmented in
    future to allocate bandwidth.

    Attributes
    ----------
    network : str
        Logical network name
    infiniband : bool
        If true, the device must support Infiniband APIs (e.g. ibverbs), and
        the corresponding devices will be passed through.
    affinity : bool
        If true, the network device must be on the same NUMA node as the chosen
        CPU cores (ignored if no CPU cores are reserved).
    bandwidth_in : float
        Ingress bandwidth, in bps
    bandwidth_out : float
        Egress bandwidth, in bps
    """
    def __init__(self, network, infiniband=False, affinity=False):
        self.network = network
        self.infiniband = infiniband
        self.affinity = affinity
        for r in INTERFACE_SCALAR_RESOURCES:
            setattr(self, r, 0.0)

    def matches(self, interface, numa_node):
        if self.affinity and numa_node is not None and interface.numa_node != numa_node:
            return False
        if self.infiniband and not interface.infiniband_devices:
            return False
        return self.network == interface.network


class VolumeRequest:
    """Request to mount a host directory on the agent.

    Attributes
    ----------
    name : str
        Logical name advertised by the agent
    container_path : str
        Mount point inside the container
    mode : {'RW', 'RO'}
        Read-write or Read-Only mode
    affinity : bool
        If true, the storage must be on the same NUMA node as the chosen
        CPU cores (ignored if no CPU cores are reserved).
    """
    def __init__(self, name, container_path, mode, affinity=False):
        self.name = name
        self.container_path = container_path
        self.mode = mode
        self.affinity = affinity

    def matches(self, volume, numa_node):
        if self.affinity and numa_node is not None and volume.numa_node != numa_node:
            return False
        return volume.name == self.name


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


async def poll_ports(host, ports, loop):
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
    loop : :class:`asyncio.AbstractEventLoop`
        The event loop used for socket operations

    Raises
    ------
    OSError
        on any socket operation errors other than the connect
    """
    # protect against temporary name resolution failure.
    # in the case of permanent DNS failure this will block
    # indefinitely and higher level timeouts will be needed
    while True:
        try:
            addrs = await (loop.getaddrinfo(
                host=host, port=None,
                type=socket.SOCK_STREAM,
                proto=socket.IPPROTO_TCP,
                flags=socket.AI_ADDRCONFIG | socket.AI_V4MAPPED))
        except socket.gaierror as error:
            logger.error('Failure to resolve address for %s (%s). Waiting 5s to retry.',
                         host, error)
            await asyncio.sleep(5, loop=loop)
        else:
            break

    # getaddrinfo always returns at least 1 (it is an error if there are no
    # matches), so we do not need to check for the empty case
    (family, type_, proto, canonname, sockaddr) = addrs[0]
    for port in ports:
        while True:
            sock = socket.socket(family=family, type=type_, proto=proto)
            with contextlib.closing(sock):
                sock.setblocking(False)
                try:
                    await loop.sock_connect(sock, (sockaddr[0], port))
                except OSError as error:
                    logger.debug('Port %d on %s not ready: %s', port, host, error)
                    await asyncio.sleep(1, loop=loop)
                else:
                    break
        logger.debug('Port %d on %s ready', port, host)


class RangeResource:
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
        return itertools.chain(*[range(start, stop) for (start, stop) in self._ranges])

    def remove(self, item):
        for i, (start, stop) in enumerate(self._ranges):
            if item >= start and item < stop:
                if item == start and item + 1 == stop:
                    del self._ranges[i]
                elif item == start:
                    self._ranges[i] = (start + 1, stop)
                elif item + 1 == stop:
                    self._ranges[i] = (start, stop - 1)
                else:
                    # Need to split the range. deque doesn't have .insert, so we
                    # have to fake it using rotations.
                    self._ranges.rotate(-i)
                    self._ranges[0] = (item + 1, stop)
                    self._ranges.append((start, item))
                    self._ranges.rotate(i + 1)
                return
        raise ValueError('item not in list')

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

    def popleft_min(self, bound):
        """Remove the first element greater than or equal to `bound`.

        Raises
        ------
        IndexError
            If no such element exists
        """
        for _, (start, stop) in enumerate(self._ranges):
            if stop > bound:
                value = max(start, bound)
                self.remove(value)
                return value
        raise IndexError('no element greater than or equal to bound')

    def __str__(self):
        def format_range(rng):
            start, stop = rng
            if stop - start == 1:
                return '{}'.format(start)
            else:
                return '{}-{}'.format(start, stop - 1)
        return ','.join(format_range(rng) for rng in self._ranges)


class GPUResourceAllocation:
    """Collection of specific resources allocated from a single agent GPU.

    Attributes
    ----------
    index : int
        Index into the agent's list of GPUs
    """
    def __init__(self, index):
        self.index = index
        for r in GPU_SCALAR_RESOURCES:
            setattr(self, r, 0.0)


class InterfaceResourceAllocation:
    """Collection of specific resources allocated from a single agent network interface.

    Attributes
    ----------
    index : int
        Index into the agent's list of interfaces
    """
    def __init__(self, index):
        self.index = index
        for r in INTERFACE_SCALAR_RESOURCES:
            setattr(self, r, 0.0)


class ResourceAllocation:
    """Collection of specific resources allocated from a collection of offers.

    Attributes
    ----------
    agent : :class:`Agent`
        Agent from which the resources are allocated
    """
    def __init__(self, agent):
        self.agent = agent
        for r in SCALAR_RESOURCES:
            setattr(self, r, 0.0)
        for r in RANGE_RESOURCES:
            setattr(self, r, [])
        self.gpus = []
        self.interfaces = []
        self.volumes = []


class ImageResolver:
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
        It does not affect overrides, to allow them to specify their own tags.
    tag : str, optional
        If specified, `tag_file` is ignored and this tag is used.
    use_digests : bool, optional
        Whether to look up the latest digests from the `registry`. If this is
        not specified, old versions of images on the agents could be used.
    """
    def __init__(self, private_registry=None, tag_file=None, tag=None, use_digests=True):
        self._tag_file = tag_file
        self._private_registry = private_registry
        self._overrides = {}
        self._cache = {}
        self._use_digests = use_digests
        if tag is not None:
            self._tag = tag
            self._tag_file = None
        elif self._tag_file is None:
            self._tag = 'latest'
        else:
            with open(self._tag_file, 'r') as f:
                self._tag = f.read().strip()
                # This is a regex that appeared in older versions of Docker
                # (see https://github.com/docker/docker/pull/8447/files).
                # It's probably a reasonable constraint so that we don't allow
                # whitespace, / and other nonsense, even if Docker itself no
                # longer enforces it.
                if not re.match(r'^[\w][\w.-]{0,127}$', self._tag):
                    raise ValueError('Invalid tag {} in {}'.format(repr(self._tag), self._tag_file))
        if use_digests and private_registry is not None:
            authconfig = docker.auth.load_config()
            authdata = docker.auth.resolve_authconfig(authconfig, private_registry)
            if authdata is None:
                self._auth = None
            else:
                self._auth = aiohttp.BasicAuth(authdata['username'], authdata['password'])
        else:
            self._auth = None

    @property
    def tag(self):
        return self._tag

    @property
    def overrides(self):
        return dict(self._overrides)

    def override(self, name, path):
        self._overrides[name] = path

    async def __call__(self, name, loop):
        if name in self._overrides:
            return self._overrides[name]
        elif name in self._cache:
            return self._cache[name]

        colon = name.rfind(':')
        if colon != -1:
            # A tag was already specified in the graph
            logger.warning("Image %s has a predefined tag, ignoring tag %s", name, self._tag)
            tag = name[colon + 1:]
            repo = name[:colon]
        else:
            tag = self._tag
            repo = name

        if self._private_registry is None:
            resolved = 'sdp/{}:{}'.format(repo, tag)
        elif self._use_digests:
            # TODO: see if it's possible to do some connection pooling
            # here. That probably requires the caller to initiate a
            # Session and close it when done.
            url = '{}/v2/{}/manifests/{}'.format(self._private_registry, repo, tag)
            if not url.startswith('http'):
                # If no scheme is specified, assume https
                url = 'https://' + url
            kwargs = dict(
                headers={'Accept': 'application/vnd.docker.distribution.manifest.v2+json'},
                auth=self._auth)
            cafile = '/etc/ssl/certs/ca-certificates.crt'
            if os.path.exists(cafile):
                ssl_context = ssl.create_default_context(cafile=cafile)
            else:
                ssl_context = None
            with aiohttp.ClientSession(loop=loop, **kwargs) as session:
                try:
                    # Use a low timeout, so that we don't wedge the entire launch if
                    # there is a connection problem.
                    async with session.head(url, timeout=5, ssl_context=ssl_context) as response:
                        response.raise_for_status()
                        digest = response.headers['Docker-Content-Digest']
                except aiohttp.client.ClientError as error:
                    raise ImageError('Failed to get digest from {}: {}'.format(url, error)) \
                        from error
                except KeyError:
                    raise ImageError('Docker-Content-Digest header not found for {}'.format(url))
            resolved = '{}/{}@{}'.format(self._private_registry, repo, digest)
        else:
            resolved = '{}/{}:{}'.format(self._private_registry, repo, tag)
        if name in self._cache:
            # Another asynchronous caller beat us to it. Use the value
            # that caller put in the cache so that calls with the same
            # name always return the same value.
            resolved = self._cache[name]
            logger.debug('ImageResolver race detected resolving %s to %s', name, resolved)
        else:
            logger.debug('ImageResolver resolved %s to %s', name, resolved)
            self._cache[name] = resolved
        return resolved


class ImageResolverFactory:
    """Factory for generating image resolvers. An :class:`ImageResolver`
    caches lookups, so it is useful to be able to generate a new one to
    receive fresh information.

    See :class:`ImageResolver` for an explanation of the constructor
    arguments and :meth:`~ImageResolver.override`.
    """
    def __init__(self, private_registry=None, tag_file=None, tag=None, use_digests=True):
        self._args = dict(private_registry=private_registry,
                          tag_file=tag_file, tag=tag, use_digests=use_digests)
        self._overrides = {}

    def override(self, name, path):
        self._overrides[name] = path

    def __call__(self, **kwargs):
        args = dict(self._args)
        args.update(kwargs)
        image_resolver = ImageResolver(**args)
        for name, path in self._overrides.items():
            image_resolver.override(name, path)
        return image_resolver


class TaskIDAllocator:
    """Allocates unique task IDs, with a custom prefix on the name.

    Because IDs must be globally unique (within the framework), the
    ``__new__`` method is overridden to return a per-prefix singleton.
    """
    _by_prefix = {}

    def __init__(self, prefix=''):
        pass   # Initialised by new

    def __new__(cls, prefix=''):
        # Obtain the singleton
        try:
            return TaskIDAllocator._by_prefix[prefix]
        except KeyError:
            alloc = super(TaskIDAllocator, cls).__new__(cls)
            alloc._prefix = prefix
            alloc._next_id = 0
            TaskIDAllocator._by_prefix[prefix] = alloc
            return alloc

    def __call__(self):
        ret = self._prefix + str(self._next_id).zfill(8)
        self._next_id += 1
        return ret


class Resolver:
    """General-purpose base class to connect extra resources to a graph. The
    base implementation contains an :class:`ImageResolver`, a
    :class:`TaskIDAllocator` and the URL for the scheduler's HTTP server.
    However, other resources can be connected to tasks by subclassing both this
    class and :class:`PhysicalNode`.
    """
    def __init__(self, image_resolver, task_id_allocator, http_url):
        self.image_resolver = image_resolver
        self.task_id_allocator = task_id_allocator
        self.http_url = http_url


class InsufficientResourcesError(RuntimeError):
    """Indicates that there are insufficient resources to launch a task. This
    is also used for internal operations when trying to allocate a specific
    task to a specific agent.
    """
    pass


class NoOffersError(InsufficientResourcesError):
    """Indicates that resources were insufficient because no offers were
    received before the timeout.
    """
    def __init__(self):
        super().__init__("No offers were received")


class TaskNoAgentError(InsufficientResourcesError):
    """Indicates that no agent was suitable for a task. Where possible, a
    sub-class is used to indicate a more specific error.
    """
    def __init__(self, node):
        self.node = node

    def __str__(self):
        return "No agent was found suitable for {0.node.name}".format(self)


class TaskInsufficientResourcesError(TaskNoAgentError):
    """Indicates that a specific task required more of some resource than
    were available on any agent."""
    def __init__(self, node, resource, needed, available):
        super().__init__(node)
        self.resource = resource
        self.needed = needed
        self.available = available

    def __str__(self):
        return ("Not enough {0.resource} for {0.node.name} on any agent "
                "({0.needed} > {0.available})".format(self))


class TaskInsufficientGPUResourcesError(TaskNoAgentError):
    """Indicates that a specific task GPU request needed more of some resource
    than was available on any agent GPU."""
    def __init__(self, node, request_index, resource, needed, available):
        super().__init__(node)
        self.request_index = request_index
        self.resource = resource
        self.needed = needed
        self.available = available

    def __str__(self):
        return ("Not enough GPU {0.resource} for {0.node.name} (request #{0.request_index}) "
                "on any agent ({0.needed} > {0.available})".format(self))


class TaskInsufficientInterfaceResourcesError(TaskNoAgentError):
    """Indicates that a specific task interface request needed more of some
    resource than was available on any agent interface."""
    def __init__(self, node, request, resource, needed, available):
        super().__init__(node)
        self.request = request
        self.resource = resource
        self.needed = needed
        self.available = available

    def __str__(self):
        return ("Not enough interface {0.resource} on {0.request.network} for "
                "{0.node.name} on any agent ({0.needed} > {0.available})".format(self))


class TaskNoInterfaceError(TaskNoAgentError):
    """Indicates that a task required a network interface that was not present on any agent."""
    def __init__(self, node, request):
        super().__init__(node)
        self.request = request

    def __str__(self):
        return ("No agent matches {1}network request for {0.request.network} from "
                "task {0.node.name}".format(self, "Infiniband " if self.request.infiniband else ""))


class TaskNoVolumeError(TaskNoAgentError):
    """Indicates that a task required a volume that was not present on any agent."""
    def __init__(self, node, request):
        super().__init__(node)
        self.request = request

    def __str__(self):
        return ("No agent matches volume request for {0.request.name} "
                "from {0.node.name}".format(self))


class TaskNoGPUError(TaskNoAgentError):
    """Indicates that a task required a GPU that did not match any agent"""
    def __init__(self, node, request_index):
        super().__init__(node)
        self.request_index = request_index

    def __str__(self):
        return "No agent matches GPU request #{0.request_index} from {0.node.name}".format(self)


class GroupInsufficientResourcesError(InsufficientResourcesError):
    """Indicates that a group of tasks collectively required more of some
    resource than were available between all the agents."""
    def __init__(self, resource, needed, available):
        super().__init__()
        self.resource = resource
        self.needed = needed
        self.available = available

    def __str__(self):
        return ("Insufficient total {0.resource} to launch all tasks "
                "({0.needed} > {0.available})".format(self))


class GroupInsufficientGPUResourcesError(InsufficientResourcesError):
    """Indicates that a group of tasks collectively required more of some
    GPU resource than were available between all the agents."""
    def __init__(self, resource, needed, available):
        super().__init__()
        self.resource = resource
        self.needed = needed
        self.available = available

    def __str__(self):
        return ("Insufficient total GPU {0.resource} to launch all tasks "
                "({0.needed} > {0.available})".format(self))


class GroupInsufficientInterfaceResourcesError(InsufficientResourcesError):
    """Indicates that a group of tasks collectively required more of some
    interface resource than were available between all the agents."""
    def __init__(self, network, resource, needed, available):
        super().__init__()
        self.network = network
        self.resource = resource
        self.needed = needed
        self.available = available

    def __str__(self):
        return ("Insufficient total interface {0.resource} on network {0.network} "
                "to launch all tasks ({0.needed} > {0.available})".format(self))


class CycleError(ValueError):
    """Raised for a graph that contains an illegal dependency cycle"""
    pass


class DependencyError(ValueError):
    """Raised if a launch is impossible due to an unsatisfied dependency"""
    pass


class ImageError(RuntimeError):
    """Indicates that the Docker image could not be resolved due to a problem
    while contacting the registry.
    """
    pass


class LogicalNode:
    """A node in a logical graph. This is a base class. For nodes that
    execute code and use Mesos resources, see :class:`LogicalTask`.

    Attributes
    ----------
    name : str
        Node name
    wait_ports : list of str
        Subset of ports which must be open before the task is considered
        ready. If set to `None`, defaults to `ports`.
    physical_factory : callable
        Creates the physical task (must return :class:`PhysicalNode`
        or subclass). It is passed the logical task and the event loop.
    """
    def __init__(self, name):
        self.name = name
        self.wait_ports = None
        self.physical_factory = PhysicalNode


class LogicalExternal(LogicalNode):
    """An external service. It is assumed to be running as soon as it starts.

    The host and port must be set manually on the physical node. It also
    defaults to an empty :attr:`~LogicalNode.wait_ports`, which must be
    overridden is waiting is desired."""
    def __init__(self, name):
        super().__init__(name)
        self.physical_factory = PhysicalExternal
        self.wait_ports = []


class LogicalTask(LogicalNode):
    """A node in a logical graph that indicates how to run a task
    and what resources it requires, but does not correspond to any specific
    running task.

    The command is processed by :meth:`str.format` to substitute dynamic
    values.

    Attributes
    ----------
    cpus : float
        Mesos CPU shares.
    mem : float
        Mesos memory reservation (megabytes)
    disk : float
        Mesos disk reservation (megabytes)
    cores : list of str
        Reserved CPU cores. If this is empty, then the task will not be pinned.
        If it is non-empty, then the task will be pinned to the assigned cores,
        and no other tasks will be pinned to those cores. In addition, the
        cores will all be taken from the same NUMA socket, and network
        interfaces in :attr:`interfaces` will also come from the same socket.
        The strings in the list become keys in :attr:`Task.cores`. You can use
        ``None`` if you don't need to use the core numbers in the command to
        pin individual threads.
    ports : list of str
        Network service ports. Each element in the list is a logical name for
        the port.
    interfaces : list
        List of :class:`InterfaceRequest` objects, one per interface requested
    gpus : list
        List of :class:`GPURequest` objects, one per GPU needed
    capabilities : list
        List of Linux capability names to add e.g. `SYS_NICE`. Note that this
        doesn't guarantee that the process can use the capability, if it runs
        as a non-root user.
    image : str
        Base name of the Docker image (without registry or tag).
    container : :class:`addict.Dict`
        Modify this to override properties of the container. The `image` and
        field is set by the :class:`ImageResolver`, overriding anything set
        here.
    command : list of str
        Command to run inside the image. Each element is passed through
        :meth:`str.format` to generate a command for the physical node. The
        following keys are available for substitution.
        - `ports` : dictionary of port numbers
        - `cores` : dictionary of reserved core IDs
        - `interfaces` : dictionary mapping requested networks to :class:`Interface` objects
        - `endpoints` : dictionary of remote endpoints. Keys are of the form
          :samp:`{service}_{port}`, and values are :class:`Endpoint` objects.
        - `resolver` : resolver object
    wrapper : str
        URI for a wrapper around the command. If specified, it will be
        downloaded to the sandbox directory and executed, with the original
        command being passed to it.
    """
    def __init__(self, name):
        super().__init__(name)
        for r in SCALAR_RESOURCES:
            setattr(self, r, 0.0)
        for r in RANGE_RESOURCES:
            setattr(self, r, [])
        self.gpus = []
        self.interfaces = []
        self.volumes = []
        self.capabilities = []
        self.image = None
        self.command = []
        self.wrapper = None
        self.container = Dict()
        self.container.type = 'DOCKER'
        self.physical_factory = PhysicalTask

    def valid_agent(self, agent):
        """Checks whether the attributes of an agent are suitable for running
        this task. Subclasses may override this to enforce constraints e.g.,
        requiring a special type of hardware."""
        # TODO: enforce x86-64, if we ever introduce ARM or other hardware
        return True


class ResourceCollector:
    def inc_attr(self, key, delta):
        setattr(self, key, getattr(self, key) + delta)

    def add_range_attr(self, key, resource):
        getattr(self, key).add_resource(resource)


class AgentGPU(ResourceCollector):
    """A single GPU on an agent machine, tracking both attributes and free resources."""
    def __init__(self, spec):
        self.devices = spec['devices']
        self.driver_version = spec['driver_version']
        self.name = spec['name']
        self.compute_capability = tuple(spec['compute_capability'])
        self.device_attributes = spec['device_attributes']
        self.numa_node = spec.get('numa_node')
        for r in GPU_SCALAR_RESOURCES:
            setattr(self, r, 0.0)


class AgentInterface(ResourceCollector):
    """A single interface on an agent machine, trackign both attributes and free resources.

    Attributes
    ----------
    name : str
        Kernel interface name
    network : str
        Logical name for the network to which the interface is attached
    ipv4_address : :class:`ipaddress.IPv4Address`
        IPv4 local address of the interface
    numa_node : int, optional
        Index of the NUMA socket to which the NIC is connected
    infiniband_devices : list of str
        Device inodes that should be passed into Docker containers to use Infiniband libraries
    """
    def __init__(self, spec):
        self.name = spec['name']
        self.network = spec['network']
        self.ipv4_address = ipaddress.IPv4Address(spec['ipv4_address'])
        self.numa_node = spec.get('numa_node')
        self.infiniband_devices = spec.get('infiniband_devices', [])
        for r in INTERFACE_SCALAR_RESOURCES:
            setattr(self, r, 0.0)


def _decode_json_base64(value):
    """Decodes a object that has been encoded with JSON then url-safe base64."""
    json_bytes = base64.urlsafe_b64decode(value)
    return json.loads(json_bytes.decode('utf-8'))


class Agent(ResourceCollector):
    """Collects multiple offers for a single Mesos agent and allows
    :class:`ResourceAllocation`s to be made from it.

    Parameters
    ----------
    offers : list
        List of Mesos offer dicts
    min_port : int
        A soft lower bound on port numbers to allocate. If higher numbers are
        exhausted, this will be ignored.
    """
    def __init__(self, offers, min_port=0):
        if not offers:
            raise ValueError('At least one offer must be specified')
        self.offers = offers
        self.agent_id = offers[0].agent_id.value
        self.host = offers[0].hostname
        self.attributes = offers[0].attributes
        self.interfaces = []
        self.volumes = []
        self.gpus = []
        self.numa = []
        self.priority = None
        self._min_port = min_port
        for attribute in offers[0].attributes:
            try:
                if attribute.name == 'katsdpcontroller.interfaces' and attribute.type == 'TEXT':
                    value = _decode_json_base64(attribute.text.value)
                    schemas.INTERFACES.validate(value)
                    self.interfaces = [AgentInterface(item) for item in value]
                elif attribute.name == 'katsdpcontroller.volumes' and attribute.type == 'TEXT':
                    value = _decode_json_base64(attribute.text.value)
                    schemas.VOLUMES.validate(value)
                    volumes = []
                    for item in value:
                        volumes.append(Volume(
                            name=item['name'],
                            host_path=item['host_path'],
                            numa_node=item.get('numa_node')))
                    self.volumes = volumes
                elif attribute.name == 'katsdpcontroller.gpus' and attribute.type == 'TEXT':
                    value = _decode_json_base64(attribute.text.value)
                    schemas.GPUS.validate(value)
                    self.gpus = [AgentGPU(item) for item in value]
                elif attribute.name == 'katsdpcontroller.numa' and attribute.type == 'TEXT':
                    value = _decode_json_base64(attribute.text.value)
                    schemas.NUMA.validate(value)
                    self.numa = value
                elif attribute.name == 'katsdpcontroller.priority' and attribute.type == 'SCALAR':
                    self.priority = attribute.scalar.value
            except (ValueError, KeyError, TypeError, ipaddress.AddressValueError):
                logger.warn('Could not parse %s (%s)',
                            attribute.name, attribute.text.value)
                logger.debug('Exception', exc_info=True)
            except jsonschema.ValidationError as e:
                logger.warn('Validation error parsing %s: %s', value, e)

        # These resources all represent resources not yet allocated
        for r in SCALAR_RESOURCES:
            setattr(self, r, 0.0)
        for r in RANGE_RESOURCES:
            setattr(self, r, RangeResource())
        for offer in offers:
            for resource in offer.resources:
                # Skip specialised resource types and use only general-purpose
                # resources.
                if 'disk' in resource and 'source' in resource.disk:
                    continue
                if resource.name in SCALAR_RESOURCES:
                    self.inc_attr(resource.name, resource.scalar.value)
                elif resource.name in RANGE_RESOURCES:
                    self.add_range_attr(resource.name, resource)
                elif resource.name.startswith('katsdpcontroller.gpu.'):
                    parts = resource.name.split('.', 3)
                    # TODO: catch exceptions here
                    index = int(parts[2])
                    resource_name = parts[3]
                    if resource_name in GPU_SCALAR_RESOURCES:
                        self.gpus[index].inc_attr(resource_name, resource.scalar.value)
                elif resource.name.startswith('katsdpcontroller.interface.'):
                    parts = resource.name.split('.', 3)
                    # TODO: catch exceptions here
                    index = int(parts[2])
                    resource_name = parts[3]
                    if resource_name in INTERFACE_SCALAR_RESOURCES:
                        self.interfaces[index].inc_attr(resource_name, resource.scalar.value)
        if self.priority is None:
            self.priority = float(len(self.gpus) +
                                  len(self.interfaces) +
                                  len(self.volumes))
        logger.debug('Agent %s has priority %f', self.agent_id, self.priority)

    @classmethod
    def _match_children(cls, numa_node, requested, actual, scalar_resources, msg):
        """Match requests for child devices (e.g. GPUs) against actual supply.

        This uses a very simple first-come-first-served algorithm which could
        be improved.

        Parameters
        ----------
        numa_node : int, optional
            NUMA node on which the logical task will be assigned
        requested : list
            List of request objects (e.g. :class:`GPURequest`)
        actual : list
            List of objects representing available resources (e.g. :class:`AgentGPU`)
        scalar_resources : list
            Names of scalar resource types to match between `requested` and `actual`
        msg : str
            Name for the children e.g. "GPU" (used only for debug messages)

        Returns
        -------
        assign : list
            For each element of `actual`, the corresponding element of the result
            is either ``None`` or the index of the request that is satisfied by
            that element.

        Raises
        ------
        InsufficientResourcesError
            if the matching could not be done.
        """
        assign = [None] * len(actual)
        for i, request in enumerate(requested):
            use = None
            for j, item in enumerate(actual):
                if assign[j] is not None:
                    continue     # Already been used for a request in this task
                if not request.matches(item, numa_node):
                    continue
                good = True
                for r in scalar_resources:
                    need = getattr(request, r)
                    have = getattr(item, r)
                    if have < need:
                        logger.debug('Not enough %s on %s %d for request %d',
                                     r, msg, j, i)
                        good = False
                        break
                if good:
                    use = j
                    break
            if use is None:
                raise InsufficientResourcesError('No suitable {} found for request {}'
                                                 .format(msg, i))
            assign[use] = i
        return assign

    def _allocate_numa_node(self, numa_node, logical_task):
        # Check that there are sufficient cores on this node
        if numa_node is not None:
            # TODO: would be more efficient to replace self.numa with a map
            # from core to node
            cores = deque(core for core in self.numa[numa_node] if core in self.cores)
        else:
            cores = deque()
        need = len(logical_task.cores)
        have = len(cores)
        if need > have:
            raise InsufficientResourcesError('not enough cores on node {} ({} < {})'.format(
                numa_node, have, need))

        # Match network requests to interfaces
        interface_map = self._match_children(
            numa_node, logical_task.interfaces, self.interfaces,
            INTERFACE_SCALAR_RESOURCES, 'interface')
        # Match volume requests to volumes
        for request in logical_task.volumes:
            if not any(request.matches(volume, numa_node) for volume in self.volumes):
                if not any(request.matches(volume, None) for volume in self.volumes):
                    raise InsufficientResourcesError('Volume {} not present'.format(request.name))
                else:
                    raise InsufficientResourcesError(
                        'Volume {} not present on NUMA node {}'.format(request.name, numa_node))
        # Match GPU requests to GPUs
        gpu_map = self._match_children(
            numa_node, logical_task.gpus, self.gpus,
            GPU_SCALAR_RESOURCES, 'GPU')

        # Have now verified that the task fits. Create the resources for it
        alloc = ResourceAllocation(self)
        for r in SCALAR_RESOURCES:
            need = getattr(logical_task, r)
            self.inc_attr(r, -need)
            setattr(alloc, r, need)
        for r in RANGE_RESOURCES:
            if r == 'cores':
                for _name in logical_task.cores:
                    value = cores.popleft()
                    alloc.cores.append(value)
                    self.cores.remove(value)
            elif r == 'ports':
                for _name in logical_task.ports:
                    try:
                        value = self.ports.popleft_min(self._min_port)
                    except IndexError:
                        value = self.ports.popleft()
                    alloc.ports.append(value)
            else:
                for _name in getattr(logical_task, r):
                    value = getattr(self, r).popleft()
                    getattr(alloc, r).append(value)

        alloc.interfaces = [None] * len(logical_task.interfaces)
        for i, interface in enumerate(self.interfaces):
            idx = interface_map[i]
            if idx is not None:
                request = logical_task.interfaces[idx]
                interface_alloc = InterfaceResourceAllocation(i)
                for r in INTERFACE_SCALAR_RESOURCES:
                    need = getattr(request, r)
                    interface.inc_attr(r, -need)
                    setattr(interface_alloc, r, need)
                alloc.interfaces[idx] = interface_alloc
        for request in logical_task.volumes:
            alloc.volumes.append(next(volume for volume in self.volumes
                                      if request.matches(volume, numa_node)))
        alloc.gpus = [None] * len(logical_task.gpus)
        for i, gpu in enumerate(self.gpus):
            idx = gpu_map[i]
            if idx is not None:
                request = logical_task.gpus[idx]
                gpu_alloc = GPUResourceAllocation(i)
                for r in GPU_SCALAR_RESOURCES:
                    need = getattr(request, r)
                    gpu.inc_attr(r, -need)
                    setattr(gpu_alloc, r, need)
                alloc.gpus[idx] = gpu_alloc
        return alloc

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
        if not logical_task.valid_agent(self):
            raise InsufficientResourcesError('Task does not match this agent')
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

        if logical_task.cores:
            # For tasks requesting cores we activate NUMA awareness
            for numa_node in range(len(self.numa)):
                try:
                    return self._allocate_numa_node(numa_node, logical_task)
                except InsufficientResourcesError:
                    logger.debug('Failed to allocate NUMA node %d on %s',
                                 numa_node, self.agent_id, exc_info=True)
            raise InsufficientResourcesError('No suitable NUMA node found')
        else:
            return self._allocate_numa_node(None, logical_task)

    def can_allocate(self, logical_task):
        """Check whether :meth:`allocate` will succeed, without modifying
        the resources.
        """
        dup = copy.deepcopy(self)
        try:
            dup.allocate(logical_task)
            return True
        except InsufficientResourcesError:
            return False


class TaskState(OrderedEnum):
    """States in a task's lifecycle. In almost all cases, tasks only ever move
    forward through the states, not backwards. An exception is that a launch
    may be cancelled while still waiting for resources for the task, in which
    case it moves from :const:`STARTING` to :const:`NOT_READY`.
    """
    NOT_READY = 0    #: We have not yet started it
    STARTING = 1     #: We have been asked to start, but have not yet asked Mesos
    STARTED = 2      #: We have asked Mesos to start it, but it is not yet running
    RUNNING = 3      #: Process is running, but we're waiting for ports to open
    READY = 4        #: Node is completely ready
    KILLING = 5      #: We have asked the task to kill itself, but do not yet have confirmation
    DEAD = 6         #: Have received terminal status message


class PhysicalNode:
    """Base class for physical nodes.

    Parameters
    ----------
    logical_node : :class:`LogicalNode`
        The logical node from which this physical node is constructed
    loop : :class:`asyncio.AbstractEventLoop`
        The event loop used for constructing futures etc

    Attributes
    ----------
    logical_node : :class:`LogicalNode`
        The logical node passed to the constructor
    loop : :class:`asyncio.AbstractEventLoop`
        The event loop used for constructing futures etc
    host : str
        Host on which this node is operating (if any).
    ports : dict
        Dictionary mapping logical port names to port numbers.
    state : :class:`TaskState`
        The current state of the task (see :class:`TaskState`). Do not
        modify this directly; use :meth:`set_state` instead.
    ready_event : :class:`asyncio.Event`
        An event that becomes set once the task reaches either
        :class:`~TaskState.READY` or :class:`~TaskState.DEAD`. It is never
        unset.
    dead_event : :class:`asyncio.Event`
        An event that becomes set once the task reaches
        :class:`~TaskState.DEAD`.
    depends_ready : list of :class:`PhysicalNode`
        Nodes that this node has `depends_ready` dependencies on. This is only
        populated during :meth:`resolve`.
    _ready_waiter : :class:`asyncio.Task`
        Task which asynchronously waits for the to be ready (e.g. for ports to
        be open). It is started on reaching :class:`~TaskState.RUNNING`.
    """
    def __init__(self, logical_node, loop):
        self.logical_node = logical_node
        self.name = logical_node.name
        # In PhysicalTask it is a property and cannot be set
        try:
            self.host = None
        except AttributeError:
            pass
        self.ports = {}
        self.state = TaskState.NOT_READY
        self.ready_event = asyncio.Event(loop=loop)
        self.dead_event = asyncio.Event(loop=loop)
        self.loop = loop
        self._ready_waiter = None

    async def resolve(self, resolver, graph, loop):
        """Make final preparations immediately before starting.

        Parameters
        ----------
        resolver : :class:`Resolver`
            Resolver for images etc.
        graph : :class:`networkx.MultiDiGraph`
            Physical graph containing the task
        loop : :class:`asyncio.AbstractEventLoop`
            Current event loop
        """
        self.depends_ready = []
        for _src, trg, attr in graph.out_edges([self], data=True):
            if attr.get(DEPENDS_READY):
                self.depends_ready.append(trg)

    async def wait_ready(self):
        """Wait for the task to be ready for dependent tasks to communicate
        with, by polling the ports and waiting for dependent tasks. This method
        may be overloaded to implement other checks, but it must be
        cancellation-safe.
        """
        for dep in self.depends_ready:
            await dep.ready_event.wait()
        if self.logical_node.wait_ports is not None:
            wait_ports = [self.ports[port] for port in self.logical_node.wait_ports]
        else:
            wait_ports = list(self.ports.values())
        if wait_ports:
            await poll_ports(self.host, wait_ports, self.loop)

    def _ready_callback(self, future):
        """This callback is called when the waiter is either finished or
        cancelled. If it is finished, we would normally not have
        advanced beyond READY, because set_state will cancel the
        waiter if we do. However, there is a race condition if
        set_state is called between the waiter completing and the
        call to this callback."""
        self._ready_waiter = None
        try:
            future.result()  # throw the exception, if any
            if self.state < TaskState.READY:
                self.set_state(TaskState.READY)
        except asyncio.CancelledError:
            pass
        except Exception:
            logger.error('exception while waiting for task %s to be ready', self.name,
                         exc_info=True)

    def set_state(self, state):
        """Update :attr:`state`.

        This checks for invalid transitions, and sets the events when
        appropriate.

        Subclasses may override this to take special actions on particular
        transitions, but should chain to the base class.
        """
        logger.debug('Task %s: %s -> %s', self.name, self.state, state)
        if state < self.state:
            # STARTING -> NOT_READY is permitted, for the case where a
            # launch is cancelled before the tasks are actually launched.
            if state != TaskState.NOT_READY or self.state != TaskState.STARTING:
                logger.warn('Ignoring state change that went backwards (%s -> %s) on task %s',
                            self.state, state, self.name)
                return
        self.state = state
        if state == TaskState.DEAD:
            self.dead_event.set()
            self.ready_event.set()
        elif state == TaskState.READY:
            self.ready_event.set()
        elif state == TaskState.RUNNING and self._ready_waiter is None:
            self._ready_waiter = asyncio.ensure_future(self.wait_ready(), loop=self.loop)
            self._ready_waiter.add_done_callback(self._ready_callback)
        if state > TaskState.READY and self._ready_waiter is not None:
            self._ready_waiter.cancel()
            self._ready_waiter = None

    def kill(self, driver, **kwargs):
        """Start killing off the node. This is guaranteed to be called only if
        the task made it to at least :const:`TaskState.STARTED` and it is not
        in state :const:`TaskState.DEAD`. Note that it might be called more
        than once, with different keyword args.

        This function should not manipulate the task state; the caller will
        set the state to :const:`TaskState.KILLING`.

        Parameters
        ----------
        driver : :class:`pymesos.MesosSchedulerDriver`
            Scheduler driver
        kwargs : dict
            Extra arguments passed to :meth:`.Scheduler.kill`.
        """
        pass


class PhysicalExternal(PhysicalNode):
    """External service with a hostname and ports. The service moves
    automatically from :const:`TaskState.STARTED` to
    :const:`TaskState.RUNNING`, and from :const:`TaskState.KILLING` to
    :const:`TaskState.DEAD`.
    """
    def set_state(self, state):
        if state >= TaskState.STARTED and state < TaskState.READY:
            state = TaskState.RUNNING
        elif state == TaskState.KILLING:
            state = TaskState.DEAD
        super().set_state(state)


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
    loop : :class:`asyncio.AbstractEventLoop`
        The event loop used for constructing futures etc

    Attributes
    ----------
    interfaces : dict
        Map from logical interface names requested by
        :class:`LogicalTask.interfaces` to :class:`AgentInterface`s.
    taskinfo : :class:`mesos_pb2.TaskInfo`
        Task info for Mesos to run the task
    allocation : :class:`ResourceAllocation`
        Resources allocated to this task.
    status : :class:`mesos_pb2.TaskStatus`
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
    agent_id : str
        Slave ID of the agent on which this task is running
    """
    def __init__(self, logical_task, loop):
        super().__init__(logical_task, loop)
        self.interfaces = {}
        self.endpoints = {}
        self.taskinfo = None
        self.allocation = None
        self.status = None
        for r in RANGE_RESOURCES:
            setattr(self, r, {})

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
    def agent_id(self):
        if self.allocation:
            return self.allocation.agent.agent_id
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
        for request, value in zip(self.logical_node.interfaces, self.allocation.interfaces):
            self.interfaces[request.network] = self.allocation.agent.interfaces[value.index]
        for r in RANGE_RESOURCES:
            d = {}
            for name, value in zip(getattr(self.logical_node, r), getattr(self.allocation, r)):
                if name is not None:
                    d[name] = value
            setattr(self, r, d)

    async def resolve(self, resolver, graph, loop):
        """Do final preparation before moving to :const:`TaskState.STAGING`.
        At this point all dependencies are guaranteed to have resources allocated.

        Parameters
        ----------
        resolver : :class:`Resolver`
            Resolver to allocate resources like task IDs
        graph : :class:`networkx.MultiDiGraph`
            Physical graph
        loop : :class:`asyncio.AbstractEventLoop`
            Current event loop
        """
        await super().resolve(resolver, graph, loop)
        for _src, trg, attr in graph.out_edges([self], data=True):
            if 'port' in attr:
                port = attr['port']
                endpoint_name = '{}_{}'.format(trg.logical_node.name, port)
                self.endpoints[endpoint_name] = Endpoint(trg.host, trg.ports[port])

        docker_devices = set()
        docker_parameters = []
        taskinfo = Dict()
        taskinfo.name = self.name
        taskinfo.task_id.value = resolver.task_id_allocator()
        args = self.subst_args(resolver)
        command = [x.format(**args) for x in self.logical_node.command]
        if self.logical_node.wrapper is not None:
            uri = Dict()
            uri.value = self.logical_node.wrapper
            # Archive types recognised by Mesos Fetcher (.gz is excluded
            # because it doesn't contain a collection of files).
            archive_exts = ['.tar', '.tgz', '.tar.gz', '.tbz2', '.tar.bz2',
                            '.txz', '.tar.xz', '.zip']
            if not any(self.logical_node.wrapper.endswith(ext) for ext in archive_exts):
                uri.output_file = 'wrapper'
                uri.executable = True
            taskinfo.command.setdefault('uris', []).append(uri)
            command.insert(0, '/mnt/mesos/sandbox/wrapper')
        if self.depends_ready:
            uri = Dict()
            uri.value = urllib.parse.urljoin(resolver.http_url, 'static/delay_run.sh')
            uri.executable = True
            taskinfo.command.setdefault('uris', []).append(uri)
            command = ['/mnt/mesos/sandbox/delay_run.sh',
                       urllib.parse.urljoin(
                           resolver.http_url,
                           'tasks/{}/wait_start'.format(taskinfo.task_id.value))] + command
        if command:
            taskinfo.command.value = command[0]
            taskinfo.command.arguments = command[1:]
        taskinfo.command.shell = False
        taskinfo.container = copy.deepcopy(self.logical_node.container)
        image_path = await resolver.image_resolver(self.logical_node.image, loop)
        taskinfo.container.docker.image = image_path
        taskinfo.agent_id.value = self.agent_id
        taskinfo.resources = []
        for r in SCALAR_RESOURCES:
            value = getattr(self.logical_node, r)
            if value > 0:
                resource = Dict()
                resource.name = r
                resource.type = 'SCALAR'
                resource.scalar.value = value
                taskinfo.resources.append(resource)
        for r in RANGE_RESOURCES:
            value = getattr(self.allocation, r)
            if value:
                resource = Dict()
                resource.name = r
                resource.type = 'RANGES'
                resource.ranges.range = []
                for item in value:
                    resource.ranges.range.append(Dict(begin=item, end=item))
                taskinfo.resources.append(resource)

        if self.allocation.cores:
            core_list = ','.join(str(core) for core in self.allocation.cores)
            docker_parameters.append({'key': 'cpuset-cpus', 'value': core_list})

        any_infiniband = False
        for request, interface_alloc in zip(self.logical_node.interfaces,
                                            self.allocation.interfaces):
            for r in INTERFACE_SCALAR_RESOURCES:
                value = getattr(interface_alloc, r)
                if value:
                    resource = Dict()
                    resource.name = 'katsdpcontroller.interface.{}.{}'.format(
                        interface_alloc.index, r)
                    resource.type = 'SCALAR'
                    resource.scalar.value = value
                    taskinfo.resources.append(resource)
            if request.infiniband:
                any_infiniband = True
        if any_infiniband:
            # ibverbs uses memory mapping for DMA. Take away the default rlimit
            # maximum since Docker tends to set a very low limit.
            docker_parameters.append({'key': 'ulimit', 'value': 'memlock=-1'})
            # rdma_get_devices requires *all* the devices to be present to
            # succeed, even if they're not all used.
            for interface in self.agent.interfaces:
                docker_devices.update(interface.infiniband_devices)

        gpu_driver_version = None
        for gpu_alloc in self.allocation.gpus:
            for r in GPU_SCALAR_RESOURCES:
                value = getattr(gpu_alloc, r)
                if value:
                    resource = Dict()
                    resource.name = 'katsdpcontroller.gpu.{}.{}'.format(gpu_alloc.index, r)
                    resource.type = 'SCALAR'
                    resource.scalar.value = value
                    taskinfo.resources.append(resource)
            gpu = self.agent.gpus[gpu_alloc.index]
            docker_devices.update(gpu.devices)
            # We assume all GPUs on an agent have the same driver version.
            # This is reflected in the NVML API, so should be safe.
            gpu_driver_version = gpu.driver_version
        if gpu_driver_version is not None:
            volume = Dict()
            volume.mode = 'RO'
            volume.container_path = '/usr/local/nvidia'
            volume.source.type = 'DOCKER_VOLUME'
            volume.source.docker_volume.driver = 'nvidia-docker'
            volume.source.docker_volume.name = 'nvidia_driver_' + gpu_driver_version
            taskinfo.container.setdefault('volumes', []).append(volume)

        # container.linux_info.capabilities doesn't work with Docker
        # containerizer (https://issues.apache.org/jira/browse/MESOS-6163), so
        # pass in the parameters.
        for capability in self.logical_node.capabilities:
            docker_parameters.append({'key': 'cap-add', 'value': capability})

        for device in docker_devices:
            docker_parameters.append({'key': 'device', 'value': device})
        if docker_parameters:
            taskinfo.container.docker.setdefault('parameters', []).extend(docker_parameters)

        for rvolume, avolume in zip(self.logical_node.volumes, self.allocation.volumes):
            volume = Dict()
            volume.mode = rvolume.mode
            volume.container_path = rvolume.container_path
            volume.host_path = avolume.host_path
            taskinfo.container.setdefault('volumes', []).append(volume)

        taskinfo.discovery.visibility = 'EXTERNAL'
        taskinfo.discovery.name = self.name
        taskinfo.discovery.ports.ports = []
        for port_name, port_number in self.ports.items():
            # TODO: need a way to indicate non-TCP
            taskinfo.discovery.ports.ports.append(
                Dict(number=port_number,
                     name=port_name,
                     protocol='tcp'))
        self.taskinfo = taskinfo

    def subst_args(self, resolver):
        """Returns a dictionary that is passed when formatting the command
        from the logical task.
        """
        args = {}
        for r in RANGE_RESOURCES:
            args[r] = getattr(self, r)
        args['interfaces'] = self.interfaces
        args['endpoints'] = self.endpoints
        args['host'] = self.host
        args['resolver'] = resolver
        return args

    def kill(self, driver, **kwargs):
        # TODO: according to the Mesos docs, killing a task is not reliable,
        # and may need to be attempted again.
        # The poller is stopped by set_state, so we do not need to do it here.
        driver.killTask(self.taskinfo.task_id)
        super().kill(driver, **kwargs)


def instantiate(logical_graph, loop):
    """Create a physical graph from a logical one. Each physical node is
    created by calling :attr:`LogicalNode.physical_factory` on the
    corresponding logical node. Edges, and graph, node and edge attributes are
    transferred as-is.

    Parameters
    ----------
    logical_graph : :class:`networkx.MultiDiGraph`
        Logical graph to instantiate
    loop : :class:`asyncio.AbstractEventLoop`
        Event loop used to create futures
    """
    # Create physical nodes
    mapping = {logical: logical.physical_factory(logical, loop)
               for logical in logical_graph}
    return networkx.relabel_nodes(logical_graph, mapping)


class _LaunchGroup:
    """Set of tasks that are in state STARTING and waiting for Mesos
    resources.

    Attributes
    ----------
    graph : :class:`networkx.MultiDiGraph`
        Graph from which the nodes originated
    nodes : list
        List of :class:`PhysicalNode`s to launch
    resolver : :class:`Resolver`
        Resolver which will be used to launch the nodes
    future : :class:`asyncio.Future`
        A future that is set with the result of the launch i.e., once the
        group has been removed from the queue.
    """
    def __init__(self, graph, nodes, resolver, loop):
        self.nodes = nodes
        self.graph = graph
        self.resolver = resolver
        self.future = asyncio.Future(loop=loop)
        self.last_insufficient = InsufficientResourcesError('No resource offers received')


class LaunchQueue:
    """Queue of launch requests."""
    def __init__(self, name=''):
        self.name = name
        self._groups = deque()

    def _clear_cancelled(self):
        while self._groups and self._groups[0].future.cancelled():
            self._groups.popleft()

    def front(self):
        """First non-cancelled group"""
        self._clear_cancelled()
        return self._groups[0]

    def __bool__(self):
        self._clear_cancelled()
        return bool(self._groups)

    def remove(self, group):
        self._groups.remove(group)

    def add(self, group):
        self._groups.append(group)

    def __iter__(self):
        return (group for group in self._groups if not group.future.cancelled())

    def __repr__(self):
        return "<LaunchQueue '{}' with {} items at {:#x}>".format(
            self.name, len(self._groups), id(self))


@decorator
def run_in_event_loop(func, *args, **kw):
    args[0]._loop.call_soon_threadsafe(func, *args, **kw)


async def wait_start_handler(request):
    scheduler = request.app['katsdpcontroller_scheduler']
    task_id = request.match_info['id']
    task, graph = scheduler.get_task(task_id, return_graph=True)
    if task is None:
        return aiohttp.web.HTTPNotFound(reason='Task ID {} not active\n'.format(task_id))
    else:
        try:
            for dep in task.depends_ready:
                await dep.ready_event.wait()
        except Exception as error:
            logger.exception('Exception while waiting for dependencies')
            return aiohttp.web.HTTPInternalServerError(
                reason='Exception while waiting for dependencies:\n{}\n'.format(error))
        else:
            return aiohttp.web.Response(body='')


def subgraph(graph, edge_filter, nodes=None):
    """Return a new graph containing only edges with a particular attribute.

    An edge is added to the returned graph only if the filter passes. If
    `nodes` is specified, an edge must also have both endpoints in that set.

    Attribute data is not tranferred to the new graph.

    Parameters
    ----------
    graph : networkx graph
        Original graph (will not be modified)
    edge_filter : str or callable
        If callable, a function that takes edge attributes and returns true or
        false to accept/reject the edge. Otherwise, a key that must be present
        and truthy in the attribute dict.
    nodes : iterable, optional
        Subset of nodes to retain in the output
    """
    if nodes is None:
        nodes = graph.nodes()
    if not callable(edge_filter):
        attr = edge_filter
        edge_filter = lambda data: bool(data.get(attr))
    nodes = set(nodes)
    out = graph.__class__()
    out.add_nodes_from(nodes)
    for a, b, data in graph.edges(nodes, data=True):
        if b in nodes and edge_filter(data):
            out.add_edge(a, b)
    return out


class Scheduler(pymesos.Scheduler):
    """Top-level scheduler implementing the Mesos Scheduler API.

    Mesos calls the callbacks provided in this class from another thread. To
    ensure thread safety, they are all posted to the event loop via a
    decorator.

    The following invariants are maintained at each yield point:
    - each dictionary within :attr:`_offers` is non-empty

    Parameters
    ----------
    loop : :class:`asyncio.AbstractEventLoop`
        Event loop
    http_port : int
        Port for the embedded HTTP server, or 0 to assign a free one
    http_url : str, optional
        URL at which agent nodes can reach the HTTP server. If not specified,
        tries to deduce it from the host's FQDN.

    Attributes
    ----------
    resources_timeout : float or None
        Default time limit for resources to become available once a task is ready to
        launch.
    http_port : int
        Actual HTTP port used by :attr:`http_server` (after :meth:`start`)
    http_handler : :class:`aiohttp.web.Server`
        Web server connection handler (after :meth:`start`)
    http_url : str
        Actual URL to use for :attr:`http_server` (after :meth:`start`)
    http_server : :class:`asyncio.Server`
        Embedded HTTP server
    """
    def __init__(self, loop, http_port, http_url=None):
        self._loop = loop
        self._driver = None
        self._offers = {}           #: offers keyed by slave ID then offer ID
        #: set when it's time to retry a launch (see _launcher)
        self._retry_launch = asyncio.Event(loop=self._loop)
        self._default_queue = LaunchQueue()
        self._queues = [self._default_queue]
        self._offers_suppressed = False
        #: (task, graph) for tasks that have been launched (STARTED to KILLING), indexed by task ID
        self._active = {}
        self._closing = False       #: set to ``True`` when :meth:`close` is called
        self._min_ports = {}        #: next preferred port for each agent (keyed by ID)
        # If offers come at 5s intervals, then 11s gives two chances.
        self.resources_timeout = 11.0   #: Time to wait for sufficient resources to be offered
        self.http_server = None
        self.http_handler = None
        self.http_port = http_port
        self.http_url = http_url
        self._launcher_task = loop.create_task(self._launcher())

    async def start(self):
        """Start the internal HTTP server running. This must be called before any launches."""
        if self.http_server:
            raise RuntimeError('Already started')
        app = aiohttp.web.Application(loop=self._loop)
        app['katsdpcontroller_scheduler'] = self
        app.router.add_get('/tasks/{id}/wait_start', wait_start_handler)
        app.router.add_static('/static',
                              pkg_resources.resource_filename('katsdpcontroller', 'static'))
        self.http_handler = app.make_handler()
        # We want a single port serving both IPv4 and IPv6. By default asyncio
        # will create a separate socket for each, and if http_port is 0 (used
        # by unit tests) they end up with different ports.
        # See https://stackoverflow.com/questions/45907833 for more details.
        sock = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
        sock.bind(('::', self.http_port))
        self.http_server = await self._loop.create_server(
            self.http_handler, sock=sock)
        if not self.http_port:
            self.http_port = sock.getsockname()[1]
        if self.http_url is None:
            netloc = '{}:{}'.format(socket.getfqdn(), self.http_port)
            self.http_url = urllib.parse.urlunsplit(('http', netloc, '/', '', ''))
        logger.info('Internal HTTP server at %s', self.http_url)

    def add_queue(self, queue):
        """Register a new queue.

        Raises
        ------
        ValueError
            If `queue` has already been added
        """
        if queue in self._queues:
            raise ValueError('Queue has already been added')
        self._queues.append(queue)

    def remove_queue(self, queue):
        """Deregister a previously-registered queue.

        The queue must be empty before deregistering it.

        Raises
        ------
        ValueError
            If `queue` has not been registered with :meth:`add_queue`
        asyncio.InvalidStateError
            If `queue` is not empty
        """
        if queue:
            raise asyncio.InvalidStateError('queue is not empty')
        self._queues.remove(queue)

    @classmethod
    def _node_sort_key(cls, physical_node):
        """Sort key for nodes when launching.

        Sort nodes. Those requiring core affinity are put first,
        since they tend to be the trickiest to pack. Then order by
        CPUs, GPUs, memory. Finally sort by name so that results
        are more reproducible.
        """
        node = physical_node.logical_node
        if isinstance(node, LogicalTask):
            return (len(node.cores), node.cpus, len(node.gpus), node.mem, node.name)
        else:
            return (0, 0, 0, 0, node.name)

    def _clear_offers(self):
        for offers in self._offers.values():
            self._driver.acceptOffers([offer.id for offer in offers.values()], [])
        self._offers = {}
        if not self._offers_suppressed:
            self._driver.suppressOffers()
            self._offers_suppressed = True

    def _remove_offer(self, agent_id, offer_id):
        try:
            del self._offers[agent_id][offer_id]
            if not self._offers[agent_id]:
                del self._offers[agent_id]
        except KeyError:
            # There are various race conditions that mean an offer could be
            # removed twice (e.g. if we launch a task with an offer at the
            # same time it is rescinded - the task will be lost but we won't
            # crash).
            pass

    def set_driver(self, driver):
        self._driver = driver

    def registered(self, driver, framework_id, master_info):
        pass

    @run_in_event_loop
    def resourceOffers(self, driver, offers):
        for offer in offers:
            self._offers.setdefault(offer.agent_id.value, {})[offer.id.value] = offer
        self._retry_launch.set()

    @run_in_event_loop
    def offerRescinded(self, driver, offer_id):
        for agent_id, offers in list(self._offers.items()):
            if offer_id.value in offers:
                self._remove_offer(agent_id, offer_id.value)

    @run_in_event_loop
    def statusUpdate(self, driver, status):
        logger.debug(
            'Update: task %s in state %s (%s)',
            status.task_id.value, status.state, status.message)
        task = self.get_task(status.task_id.value)
        if task is not None:
            if status.state in TERMINAL_STATUSES:
                task.set_state(TaskState.DEAD)
                del self._active[status.task_id.value]
            elif status.state == 'TASK_RUNNING':
                if task.state < TaskState.RUNNING:
                    task.set_state(TaskState.RUNNING)
                    # The task itself is responsible for advancing to
                    # READY
            task.status = status
        self._driver.acknowledgeStatusUpdate(status)

    @classmethod
    def _diagnose_insufficient(cls, agents, nodes):
        """Try to determine *why* offers are insufficient.

        This function does not return, instead raising an instance of
        :exc:`InsufficientResourcesError` or a subclass.

        Parameters
        ----------
        agents : list
            :class:`Agent`s from which allocation was attempted
        nodes : list
            :class:`PhysicalNode`s for which allocation failed. This may
            include non-tasks, which will be ignored.
        """
        # Non-tasks aren't relevant, so filter them out.
        nodes = [node for node in nodes if isinstance(node, PhysicalTask)]
        # Pre-compute the maximum resources of each type on any agent,
        # and the total amount of each resource.
        max_resources = {}
        max_gpu_resources = {}
        max_interface_resources = {}     # Double-hash, indexed by network then resource
        total_resources = {}
        total_gpu_resources = {}
        total_interface_resources = {}   # Double-hash, indexed by network then resource
        for r in SCALAR_RESOURCES:
            available = [getattr(agent, r) for agent in agents]
            max_resources[r] = max(available) if available else 0.0
            total_resources[r] = sum(available)
        for r in RANGE_RESOURCES:
            # Cores are special because only the cores on a single NUMA node
            # can be allocated together
            if r == 'cores':
                available = []
                for agent in agents:
                    for numa_node in agent.numa:
                        available.append(len([core for core in numa_node if core in agent.cores]))
            else:
                available = [len(getattr(agent, r)) for agent in agents]
            max_resources[r] = max(available) if available else 0.0
            total_resources[r] = sum(available)
        for r in GPU_SCALAR_RESOURCES:
            available = [getattr(gpu, r) for agent in agents for gpu in agent.gpus]
            max_gpu_resources[r] = max(available) if available else 0.0
            total_gpu_resources[r] = sum(available)

        # Collect together all interfaces on the same network
        networks = {}
        for agent in agents:
            for interface in agent.interfaces:
                networks.setdefault(interface.network, []).append(interface)
        for network, interfaces in networks.items():
            max_interface_resources[network] = {}
            total_interface_resources[network] = {}
            for r in INTERFACE_SCALAR_RESOURCES:
                available = [getattr(interface, r) for interface in interfaces]
                max_interface_resources[network][r] = max(available)
                total_interface_resources[network][r] = sum(available)

        # Check if there is a single node that won't run anywhere
        for node in nodes:
            logical_task = node.logical_node
            if not any(agent.can_allocate(logical_task) for agent in agents):
                # Check if there is an interface/volume/GPU request that
                # doesn't match anywhere.
                for request in logical_task.interfaces:
                    if not any(request.matches(interface, None)
                               for agent in agents for interface in agent.interfaces):
                        raise TaskNoInterfaceError(node, request)
                for request in logical_task.volumes:
                    if not any(request.matches(volume, None)
                               for agent in agents for volume in agent.volumes):
                        raise TaskNoVolumeError(node, request)
                for i, request in enumerate(logical_task.gpus):
                    if not any(request.matches(gpu, None)
                               for agent in agents for gpu in agent.gpus):
                        raise TaskNoGPUError(node, i)
                # Check if there is some specific resource that is lacking.
                for r in SCALAR_RESOURCES:
                    need = getattr(logical_task, r)
                    if need > max_resources[r]:
                        raise TaskInsufficientResourcesError(node, r, need, max_resources[r])
                for r in RANGE_RESOURCES:
                    need = len(getattr(logical_task, r))
                    if need > max_resources[r]:
                        raise TaskInsufficientResourcesError(node, r, need, max_resources[r])
                for i, request in enumerate(logical_task.gpus):
                    for r in GPU_SCALAR_RESOURCES:
                        need = getattr(request, r)
                        if need > max_gpu_resources[r]:
                            raise TaskInsufficientGPUResourcesError(
                                node, i, r, need, max_gpu_resources[r])
                for request in logical_task.interfaces:
                    for r in INTERFACE_SCALAR_RESOURCES:
                        need = getattr(request, r)
                        if need > max_interface_resources[request.network][r]:
                            raise TaskInsufficientInterfaceResourcesError(
                                node, request, r, need, max_interface_resources[request.network][r])
                # This node doesn't fit but the reason is more complex e.g.
                # there is enough of each resource individually but not all on
                # the same agent or NUMA node.
                raise TaskNoAgentError(node)

        # Nodes are all individually launchable, but we weren't able to launch
        # all of them due to some contention. Check if any one resource is
        # over-subscribed.
        for r in SCALAR_RESOURCES:
            need = sum(getattr(node.logical_node, r) for node in nodes)
            if need > total_resources[r]:
                raise GroupInsufficientResourcesError(r, need, total_resources[r])
        for r in RANGE_RESOURCES:
            need = sum(len(getattr(node.logical_node, r)) for node in nodes)
            if need > total_resources[r]:
                raise GroupInsufficientResourcesError(r, need, total_resources[r])
        for r in GPU_SCALAR_RESOURCES:
            need = sum(getattr(request, r) for node in nodes for request in node.logical_node.gpus)
            if need > total_gpu_resources[r]:
                raise GroupInsufficientGPUResourcesError(r, need, total_gpu_resources[r])
        for network in networks:
            for r in INTERFACE_SCALAR_RESOURCES:
                need = sum(getattr(request, r)
                           for node in nodes for request in node.logical_node.interfaces
                           if request.network == network)
                if need > total_interface_resources[network][r]:
                    raise GroupInsufficientInterfaceResourcesError(
                        network, r, need, total_interface_resources[network][r])
        # Not a simple error e.g. due to packing problems
        raise InsufficientResourcesError("Insufficient resources to launch all tasks")

    async def _launch_once(self):
        """Run single iteration of :meth:`_launcher`"""
        candidates = [(queue, queue.front()) for queue in self._queues if queue]
        if not candidates:
            self._clear_offers()
            return
        elif not self._offers and self._offers_suppressed:
            self._driver.reviveOffers()
            self._offers_suppressed = False
            return

        for queue, group in candidates:
            nodes = group.nodes
            try:
                # Due to concurrency, another coroutine may have altered
                # the state of the tasks since they were put onto the
                # pending list (e.g. by killing them). Filter those out.
                nodes = [node for node in nodes if node.state == TaskState.STARTING]
                agents = [Agent(list(offers.values()), self._min_ports.get(agent_id, 0))
                          for agent_id, offers in self._offers.items()]
                # Back up the original agents so that if allocation fails we can
                # diagnose it.
                orig_agents = copy.deepcopy(agents)
                # Sort agents by GPUs then free memory. This makes it less likely that a
                # low-memory process will hog all the other resources on a high-memory
                # box. Similarly, non-GPU tasks will only use GPU-equipped agents if
                # there is no other choice. Should eventually look into smarter
                # algorithms e.g. Dominant Resource Fairness
                # (http://mesos.apache.org/documentation/latest/allocation-module/)
                agents.sort(key=lambda agent: (agent.priority, agent.mem))
                nodes.sort(key=self._node_sort_key, reverse=True)
                allocations = []
                for node in nodes:
                    allocation = None
                    if isinstance(node, PhysicalTask):
                        for agent in agents:
                            try:
                                allocation = agent.allocate(node.logical_node)
                                break
                            except InsufficientResourcesError as error:
                                logger.debug('Cannot add %s to %s: %s',
                                             node.name, agent.agent_id, error)
                        else:
                            # Raises an InsufficientResourcesError
                            self._diagnose_insufficient(orig_agents, nodes)
                        allocations.append((node, allocation))
            except InsufficientResourcesError as error:
                logger.debug('Could not yet launch graph: %s', error)
                group.last_insufficient = error
            else:
                # At this point we have a sufficient set of offers.
                # Two-phase resolving
                logger.debug('Allocating resources to tasks')
                for (node, allocation) in allocations:
                    node.allocate(allocation)
                logger.debug('Performing resolution')
                order_graph = subgraph(group.graph, DEPENDS_RESOLVE, nodes)
                # Lexicographical sorting isn't required for
                # functionality, but the unit tests depend on it to get
                # predictable task IDs.
                for node in networkx.lexicographical_topological_sort(order_graph.reverse(),
                                                                      key=lambda x: x.name):
                    logger.debug('Resolving %s', node.name)
                    await node.resolve(group.resolver, group.graph, self._loop)
                # Last chance for the group to be cancelled. After this point, we must
                # not await anything.
                if group.future.cancelled():
                    # No need to set _retry_launch, because the cancellation did so.
                    continue
                # Launch the tasks
                new_min_ports = {}
                taskinfos = {agent: [] for agent in agents}
                for (node, allocation) in allocations:
                    taskinfos[node.agent].append(node.taskinfo)
                    for port in allocation.ports:
                        prev = new_min_ports.get(node.agent_id, 0)
                        new_min_ports[node.agent_id] = max(prev, port + 1)
                    self._active[node.taskinfo.task_id.value] = (node, group.graph)
                self._min_ports.update(new_min_ports)
                for agent in agents:
                    offer_ids = [offer.id for offer in agent.offers]
                    # TODO: does this need to use run_in_executor? Is it
                    # thread-safe to do so?
                    # TODO: if there are more pending in the queue then we
                    # should use a filter to be re-offered resources more
                    # quickly.
                    self._driver.launchTasks(offer_ids, taskinfos[agent])
                    for offer_id in offer_ids:
                        self._remove_offer(agent.agent_id, offer_id.value)
                    logger.info('Launched %d tasks on %s',
                                len(taskinfos[agent]), agent.agent_id)
                for node in nodes:
                    node.set_state(TaskState.STARTED)
                group.future.set_result(None)
                queue.remove(group)
                self._retry_launch.set()
                break

    async def _launcher(self):
        """Background task to monitor the queues and offers and launch when possible.

        This function does not handle timing out launch requests. The caller
        deals with that and cancels the launch where necessary.
        """

        # There are a number of conditions under which we need to do more work:
        # - A group is added to an empty queue (non-empty queues aren't an issue,
        #   because only the front-of-queue is a candidate).
        # - A group is cancelled from the front of a queue
        # - A task is killed in a front-of-queue group (this reduces the
        #   resource requirements to launch the remainder of the group)
        # - A new offer is received
        # - After a successful launch (since a new front-of-queue group may
        #   appear).
        # All are signalled by setting _retry_launch. In some cases it is set
        # when it is not actually needed - this is harmless as long as the number
        # of queues doesn't get out of hand.
        while True:
            await self._retry_launch.wait()
            self._retry_launch.clear()
            try:
                await self._launch_once()
            except asyncio.CancelledError:
                raise     # Normal operation in close()
            except Exception:
                logger.exception('Error in _launch_once')

    @classmethod
    def depends_resources(cls, data):
        """Whether edge attribute has a `depends_resources` dependency.

        It takes into account the keys that implicitly specify a
        `depends_resources` dependency.
        """
        keys = [DEPENDS_RESOURCES, DEPENDS_RESOLVE, DEPENDS_READY, 'port']
        return any(data.get(key) for key in keys)

    async def launch(self, graph, resolver, nodes=None, *,
                     queue=None, resources_timeout=None):
        """Launch a physical graph, or a subset of nodes from a graph. This is
        a coroutine that returns only once the nodes are ready.

        It is legal to pass nodes that have already been passed to
        :meth:`launch` with the same queue. They will not be re-launched, but
        this call will wait until they are ready.

        It is safe to run this coroutine as an asyncio task and cancel it.
        However, some of the nodes may have been started already. If the
        launch is cancelled, it is recommended to kill the nodes to reach a
        known state.

        Running two launches with the same graph concurrently is possible but
        not recommended. In particular, cancelling the first launch can cause
        the second to stall indefinitely if it was waiting for nodes from the
        first launch to be ready.

        If an exception occurs, the state of nodes is undefined. In particular,
        it is possible that some nodes were successfully launched.

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
        queue : :class:`LaunchQueue`
            Queue from which to launch the nodes. If not specified, a default
            queue is used.
        resources_timeout : float
            Time to wait for sufficient resources to launch the nodes. If not
            specified, defaults to the class value.

        Raises
        ------
        asyncio.InvalidStateError
            If :meth:`close` has been called.
        InsufficientResourcesError
            If, once a launch request reached the head of the queue, no
            sufficient resource offers were received within the timeout. Note
            that a call to :meth:`launch` can be split into several phases,
            each with its own timeout, so it is possible for there to be a
            long delay and still receive this error.
        CycleError
            If it is impossible to launch the nodes due to a cyclic dependency.
        DependencyError
            If any nodes in `nodes` is in state :const:`TaskState.NOT_READY`
            and has a `depends_resources` or `depends_ready` dependency on a
            node that is not in `nodes` and is also in state
            :const:`TaskState.NOT_READY`.
        ValueError
            If `queue` has not been added with :meth:`add_queue`.
        """
        if self._closing:
            raise asyncio.InvalidStateError('Cannot launch tasks while shutting down')
        if nodes is None:
            nodes = graph.nodes()
        if queue is None:
            queue = self._default_queue
        if queue not in self._queues:
            raise ValueError('queue has not been added to the scheduler')
        if resources_timeout is None:
            resources_timeout = self.resources_timeout
        # Create a startup schedule. The nodes are partitioned into groups that can
        # be started at the same time.

        # Check that we don't depend on some non-ready task outside the set, while also
        # building a graph of readiness dependencies.
        remaining = [node for node in nodes if node.state == TaskState.NOT_READY]
        remaining_set = set(remaining)
        depends_ready_graph = subgraph(graph, DEPENDS_READY, remaining_set)
        depends_resolve_graph = subgraph(graph, DEPENDS_RESOLVE, remaining_set)
        for src, trg, data in graph.out_edges(remaining, data=True):
            if trg not in remaining_set and trg.state == TaskState.NOT_READY:
                if self.depends_resources(data):
                    raise DependencyError('{} depends on {} but it is neither'
                                          'started nor scheduled'.format(src.name, trg.name))
        if not networkx.is_directed_acyclic_graph(depends_ready_graph):
            raise CycleError('cycle between depends_ready dependencies')
        if not networkx.is_directed_acyclic_graph(depends_resolve_graph):
            raise CycleError('cycle between depends_resolve dependencies')

        for node in remaining:
            node.state = TaskState.STARTING
        pending = _LaunchGroup(graph, remaining, resolver, self._loop)
        empty = not queue
        queue.add(pending)
        if empty:
            self._retry_launch.set()
        try:
            await asyncio.wait_for(pending.future, timeout=resources_timeout)
        except Exception as error:
            logger.debug('Exception in launching group', exc_info=True)
            # Could be
            # - a timeout
            # - we were cancelled
            # - close() was called, which cancels pending.future
            for node in remaining:
                if node.state == TaskState.STARTING:
                    node.set_state(TaskState.NOT_READY)
            queue.remove(pending)
            self._retry_launch.set()
            if isinstance(error, asyncio.TimeoutError):
                raise pending.last_insufficient
            else:
                raise
        ready_futures = [node.ready_event.wait() for node in nodes]
        await asyncio.gather(*ready_futures, loop=self._loop)

    async def kill(self, graph, nodes=None, **kwargs):
        """Kill a graph or set of nodes from a graph. It is safe to kill nodes
        from any state. Dependencies specified with `depends_kill` will be
        honoured, leaving nodes alive until all nodes that depend on them are
        killed. Note that if any such node is not part of `nodes` then this
        function could block indefinitely until that node dies in some other
        way.

        Parameters
        ----------
        graph : :class:`networkx.MultiDiGraph`
            Physical graph to kill
        nodes : list, optional
            If specified, the nodes to kill. The default is to kill all nodes
            in the graph.
        kwargs : dict
            Any other keyword arguments are passed to the tasks's
            :meth:`~.PhysicalTask.kill` method.

        Raises
        ------
        CycleError
            If there is a cyclic dependency within the set of nodes to kill.
        """
        async def kill_one(node, graph):
            if node.state <= TaskState.STARTING:
                if node.state == TaskState.STARTING:
                    # Fewer resources now needed to start rest of the group
                    self._retry_launch.set()
                node.set_state(TaskState.DEAD)
            elif node.state <= TaskState.KILLING:
                for src in graph.predecessors(node):
                    await src.dead_event.wait()
                # Re-check state because it might have changed while waiting
                if node.state <= TaskState.KILLING:
                    logger.debug('Killing %s', node.name)
                    node.kill(self._driver, **kwargs)
                    node.set_state(TaskState.KILLING)
            await node.dead_event.wait()

        kill_graph = subgraph(graph, DEPENDS_KILL, nodes)
        if not networkx.is_directed_acyclic_graph(kill_graph):
            raise CycleError('cycle between depends_kill dependencies')
        futures = []
        for node in kill_graph:
            futures.append(asyncio.ensure_future(kill_one(node, kill_graph), loop=self._loop))
        await asyncio.gather(*futures, loop=self._loop)

    async def close(self):
        """Shut down the scheduler. This is a coroutine that kills any graphs
        with running tasks or pending launches, and joins the driver thread. It
        is an error to make any further calls to the scheduler after calling
        this function.

        .. note::

            A graph with no running tasks but other types of nodes still
            running will not be shut down. This is subject to change in the
            future.
        """
        # TODO: do we need to explicitly decline outstanding offers?
        self._closing = True    # Prevents concurrent launches
        if self.http_server is not None:
            self.http_server.close()
            await self.http_handler.shutdown(1)
            await self.http_server.wait_closed()
        # Find the graphs that are still running
        graphs = set()
        for (_task, graph) in self._active.values():
            graphs.add(graph)
        for queue in self._queues:
            for group in queue:
                graphs.add(group.graph)
        for graph in graphs:
            await self.kill(graph)
        for queue in self._queues:
            while queue:
                queue.front().future.cancel()
        self._launcher_task.cancel()
        try:
            await self._launcher_task
        except asyncio.CancelledError:
            pass
        self._launcher_task = None
        self._driver.stop()
        # If self._driver is a mock, then asyncio incorrectly complains about passing
        # self._driver.join directly, due to
        # https://github.com/python/asyncio/issues/458. So we wrap it in a lambda.
        status = await self._loop.run_in_executor(None, lambda: self._driver.join())
        return status

    async def get_master_and_slaves(self, timeout=None):
        """Obtain a list of slave hostnames from the master.

        Parameters
        ----------
        timeout : float, optional
            Timeout for HTTP connection to the master

        Raises
        ------
        aiohttp.client.ClientError
            If there was an HTTP connection problem (including timeout)
        ValueError
            If the HTTP response from the master was malformed
        ValueError
            If there is no current master

        Returns
        -------
        master : list of str
            Hostname of the master
        slaves : list of str
            Hostnames of slaves
        """
        if self._driver is None:
            raise ValueError('No driver is set')
        # Need to copy, because the driver runs in a separate thread
        # (self.master is a property that takes a lock).
        master = self._driver.master
        if master is None:
            raise ValueError('No master is set')
        url = urllib.parse.urlunsplit(('http', master, '/slaves', '', ''))
        with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=timeout) as r:
                r.raise_for_status()
                data = await r.json()
                try:
                    slaves = data['slaves']
                    master_host = urllib.parse.urlsplit(url).hostname
                    return master_host, [slave['hostname'] for slave in slaves]
                except (KeyError, TypeError) as error:
                    raise ValueError('Malformed response') from error

    def get_task(self, task_id, return_graph=False):
        try:
            if return_graph:
                return self._active[task_id]
            else:
                return self._active[task_id][0]
        except KeyError:
            return None


__all__ = [
    'LogicalNode', 'PhysicalNode',
    'LogicalExternal', 'PhysicalExternal',
    'LogicalTask', 'PhysicalTask', 'TaskState',
    'Volume',
    'ResourceAllocation',
    'GPUResourceAllocation', 'InterfaceResourceAllocation',
    'InsufficientResourcesError',
    'NoOffersError',
    'TaskNoAgentError',
    'TaskInsufficientResourcesError',
    'TaskInsufficientGPUResourcesError',
    'TaskInsufficientInterfaceResourcesError',
    'TaskNoInterfaceError',
    'TaskNoVolumeError',
    'TaskNoGPUError',
    'GroupInsufficientResourcesError',
    'GroupInsufficientGPUResourcesError',
    'GroupInsufficientInterfaceResourcesError',
    'InterfaceRequest', 'GPURequest',
    'ImageResolver', 'TaskIDAllocator', 'Resolver',
    'Agent', 'AgentGPU', 'AgentInterface',
    'Scheduler', 'instantiate']
