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

Queues can also have priorities. As long as there are tasks waiting in a
higher-priority queue, no tasks from a lower-priority queue will be launched,
even if the resources are available for them. This is a crude mechanism to
ensure that a stream of low-priority jobs with minimal resource requirements do
not block high-priority tasks that needs to wait for a more significant portion
of the cluster to be available.

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
import re
import base64
import socket
import contextlib
import copy
from collections import namedtuple, deque
from enum import Enum
import math
import asyncio
import urllib
import ssl
import ipaddress
import decimal
from decimal import Decimal
import time
import io

import pkg_resources
import docker
import networkx
import jsonschema
from decorator import decorator
from addict import Dict
import pymesos
import prometheus_client

import aiohttp.web

from katsdptelstate.endpoint import Endpoint

from . import schemas


TASKS_IN_STATE = prometheus_client.Gauge(
    'katsdpcontroller_tasks_in_state', 'Number of physical tasks in each state, per queue',
    ['queue', 'state'])


#: Mesos task states that indicate that the task is dead
#: (see https://github.com/apache/mesos/blob/1.0.1/include/mesos/mesos.proto#L1374)
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
DECIMAL_CONTEXT = decimal.Context(traps=[
    decimal.Overflow, decimal.InvalidOperation, decimal.DivisionByZero,  # defaults
    decimal.Inexact, decimal.FloatOperation])
DECIMAL_CAST_CONTEXT = decimal.Context()
DECIMAL_ZERO = Decimal('0.000')
logger = logging.getLogger(__name__)


Volume = namedtuple('Volume', ['name', 'host_path', 'numa_node'])
Volume.__doc__ = \
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


def _as_decimal(value):
    """Forces `value` to a Decimal with 3 decimal places"""
    with decimal.localcontext(DECIMAL_CAST_CONTEXT):
        return Decimal(value).quantize(DECIMAL_ZERO)


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
    (family, type_, proto, _canonname, sockaddr) = addrs[0]
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


class ResourceRequest:
    """
    Request for some quantity of a Mesos resource.

    This is an abstract base class. See :class:`ScalarResourceRequest` and
    :class:`RangeResourceRequest`.

    Attributes
    ----------
    value
        Resource request (read-write). Requesters set this field to indicate
        their requirements. The type depends on the subclass.
    amount
        Requested amount (read-only). Subclasses provide this property to
        indicate how much of the resource is needed, as a real number
        (typically :class:`int` or :class:`Decimal`).
    """
    pass


class ScalarResourceRequest(ResourceRequest):
    """
    Request for some amount of a scalar resource.

    The amount requested can be specified as any type convertible to
    :class:`Decimal` (the default value is float). It is converted to
    3 decimal places before use.

    Attributes
    ----------
    value : real
        Requested amount
    amount : :class:`Decimal`
        Requested amount, with exactly 3 decimal places (read-only)
    """

    def __init__(self):
        self.value = 0.0

    @property
    def amount(self):
        return _as_decimal(self.value)


class RangeResourceRequest(ResourceRequest):
    """
    Request for some resources from a range resource.

    The value is required in the form of a list of unique names, which are
    meaningful only to the caller (:class:`PhysicalTask` provides a mapping
    from these names to the assigned resources). Names can be ``None`` if no
    name is required.

    Attributes
    ----------
    value : list of str
        Names to associate with resources
    amount : int
        Number of resources requested (read-only)
    """
    def __init__(self):
        self.value = []

    @property
    def amount(self):
        return len(self.value)


class Resource:
    """
    Model a single resource, in either an offer or a task info.

    It collects multiple Mesos Resource messages with the same name. It also
    allows a subset of the resource to be allocated and returned.

    This is an abstract base class. Use :class:`ScalarResource` or
    :class:`RangeResource`.

    Attributes
    ----------
    name : str
        Resource name
    available : real
        Amount of the resource that is available in this object
    parts : list
        The internal representations of the pieces of the resource. This should
        only be accessed by subclasses.
    """

    ZERO = 0
    REQUEST_CLASS = ResourceRequest

    def __init__(self, name):
        self.name = name
        self.parts = []
        self.available = self.ZERO

    def add(self, resource):
        """Add a Mesos resource message to the internal resource list"""
        if resource.name != self.name:
            raise ValueError('Name mismatch {} != {}'.format(self.name, resource.name))
        # Keeps the most specifically reserved resources at the end
        # so that they're used first before unreserved resources.
        pos = 0
        role = resource.get('role', '*')
        while (pos < len(self.parts)
               and len(self.parts[pos].get('role', '*')) < len(role)):
            pos += 1
        transformed = self._transform(resource)
        self.parts.insert(pos, transformed)
        self.available += self._available(transformed)

    def __bool__(self):
        return bool(self.available)

    def allocate(self, amount, **kwargs):
        """Allocate a quantity of the resource.

        Returns another Resource object of the same type with the requested
        amount of resources. The resources are subtracted from this object.
        """
        if not kwargs:
            available = self.available
        else:
            available = sum((self._available(part, **kwargs) for part in self.parts),
                            self.ZERO)
        if amount > available:
            raise ValueError('Requested amount {} of {} is more than available {} (kwargs={})'
                             .format(amount, self.name, available, kwargs))
        out = type(self)(self.name)
        pos = len(self.parts) - 1
        with decimal.localcontext(DECIMAL_CONTEXT):
            while amount > self.ZERO:
                assert self.parts, "self.available is out of sync"
                part_available = self._available(self.parts[pos], **kwargs)
                use = min(part_available, amount)
                out.parts.append(self._allocate(self.parts[pos], use, **kwargs))
                if self._available(self.parts[pos]) == 0:
                    del self.parts[pos]
                amount -= use
                self.available -= use
                out.available += use
                pos -= 1
        out.parts.reverse()   # Put output pieces into same order as input
        return out

    def info(self):
        """Iterate over the messages that should be placed in task info"""
        for part in self.parts:
            yield self._untransform(part)

    def _transform(self, resource):
        """Convert a resource into the internal representation."""
        raise NotImplementedError    # pragma: nocover

    def _untransform(self, resource):
        """Convert a resource from the internal representation."""
        raise NotImplementedError    # pragma: nocover

    def _available(self, resource, **kwargs):
        """Amount of resource available in a (transformed) part.

        Subclasses may accept kwargs to specify additional constraints
        on the manner of the allocation.
        """
        raise NotImplementedError    # pragma: nocover

    def _value_str(self, resource):
        raise NotImplementedError    # pragma: nocover

    def _allocate(self, resource, amount, **kwargs):
        """Take a piece of a resource.

        Returns the allocated part, and modifies `resource` in place.
        `amount` must be at most the result of
        ``self._available(resource, **kwargs)``.
        """
        raise NotImplementedError    # pragma: nocover

    def __str__(self):
        parts = []
        for part in self.parts:
            key = self.name
            if part.get('role', '*') != '*':
                key += '(' + part['role'] + ')'
            parts.append(key + ':' + self._value_str(part))
        return '; '.join(parts)

    @classmethod
    def empty_request(cls):
        return cls.REQUEST_CLASS()


class ScalarResource(Resource):
    """Resource model for Mesos scalar resources"""

    ZERO = DECIMAL_ZERO
    REQUEST_CLASS = ScalarResourceRequest

    def _transform(self, resource):
        if resource.type != 'SCALAR':
            raise TypeError('Expected SCALAR resource, got {}'.format(resource.type))
        resource = copy.deepcopy(resource)
        resource.scalar.value = _as_decimal(resource.scalar.value)
        return resource

    def _untransform(self, resource):
        resource = copy.deepcopy(resource)
        resource.scalar.value = float(resource.scalar.value)
        return resource

    def _available(self, resource):
        return resource.scalar.value

    def _allocate(self, resource, amount):
        out = copy.deepcopy(resource)
        out.scalar.value = amount
        with decimal.localcontext(DECIMAL_CONTEXT):
            resource.scalar.value -= amount
        return out

    def _value_str(self, resource):
        return str(resource.scalar.value)

    def allocate(self, amount):
        return super().allocate(_as_decimal(amount))


class RangeResource(Resource):
    """Resource model for Mesos range resources"""

    REQUEST_CLASS = RangeResourceRequest

    def _transform(self, resource):
        if resource.type != 'RANGES':
            raise TypeError('Expected RANGES resource, got {}'.format(resource.type))
        resource = copy.deepcopy(resource)
        # Ensures we take resources from the first range first
        resource.ranges.range.reverse()
        return resource

    def _untransform(self, resource):
        return self._transform(resource)

    def _available(self, resource, *, minimum=None):
        total = 0
        for r in resource.ranges.range:
            if minimum is None:
                total += r.end - r.begin + 1
            elif minimum <= r.end:
                total += r.end - max(r.begin, minimum) + 1
        return total

    def _value_str(self, resource):
        return '[' + ','.join('{}-{}'.format(r.begin, r.end) for r in resource.ranges.range) + ']'

    def _allocate(self, resource, amount, *, minimum=None):
        out = copy.deepcopy(resource)
        out.ranges.range.clear()
        pos = len(resource.ranges.range) - 1
        while amount > 0:
            r = resource.ranges.range[pos]
            use = 0
            if minimum is None or minimum <= r.begin:
                use = min(amount, r.end - r.begin + 1)
                out.ranges.range.append(Dict({'begin': r.begin, 'end': r.begin + use - 1}))
                r.begin += use
            elif minimum <= r.end:
                use = min(amount, r.end - minimum + 1)
                out.ranges.range.append(Dict({'begin': minimum, 'end': minimum + use - 1}))
                if minimum + use <= r.end:
                    # Note: this will cause the empty check lower down to check
                    # this range, instead of r. That's harmless, since we only
                    # get here if both are non-empty.
                    resource.ranges.range.insert(
                        pos,
                        Dict({'begin': minimum + use, 'end': r.end}))
                r.end = minimum - 1
            if r.begin > r.end:
                del resource.ranges.range[pos]
            amount -= use
            pos -= 1
        # Put into transformed form
        out.ranges.range.reverse()
        return out

    def _subset_part(self, part, group):
        out = copy.deepcopy(part)
        out.ranges.range.clear()
        for r in part.ranges.range:
            for i in range(r.end, r.begin - 1, -1):
                if i in group:
                    out.ranges.range.append(Dict({'begin': i, 'end': i}))
        return out

    def subset(self, group):
        """Get a new RangeResources containing only the items that are in `group`.

        The original is not modified.

        This is **not** an efficient implementation. It is currently used for
        partitioning CPU cores into NUMA nodes, where there are dozens of
        cores and a handful of NUMA nodes.
        """
        group = frozenset(group)
        out = type(self)(self.name)
        for part in self.parts:
            new_part = self._subset_part(part, group)
            if new_part.ranges.range:
                out.parts.append(new_part)
                out.available += self._available(new_part)
        return out

    def __len__(self):
        return self.available

    def __iter__(self):
        for part in reversed(self.parts):
            for r in reversed(part.ranges.range):
                yield from range(r.begin, r.end + 1)


class ResourceRequestDescriptor:
    def __init__(self, name):
        self._name = name

    def __get__(self, instance, owner):
        return instance.requests[self._name].value

    def __set__(self, instance, value):
        instance.requests[self._name].value = value


class ResourceRequestsContainerMeta(type):
    """Metaclass powering :class:`ResourceRequestsContainer`"""
    def __new__(cls, name, bases, namespace, **kwargs):
        result = super().__new__(cls, name, bases, namespace)
        for resource_name in result.RESOURCE_REQUESTS:
            setattr(result, resource_name, ResourceRequestDescriptor(resource_name))
        return result


class ResourceRequestsContainer(metaclass=ResourceRequestsContainerMeta):
    """Container for a collection of resource requests.

    It
    - has a :attr:`requests` member dictionary holding the request objects
    - has descriptors to allow each request value to be accessed as an
      attribute of the object.
    - provides utilities for formatting the request to be human readable.

    Subclasses must provide a RESOURCE_REQUESTS class member dictionary listing
    the supported requests.
    """
    RESOURCE_REQUESTS = {}

    def __init__(self):
        self.requests = {name: cls.empty_request() for name, cls in self.RESOURCE_REQUESTS.items()}

    def format_requests(self):
        return ''.join(' {}={}'.format(name, request.amount)
                       for name, request in self.requests.items() if request.amount)

    def __repr__(self):
        return '<{}{}>'.format(self.__class__.__name__, self.format_requests())


GLOBAL_RESOURCES = {'cpus': ScalarResource, 'mem': ScalarResource, 'disk': ScalarResource,
                    'ports': RangeResource, 'cores': RangeResource}
GPU_RESOURCES = {'compute': ScalarResource, 'mem': ScalarResource}
INTERFACE_RESOURCES = {'bandwidth_in': ScalarResource, 'bandwidth_out': ScalarResource}


class GPURequest(ResourceRequestsContainer):
    """Request for resources on a single GPU. These resources are not isolated,
    so the request functions purely to ensure that the scheduler does not try
    to over-allocate a GPU.

    Attributes
    ----------
    compute : float or Decimal
        Fraction of GPU's compute resource consumed
    mem : float or Decimal
        Memory usage (megabytes)
    affinity : bool
        If true, the GPU must be on the same NUMA node as the chosen CPU
        cores (ignored if no CPU cores are reserved).
    name : str, optional
        If specified, the name of the GPU must match this.
    """
    RESOURCE_REQUESTS = GPU_RESOURCES

    def __init__(self):
        super().__init__()
        self.affinity = False
        self.name = None

    def matches(self, agent_gpu, numa_node):
        if self.name is not None and self.name != agent_gpu.name:
            return False
        return numa_node is None or not self.affinity or agent_gpu.numa_node == numa_node

    def __repr__(self):
        return '<{} {}{}{}>'.format(
            self.__class__.__name__,
            self.name if self.name is not None else '*',
            self.format_requests(),
            ' affinity=True' if self.affinity else '')


class InterfaceRequest(ResourceRequestsContainer):
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
    bandwidth_in : float or Decimal
        Ingress bandwidth, in bps
    bandwidth_out : float or Decimal
        Egress bandwidth, in bps
    """
    RESOURCE_REQUESTS = INTERFACE_RESOURCES

    def __init__(self, network, infiniband=False, affinity=False):
        super().__init__()
        self.network = network
        self.infiniband = infiniband
        self.affinity = affinity

    def matches(self, interface, numa_node):
        if self.affinity and numa_node is not None and interface.numa_node != numa_node:
            return False
        if self.infiniband and not interface.infiniband_devices:
            return False
        return self.network == interface.network

    def __repr__(self):
        return '<{} {}{}{}{}>'.format(
            self.__class__.__name__,
            self.network, self.format_requests(),
            ' infiniband=True' if self.infiniband else '',
            ' affinity=True' if self.affinity else '')


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

    def __repr__(self):
        return '<{} {} {}{}>'.format(
            self.__class__.__name__, self.name, self.mode,
            ' affinity=True' if self.affinity else '')


class GPUResources:
    """Collection of specific resources allocated from a single agent GPU.

    Attributes
    ----------
    index : int
        Index into the agent's list of GPUs
    """
    def __init__(self, index):
        self.index = index
        prefix = 'katsdpcontroller.gpu.{}.'.format(index)
        self.resources = {name: cls(prefix + name) for name, cls in GPU_RESOURCES.items()}


class InterfaceResources:
    """Collection of specific resources for a single agent network interface.

    Attributes
    ----------
    index : int
        Index into the agent's list of interfaces
    """
    def __init__(self, index):
        self.index = index
        prefix = 'katsdpcontroller.interface.{}.'.format(index)
        self.resources = {name: cls(prefix + name) for name, cls in INTERFACE_RESOURCES.items()}


class ResourceAllocation:
    """Collection of specific resources allocated from a collection of offers.

    Attributes
    ----------
    agent : :class:`Agent`
        Agent from which the resources are allocated
    """
    def __init__(self, agent):
        self.agent = agent
        self.resources = {name: cls(name) for name, cls in GLOBAL_RESOURCES.items()}
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
            async with aiohttp.ClientSession(loop=loop, **kwargs) as session:
                try:
                    # Use a lowish timeout, so that we don't wedge the entire launch if
                    # there is a connection problem.
                    async with session.head(url, timeout=15, ssl_context=ssl_context) as response:
                        response.raise_for_status()
                        digest = response.headers['Docker-Content-Digest']
                except (aiohttp.client.ClientError, asyncio.TimeoutError) as error:
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


class TaskNoAgentError(InsufficientResourcesError):
    """Indicates that no agent was suitable for a task. Where possible, a
    sub-class is used to indicate a more specific error.
    """
    def __init__(self, node):
        super().__init__()
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


class TaskError(RuntimeError):
    """A batch job failed."""
    def __init__(self, node):
        super().__init__("Node {} failed with status {}".format(node.name, node.status.state))
        self.node = node


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

    def __repr__(self):
        return '<{} {!r}>'.format(self.__class__.__name__, self.name)


class LogicalExternal(LogicalNode):
    """An external service. It is assumed to be running as soon as it starts.

    The host and port must be set manually on the physical node. It also
    defaults to an empty :attr:`~LogicalNode.wait_ports`, which must be
    overridden if waiting is desired."""
    def __init__(self, name):
        super().__init__(name)
        self.physical_factory = PhysicalExternal
        self.wait_ports = []


class LogicalTask(LogicalNode, ResourceRequestsContainer):
    """A node in a logical graph that indicates how to run a task
    and what resources it requires, but does not correspond to any specific
    running task.

    The command is processed by :meth:`str.format` to substitute dynamic
    values.

    Attributes
    ----------
    cpus : Decimal
        Mesos CPU shares.
    mem : Decimal
        Mesos memory reservation (megabytes)
    disk : Decimal
        Mesos disk reservation (megabytes)
    max_run_time : float or None
        Maximum time to run with :meth:`batch_run` (seconds)
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
    host : str
        Hostname of agent where the task must be run. If ``None`` (the default)
        it is unconstrained.
    capabilities : list
        List of Linux capability names to add e.g. `SYS_NICE`. Note that this
        doesn't guarantee that the process can use the capability, if it runs
        as a non-root user.
    image : str
        Base name of the Docker image (without registry or tag).
    taskinfo : :class:`addict.Dict`
        A template for the final taskinfo message passed to Mesos. It will be
        copied into the physical task before being populated with computed
        fields. This is primarily intended to allow setting fields that are not
        otherwise configurable. Use with care.
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
    RESOURCE_REQUESTS = GLOBAL_RESOURCES

    def __init__(self, name):
        LogicalNode.__init__(self, name)
        ResourceRequestsContainer.__init__(self)
        self.max_run_time = None
        self.gpus = []
        self.interfaces = []
        self.volumes = []
        self.host = None
        self.capabilities = []
        self.image = None
        self.command = []
        self.wrapper = None
        self.taskinfo = Dict()
        self.taskinfo.container.type = 'DOCKER'
        self.taskinfo.command.shell = False
        self.physical_factory = PhysicalTask

    def valid_agent(self, agent):
        """Checks whether the attributes of an agent are suitable for running
        this task. Subclasses may override this to enforce constraints e.g.,
        requiring a special type of hardware."""
        # TODO: enforce x86-64, if we ever introduce ARM or other hardware
        return self.host is None or agent.host == self.host

    def __repr__(self):
        s = io.StringIO()
        s.write('<{} {!r}{}'.format(self.__class__.__name__, self.name, self.format_requests()))
        if self.gpus:
            s.write(' gpus={}'.format(self.gpus))
        if self.interfaces:
            s.write(' interfaces={}'.format(self.interfaces))
        if self.volumes:
            s.write(' volumes={}'.format(self.volumes))
        if self.host:
            s.write(' host={!r}'.format(self.host))
        s.write('>')
        return s.getvalue()


class AgentGPU(GPUResources):
    """A single GPU on an agent machine, tracking both attributes and free resources."""
    def __init__(self, spec, index):
        super().__init__(index)
        self.devices = spec['devices']
        self.uuid = spec.get('uuid')
        self.driver_version = spec['driver_version']
        self.name = spec['name']
        self.compute_capability = tuple(spec['compute_capability'])
        self.device_attributes = spec['device_attributes']
        self.numa_node = spec.get('numa_node')


class AgentInterface(InterfaceResources):
    """A single interface on an agent machine, tracking both attributes and free resources.

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
    resources : list of :class:`Resource`
        Available resources
    """
    def __init__(self, spec, index):
        super().__init__(index)
        self.name = spec['name']
        self.network = spec['network']
        self.ipv4_address = ipaddress.IPv4Address(spec['ipv4_address'])
        self.numa_node = spec.get('numa_node')
        self.infiniband_devices = spec.get('infiniband_devices', [])


def _decode_json_base64(value):
    """Decodes a object that has been encoded with JSON then url-safe base64."""
    json_bytes = base64.urlsafe_b64decode(value)
    return json.loads(json_bytes.decode('utf-8'))


class Agent:
    """Collects multiple offers for a single Mesos agent and role and allows
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
        self.role = offers[0].allocation_info.role
        self.host = offers[0].hostname
        self.attributes = offers[0].attributes
        self.interfaces = []
        self.infiniband_devices = []
        self.volumes = []
        self.gpus = []
        self.numa = []
        self.priority = None
        self.nvidia_container_runtime = False
        self._min_port = min_port
        for attribute in offers[0].attributes:
            try:
                if attribute.name == 'katsdpcontroller.interfaces' and attribute.type == 'TEXT':
                    value = _decode_json_base64(attribute.text.value)
                    schemas.INTERFACES.validate(value)
                    self.interfaces = [AgentInterface(item, i) for i, item in enumerate(value)]
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
                    self.gpus = [AgentGPU(item, i) for i, item in enumerate(value)]
                elif attribute.name == 'katsdpcontroller.numa' and attribute.type == 'TEXT':
                    value = _decode_json_base64(attribute.text.value)
                    schemas.NUMA.validate(value)
                    self.numa = value
                elif (attribute.name == 'katsdpcontroller.infiniband_devices'
                      and attribute.type == 'TEXT'):
                    value = _decode_json_base64(attribute.text.value)
                    schemas.INFINIBAND_DEVICES.validate(value)
                    self.infiniband_devices = value
                elif (attribute.name == 'katsdpcontroller.nvidia_container_runtime'
                      and attribute.type == 'TEXT'):
                    value = _decode_json_base64(attribute.text.value)
                    schemas.NVIDIA_CONTAINER_RUNTIME.validate(value)
                    self.nvidia_container_runtime = value
                elif attribute.name == 'katsdpcontroller.priority' and attribute.type == 'SCALAR':
                    self.priority = attribute.scalar.value
            except (ValueError, KeyError, TypeError, ipaddress.AddressValueError):
                logger.warning('Could not parse %s (%s)',
                               attribute.name, attribute.text.value)
                logger.debug('Exception', exc_info=True)
            except jsonschema.ValidationError as e:
                logger.warning('Validation error parsing %s: %s', value, e)

        # These resources all represent resources not yet allocated
        self.resources = {name: cls(name) for name, cls in GLOBAL_RESOURCES.items()}
        for offer in offers:
            for resource in offer.resources:
                # Skip specialised resource types and use only general-purpose
                # resources.
                if 'disk' in resource and 'source' in resource.disk:
                    continue
                if resource.name in self.resources:
                    self.resources[resource.name].add(resource)
                elif resource.name.startswith('katsdpcontroller.gpu.'):
                    parts = resource.name.split('.', 3)
                    # TODO: catch exceptions here
                    index = int(parts[2])
                    resource_name = parts[3]
                    if resource_name in self.gpus[index].resources:
                        self.gpus[index].resources[resource_name].add(resource)
                elif resource.name.startswith('katsdpcontroller.interface.'):
                    parts = resource.name.split('.', 3)
                    # TODO: catch exceptions here
                    index = int(parts[2])
                    resource_name = parts[3]
                    if resource_name in self.interfaces[index].resources:
                        self.interfaces[index].resources[resource_name].add(resource)
        if self.priority is None:
            self.priority = float(len(self.gpus) +
                                  len(self.interfaces) +
                                  len(self.volumes))
        # Split offers of cores by NUMA node
        self.numa_cores = [self.resources['cores'].subset(numa_node)
                           for numa_node in self.numa]
        del self.resources['cores']   # Prevent accidentally allocating from this
        logger.debug('Agent %s has priority %f', self.agent_id, self.priority)

    @classmethod
    def _match_children(cls, numa_node, requested, actual, msg):
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
                for name in request.requests:
                    need = request.requests[name].amount
                    have = item.resources[name].available
                    if have < need:
                        logger.debug('Not enough %s on %s %d for request %d',
                                     name, msg, j, i)
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
            cores = self.numa_cores[numa_node]
        else:
            cores = RangeResource('cores')
        need = logical_task.requests['cores'].amount
        have = cores.available
        if need > have:
            raise InsufficientResourcesError('not enough cores on node {} ({} < {})'.format(
                numa_node, have, need))

        # Match network requests to interfaces
        interface_map = self._match_children(
            numa_node, logical_task.interfaces, self.interfaces, 'interface')
        # Match volume requests to volumes
        for request in logical_task.volumes:
            if not any(request.matches(volume, numa_node) for volume in self.volumes):
                if not any(request.matches(volume, None) for volume in self.volumes):
                    raise InsufficientResourcesError('Volume {} not present'.format(request.name))
                else:
                    raise InsufficientResourcesError(
                        'Volume {} not present on NUMA node {}'.format(request.name, numa_node))
        # Match GPU requests to GPUs
        gpu_map = self._match_children(numa_node, logical_task.gpus, self.gpus, 'GPU')

        # Have now verified that the task fits. Create the resources for it
        alloc = ResourceAllocation(self)
        for name, request in logical_task.requests.items():
            if name == 'cores':
                res = cores.allocate(request.amount)
            elif name == 'ports':
                try:
                    res = self.resources[name].allocate(request.amount, minimum=self._min_port)
                except ValueError:
                    res = self.resources[name].allocate(request.amount)
            else:
                res = self.resources[name].allocate(request.amount)
            alloc.resources[name] = res

        alloc.interfaces = [None] * len(logical_task.interfaces)
        for i, interface in enumerate(self.interfaces):
            idx = interface_map[i]
            if idx is not None:
                request = logical_task.interfaces[idx]
                interface_alloc = InterfaceResources(i)
                for name, req in request.requests.items():
                    interface_alloc.resources[name] = interface.resources[name].allocate(req.amount)
                alloc.interfaces[idx] = interface_alloc
        for request in logical_task.volumes:
            alloc.volumes.append(next(volume for volume in self.volumes
                                      if request.matches(volume, numa_node)))
        alloc.gpus = [None] * len(logical_task.gpus)
        for i, gpu in enumerate(self.gpus):
            idx = gpu_map[i]
            if idx is not None:
                request = logical_task.gpus[idx]
                gpu_alloc = GPUResources(i)
                for name, req in request.requests.items():
                    gpu_alloc.resources[name] = gpu.resources[name].allocate(req.amount)
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
        for name, request in logical_task.requests.items():
            if name == 'cores':
                continue    # Handled specially lower down
            need = request.amount
            have = self.resources[name].available
            if have < need:
                raise InsufficientResourcesError(
                    'Not enough {} ({} < {})'.format(name, have, need))

        if logical_task.requests['cores'].amount:
            # For tasks requesting cores we activate NUMA awareness
            for numa_node in range(len(self.numa)):
                try:
                    return self._allocate_numa_node(numa_node, logical_task)
                except InsufficientResourcesError:
                    logger.debug('Failed to allocate NUMA node %d on %s',
                                 numa_node, self.agent_id, exc_info=True)
            raise InsufficientResourcesError('No suitable NUMA node found')
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
    death_expected : bool
        The task has been requested to exit or is a batch task, and so if it
        dies it is normal rather than a failure.
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
        self.depends_ready = []
        self.death_expected = False
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
            logger.exception('exception while waiting for task %s to be ready', self.name)

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
                logger.warning('Ignoring state change that went backwards (%s -> %s) on task %s',
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
        self.death_expected = True

    def clone(self):
        """Create a duplicate of the task, ready to be run.

        The duplicate is in state :const:`TaskState.NOT_READY` and is
        unresolved.
        """
        return self.logical_node.physical_factory(self.logical_node, self.loop)


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

    def clone(self):
        dup = super().clone()
        dup.host = self.host
        dup.ports = copy.copy(self.ports)
        return dup


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
    host : str
        Host running the task
    status : :class:`mesos_pb2.TaskStatus`
        Last Mesos status allocated to this task. This should agree with
        :attr:`state`, but if we receive notifications out-of-order from Mesos
        then it might not. If :attr:`state` is :const:`TaskState.DEAD` then it
        is guaranteed to be in sync.
    start_time : float
        Timestamp at which status became TASK_RUNNING
    ports : dict
        Maps port names given in the logical task to port numbers.
    cores : dict
        Maps core names given in the logical task to core numbers.
    agent : :class:`Agent`
        Information about the agent on which this task is running.
    agent_id : str
        Slave ID of the agent on which this task is running
    queue : :class:`LaunchQueue`
        The queue on which this task was (most recently) launched
    """
    def __init__(self, logical_task, loop):
        super().__init__(logical_task, loop)
        self.interfaces = {}
        self.endpoints = {}
        self.taskinfo = None
        self.allocation = None
        self.status = None
        self.start_time = None
        self._queue = None
        for name, cls in GLOBAL_RESOURCES.items():
            if issubclass(cls, RangeResource):
                setattr(self, name, {})

    @property
    def agent(self):
        if self.allocation:
            return self.allocation.agent
        return None

    @property
    def host(self):
        if self.allocation:
            return self.allocation.agent.host
        return None

    @property
    def agent_id(self):
        if self.allocation:
            return self.allocation.agent.agent_id
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
        for resource in self.allocation.resources.values():
            if isinstance(resource, RangeResource):
                d = {}
                for name, value in zip(self.logical_node.requests[resource.name].value, resource):
                    if name is not None:
                        d[name] = value
                setattr(self, resource.name, d)

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
        taskinfo = copy.deepcopy(self.logical_node.taskinfo)
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
        image_path = await resolver.image_resolver(self.logical_node.image, loop)
        taskinfo.container.docker.image = image_path
        taskinfo.agent_id.value = self.agent_id
        taskinfo.resources = []
        for resource in self.allocation.resources.values():
            taskinfo.resources.extend(resource.info())

        if self.allocation.resources['cores']:
            core_list = ','.join(str(core) for core in self.allocation.resources['cores'])
            docker_parameters.append({'key': 'cpuset-cpus', 'value': core_list})

        any_infiniband = False
        for request, interface_alloc in zip(self.logical_node.interfaces,
                                            self.allocation.interfaces):
            for resource in interface_alloc.resources.values():
                taskinfo.resources.extend(resource.info())
            if request.infiniband:
                any_infiniband = True
        if any_infiniband:
            # ibverbs uses memory mapping for DMA. Take away the default rlimit
            # maximum since Docker tends to set a very low limit.
            docker_parameters.append({'key': 'ulimit', 'value': 'memlock=-1'})
            # rdma_get_devices requires *all* the devices to be present to
            # succeed, even if they're not all used.
            if self.agent.infiniband_devices:
                docker_devices.update(self.agent.infiniband_devices)
            else:
                # Fallback for machines that haven't been updated with the
                # latest agent_mkconfig.py.
                for interface in self.agent.interfaces:
                    docker_devices.update(interface.infiniband_devices)

        gpu_driver_version = None
        # UUIDs for GPUs to be handled by nvidia-container-runtime
        gpu_uuids = []
        for gpu_alloc in self.allocation.gpus:
            for resource in gpu_alloc.resources.values():
                taskinfo.resources.extend(resource.info())
            gpu = self.agent.gpus[gpu_alloc.index]
            if self.agent.nvidia_container_runtime and gpu.uuid:
                gpu_uuids.append(gpu.uuid)
            else:
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
        if gpu_uuids:
            docker_parameters.append({'key': 'runtime', 'value': 'nvidia'})
            env = taskinfo.command.environment.setdefault('variables', [])
            env.append({
                'name': 'NVIDIA_VISIBLE_DEVICES',
                'value': ','.join(gpu_uuids)
            })

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
        for r, cls in GLOBAL_RESOURCES.items():
            if issubclass(cls, RangeResource):
                args[r] = getattr(self, r)
        args['interfaces'] = self.interfaces
        args['endpoints'] = self.endpoints
        args['host'] = self.host
        args['resolver'] = resolver
        return args

    def set_state(self, state):
        if self._queue is not None:
            self._queue.state_gauges[self.state].dec()
        try:
            super().set_state(state)
        finally:
            if self._queue is not None:
                self._queue.state_gauges[self.state].inc()

    def set_status(self, status):
        self.status = status
        if status.state == 'TASK_RUNNING':
            self.start_time = status.timestamp

    def kill(self, driver, **kwargs):
        # TODO: according to the Mesos docs, killing a task is not reliable,
        # and may need to be attempted again.
        # The poller is stopped by set_state, so we do not need to do it here.
        driver.killTask(self.taskinfo.task_id)
        super().kill(driver, **kwargs)

    @property
    def queue(self):
        return self._queue

    @queue.setter
    def queue(self, queue):
        if self._queue is not None:
            self._queue.state_gauges[self.state].dec()
        self._queue = queue
        if self._queue is not None:
            self._queue.state_gauges[self.state].inc()

    def __del__(self):
        # Avoid racking up counts if a launch is cancelled. However, once
        # a task gets going (and presumably eventually gets to DEAD), leave
        # it so that we can see how many tasks ran in total.
        # The hasattr check is in case we somehow fail early in __init__.
        if hasattr(self, '_queue') and self._queue is not None and self.state >= TaskState.STARTED:
            self._queue.state_gauges[self.state].dec()


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
    resources_future : :class:`asyncio.Future`
        A future that is set once the resources are acquired. The value is
        ``None``, so it is only useful to get completion (it is not used
        for exceptions; see :attr:`last_insufficient` instead).
    last_insufficient : :class:`InsufficientResourcesError`
        An exception describing in more detail why there were insufficient
        resources. It is set to ``None`` once the resources are acquired.
    future : :class:`asyncio.Future`
        A future that is set with the result of the launch i.e., once the
        group has been removed from the queue. The value is ``None``, so it
        is only useful to get completion and exceptions.
    deadline : float
        Loop timestamp by which the resources must be acquired. This is
        currently approximate (i.e. the actual timeout may occur at a slightly
        different time) and is used only for sorting.
    """
    def __init__(self, graph, nodes, resolver, deadline, loop):
        self.nodes = nodes
        self.graph = graph
        self.resolver = resolver
        self.resources_future = asyncio.Future(loop=loop)
        self.future = asyncio.Future(loop=loop)
        self.deadline = deadline
        self.last_insufficient = InsufficientResourcesError('No resource offers received')


class LaunchQueue:
    """Queue of launch requests.

    Parameters
    ----------
    role : str
        Mesos role used for tasks in the queue
    name : str, optional
        Name of the queue for ``__repr__``
    priority : int
        Priority of the tasks in the queue. A smaller numeric value indicates a
        higher-priority queue (ala UNIX nice).
    """
    def __init__(self, role, name='', *, priority=0):
        self.role = role
        self.name = name
        self.priority = priority
        self._groups = deque()
        self.state_gauges = {
            state: TASKS_IN_STATE.labels(name, state.name) for state in TaskState
        }

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
        for node in group.nodes:
            if isinstance(node, PhysicalTask):
                node.queue = self

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

    Attribute data is not transferred to the new graph.

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
        edge_filter = lambda data: bool(data.get(attr))   # noqa: E731
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
    - every role in :attr:`_roles_wanted` is non-suppressed (there may be
      additional non-suppressed roles, but they will be suppressed when an
      offer arrives)
    - every role in :attr:`_offers` also appears in :attr:`_roles_wanted`

    Parameters
    ----------
    loop : :class:`asyncio.AbstractEventLoop`
        Event loop
    default_role : str
        Mesos role used by the default queue
    http_port : int
        Port for the embedded HTTP server, or 0 to assign a free one
    http_url : str, optional
        URL at which agent nodes can reach the HTTP server. If not specified,
        tries to deduce it from the host's FQDN.
    runner_kwargs : dict, optional
        Extra arguments to pass to construct the :class:`aiohttp.web.AppRunner`

    Attributes
    ----------
    resources_timeout : float or None
        Default time limit for resources to become available once a task is ready to
        launch.
    app : :class:`aiohttp.web.Application`
        Web application used internally for scheduling. Prior to calling
        :meth:`start` it can be modified e.g. to add additional endpoints.
    http_port : int
        Actual HTTP port used for the HTTP server (after :meth:`start`)
    http_url : str
        Actual external URL to use for the HTTP server (after :meth:`start`)
    http_runner : :class:`aiohttp.web.AppRunner`
        Runner for the HTTP app
    """
    def __init__(self, loop, default_role, http_port, http_url=None, runner_kwargs=None):
        self._loop = loop
        self._driver = None
        self._offers = {}           #: offers keyed by role then agent ID then offer ID
        #: set when it's time to retry a launch (see _launcher)
        self._wakeup_launcher = asyncio.Event(loop=self._loop)
        self._default_queue = LaunchQueue(default_role)
        self._queues = [self._default_queue]
        #: Mesos roles for which we want to (and expect to) receive offers
        self._roles_needed = set()
        #: (task, graph) for tasks that have been launched (STARTED to KILLING), indexed by task ID
        self._active = {}
        self._closing = False       #: set to ``True`` when :meth:`close` is called
        self._min_ports = {}        #: next preferred port for each agent (keyed by ID)
        # If offers come at 5s intervals, then 11s gives two chances.
        self.resources_timeout = 11.0   #: Time to wait for sufficient resources to be offered
        self.http_port = http_port
        self.http_url = http_url
        self._launcher_task = loop.create_task(self._launcher())

        # Configure the web app
        app = aiohttp.web.Application(loop=self._loop)
        app['katsdpcontroller_scheduler'] = self
        app.router.add_get('/tasks/{id}/wait_start', wait_start_handler)
        app.router.add_static('/static',
                              pkg_resources.resource_filename('katsdpcontroller', 'static'))
        self.app = app
        if runner_kwargs is None:
            runner_kwargs = {}
        self.http_runner = aiohttp.web.AppRunner(app, **runner_kwargs)

    async def start(self):
        """Start the internal HTTP server running. This must be called before any launches.

        This starts up the webapp, so any additions to it must be made first.
        """
        if self.http_runner.sites:
            raise RuntimeError('Already started')
        await self.http_runner.setup()
        # We want a single port serving both IPv4 and IPv6. Using TCPSite
        # will create a separate socket for each, and if http_port is 0 (used
        # by unit tests) they end up with different ports.
        # See https://stackoverflow.com/questions/45907833 for more details.
        sock = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(('::', self.http_port))

        site = aiohttp.web.SockSite(self.http_runner, sock)
        await site.start()
        if not self.http_port:
            self.http_port = self.http_runner.addresses[0][1]
        if not self.http_url:
            self.http_url = site.name
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

        Sort nodes. Pinned nodes come first, since they don't have any choice
        about placement. Those requiring core affinity are put next, since they
        tend to be the trickiest to pack. Then order by CPUs, GPUs, memory.
        Finally sort by name so that results are more reproducible.
        """
        node = physical_node.logical_node
        if isinstance(node, LogicalTask):
            return (node.host is not None, len(node.cores), node.cpus,
                    len(node.gpus), node.mem, node.name)
        return (False, 0, 0, 0, 0, node.name)

    def _update_roles(self, new_roles):
        revive_roles = new_roles - self._roles_needed
        suppress_roles = self._roles_needed - new_roles
        self._roles_needed = new_roles
        if revive_roles:
            self._driver.reviveOffers(revive_roles)
        if suppress_roles:
            self._driver.suppressOffers(suppress_roles)
            # Decline all held offers that aren't needed any more
            to_decline = []
            for role in suppress_roles:
                role_offers = self._offers.pop(role, {})
                for agent_offers in role_offers.values():
                    to_decline.extend([offer.id for offer in agent_offers.values()])
            if to_decline:
                self._driver.declineOffer(to_decline)

    def _remove_offer(self, role, agent_id, offer_id):
        try:
            del self._offers[role][agent_id][offer_id]
            if not self._offers[role][agent_id]:
                del self._offers[role][agent_id]
            if not self._offers[role]:
                del self._offers[role]
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
        def format_time(t):
            return time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime(t))

        to_decline = []
        to_suppress = set()
        for offer in offers:
            if offer.unavailability:
                start_time_ns = offer.unavailability.start.nanoseconds
                end_time_ns = start_time_ns + offer.unavailability.duration.nanoseconds
                start_time = start_time_ns / 1e9
                end_time = end_time_ns / 1e9
                if end_time >= time.time():
                    logger.debug('Declining offer %s from %s: unavailable from %s to %s',
                                 offer.id.value, offer.hostname,
                                 format_time(start_time), format_time(end_time))
                    to_decline.append(offer.id)
                    continue
                else:
                    logger.debug('Offer %s on %s has unavailability in the past: %s to %s',
                                 offer.id.value, offer.hostname,
                                 format_time(start_time), format_time(end_time))
            role = offer.allocation_info.role
            if role in self._roles_needed:
                logger.debug('Adding offer %s on %s with role %s to pool',
                             offer.id.value, offer.agent_id.value, role)
                role_offers = self._offers.setdefault(role, {})
                agent_offers = role_offers.setdefault(offer.agent_id.value, {})
                agent_offers[offer.id.value] = offer
            else:
                # This can happen either due to a race or at startup, when
                # we haven't yet suppressed roles.
                logger.debug('Declining offer %s on %s with role %s',
                             offer.id.value, offer.agent_id.value, role)
                to_decline.append(offer.id)
                to_suppress.add(role)

        if to_decline:
            self._driver.declineOffer(to_decline)
        if to_suppress:
            self._driver.suppressOffers(to_suppress)
        self._wakeup_launcher.set()

    @run_in_event_loop
    def inverseOffers(self, driver, offers):
        for offer in offers:
            logger.debug('Declining inverse offer %s', offer.id.value)
            self._driver.declineInverseOffer(offer.id)

    @run_in_event_loop
    def offerRescinded(self, driver, offer_id):
        # TODO: this is not very efficient. A secondary lookup from offer id to
        # the relevant offer info would speed it up.
        for role, role_offers in self._offers.items():
            for agent_id, agent_offers in role_offers.items():
                if offer_id.value in agent_offers:
                    self._remove_offer(role, agent_id, offer_id.value)
                    return

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
            task.set_status(status)
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
        with decimal.localcontext(DECIMAL_CONTEXT):
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
            for r, cls in GLOBAL_RESOURCES.items():
                # Cores are special because only the cores on a single NUMA node
                # can be allocated together
                if r == 'cores':
                    available = [cores.available
                                 for agent in agents for cores in agent.numa_cores]
                else:
                    available = [agent.resources[r].available for agent in agents]
                max_resources[r] = max(available) if available else cls.ZERO
                total_resources[r] = sum(available, cls.ZERO)
            for r, cls in GPU_RESOURCES.items():
                available = [gpu.resources[r].available for agent in agents for gpu in agent.gpus]
                max_gpu_resources[r] = max(available) if available else cls.ZERO
                total_gpu_resources[r] = sum(available, cls.ZERO)

            # Collect together all interfaces on the same network
            networks = {}
            for agent in agents:
                for interface in agent.interfaces:
                    networks.setdefault(interface.network, []).append(interface)
            for network, interfaces in networks.items():
                max_interface_resources[network] = {}
                total_interface_resources[network] = {}
                for r in INTERFACE_RESOURCES:
                    available = [interface.resources[r].available for interface in interfaces]
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
                    for r in GLOBAL_RESOURCES:
                        need = logical_task.requests[r].amount
                        if need > max_resources[r]:
                            raise TaskInsufficientResourcesError(node, r, need, max_resources[r])
                    for i, request in enumerate(logical_task.gpus):
                        for r in GPU_RESOURCES:
                            need = request.requests[r].amount
                            if need > max_gpu_resources[r]:
                                raise TaskInsufficientGPUResourcesError(
                                    node, i, r, need, max_gpu_resources[r])
                    for request in logical_task.interfaces:
                        for r in INTERFACE_RESOURCES:
                            need = request.requests[r].amount
                            if need > max_interface_resources[request.network][r]:
                                raise TaskInsufficientInterfaceResourcesError(
                                    node, request, r, need,
                                    max_interface_resources[request.network][r])
                    # This node doesn't fit but the reason is more complex e.g.
                    # there is enough of each resource individually but not all on
                    # the same agent or NUMA node.
                    raise TaskNoAgentError(node)

            # Nodes are all individually launchable, but we weren't able to launch
            # all of them due to some contention. Check if any one resource is
            # over-subscribed.
            for r, cls in GLOBAL_RESOURCES.items():
                need = sum((node.logical_node.requests[r].amount for node in nodes),
                           cls.ZERO)
                if need > total_resources[r]:
                    raise GroupInsufficientResourcesError(r, need, total_resources[r])
            for r, cls in GPU_RESOURCES.items():
                need = sum((request.requests[r].amount
                            for node in nodes for request in node.logical_node.gpus),
                           cls.ZERO)
                if need > total_gpu_resources[r]:
                    raise GroupInsufficientGPUResourcesError(r, need, total_gpu_resources[r])
            for network in networks:
                for r, cls in INTERFACE_RESOURCES.items():
                    need = sum((request.requests[r].amount
                                for node in nodes for request in node.logical_node.interfaces
                                if request.network == network),
                               cls.ZERO)
                    if need > total_interface_resources[network][r]:
                        raise GroupInsufficientInterfaceResourcesError(
                            network, r, need, total_interface_resources[network][r])
            # Not a simple error e.g. due to packing problems
            raise InsufficientResourcesError("Insufficient resources to launch all tasks")

    async def _launch_once(self):
        """Run single iteration of :meth:`_launcher`"""
        candidates = [(queue, queue.front()) for queue in self._queues if queue]
        if candidates:
            # Filter out any candidates other than the highest priority
            priority = min(queue.priority for (queue, group) in candidates)
            candidates = [candidate for candidate in candidates
                          if candidate[0].priority == priority]
            # Order by deadline, to give some degree of fairness
            candidates.sort(key=lambda x: x[1].deadline)
        # Revive/suppress to match the necessary roles
        roles = set(queue.role for (queue, group) in candidates)
        self._update_roles(roles)

        for queue, group in candidates:
            nodes = group.nodes
            role = queue.role
            try:
                # Due to concurrency, another coroutine may have altered
                # the state of the tasks since they were put onto the
                # pending list (e.g. by killing them). Filter those out.
                nodes = [node for node in nodes if node.state == TaskState.STARTING]
                agents = [Agent(list(offers.values()), self._min_ports.get(agent_id, 0))
                          for agent_id, offers in self._offers.get(role, {}).items()]
                # Back up the original agents so that if allocation fails we can
                # diagnose it.
                orig_agents = copy.deepcopy(agents)
                # Sort agents by GPUs then free memory. This makes it less likely that a
                # low-memory process will hog all the other resources on a high-memory
                # box. Similarly, non-GPU tasks will only use GPU-equipped agents if
                # there is no other choice. Should eventually look into smarter
                # algorithms e.g. Dominant Resource Fairness
                # (http://mesos.apache.org/documentation/latest/allocation-module/)
                agents.sort(key=lambda agent: (agent.priority, agent.resources['mem'].available))
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
                try:
                    # At this point we have a sufficient set of offers.
                    group.last_insufficient = None
                    if not group.resources_future.done():
                        group.resources_future.set_result(None)
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
                        # No need to set _wakeup_launcher, because the cancellation did so.
                        continue
                    # Launch the tasks
                    new_min_ports = {}
                    taskinfos = {agent: [] for agent in agents}
                    for (node, allocation) in allocations:
                        taskinfos[node.agent].append(node.taskinfo)
                        for port in list(allocation.resources['ports']):
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
                            self._remove_offer(role, agent.agent_id, offer_id.value)
                        logger.info('Launched %d tasks on %s',
                                    len(taskinfos[agent]), agent.agent_id)
                    for node in nodes:
                        node.set_state(TaskState.STARTED)
                    group.future.set_result(None)
                    queue.remove(group)
                    self._wakeup_launcher.set()
                except Exception as error:
                    if not group.future.done():
                        group.future.set_exception(error)
                    # Don't remove from the queue: launch() handles that
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
        # All are signalled by setting _wakeup_launcher. In some cases it is set
        # when it is not actually needed - this is harmless as long as the number
        # of queues doesn't get out of hand.
        while True:
            await self._wakeup_launcher.wait()
            self._wakeup_launcher.clear()
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
            Time (seconds) to wait for sufficient resources to launch the
            nodes. If not specified, defaults to the class value.

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
            node.set_state(TaskState.STARTING)
        if resources_timeout is not None:
            deadline = self._loop.time() + resources_timeout
        else:
            deadline = math.inf
        pending = _LaunchGroup(graph, remaining, resolver, deadline, self._loop)
        empty = not queue
        queue.add(pending)
        if empty:
            self._wakeup_launcher.set()
        try:
            await asyncio.wait_for(pending.resources_future, timeout=resources_timeout)
            await pending.future
        except Exception as error:
            logger.debug('Exception in launching group', exc_info=True)
            # Could be
            # - a timeout
            # - we were cancelled
            # - some internal error, such as the image was not found
            # - close() was called, which cancels pending.future
            for node in remaining:
                if node.state == TaskState.STARTING:
                    node.set_state(TaskState.NOT_READY)
            queue.remove(pending)
            self._wakeup_launcher.set()
            if isinstance(error, asyncio.TimeoutError) and pending.last_insufficient is not None:
                raise pending.last_insufficient from None
            else:
                raise
        ready_futures = [node.ready_event.wait() for node in nodes]
        await asyncio.gather(*ready_futures, loop=self._loop)

    async def _batch_run_once(self, graph, resolver, nodes, *,
                              queue, resources_timeout):
        """Single attempt for :meth:`batch_run`."""
        async def wait_one(node):
            await asyncio.wait_for(node.dead_event.wait(), timeout=node.logical_node.max_run_time)
            if node.status is not None and node.status.state != 'TASK_FINISHED':
                raise TaskError(node)

        for node in nodes:
            # Batch tasks die on their own
            node.death_expected = True
        await self.launch(graph, resolver, nodes, queue=queue, resources_timeout=resources_timeout)
        futures = []
        for node in nodes:
            if isinstance(node, PhysicalTask):
                futures.append(self._loop.create_task(wait_one(node)))
        try:
            done, pending = await asyncio.wait(futures, loop=self._loop,
                                               return_when=asyncio.FIRST_EXCEPTION)
            # Raise the TaskError if any
            for future in done:
                future.result()
        finally:
            # asyncio.wait doesn't cancel futures if it is itself cancelled
            for future in futures:
                future.cancel()
            # In success case this will be immediate since it's all dead already
            await self.kill(graph, nodes)

    async def batch_run(self, graph, resolver, nodes=None, *,
                        queue=None, resources_timeout=None,
                        attempts=1):
        """Launch and run a batch graph (i.e., one that terminates on its own).

        Apart from the exceptions explicitly listed below, any exceptions
        raised by :meth:`launch` are also applicable.

        .. note::
            If retries are needed, then `graph` is modified in place with
            replacement physical nodes (see :meth:`PhysicalNode.clone`).

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
            Time (seconds) to wait for sufficient resources to launch the nodes. If not
            specified, defaults to the class value.
        attempts : int
            Number of times to try running the graph

        Raises
        ------
        TaskError
            if the graph failed (any of the tasks exited with a status other
            than TASK_FINISHED) on all attempts
        asyncio.TimeoutError
            if the :attr:`~LogicalTask.max_run_time` is breached for some task
        """
        if nodes is None:
            nodes = list(graph.nodes())
        while attempts > 0:
            attempts -= 1
            try:
                await self._batch_run_once(graph, resolver, nodes,
                                           queue=queue,
                                           resources_timeout=resources_timeout)
            except TaskError as error:
                if not attempts:
                    raise
                else:
                    logger.warning('Batch graph failed (%s), retrying', error)
                    new_nodes = [node.clone() for node in nodes]
                    mapping = dict(zip(nodes, new_nodes))
                    networkx.relabel_nodes(graph, mapping, copy=False)
                    nodes = new_nodes
            else:
                break

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
                    self._wakeup_launcher.set()
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
        await self.http_runner.cleanup()
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
        async with aiohttp.ClientSession() as session:
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
            return self._active[task_id][0]
        except KeyError:
            return None


__all__ = [
    'LogicalNode', 'PhysicalNode',
    'LogicalExternal', 'PhysicalExternal',
    'LogicalTask', 'PhysicalTask', 'TaskState',
    'Volume',
    'ResourceRequest', 'ScalarResourceRequest', 'RangeResourceRequest',
    'Resource', 'ScalarResource', 'RangeResource',
    'ResourceAllocation',
    'GPUResources', 'InterfaceResources',
    'InsufficientResourcesError',
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
