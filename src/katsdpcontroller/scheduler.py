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

The node must also provide nvidia-container-runtime for access to the
devices.

Network interface support
-------------------------
There is support for requesting use of a network interface on a specific
network (for example, providing high-speed access to a particular resource).
Information about which interfaces on an agent are connected to which networks
is encoded in the :code:`katsdpcontroller.interfaces` Mesos attribute, as a
base64url-encoded JSON string conforming to
:const:`katsdpcontroller.schemas.INTERFACES`. For the ith interface in this
list (counting from 0) there must be corresponding resources
:samp:`katsdpcontroller.interface.{i}.bandwidth_in` and
:samp:`katsdpcontroller.interface.{i}.bandwidth_out` set to the interface speed
in bits per second.

There is also support for using the ibverbs library for raw Ethernet. An
interface is assumed to support this if it has an `infiniband_devices` section
in its attributes, listing related device files.  Tasks can request this
support by specifying `infiniband=True` when constructing the
:class:`InterfaceRequest`.  The scheduler will pass through the devices and
allow an unlimited amount of locked memory, but if the container runs as a
non-root user, it may need to take its own steps to obtain the necessary
capabilities.

In the Mellanox implementation there are some limitations on multicast:

1. If the device is configured (via
:samp:`/sys/class/net/{interface}/settings/force_local_lb_disable`) to disallow
multicast loopback, then if one task sends multicast data via ibverbs, and
another subscribes (whether or not via ibverbs), then they cannot use the same
network interface. To ensure that this does not happen, you must

  - Add ``infiniband_multicast_loopback: false`` to the Mesos attributes for
    the interface (this is done automatically by the
    :mod:`katsdpcontroller.agent_mkconfig` module defined below).
  - Declare the multicast groups used by each task, using the `multicast_in`
    and `multicast_out` attributes of :class:`InterfaceRequest`.

2. If a subscriber uses ibverbs but the sender does not, then they cannot use
   the same network interface, regardless of the loopback setting. Currently
   katsdpcontroller has no logic to handle this, and any multicast group that
   is consumed using ibverbs should also be produced using ibverbs.

Agent prioritisation
--------------------
Each agent is assigned a 'priority', and tasks are assigned to the
lowest-priority agent where they fit (so that specialised tasks that can only
run on one agent are not blocked by more generic tasks being assigned to that
agent). By default, the priority is the total number of volumes, interfaces and
GPUs it has. This can be overridden by assigning a `katsdpcontroller.priority`
scalar attribute. Ties are broken by amount of available memory.

Subsystems
----------
It may be desirable to restrict particular sets of tasks to particular sets of
agents for administrative reasons. Each task may be assigned to a "subsystem"
(a string). Each agent may belong to a set of subsystems, or be unassigned and
able to run any task. A task with an assigned subsystem will only run on
agents belonging to that subsystem (or agents without assigned subsystems).
The subsystems for an agent are specified by assigning a
`katsdpcontroller.subsystems` attribute as a base64url-encoded JSON string
conforming to :const:`katsdpcontroller.schemas.SUBSYSTEMS`.

Setting up agents
-----------------
The previous sections list a number of resources and attributes that need to be
configured on each agent. To simplify this, an executable module
(:mod:`katsdpcontroller.agent_mkconfig`) is provided that will interrogate most of
the necessary information from the system. It assumes that Mesos is run with
system wrappers that source arguments from :file:`/etc/mesos-slave/attributes`
and :file:`/etc/mesos-slave/resources` (this is the case for the Ubuntu
packages; untested for other operating systems).

Note that Mesos slaves typically require a manual `recovery`_ step after
changing resources or attributes and restarting.

.. _recovery: http://mesos.apache.org/documentation/latest/agent-recovery/
"""

import asyncio
import base64
import contextlib
import copy
import decimal
import io
import ipaddress
import json
import logging
import math
import os.path
import random
import re
import socket
import ssl
import time
import typing
import urllib
from abc import ABC, abstractmethod
from collections import deque, namedtuple
from contextlib import AsyncExitStack
from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum

# Note: don't include Dict here, because it conflicts with addict.Dict.
from typing import AsyncContextManager, ClassVar, List, Mapping, Optional, Tuple, Type, Union

import aiohttp.web
import docker
import importlib_resources
import jsonschema
import networkx
import pymesos
import www_authenticate
from addict import Dict
from decorator import decorator
from katsdptelstate.endpoint import Endpoint

from . import schemas
from .defaults import LOCALHOST

#: Mesos task states that indicate that the task is dead
#: (see https://github.com/apache/mesos/blob/1.0.1/include/mesos/mesos.proto#L1374)
TERMINAL_STATUSES = frozenset(
    ["TASK_FINISHED", "TASK_FAILED", "TASK_KILLED", "TASK_LOST", "TASK_ERROR"]
)
# Names for standard edge attributes, to give some protection against typos
DEPENDS_READY = "depends_ready"
DEPENDS_RESOURCES = "depends_resources"
DEPENDS_RESOLVE = "depends_resolve"
DEPENDS_KILL = "depends_kill"
DEPENDS_FINISHED = "depends_finished"  # for batch tasks
DEPENDS_FINISHED_CRITICAL = "depends_finished_critical"
DECIMAL_CONTEXT = decimal.Context(
    traps=[
        decimal.Overflow,
        decimal.InvalidOperation,
        decimal.DivisionByZero,  # defaults
        decimal.Inexact,
        decimal.FloatOperation,
    ]
)
DECIMAL_CAST_CONTEXT = decimal.Context()
DECIMAL_ZERO = Decimal("0.000")
logger = logging.getLogger(__name__)


Volume = namedtuple("Volume", ["name", "host_path", "numa_node"])
Volume.__doc__ = """Abstraction of a host path offered by an agent.

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


async def poll_ports(host, ports):
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

    Raises
    ------
    OSError
        on any socket operation errors other than the connect
    """
    # protect against temporary name resolution failure.
    # in the case of permanent DNS failure this will block
    # indefinitely and higher level timeouts will be needed
    loop = asyncio.get_event_loop()
    while True:
        try:
            addrs = await loop.getaddrinfo(
                host=host,
                port=None,
                type=socket.SOCK_STREAM,
                proto=socket.IPPROTO_TCP,
                flags=socket.AI_ADDRCONFIG | socket.AI_V4MAPPED,
            )
        except socket.gaierror as error:
            logger.error(
                "Failure to resolve address for %s (%s). Waiting 5s to retry.", host, error
            )
            await asyncio.sleep(5)
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
                    logger.debug("Port %d on %s not ready: %s", port, host, error)
                    await asyncio.sleep(1)
                else:
                    break
        logger.debug("Port %d on %s ready", port, host)


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

    ZERO: ClassVar[Union[int, Decimal]] = 0
    REQUEST_CLASS = ResourceRequest

    def __init__(self, name):
        self.name = name
        self.parts = []
        self.available = self.ZERO

    def add(self, resource):
        """Add a Mesos resource message to the internal resource list"""
        if resource.name != self.name:
            raise ValueError(f"Name mismatch {self.name} != {resource.name}")
        # Keeps the most specifically reserved resources at the end
        # so that they're used first before unreserved resources.
        pos = 0
        role = resource.get("role", "*")
        while pos < len(self.parts) and len(self.parts[pos].get("role", "*")) < len(role):
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
            available = sum((self._available(part, **kwargs) for part in self.parts), self.ZERO)
        if amount > available:
            raise ValueError(
                f"Requested amount {amount} of {self.name} "
                f"is more than available {available} (kwargs={kwargs})"
            )
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
        out.parts.reverse()  # Put output pieces into same order as input
        return out

    def info(self):
        """Iterate over the messages that should be placed in task info"""
        for part in self.parts:
            yield self._untransform(part)

    def _transform(self, resource):
        """Convert a resource into the internal representation."""
        raise NotImplementedError  # pragma: nocover

    def _untransform(self, resource):
        """Convert a resource from the internal representation."""
        raise NotImplementedError  # pragma: nocover

    def _available(self, resource, **kwargs):
        """Amount of resource available in a (transformed) part.

        Subclasses may accept kwargs to specify additional constraints
        on the manner of the allocation.
        """
        raise NotImplementedError  # pragma: nocover

    def _value_str(self, resource):
        raise NotImplementedError  # pragma: nocover

    def _allocate(self, resource, amount, **kwargs):
        """Take a piece of a resource.

        Returns the allocated part, and modifies `resource` in place.
        `amount` must be at most the result of
        ``self._available(resource, **kwargs)``.
        """
        raise NotImplementedError  # pragma: nocover

    def __str__(self):
        parts = []
        for part in self.parts:
            key = self.name
            if part.get("role", "*") != "*":
                key += "(" + part["role"] + ")"
            parts.append(key + ":" + self._value_str(part))
        return "; ".join(parts)

    @classmethod
    def empty_request(cls):
        return cls.REQUEST_CLASS()


class ScalarResource(Resource):
    """Resource model for Mesos scalar resources"""

    ZERO = DECIMAL_ZERO
    REQUEST_CLASS = ScalarResourceRequest

    def _transform(self, resource):
        if resource.type != "SCALAR":
            raise TypeError(f"Expected SCALAR resource, got {resource.type}")
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
    """Resource model for Mesos range resources

    Allocation takes an optional keyword argument ``use_random``. If false (the
    default), resources are allocated sequentially (which typically means
    starting from the smallest number of the most specific role). If it is
    true, the items are selected uniformly at random from the last (most
    specific) role until it is exhausted before moving on to the next role.
    It can also be an instance of :class:`random.Random` to use a specific
    random generator.
    """

    REQUEST_CLASS = RangeResourceRequest

    def _transform(self, resource):
        if resource.type != "RANGES":
            raise TypeError(f"Expected RANGES resource, got {resource.type}")
        resource = copy.deepcopy(resource)
        # Ensures we take resources from the first range first
        resource.ranges.range.reverse()
        return resource

    def _untransform(self, resource):
        return self._transform(resource)

    def _available(self, resource, *, use_random=False):
        total = 0
        for r in resource.ranges.range:
            total += r.end - r.begin + 1
        return total

    def _value_str(self, resource):
        return "[" + ",".join(f"{r.begin}-{r.end}" for r in resource.ranges.range) + "]"

    def _allocate(self, resource, amount, *, use_random=False):
        out = copy.deepcopy(resource)
        out.ranges.range.clear()
        if not use_random:
            pos = len(resource.ranges.range) - 1
            while amount > 0:
                r = resource.ranges.range[pos]
                use = min(amount, r.end - r.begin + 1)
                # TODO: use_random
                out.ranges.range.append(Dict({"begin": r.begin, "end": r.begin + use - 1}))
                r.begin += use
                if r.begin > r.end:
                    del resource.ranges.range[pos]
                amount -= use
                pos -= 1
            # Put into transformed form
            out.ranges.range.reverse()
        else:
            items = []
            if use_random is True:
                use_random = random
            for i in range(amount):
                # Determine the size of each range so that we can pick a random
                # range and have each item be equally likely.
                weights = [r.end - r.begin + 1 for r in resource.ranges.range]
                (pos,) = use_random.choices(range(len(resource.ranges.range)), weights)
                # Pick random element of the range
                r = resource.ranges.range[pos]
                item = use_random.randint(r.begin, r.end)
                items.append(item)
                # Update the range to remove the item
                if r.begin == r.end:
                    del resource.ranges.range[pos]
                elif item == r.begin:
                    r.begin += 1
                elif item == r.end:
                    r.end -= 1
                else:
                    resource.ranges.range.insert(pos, Dict({"begin": item + 1, "end": r.end}))
                    r.end = item - 1
            items.sort(reverse=True)
            for item in items:
                out.ranges.range.append(Dict({"begin": item, "end": item}))
        return out

    def _subset_part(self, part, group):
        out = copy.deepcopy(part)
        out.ranges.range.clear()
        for r in part.ranges.range:
            for i in range(r.end, r.begin - 1, -1):
                if i in group:
                    out.ranges.range.append(Dict({"begin": i, "end": i}))
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

    RESOURCE_REQUESTS: Mapping[str, Type[Resource]] = {}

    def __init__(self):
        self.requests = {name: cls.empty_request() for name, cls in self.RESOURCE_REQUESTS.items()}

    def format_requests(self):
        return "".join(
            f" {name}={request.amount}" for name, request in self.requests.items() if request.amount
        )

    def __repr__(self):
        return f"<{self.__class__.__name__}{self.format_requests()}>"


GLOBAL_RESOURCES = {
    "cpus": ScalarResource,
    "mem": ScalarResource,
    "disk": ScalarResource,
    "ports": RangeResource,
    "cores": RangeResource,
}
GPU_RESOURCES = {"compute": ScalarResource, "mem": ScalarResource}
INTERFACE_RESOURCES = {"bandwidth_in": ScalarResource, "bandwidth_out": ScalarResource}


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
    min_compute_capability : Tuple[int, int], optional
        If specified, the minimum CUDA compute capability
    """

    RESOURCE_REQUESTS = GPU_RESOURCES

    def __init__(self):
        super().__init__()
        self.affinity = False
        self.name = None
        self.min_compute_capability = None

    def matches(self, agent_gpu, numa_node):
        if self.name is not None and self.name != agent_gpu.name:
            return False
        if (
            self.min_compute_capability is not None
            and agent_gpu.compute_capability < self.min_compute_capability
        ):
            return False
        return numa_node is None or not self.affinity or agent_gpu.numa_node == numa_node

    def __repr__(self):
        return "<{} {}{}{}>".format(
            self.__class__.__name__,
            self.name if self.name is not None else "*",
            self.format_requests(),
            " affinity=True" if self.affinity else "",
        )


class InterfaceRequest(ResourceRequestsContainer):
    """Request for resources on a network interface.

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
    multicast_in : a set of abstract names of multicast groups that will be
        subscribed to on this interface. The names are arbitrary hashable
        values.
    multicast_out : like `multicast_in`, but groups to which data will be
        sent.
    """

    RESOURCE_REQUESTS = INTERFACE_RESOURCES

    def __init__(
        self,
        network,
        infiniband=False,
        affinity=False,
        multicast_in=frozenset(),
        multicast_out=frozenset(),
    ):
        super().__init__()
        self.network = network
        self.infiniband = infiniband
        self.affinity = affinity
        self.multicast_in = set(multicast_in)
        self.multicast_out = set(multicast_out)

    def matches(self, interface, numa_node):
        if self.affinity and numa_node is not None and interface.numa_node != numa_node:
            return False
        if self.infiniband and not interface.infiniband_devices:
            return False
        if not interface.infiniband_multicast_loopback:
            new_out = interface.infiniband_multicast_out
            if self.infiniband:
                new_out = new_out | self.multicast_out
            new_in = interface.multicast_in | self.multicast_in
            if new_out & new_in:
                # Transmitted data sent with ibverbs won't be received on the same interface
                return False
        return self.network in interface.networks

    def __repr__(self):
        return "<{} {}{}{}{}>".format(
            self.__class__.__name__,
            self.network,
            self.format_requests(),
            " infiniband=True" if self.infiniband else "",
            " affinity=True" if self.affinity else "",
        )


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
        return "<{} {} {}{}>".format(
            self.__class__.__name__, self.name, self.mode, " affinity=True" if self.affinity else ""
        )


class GPUResources:
    """Collection of specific resources allocated from a single agent GPU.

    Attributes
    ----------
    index : int
        Index into the agent's list of GPUs
    """

    def __init__(self, index):
        self.index = index
        prefix = f"katsdpcontroller.gpu.{index}."
        self.resources = {name: cls(prefix + name) for name, cls in GPU_RESOURCES.items()}


class InterfaceResources:
    """Collection of specific resources for a single agent network interface.

    Attributes
    ----------
    index : int
        Index into the agent's list of interfaces
    infiniband_multicast_out, multicast_in
        See :class:`AgentInterface`
    """

    def __init__(self, index):
        self.index = index
        prefix = f"katsdpcontroller.interface.{index}."
        self.resources = {name: cls(prefix + name) for name, cls in INTERFACE_RESOURCES.items()}
        self.infiniband_multicast_out = set()
        self.multicast_in = set()


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


def _strip_scheme(image: str) -> str:
    """Remove optional http:// or https:// prefix.

    Docker doesn't like these on image paths.
    """
    return re.sub(r"^https?://", "", image)


@dataclass
class Image:
    """Information about a (fully resolved) container image."""

    registry: str
    repo: str
    tag: Optional[str] = None
    digest: Optional[str] = None
    labels: typing.Dict[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.tag is None and self.digest is None:
            raise TypeError("at least one of tag and digest must be specified")

    @classmethod
    def from_path(cls: Type["Image"], path: str) -> "Image":
        """Construct an Image from a path.

        This is not particularly robust, and does not infer a registry from an
        incomplete path. It is intended only for use where backwards
        compatibility is required. In most cases, ImageLookup subclasses are a
        better choice.
        """
        parts = path.split("/", 1)
        if len(parts) == 1:
            registry = "unknown"
            repo = path
        else:
            registry, repo = parts
        tag = None
        digest = None
        if "@" in repo:
            repo, digest = repo.split("@", 1)
        elif ":" in repo:
            repo, tag = repo.split(":", 1)
        else:
            tag = "latest"
        return cls(registry=registry, repo=repo, tag=tag, digest=digest)

    @property
    def path(self) -> str:
        """Get image path that can be passed to Docker."""
        if self.digest is not None:
            result = f"{self.registry}/{self.repo}@{self.digest}"
        else:
            result = f"{self.registry}/{self.repo}:{self.tag}"
        # Docker doesn't like leading http:// or https://
        return re.sub(r"^https?://", "", result)

    @property
    def source(self) -> Optional[str]:
        """Version control source for the image, if available."""
        for key in ["org.opencontainers.image.source", "org.label-schema.vcs-url"]:
            if key in self.labels:
                return self.labels[key]
        return None

    @property
    def revision(self) -> Optional[str]:
        """Version control revision for the image, if available."""
        for key in ["org.opencontainers.image.revision", "org.label-schema.vcs-ref"]:
            if key in self.labels:
                return self.labels[key]
        return None


class ImageLookup(ABC):
    """Abstract base class to get a full image name from a repo and tag."""

    @abstractmethod
    async def __call__(self, repo: str, tag: str) -> Image:
        pass  # pragma: nocover


class _RegistryImageLookup(ImageLookup):
    """Utility class for ImageLookup implementations that store a default private registry."""

    def __init__(self, private_registry: str) -> None:
        self._private_registry = private_registry

    def _split_repo_name(self, name: str) -> Tuple[str, str]:
        """Like docker.auth.split_repo_name, but defaults to the private registry."""
        # This implementation is an adapted version of docker.auth.split_repo_name.
        parts = name.split("/", 1)
        if len(parts) == 1 or (
            "." not in parts[0] and ":" not in parts[0] and parts[0] != "localhost"
        ):
            registry = self._private_registry
            repo = name
        else:
            registry, repo = parts
        return registry, repo


class SimpleImageLookup(_RegistryImageLookup):
    """Resolver that simply concatenates registry, repo and tag."""

    async def __call__(self, repo: str, tag: str) -> Image:
        registry, repo = self._split_repo_name(repo)
        return Image(registry=registry, repo=repo, tag=tag)


class HTTPImageLookup(_RegistryImageLookup):
    """Resolve digests from tags by directly contacting registry."""

    def __init__(self, private_registry: str) -> None:
        super().__init__(private_registry)
        self._authconfig = docker.auth.load_config()

    @staticmethod
    async def _get_image(
        session: aiohttp.ClientSession,
        registry: str,
        repo: str,
        tag: str,
        ssl_context: Optional[ssl.SSLContext],
        auth_header: Optional[str],
    ) -> Image:
        """Make a single attempt to get the image information.

        This fails if there is an authorization error, leaving the caller to
        obtain a token and try again.
        """
        if "://" not in registry:
            # If no scheme is specified, assume https
            registry = "https://" + registry
        manifest_url = f"{registry}/v2/{repo}/manifests/{tag}"
        headers = {aiohttp.hdrs.ACCEPT: "application/vnd.docker.distribution.manifest.v2+json"}
        if auth_header:
            headers[aiohttp.hdrs.AUTHORIZATION] = auth_header
        try:
            # Use a lowish timeout, so that we don't wedge the entire launch if
            # there is a connection problem.
            async with session.get(
                manifest_url, timeout=15, ssl_context=ssl_context, headers=headers
            ) as response:
                response.raise_for_status()
                digest = response.headers["Docker-Content-Digest"]
                manifest_data = await response.json(content_type=None)
        except (aiohttp.client.ClientError, asyncio.TimeoutError) as error:
            raise ImageError(f"Failed to get digest from {manifest_url}: {error}") from error
        except KeyError:
            raise ImageError(f"Docker-Content-Digest header not found for {manifest_url}")
        except ValueError:
            raise ImageError(f"Invalid manifest for {manifest_url}")

        try:
            content_type = manifest_data["config"]["mediaType"]
            if content_type != "application/vnd.docker.container.image.v1+json":
                raise ImageError(f"Unknown mediaType {content_type!r} in {manifest_url}")
            image_blob = manifest_data["config"]["digest"]
        except (KeyError, TypeError):
            raise ImageError(f"Could not find image blob in {manifest_url}")

        image_url = f"{registry}/v2/{repo}/blobs/{image_blob}"
        headers[aiohttp.hdrs.ACCEPT] = content_type
        try:
            async with session.get(
                image_url, timeout=15, ssl_context=ssl_context, headers=headers
            ) as response:
                response.raise_for_status()
                # Docker registry returns Content-Type of
                # application/octet-stream. Passing content_type=None
                # here suppresses the content-type check.
                response_json = await response.json(content_type=None)
            labels = response_json.get("config", {}).get("Labels", {})
            if not isinstance(labels, dict):
                labels = {}
        except (aiohttp.client.ClientError, asyncio.TimeoutError) as error:
            raise ImageError(f"Failed to get labels from {image_url}: {error}") from error
        return Image(registry=registry, repo=repo, tag=tag, digest=digest, labels=labels)

    async def _get_token(
        self,
        session: aiohttp.ClientSession,
        realm: str,
        service: str,
        scope: str,
        auth: Optional[aiohttp.BasicAuth],
    ) -> str:
        headers = {aiohttp.hdrs.ACCEPT: "application/json"}
        params = {"scope": scope, "service": service, "client_id": "katsdpcontroller"}
        try:
            async with session.get(
                realm, params=params, headers=headers, timeout=15, auth=auth
            ) as resp:
                resp.raise_for_status()
                content = await resp.json()
                token = content["token"]
                # Valid syntax determined by RFC 6750
                if not re.fullmatch("[-A-Za-z0-9._~+/]+=*", token):
                    raise ValueError("Invalid syntax for authentication token")
                return token
        except (aiohttp.ClientError, asyncio.TimeoutError, KeyError, ValueError) as error:
            raise ImageError(f"Failed to get authentication token from {realm}: {error}") from error

    async def __call__(self, repo: str, tag: str) -> Image:
        # TODO: see if it's possible to do some connection pooling
        # here. That probably requires the caller to initiate a
        # Session and close it when done.

        registry, repo = self._split_repo_name(repo)
        authdata = docker.auth.resolve_authconfig(self._authconfig, registry)
        if authdata is None:
            auth = None
        else:
            auth = aiohttp.BasicAuth(authdata["username"], authdata["password"])

        cafile = "/etc/ssl/certs/ca-certificates.crt"
        ssl_context: Optional[ssl.SSLContext]
        if os.path.exists(cafile):
            ssl_context = ssl.create_default_context(cafile=cafile)
        else:
            ssl_context = None
        async with aiohttp.ClientSession() as session:
            try:
                auth_header = auth.encode() if auth else None
                image = await self._get_image(
                    session, registry, repo, tag, ssl_context, auth_header
                )
            except ImageError as error:
                cause = error.__cause__
                # If it's an authorization error, see if we can get a bearer token
                # (see https://docs.docker.com/registry/spec/auth/token/).
                if isinstance(cause, aiohttp.client.ClientResponseError) and cause.status == 401:
                    try:
                        assert cause.headers is not None
                        hdr = cause.headers[aiohttp.hdrs.WWW_AUTHENTICATE]
                        challenge = www_authenticate.parse(hdr)["Bearer"]
                        realm = challenge["realm"]
                        service = challenge["service"]
                        scope = challenge["scope"]
                    except (ValueError, KeyError):
                        raise error from None  # Raise the original error if we can't parse
                    # Note: since we're running images from this registry, we
                    # trust it, and don't bother checking the realm for CSRF.
                    token = await self._get_token(session, realm, service, scope, auth)
                    auth_header = f"Bearer {token}"
                    image = await self._get_image(
                        session, registry, repo, tag, ssl_context, auth_header
                    )
                else:
                    raise
        return image


class ImageResolver:
    """Class to map an abstract Docker image name to a fully-qualified name.
    If no private registry is specified, it looks up names in the `sdp/`
    namespace, otherwise in the private registry. One can also override
    individual entries.

    This wraps an instance of :class:`ImageLookup` to do the actual lookups,
    and handles the generic logic like caching, overrides etc.

    Parameters
    ----------
    lookup : :class:`ImageLookup`
        Low-level image lookup.
    tag_file : str, optional
        If specified, the file will be read to determine the image tag to use.
        It does not affect overrides, to allow them to specify their own tags.
    tag : str, optional
        If specified, `tag_file` is ignored and this tag is used.
    """

    def __init__(
        self, lookup: ImageLookup, tag_file: Optional[str] = None, tag: Optional[str] = None
    ) -> None:
        self._lookup = lookup
        self._tag_file = tag_file
        self._overrides: typing.Dict[str, str] = {}
        self._cache: typing.Dict[str, Image] = {}
        if tag is not None:
            self._tag = tag
            self._tag_file = None
        elif self._tag_file is None:
            self._tag = "latest"
        else:
            with open(self._tag_file) as f:
                self._tag = f.read().strip()
                # This is a regex that appeared in older versions of Docker
                # (see https://github.com/docker/docker/pull/8447/files).
                # It's probably a reasonable constraint so that we don't allow
                # whitespace, / and other nonsense, even if Docker itself no
                # longer enforces it.
                if not re.match(r"^[\w][\w.-]{0,127}$", self._tag):
                    raise ValueError(f"Invalid tag {repr(self._tag)} in {self._tag_file}")

    @property
    def tag(self) -> str:
        return self._tag

    @property
    def overrides(self) -> Mapping[str, str]:
        return dict(self._overrides)

    def override(self, name: str, path: str):
        self._overrides[name] = path

    async def __call__(self, name: str) -> Image:
        if name in self._overrides:
            name = self._overrides[name]
        if name in self._cache:
            return self._cache[name]

        # Use split_repo_name to avoid being confused by a :port in the registry part
        if ":" in docker.auth.split_repo_name(name)[1]:
            # A tag was already specified in the graph or the override
            logger.warning("Image %s has a predefined tag, ignoring tag %s", name, self._tag)
            repo, tag = name.rsplit(":", 1)
        else:
            repo = name
            tag = self._tag

        resolved = await self._lookup(repo, tag)
        if name in self._cache:
            # Another asynchronous caller beat us to it. Use the value
            # that caller put in the cache so that calls with the same
            # name always return the same value.
            resolved = self._cache[name]
            logger.debug("ImageResolver race detected resolving %s to %s", name, resolved)
        else:
            logger.debug("ImageResolver resolved %s to %s", name, resolved)
            self._cache[name] = resolved
        return resolved


class ImageResolverFactory:
    """Factory for generating image resolvers. An :class:`ImageResolver`
    caches lookups, so it is useful to be able to generate a new one to
    receive fresh information.

    See :class:`ImageResolver` for an explanation of the constructor
    arguments and :meth:`~ImageResolver.override`.
    """

    def __init__(self, lookup, tag_file=None, tag=None):
        self._args = dict(lookup=lookup, tag_file=tag_file, tag=tag)
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

    _by_prefix: ClassVar[typing.Dict[str, "TaskIDAllocator"]] = {}
    _prefix: str
    _next_id: int

    def __init__(self, prefix=""):
        pass  # Initialised by new

    def __new__(cls, prefix=""):
        # Obtain the singleton
        try:
            return TaskIDAllocator._by_prefix[prefix]
        except KeyError:
            alloc = super().__new__(cls)
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
    """There are insufficient resources to launch a task or tasks.

    This is also used for internal operations when trying to allocate a
    specific task to a specific agent.
    """

    pass


class QueueBusyError(InsufficientResourcesError):
    """The launch group did not reach the front of the queue before its timeout expired."""

    def __init__(self, timeout):
        self.timeout = timeout
        super().__init__(
            f"The launch group timeout ({timeout}s) fired before reaching the front of the queue"
        )


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

    def __init__(self, node, msg=None):
        if msg is None:
            msg = f"Node {node.name} failed with status {node.status.state}"
            if hasattr(node.status, "reason"):
                msg += f"/{node.status.reason}"
            if hasattr(node.status, "message"):
                msg += f" ({node.status.message})"
        super().__init__(msg)
        self.node = node


class TaskSkipped(TaskError):
    """A batch job was skipped because a dependency failed"""

    def __init__(self, node, msg=None):
        if msg is None:
            msg = f"Node {node.name} was skipped because a dependency failed"
        super().__init__(node, msg)


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
        or subclass). It is passed the logical task.
    """

    def __init__(self, name):
        self.name = name
        self.wait_ports = None
        self.physical_factory = PhysicalNode

    def __repr__(self):
        return f"<{self.__class__.__name__} {self.name!r}>"


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
        The strings in the list become keys in :attr:`PhysicalTask.cores`. You
        can use ``None`` if you don't need to use the core numbers in the
        command to pin individual threads.
    numa_nodes : float
        Minimum fraction of a NUMA node's cores to allocate. This should only
        be set if `cores` is, and must not be set higher than 1.0.
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
        - `interfaces` : dictionary mapping requested networks to :class:`AgentInterface` objects
        - `endpoints` : dictionary of remote endpoints. Keys are of the form
          :samp:`{service}_{port}`, and values are :class:`Endpoint` objects.
        - `generation` : number of times task was cloned
        - `resolver` : resolver object
    wrapper : str
        URI for a wrapper around the command. If specified, it will be
        downloaded to the sandbox directory and executed, with the original
        command being passed to it.
    """

    RESOURCE_REQUESTS = GLOBAL_RESOURCES

    # Type annotations for mypy. This are actually provided by the metaclass
    cpus: float
    mem: float
    disk: float
    ports: List[Optional[str]]
    cores: List[Optional[str]]

    def __init__(self, name):
        LogicalNode.__init__(self, name)
        ResourceRequestsContainer.__init__(self)
        self.max_run_time = None
        self.gpus = []
        self.interfaces = []
        self.volumes = []
        self.numa_nodes = 0.0
        self.host = None
        self.subsystem = None
        self.capabilities = []
        self.image = None
        self.command = []
        self.wrapper = None
        self.taskinfo = Dict()
        self.taskinfo.container.type = "DOCKER"
        self.taskinfo.command.shell = False
        self.physical_factory = PhysicalTask

    def valid_agent(self, agent) -> bool:
        """Checks whether the attributes of an agent are suitable for running
        this task. Subclasses may override this to enforce constraints e.g.,
        requiring a special type of hardware."""
        if self.subsystem is not None and self.subsystem not in agent.subsystems:
            return False
        # TODO: enforce x86-64, if we ever introduce ARM or other hardware
        if self.host is not None and agent.host != self.host:
            return False
        return True

    def __repr__(self):
        s = io.StringIO()
        s.write(f"<{self.__class__.__name__} {self.name!r}{self.format_requests()}")
        if self.gpus:
            s.write(f" gpus={self.gpus}")
        if self.interfaces:
            s.write(f" interfaces={self.interfaces}")
        if self.volumes:
            s.write(f" volumes={self.volumes}")
        if self.host:
            s.write(f" host={self.host!r}")
        s.write(">")
        return s.getvalue()


class AgentGPU(GPUResources):
    """A single GPU on an agent machine, tracking both attributes and free resources."""

    def __init__(self, spec, index):
        super().__init__(index)
        self.uuid = spec.get("uuid")
        self.name = spec["name"]
        self.compute_capability = tuple(spec["compute_capability"])
        self.device_attributes = spec["device_attributes"]
        self.numa_node = spec.get("numa_node")


class AgentInterface(InterfaceResources):
    """A single interface on an agent machine, tracking both attributes and free resources.

    Attributes
    ----------
    name : str
        Kernel interface name
    networks : Set[str]
        Logical names for the networks to which the interface is attached
    ipv4_address : :class:`ipaddress.IPv4Address`
        IPv4 local address of the interface
    numa_node : int, optional
        Index of the NUMA socket to which the NIC is connected
    infiniband_devices : list of str
        Device inodes that should be passed into Docker containers to use Infiniband libraries
    infiniband_multicast_loopback : bool
        If false, multicast data sent using ibverbs will not be received on the same interface
    infiniband_multicast_out : set
        Abstract names of multicast groups for which this interface sends data using ibverbs
    multicast_in : set
        Abstract names of multicast groups for which this interfaces receives data
    resources : list of :class:`Resource`
        Available resources
    """

    def __init__(self, spec, index):
        super().__init__(index)
        self.name = spec["name"]
        # For compatibility, spec['network'] is either a string or a list of strings
        networks = spec["network"]
        self.networks = {networks} if isinstance(networks, str) else set(networks)
        self.ipv4_address = ipaddress.IPv4Address(spec["ipv4_address"])
        self.numa_node = spec.get("numa_node")
        self.infiniband_devices = spec.get("infiniband_devices", [])
        # Default to True for backwards compatibility with nodes that don't
        # advertise the setting (where the loopback has always been enabled).
        self.infiniband_multicast_loopback = spec.get("infiniband_multicast_loopback", True)
        self.infiniband_multicast_out = set()
        self.multicast_in = set()


def decode_json_base64(value):
    """Decodes a object that has been encoded with JSON then url-safe base64."""
    json_bytes = base64.urlsafe_b64decode(value)
    return json.loads(json_bytes.decode("utf-8"))


class _Everything:
    """Acts like a set that contains any possible element."""

    def __contains__(self, x) -> bool:
        return True


class Agent:
    """Collects multiple offers for a single Mesos agent and role and allows
    :class:`ResourceAllocation`s to be made from it.

    Parameters
    ----------
    offers : list
        List of Mesos offer dicts
    """

    # Internally used random generator for port assignment. It's used instead
    # of the default one to make it easier to mock out.
    _random = random.Random()

    def __init__(self, offers):
        if not offers:
            raise ValueError("At least one offer must be specified")
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
        self.subsystems = _Everything()
        self.priority = None
        for attribute in offers[0].attributes:
            try:
                if attribute.name == "katsdpcontroller.interfaces" and attribute.type == "TEXT":
                    value = decode_json_base64(attribute.text.value)
                    schemas.INTERFACES.validate(value)
                    self.interfaces = [AgentInterface(item, i) for i, item in enumerate(value)]
                elif attribute.name == "katsdpcontroller.volumes" and attribute.type == "TEXT":
                    value = decode_json_base64(attribute.text.value)
                    schemas.VOLUMES.validate(value)
                    volumes = []
                    for item in value:
                        volumes.append(
                            Volume(
                                name=item["name"],
                                host_path=item["host_path"],
                                numa_node=item.get("numa_node"),
                            )
                        )
                    self.volumes = volumes
                elif attribute.name == "katsdpcontroller.gpus" and attribute.type == "TEXT":
                    value = decode_json_base64(attribute.text.value)
                    schemas.GPUS.validate(value)
                    self.gpus = [AgentGPU(item, i) for i, item in enumerate(value)]
                elif attribute.name == "katsdpcontroller.numa" and attribute.type == "TEXT":
                    value = decode_json_base64(attribute.text.value)
                    schemas.NUMA.validate(value)
                    self.numa = value
                elif (
                    attribute.name == "katsdpcontroller.infiniband_devices"
                    and attribute.type == "TEXT"
                ):
                    value = decode_json_base64(attribute.text.value)
                    schemas.INFINIBAND_DEVICES.validate(value)
                    self.infiniband_devices = value
                elif attribute.name == "katsdpcontroller.priority" and attribute.type == "SCALAR":
                    self.priority = attribute.scalar.value
                elif attribute.name == "katsdpcontroller.subsystems" and attribute.type == "TEXT":
                    value = decode_json_base64(attribute.text.value)
                    schemas.SUBSYSTEMS.validate(value)
                    self.subsystems = set(value)
            except (ValueError, KeyError, TypeError, ipaddress.AddressValueError):
                logger.warning("Could not parse %s (%s)", attribute.name, attribute.text.value)
                logger.debug("Exception", exc_info=True)
            except jsonschema.ValidationError as e:
                logger.warning("Validation error parsing %s: %s", value, e)

        # These resources all represent resources not yet allocated
        self.resources = {name: cls(name) for name, cls in GLOBAL_RESOURCES.items()}
        for offer in offers:
            for resource in offer.resources:
                # Skip specialised resource types and use only general-purpose
                # resources.
                if "disk" in resource and "source" in resource.disk:
                    continue
                if resource.name in self.resources:
                    self.resources[resource.name].add(resource)
                elif resource.name.startswith("katsdpcontroller.gpu."):
                    parts = resource.name.split(".", 3)
                    # TODO: catch exceptions here
                    index = int(parts[2])
                    resource_name = parts[3]
                    if resource_name in self.gpus[index].resources:
                        self.gpus[index].resources[resource_name].add(resource)
                elif resource.name.startswith("katsdpcontroller.interface."):
                    parts = resource.name.split(".", 3)
                    # TODO: catch exceptions here
                    index = int(parts[2])
                    resource_name = parts[3]
                    if resource_name in self.interfaces[index].resources:
                        self.interfaces[index].resources[resource_name].add(resource)
        if self.priority is None:
            self.priority = float(len(self.gpus) + len(self.interfaces) + len(self.volumes))
        # Split offers of cores by NUMA node
        self.numa_cores = [self.resources["cores"].subset(numa_node) for numa_node in self.numa]
        del self.resources["cores"]  # Prevent accidentally allocating from this
        logger.debug("Agent %s has priority %f", self.agent_id, self.priority)

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
                    continue  # Already been used for a request in this task
                if not request.matches(item, numa_node):
                    continue
                good = True
                for name in request.requests:
                    need = request.requests[name].amount
                    have = item.resources[name].available
                    if have < need:
                        logger.debug("Not enough %s on %s %d for request %d", name, msg, j, i)
                        good = False
                        break
                if good:
                    use = j
                    break
            if use is None:
                raise InsufficientResourcesError(f"No suitable {msg} found for request {i}")
            assign[use] = i
        return assign

    def _allocate_numa_node(self, numa_node, logical_task):
        # Check that there are sufficient cores on this node
        if numa_node is not None:
            cores = self.numa_cores[numa_node]
            total_cores = len(self.numa[numa_node])
        else:
            cores = RangeResource("cores")
            total_cores = 0
        need = max(
            logical_task.requests["cores"].amount, math.ceil(logical_task.numa_nodes * total_cores)
        )
        have = cores.available
        if need > have:
            raise InsufficientResourcesError(
                f"not enough cores on node {numa_node} ({have} < {need})"
            )

        # Match network requests to interfaces
        interface_map = self._match_children(
            numa_node, logical_task.interfaces, self.interfaces, "interface"
        )
        # Match volume requests to volumes
        for request in logical_task.volumes:
            if not any(request.matches(volume, numa_node) for volume in self.volumes):
                if not any(request.matches(volume, None) for volume in self.volumes):
                    raise InsufficientResourcesError(f"Volume {request.name} not present")
                else:
                    raise InsufficientResourcesError(
                        f"Volume {request.name} not present on NUMA node {numa_node}"
                    )
        # Match GPU requests to GPUs
        gpu_map = self._match_children(numa_node, logical_task.gpus, self.gpus, "GPU")

        # Have now verified that the task fits. Create the resources for it
        alloc = ResourceAllocation(self)
        for name, request in logical_task.requests.items():
            if name == "cores":
                res = cores.allocate(need)
            elif name == "ports":
                res = self.resources[name].allocate(request.amount, use_random=self._random)
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
                if request.infiniband:
                    interface.infiniband_multicast_out |= request.multicast_out
                    interface_alloc.infiniband_multicast_out |= request.multicast_out
                interface.multicast_in |= request.multicast_in
                interface_alloc.multicast_in |= request.multicast_in
                alloc.interfaces[idx] = interface_alloc
        for request in logical_task.volumes:
            alloc.volumes.append(
                next(volume for volume in self.volumes if request.matches(volume, numa_node))
            )
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
            raise InsufficientResourcesError("Task does not match this agent")
        for name, request in logical_task.requests.items():
            if name == "cores":
                continue  # Handled specially lower down
            need = request.amount
            have = self.resources[name].available
            if have < need:
                raise InsufficientResourcesError(f"Not enough {name} ({have} < {need})")

        if logical_task.requests["cores"].amount:
            # For tasks requesting cores we activate NUMA awareness
            for numa_node in range(len(self.numa)):
                try:
                    return self._allocate_numa_node(numa_node, logical_task)
                except InsufficientResourcesError:
                    logger.debug(
                        "Failed to allocate NUMA node %d on %s",
                        numa_node,
                        self.agent_id,
                        exc_info=True,
                    )
            raise InsufficientResourcesError("No suitable NUMA node found")
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

    NOT_READY = 0  #: We have not yet started it
    STARTING = 1  #: We have been asked to start, but have not yet asked Mesos
    STARTED = 2  #: We have asked Mesos to start it, but it is not yet running
    RUNNING = 3  #: Process is running, but we're waiting for ports to open
    READY = 4  #: Node is completely ready
    KILLING = 5  #: We have asked the task to kill itself, but do not yet have confirmation
    DEAD = 6  #: Have received terminal status message


class PhysicalNode:
    """Base class for physical nodes.

    Parameters
    ----------
    logical_node : :class:`LogicalNode`
        The logical node from which this physical node is constructed

    Attributes
    ----------
    logical_node : :class:`LogicalNode`
        The logical node passed to the constructor
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
    was_ready : bool
        Set to true once the task enters state :class:`~TaskState.READY`. This
        can be used to distinguish a task that become ready and then died from
        one that died without ever becoming ready.
    generation : int
        Number of times that :meth:`clone` was used to obtain this node.
    _ready_waiter : :class:`asyncio.Task`
        Task which asynchronously waits for the to be ready (e.g. for ports to
        be open). It is started on reaching :class:`~TaskState.RUNNING`.
    """

    ports: typing.Dict[str, int]

    def __init__(self, logical_node):
        self.logical_node = logical_node
        self.name = logical_node.name
        # In PhysicalTask it is a property and cannot be set
        try:
            self.host = None
        except AttributeError:
            pass
        self.ports = {}
        self.state = TaskState.NOT_READY
        self.ready_event = asyncio.Event()
        self.dead_event = asyncio.Event()
        self.depends_ready = []
        self.death_expected = False
        self.was_ready = False
        self._ready_waiter = None
        self.generation = 0

    async def resolve(self, resolver, graph, image=None):
        """Make final preparations immediately before starting.

        Parameters
        ----------
        resolver : :class:`Resolver`
            Resolver for images etc.
        graph : :class:`networkx.MultiDiGraph`
            Physical graph containing the task
        image : :class:`Image`, optional
            Full path to image to use, bypassing the `resolver`
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

        Returns true if the task is now ready or false if a dependency failed
        and the task is now expected to die. Subclasses must do something to
        ensure that the task dies.
        """
        for dep in self.depends_ready:
            await dep.ready_event.wait()
            if not dep.was_ready:
                logger.debug("Dependency %s of %s failed", dep.name, self.name)
                return False
        if self.logical_node.wait_ports is not None:
            wait_ports = [self.ports[port] for port in self.logical_node.wait_ports]
        else:
            wait_ports = list(self.ports.values())
        if wait_ports:
            await poll_ports(self.host, wait_ports)
        return True

    def _ready_callback(self, future):
        """This callback is called when the waiter is either finished or
        cancelled. If it is finished, we would normally not have
        advanced beyond READY, because set_state will cancel the
        waiter if we do. However, there is a race condition if
        set_state is called between the waiter completing and the
        call to this callback."""
        self._ready_waiter = None
        try:
            success = future.result()  # throw the exception, if any
            if self.state < TaskState.READY:
                if success:
                    self.set_state(TaskState.READY)
                else:
                    self.dependency_abort()
                    self.set_state(TaskState.KILLING)
        except asyncio.CancelledError:
            pass
        except Exception:
            logger.exception("exception while waiting for task %s to be ready", self.name)

    def set_state(self, state):
        """Update :attr:`state`.

        This checks for invalid transitions, and sets the events when
        appropriate.

        Subclasses may override this to take special actions on particular
        transitions, but should chain to the base class.
        """
        logger.debug("Task %s: %s -> %s", self.name, self.state, state)
        if state < self.state:
            # STARTING -> NOT_READY is permitted, for the case where a
            # launch is cancelled before the tasks are actually launched.
            if state != TaskState.NOT_READY or self.state != TaskState.STARTING:
                logger.warning(
                    "Ignoring state change that went backwards (%s -> %s) on task %s",
                    self.state,
                    state,
                    self.name,
                )
                return
        self.state = state
        if state == TaskState.DEAD:
            self.dead_event.set()
            self.ready_event.set()
        elif state == TaskState.READY:
            self.was_ready = True
            self.ready_event.set()
        elif state == TaskState.RUNNING and self._ready_waiter is None:
            self._ready_waiter = asyncio.ensure_future(self.wait_ready())
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

    def dependency_abort(self):
        """Arrange for a task to die due to a failed ``depends_ready``.

        This will only be called in state :const:`TaskState.RUNNING`.
        This function should not manipulate the task state; the caller will
        set the state to :const:`TaskState.KILLING`.
        """
        self.death_expected = True

    def clone(self):
        """Create a duplicate of the task, ready to be run.

        The duplicate is in state :const:`TaskState.NOT_READY` and is
        unresolved.
        """
        clone = self.logical_node.physical_factory(self.logical_node)
        clone.generation = self.generation + 1
        return clone


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
        Timestamp at which status became TASK_RUNNING (or ``None`` if it hasn't)
    end_time : float
        Timestamp at which status became TASK_DEAD (or ``None`` if it hasn't)
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
    task_stats : :class:`TaskStats`
        Statistics collector of the scheduler. It is only set when
        the task is being launched.
    """

    cores: typing.Dict[str, int]

    def __init__(self, logical_task: LogicalTask) -> None:
        super().__init__(logical_task)
        self.interfaces: typing.Dict[str, AgentInterface] = {}
        self.endpoints: typing.Dict[str, Endpoint] = {}
        self.taskinfo = None
        self.image: Optional[Image] = None
        self.allocation: Optional[Dict] = None
        self.status = None
        self.start_time = None  # time.time()
        self.end_time = None  # time.time()
        self.kill_sent_time = None  # loop.time() when killTask called
        self._queue: Optional["LaunchQueue"] = None
        self.task_stats = None
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

    async def resolve(self, resolver, graph, image=None):
        """Do final preparation before moving to :const:`TaskState.STARTING`.
        At this point all dependencies are guaranteed to have resources allocated.

        Parameters
        ----------
        resolver : :class:`Resolver`
            Resolver to allocate resources like task IDs
        graph : :class:`networkx.MultiDiGraph`
            Physical graph
        image : :class:`Image`, optional
            Full path to image to use, bypassing the `resolver`
        """
        await super().resolve(resolver, graph)
        for _src, trg, attr in graph.out_edges([self], data=True):
            if "port" in attr:
                port = attr["port"]
                endpoint_name = f"{trg.logical_node.name}_{port}"
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
            archive_exts = [
                ".tar",
                ".tgz",
                ".tar.gz",
                ".tbz2",
                ".tar.bz2",
                ".txz",
                ".tar.xz",
                ".zip",
            ]
            if not any(self.logical_node.wrapper.endswith(ext) for ext in archive_exts):
                uri.output_file = "wrapper"
                uri.executable = True
            taskinfo.command.setdefault("uris", []).append(uri)
            command.insert(0, "/mnt/mesos/sandbox/wrapper")
        if self.depends_ready:
            uri = Dict()
            uri.value = urllib.parse.urljoin(resolver.http_url, "static/delay_run.sh")
            uri.executable = True
            taskinfo.command.setdefault("uris", []).append(uri)
            command = [
                "/mnt/mesos/sandbox/delay_run.sh",
                urllib.parse.urljoin(
                    resolver.http_url, f"tasks/{taskinfo.task_id.value}/wait_start"
                ),
            ] + command
        if command:
            taskinfo.command.value = command[0]
            taskinfo.command.arguments = command[1:]
        if image is None:
            image = await resolver.image_resolver(self.logical_node.image)
        taskinfo.container.docker.image = image.path
        taskinfo.agent_id.value = self.agent_id
        taskinfo.resources = []
        for resource in self.allocation.resources.values():
            taskinfo.resources.extend(resource.info())
        if self.logical_node.max_run_time is not None:
            taskinfo.max_completion_time.nanoseconds = int(self.logical_node.max_run_time * 1e9)

        if self.allocation.resources["cores"]:
            core_list = ",".join(str(core) for core in self.allocation.resources["cores"])
            docker_parameters.append({"key": "cpuset-cpus", "value": core_list})

        any_infiniband = False
        for request, interface_alloc in zip(
            self.logical_node.interfaces, self.allocation.interfaces
        ):
            for resource in interface_alloc.resources.values():
                taskinfo.resources.extend(resource.info())
            if request.infiniband:
                any_infiniband = True
        if any_infiniband:
            # ibverbs uses memory mapping for DMA. Take away the default rlimit
            # maximum since Docker tends to set a very low limit.
            docker_parameters.append({"key": "ulimit", "value": "memlock=-1"})
            # rdma_get_devices requires *all* the devices to be present to
            # succeed, even if they're not all used.
            if self.agent.infiniband_devices:
                docker_devices.update(self.agent.infiniband_devices)
            else:
                # Fallback for machines that haven't been updated with the
                # latest agent_mkconfig.py.
                for interface in self.agent.interfaces:
                    docker_devices.update(interface.infiniband_devices)

        # UUIDs for GPUs to be handled by nvidia-container-runtime
        gpu_uuids = []
        for gpu_alloc in self.allocation.gpus:
            for resource in gpu_alloc.resources.values():
                taskinfo.resources.extend(resource.info())
            gpu = self.agent.gpus[gpu_alloc.index]
            gpu_uuids.append(gpu.uuid)
        if gpu_uuids:
            # TODO: once we've upgraded to Docker 19.03 everywhere we can use its
            # built-in GPU support.
            docker_parameters.append({"key": "runtime", "value": "nvidia"})
            env = taskinfo.command.environment.setdefault("variables", [])
            env.append({"name": "NVIDIA_VISIBLE_DEVICES", "value": ",".join(gpu_uuids)})

        # container.linux_info.capabilities doesn't work with Docker
        # containerizer (https://issues.apache.org/jira/browse/MESOS-6163), so
        # pass in the parameters.
        for capability in self.logical_node.capabilities:
            docker_parameters.append({"key": "cap-add", "value": capability})

        for device in docker_devices:
            docker_parameters.append({"key": "device", "value": device})
        if docker_parameters:
            taskinfo.container.docker.setdefault("parameters", []).extend(docker_parameters)

        for rvolume, avolume in zip(self.logical_node.volumes, self.allocation.volumes):
            volume = Dict()
            volume.mode = rvolume.mode
            volume.container_path = rvolume.container_path
            volume.host_path = avolume.host_path
            taskinfo.container.setdefault("volumes", []).append(volume)

        taskinfo.discovery.visibility = "EXTERNAL"
        taskinfo.discovery.name = self.name
        taskinfo.discovery.ports.ports = []
        for port_name, port_number in self.ports.items():
            # TODO: need a way to indicate non-TCP
            taskinfo.discovery.ports.ports.append(
                Dict(number=port_number, name=port_name, protocol="tcp")
            )
        self.taskinfo = taskinfo
        self.image = image

    def subst_args(self, resolver):
        """Returns a dictionary that is passed when formatting the command
        from the logical task.
        """
        args = {}
        for r, cls in GLOBAL_RESOURCES.items():
            if issubclass(cls, RangeResource):
                args[r] = getattr(self, r)
        args["interfaces"] = self.interfaces
        args["endpoints"] = self.endpoints
        args["host"] = self.host
        args["resolver"] = resolver
        args["generation"] = self.generation
        return args

    def set_state(self, state: TaskState) -> None:
        old_state = self.state
        try:
            super().set_state(state)
        finally:
            if self.task_stats is not None and self._queue is not None and self.state != old_state:
                self.task_stats.task_state_changes({self._queue: {old_state: -1, self.state: 1}})

    def set_status(self, status):
        self.status = status
        if status.state == "TASK_RUNNING" and self.start_time is None:
            self.start_time = status.timestamp
        elif status.state in TERMINAL_STATUSES and self.end_time is None:
            self.end_time = status.timestamp
            if self.start_time is None:
                # Can happen if the task failed to launch
                self.start_time = status.timestamp

    def kill(self, driver, **kwargs):
        # The poller is stopped by set_state, so we do not need to do it here.
        driver.killTask(self.taskinfo.task_id)
        # Record the first time we sent the kill, so that if it is lost (which
        # can happen according to Mesos docs), reconciliation can try again.
        self.kill_sent_time = asyncio.get_event_loop().time()
        super().kill(driver, **kwargs)

    def dependency_abort(self):
        super().dependency_abort()
        # The kill is done by asking delay_run.sh to abort rather than a Mesos
        # kill. But if that fails, we want reconciliation to try harder.
        self.kill_sent_time = asyncio.get_event_loop().time()

    @property
    def queue(self) -> Optional["LaunchQueue"]:
        return self._queue

    @queue.setter
    def queue(self, queue: Optional["LaunchQueue"]) -> None:
        old_queue = self._queue
        self._queue = queue
        # Once a task has reached STARTED, the queue only changes due to __del__,
        # and in that case we don't subtract it as we want to count dead tasks
        # even after they're garbage collected.
        if (
            self.task_stats is not None
            and queue is not old_queue
            and self.state < TaskState.STARTED
        ):
            changes = {}
            if old_queue is not None:
                changes[old_queue] = {self.state: -1}
            if queue is not None:
                changes[queue] = {self.state: 1}
            self.task_stats.task_state_changes(changes)

    def __del__(self):
        # hasattr is to protect against failure early in __init__
        if hasattr(self, "_queue"):
            self.queue = None


class FakePhysicalTask(PhysicalNode):
    """Drop-in replacement for PhysicalTask that does not actually launch anything.

    It is intended for use with :option:`!--interface-mode` and for unit testing. It
    has the following behaviour:

    - When set to :const:`TaskState.STARTED`, it moves itself to
      :const:`TaskState.RUNNING` (although not instantly).
    - When killed, it takes care of going to state DEAD.
    - If a `max_run_time` is set, it exits immediately; otherwise it waits
      until it is killed.
    - It allocates ports and listens on them. By default, incoming connections
      are immediately closed.

    .. todo::

       It does not currently handle task_stats, start_time, stop_time or status.
    """

    logical_node: LogicalTask

    def __init__(self, logical_task: LogicalTask) -> None:
        super().__init__(logical_task)
        self.queue = None
        self.host = LOCALHOST
        self._task: Optional[asyncio.Task] = None  # Started when we're asked to run
        self._kill_event = asyncio.Event()  # Signalled when we're asked to shut down
        self._sockets: Dict[str, socket.socket] = {}  # Pre-created sockets

        # Sockets have to be created now rather than in _run because we
        # need to populate ports. For PhysicalTask this is done during
        # `allocate` but we don't have that method.
        for port_name in self.logical_node.ports:
            if port_name is not None:
                sock = self._create_socket(self.host, 0)
                self._sockets[port_name] = sock
                self.ports[port_name] = sock.getsockname()[1]

    def _create_socket(self, host: str, port: int) -> socket.socket:
        # This method is mocked by the unit tests, so do not inline it.
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP)
        sock.bind((host, port))
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, True)
        return sock

    @staticmethod
    async def _connected_cb(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        """Handle a new connection to a port by immediately closing it."""
        writer.close()
        await writer.wait_closed()

    async def _create_server(self, port: str, sock: socket.socket) -> AsyncContextManager:
        """Create a server to service a named port.

        The default implementation will accept connections and immediately
        close them. Subclasses may override this method to provide more
        useful functionality.

        Parameters
        ----------
        port
            Name assigned to the port in the logical task
        sock
            Socket on which the server should listen. If it is not possible to
            start the server on an existing socket, a subclass implementation
            may instead close the socket then listen on the same address.

        Returns
        -------
        server
            Any asynchronous context manager
        """
        return await asyncio.start_server(self._connected_cb, sock=sock)

    async def _run(self) -> None:
        async with AsyncExitStack() as stack:
            for port in self.logical_node.ports:
                if port is not None:
                    server = await self._create_server(port, self._sockets[port])
                    del self._sockets[port]  # The server should now close the socket
                    await stack.enter_async_context(server)
            self.set_state(TaskState.RUNNING)
            # If we have a max_run_time, assume it is a batch task and emulate
            # it terminating immediately. Otherwise emulate a service and wait
            # to be shut down.
            if self.logical_node.max_run_time is None:
                await self._kill_event.wait()

    def _dead_callback(self, task: asyncio.Task) -> None:
        try:
            task.result()  # Evaluate for exceptions
        except Exception:
            logger.exception("Fake task %s died unexpectedly", task.get_name())
        self._task = None
        self.set_state(TaskState.DEAD)

    def set_state(self, state):
        if state == TaskState.KILLING and self._task is None:
            state = TaskState.DEAD  # Nothing to kill, so immediately become dead
        super().set_state(state)
        if self.state >= TaskState.STARTED and self.state <= TaskState.READY and self._task is None:
            self._task = asyncio.get_event_loop().create_task(self._run(), name=self.name)
            self._task.add_done_callback(self._dead_callback)
        if self.state == TaskState.DEAD:
            # Clean up any sockets that weren't absorbed into servers
            for sock in self._sockets.values():
                sock.close()
            self._sockets.clear()

    def kill(self, driver, **kwargs):
        self._kill_event.set()
        super().kill(driver, **kwargs)

    def dependency_abort(self):
        self._kill_event.set()
        super().dependency_abort()


AnyPhysicalTask = Union[PhysicalTask, FakePhysicalTask]


def instantiate(logical_graph):
    """Create a physical graph from a logical one. Each physical node is
    created by calling :attr:`LogicalNode.physical_factory` on the
    corresponding logical node. Edges, and graph, node and edge attributes are
    transferred as-is.

    Parameters
    ----------
    logical_graph : :class:`networkx.MultiDiGraph`
        Logical graph to instantiate
    """
    # Create physical nodes
    mapping = {logical: logical.physical_factory(logical) for logical in logical_graph}
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

    def __init__(self, graph, nodes, resolver, deadline):
        self.nodes = nodes
        self.graph = graph
        self.resolver = resolver
        self.resources_future = asyncio.Future()
        self.future = asyncio.Future()
        self.deadline = deadline
        self.last_insufficient = InsufficientResourcesError("No resource offers received")


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

    def __init__(self, role, name="", *, priority=0):
        self.role = role
        self.name = name
        self.priority = priority
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

    def add(self, group, task_stats):
        self._groups.append(group)
        for node in group.nodes:
            if isinstance(node, PhysicalTask):
                node.task_stats = task_stats
                node.queue = self

    def __iter__(self):
        return (group for group in self._groups if not group.future.cancelled())

    def __repr__(self):
        return "<LaunchQueue '{}' with {} items at {:#x}>".format(
            self.name, len(self._groups), id(self)
        )


@decorator
def run_in_event_loop(func, *args, **kw):
    args[0]._loop.call_soon_threadsafe(func, *args, **kw)


async def wait_start_handler(request):
    scheduler = request.app["katsdpcontroller_scheduler"]
    task_id = request.match_info["id"]
    task, graph = scheduler.get_task(task_id, return_graph=True)
    if task is None:
        raise aiohttp.web.HTTPNotFound(text=f"Task ID {task_id} not active\n")
    else:
        try:
            for dep in task.depends_ready:
                await dep.ready_event.wait()
                if not dep.was_ready:
                    logger.info("Not starting %s because %s died", task.name, dep.name)
                    raise aiohttp.web.HTTPServiceUnavailable(
                        text=f"Dependency {dep.name} died without becoming ready\n"
                    )
        except (asyncio.CancelledError, aiohttp.web.HTTPException):
            raise
        except Exception as error:
            logger.exception("Exception while waiting for dependencies")
            raise aiohttp.web.HTTPInternalServerError(
                text=f"Exception while waiting for dependencies:\n{error}\n"
            )
        else:
            return aiohttp.web.Response(body="")


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
        edge_filter = lambda data: bool(data.get(attr))  # noqa: E731
    nodes = set(nodes)
    out = graph.__class__()
    out.add_nodes_from(nodes)
    for a, b, data in graph.edges(nodes, data=True):
        if b in nodes and edge_filter(data):
            out.add_edge(a, b, **data)
    return out


class TaskStats:
    """Base class for plugging in listeners that keep track of the number of tasks.

    Users should subclass this and override the methods that they are
    interested in receiving.
    """

    def task_state_changes(self, changes):
        """Changes to number of tasks in each state of each queue.

        changes : Mapping[LaunchQueue, Mapping[TaskState`, int]]
            Delta in number of tasks of each queue and state. Queues and states
            with no change will not necessarily be listed.
        """
        pass

    def batch_tasks_created(self, n_tasks):
        """`n_tasks` batch tasks have been created."""
        pass

    def batch_tasks_started(self, n_tasks):
        """`n_tasks` batch tasks have become ready to start."""
        pass

    def batch_tasks_skipped(self, n_tasks):
        """`n_tasks` batch tasks were skipped because a dependency failed."""
        pass

    def batch_tasks_retried(self, n_tasks):
        """`n_tasks` batch tasks failed and were re-scheduled."""
        pass

    def batch_tasks_failed(self, n_tasks):
        """`n_tasks` batch tasks failed after all retries."""
        pass

    def batch_tasks_done(self, n_tasks):
        """`n_tasks` batch tasks completed (including failed or skipped)."""
        pass


async def _cleanup_task(task):
    if not task:
        return
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


class SchedulerBase:
    """Top-level scheduler.

    This base class does not actually connect to Mesos, so can only be used
    to launch nodes that are not derived from :class:`PhysicalTask`. This is
    useful for simulation or testing. For interacting with Mesos, use
    :class:`Scheduler`.

    Parameters
    ----------
    default_role : str
        Mesos role used by the default queue
    http_host : str
        Hostname to bind for the embedded HTTP server (defaults to all interfaces)
    http_port : int
        Port for the embedded HTTP server, or 0 to assign a free one
    http_url : str, optional
        URL at which agent nodes can reach the HTTP server. If not specified,
        tries to deduce it from the host's FQDN.
    task_stats : :class:`TaskStats`, optional
        Set of callbacks for tracking statistics about tasks.
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
    http_host : str
        Actual host binding for the HTTP server (after :meth:`start`)
    http_port : int
        Actual HTTP port used for the HTTP server (after :meth:`start`)
    http_url : str
        Actual external URL to use for the HTTP server (after :meth:`start`)
    http_runner : :class:`aiohttp.web.AppRunner`
        Runner for the HTTP app
    task_stats : :class:`TaskStats`
        Statistics collector passed to constructor
    """

    # If offers come at 5s intervals, then 11s gives two chances.
    resources_timeout = 11.0  #: Time to wait for sufficient resources to be offered
    reconciliation_interval = 30.0  #: Time to wait between reconciliation requests
    kill_timeout = 5.0  #: Minimum time before trying to re-kill a task

    def __init__(
        self,
        default_role,
        http_host,
        http_port,
        http_url=None,
        *,
        task_stats=None,
        runner_kwargs=None,
    ):
        self._loop = asyncio.get_event_loop()
        self._driver = None
        self._offers = {}  #: offers keyed by role then agent ID then offer ID
        #: set when it's time to retry a launch (see _launcher)
        self._wakeup_launcher = asyncio.Event()
        self._default_queue = LaunchQueue(default_role, "default")
        self._queues = [self._default_queue]
        #: Mesos roles for which we want to (and expect to) receive offers
        self._roles_needed = set()
        #: (task, graph) for tasks that have been launched (STARTED to KILLING), indexed by task ID
        self._active = {}
        self._closing = False  #: set to ``True`` when :meth:`close` is called
        self.http_host = http_host
        self.http_port = http_port
        self.http_url = http_url
        self.task_stats = task_stats if task_stats is not None else TaskStats()
        self._launcher_task = self._loop.create_task(self._launcher())

        # Configure the web app
        app = aiohttp.web.Application()
        app["katsdpcontroller_scheduler"] = self
        app.router.add_get("/tasks/{id}/wait_start", wait_start_handler)
        app.router.add_static("/static", importlib_resources.files("katsdpcontroller") / "static")
        self.app = app
        if runner_kwargs is None:
            runner_kwargs = {}
        self.http_runner = aiohttp.web.AppRunner(app, **runner_kwargs)

    async def start(self):
        """Start the internal HTTP server running. This must be called before any launches.

        This starts up the webapp, so any additions to it must be made first.
        """
        if self.http_runner.sites:
            raise RuntimeError("Already started")
        await self.http_runner.setup()
        site = aiohttp.web.TCPSite(self.http_runner, self.http_host, self.http_port)
        await site.start()
        self.http_host, self.http_port = self.http_runner.addresses[0][:2]
        if not self.http_url:
            self.http_url = site.name
        logger.info("Internal HTTP server at %s", self.http_url)

    def add_queue(self, queue):
        """Register a new queue.

        Raises
        ------
        ValueError
            If `queue` has already been added
        """
        if queue in self._queues:
            raise ValueError("Queue has already been added")
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
            raise asyncio.InvalidStateError("queue is not empty")
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
            return (
                node.host is not None,
                len(node.cores),
                node.cpus,
                len(node.gpus),
                node.mem,
                node.name,
            )
        return (False, 0, 0, 0, 0, node.name)

    def _update_roles(self, new_roles):
        pass  # Real implementation in Scheduler

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

    def _update_agents_multicast(self, agents):
        """Update the multicast group information for a freshly minted set of :class:`Agent` s."""
        # The agent objects in active tasks are not the same Python objects
        # as in `agents`, so we have to map between them. We build a map from
        # (agent_id, interface_name) to interface object
        interface_map = {}
        for agent in agents:
            for interface in agent.interfaces:
                interface_map[(agent.agent_id, interface.name)] = interface

        for (task, _) in self._active.values():
            for interface in task.allocation.interfaces:
                key = (task.agent_id, task.agent.interfaces[interface.index])
                new_interface = interface_map.get(key)
                if new_interface is not None:
                    new_interface.infiniband_multicast_out |= interface.infiniband_multicast_out
                    new_interface.multicast_in |= interface.multicast_in

    async def _launch_once(self):
        """Run single iteration of :meth:`_launcher`"""
        candidates = [(queue, queue.front()) for queue in self._queues if queue]
        if candidates:
            # Filter out any candidates other than the highest priority
            priority = min(queue.priority for (queue, group) in candidates)
            candidates = [
                candidate for candidate in candidates if candidate[0].priority == priority
            ]
            # Order by deadline, to give some degree of fairness
            candidates.sort(key=lambda x: x[1].deadline)
        # Revive/suppress to match the necessary roles
        roles = {queue.role for (queue, group) in candidates}
        self._update_roles(roles)
        # See comment at the bottom
        useful_agents = set()

        for queue, group in candidates:
            nodes = group.nodes
            role = queue.role
            try:
                # Due to concurrency, another coroutine may have altered
                # the state of the tasks since they were put onto the
                # pending list (e.g. by killing them). Filter those out.
                nodes = [node for node in nodes if node.state == TaskState.STARTING]
                agents = [
                    Agent(list(offers.values()))
                    for agent_id, offers in self._offers.get(role, {}).items()
                ]
                self._update_agents_multicast(agents)

                # Back up the original agents so that if allocation fails we can
                # diagnose it.
                orig_agents = copy.deepcopy(agents)
                # Sort agents by GPUs then free memory. This makes it less likely that a
                # low-memory process will hog all the other resources on a high-memory
                # box. Similarly, non-GPU tasks will only use GPU-equipped agents if
                # there is no other choice. Should eventually look into smarter
                # algorithms e.g. Dominant Resource Fairness
                # (http://mesos.apache.org/documentation/latest/allocation-module/)
                agents.sort(key=lambda agent: (agent.priority, agent.resources["mem"].available))
                nodes.sort(key=self._node_sort_key, reverse=True)
                allocations = []
                insufficient = False
                for node in nodes:
                    allocation = None
                    if isinstance(node, PhysicalTask):
                        for agent in agents:
                            try:
                                allocation = agent.allocate(node.logical_node)
                                useful_agents.add((role, agent.agent_id))
                                break
                            except InsufficientResourcesError as error:
                                logger.debug(
                                    "Cannot add %s to %s: %s", node.name, agent.agent_id, error
                                )
                        else:
                            insufficient = True
                            # We keep trying to allocate remaining nodes
                            # anyway, just to populate useful_agents.
                            continue
                        allocations.append((node, allocation))
                if insufficient:
                    # Raises an InsufficientResourcesError
                    from .diagnose_insufficient import diagnose_insufficient

                    diagnose_insufficient(orig_agents, nodes)
            except InsufficientResourcesError as error:
                logger.debug("Could not yet launch graph: %s", error)
                group.last_insufficient = error
            else:
                try:
                    # At this point we have a sufficient set of offers.
                    group.last_insufficient = None
                    if not group.resources_future.done():
                        group.resources_future.set_result(None)
                    # Two-phase resolving
                    logger.debug("Allocating resources to tasks")
                    for (node, allocation) in allocations:
                        node.allocate(allocation)
                    logger.debug("Performing resolution")
                    order_graph = subgraph(group.graph, DEPENDS_RESOLVE, nodes)
                    # Lexicographical sorting isn't required for
                    # functionality, but the unit tests depend on it to get
                    # predictable task IDs.
                    for node in networkx.lexicographical_topological_sort(
                        order_graph.reverse(), key=lambda x: x.name
                    ):
                        logger.debug("Resolving %s", node.name)
                        await node.resolve(group.resolver, group.graph)
                    # Last chance for the group to be cancelled. After this point, we must
                    # not await anything.
                    if group.future.cancelled():
                        # No need to set _wakeup_launcher, because the cancellation did so.
                        continue
                    # Launch the tasks
                    taskinfos = {agent: [] for agent in agents}
                    for (node, allocation) in allocations:
                        taskinfos[node.agent].append(node.taskinfo)
                        self._active[node.taskinfo.task_id.value] = (node, group.graph)
                    for agent in agents:
                        if not taskinfos[agent]:
                            # Leave the offers in place: they might be useful
                            # for the next item in the queue, and if they're
                            # not needed then the next call to _update_roles
                            # will remove them.
                            continue
                        offer_ids = [offer.id for offer in agent.offers]
                        # TODO: does this need to use run_in_executor? Is it
                        # thread-safe to do so?
                        # TODO: if there are more pending in the queue then we
                        # should use a filter to be re-offered remaining
                        # resources of this agent more quickly.
                        assert self._driver is not None
                        self._driver.launchTasks(offer_ids, taskinfos[agent])
                        for offer_id in offer_ids:
                            self._remove_offer(role, agent.agent_id, offer_id.value)
                        logger.info(
                            "Launched %d tasks on %s", len(taskinfos[agent]), agent.agent_id
                        )
                    for node in nodes:
                        node.set_state(TaskState.STARTED)
                    group.future.set_result(None)
                    queue.remove(group)
                    self._wakeup_launcher.set()
                except BaseException as error:
                    if not group.future.done():
                        group.future.set_exception(error)
                    # Don't remove from the queue: launch() handles that
                break

        if candidates and not self._wakeup_launcher.is_set():
            # We failed to launch anything and we're not about to try again,
            # so nothing will happen until there is a change. We want to
            # avoid holding on to offers that aren't useful (so that other
            # Mesos frameworks can use them), while not releasing offers that
            # will be useful once more offers become available.
            #
            # This is just an approximation and may decline more than it
            # should. For example, if there are two candidates, it might
            # consider one offer sufficient to satisfy a requirement in both
            # candidates, even though it cannot be used for both. However, it
            # should still always ensure forward progress.
            to_decline = []
            for role, role_offers in self._offers.items():
                for agent_id, agent_offers in list(role_offers.items()):
                    if (role, agent_id) not in useful_agents:
                        to_decline.extend([offer.id for offer in agent_offers.values()])
                        del role_offers[agent_id]
            if to_decline:
                assert self._driver is not None
                self._driver.declineOffer(to_decline)

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
                raise  # Normal operation in close()
            except Exception:
                logger.exception("Error in _launch_once")

    @classmethod
    def depends_resources(cls, data):
        """Whether edge attribute has a `depends_resources` dependency.

        It takes into account the keys that implicitly specify a
        `depends_resources` dependency.
        """
        keys = [DEPENDS_RESOURCES, DEPENDS_RESOLVE, DEPENDS_READY, "port"]
        return any(data.get(key) for key in keys)

    async def launch(self, graph, resolver, nodes=None, *, queue=None, resources_timeout=None):
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
            raise asyncio.InvalidStateError("Cannot launch tasks while shutting down")
        if nodes is None:
            nodes = graph.nodes()
        if queue is None:
            queue = self._default_queue
        if queue not in self._queues:
            raise ValueError("queue has not been added to the scheduler")
        if resources_timeout is None:
            resources_timeout = self.resources_timeout
        if self._driver is None:
            for node in nodes:
                if isinstance(node, PhysicalTask):
                    raise TypeError(f"{node.name} is a PhysicalTask but there is no Mesos driver")
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
                    raise DependencyError(
                        "{} depends on {} but it is neither"
                        "started nor scheduled".format(src.name, trg.name)
                    )
        if not networkx.is_directed_acyclic_graph(depends_ready_graph):
            raise CycleError("cycle between depends_ready dependencies")
        if not networkx.is_directed_acyclic_graph(depends_resolve_graph):
            raise CycleError("cycle between depends_resolve dependencies")

        for node in remaining:
            node.set_state(TaskState.STARTING)
        if resources_timeout is not None:
            deadline = self._loop.time() + resources_timeout
        else:
            deadline = math.inf
        pending = _LaunchGroup(graph, remaining, resolver, deadline)
        empty = not queue
        queue.add(pending, self.task_stats)
        if empty:
            self._wakeup_launcher.set()
        try:
            await asyncio.wait_for(pending.resources_future, timeout=resources_timeout)
            await pending.future
        except BaseException as error:
            logger.debug("Exception in launching group", exc_info=True)
            # Could be
            # - a timeout
            # - we were cancelled
            # - some internal error, such as the image was not found
            # - close() was called, which cancels pending.future
            for node in remaining:
                if node.state == TaskState.STARTING:
                    node.set_state(TaskState.NOT_READY)
            at_front = queue and queue.front() is pending
            queue.remove(pending)
            self._wakeup_launcher.set()
            if isinstance(error, asyncio.TimeoutError) and pending.last_insufficient is not None:
                if not at_front:
                    raise QueueBusyError(resources_timeout)
                else:
                    raise pending.last_insufficient from None
            else:
                raise
        ready_futures = [node.ready_event.wait() for node in nodes]
        await asyncio.gather(*ready_futures)

    async def _batch_run_once(self, graph, resolver, nodes, *, queue, resources_timeout):
        """Single attempt for :meth:`batch_run`."""

        async def wait_one(node):
            await node.dead_event.wait()
            if node.status is not None and node.status.state != "TASK_FINISHED":
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
            done, pending = await asyncio.wait(futures, return_when=asyncio.FIRST_EXCEPTION)
            # Raise the TaskError if any
            for future in done:
                future.result()
        finally:
            # asyncio.wait doesn't cancel futures if it is itself cancelled
            for future in futures:
                future.cancel()
            # In success case this will be immediate since it's all dead already
            await self.kill(graph, nodes)

    async def _batch_run_retry(
        self, graph, resolver, nodes=None, *, queue=None, resources_timeout=None, attempts=1
    ):
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
        """
        if nodes is None:
            nodes = list(graph.nodes())
        while attempts > 0:
            attempts -= 1
            try:
                await self._batch_run_once(
                    graph, resolver, nodes, queue=queue, resources_timeout=resources_timeout
                )
            except TaskError as error:
                if not attempts:
                    raise
                else:
                    logger.warning("Batch graph failed (%s), retrying", error)
                    new_nodes = [node.clone() for node in nodes]
                    mapping = dict(zip(nodes, new_nodes))
                    networkx.relabel_nodes(graph, mapping, copy=False)
                    nodes = new_nodes
                    self.task_stats.batch_tasks_retried(len(nodes))
            else:
                break

    async def batch_run(
        self, graph, resolver, nodes=None, *, queue=None, resources_timeout=None, attempts=1
    ):
        """Run a collection of batch tasks.

        Each element of `nodes` may be either a single node or a sequence of
        nodes to launch jointly. If not specified, it defaults to all the nodes
        in the graph (launched separately).

        Dependencies may be specified by using edges with a
        ``depends_finished`` attribute. At present there is **no** checking
        for cyclic dependencies (it is complicated because it interacts with
        grouping and with the other types of dependencies), but in future it
        may raise :exc:`CycleError`. If a dependent task group fails (after
        all retries), the depending task is skipped, unless the edge also
        has a ``depends_finished_critical`` attribute set to false.

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
            See above
        queue : :class:`LaunchQueue`
            Queue from which to launch the nodes. If not specified, a default
            queue is used.
        resources_timeout : float
            Time (seconds) to wait for sufficient resources to launch the nodes. If not
            specified, defaults to the class value.
        attempts : int
            Number of times to try running the graph

        Returns
        -------
        dict
            Maps each node run (the original passed in, not any replacement
            created by retries) to an exception, or to ``None`` if it ran
            successfully. For tasks that were skipped, it will be
            :exc:`TaskSkipped`.

            Each group of tasks will share a single
            exception, and so the error message may refer to a different
            task.
        """
        if nodes is None:
            nodes = list(graph.nodes())

        order_graph = subgraph(graph, DEPENDS_FINISHED)
        # After this point, we are careful not to touch the physical_graph
        # again, because it gets mutated by retries. All node references
        # refer to the original nodes.
        futures = {}

        async def run(node_set):
            """Runs a single groups of task, returning True if it ran successfully."""

            try:
                for node in node_set:
                    for _, dep, critical in order_graph.edges(
                        node, data=DEPENDS_FINISHED_CRITICAL, default=True
                    ):
                        future = futures[dep]
                        logger.info("%s waiting for %s", node.name, dep.name)
                        try:
                            await future
                        except Exception:
                            if len(node_set) == 1:
                                desc = node.name
                            else:
                                desc = node.name + f" (and {len(node_set) - 1} others)"
                            if critical:
                                logger.info("Skipping %s because %s failed", desc, dep.name)
                                self.task_stats.batch_tasks_skipped(len(node_set))
                                raise TaskSkipped(node) from None
                            else:
                                logger.debug(
                                    "Continuing with %s after non-critical %s failed",
                                    desc,
                                    dep.name,
                                )

                self.task_stats.batch_tasks_started(len(node_set))
                try:
                    await self._batch_run_retry(
                        graph,
                        resolver,
                        node_set,
                        queue=queue,
                        resources_timeout=resources_timeout,
                        attempts=attempts,
                    )
                except Exception:
                    logger.exception("Batch task %s failed", node.name)
                    self.task_stats.batch_tasks_failed(len(node_set))
                    raise
            finally:
                self.task_stats.batch_tasks_done(len(node_set))

        n_nodes = 0
        future_list = []
        for node_set in nodes:
            if isinstance(node_set, PhysicalNode):
                node_set = [node_set]
            n_nodes += len(node_set)
            future = self._loop.create_task(run(node_set))
            future_list.append(future)
            for node in node_set:
                futures[node] = future

        self.task_stats.batch_tasks_created(n_nodes)
        await asyncio.gather(*future_list, return_exceptions=True)
        return {node: future.exception() for (node, future) in futures.items()}

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
                    logger.debug("Killing %s", node.name)
                    node.kill(self._driver, **kwargs)
                    node.set_state(TaskState.KILLING)
            await node.dead_event.wait()

        kill_graph = subgraph(graph, DEPENDS_KILL, nodes)
        if not networkx.is_directed_acyclic_graph(kill_graph):
            raise CycleError("cycle between depends_kill dependencies")
        futures = []
        for node in kill_graph:
            futures.append(asyncio.ensure_future(kill_one(node, kill_graph)))
        await asyncio.gather(*futures)

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
        self._closing = True  # Prevents concurrent launches
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
        await _cleanup_task(self._launcher_task)
        self._launcher_task = None

    def get_task(self, task_id, return_graph=False):
        try:
            if return_graph:
                return self._active[task_id]
            return self._active[task_id][0]
        except KeyError:
            return None


class Scheduler(SchedulerBase, pymesos.Scheduler):
    """Top-level scheduler implementing the Mesos Scheduler API.

    Mesos calls the callbacks provided in this class from another thread. To
    ensure thread safety, they are all posted to the event loop via a
    decorator.

    The following invariants are maintained at each yield point:
    - each dictionary within :attr:`_offers` is non-empty
    - every role in :attr:`_roles_needed` is non-suppressed (there may be
      additional non-suppressed roles, but they will be suppressed when an
      offer arrives)
    - every role in :attr:`_offers` also appears in :attr:`_roles_needed`

    Refer to :class:`SchedulerBase` for descriptions of parameters, attributes
    etc.
    """

    def __init__(self, *args, **kwargs) -> None:
        SchedulerBase.__init__(self, *args, **kwargs)
        self._reconciliation_task = asyncio.get_event_loop().create_task(self._reconciliation())

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

    def set_driver(self, driver):
        self._driver = driver

    def registered(self, driver, framework_id, master_info):
        pass

    @run_in_event_loop
    def resourceOffers(self, driver, offers):
        def format_time(t):
            return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(t))

        to_decline = []
        to_suppress = set()
        for offer in offers:
            if offer.unavailability:
                start_time_ns = offer.unavailability.start.nanoseconds
                start_time = start_time_ns / 1e9
                if not offer.unavailability.duration:
                    logger.debug(
                        "Declining offer %s from %s: unavailable from %s forever",
                        offer.id.value,
                        offer.hostname,
                        format_time(start_time),
                    )
                    to_decline.append(offer.id)
                    continue
                end_time_ns = start_time_ns + offer.unavailability.duration.nanoseconds
                end_time = end_time_ns / 1e9
                if end_time >= time.time():
                    logger.debug(
                        "Declining offer %s from %s: unavailable from %s to %s",
                        offer.id.value,
                        offer.hostname,
                        format_time(start_time),
                        format_time(end_time),
                    )
                    to_decline.append(offer.id)
                    continue
                else:
                    logger.debug(
                        "Offer %s on %s has unavailability in the past: %s to %s",
                        offer.id.value,
                        offer.hostname,
                        format_time(start_time),
                        format_time(end_time),
                    )
            role = offer.allocation_info.role
            if role in self._roles_needed:
                logger.debug(
                    "Adding offer %s on %s with role %s to pool",
                    offer.id.value,
                    offer.agent_id.value,
                    role,
                )
                role_offers = self._offers.setdefault(role, {})
                agent_offers = role_offers.setdefault(offer.agent_id.value, {})
                agent_offers[offer.id.value] = offer
            else:
                # This can happen either due to a race or at startup, when
                # we haven't yet suppressed roles.
                logger.debug(
                    "Declining offer %s on %s with role %s",
                    offer.id.value,
                    offer.agent_id.value,
                    role,
                )
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
            logger.debug("Declining inverse offer %s", offer.id.value)
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
            "Update: task %s in state %s (%s)", status.task_id.value, status.state, status.message
        )
        task = self.get_task(status.task_id.value)
        if task is not None:
            if status.state in TERMINAL_STATUSES:
                task.set_state(TaskState.DEAD)
                del self._active[status.task_id.value]
            elif status.state == "TASK_RUNNING":
                if task.state < TaskState.RUNNING:
                    task.set_state(TaskState.RUNNING)
                    # The task itself is responsible for advancing to
                    # READY
            task.set_status(status)
            if (
                task.kill_sent_time is not None
                and status.state != "TASK_KILLING"
                and status.state not in TERMINAL_STATUSES
                and self._loop.time() > task.kill_sent_time + self.kill_timeout
            ):
                logger.warning("Retrying kill on %s (task ID %s)", task.name, status.task_id.value)
                self._driver.killTask(status.task_id)
        else:
            if status.state not in TERMINAL_STATUSES:
                logger.warning(
                    "Received status update for unknown task %s, killing it", status.task_id.value
                )
                self._driver.killTask(status.task_id)
        self._driver.acknowledgeStatusUpdate(status)

    async def _reconciliation(self) -> None:
        """Background task that periodically requests reconciliation.

        See http://mesos.apache.org/documentation/latest/reconciliation/ for
        a description of reconciliation.

        We don't use the (somewhat complex) algorithm described there, instead
        just periodically requesting either implicit or explicit
        reconciliation (the former is useful to learn about tasks we thought
        were dead, the latter to learn about tasks we thought were alive but
        have been lost). This means that it may take somewhat longer to
        converge.
        """
        explicit = True
        while True:
            try:
                if explicit:
                    tasks = [{"task_id": {"value": task_id}} for task_id in self._active]
                    logger.debug("Requesting explicit reconciliation of %d tasks", len(tasks))
                else:
                    tasks = []
                    logger.debug("Requesting implicit reconciliation")
                self._driver.reconcileTasks(tasks)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.warning("Exception during task reconciliation", exc_info=True)
            await asyncio.sleep(self.reconciliation_interval)
            explicit = not explicit

    async def close(self):
        await super().close()
        await _cleanup_task(self._reconciliation_task)
        self._reconciliation_task = None
        self._driver.stop()
        # If self._driver is a mock, then asyncio incorrectly complains about passing
        # self._driver.join directly, due to
        # https://github.com/python/asyncio/issues/458. So we wrap it in a lambda.
        status = await self._loop.run_in_executor(None, lambda: self._driver.join())
        return status


__all__ = [
    "LogicalNode",
    "PhysicalNode",
    "LogicalExternal",
    "PhysicalExternal",
    "LogicalTask",
    "PhysicalTask",
    "FakePhysicalTask",
    "AnyPhysicalTask",
    "TaskState",
    "Volume",
    "ResourceRequest",
    "ScalarResourceRequest",
    "RangeResourceRequest",
    "Resource",
    "ScalarResource",
    "RangeResource",
    "ResourceAllocation",
    "GPUResources",
    "InterfaceResources",
    "InsufficientResourcesError",
    "InterfaceRequest",
    "GPURequest",
    "Image",
    "ImageLookup",
    "SimpleImageLookup",
    "HTTPImageLookup",
    "ImageResolver",
    "TaskIDAllocator",
    "Resolver",
    "Agent",
    "AgentGPU",
    "AgentInterface",
    "SchedulerBase",
    "Scheduler",
    "instantiate",
]
