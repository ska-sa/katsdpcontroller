"""
Mesos-based scheduler for launching collections of inter-dependent tasks. It
uses trollius for asynchronous execution. It is **not** thread-safe.

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
:const:`GPUS_SCHEMA`. For the ith GPU in this list (counting from 0), there
must be corresponding resources :samp:`katsdpcontroller.gpu.{i}.compute`
(generally with capacity 1.0) and :samp:`katsdpcontroller.gpu.{i}.mem` (in
MiB).

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

Note that Mesos slaves typically require a manual recovery_ step after changing
resources or attributes and restarting.

.. _recovery: http://mesos.apache.org/documentation/latest/agent-recovery/
"""


from __future__ import print_function, division, absolute_import
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
import ipaddress
import requests
import docker
import six
from six.moves import urllib
import networkx
import jsonschema
from katsdptelstate.endpoint import Endpoint
from decorator import decorator
import trollius
from trollius import From, Return
from addict import Dict
import pymesos


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
NUMA_SCHEMA = {
    'type': 'array',
    'items': {
        'type': 'array',
        'items': {
            'type': 'integer',
            'minimum': 0,
        },
        'minItems': 1,
        'uniqueItems': True
    },
    'minItems': 1,
    'uniqueItems': True
}
INTERFACES_SCHEMA = {
    'type': 'array',
    'items': {
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
                'format': 'ipv4'
            },
            'numa_node': {
                'type': 'integer',
                'minimum': 0
            },
            'infiniband_devices': {
                'type': 'array',
                'items': {
                    'type': 'string',
                    'minLength': 1
                }
            }
        },
        'required': ['name', 'network', 'ipv4_address']
    }
}
VOLUMES_SCHEMA = {
    'type': 'array',
    'items': {
        'type': 'object',
        'properties': {
            'name': {
                'type': 'string',
                'minLength': 1
            },
            'host_path': {
                'type': 'string',
                'minLength': 1
            },
            'numa_node': {
                'type': 'integer',
                'minimum': 0
            }
        },
        'required': ['name', 'host_path']
    }
}
GPUS_SCHEMA = {
    'type': 'array',
    'items': {
        'type': 'object',
        'properties': {
            'devices': {
                'type': 'array',
                'items': {
                    'type': 'string',
                    'minLength': 1
                }
            },
            'driver_version': {
                'type': 'string',
                'minLength': 1
            },
            'name': {
                'type': 'string',
                'minLength': 1
            },
            'compute_capability': {
                'type': 'array',
                'items': {'type': 'integer', 'minimum': 0},
                'minLength': 2,
                'maxLength': 2
            },
            'device_attributes': {
                'type': 'object'
            },
            'numa_node': {
                'type': 'integer',
                'minimum': 0
            }
        },
        'required': ['devices', 'driver_version', 'name', 'compute_capability', 'device_attributes']
    }
}
logger = logging.getLogger(__name__)


Volume = namedtuple('Volume', ['name', 'host_path', 'numa_node'])
"""Abstraction of a host path offered by an agent.

Volumes are defined by setting the Mesos attribute
:code:`katsdpcontroller.volumes`, whose value is a JSON string that adheres
to the schema in :const:`VOLUMES_SCHEMA`.

Attributes
----------
name : str
    A logical name that indicates the purpose of the path
host_path : str
    Path on the host machine
numa_node : int, optional
    Index of the NUMA socket to which the storage is connected
"""

class GPURequest(object):
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


class InterfaceRequest(object):
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


class VolumeRequest(object):
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
                except OSError as error:
                    logger.debug('Port %d on %s not ready: %s', port, host, error)
                    yield From(trollius.sleep(1, loop=loop))
                else:
                    break
        logger.debug('Port %d on %s ready', port, host)


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
        return itertools.chain(*[six.moves.range(start, stop) for (start, stop) in self._ranges])

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
        for i, (start, stop) in enumerate(self._ranges):
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


class GPUResourceAllocation(object):
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


class InterfaceResourceAllocation(object):
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


class ResourceAllocation(object):
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
        It does not affect overrides, to allow them to specify their own tags.
    use_digests : bool, optional
        Whether to look up the latest digests from the `registry`. If this is
        not specified, old versions of images on the agents could be used.
    """
    def __init__(self, private_registry=None, tag_file=None, use_digests=True):
        self._tag_file = tag_file
        self._private_registry = private_registry
        self._overrides = {}
        self._cache = {}
        self._use_digests = use_digests
        if self._tag_file is None:
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
                self._auth = (authdata['username'], authdata['password'])
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

    @trollius.coroutine
    def __call__(self, name, loop):
        if name in self._overrides:
            raise Return(self._overrides[name])
        elif name in self._cache:
            raise Return(self._cache[name])

        orig_name = name
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
            # Use a low timeout, so that we don't wedge the entire launch if
            # there is a connection problem.
            kwargs = dict(
                headers={'Accept': 'application/vnd.docker.distribution.manifest.v2+json'},
                auth=self._auth,
                timeout=5)
            if os.path.exists('/etc/ssl/certs/ca-certificates.crt'):
                kwargs['verify'] = '/etc/ssl/certs/ca-certificates.crt'

            def do_request():
                response = None
                try:
                    response = requests.head(url, **kwargs)
                    response.raise_for_status()
                    return response.headers['Docker-Content-Digest']
                except requests.exceptions.RequestException as error:
                    six.raise_from(ImageError('Failed to get digest from {}: {}'.format(url, error)),
                                   error)
                except KeyError:
                    raise ImageError('Docker-Content-Digest header not found for {}'.format(url))
                finally:
                    if response is not None:
                        response.close()

            digest = yield From(loop.run_in_executor(None, do_request))
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
        raise Return(resolved)


class ImageResolverFactory(object):
    """Factory for generating image resolvers. An :class:`ImageResolver`
    caches lookups, so it is useful to be able to generate a new one to
    receive fresh information.

    See :class:`ImageResolver` for an explanation of the constructor
    arguments and :meth:`~ImageResolver.override`.
    """
    def __init__(self, private_registry=None, tag_file=None, use_digests=True):
        self._tag_file = tag_file
        self._private_registry = private_registry
        self._use_digests = use_digests
        self._overrides = {}

    def override(self, name, path):
        self._overrides[name] = path

    def __call__(self):
        image_resolver = ImageResolver(self._private_registry, self._tag_file, self._use_digests)
        for name, path in six.iteritems(self._overrides):
            image_resolver.override(name, path)
        return image_resolver


class TaskIDAllocator(object):
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
        super(NoOffersError, self).__init__("No offers were received")


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
        super(TaskInsufficientResourcesError, self).__init__(node)
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
        super(TaskInsufficientGPUResourcesError, self).__init__(node)
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
        super(TaskInsufficientInterfaceResourcesError, self).__init__(node)
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
        super(TaskNoInterfaceError, self).__init__(node)
        self.request = request

    def __str__(self):
        return ("No agent matches {1}network request for {0.request.network} from "
                "task {0.node.name}".format(self, "Infiniband " if self.request.infiniband else ""))


class TaskNoVolumeError(TaskNoAgentError):
    """Indicates that a task required a volume that was not present on any agent."""
    def __init__(self, node, request):
        super(TaskNoVolumeError, self).__init__(node)
        self.request = request

    def __str__(self):
        return ("No agent matches volume request for {0.request.name} "
                "from {0.node.name}".format(self))


class TaskNoGPUError(TaskNoAgentError):
    """Indicates that a task required a GPU that did not match any agent"""
    def __init__(self, node, request_index):
        super(TaskNoGPUError, self).__init__(node)
        self.request_index = request_index

    def __str__(self):
        return "No agent matches GPU request #{0.request_index} from {0.node.name}".format(self)


class GroupInsufficientResourcesError(InsufficientResourcesError):
    """Indicates that a group of tasks collectively required more of some
    resource than were available between all the agents."""
    def __init__(self, resource, needed, available):
        super(GroupInsufficientResourcesError, self).__init__()
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
        super(GroupInsufficientGPUResourcesError, self).__init__()
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
        super(GroupInsufficientInterfaceResourcesError, self).__init__()
        self.network = network
        self.resource = resource
        self.needed = needed
        self.available = available

    def __str__(self):
        return ("Insufficient total interface {0.resource} on network {0.network} "
                "to launch all tasks ({0.needed} > {0.available})".format(self))


class CycleError(ValueError):
    """Raised for a graph that contains a cycle with a strong ordering dependency"""
    pass


class DependencyError(ValueError):
    """Raised if a launch is impossible due to an unsatisfied dependency"""
    pass


class ImageError(RuntimeError):
    """Indicates that the Docker image could not be resolved due to a problem
    while contacting the registry.
    """
    pass


class LogicalNode(object):
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
        super(LogicalExternal, self).__init__(name)
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
    """
    def __init__(self, name):
        super(LogicalTask, self).__init__(name)
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
        self.container = Dict()
        self.container.type = 'DOCKER'
        self.physical_factory = PhysicalTask

    def valid_agent(self, agent):
        """Checks whether the attributes of an agent are suitable for running
        this task. Subclasses may override this to enforce constraints e.g.,
        requiring a special type of hardware."""
        # TODO: enforce x86-64, if we ever introduce ARM or other hardware
        return True


class ResourceCollector(object):
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


def _decode_json_base64(text):
    """Decodes a object that has been encoded with JSON then url-safe base64.
    It gets a bit complicated because the text is unicode, but base64 expects
    bytes (in Python 2; Python 3 allows bytes), and returns bytes, and JSON
    decode expects text."""
    json_bytes = base64.urlsafe_b64decode(text.encode('us-ascii'))
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
                    jsonschema.validate(value, INTERFACES_SCHEMA)
                    self.interfaces = [AgentInterface(item) for item in value]
                elif attribute.name == 'katsdpcontroller.volumes' and attribute.type == 'TEXT':
                    value = _decode_json_base64(attribute.text.value)
                    jsonschema.validate(value, VOLUMES_SCHEMA)
                    volumes = []
                    for item in value:
                        volumes.append(Volume(
                            name=item['name'],
                            host_path=item['host_path'],
                            numa_node=item.get('numa_node')))
                    self.volumes = volumes
                elif attribute.name == 'katsdpcontroller.gpus' and attribute.type == 'TEXT':
                    value = _decode_json_base64(attribute.text.value)
                    jsonschema.validate(value, GPUS_SCHEMA)
                    self.gpus = [AgentGPU(item) for item in value]
                elif attribute.name == 'katsdpcontroller.numa' and attribute.type == 'TEXT':
                    value = _decode_json_base64(attribute.text.value)
                    jsonschema.validate(value, NUMA_SCHEMA)
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
                raise InsufficientResourcesError('No suitable {} found for request {}'.format(msg, i))
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
                for name in logical_task.cores:
                    value = cores.popleft()
                    alloc.cores.append(value)
                    self.cores.remove(value)
            elif r == 'ports':
                for name in logical_task.ports:
                    try:
                        value = self.ports.popleft_min(self._min_port)
                    except IndexError:
                        value = self.ports.popleft()
                    alloc.ports.append(value)
            else:
                for name in getattr(logical_task, r):
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
    host : str
        Host on which this node is operating (if any).
    ports : dict
        Dictionary mapping logical port names to port numbers.
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
    _ready_waiter : :class:`trollius.Task`
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
        self.ready_event = trollius.Event(loop=loop)
        self.dead_event = trollius.Event(loop=loop)
        self.loop = loop
        self._ready_waiter = None

    @trollius.coroutine
    def resolve(self, resolver, graph, loop):
        """Make final preparations immediately before starting.

        Parameters
        ----------
        resolver : :class:`Resolver`
            Resolver for images etc.
        graph : :class:`networkx.MultiDiGraph`
            Physical graph containing the task
        loop : :class:`trollius.BaseEventLoop`
            Current event loop
        """
        pass

    @trollius.coroutine
    def wait_ready(self):
        """Wait for the task to be ready for dependent tasks to communicate
        with, by polling the ports. This method may be overloaded to implement
        other checks, but it must be cancellation-safe.
        """
        if self.logical_node.wait_ports is not None:
            wait_ports = [self.ports[port] for port in self.logical_node.wait_ports]
        else:
            wait_ports = list(six.itervalues(self.ports))
        if wait_ports:
            yield From(poll_ports(self.host, wait_ports, self.loop))

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
        except trollius.CancelledError:
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
            self._ready_waiter = trollius.async(self.wait_ready(), loop=self.loop)
            self._ready_waiter.add_done_callback(self._ready_callback)
        if state > TaskState.READY and self._ready_waiter is not None:
            self._ready_waiter.cancel()
            self._ready_waiter = None

    def kill(self, driver):
        """Start killing off the node. This is guaranteed to be called only if
        the task made it to at least :const:`TaskState.STARTED` and it is not
        in :const:`TaskState.DEAD`.

        This function should not manipulate the task state; the caller will
        set the state to :const:`TaskState.KILLED`.
        """
        pass


class PhysicalExternal(PhysicalNode):
    """External service with a hostname and ports. The service moves
    automatically from :const:`TaskState.STARTED` to
    :const:`TaskState.RUNNING`, and from :const:`TaskState.KILLED` to
    :const:`TaskState.DEAD`.
    """
    def set_state(self, state):
        if state >= TaskState.STARTED and state < TaskState.READY:
            state = TaskState.RUNNING
        elif state == TaskState.KILLED:
            state = TaskState.DEAD
        super(PhysicalExternal, self).set_state(state)


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
        :class:`LogicalTask.interfaces` to :class:`AgentInterface`s.
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
    agent_id : str
        Slave ID of the agent on which this task is running
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

    @trollius.coroutine
    def resolve(self, resolver, graph, loop):
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

        docker_devices = set()
        docker_parameters = []
        taskinfo = Dict()
        taskinfo.name = self.name
        taskinfo.task_id.value = resolver.task_id_allocator()
        args = self.subst_args(resolver)
        command = [x.format(**args) for x in self.logical_node.command]
        if command:
            taskinfo.command.value = command[0]
            taskinfo.command.arguments = command[1:]
        taskinfo.command.shell = False
        taskinfo.container = copy.deepcopy(self.logical_node.container)
        image_path = yield From(resolver.image_resolver(self.logical_node.image, loop))
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
        for request, interface_alloc in zip(self.logical_node.interfaces, self.allocation.interfaces):
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
                interface = self.agent.interfaces[interface_alloc.index]
                docker_devices.update(interface.infiniband_devices)
                any_infiniband = True
        if any_infiniband:
            # ibverbs uses memory mapping for DMA. Take away the default rlimit
            # maximum since Docker tends to set a very low limit.
            docker_parameters.append({'key': 'ulimit', 'value': 'memlock=-1'})

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
        for port_name, port_number in six.iteritems(self.ports):
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

    def kill(self, driver):
        # TODO: according to the Mesos docs, killing a task is not reliable,
        # and may need to be attempted again.
        # The poller is stopped by set_state, so we do not need to do it here.
        driver.killTask(self.taskinfo.task_id)
        super(PhysicalTask, self).kill(driver)


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


class _LaunchGroup(object):
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
    started_event : :class:`trollius.Event`
        An event that is marked ready when the launch group has been removed
        from the pending list (either because the tasks were launched or
        because the launch failed in some way).
    """
    def __init__(self, graph, nodes, resolver, loop):
        self.nodes = nodes
        self.graph = graph
        self.resolver = resolver
        self.started_event = trollius.Event(loop=loop)

@decorator
def run_in_event_loop(func, *args, **kw):
    args[0]._loop.call_soon_threadsafe(func, *args, **kw)


class Scheduler(pymesos.Scheduler):
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

    Attributes
    ----------
    resources_timeout : float
        Time limit for resources to become available one a task is ready to
        launch.
    """
    def __init__(self, loop):
        self._loop = loop
        self._driver = None
        self._offers = {}           #: offers keyed by slave ID then offer ID
        self._retry_launch = trollius.Event(loop=self._loop)  #: set when it's time to retry a launch
        self._pending = deque()     #: node groups for which we do not yet have resources
        self._active = {}           #: (task, graph) for tasks that have been launched (STARTED to KILLED), indexed by task ID
        self._closing = False       #: set to ``True`` when :meth:`close` is called
        self._min_ports = {}        #: next preferred port for each agent (keyed by ID)
        # If offers come at 5s intervals, then 11s gives two chances.
        self.resources_timeout = 11.0   #: Time to wait for sufficient resources to be offered

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
            return (node.name,)

    def _clear_offers(self):
        for offers in six.itervalues(self._offers):
            self._driver.acceptOffers([offer.id for offer in six.itervalues(offers)], [])
        self._offers = {}
        self._driver.suppressOffers()

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
        if not self._pending:
            self._clear_offers()
        elif offers:
            self._retry_launch.set()

    @run_in_event_loop
    def offerRescinded(self, driver, offer_id):
        for agent_id, offers in six.iteritems(self._offers):
            if offer_id.value in offers:
                self._remove_offer(agent_id, offer_id.value)

    @run_in_event_loop
    def statusUpdate(self, driver, status):
        logger.debug(
            'Update: task %s in state %s (%s)',
            status.task_id.value, status.state, status.message)
        try:
            task = self._active[status.task_id.value][0]
        except KeyError:
            pass
        else:
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
        for network, interfaces in six.iteritems(networks):
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
        for network in six.iterkeys(networks):
            for r in INTERFACE_SCALAR_RESOURCES:
                need = sum(getattr(request, r)
                           for node in nodes for request in node.logical_node.interfaces
                           if request.network == network)
                if need > total_interface_resources[network][r]:
                    raise GroupInsufficientInterfaceResourcesError(
                        network, r, need, total_interface_resources[network][r])
        # Not a simple error e.g. due to packing problems
        raise InsufficientResourcesError("Insufficient resources to launch all tasks")

    @trollius.coroutine
    def _launch_group(self, group):
        try:
            empty = not self._pending
            self._pending.append(group)
            if empty:
                # TODO: use requestResources to ask the allocator for resources?
                self._driver.reviveOffers()
            else:
                # Wait for the previous entry in the queue to be done
                yield From(self._pending[-2].started_event.wait())
            assert self._pending[0] is group

            deadline = self._loop.time() + self.resources_timeout
            last_insufficient = InsufficientResourcesError('No resource offers received')
            nodes = group.nodes
            while True:
                # Wait until we have offers, but time out if we cross the
                # deadline.  If one of the tasks is killed, that can also
                # unblock us since we no longer require resources for that,
                # so wait for that too.
                remaining = max(0.0, deadline - self._loop.time())
                futures = [node.dead_event.wait() for node in nodes]
                futures.append(self._retry_launch.wait())
                # Make sure these are real futures, not coroutines
                futures = [trollius.ensure_future(future, loop=self._loop)
                           for future in futures]
                done_futures, pending_futures = yield From(trollius.wait(
                    futures, loop=self._loop, timeout=remaining,
                    return_when=trollius.FIRST_COMPLETED))
                for future in pending_futures:
                    future.cancel()
                if not done_futures:
                    # We hit the timeout (trollius.wait does not throw TimeoutError)
                    raise last_insufficient
                self._retry_launch.clear()
                try:
                    # Due to concurrency, another coroutine may have altered
                    # the state of the tasks since they were put onto the
                    # pending list (e.g. by killing them). Filter those out.
                    nodes = [node for node in nodes if node.state == TaskState.STARTING]
                    agents = [Agent(list(six.itervalues(offers)), self._min_ports.get(agent_id, 0))
                              for agent_id, offers in six.iteritems(self._offers)]
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
                    last_insufficient = error
                else:
                    # At this point we have a sufficient set of offers.
                    # Two-phase resolving
                    logger.debug('Allocating resources to tasks')
                    for (node, allocation) in allocations:
                        node.allocate(allocation)
                    logger.debug('Performing resolution')
                    for node in nodes:
                        yield From(node.resolve(group.resolver, group.graph, self._loop))
                    # Launch the tasks
                    new_min_ports = {}
                    taskinfos = {agent: [] for agent in agents}
                    for (node, allocation) in allocations:
                        taskinfos[node.agent].append(node.taskinfo)
                        for port in allocation.ports:
                            prev = new_min_ports.get(node.agent_id, 0)
                            new_min_ports[node.agent_id] = max(prev, port + 1)
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
                        logger.info('Launched %d tasks on %s', len(taskinfos[agent]), agent.agent_id)
                    for node in nodes:
                        node.set_state(TaskState.STARTED)
                    for (node, _) in allocations:
                        self._active[node.taskinfo.task_id.value] = (node, group.graph)
                    break
        finally:
            self._pending.remove(group)
            if not self._pending:
                # Nothing that can use the offers, so get rid of them
                self._clear_offers()
            group.started_event.set()

        # Note: don't use "nodes" here: we have to wait for everything to start
        ready_futures = [node.ready_event.wait() for node in group.nodes]
        yield From(trollius.gather(*ready_futures, loop=self._loop))

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

        Raises
        ------
        trollius.InvalidStateError
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
            and has a strong dependency on a node that is not in `nodes` and
            is also in state :const:`TaskState.NOT_READY`.
        """
        if self._closing:
            raise trollius.InvalidStateError('Cannot launch tasks while shutting down')
        if nodes is None:
            nodes = graph.nodes()
        # Create a startup schedule. The nodes are partitioned into groups that can
        # be started at the same time.

        # Check that we don't depend on some non-ready task outside the set.
        remaining = set(node for node in nodes if node.state == TaskState.NOT_READY)
        for src, trg in graph.out_edges_iter(remaining):
            if trg not in remaining and trg.state == TaskState.NOT_READY:
                raise DependencyError('{} depends on {} but it is neither ready not scheduled'.format(
                    src.name, trg.name))
        groups = deque()
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
                raise CycleError('strong cyclic dependency in graph')
            groups.append(list(group))
            for node in groups[-1]:
                node.state = TaskState.STARTING
            remaining = blocked

        while groups:
            # Filter out nodes that have asynchronously changed state. This
            # is particularly necessary so that we don't try to revive
            # offers after close().
            group_nodes = [node for node in groups[0] if node.state == TaskState.STARTING]
            if group_nodes:
                pending = _LaunchGroup(graph, group_nodes, resolver, self._loop)
                # TODO: check if any dependencies have died, and if so, bail out?
                try:
                    yield From(self._launch_group(pending))
                except Exception:
                    logger.debug('Exception in launching group', exc_info=True)
                    # Could be either an InsufficientResourcesError from
                    # the yield or a CancelledError if this function was
                    # cancelled.
                    while groups:
                        for node in groups.popleft():
                            if node.state == TaskState.STARTING:
                                node.set_state(TaskState.NOT_READY)
                    raise
            groups.popleft()

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
        # Find the graphs that are still running
        graphs = set()
        for (task, graph) in six.itervalues(self._active):
            graphs.add(graph)
        for group in self._pending:
            graphs.add(group.graph)
        for graph in graphs:
            yield From(self.kill(graph))
        if self._pending:
            yield From(self._pending[-1].started_event.wait())
        assert not self._pending
        self._driver.stop()
        status = yield From(self._loop.run_in_executor(None, self._driver.join))
        raise Return(status)

    def _get_master_and_slaves(self, master, timeout):
        """Implementation of :meth:`get_master_slaves`, which is run on a separate
        thread since the requests library is blocking.
        """
        url = urllib.parse.urlunsplit(('http', master, '/slaves', '', ''))
        r = requests.get(url, timeout=timeout)
        r.raise_for_status()
        try:
            slaves = r.json()['slaves']
            master_host = urllib.parse.urlsplit(url).hostname
            return master_host, [slave['hostname'] for slave in slaves]
        except (KeyError, TypeError) as error:
            six.raise_from(ValueError('Malformed response'), error)

    @trollius.coroutine
    def get_master_and_slaves(self, timeout=None):
        """Obtain a list of slave hostnames from the master.

        Parameters
        ----------
        timeout : float, optional
            Timeout for HTTP connection to the master

        Raises
        ------
        requests.exceptions.RequestException
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
        result = yield From(self._loop.run_in_executor(
            None, self._get_master_and_slaves, master, timeout))
        raise Return(result)


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
