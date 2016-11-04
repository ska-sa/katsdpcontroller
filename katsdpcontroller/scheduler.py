from __future__ import print_function, division, absolute_import
import networkx
import ipaddress
import json
import itertools
from collections import namedtuple, deque


Interface = namedtuple('Interface', ['name', 'network', 'ipv4_address'])
"""Abstraction of a network interface on a machine

Attributes
----------
name : str
    Kernel interface name
network : str
    Logical name for the network to which the interface is attached
ipv4_address : :class:`ipaddress.IPv4Address`
    IPv4 local address of the interface
"""


class Node(object):
    """A node in a :class:`LogicalGraph`. It indicates how to run a task
    and what resources it requires, but does not correspond to any specific
    running task.

    Several of the attributes are processed by :meth:`str.format` to
    substitute dynamic values. The following named values are provided:

    - `ports` : array of port numbers
    - `cores` : array of reserved core IDs
    - `interfaces` : dictionary mapping requested networks to :class:`Interface` objects

    Attributes
    ----------
    name : str
        Service name
    cpus : float
        Mesos CPU shares, excluding `cores`. At least one (and usually only one)
        of :attr:`cpus` and :attr:`cores` must be set.
    gpus : float
        GPU resources required
    cores : int
        Reserved CPU cores. If this is zero, then `cpus` must be non-zero, and
        the task will not be pinned. If it is non-zero, then the task will be
        pinned to the assigned cores, and no other tasks will be pinned to
        those cores. In addition, the cores will all be taken from the same
        NUMA socket, and network interfaces in :attr:`networks` will also come
        from the same socket.
    mem : float
        Mesos memory reservation (megabytes)
    ports : int
        Network service ports
    networks : list
        Abstract names of networks for which network interfaces are required.
        At present duplicating a name in the list is not supported, but in
        future it may allow requesting multiple interfaces on the same
        network.
    image : str
        Base name of the Docker image (without registry or tag)
    docker_options : list
        Extra command-line options to pass to Docker
    command : list of str
        Command to run inside the image (formatted)
    config : dictionary
        Configuration options passed via telstate (string values are formatted)
    """
    def __init__(self, name):
        self.name = name
        self.cpus = 0.0
        self.gpus = 0.0
        self.cores = 0
        self.mem = 0
        self.ports = 0
        self.networks = []
        self.labels = set()
        self.image = None
        self.command = []
        self.docker_options = []

    def valid_agent(self, attributes):
        """Checks whether the attributes of an agent are suitable for running
        this node. Subclasses may override this to enforce constraints e.g.,
        requiring a special type of hardware."""
        # TODO: enforce x86-64, if we ever introduce ARM or other hardware
        return True


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

    def add_range(start, stop):
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


class Agent(object):
    """Collects multiple offers for a single Mesos agent and allows nodes to
    be added, turning them into (unlaunched) tasks.
    """
    SCALAR_RESOURCES = ['cpus', 'gpus', 'mem']
    RANGE_RESOURCES = ['ports', 'cores']

    def __init__(self, slave_id):
        self.slave_id = slave_id
        self.offers = []
        self.tasks = []
        # These resources all represent resources not yet allocated to tasks
        for r in self.SCALAR_RESOURCES:
            setattr(self, r, 0.0)
        for r in self.RANGE_RESOURCES:
            setattr(self, r, RangeResource())
        self.interfaces = set()
        self.attributes = None

    def _inc_attr(self, key, delta):
        setattr(self, key, getattr(self, key) + delta)

    def _add_range_attr(self, key, resource):
        getattr(self, key).add_resource(resource)

    def add_offer(self, offer):
        """Add a store an offer received from Mesos, updating the free resources"""
        self.offers.append(offer)
        for resource in offer.resources:
            if resource.name in self.SCALAR_RESOURCES:
                self._inc_attr(resource.name, resource.scalar.value)
            elif resource.name in self.RANGE_RESOURCES:
                self._add_range_attr(resource.name, resource)
        if self.attributes is None:
            self.attributes = offer.attributes
            for attribute in offer.attributes:
                if attribute.name.startswith('interface_') and \
                        attribute.type == mesos_pb2.Value.SCALAR:
                    try:
                        value = json.loads(attribute.text.value)
                        interface = Interface(value['name'], value['network'],
                                ipaddress.IPv4Address(value['ipv4_address']))
                    except (ValueError, KeyError, TypeError, ipaddress.AddressValueError):
                        logger.warn('Could not parse {} ({})'.format(attribute.name, attribute.text.value))
                    else:
                        self.networks.append(interface)

    def add_node(self, node):
        """Add a node to the list of assigned nodes, if possible.

        Raises
        ------
        InsufficientResourcesError
            if there are not enough resources to add the node
        """
        if not self.offers:
            raise InsufficientResourcesError('No offers have been received for this agent')
        if not node.valid_node(self.attributes):
            raise InsufficientResourcesError('Node does not match this agent')
        # TODO: add NUMA awareness
        for r in self.SCALAR_RESOURCES:
            need = getattr(node, r)
            have = getattr(self, r)
            if have < need:
                raise InsufficientResourcesError('Not enough {} ({} < {})'.format(r, have, need))
        for r in self.RANGE_RESOURCES:
            need = getattr(node, r)
            have = len(getattr(self, r))
            if have < need:
                raise InsufficientResourcesError('Not enough {} ({} < {})'.format(r, have, need))
        for network in node.networks:
            if not any(interface.network == network for interface in interfaces):
                raise InsufficientResourcesError('Network {} not present'.format(network))

        # Have now verified that the node fits. Create a task for it and assign resources
        task = Task(node)
        task.slave_id = self.slave_id
        for r in self.SCALAR_RESOURCES:
            setattr(task, r, getattr(node, r))
            self._inc_attr(r, -getattr(node, r))
        for r in self.RANGE_RESOURCES:
            value = getattr(self, r).popleft()
            getattr(task, r).append(value)


class LogicalGraph(networkx.MultiDiGraph):
    """Collection of :class:`Node`s"""

    def instantiate(self, offers):
        """Create a physical graph from the given offers, if possible

        Returns
        -------
        PhysicalGraph

        Raises
        ------
        InsufficientResourcesError
            if `offers` does not contain sufficient resources to launch the graph
        """
        # Group offers by slave
        by_slave = {}
        for offer in offers:
            by_slave.setdefault(offer.slave_id.value, []).append(offer)


__all__ = ['Node', 'LogicalGraph', 'InsufficientResourcesError']
