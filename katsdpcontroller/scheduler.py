from __future__ import print_function, division, absolute_import
import networkx
import ipaddress
import logging
import json
import jsonschema
import itertools
import six
import re
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


class InsufficientResourcesError(RuntimeError):
    pass


class LogicalTask(object):
    """A node in a :class:`LogicalGraph`. It indicates how to run a task
    and what resources it requires, but does not correspond to any specific
    running task.

    Several of the attributes are processed by :meth:`str.format` to
    substitute dynamic values. The following named values are provided:

    - `ports` : dictionary of port numbers
    - `cores` : dictionary of reserved core IDs
    - `interfaces` : dictionary mapping requested networks to :class:`Interface` objects

    Attributes
    ----------
    name : str
        Service name
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
    physical_factory : callable
        Class to use for the physical task (must return :class:`PhysicalTask` or subclass)
    """
    def __init__(self, name):
        self.name = name
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


class PhysicalTask(object):
    """Running task (or one that is being prepared to run). Unlike
    a :class:`LogicalTask`, it has a specific agent, ports, etc assigned.
    """
    def __init__(self, logical_task, agent):
        self.logical_task = logical_task
        self.slave_id = agent.slave_id
        self.interfaces = {}
        for network in self.logical_task.networks:
            for interface in agent.interfaces:
                if interface.network == network:
                    self.interfaces[network] = interface
                    break
        self.task_id = None
        self.dead_future = None     #: Future signalled when the task dies
        for r in RANGE_RESOURCES:
            setattr(self, r, {})
            setattr(self, r + '_list', [])

    def command(self):
        args = {}     # dictionary for substitutions in command
        for r in RANGE_RESOURCES:
            args[r] = getattr(self, r)
        args['interfaces'] = self.interfaces
        return [x.format(**args) for x in self.logical_task.command]

    def taskinfo(self, image_resolver):
        """Generates a Mesos TaskInfo message suitable for launching this task.
        The returned TaskInfo lacks a task ID.
        """
        taskinfo = mesos_pb2.TaskInfo()
        taskinfo.name = self.logical_task.name
        command = self.command()
        if self.cores_list:
            command = ['taskset', '-c', ','.join(str(core) for core in self.cores_list)] + command
        if command:
            taskinfo.command.value = command[0]
            taskinfo.command.arguments.extend(command[1:])
        taskinfo.command.shell = False
        taskinfo.container.CopyFrom(self.logical_task.container)
        image_path = image_resolver(self.logical_task.image)
        taskinfo.container.docker.image = image_path
        taskinfo.container.docker.force_pull_image = image_resolver.pullable(image_path)
        taskinfo.task_id.value = self.task_id
        taskinfo.slave_id.value = self.slave_id
        for r in SCALAR_RESOURCES:
            value = getattr(self.logical_task, r)
            if value > 0:
                resource = taskinfo.resources.add()
                resource.name = r
                resource.type = mesos_pb2.Value.SCALAR
                resource.scalar.value = value
        for r in RANGE_RESOURCES:
            value = getattr(self, r + '_list')
            if value:
                resource = taskinfo.resources.add()
                resource.name = r
                resource.type = mesos_pb2.Value.RANGES
                for item in value:
                    rng = resource.ranges.range.add()
                    rng.begin = item
                    rng.end = item
        return taskinfo


class Agent(object):
    """Collects multiple offers for a single Mesos agent and allows tasks to
    be added.

    TODO: think up a better name for this. It's a set of offers and tasks that
    are all associated with one agent, but the system as a whole may have
    multiple Agent objects corresponding to the same Mesos agent.
    """
    def __init__(self, slave_id):
        self.slave_id = slave_id
        self.offers = []
        self.tasks = []      #: physical tasks
        # These resources all represent resources not yet allocated to tasks
        for r in SCALAR_RESOURCES:
            setattr(self, r, 0.0)
        for r in RANGE_RESOURCES:
            setattr(self, r, RangeResource())
        self.interfaces = []
        self.attributes = None

    def _inc_attr(self, key, delta):
        setattr(self, key, getattr(self, key) + delta)

    def _add_range_attr(self, key, resource):
        getattr(self, key).add_resource(resource)

    def add_offer(self, offer):
        """Add an offer received from Mesos, updating the free resources"""
        self.offers.append(offer)
        for resource in offer.resources:
            if resource.name in SCALAR_RESOURCES:
                self._inc_attr(resource.name, resource.scalar.value)
            elif resource.name in RANGE_RESOURCES:
                self._add_range_attr(resource.name, resource)
        if self.attributes is None:
            self.attributes = offer.attributes
            for attribute in offer.attributes:
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

    def add_task(self, logical_task):
        """Add a task to the list of assigned nodes, if possible, creating
        a physical task from the logical task.

        Raises
        ------
        InsufficientResourcesError
            if there are not enough resources to add the task
        """
        if not self.offers:
            raise InsufficientResourcesError('No offers have been received for this agent')
        if not logical_task.valid_agent(self.attributes):
            raise InsufficientResourcesError(' does not match this agent')
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

        # Have now verified that the task fits. Create a physical task for it
        # and assign resources.
        physical_task = logical_task.physical_factory(logical_task, self)
        for r in SCALAR_RESOURCES:
            self._inc_attr(r, -getattr(logical_task, r))
        for r in RANGE_RESOURCES:
            for name in getattr(logical_task, r):
                value = getattr(self, r).popleft()
                getattr(physical_task, r + '_list').append(value)
                if name is not None:
                    getattr(physical_task, r)[name] = value
        self.tasks.append(physical_task)


class LogicalGraph(networkx.MultiDiGraph):
    """Collection of :class:`Node`s"""
    pass


class PhysicalGraph(networkx.MultiDiGraph):
    """Collection of :class:`PhysicalTask`s.

    The constructor does not actually launch the tasks; it merely prepares
    them.

    Parameters
    ----------
    name : str
        Name associated with this graph
    logical_graph : :class:`LogicalGraph`
        Logical graph to instantiate
    offers : list
        List of Mesos resource offers to use

    Raises
    ------
    InsufficientResourcesError
        if `offers` does not contain sufficient resources for the graph
    """
    def __init__(self, name, logical_graph, offers):
        super(PhysicalGraph, self).__init__()
        self.name = name

        # Group offers by agent
        agents = {}
        for offer in offers:
            agent = agents.get(offer.slave_id.value)
            if agent is None:
                agent = agents.setdefault(offer.slave_id.value, Agent(offer.slave_id.value))
            agent.add_offer(offer)
        # Sort agents by GPUs then free memory. This makes it less likely that a
        # low-memory process will hog all the other resources on a high-memory
        # box. Similarly, non-GPU tasks will only use GPU-equipped agents if
        # there is no other choice. Should eventually look into smarter
        # algorithms e.g. Dominant Resource Fairness
        # (http://mesos.apache.org/documentation/latest/allocation-module/)
        agents = list(six.itervalues(agents))
        agents.sort(key=lambda agent: (agent.gpus, agent.mem))
        for node in logical_graph:
            if not isinstance(node, LogicalTask):
                pass
            found = False
            for agent in agents:
                try:
                    agent.add_task(node)
                    found = True
                    break
                except InsufficientResourcesError as e:
                    logger.debug('Cannot add %s to %s: %s',
                                 node.name, agent.slave_id, e)
                    pass
            if not found:
                raise InsufficientResourcesError(
                    'Insufficient resources to run {}'.format(node.name))
        self.agents = agents
        for agent in agents:
            for task in agent.tasks:
                self.add_node(task)


@decorator
def run_in_event_loop(func, *args, **kw):
    args[0]._loop.call_soon_threadsafe(func, *args, **kw)


class Scheduler(mesos.interface.Scheduler):
    def __init__(self, image_resolver, loop):
        self._next_task_id = 0
        self._loop = loop
        self._drivers = None
        self._offers = {}     # offer ID forms the key
        self._pending_logical = deque()
        self._running = {}    # physical graphs, indexed by name
        self._closing = False
        self._image_resolver = image_resolver

    def _make_task_id(self):
        ret = str(self._next_task_id).zfill(8)
        self._next_task_id += 1
        return ret

    def _try_launch(self):
        # TODO: if there are multiple pending graphs, we could try to pack
        # them all into the available offers, rather than having to wait
        # for the unused resources to be offered to us again.
        if self._pending_logical and self._offers:
            try:
                (logical, physical_name, future) = self._pending_logical[0]
                physical = PhysicalGraph(physical_name, logical, self._offers.values())
            except InsufficientResourcesError:
                logger.debug('Could not launch %s due to insufficient resources', physical_name)
                return
            else:
                # Assign IDs to the tasks
                for task in physical.nodes():
                    task.task_id = self._make_task_id()
                # Launch the tasks
                # TODO: does this need to use run_in_executor? Is it
                # thread-safe to do so?
                for agent in physical.agents:
                    offer_ids = [offer.id for offer in agent.offers]
                    taskinfos = [task.taskinfo(self._image_resolver) for task in agent.tasks]
                    self._driver.launchTasks(offer_ids, taskinfos)
                    for task in agent.tasks:
                        task.dead_future = trollius.Future(loop=self._loop)
                    logger.info('Launched %d tasks on %s', len(agent.tasks), agent.slave_id)
                self._offers = {}   # launchTasks declines any unused resources
                self._running[physical_name] = physical
                # TODO: should we wait for statuses to come back first?
                self._pending_logical.popleft()
                future.set_result(physical)
                if not self._pending_logical:
                    self._driver.suppressOffers()

    def set_driver(self, driver):
        self._driver = driver

    def registered(self, driver, framework_id, master_info):
        pass

    @run_in_event_loop
    def resourceOffers(self, driver, offers):
        for offer in offers:
            self._offers[offer.id.value] = offer
        self._try_launch()

    @run_in_event_loop
    def offerRescinded(self, offer_id):
        try:
            del self._offers[offer_id]
        except KeyError:
            pass

    @run_in_event_loop
    def statusUpdate(self, driver, status):
        logging.debug('Update: task %s in state %s (%s)',
            status.task_id.value, mesos_pb2.TaskState.Name(status.state),
            status.message)
        # TODO: use a lookup to more easily find tasks from IDs
        for physical in self._running.values():
            for task in physical.nodes():
                if task.task_id == status.task_id.value and not task.dead_future.done():
                    if status.state in TERMINAL_STATES:
                        task.dead_future.set_result(status.state)
        self._driver.acknowledgeStatusUpdate(status)

    @trollius.coroutine
    def launch(self, logical, physical_name):
        if self._closing:
            raise RuntimeError('Cannot launch {} while shutting down'.format(physical_name))
        if physical_name in self._running:
            raise ValueError('Graph {} is already running'.format(physical_name))
        for pending in self._pending_logical:
            if physical_name == pending[1]:
                raise ValueError('Graph {} is already pending'.format(physical_name))
        if not self._pending_logical:
            self._driver.reviveOffers()
        # TODO: use requestResources to ask the allocator for resources
        future = trollius.Future(loop=self._loop)
        pending = (logical, physical_name, future)
        self._pending_logical.append(pending)
        self._try_launch()
        try:
            physical = yield From(future)
            raise Return(physical)
        finally:
            # In particular, if the user cancels the launch, we need to
            # do this.
            if pending in self._pending_logical:
                self._pending_logical.remove(pending)

    @trollius.coroutine
    def kill(self, name):
        try:
            physical = self._running[name]
        except KeyError:
            raise KeyError('Graph {} is not running'.format(name))
        futures = []
        for task in physical.nodes():
            # TODO: according to the Mesos docs, killing a task is not reliable,
            # and may need to be attempted again.
            if not task.dead_future.done():
                task_id = mesos_pb2.TaskID()
                task_id.value = task.task_id
                self._driver.killTask(task_id)
            futures.append(task.dead_future)
        yield From(trollius.gather(*futures, loop=self._loop))
        try:
            del self._running[name]
        except KeyError:
            # Can happen if another caller also killed the graph when we yielded
            pass

    @trollius.coroutine
    def close(self):
        self._closing = True
        # TODO: do we need to explicitly decline outstanding offers?
        for (logical, physical_name, future) in self._pending_logical:
            future.cancel()
        self._pending_logical = []
        names = list(six.iterkeys(self._running))
        for name in names:
            yield From(self.kill(name))
        self._driver.stop()
        status = yield From(self._loop.run_in_executor(None, self._driver.join))
        raise Return(status)


__all__ = [
    'LogicalTask', 'PhysicalTask',
    'Interface', 'InsufficientResourcesError',
    'LogicalGraph', 'PhysicalGraph',
    'Scheduler', 'ImageResolver']
