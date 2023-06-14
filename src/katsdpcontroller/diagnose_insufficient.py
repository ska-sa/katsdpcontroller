import decimal
from dataclasses import dataclass
from decimal import Decimal
from enum import Enum
from typing import Callable, Iterable, List, NoReturn, Optional, Sequence, Tuple, Union

import networkx

from . import scheduler
from .scheduler import DECIMAL_CONTEXT, Agent, InsufficientResourcesError, PhysicalTask


class ResourceGroup(Enum):
    GLOBAL = 0
    GPU = 1
    INTERFACE = 2


@dataclass
class InsufficientResource:
    """A resource that is unsatisfiable in :exc:`InsufficientResourcesError`."""

    resource: str  # Name of the resource
    resource_group: ResourceGroup
    network: Optional[str] = None  # Network name, if this is an INTERFACE resource

    def __str__(self) -> str:
        if self.resource_group == ResourceGroup.GLOBAL:
            return self.resource
        elif self.resource_group == ResourceGroup.GPU:
            return f"GPU {self.resource}"
        else:
            ret = f"interface {self.resource}"
            if self.network is not None:
                ret += " on network " + self.network
            return ret


@dataclass
class InsufficientRequester:
    """A requester that need too much of a resource in :exc:`InsufficientResourcesError`."""

    task: PhysicalTask

    def __str__(self) -> str:
        return self.task.name


@dataclass
class InsufficientRequesterGPU(InsufficientRequester):
    request_index: int

    def __str__(self) -> str:
        return f"{self.task.name} (GPU request #{self.request_index})"


@dataclass
class InsufficientRequesterInterface(InsufficientRequester):
    request: scheduler.InterfaceRequest

    def __str__(self) -> str:
        return f"{self.task.name} (network {self.request.network})"


@dataclass
class InsufficientRequesterVolume(InsufficientRequester):
    request: scheduler.VolumeRequest

    def __str__(self) -> str:
        return f"{self.task.name} (volume {self.request.name})"


class TaskNoAgentError(InsufficientResourcesError):
    """No agent was suitable for a task.

    Where possible, a sub-class is used to indicate a more specific error.
    """

    def __init__(self, task: PhysicalTask) -> None:
        super().__init__()
        self.task = task

    def __str__(self):
        return f"No agent was found suitable for {self.task.name}"


class TaskInsufficientResourcesError(TaskNoAgentError):
    """A task required more of some resource than was available on any agent."""

    def __init__(
        self,
        requester: InsufficientRequester,
        resource: InsufficientResource,
        needed: Union[int, Decimal],
        available: Union[int, Decimal],
    ) -> None:
        super().__init__(requester.task)
        self.requester = requester
        self.resource = resource
        self.needed = needed
        self.available = available

    def __str__(self):
        return (
            f"Not enough {self.resource} for {self.requester} on any agent "
            f"({self.needed} > {self.available})"
        )


class TaskNoDeviceError(TaskNoAgentError):
    """A task required a device that was not present on any agent."""

    def __init__(self, requester: InsufficientRequester) -> None:
        super().__init__(requester.task)
        self.requester = requester

    def __str__(self):
        return f"{self.requester} could not be satisfied by any agent"


class GroupInsufficientResourcesError(InsufficientResourcesError):
    """A group of tasks collectively required more of some resource than available.

    If `requesters_desc` is provided, it is a human-readable summary of the
    requesters. If set to None, a description is generated.
    """

    def __init__(
        self,
        requesters: List[InsufficientRequester],
        requesters_desc: Optional[str],
        resource: InsufficientResource,
        needed: Union[int, Decimal],
        available: Union[int, Decimal],
    ) -> None:
        super().__init__()
        self.requesters = requesters
        if requesters_desc is None:
            self.requesters_desc = ", ".join(str(r) for r in requesters)
        else:
            self.requesters_desc = requesters_desc
        self.resource = resource
        self.needed = needed
        self.available = available

    def __str__(self):
        return (
            f"Insufficient {self.resource} to launch {self.requesters_desc} "
            f"({self.needed} > {self.available})"
        )


def _subset_resources(
    g: networkx.DiGraph, task_filter: Callable[[PhysicalTask], bool]
) -> Tuple[list, Union[int, Decimal], Union[int, Decimal]]:
    """Measure the required and available resources for a subset of graph nodes.

    Returns
    -------
    lhs
        The graph nodes corresponding to the task filter
    needed
        The resources required by the nodes in `lhs`
    available
        The resources available on all the nodes reachable from `lhs`
    """
    lhs = [node for node in g.successors("src") if task_filter(g.nodes[node]["requester"].task)]
    needed = sum(capacity for _, _, capacity in g.in_edges(lhs, data="capacity"))
    rhs = set(n for _, n in g.out_edges(lhs, data=False))
    available = sum(capacity for _, _, capacity in g.out_edges(rhs, data="capacity"))
    return lhs, needed, available


def _global_graphs(
    agents: Sequence[Agent],
    tasks: Sequence[PhysicalTask],
    agent_filter: Callable[[Agent, PhysicalTask], bool],
) -> Iterable[networkx.DiGraph]:
    # Global resources
    for r, rcls in scheduler.GLOBAL_RESOURCES.items():
        g = networkx.DiGraph(
            resource_class=rcls, resource=InsufficientResource(r, ResourceGroup.GLOBAL)
        )
        g.add_nodes_from(["src", "sink"])
        for task in tasks:
            logical_task = task.logical_node
            need = logical_task.requests[r].amount
            if need > rcls.ZERO:
                g.add_node(task, requester=InsufficientRequester(task))
                g.add_edge("src", task, capacity=need)

        if r == "cores":
            numas = [(agent, i) for agent in agents for i in range(len(agent.numa_cores))]
            for numa in numas:
                agent, numa_idx = numa
                have = agent.numa_cores[numa_idx].available
                if have > rcls.ZERO:
                    g.add_node(numa)
                    g.add_edge(numa, "sink", capacity=have)
                    for task in tasks:
                        if agent_filter(agent, task):  # TODO: use a numa_filter?
                            g.add_edge(task, numa)
        else:
            for agent in agents:
                have = agent.resources[r].available
                if have > rcls.ZERO:
                    g.add_node(agent)
                    g.add_edge(agent, "sink", capacity=have)
                    for task in tasks:
                        if agent_filter(agent, task):
                            g.add_edge(task, agent)  # Infinity capacity
        yield g


def _gpu_graphs(
    agents: Sequence[Agent],
    tasks: Sequence[PhysicalTask],
    agent_filter: Callable[[Agent, PhysicalTask], bool],
) -> Iterable[networkx.DiGraph]:
    for r, rcls in scheduler.GPU_RESOURCES.items():
        g = networkx.DiGraph(
            resource_class=rcls, resource=InsufficientResource(r, ResourceGroup.GPU)
        )
        g.add_nodes_from(["src", "sink"])
        for task in tasks:
            for i, req in enumerate(task.logical_node.gpus):
                need = req.requests[r].amount
                if need > rcls.ZERO:
                    g.add_node(req, requester=InsufficientRequesterGPU(task, i))
                    g.add_edge("src", req, capacity=need)
        for agent in agents:
            for gpu in agent.gpus:
                have = gpu.resources[r].available
                if have > rcls.ZERO:
                    g.add_node(gpu)
                    g.add_edge(gpu, "sink", capacity=have)
                    for task in tasks:
                        if agent_filter(agent, task):
                            for req in task.logical_node.gpus:
                                if req.matches(gpu, None):  # TODO: NUMA awareness
                                    g.add_edge(req, gpu)  # Infinity capacity
        yield g


def _interface_graphs(
    agents: Sequence[Agent],
    tasks: Sequence[PhysicalTask],
    agent_filter: Callable[[Agent, PhysicalTask], bool],
) -> Iterable[networkx.DiGraph]:
    # Start by identifying the networks
    networks = set()
    for task in tasks:
        for req in task.logical_node.interfaces:
            networks.add(req.network)
    for agent in agents:
        for interface in agent.interfaces:
            for network in interface.networks:
                networks.add(network)

    for r, rcls in scheduler.INTERFACE_RESOURCES.items():
        gs = {}
        for network in networks:
            g = networkx.DiGraph(
                resource_class=rcls,
                resource=InsufficientResource(r, ResourceGroup.INTERFACE, network=network),
            )
            g.add_nodes_from(["src", "sink"])
            gs[network] = g
        for task in tasks:
            for req in task.logical_node.interfaces:
                need = req.requests[r].amount
                if need > rcls.ZERO:
                    g = gs[req.network]
                    g.add_node(req, requester=InsufficientRequesterInterface(task, req))
                    g.add_edge("src", req, capacity=need)
        for agent in agents:
            for interface in agent.interfaces:
                have = interface.resources[r].available
                if have > rcls.ZERO:
                    for network in interface.networks:
                        g = gs[network]
                        g.add_node(interface)
                        g.add_edge(interface, "sink", capacity=have)
                    for task in tasks:
                        if agent_filter(agent, task):
                            for req in task.logical_node.interfaces:
                                if req.matches(interface, None):  # TODO: NUMA awareness
                                    g = gs[req.network]
                                    g.add_edge(req, interface)  # Infinity capacity
        yield from gs.values()


def _check_singletons(g: networkx.DiGraph) -> None:
    """Check whether there is a singleton that cannot be allocated anywhere."""
    resource = g.graph["resource"]
    rcls = g.graph["resource_class"]
    for (_, lhs, need) in g.out_edges("src", data="capacity"):
        have_max = rcls.ZERO
        for (_, rhs) in g.out_edges(lhs, data=False):
            have_max = max(have_max, g.edges[rhs, "sink"]["capacity"])
        if need > have_max:
            requester = g.nodes[lhs]["requester"]
            raise TaskInsufficientResourcesError(requester, resource, need, have_max)


def _check_fixed(graphs: Sequence[networkx.DiGraph], tasks: Sequence[PhysicalTask]) -> None:
    """Check some easy-to-describe sets to see if they are too big."""
    sets = []
    subsystems = {task.logical_node.subsystem for task in tasks}
    subsystems.discard(None)  # Only want specific subsystems
    for subsystem in sorted(subsystems):  # Sort just for reproducibility
        sets.append(
            (f"all {subsystem} tasks", lambda task: task.logical_node.subsystem == subsystem)
        )
    sets.append(("all tasks", lambda task: True))

    for set_name, set_filter in sets:
        for g in graphs:
            lhs, need, have = _subset_resources(g, set_filter)
            if need > have:
                resource = g.graph["resource"]
                requesters = [g.nodes[item]["requester"] for item in lhs]
                raise GroupInsufficientResourcesError(requesters, set_name, resource, need, have)


def _check_subsets(g: networkx.DiGraph) -> None:
    """Check if there is any subset of tasks that doesn't fit."""
    total_need = sum(capacity for (_, _, capacity) in g.out_edges("src", data="capacity"))
    cut_value, partition = networkx.minimum_cut(g, "src", "sink")
    if cut_value < total_need:
        # Maximum flow couldn't satisfy all the requirements. The
        # tasks in the src side of the partition are unsatisfiable.
        bad = [item for item in g.successors("src") if item in partition[0]]
        need = sum(capacity for (_, _, capacity) in g.in_edges(bad, data="capacity"))
        have = need - (total_need - cut_value)
        resource = g.graph["resource"]
        requesters = [g.nodes[item]["requester"] for item in bad]
        raise GroupInsufficientResourcesError(requesters, None, resource, need, have)


def _diagnose_insufficient_filter(
    agents: Sequence[Agent],
    tasks: Sequence[PhysicalTask],
    agent_filter: Callable[[Agent, PhysicalTask], bool],
) -> None:
    """Implement :meth:`_diagnose_insufficient` with a specific edge filter.

    This either raises a subclass of :exc:`InsufficientResourcesError`,
    or it returns if it couldn't identify a bottleneck. The `agent_filter`
    determines whether a task may be placed on a particular agent.

    The caller is responsible for setting DECIMAL_CONTEXT and excluding
    non-tasks from the nodes.
    """
    graphs: List[networkx.DiGraph] = []
    graphs.extend(_global_graphs(agents, tasks, agent_filter))
    graphs.extend(_gpu_graphs(agents, tasks, agent_filter))
    graphs.extend(_interface_graphs(agents, tasks, agent_filter))
    # TODO: volumes? Not sure if they're needed

    for g in graphs:
        _check_singletons(g)
    _check_fixed(graphs, tasks)
    for g in graphs:
        _check_subsets(g)


def _check_no_device(task: PhysicalTask, agents: Sequence[Agent]) -> None:
    """Check if a task is unrunnable due to a missing device.

    If so, it raises a :exc:`TaskNoDeviceError`. If not, it returns.
    """
    logical_task = task.logical_node
    for request in logical_task.interfaces:
        if not any(
            request.matches(interface, None) for agent in agents for interface in agent.interfaces
        ):
            raise TaskNoDeviceError(InsufficientRequesterInterface(task, request))
    for request in logical_task.volumes:
        if not any(request.matches(volume, None) for agent in agents for volume in agent.volumes):
            raise TaskNoDeviceError(InsufficientRequesterVolume(task, request))
    for i, request in enumerate(logical_task.gpus):
        if not any(request.matches(gpu, None) for agent in agents for gpu in agent.gpus):
            raise TaskNoDeviceError(InsufficientRequesterGPU(task, i))


def diagnose_insufficient(
    agents: Sequence[Agent], nodes: Sequence[scheduler.PhysicalNode]
) -> NoReturn:
    """Try to determine *why* offers are insufficient.

    This function does not return, instead raising an instance of
    :exc:`InsufficientResourcesError` or a subclass.

    Parameters
    ----------
    agents
        :class:`~.Agent`s from which allocation was attempted
    nodes
        :class:`~.PhysicalNode`s for which allocation failed. This may
        include non-tasks, which will be ignored.
    """
    with decimal.localcontext(DECIMAL_CONTEXT):
        # Non-tasks aren't relevant, so filter them out.
        tasks = [node for node in nodes if isinstance(node, PhysicalTask)]

        _diagnose_insufficient_filter(
            agents, tasks, lambda agent, task: task.logical_node.valid_agent(agent)
        )
        # Check for a task that doesn't fit anywhere
        for task in tasks:
            if not any(agent.can_allocate(task.logical_node) for agent in agents):
                _check_no_device(task, agents)
                # If _check_no_device returns, we don't have a single
                # bottleneck (a single resource bottleneck would have been
                # raised from _diagnose_insufficient_filter above).
                raise TaskNoAgentError(task)

        # Try find a resource bottleneck again, but now with a stronger filter
        _diagnose_insufficient_filter(
            agents, tasks, lambda agent, task: agent.can_allocate(task.logical_node)
        )
        # Not a simple error e.g. due to packing problems
        raise InsufficientResourcesError("Insufficient resources to launch all tasks")
