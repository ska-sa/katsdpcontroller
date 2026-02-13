################################################################################
# Copyright (c) 2013-2023, National Research Foundation (SARAO)
#
# Licensed under the BSD 3-Clause License (the "License"); you may not use
# this file except in compliance with the License. You may obtain a copy
# of the License at
#
#   https://opensource.org/licenses/BSD-3-Clause
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

import pytest

from katsdpcontroller import scheduler
from katsdpcontroller.diagnose_insufficient import (
    GroupInsufficientResourcesError,
    InsufficientRequesterGPU,
    InsufficientRequesterInterface,
    InsufficientRequesterVolume,
    InsufficientResource,
    InsufficientResourcesError,
    ResourceGroup,
    TaskHostMissingError,
    TaskInsufficientResourcesError,
    TaskNoAgentError,
    TaskNoAgentErrorGeneric,
    TaskNoDeviceError,
    TaskNoValidAgentError,
    diagnose_insufficient,
)

from .utils import make_json_attr, make_offer


class TestDiagnoseInsufficient:
    """Test :class:`katsdpcontroller.diagnose_insufficient.diagnose_insufficient."""

    def _make_offer(self, resources, agent_num=0, attrs=()):
        return make_offer(
            "frameworkid", f"agentid{agent_num}", f"agenthost{agent_num}", resources, attrs
        )

    def setup_method(self):
        # Create a number of agents, each of which has a large quantity of
        # some resource but not much of others. This makes it easier to
        # control which resources are plentiful in the simulated cluster.
        numa_attr = make_json_attr("katsdpcontroller.numa", [[0, 2, 4, 6], [1, 3, 5, 7]])
        gpu_attr = make_json_attr(
            "katsdpcontroller.gpus",
            [
                {
                    "name": "Dummy GPU",
                    "compute_capability": (5, 2),
                    "numa_node": 1,
                    "uuid": "GPU-123",
                }
            ],
        )
        interface_attr = make_json_attr(
            "katsdpcontroller.interfaces",
            [
                {
                    "name": "eth0",
                    "network": "net0",
                    "ipv4_address": "192.168.1.1",
                    "infiniband_devices": ["/dev/infiniband/rdma_cm", "/dev/infiniband/uverbs0"],
                },
                {"name": "eth1", "network": "net1", "ipv4_address": "192.168.1.2"},
            ],
        )
        volume_attr = make_json_attr(
            "katsdpcontroller.volumes", [{"name": "vol0", "host_path": "/host0"}]
        )

        self.cpus_agent = scheduler.Agent([self._make_offer({"cpus": 32, "mem": 2, "disk": 7}, 0)])
        self.mem_agent = scheduler.Agent(
            [self._make_offer({"cpus": 1.25, "mem": 256, "disk": 8}, 1)]
        )
        self.disk_agent = scheduler.Agent(
            [self._make_offer({"cpus": 1.5, "mem": 3, "disk": 1024}, 2)]
        )
        self.ports_agent = scheduler.Agent(
            [self._make_offer({"cpus": 1.75, "mem": 4, "disk": 9, "ports": [(30000, 30005)]}, 3)]
        )
        self.cores_agent = scheduler.Agent(
            [self._make_offer({"cpus": 6, "mem": 5, "disk": 10, "cores": [(0, 6)]}, 4, [numa_attr])]
        )
        self.gpu_compute_agent = scheduler.Agent(
            [
                self._make_offer(
                    {
                        "cpus": 0.75,
                        "mem": 6,
                        "disk": 11,
                        "katsdpcontroller.gpu.0.compute": 1.0,
                        "katsdpcontroller.gpu.0.mem": 2.25,
                    },
                    5,
                    [numa_attr, gpu_attr],
                )
            ]
        )
        self.gpu_mem_agent = scheduler.Agent(
            [
                self._make_offer(
                    {
                        "cpus": 1.0,
                        "mem": 7,
                        "disk": 11,
                        "katsdpcontroller.gpu.0.compute": 0.125,
                        "katsdpcontroller.gpu.0.mem": 256.0,
                    },
                    6,
                    [numa_attr, gpu_attr],
                )
            ]
        )
        self.interface_agent = scheduler.Agent(
            [
                self._make_offer(
                    {
                        "cpus": 1.0,
                        "mem": 1,
                        "disk": 1,
                        "katsdpcontroller.interface.0.bandwidth_in": 1e9,
                        "katsdpcontroller.interface.0.bandwidth_out": 1e9,
                        "katsdpcontroller.interface.1.bandwidth_in": 1e9,
                        "katsdpcontroller.interface.1.bandwidth_out": 1e9,
                    },
                    7,
                    [interface_attr],
                )
            ]
        )
        self.volume_agent = scheduler.Agent(
            [self._make_offer({"cpus": 1.0, "mem": 1, "disk": 1}, 8, [volume_attr])]
        )
        # Create some template logical and physical tasks
        self.logical_tasks = [scheduler.LogicalTask(f"logical{i}") for i in range(3)]
        self.physical_tasks = [task.physical_factory(task) for task in self.logical_tasks]
        for i, task in enumerate(self.physical_tasks):
            task.name = f"physical{i}"

    def test_task_insufficient_scalar_resource(self):
        """A task requests more of a scalar resource than any agent has"""
        self.logical_tasks[0].cpus = 4
        with pytest.raises(TaskInsufficientResourcesError) as cm:
            diagnose_insufficient([self.mem_agent, self.disk_agent], [self.physical_tasks[0]])
        assert cm.value.task is self.physical_tasks[0]
        assert cm.value.resource == InsufficientResource("cpus", ResourceGroup.GLOBAL)
        assert cm.value.needed == 4
        assert cm.value.available == 1.5
        assert str(cm.value) == "Not enough cpus for physical0 on any agent (4.000 > 1.500)"

    def test_task_insufficient_range_resource(self):
        """A task requests more of a range resource than any agent has"""
        self.logical_tasks[0].ports = ["a", "b", "c"]
        with pytest.raises(TaskInsufficientResourcesError) as cm:
            diagnose_insufficient([self.mem_agent, self.disk_agent], [self.physical_tasks[0]])
        assert cm.value.task is self.physical_tasks[0]
        assert cm.value.resource == InsufficientResource("ports", ResourceGroup.GLOBAL)
        assert cm.value.needed == 3
        assert cm.value.available == 0
        assert str(cm.value) == "Not enough ports for physical0 on any agent (3 > 0)"

    def test_task_insufficient_cores(self):
        """A task requests more cores than are available on a single NUMA node"""
        self.logical_tasks[0].cores = ["a", "b", "c", "d"]
        with pytest.raises(TaskInsufficientResourcesError) as cm:
            diagnose_insufficient([self.mem_agent, self.cores_agent], [self.physical_tasks[0]])
        assert cm.value.task is self.physical_tasks[0]
        assert cm.value.resource == InsufficientResource("cores", ResourceGroup.GLOBAL)
        assert cm.value.needed == 4
        assert cm.value.available == 3
        assert str(cm.value) == "Not enough cores for physical0 on any agent (4 > 3)"

    def test_task_insufficient_gpu_scalar_resource(self):
        """A task requests more of a GPU scalar resource than any agent has"""
        req = scheduler.GPURequest()
        req.mem = 2048
        self.logical_tasks[0].gpus = [req]
        with pytest.raises(TaskInsufficientResourcesError) as cm:
            diagnose_insufficient(
                [self.mem_agent, self.gpu_compute_agent], [self.physical_tasks[0]]
            )
        assert cm.value.task is self.physical_tasks[0]
        assert cm.value.requester == InsufficientRequesterGPU(self.physical_tasks[0], 0)
        assert cm.value.resource == InsufficientResource("mem", ResourceGroup.GPU)
        assert cm.value.needed == 2048
        assert cm.value.available == 2.25
        assert (
            str(cm.value)
            == "Not enough GPU mem for physical0 (GPU request #0) on any agent (2048.000 > 2.250)"
        )

    def test_task_insufficient_interface_scalar_resources(self):
        """A task requests more of an interface scalar resource than any agent has"""
        req = scheduler.InterfaceRequest("net0")
        req.bandwidth_in = 5e9
        self.logical_tasks[0].interfaces = [req]
        with pytest.raises(TaskInsufficientResourcesError) as cm:
            diagnose_insufficient([self.mem_agent, self.interface_agent], [self.physical_tasks[0]])
        assert cm.value.task is self.physical_tasks[0]
        assert cm.value.requester == InsufficientRequesterInterface(self.physical_tasks[0], req)
        assert cm.value.resource == InsufficientResource(
            "bandwidth_in", ResourceGroup.INTERFACE, "net0"
        )
        assert cm.value.needed == 5e9
        assert cm.value.available == 1e9
        assert (
            str(cm.value) == "Not enough interface bandwidth_in on network net0 for "
            "physical0 (network net0) on any agent (5000000000.000 > 1000000000.000)"
        )

    def test_task_no_interface(self):
        """A task requests a network interface that is not available on any agent"""
        self.logical_tasks[0].interfaces = [
            scheduler.InterfaceRequest("net0"),
            scheduler.InterfaceRequest("badnet"),
        ]
        with pytest.raises(TaskNoDeviceError) as cm:
            diagnose_insufficient([self.mem_agent, self.interface_agent], [self.physical_tasks[0]])
        assert cm.value.task is self.physical_tasks[0]
        assert cm.value.requester == InsufficientRequesterInterface(
            self.physical_tasks[0], self.logical_tasks[0].interfaces[1]
        )
        assert str(cm.value) == "physical0 (network badnet) could not be satisfied by any agent"

    def test_task_no_volume(self):
        """A task requests a volume that is not available on any agent"""
        self.logical_tasks[0].volumes = [
            scheduler.VolumeRequest("vol0", "/vol0", "RW"),
            scheduler.VolumeRequest("badvol", "/badvol", "RO"),
        ]
        with pytest.raises(TaskNoDeviceError) as cm:
            diagnose_insufficient([self.mem_agent, self.volume_agent], [self.physical_tasks[0]])
        assert cm.value.task is self.physical_tasks[0]
        assert cm.value.requester == InsufficientRequesterVolume(
            self.physical_tasks[0], self.logical_tasks[0].volumes[1]
        )
        assert str(cm.value) == "physical0 (volume badvol) could not be satisfied by any agent"

    def test_task_no_gpu(self):
        """A task requests a GPU that is not available on any agent"""
        req = scheduler.GPURequest()
        req.name = "GPU that does not exist"
        self.logical_tasks[0].gpus = [req]
        with pytest.raises(TaskNoDeviceError) as cm:
            diagnose_insufficient(
                [self.mem_agent, self.gpu_compute_agent], [self.physical_tasks[0]]
            )
        assert cm.value.task == self.physical_tasks[0]
        assert cm.value.requester == InsufficientRequesterGPU(self.physical_tasks[0], 0)
        assert str(cm.value) == "physical0 (GPU request #0) could not be satisfied by any agent"

    def test_task_missing_host(self):
        """A task requests a host for which no offers were received."""
        self.logical_tasks[0].host = "spam"
        with pytest.raises(TaskHostMissingError) as cm:
            diagnose_insufficient([self.mem_agent], [self.physical_tasks[0]])
        assert cm.value.task == self.physical_tasks[0]
        assert str(cm.value) == "Task physical0 needs host spam but no offers were received for it"

    def test_task_no_valid_agents(self):
        """A task requests a subsystem but there are no agents for it."""
        self.logical_tasks[0].subsystem = "spam"
        self.mem_agent.subsystems = {"ham"}
        with pytest.raises(TaskNoValidAgentError) as cm:
            diagnose_insufficient([self.mem_agent], [self.physical_tasks[0]])
        assert cm.value.task == self.physical_tasks[0]
        assert str(cm.value) == "No valid agent for physical0"

    def test_task_no_agent(self):
        """A task does not fit on any agent, but not due to a single reason"""
        # Ask for more combined cpu+ports than is available on one agent
        self.logical_tasks[0].cpus = 8
        self.logical_tasks[0].ports = ["a", "b", "c"]
        with pytest.raises(TaskNoAgentError) as cm:
            diagnose_insufficient([self.cpus_agent, self.ports_agent], [self.physical_tasks[0]])
        assert cm.value.task is self.physical_tasks[0]
        # Make sure that it didn't incorrectly return a subclass
        assert cm.type is TaskNoAgentErrorGeneric
        assert str(cm.value) == (
            "No agent was found suitable for physical0:\n"
            "  agenthost0: Not enough ports (0 < 3)\n"
            "  agenthost3: Not enough cpus (1.750 < 8.000)"
        )

    def test_group_insufficient_scalar_resource(self):
        """A group of tasks require more of a scalar resource than available"""
        self.logical_tasks[0].cpus = 24
        self.logical_tasks[1].cpus = 16
        with pytest.raises(GroupInsufficientResourcesError) as cm:
            diagnose_insufficient([self.cpus_agent, self.mem_agent], self.physical_tasks[:2])
        assert cm.value.requesters_desc == "all tasks"
        assert cm.value.resource == InsufficientResource("cpus", ResourceGroup.GLOBAL)
        assert cm.value.needed == 40
        assert cm.value.available == 33.25
        assert str(cm.value) == "Insufficient cpus to launch all tasks (40.000 > 33.250)"

    def test_group_insufficient_range_resource(self):
        """A group of tasks require more of a range resource than available"""
        self.logical_tasks[0].ports = ["a", "b", "c"]
        self.logical_tasks[1].ports = ["d", "e", "f"]
        with pytest.raises(GroupInsufficientResourcesError) as cm:
            diagnose_insufficient([self.ports_agent], self.physical_tasks[:2])
        assert cm.value.requesters_desc == "all tasks"
        assert cm.value.resource == InsufficientResource("ports", ResourceGroup.GLOBAL)
        assert cm.value.needed == 6
        assert cm.value.available == 5
        assert str(cm.value) == "Insufficient ports to launch all tasks (6 > 5)"

    def test_group_insufficient_gpu_scalar_resources(self):
        """A group of tasks require more of a GPU scalar resource than available"""
        self.logical_tasks[0].gpus = [scheduler.GPURequest()]
        self.logical_tasks[0].gpus[-1].compute = 0.75
        self.logical_tasks[1].gpus = [scheduler.GPURequest()]
        self.logical_tasks[1].gpus[-1].compute = 0.5
        with pytest.raises(GroupInsufficientResourcesError) as cm:
            diagnose_insufficient(
                [self.gpu_compute_agent, self.gpu_mem_agent],
                self.physical_tasks[:2],
            )
        assert cm.value.requesters_desc == "all tasks"
        assert cm.value.resource == InsufficientResource("compute", ResourceGroup.GPU)
        assert cm.value.needed == 1.25
        assert cm.value.available == 1.125
        assert str(cm.value) == "Insufficient GPU compute to launch all tasks (1.250 > 1.125)"

    def test_group_insufficient_interface_scalar_resources(self):
        """A group of tasks require more of a network resource than available"""
        self.logical_tasks[0].interfaces = [
            scheduler.InterfaceRequest("net0"),
            scheduler.InterfaceRequest("net1"),
        ]
        self.logical_tasks[0].interfaces[0].bandwidth_in = 800e6
        # An amount that must not be added to the needed value reported
        self.logical_tasks[0].interfaces[1].bandwidth_in = 50e6
        self.logical_tasks[1].interfaces = [scheduler.InterfaceRequest("net0")]
        self.logical_tasks[1].interfaces[0].bandwidth_in = 700e6
        with pytest.raises(GroupInsufficientResourcesError) as cm:
            diagnose_insufficient([self.interface_agent], self.physical_tasks[:2])
        assert cm.value.requesters_desc == "all tasks"
        assert cm.value.resource == InsufficientResource(
            "bandwidth_in", ResourceGroup.INTERFACE, "net0"
        )
        assert cm.value.needed == 1500e6
        assert cm.value.available == 1000e6
        assert (
            str(cm.value) == "Insufficient interface bandwidth_in on network net0 "
            "to launch all tasks (1500000000.000 > 1000000000.000)"
        )

    def test_subsystem(self):
        """A subsystem has insufficient resources, although the system has enough."""
        self.logical_tasks[0].subsystem = "sdp"
        self.logical_tasks[0].cpus = 32  # Consumes all of cpus_agent
        self.logical_tasks[1].subsystem = "sdp"
        self.logical_tasks[1].cpus = 1
        self.cpus_agent.subsystems = {"sdp"}
        self.cores_agent.subsystems = {"cbf", "other"}
        with pytest.raises(GroupInsufficientResourcesError) as cm:
            diagnose_insufficient([self.cpus_agent, self.cores_agent], self.physical_tasks[:2])
        assert cm.value.requesters_desc == "all sdp tasks"
        assert cm.value.resource == InsufficientResource("cpus", ResourceGroup.GLOBAL)
        assert cm.value.needed == 33
        assert cm.value.available == 32
        assert str(cm.value) == "Insufficient cpus to launch all sdp tasks (33.000 > 32.000)"

    def test_subset(self):
        """A subset of the tasks has insufficient resources, although the system has enough."""
        # Tasks 1 and 2 can each only run on cpus_agent, but can't both fit
        self.logical_tasks[0].cpus = 17
        self.logical_tasks[1].cpus = 17
        self.logical_tasks[2].cpus = 1
        with pytest.raises(GroupInsufficientResourcesError) as cm:
            diagnose_insufficient([self.cpus_agent, self.cores_agent], self.physical_tasks[:3])
        assert cm.value.requesters_desc == "physical0, physical1"
        assert cm.value.resource == InsufficientResource("cpus", ResourceGroup.GLOBAL)
        assert cm.value.needed == 34
        assert cm.value.available == 32
        assert str(cm.value) == "Insufficient cpus to launch physical0, physical1 (34.000 > 32.000)"

    def test_generic(self):
        """A group of tasks can't fit, but no simpler explanation is available"""
        # Create a task that uses just too much memory for the
        # low-memory agents, forcing it to consume memory from the
        # big-memory agent and not leaving enough for the big-memory task.
        self.logical_tasks[0].mem = 5
        self.logical_tasks[1].mem = 251
        with pytest.raises(InsufficientResourcesError) as cm:
            diagnose_insufficient(
                [self.cpus_agent, self.mem_agent, self.disk_agent],
                self.physical_tasks[:2],
            )
        # Check that it wasn't a subclass raised
        assert cm.type == InsufficientResourcesError
        assert str(cm.value) == "Insufficient resources to launch all tasks"
