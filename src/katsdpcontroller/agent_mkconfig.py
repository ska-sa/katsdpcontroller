#!/usr/bin/env python

################################################################################
# Copyright (c) 2013-2024, National Research Foundation (SARAO)
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

"""Sets up an agent with resources and attributes for the katsdpcontroller scheduler.

See :mod:`katsdpcontroller.scheduler` for details.
"""

import argparse
import base64
import contextlib
import enum
import glob
import json
import numbers
import os
import os.path
import subprocess
import sys
import xml.etree.ElementTree
from typing import Any, Dict, Generator, Iterable, List, Mapping, Optional, Tuple
from xml.etree.ElementTree import Element

import netifaces
import psutil

try:
    import pynvml
except ImportError:
    pynvml = None


class DomainType(enum.Enum):
    NUMA = 0
    L3 = 1


def _is_power_two(x: int) -> bool:
    return x > 0 and (x & (x - 1)) == 0


def attr_get(elem: Element, attr: str) -> str:
    """Get an attribute from an XML element, raising KeyError if not found."""
    value = elem.get(attr)
    if value is None:
        raise KeyError(f"Attribute {attr} not found")
    return value


@contextlib.contextmanager
def nvml_manager() -> Generator[Any, None, None]:
    """Context manager to initialise and shut down NVML."""
    global pynvml
    if pynvml:
        try:
            pynvml.nvmlInit()
        except pynvml.NVMLError as error:
            print("Warning:", error, file=sys.stderr)
            pynvml = None
    yield pynvml
    if pynvml:
        pynvml.nvmlShutdown()


class NUMANode:
    """Encapsulates a NUMA node.

    Parameters
    ----------
    element
        The XML NUMANode element, or the XML root if there are no NUMANode elements
    root
        The highest ancestor of `element` that does not contain other NUMANode elements
    """

    def __init__(self, element: Element, root: Element) -> None:
        self.element = element
        self.root = root
        self.pu_indices = _pu_indices(root)
        self.os_index = int(element.get("os_index", 0))
        self.domains: List[int] = []  # Indices of domains corresponding to this NUMA node


class GPU:
    def __init__(self, handle: "pynvml.c_nvmlDevice_t", nodes: Mapping[int, NUMANode]) -> None:
        # TODO: use number of NUMA nodes to determine nodeset size
        # This is very hacky at the moment (assumes no more than 64
        # NUMA nodes).
        affinity = int(
            pynvml.nvmlDeviceGetMemoryAffinity(handle, 1, pynvml.NVML_AFFINITY_SCOPE_NODE)[0]
        )
        if _is_power_two(affinity):
            self.node: Optional[NUMANode] = nodes[affinity.bit_length() - 1]
            # TODO: extend the model to allow for multiple domains
            self.domain: Optional[int] = self.node.domains[0]
        else:
            self.node = None
            self.domain = None
        self.mem = pynvml.nvmlDeviceGetMemoryInfo(handle).total
        self.name = pynvml.nvmlDeviceGetName(handle)
        # NVML doesn't report compute capability, so we need CUDA
        pci_bus_id = pynvml.nvmlDeviceGetPciInfo(handle).busId
        if not isinstance(pci_bus_id, str):
            pci_bus_id = pci_bus_id.decode("ascii")
        self.compute_capability = pynvml.pynvml.nvmlDeviceGetCudaComputeCapability(handle)
        self.device_attributes = {}
        self.uuid = pynvml.nvmlDeviceGetUUID(handle)

def _descendant_or_self_elements(element: Element, predicate: str) -> List[Element]:
    """Find all descendants (including the element) matching an XPath predicate."""
    # ElementTree only implements a small subset of XPath, so we can't use
    # expressions like ./descendant-or-self::object[...].
    out = element.findall(f".//object{predicate}")
    if element.tag == "object":
        out.extend(element.findall(f".{predicate}"))
    return out


def _pu_elements(element: Element) -> List[Element]:
    """Return all the processing units under `element`."""
    return _descendant_or_self_elements(element, "[@type='PU']")


def _pu_indices(element: Element) -> List[int]:
    """Return OS indices of all processing units under `element`."""
    return sorted(int(attr_get(pu, "os_index")) for pu in _pu_elements(element))


def _l3_elements(element: Element) -> List[Element]:
    """Return all the L3 caches under `element`."""
    return _descendant_or_self_elements(element, "[@type='L3Cache']")


def _numa_node_elements(element: Element) -> List[Element]:
    """Return all the NUMA nodes under `element`."""
    return _descendant_or_self_elements(element, "[@type='NUMANode']")


def _numa_nodes(tree: Element) -> Mapping[int, NUMANode]:
    """Retrieve all NUMA nodes from the system.

    The :attr`NUMANode.domains` field is not populated.
    """
    node_elements = _numa_node_elements(tree)
    node_elements.sort(key=lambda element: int(attr_get(element, "os_index")))
    # On single-socket machines, hwloc 1.x doesn't create NUMANode.
    if not node_elements:
        node_elements = [tree]

    nodes: Dict[int, NUMANode] = {}  # NUMA nodes, indexed by os_index
    # In hwloc 2.x, NUMANode is a leaf that doesn't contain the associated
    # cores and devices. We need to ascend the tree to find the top-level
    # container.
    parent_map = {child: parent for parent in tree.iter() for child in parent}
    for element in node_elements:
        root = element
        while root in parent_map and len(_numa_node_elements(parent_map[root])) <= 1:
            root = parent_map[root]
        node = NUMANode(element, root)
        nodes[node.os_index] = node
    return nodes


class HWLocParser:
    """Parse XML output from lstopo.

    The CPUs and devices are associated with "domains". Normally a "domain"
    is the same as a NUMA node. However, command-line options can change
    the interpretation.

    Parameters
    ----------
    domain_type
        Control what domains correspond to.

        If set to L3, domains will correspond to L3 caches rather than
        NUMA nodes. This will raise an exception if the L3 caches do not
        partition the CPU set of each NUMA node or if there are NUMA nodes
        without L3 caches. Hardware devices will be associated with the first
        domain of each NUMA node.
    """

    def __init__(self, domain_type: DomainType) -> None:
        cmd = ["lstopo", "--output-format", "xml"]
        result = subprocess.check_output(cmd).decode("ascii")
        self._tree = xml.etree.ElementTree.fromstring(result)
        self._nodes = _numa_nodes(self._tree)

        # Each domain is a list of CPU indices
        self._domains: List[List[int]] = []
        if domain_type == DomainType.NUMA:
            for node in self._nodes.values():
                node.domains = [len(self._domains)]
                self._domains.append(node.pu_indices)
        elif domain_type == DomainType.L3:
            for node in self._nodes.values():
                node_cpuset = set(node.pu_indices)  # We'll remove from this as we go
                orig_node_cpuset = frozenset(node_cpuset)
                for l3 in _l3_elements(node.root):
                    l3_cpuset = set(_pu_indices(l3))
                    if not l3_cpuset.issubset(orig_node_cpuset):
                        raise RuntimeError("L3 cache PUs not a subset of the NUMA node's")
                    if not l3_cpuset.issubset(node_cpuset):
                        raise RuntimeError("L3 caches contain overlapping PUs")
                    node_cpuset -= l3_cpuset
                    node.domains.append(len(self._domains))
                    self._domains.append(sorted(l3_cpuset))
                if node_cpuset:
                    raise RuntimeError("NUMA node contains PUs not covered by any L3 cache")
        else:
            raise RuntimeError(f"Unhandled domain type {domain_type}")

    def cpus_by_domain(self) -> List[List[int]]:
        return self._domains

    def cpu_domains(self) -> Mapping[int, int]:
        out = {}
        for i, domain in enumerate(self._domains):
            for cpu in domain:
                out[cpu] = i
        return out

    def interface_domains(self) -> Mapping[str, int]:
        out = {}
        for node in self._nodes.values():
            # TODO: need to change the module to allow for multiple domains
            domain = node.domains[0]
            # hwloc uses type 2 for network devices
            for device in node.root.iterfind(".//object[@type='OSDev'][@osdev_type='2']"):
                out[attr_get(device, "name")] = domain
        return out

    def gpus(self) -> List[GPU]:
        out: List[GPU] = []
        with nvml_manager():
            if not pynvml:
                return out
            n_devices = pynvml.nvmlDeviceGetCount()
            for i in range(n_devices):
                handle = pynvml.nvmlDeviceGetHandleByIndex(i)
                out.append(GPU(handle, self._nodes))
        return out


def infiniband_devices(interface: str) -> List[str]:
    """Return a list of device paths associated with a kernel network interface.

    Return an empty list if `interface` is not an Infiniband device.

    This is based on ibdev2netdev (installed with MLNX OFED) plus inspection of
    /sys.
    """
    try:
        for ibdev in os.listdir("/sys/class/infiniband"):
            for port in os.listdir(f"/sys/class/infiniband/{ibdev}/ports"):
                with open(f"/sys/class/infiniband/{ibdev}/ports/{port}/gid_attrs/ndevs/0") as f:
                    ndev = f.read().strip()
                if ndev == interface:
                    # Found the matching device. Identify device inodes
                    devices = ["/dev/infiniband/rdma_cm"]
                    for sub in ["infiniband_cm", "infiniband_mad", "infiniband_verbs"]:
                        path = f"/sys/class/infiniband/{ibdev}/device/{sub}"
                        if os.path.exists(path):
                            for item in os.listdir(path):
                                device = "/dev/infiniband/" + item
                                if os.path.exists(device):
                                    devices.append(device)
                    return devices
    except OSError:
        pass
    return []


def collapse_ranges(values: Iterable[int]) -> str:
    values = sorted(values)
    out = []
    pos = 0
    while pos < len(values):
        stop = pos + 1
        while stop < len(values) and values[stop] == values[pos] + (stop - pos):
            stop += 1
        if stop == pos + 1:
            out.append(str(values[pos]))
        else:
            out.append(f"{values[pos]}-{values[stop - 1]}")
        pos = stop
    return "[" + ",".join(out) + "]"


def attributes_resources(args: argparse.Namespace) -> Tuple[Mapping[str, Any], Mapping[str, Any]]:
    hwloc = HWLocParser(DomainType[args.domains.upper()])
    attributes: Dict[str, Any] = {}
    resources: Dict[str, Any] = {}

    interface_domains = hwloc.interface_domains()
    interfaces = []
    for i, network_spec in enumerate(args.networks):
        try:
            parts = network_spec.split(":")
            if not 2 <= len(parts) <= 3:
                raise ValueError
            interface, networks = parts[:2]
            networks = networks.split(",")
            if len(parts) >= 3:
                speed = float(parts[2]) * 1e9
            else:
                speed = None
        except ValueError:
            raise RuntimeError(
                f"Error: --network argument {network_spec} does not have the format "
                "INTERFACE:NETWORK[,NETWORK...][:MAX-GBPS]"
            )
        if len(networks) == 1:
            # Older versions of katsdpcontroller only supported a single
            # string. Use that format where possible for maximum
            # compatibility.
            networks = networks[0]
        config = {"name": interface, "network": networks}
        ibdevs = infiniband_devices(interface)
        if ibdevs:
            config["infiniband_devices"] = ibdevs
            try:
                with open(f"/sys/class/net/{interface}/settings/force_local_lb_disable") as f:
                    line = f.read().strip()
                    if line == "Force local loopback disable is ON":
                        config["infiniband_multicast_loopback"] = False
                    elif line == "Force local loopback disable is OFF":
                        # Don't set it to True, because that's the default, and
                        # having it explicit would break older versions of
                        # katsdpcontroller that don't expect it.
                        pass
                    else:
                        raise RuntimeError(
                            f"Could not parse force_local_lb_disable for {interface}"
                        )
            except OSError:
                pass  # Ignore if the driver doesn't provide the setting
        try:
            config["ipv4_address"] = netifaces.ifaddresses(interface)[netifaces.AF_INET][0]["addr"]
        except (KeyError, IndexError):
            raise RuntimeError(f"Could not obtain IPv4 address for interface {interface}")
        try:
            config["numa_node"] = interface_domains[interface]
        except KeyError:
            pass
        interfaces.append(config)
        if speed is None:
            try:
                with open(f"/sys/class/net/{interface}/speed") as f:
                    speed_str = f.read().strip()
                    # This dummy value has been observed on a NIC which had been
                    # configured but had no cable attached.
                    if speed_str == "4294967295" or float(speed_str) < 0:
                        raise ValueError("cable unplugged?")
                    speed = float(speed_str) * 1e6  # /sys/class/net has speed in Mbps
            except (OSError, ValueError) as error:
                if interface == "lo":
                    # Loopback interface speed is limited only by CPU power. Just
                    # pick a large number - this will only be used for testing
                    # anyway.
                    speed = 100e9
                else:
                    raise RuntimeError(
                        f"Could not determine speed of interface {interface}: {error}"
                    )
        resources[f"katsdpcontroller.interface.{i}.bandwidth_in"] = speed
        resources[f"katsdpcontroller.interface.{i}.bandwidth_out"] = speed
    attributes["katsdpcontroller.interfaces"] = interfaces
    attributes["katsdpcontroller.infiniband_devices"] = glob.glob("/dev/infiniband/*")

    volumes = []
    for volume_spec in args.volumes:
        try:
            fields = volume_spec.split(":", 2)
            name = fields[0]
            path = fields[1]
            if len(fields) >= 3:
                domain = int(fields[2])
            else:
                domain = None
        except (ValueError, IndexError):
            raise RuntimeError(
                f"Error: --volume argument {volume_spec} "
                "does not have the format NAME:PATH[:DOMAIN]"
            )
        if not os.path.exists(path):
            raise RuntimeError(f"Path {path} does not exist")
        config = {"name": name, "host_path": path}
        if domain is not None:
            config["numa_node"] = domain
        volumes.append(config)
    attributes["katsdpcontroller.volumes"] = volumes

    gpus = []
    if not args.ignore_gpus:
        for i, gpu in enumerate(hwloc.gpus()):
            config = {
                "name": gpu.name,
                "compute_capability": gpu.compute_capability,
                "device_attributes": gpu.device_attributes,
                "uuid": gpu.uuid,
            }
            if gpu.domain is not None:
                config["numa_node"] = gpu.domain
            gpus.append(config)
            resources[f"katsdpcontroller.gpu.{i}.compute"] = 1.0
            # Convert memory to MiB, for consistency with Mesos' other resources
            resources[f"katsdpcontroller.gpu.{i}.mem"] = float(gpu.mem) / 2**20
    attributes["katsdpcontroller.gpus"] = gpus

    if args.priority is not None:
        attributes["katsdpcontroller.priority"] = args.priority

    if args.subsystems:
        attributes["katsdpcontroller.subsystems"] = args.subsystems

    attributes["katsdpcontroller.numa"] = hwloc.cpus_by_domain()
    resources["cores"] = collapse_ranges(hwloc.cpu_domains().keys())
    resources["cpus"] = float(len(hwloc.cpu_domains())) - args.reserve_cpu
    if resources["cpus"] < 0.1:
        raise RuntimeError(f"--reserve-cpu ({args.reserve_cpu}) is too high")
    resources["mem"] = psutil.virtual_memory().total // 2**20 - args.reserve_mem
    if resources["mem"] < 1024.0:
        raise RuntimeError(f"--reserve-mem ({args.reserve_mem}) is too high")
    return attributes, resources


def encode(d: Any) -> str:
    """Encode an object in a way that can be transported via Mesos attributes.

    The item is first encoded to JSON, then to base64url. The JSON string is
    padded with spaces so that the base64 string has no = pad characters, which
    are outside the legal set for Mesos.

    As a special case, real numbers are returned as strings.
    """
    if isinstance(d, numbers.Real) and not isinstance(d, bool):
        return repr(float(d))
    else:
        s = json.dumps(d, sort_keys=True)
        while len(s) % 3:
            s += " "
        return base64.urlsafe_b64encode(s.encode("utf-8")).decode("ascii")


def write_dict(
    name: str, path: str, args: argparse.Namespace, d: Mapping[str, Any], do_encode: bool = False
) -> bool:
    """For each key in `d`, write the value to `path`/`key`.

    Parameters
    ----------
    name
        Human-readable description of what is being written
    path
        Base directory (created if it does not exist)
    args
        Command-line arguments
    d
        Values to write
    do_encode
        If true, values are first encoded with :func:`encode`

    Returns
    -------
    changed
        Whether any files were modified
    """
    changed = False

    if args.dry_run:
        print(name + ":")
        if do_encode:
            converted = json.loads(json.dumps(d))
        else:
            converted = d
        for key, value in converted.items():
            print(f"    {key}:{value}")
    else:
        try:
            os.makedirs(path)
        except OSError:
            # makedirs raises an error if the path already exists.
            if not os.path.exists(path):
                raise
        else:
            # The path didn't exist
            changed = True
        for key, value in d.items():
            filename = os.path.join(path, key)
            content = encode(value) if do_encode else value
            content = f"{content}\n"
            try:
                with open(filename) as f:
                    orig_content = f.read()
            except OSError:
                orig_content = None
            if content != orig_content:
                with open(os.path.join(path, key), "w") as f:
                    f.write(content)
                changed = True
        # Delete any katsdpcontroller-specific entries that we didn't
        # ask for (e.g. because a GPU was removed).
        for item in os.listdir(path):
            if item.startswith("katsdpcontroller.") and item not in d:
                os.remove(os.path.join(path, item))
                changed = True
    return changed


def parse_args(argv: List[str]) -> argparse.Namespace:
    if os.path.isdir("/etc/mesos-agent"):
        attributes_dir = "/etc/mesos-agent/attributes"
        resources_dir = "/etc/mesos-agent/resources"
    else:
        attributes_dir = "/etc/mesos-slave/attributes"
        resources_dir = "/etc/mesos-slave/resources"
    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--dry-run", action="store_true", help="Report what would be done, without doing it"
    )
    group.add_argument(
        "--exit-code", action="store_true", help="Use exit code 100 if there are no changes"
    )
    parser.add_argument(
        "--attributes-dir",
        default=attributes_dir,
        help="Directory for attributes [%(default)s]",
    )
    parser.add_argument(
        "--resources-dir",
        default=resources_dir,
        help="Directory for resources [%(default)s]",
    )
    parser.add_argument(
        "--reserve-cpu",
        type=float,
        default=0.0,
        help="Amount of CPU resource to exclude from Mesos [%(default)s]",
    )
    parser.add_argument(
        "--reserve-mem",
        type=float,
        default=1024.0,
        help="MiB of memory resource to exclude from Mesos [%(default)s]",
    )
    parser.add_argument(
        "--network",
        dest="networks",
        action="append",
        default=[],
        metavar="INTERFACE:NETWORK[,NETWORK...][:MAX-GBPS]",
        help="Map network interface to a logical network",
    )
    parser.add_argument(
        "--volume",
        dest="volumes",
        action="append",
        default=[],
        metavar="NAME:PATH[:NUMA]",
        help="Map host directory to a logical volume name",
    )
    parser.add_argument(
        "--domains",
        choices=[value.name.lower() for value in DomainType],
        default="numa",
        help="Domains by which to partition hardware [%(default)s]",
    )
    parser.add_argument("--priority", type=float, help="Set agent priority for placement")
    parser.add_argument(
        "--subsystem",
        dest="subsystems",
        action="append",
        default=[],
        help="Restrict agent to a subsystem (can be repeated)",
    )
    parser.add_argument(
        "--ignore-gpus",
        action="store_true",
        help="Don't make GPUs in this machine available for Mesos tasks",
    )
    return parser.parse_args(argv)


def main(argv: List[str]) -> None:
    args = parse_args(argv)
    attributes, resources = attributes_resources(args)
    changed = False
    if write_dict("attributes", args.attributes_dir, args, attributes, do_encode=True):
        changed = True
    if write_dict("resources", args.resources_dir, args, resources):
        changed = True
    if args.exit_code and not changed:
        sys.exit(100)


if __name__ == "__main__":
    main(sys.argv[1:])
