#!/usr/bin/env python

"""Sets up an agent with resources and attributes for the katsdpcontroller scheduler.

See :mod:`katsdpcontroller.scheduler` for details.
"""

import argparse
import base64
import contextlib
import glob
import json
import math
import numbers
import os
import os.path
import subprocess
import sys
import xml.etree.ElementTree
from collections import OrderedDict

import netifaces
import psutil

try:
    import py3nvml.py3nvml as pynvml
    import pycuda.driver

    pycuda.driver.init()
except ImportError:
    pynvml = None


@contextlib.contextmanager
def nvml_manager():
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


class GPU:
    def __init__(self, handle, cpu_to_node):
        node = None
        # TODO: use number of CPU cores to determine cpuset size
        # This is very hacky at the moment
        affinity = pynvml.nvmlDeviceGetCpuAffinity(handle, 1)
        n_cpus = max(cpu_to_node.keys()) + 1
        for j in range(n_cpus):
            if affinity[0] & (1 << j):
                cur_node = cpu_to_node[j]
                if node is not None and node != cur_node:
                    node = -1  # Sentinel to indicate unknown affinity
                else:
                    node = cur_node
        if node == -1:
            node = None
        self.node = node
        self.mem = pynvml.nvmlDeviceGetMemoryInfo(handle).total
        self.name = pynvml.nvmlDeviceGetName(handle)
        # NVML doesn't report compute capability, so we need CUDA
        pci_bus_id = pynvml.nvmlDeviceGetPciInfo(handle).busId
        # In Python 3 pci_bus_id is bytes but pycuda wants str
        if not isinstance(pci_bus_id, str):
            pci_bus_id = pci_bus_id.decode("ascii")
        cuda_device = pycuda.driver.Device(pci_bus_id)
        self.compute_capability = cuda_device.compute_capability()
        self.device_attributes = {}
        self.uuid = pynvml.nvmlDeviceGetUUID(handle)
        for key, value in cuda_device.get_attributes().items():
            if isinstance(value, (int, float, str)):
                # Some of the attributes use Boost.Python's enum, which is
                # derived from int but which leads to invalid JSON when passed
                # to json.dumps.
                if isinstance(value, int) and type(value) != int:
                    value = str(value)
                self.device_attributes[str(key)] = value


class HWLocParser:
    def __init__(self):
        cmd = ["lstopo", "--output-format", "xml"]
        result = subprocess.check_output(cmd).decode("ascii")
        self._tree = xml.etree.ElementTree.fromstring(result)
        self._nodes = self._tree.findall(".//object[@type='NUMANode']")
        # On single-socket machines, hwloc 1.x doesn't create NUMANode.
        if not self._nodes:
            self._nodes = [self._tree]
        self._nodes.sort(key=lambda node: int(node.get("os_index", 0)))
        # In hwloc 2.x, NUMANode is a leaf that doesn't contain the associated
        # cores and devices. We need to ascend the tree to find the container.
        parent_map = {child: parent for parent in self._tree.iter() for child in parent}
        for i in range(len(self._nodes)):
            if int(self._nodes[i].get("os_index", 0)) != i:
                raise RuntimeError("NUMA nodes are not numbered contiguously by the OS")
            while not self._nodes[i].findall(".//object[@type='PU']"):
                self._nodes[i] = parent_map[self._nodes[i]]

    def cpus_by_node(self):
        out = []
        for node in self._nodes:
            pus = node.findall(".//object[@type='PU']")
            out.append(sorted([int(pu.get("os_index")) for pu in pus]))
        return out

    def cpu_nodes(self):
        out = {}
        for i, node in enumerate(self.cpus_by_node()):
            for cpu in node:
                out[cpu] = i
        return out

    def interface_nodes(self):
        out = {}
        for i, node in enumerate(self._nodes):
            # hwloc uses type 2 for network devices
            for device in node.iterfind(".//object[@type='OSDev'][@osdev_type='2']"):
                out[device.get("name")] = i
        return out

    def gpus(self):
        out = []
        with nvml_manager():
            if not pynvml:
                return out
            cpu_to_node = self.cpu_nodes()
            n_devices = pynvml.nvmlDeviceGetCount()
            for i in range(n_devices):
                handle = pynvml.nvmlDeviceGetHandleByIndex(i)
                out.append(GPU(handle, cpu_to_node))
        return out


def infiniband_devices(interface):
    """Return a list of device paths associated with a kernel network
    interface, or an empty list if not an Infiniband device.

    This is based on
    https://github.com/amaumene/mlnx-en-dkms/blob/master/ofed_scripts/ibdev2netdev
    plus inspection of /sys.
    """
    try:
        with open(f"/sys/class/net/{interface}/device/resource") as f:
            resource = f.read()
        for ibdev in os.listdir("/sys/class/infiniband"):
            with open(f"/sys/class/infiniband/{ibdev}/device/resource") as f:
                ib_resource = f.read()
            if ib_resource == resource:
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
    return None


def collapse_ranges(values):
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


def attributes_resources(args):
    hwloc = HWLocParser()
    attributes = OrderedDict()
    resources = OrderedDict()

    interface_nodes = hwloc.interface_nodes()
    interfaces = []
    for i, network_spec in enumerate(args.networks):
        try:
            parts = network_spec.split(":")
            if not 2 <= len(parts) <= 3:
                raise ValueError
            interface, networks = parts[:2]
            networks = networks.split(",")
            if len(parts) >= 3:
                max_speed = float(parts[2]) * 1e9
            else:
                max_speed = math.inf
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
            config["numa_node"] = interface_nodes[interface]
        except KeyError:
            pass
        interfaces.append(config)
        try:
            with open(f"/sys/class/net/{interface}/speed") as f:
                speed = f.read().strip()
                # This dummy value has been observed on a NIC which had been
                # configured but had no cable attached.
                if speed == "4294967295" or float(speed) < 0:
                    raise ValueError("cable unplugged?")
                speed = float(speed) * 1e6  # /sys/class/net has speed in Mbps
        except (OSError, ValueError) as error:
            if interface == "lo":
                # Loopback interface speed is limited only by CPU power. Just
                # pick a large number - this will only be used for testing
                # anyway.
                speed = 40e9
            else:
                raise RuntimeError(f"Could not determine speed of interface {interface}: {error}")
        speed = min(speed, max_speed)
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
                numa_node = int(fields[2])
            else:
                numa_node = None
        except (ValueError, IndexError):
            raise RuntimeError(
                f"Error: --volume argument {volume_spec} does not have the format NAME:PATH"
            )
        if not os.path.exists(path):
            raise RuntimeError(f"Path {path} does not exist")
        config = {"name": name, "host_path": path}
        if numa_node is not None:
            config["numa_node"] = numa_node
        volumes.append(config)
    attributes["katsdpcontroller.volumes"] = volumes

    gpus = []
    for i, gpu in enumerate(hwloc.gpus()):
        config = {
            "name": gpu.name,
            "compute_capability": gpu.compute_capability,
            "device_attributes": gpu.device_attributes,
            "uuid": gpu.uuid,
        }
        if gpu.node is not None:
            config["numa_node"] = gpu.node
        gpus.append(config)
        resources[f"katsdpcontroller.gpu.{i}.compute"] = 1.0
        # Convert memory to MiB, for consistency with Mesos' other resources
        resources[f"katsdpcontroller.gpu.{i}.mem"] = float(gpu.mem) / 2**20
    attributes["katsdpcontroller.gpus"] = gpus

    if args.priority is not None:
        attributes["katsdpcontroller.priority"] = args.priority

    if args.subsystems:
        attributes["katsdpcontroller.subsystems"] = args.subsystems

    attributes["katsdpcontroller.numa"] = hwloc.cpus_by_node()
    resources["cores"] = collapse_ranges(hwloc.cpu_nodes().keys())
    resources["cpus"] = float(len(hwloc.cpu_nodes())) - args.reserve_cpu
    if resources["cpus"] < 0.1:
        raise RuntimeError(f"--reserve-cpu ({args.reserve_cpu}) is too high")
    resources["mem"] = psutil.virtual_memory().total // 2**20 - args.reserve_mem
    if resources["mem"] < 1024.0:
        raise RuntimeError(f"--reserve-mem ({args.reserve_mem}) is too high")
    return attributes, resources


def encode(d):
    """Encode an object in a way that can be transported via Mesos attributes: first to
    JSON, then to base64url. The JSON string is padded with spaces so that the base64
    string has no = pad characters, which are outside the legal set for Mesos.
    """
    if isinstance(d, numbers.Real) and not isinstance(d, bool):
        return repr(float(d))
    else:
        s = json.dumps(d, sort_keys=True)
        while len(s) % 3:
            s += " "
        return base64.urlsafe_b64encode(s.encode("utf-8")).decode("ascii")


def write_dict(name, path, args, d, do_encode=False):
    """For each key in `d`, write the value to `path`/`key`.

    Parameters
    ----------
    name : str
        Human-readable description of what is being written
    path : str
        Base directory (created if it does not exist)
    args : argparse.Namespace
        Command-line arguments
    d : dict
        Values to write
    do_encode : bool, optional
        If true, values are first encoded with :func:`encode`

    Returns
    -------
    changed : bool
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


def main():
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
        default="/etc/mesos-slave/attributes",
        help="Directory for attributes [%(default)s]",
    )
    parser.add_argument(
        "--resources-dir",
        default="/etc/mesos-slave/resources",
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
    parser.add_argument("--priority", type=float, help="Set agent priority for placement")
    parser.add_argument(
        "--subsystem",
        dest="subsystems",
        action="append",
        default=[],
        help="Restrict agent to a subsystem (can be repeated)",
    )
    args = parser.parse_args()

    attributes, resources = attributes_resources(args)
    changed = False
    if write_dict("attributes", args.attributes_dir, args, attributes, do_encode=True):
        changed = True
    if write_dict("resources", args.resources_dir, args, resources):
        changed = True
    if args.exit_code and not changed:
        sys.exit(100)


if __name__ == "__main__":
    main()
