#!/usr/bin/env python

from __future__ import print_function, division, absolute_import
import argparse
import subprocess
import contextlib
import os
import os.path
import sys
import base64
import json
from collections import OrderedDict
import xml.etree.ElementTree
import netifaces
import psutil
import six
try:
    import pynvml
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
            print('Warning:', error, file=sys.stderr)
            pynvml = None
    yield pynvml
    if pynvml:
        pynvml.nvmlShutdown()


class GPU(object):
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
                    node = -1    # Sentinel to indicate unknown affinity
                else:
                    node = cur_node
        if node == -1:
            node = None
        self.node = node
        self.mem = pynvml.nvmlDeviceGetMemoryInfo(handle).total
        self.minor = pynvml.nvmlDeviceGetMinorNumber(handle)
        self.name = pynvml.nvmlDeviceGetName(handle)
        self.driver_version = pynvml.nvmlSystemGetDriverVersion()
        # NVML doesn't report compute capability, so we need CUDA
        pci_bus_id = pynvml.nvmlDeviceGetPciInfo(handle).busId
        cuda_device = pycuda.driver.Device(pci_bus_id)
        self.compute_capability = cuda_device.compute_capability()
        self.device_attributes = {}
        for key, value in six.iteritems(cuda_device.get_attributes()):
            if isinstance(value, (six.integer_types, six.string_types, float)):
                # Some of the attributes use Boost.Python's enum, which is
                # derived from int but which leads to invalid JSON when passed
                # to json.dumps.
                if isinstance(value, int) and type(value) != int:
                    value = str(value)
                self.device_attributes[str(key)] = value


class HWLocParser(object):
    def __init__(self):
        cmd = ['lstopo', '--output-format', 'xml']
        result = subprocess.check_output(cmd).decode('ascii')
        self._tree = xml.etree.ElementTree.fromstring(result)
        self._nodes = self._tree.findall(".//object[@type='NUMANode']")
        if not self._nodes:
            # hwloc doesn't create NUMANode for a single-socket machine
            self._nodes = [self._tree]

    def cpus_by_node(self):
        out = []
        for node in self._nodes:
            pus = node.findall(".//object[@type='PU']")
            out.append(sorted([int(pu.get('os_index')) for pu in pus]))
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
                out[device.get('name')] = i
        return out

    def gpus(self):
        out = []
        with nvml_manager():
            if not pynvml:
                return out
            driver_version = pynvml.nvmlSystemGetDriverVersion()
            cpu_to_node = self.cpu_nodes()
            n_cpus = max(cpu_to_node.keys()) + 1
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
        with open('/sys/class/net/{}/device/resource'.format(interface)) as f:
            resource = f.read()
        for ibdev in os.listdir('/sys/class/infiniband'):
            with open('/sys/class/infiniband/{}/device/resource'.format(ibdev)) as f:
                ib_resource = f.read()
            if ib_resource == resource:
                # Found the matching device. Identify device inodes
                devices = ['/dev/infiniband/rdma_cm']
                for sub in ['infiniband_cm', 'infiniband_mad', 'infiniband_verbs']:
                    path = '/sys/class/infiniband/{}/device/{}'.format(ibdev, sub)
                    for item in os.listdir(path):
                        device = '/dev/infiniband/' + item
                        if os.path.exists(device):
                            devices.append(device)
                return devices
    except IOError:
        return []


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
            out.append('{}-{}'.format(values[pos], values[stop - 1]))
        pos = stop
    return '[' + ','.join(out) + ']'


def attributes_resources(args):
    hwloc = HWLocParser()
    attributes = OrderedDict()
    resources = OrderedDict()

    interface_nodes = hwloc.interface_nodes()
    interfaces = []
    for network_spec in args.networks:
        try:
            interface, network = network_spec.split(':', 1)
        except ValueError:
            raise RuntimeError(
                'Error: --network argument {} does not have the format INTERFACE:NETWORK'
                .format(network_spec))
        config = {'name': interface, 'network': network,
                  'infiniband_devices': infiniband_devices(interface)}
        try:
            config['ipv4_address'] = netifaces.ifaddresses(interface)[netifaces.AF_INET][0]['addr']
        except (KeyError, IndexError):
            raise RuntimeError('Could not obtain IPv4 address for interface {}'.format(interface))
        try:
            with open('/sys/class/net/{}/speed'.format(interface)) as f:
                config['speed'] = float(f.read().strip()) * 1e6  # /sys/class/net has speed in Mbps
        except (IOError, OSError, ValueError):
            pass
        try:
            config['numa_node'] = interface_nodes[interface]
        except KeyError:
            pass
        interfaces.append(config)
    attributes['katsdpcontroller.interfaces'] = interfaces

    volumes = []
    for volume_spec in args.volumes:
        try:
            fields = volume_spec.split(':', 2)
            name = fields[0]
            path = fields[1]
            if len(fields) >= 3:
                numa_node = int(fields[2])
            else:
                numa_node = None
        except ValueError, IndexError:
            raise RuntimeError(
                'Error: --volume argument {} does not have the format NAME:PATH'
                .format(volume_spec))
        if not os.path.exists(path):
            raise RuntimeError('Path {} does not exist'.format(path))
        config = {'name': name, 'host_path': path}
        if numa_node is not None:
            config['numa_node'] = numa_node
        volumes.append(config)
    attributes['katsdpcontroller.volumes'] = volumes

    gpus = []
    for i, gpu in enumerate(hwloc.gpus()):
        config = {
            'device': '/dev/nvidia{}'.format(gpu.minor),
            'driver_version': gpu.driver_version,
            'name': gpu.name,
            'compute_capability': gpu.compute_capability,
            'device_attributes': gpu.device_attributes
        }
        if gpu.node is not None:
            config['numa_node'] = gpu.node
        gpus.append(config)
        resources['katsdpcontroller.gpu.{}.compute'.format(i)] = 1.0
        # Convert memory to MiB, for consistency with Mesos' other resources
        resources['katsdpcontroller.gpu.{}.mem'.format(i)] = float(gpu.mem) / 2**20
    attributes['katsdpcontroller.gpus'] = gpus

    attributes['katsdpcontroller.numa'] = hwloc.cpus_by_node()
    resources['cores'] = collapse_ranges(hwloc.cpu_nodes().keys())
    # Mesos sees "cpus" and "mem" in our custom resource names, and skips the
    # automatic detection. We have to recreate its logic.
    resources['cpus'] = len(hwloc.cpu_nodes())
    resources['mem'] = (psutil.virtual_memory().total - 2**30) // 2**20
    return attributes, resources


def encode(d):
    """Encode an object in a way that can be transported via Mesos attributes: first to
    JSON, then to base64url. The JSON string is padded with spaces so that the base64
    string has no = pad characters, which are outside the legal set for Mesos.
    """
    s = json.dumps(d)
    while len(s) % 3:
        s += ' '
    return base64.urlsafe_b64encode(s)


def write_dict(name, path, args, d, do_encode=False):
    if args.dry_run:
        print(name + ':')
        if do_encode:
            converted = json.loads(json.dumps(d))
        else:
            converted = d
        for key, value in six.iteritems(converted):
            print('    {}:{}'.format(key, value))
    else:
        try:
            os.makedirs(path)
        except OSError:
            if not os.path.exists(path):
                raise
        for key, value in d.iteritems():
            with open(os.path.join(path, key), 'w') as f:
                print(encode(value) if do_encode else value, file=f)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--dry-run', action='store_true', help='Report what would be done, without doing it')
    parser.add_argument('--attributes-dir', default='/etc/mesos-slave/attributes', help='Directory for attributes [%(default)s]')
    parser.add_argument('--resources-dir', default='/etc/mesos-slave/resources', help='Directory for resources [%(default)s]')
    parser.add_argument('--network', dest='networks', action='append', default=[],
                        metavar='INTERFACE:NETWORK',
                        help='Map network interface to a logical network')
    parser.add_argument('--volume', dest='volumes', action='append', default=[],
                        metavar='NAME:PATH[:NUMA]',
                        help='Map host directory to a logical volume name')
    args = parser.parse_args()

    attributes, resources = attributes_resources(args)
    write_dict('attributes', args.attributes_dir, args, attributes, do_encode=True)
    write_dict('resources', args.resources_dir, args, resources)


if __name__ == '__main__':
    main()
