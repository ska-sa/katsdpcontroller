"""Tests for agent_mkconfig script."""

import argparse
import base64
import json
import pathlib
import re
from dataclasses import dataclass
from typing import List, Optional

import netifaces
import pytest

from katsdpcontroller import agent_mkconfig


def mock_gpu(mocker) -> None:
    pynvml = mocker.patch("katsdpcontroller.agent_mkconfig.pynvml")
    pynvml.nvmlDeviceGetCount.return_value = 1
    pynvml.nvmlDeviceGetCpuAffinity.return_value = [0xF0]
    pynvml.nvmlDeviceGetMemoryInfo.return_value.total = 8589934592
    pynvml.nvmlDeviceGetName.return_value = "NVIDIA GeForce RTX 3070 Ti"
    pynvml.nvmlDeviceGetPciInfo.return_value.busId = b"0000:81:00.0"
    pynvml.nvmlDeviceGetUUID.return_value = "GPU-2854ce83-bd19-0424-de81-c437df8f47a2"

    pycuda = mocker.patch("katsdpcontroller.agent_mkconfig.pycuda")
    device = pycuda.driver.Device.return_value
    device.compute_capability.return_value = (8, 6)
    device.get_attributes.return_value = {}


@dataclass
class FakeNic:
    ifname: str
    ipv4_address: str
    speed: int  # Mb/s
    #: Device number on /dev/infiniband/uverbsN etc
    infiniband_index: Optional[int] = None
    loopback_disable: bool = False


def mock_nics(mocker, fs, nics: List[FakeNic]) -> None:
    def ifaddresses(ifname: str) -> dict:
        for nic in nics:
            if nic.ifname == ifname:
                return {netifaces.AF_INET: [{"addr": nic.ipv4_address}]}
        raise ValueError("You must specify a valid interface name")

    mocker.patch("netifaces.ifaddresses", side_effect=ifaddresses)
    for nic in nics:
        fs.create_file(f"/sys/class/net/{nic.ifname}/speed", contents=f"{nic.speed}\n")
        if nic.infiniband_index is not None:
            ibdev = f"mlx5_{nic.infiniband_index}"
            uverbs = f"uverbs{nic.infiniband_index}"
            fs.create_file(
                f"/sys/class/infiniband/{ibdev}/ports/1/gid_attrs/ndevs/0",
                contents=f"{nic.ifname}\n",
            )
            fs.create_dir(f"/sys/class/infiniband/{ibdev}/device/infiniband_verbs/{uverbs}")
            fs.create_file(f"/dev/infiniband/{uverbs}")
            fs.create_file(
                f"/sys/class/net/{nic.ifname}/settings/force_local_lb_disable",
                contents=(
                    f"Force local loopback disable is {'ON' if nic.loopback_disable else 'OFF'}\n"
                ),
            )


def mock_lstopo(mocker, basename: str) -> None:
    lstopo_bytes = (pathlib.Path(__file__).parent / basename).read_bytes()
    mocker.patch("subprocess.check_output", return_value=lstopo_bytes)


def mock_mem(mocker) -> None:
    vm = mocker.patch("psutil.virtual_memory")
    vm.return_value.total = 64 * 1024**3


def test_attributes_resources_numa(mocker, fs) -> None:
    """Test :func:`.attributes_resources` with a mock NUMA machine."""
    args = agent_mkconfig.parse_args(
        [
            "--network=enp193s0f0np0:cbf:80",
            "--network=eno1:cbf,sdp_10g",
            "--volume=data:/foo/data",
            "--volume=ramdisk:/mnt/ramdisk:1",
        ]
    )

    fs.pause()
    mock_lstopo(mocker, "lstopo-numa.xml")
    fs.resume()
    mock_gpu(mocker)
    mock_mem(mocker)
    mock_nics(
        mocker,
        fs,
        [
            FakeNic(
                ifname="enp193s0f0np0",
                ipv4_address="10.100.1.1",
                speed=100000,
                infiniband_index=0,
                loopback_disable=True,
            ),
            FakeNic(ifname="eno1", ipv4_address="10.8.1.2", speed=1000),
        ],
    )
    fs.create_dir("/foo/data")
    fs.create_dir("/mnt/ramdisk")

    attributes, resources = agent_mkconfig.attributes_resources(args)
    assert attributes == {
        "katsdpcontroller.interfaces": [
            {
                "name": "enp193s0f0np0",
                "network": "cbf",
                "ipv4_address": "10.100.1.1",
                "numa_node": 0,
                "infiniband_devices": [
                    "/dev/infiniband/rdma_cm",
                    "/dev/infiniband/uverbs0",
                ],
                "infiniband_multicast_loopback": False,
            },
            {
                "name": "eno1",
                "network": ["cbf", "sdp_10g"],
                "ipv4_address": "10.8.1.2",
                "numa_node": 2,
            },
        ],
        "katsdpcontroller.infiniband_devices": ["/dev/infiniband/uverbs0"],
        "katsdpcontroller.volumes": [
            {"host_path": "/foo/data", "name": "data"},
            {"host_path": "/mnt/ramdisk", "name": "ramdisk", "numa_node": 1},
        ],
        "katsdpcontroller.gpus": [
            {
                "name": "NVIDIA GeForce RTX 3070 Ti",
                "compute_capability": (8, 6),
                "device_attributes": {},
                "uuid": "GPU-2854ce83-bd19-0424-de81-c437df8f47a2",
                "numa_node": 1,
            }
        ],
        "katsdpcontroller.numa": [[0, 1, 2, 3], [4, 5, 6, 7], [8, 9, 10, 11], [12, 13, 14, 15]],
    }
    assert resources == {
        "katsdpcontroller.gpu.0.compute": 1.0,
        "katsdpcontroller.gpu.0.mem": 8192.0,
        "katsdpcontroller.interface.0.bandwidth_in": 80e9,
        "katsdpcontroller.interface.0.bandwidth_out": 80e9,
        "katsdpcontroller.interface.1.bandwidth_in": 1e9,
        "katsdpcontroller.interface.1.bandwidth_out": 1e9,
        "cores": "[0-15]",
        "cpus": 16.0,
        "mem": 64512.0,
    }


def test_attributes_resources_no_numa(mocker, fs) -> None:
    """Test :func:`.attributes_resources` for a machine without NUMA nodes.

    lstopo has a different structure in this case, which is why it needs a
    separate test. It also exercises some variations on the command-line
    arguments.
    """
    args = agent_mkconfig.parse_args(
        [
            "--network=ens1:cbf",
            "--network=enp5s0d1:sdp_10g",
            "--reserve-cpu=1",
            "--reserve-mem=2048",
            "--priority=110",
            "--subsystem=cbf",
            "--subsystem=sdp",
        ]
    )

    fs.pause()
    mock_lstopo(mocker, "lstopo-no-numa.xml")
    fs.resume()
    mock_mem(mocker)
    mock_nics(
        mocker,
        fs,
        [
            FakeNic(ifname="ens1", ipv4_address="10.100.1.1", speed=100000, infiniband_index=0),
            FakeNic(ifname="enp5s0d1", ipv4_address="10.8.1.2", speed=10000),
        ],
    )
    # Simulate NVML not being installed
    mocker.patch("katsdpcontroller.agent_mkconfig.pynvml", None)

    attributes, resources = agent_mkconfig.attributes_resources(args)
    assert attributes == {
        "katsdpcontroller.interfaces": [
            {
                "name": "ens1",
                "network": "cbf",
                "ipv4_address": "10.100.1.1",
                "numa_node": 0,
                "infiniband_devices": [
                    "/dev/infiniband/rdma_cm",
                    "/dev/infiniband/uverbs0",
                ],
            },
            {
                "name": "enp5s0d1",
                "network": "sdp_10g",
                "ipv4_address": "10.8.1.2",
                "numa_node": 0,
            },
        ],
        "katsdpcontroller.infiniband_devices": ["/dev/infiniband/uverbs0"],
        "katsdpcontroller.volumes": [],
        "katsdpcontroller.gpus": [],
        "katsdpcontroller.numa": [[0, 1, 2, 3]],
        "katsdpcontroller.subsystems": ["cbf", "sdp"],
        "katsdpcontroller.priority": 110,
    }
    assert resources == {
        "katsdpcontroller.interface.0.bandwidth_in": 100e9,
        "katsdpcontroller.interface.0.bandwidth_out": 100e9,
        "katsdpcontroller.interface.1.bandwidth_in": 10e9,
        "katsdpcontroller.interface.1.bandwidth_out": 10e9,
        "cores": "[0-3]",
        "cpus": 3.0,
        "mem": 63488.0,
    }


@pytest.mark.parametrize(
    "content",
    [
        [1, 2, 3],
        {"foo": "x", "bar": 3.5, "spam": True, "list": []},
        "string",
        False,
        None,
    ],
)
def test_encode(content) -> None:
    """Test :func:`.agent_mkconfig.encode`."""
    encoded = agent_mkconfig.encode(content)
    # Legal character set for Mesos
    # (https://mesos.apache.org/documentation/latest/attributes-resources/)
    assert re.fullmatch("[A-Za-z0-9_/.-]*", encoded)
    decoded = json.loads(base64.urlsafe_b64decode(encoded))
    assert decoded == content


def test_encode_real() -> None:
    """Test :func:`.agent_mkconfig.encode` with real inputs."""
    assert agent_mkconfig.encode(123) == "123.0"
    assert agent_mkconfig.encode(123.4) == "123.4"


def test_write_dict(fs) -> None:
    """Test :func:`.agent_mkconfig.write_dict`."""
    args = argparse.Namespace(dry_run=False)
    d = {"foo": [1, 2, 3], "bar": "string"}
    changed = agent_mkconfig.write_dict(
        "attributes", "/etc/mesos-agent/attributes", args, d, do_encode=True
    )
    assert changed
    base = pathlib.Path("/etc/mesos-agent/attributes")
    assert (base / "foo").read_text() == agent_mkconfig.encode([1, 2, 3]) + "\n"
    assert (base / "bar").read_text() == agent_mkconfig.encode("string") + "\n"

    # Do it again: should be no change
    changed = agent_mkconfig.write_dict(
        "attributes", "/etc/mesos-agent/attributes", args, d, do_encode=True
    )
    assert not changed

    # Add a katsdpcontroller-prefixed attribute: it should be removed
    fs.create_file("/etc/mesos-agent/attributes/katsdpcontroller.spam")
    fs.create_file("/etc/mesos-agent/attributes/other")
    changed = agent_mkconfig.write_dict(
        "attributes", "/etc/mesos-agent/attributes", args, d, do_encode=True
    )
    assert changed
    assert not (base / "katsdpcontroller.spam").exists()
    assert (base / "other").is_file()
