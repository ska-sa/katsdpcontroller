################################################################################
# Copyright (c) 2013-2025, National Research Foundation (SARAO)
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

import copy
import enum
import itertools
import logging
import math
import os.path
import re
import time
import urllib.parse
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Optional,
    Sequence,
    Set,
    Tuple,
    Type,
    TypeVar,
    Union,
)

import addict
import astropy.units as u
import katdal
import katdal.datasources
import katpoint
import katsdpmodels.fetch.aiohttp
import katsdptelstate.aio
import networkx
import numpy as np
from aiokatcp import Sensor, SensorSampler, SensorSet
from katsdpmodels.band_mask import BandMask, SpectralWindow
from katsdpmodels.rfi_mask import RFIMask
from katsdptelstate.endpoint import Endpoint

from . import defaults, product_config, scheduler
from .aggregate_sensors import LatestSensor, SumSensor, SyncSensor
from .defaults import FAST_SENSOR_UPDATE_PERIOD, GPUCBF_PACKET_PAYLOAD_BYTES, LOCALHOST
from .fake_servers import (
    FakeCalDeviceServer,
    FakeDsimDeviceServer,
    FakeFgpuDeviceServer,
    FakeIngestDeviceServer,
    FakeXbgpuDeviceServer,
)
from .product_config import BYTES_PER_FLAG, BYTES_PER_VFW, BYTES_PER_VIS, Configuration, data_rate
from .tasks import (
    CaptureBlockState,
    KatcpTransition,
    LogicalGroup,
    ProductFakePhysicalTask,
    ProductLogicalTask,
    ProductPhysicalTask,
)

# Import just for type annotations, but avoid at runtime due to circular imports
if TYPE_CHECKING:
    from . import product_controller  # noqa


_T = TypeVar("_T")
GpucbfXBStream = Union[
    product_config.GpucbfBaselineCorrelationProductsStream,
    product_config.GpucbfTiedArrayChannelisedVoltageStream,
]


def normalise_gpu_name(name):
    # Turn spaces and dashes into underscores, remove anything that isn't
    # alphanumeric or underscore, and lowercase (because Docker doesn't
    # allow uppercase in image names).
    mangled = re.sub("[- ]", "_", name.lower())
    mangled = re.sub("nvidia_", "", mangled)
    mangled = re.sub("[^a-z0-9_]", "", mangled)
    return mangled


def escape_format(s: str) -> str:
    """Escape a string for :meth:`str.format`."""
    return s.replace("{", "{{").replace("}", "}}")


CAPTURE_TRANSITIONS = {
    CaptureBlockState.CAPTURING: [
        KatcpTransition("capture-init", "{capture_block_id}", timeout=30)
    ],
    CaptureBlockState.BURNDOWN: [KatcpTransition("capture-done", timeout=240)],
}
#: Docker images that may appear in the logical graph (used set to Docker image metadata)
IMAGES = frozenset(
    [
        "katcbfsim",
        "katgpucbf",
        "katsdpbfingest",
        "katsdpcal",
        "katsdpcam2telstate",
        "katsdpcontim",
        "katsdpdisp",
        "katsdpimager",
        "katsdpingest_" + normalise_gpu_name(defaults.INGEST_GPU_NAME),
        "katsdpdatawriter",
        "katsdpmetawriter",
        "katsdptelstate",
    ]
)
#: Number of bytes used by spectral imager per visibility
BYTES_PER_VFW_SPECTRAL = 14.5  # 58 bytes for 4 polarisation products
#: Number of visibilities in a 32 antenna 32K channel dump, for scaling.
_N32_32 = 32 * 33 * 2 * 32768
#: Number of visibilities in a 16 antenna 32K channel dump, for scaling.
_N16_32 = 16 * 17 * 2 * 32768
#: Maximum sample rate for supported bands (S-band)
_MAX_ADC_SAMPLE_RATE = 1750e6
#: Volume serviced by katsdptransfer to transfer results to the archive
DATA_VOL = scheduler.VolumeRequest("data", "/var/kat/data", "RW")
#: Like DATA_VOL, but for high speed data to be transferred to an object store
OBJ_DATA_VOL = scheduler.VolumeRequest("obj_data", "/var/kat/data", "RW")
#: Volume for persisting user configuration
CONFIG_VOL = scheduler.VolumeRequest("config", "/var/kat/config", "RW")
#: Number of components in a complex number
COMPLEX = 2

logger = logging.getLogger(__name__)


class TransmitState(enum.Enum):
    UNKNOWN = 0
    DOWN = 1
    UP = 2


class LogicalMulticast(scheduler.LogicalExternal):
    def __init__(
        self,
        stream_name,
        n_addresses=None,
        endpoint=None,
        initial_transmit_state=TransmitState.UNKNOWN,
    ):
        super().__init__("multicast." + stream_name)
        self.stream_name = stream_name
        self.physical_factory = PhysicalMulticast
        self.sdp_physical_factory = True
        self.n_addresses = n_addresses
        self.endpoint = endpoint
        if (self.n_addresses is None) == (self.endpoint is None):
            raise ValueError("Exactly one of n_addresses and endpoint must be specified")
        self.initial_transmit_state = initial_transmit_state


class PhysicalMulticast(scheduler.PhysicalExternal):
    def __init__(self, logical_task, sdp_controller, subarray_product, capture_block_id):
        super().__init__(logical_task)
        self.transmit_state = logical_task.initial_transmit_state
        stream_name = logical_task.stream_name
        self._endpoint_sensor = Sensor(
            str,
            f"{stream_name}.destination",
            f"The IP address, range and port to which data for stream {stream_name} is sent",
        )
        subarray_product.add_sensor(self._endpoint_sensor)

    async def resolve(self, resolver, graph):
        await super().resolve(resolver, graph)
        if self.logical_node.endpoint is not None:
            self.host = self.logical_node.endpoint.host
            self.ports = {"spead": self.logical_node.endpoint.port}
        else:
            self.host = await resolver.resources.get_multicast_groups(self.logical_node.n_addresses)
            self.ports = {"spead": await resolver.resources.get_port()}
        # It should already be resolved. Note that it might NOT be a valid
        # IP address if it represents multiple multicast groups in the form
        # a.b.c.d+N.
        self.address = self.host
        self._endpoint_sensor.value = str(Endpoint(self.address, self.ports["spead"]))


class TelstateTask(ProductPhysicalTask):
    async def resolve(self, resolver, graph):
        await super().resolve(resolver, graph)
        # Add a port mapping
        self.taskinfo.container.docker.network = "BRIDGE"
        host_port = self.ports["telstate"]
        if not resolver.localhost:
            portmap = addict.Dict()
            portmap.host_port = host_port
            portmap.container_port = 6379
            portmap.protocol = "tcp"
            self.taskinfo.container.docker.port_mappings = [portmap]
        else:
            # Mesos doesn't provide a way to specify a port mapping with a
            # host-side binding, so we have to provide docker parameters
            # directly.
            parameters = self.taskinfo.container.docker.setdefault("parameters", [])
            parameters.append({"key": "publish", "value": f"{LOCALHOST}:{host_port}:6379"})


class IngestTask(ProductPhysicalTask):
    async def resolve(self, resolver, graph, image=None):
        if image is None:
            gpu = self.agent.gpus[self.allocation.gpus[0].index]
            image = await resolver.image_resolver("katsdpingest_" + normalise_gpu_name(gpu.name))
            logger.info("Using %s for ingest", image.path)
        await super().resolve(resolver, graph, image)


def _mb(value):
    """Convert bytes to mebibytes"""
    return value / 1024**2


def _round_down(value, period):
    return value // period * period


def _round_up(value, period):
    return _round_down(value - 1, period) + period


def find_node(g: networkx.MultiDiGraph, name: str) -> scheduler.LogicalNode:
    for node in g:
        if node.name == name:
            return node
    raise KeyError("no node called " + name)


def _make_telstate(g: networkx.MultiDiGraph, configuration: Configuration) -> scheduler.LogicalNode:
    # Pointing sensors can account for substantial memory per antennas over a
    # long-lived subarray.
    n_antennas = 0
    for stream in configuration.streams:
        # Note: counts both real and simulated antennas
        if isinstance(stream, product_config.AntennaChannelisedVoltageStreamBase):
            n_antennas += len(stream.antennas)

    telstate = ProductLogicalTask("telstate")
    telstate.subsystem = "sdp"
    # redis is nominally single-threaded, but has some helper threads
    # for background tasks so can occasionally exceed 1 CPU.
    telstate.cpus = 1.2 if not configuration.options.develop.less_resources else 0.2
    telstate.mem = 2048 + 400 * n_antennas
    telstate.disk = telstate.mem
    telstate.image = "katsdptelstate"
    telstate.ports = ["telstate"]
    # Run it in /mnt/mesos/sandbox so that the dump.rdb ends up there.
    telstate.taskinfo.container.docker.setdefault("parameters", []).append(
        {"key": "workdir", "value": "/mnt/mesos/sandbox"}
    )
    telstate.command = ["redis-server", "/usr/local/etc/redis/redis.conf"]
    if configuration.options.interface_mode:
        telstate.physical_factory = ProductFakePhysicalTask
    else:
        telstate.physical_factory = TelstateTask
    telstate.katsdpservices_logging = False
    telstate.katsdpservices_config = False
    telstate.pass_telstate = False  # Don't pass --telstate to telstate itself
    telstate.final_state = CaptureBlockState.DEAD
    g.add_node(telstate)
    return telstate


def _make_cam2telstate(
    g: networkx.MultiDiGraph, configuration: Configuration, stream: product_config.CamHttpStream
) -> scheduler.LogicalNode:
    cam2telstate = ProductLogicalTask("cam2telstate")
    cam2telstate.subsystem = "sdp"
    cam2telstate.image = "katsdpcam2telstate"
    cam2telstate.command = ["cam2telstate.py"]
    cam2telstate.cpus = 0.75
    cam2telstate.mem = 256
    cam2telstate.ports = ["port", "aiomonitor_port", "aiomonitor_webui_port", "aioconsole_port"]
    cam2telstate.wait_ports = ["port"]
    url = stream.url
    antennas = set()
    input_: product_config.AntennaChannelisedVoltageStreamBase
    for input_ in configuration.by_class(product_config.AntennaChannelisedVoltageStream):
        antennas.update(input_.antennas)
    for input_ in configuration.by_class(product_config.GpucbfAntennaChannelisedVoltageStream):
        antennas.update(input_.antennas)
    g.add_node(
        cam2telstate,
        config=lambda task, resolver: {
            "url": str(url),
            "aiomonitor": True,
            "receptors": ",".join(sorted(antennas)),
        },
    )
    return cam2telstate


def _make_meta_writer(
    g: networkx.MultiDiGraph, configuration: Configuration
) -> scheduler.LogicalNode:
    meta_writer = ProductLogicalTask("meta_writer")
    meta_writer.subsystem = "sdp"
    meta_writer.image = "katsdpmetawriter"
    meta_writer.command = ["meta_writer.py"]
    meta_writer.cpus = 0.2
    # Only a base allocation: it also gets telstate_extra added
    # meta_writer.mem = 256
    # Temporary workaround for SR-1695, until the machines can have their
    # kernels upgraded: give it the same memory as telescore state
    telstate = find_node(g, "telstate")
    assert isinstance(telstate, scheduler.LogicalTask)
    meta_writer.mem = telstate.mem
    meta_writer.ports = ["port"]
    meta_writer.volumes = [OBJ_DATA_VOL]
    meta_writer.interfaces = [scheduler.InterfaceRequest("sdp_10g")]
    # Actual required data rate is minimal, but bursty. Use 1 Gb/s,
    # except in development mode where it might not be available.
    meta_writer.interfaces[0].bandwidth_out = (
        1e9 if not configuration.options.develop.less_resources else 10e6
    )
    meta_writer.transitions = {
        CaptureBlockState.BURNDOWN: [
            KatcpTransition("write-meta", "{capture_block_id}", True, timeout=120)  # Light dump
        ],
        CaptureBlockState.DEAD: [
            KatcpTransition("write-meta", "{capture_block_id}", False, timeout=300)  # Full dump
        ],
    }
    meta_writer.final_state = CaptureBlockState.DEAD

    g.add_node(meta_writer, config=lambda task, resolver: {"rdb_path": OBJ_DATA_VOL.container_path})
    return meta_writer


def _make_dsim(
    g: networkx.MultiDiGraph,
    configuration: Configuration,
    streams: Iterable[product_config.SimDigBasebandVoltageStream],
) -> scheduler.LogicalNode:
    """Create the dsim process for a single antenna.

    An antenna has a separate stream per polarisation, so `streams` will
    normally have two elements.
    """
    ibv = not configuration.options.develop.disable_ibverbs
    # dsim assigns digitiser IDs positionally. According to M1000-0001-053,
    # the least significant bit is the polarization ID with 0 = vertical, so
    # sort by reverse of name so that if the streams are, for example,
    # m012h and m012v then m012v comes first.
    streams = sorted(streams, key=lambda stream: stream.name, reverse=True)

    if not all(stream.sync_time == streams[0].sync_time for stream in streams):
        raise RuntimeError("inconsistent sync times for {streams[0].antenna_name}")

    n_endpoints = 8  # Matches MeerKAT digitisers

    init_telstate: Dict[Union[str, Tuple[str, ...]], Any] = g.graph["init_telstate"]
    telstate_data = {"observer": streams[0].antenna.description}
    for key, value in telstate_data.items():
        init_telstate[(streams[0].antenna.name, key)] = value
    init_telstate[("sub", "band")] = streams[0].band

    # Generate a unique name. The caller groups streams by
    # (antenna.name, adc_sample_rate), so that is guaranteed to be unique.
    name = f"sim.{streams[0].antenna.name}.{streams[0].adc_sample_rate}"
    dsim = ProductLogicalTask(name, streams=streams)
    dsim.fake_katcp_server_cls = FakeDsimDeviceServer
    dsim.subsystem = "cbf"
    dsim.image = "katgpucbf"
    dsim.mem = 8192
    dsim.ports = ["port", "prometheus"]
    dsim.interfaces = [
        scheduler.InterfaceRequest(
            "gpucbf", infiniband=ibv, multicast_out={stream.name for stream in streams}
        )
    ]
    dsim.interfaces[0].bandwidth_out = sum(stream.data_rate() for stream in streams)
    dsim.command = [
        "dsim",
        "--interface",
        "{interfaces[gpucbf].name}",
        "--adc-sample-rate",
        str(streams[0].adc_sample_rate),
        "--ttl",
        "4",
        "--sync-time",
        str(streams[0].sync_time),
        "--katcp-port",
        "{ports[port]}",
        "--prometheus-port",
        "{ports[prometheus]}",
    ]
    dsim.sensor_renames["sync-time"] = [f"{stream.name}.sync-time" for stream in streams]
    # Allow dsim task to set a realtime scheduling priority itself
    dsim.taskinfo.container.docker.parameters = [{"key": "ulimit", "value": "rtprio=1"}]
    if configuration.options.develop.less_resources:
        # In develop mode with less_resources option=True,
        # scale down reservation for low bandwidths to allow
        # testing low-bandwidth arrays on a single machine. Use a full core
        # for the maximum sample rate (which is generous - it only needs
        # about 70% for S-band, depending on the CPU).
        total_adc_sample_rate = sum(stream.adc_sample_rate for stream in streams)
        dsim.cpus = min(1.0, total_adc_sample_rate / (2 * _MAX_ADC_SAMPLE_RATE))
    else:
        dsim.cpus = 4
        # Two cores for specific purposes, the rest to parallelise computation
        # of signals.
        dsim.cores = ["main", "send", None, None]
        dsim.command = dsim.command + [
            "--affinity",
            "{cores[send]}",
            "--main-affinity",
            "{cores[main]}",
        ]
    if ibv:
        dsim.capabilities.append("NET_RAW")  # For ibverbs raw QPs
        dsim.command += ["--ibv"]
    dsim.command += streams[0].command_line_extra
    # dsim doesn't use katsdpservices or telstate
    dsim.katsdpservices_logging = False
    dsim.katsdpservices_config = False
    dsim.pass_telstate = False
    g.add_node(dsim)
    for stream in streams:
        # {{ and }} become { and } after f-string interpolation
        dsim.command.append(f"{{endpoints[multicast.{stream.name}_spead]}}")
        multicast = LogicalMulticast(stream.name, n_endpoints)
        g.add_node(multicast)
        g.add_edge(dsim, multicast, port="spead", depends_resolve=True)
        g.add_edge(multicast, dsim, depends_init=True, depends_ready=True)
    return dsim


def _make_dig(g: networkx.MultiDiGraph, stream: product_config.DigBasebandVoltageStream) -> None:
    """Populate telstate with information about a real antenna."""
    if stream.antenna is not None:
        init_telstate: Dict[Union[str, Tuple[str, ...]], Any] = g.graph["init_telstate"]
        telstate_data = {"observer": stream.antenna.description}
        for key, value in telstate_data.items():
            init_telstate[(stream.antenna.name, key)] = value


def _fgpu_key(stream: product_config.GpucbfAntennaChannelisedVoltageStream) -> tuple:
    """Comparison key for an antenna-channelised-voltage stream.

    The keys for two streams should be the same if and only if they can run
    in the same engine.
    """
    return (
        [src.name for src in stream.src_streams],
        stream.input_labels,
        stream.command_line_extra,
    )


def _xbgpu_key(stream: GpucbfXBStream) -> tuple:
    """Comparison key for an X/B stream.

    The keys for two streams should be the same if and only if they can run
    in the same engine.
    """
    return (
        stream.antenna_channelised_voltage.name,
        stream.command_line_extra,
    )


def _make_fgpu(
    g: networkx.MultiDiGraph,
    configuration: Configuration,
    streams: Iterable[product_config.GpucbfAntennaChannelisedVoltageStream],
) -> scheduler.LogicalNode:
    # streams needs to be a Sequence (not just an iterable that is consumed
    # once), which `sorted` achieves. Put wideband first for consistency.
    streams = sorted(streams, key=lambda stream: stream.narrowband is not None)
    ibv = not configuration.options.develop.disable_ibverbs
    n_engines = len(streams[0].src_streams) // 2
    base_name = streams[0].name
    sync_time = streams[0].sources(0)[0].sync_time
    fgpu_group = LogicalGroup(f"fgpu.{base_name}")
    g.add_node(fgpu_group)

    init_telstate: Dict[Union[str, Tuple[str, ...]], Any] = g.graph["init_telstate"]
    data_suspect_sensors = []
    dst_multicasts = []
    for stream in streams:
        dst_multicast = LogicalMulticast(
            stream.name, stream.n_substreams, initial_transmit_state=TransmitState.UP
        )
        g.add_node(dst_multicast)
        g.add_edge(dst_multicast, fgpu_group, depends_init=True, depends_ready=True)
        dst_multicasts.append(dst_multicast)

        # TODO: this list is not complete, and will need to be updated as the ICD evolves.
        data_suspect_sensor = Sensor(
            str,
            f"{stream.name}.input-data-suspect",
            "A bitmask of flags indicating for each input whether the data for "
            "that input should be considered to be garbage. Input order matches "
            "input labels.",
            default="0" * len(stream.src_streams),
            initial_status=Sensor.Status.NOMINAL,
        )
        data_suspect_sensors.append(data_suspect_sensor)

        if stream.narrowband is not None:
            ddc_group_delay = -(stream.narrowband.ddc_taps - 1) / 2
            subsampling = stream.narrowband.subsampling
            internal_channels = stream.n_chans
            if stream.narrowband.vlbi is None:
                internal_channels *= 2
            pfb_group_delay = -(internal_channels * stream.pfb_taps - 1) / 2 * subsampling
        else:
            ddc_group_delay = 0.0
            # Real-to-complex PFB
            pfb_group_delay = -(2 * stream.n_chans * stream.pfb_taps - 1) / 2

        stream_sensors = [
            Sensor(
                int,
                f"{stream.name}.adc-bits",
                "ADC sample bitwidth",
                default=stream.sources(0)[0].bits_per_sample,
                initial_status=Sensor.Status.NOMINAL,
            ),
            Sensor(
                float,
                f"{stream.name}.adc-sample-rate",
                "Sample rate of the ADC",
                "Hz",
                default=stream.adc_sample_rate,
                initial_status=Sensor.Status.NOMINAL,
            ),
            Sensor(
                float,
                f"{stream.name}.bandwidth",
                "The analogue bandwidth of the digitised band",
                "Hz",
                default=stream.bandwidth,
                initial_status=Sensor.Status.NOMINAL,
            ),
            Sensor(
                float,
                f"{stream.name}.pass-bandwidth",
                "Bandwidth over which the CBF produces a flat response",
                "Hz",
                default=stream.pass_bandwidth,
                initial_status=Sensor.Status.NOMINAL,
            ),
            Sensor(
                int,
                f"{stream.name}.decimation-factor",
                "The factor by which the bandwidth of the incoming digitiser stream is decimated",
                default=stream.narrowband.decimation_factor if stream.narrowband else 1,
                initial_status=Sensor.Status.NOMINAL,
            ),
            # The timestamps are simply ADC sample counts
            Sensor(
                float,
                f"{stream.name}.scale-factor-timestamp",
                "Factor by which to divide instrument timestamps to convert to seconds",
                "Hz",
                default=stream.adc_sample_rate,
                initial_status=Sensor.Status.NOMINAL,
            ),
            Sensor(
                float,
                f"{stream.name}.sync-time",
                "The time at which the digitisers were synchronised. Seconds since the Unix Epoch.",
                "s",
                default=sync_time,
                initial_status=Sensor.Status.NOMINAL,
            ),
            Sensor(
                int,
                f"{stream.name}.n-ants",
                "The number of antennas in the instrument",
                default=len(stream.antennas),
                initial_status=Sensor.Status.NOMINAL,
            ),
            Sensor(
                int,
                f"{stream.name}.n-inputs",
                "The number of single polarisation inputs to the instrument",
                default=len(stream.src_streams),
                initial_status=Sensor.Status.NOMINAL,
            ),
            Sensor(
                int,
                f"{stream.name}.n-fengs",
                "The number of F-engines in the instrument",
                default=n_engines,
                initial_status=Sensor.Status.NOMINAL,
            ),
            Sensor(
                int,
                f"{stream.name}.feng-out-bits-per-sample",
                "F-engine output bits per sample. Per number, not complex pair. "
                "Real and imaginary parts are both this wide",
                default=stream.bits_per_sample,
                initial_status=Sensor.Status.NOMINAL,
            ),
            Sensor(
                int,
                f"{stream.name}.n-chans",
                "Number of channels in the output spectrum",
                default=stream.n_chans,
                initial_status=Sensor.Status.NOMINAL,
            ),
            Sensor(
                int,
                f"{stream.name}.n-chans-per-substream",
                "Number of channels in each substream for this F-engine stream",
                default=stream.n_chans_per_substream,
                initial_status=Sensor.Status.NOMINAL,
            ),
            Sensor(
                int,
                f"{stream.name}.n-samples-between-spectra",
                "Number of samples between spectra",
                default=stream.n_samples_between_spectra,
                initial_status=Sensor.Status.NOMINAL,
            ),
            Sensor(
                float,
                f"{stream.name}.pfb-group-delay",
                "PFB group delay, specified in number of samples",
                default=pfb_group_delay,
                initial_status=Sensor.Status.NOMINAL,
            ),
            Sensor(
                float,
                f"{stream.name}.ddc-group-delay",
                "DDC group delay, specified in number of samples",
                default=ddc_group_delay,
                initial_status=Sensor.Status.NOMINAL,
            ),
            Sensor(
                float,
                f"{stream.name}.filter-group-delay",
                "Total group delay of all filters in the signal chain, "
                "specified in number of samples.",
                default=pfb_group_delay + ddc_group_delay,
                initial_status=Sensor.Status.NOMINAL,
            ),
            Sensor(
                int,
                f"{stream.name}.spectra-per-heap",
                "Number of spectrum chunks per heap",
                default=stream.n_spectra_per_heap,
                initial_status=Sensor.Status.NOMINAL,
            ),
            # Note: reports baseband centre frequency, not on-sky, and hence not
            # stream.centre_frequency.
            Sensor(
                float,
                f"{stream.name}.center-freq",
                "The centre frequency of the digitised band",
                default=stream.narrowband.centre_frequency
                if stream.narrowband is not None
                else stream.bandwidth / 2,
                initial_status=Sensor.Status.NOMINAL,
            ),
            Sensor(
                int,
                f"{stream.name}.payload-len",
                "The payload size, in bytes, of the F-engine data stream packets",
                default=GPUCBF_PACKET_PAYLOAD_BYTES,
                initial_status=Sensor.Status.NOMINAL,
            ),
            Sensor(
                int,
                f"{stream.name}.pfb-taps",
                "Number of taps in the polyphase filter bank",
                default=stream.pfb_taps,
                initial_status=Sensor.Status.NOMINAL,
            ),
            Sensor(
                int,
                f"{stream.name}.ddc-taps",
                "Number of taps in the narrowband digital down-conversion filter "
                "(0 if there isn't one)",
                default=stream.narrowband.ddc_taps if stream.narrowband is not None else 0,
                initial_status=Sensor.Status.NOMINAL,
            ),
            Sensor(
                product_config.GpucbfWindowFunction,
                f"{stream.name}.window-function",
                "Window function for shaping the PFB filter weights",
                default=stream.window_function,
                initial_status=Sensor.Status.NOMINAL,
            ),
            Sensor(
                float,
                f"{stream.name}.w-cutoff",
                "Scaling factor for sinc component of PFB filter weights",
                default=stream.w_cutoff,
                initial_status=Sensor.Status.NOMINAL,
            ),
            Sensor(
                float,
                f"{stream.name}.weight-pass",
                "Weight given to passband in narrowband digital down-conversion filter "
                "(0 if there isn't one)",
                default=stream.narrowband.weight_pass if stream.narrowband is not None else 0.0,
                initial_status=Sensor.Status.NOMINAL,
            ),
            data_suspect_sensor,
        ]
        for ss in stream_sensors:
            g.graph["stream_sensors"].add(ss)

        telstate_data = {
            "instrument_dev_name": "gpucbf",  # Made-up instrument name
            # Normally obtained from the CBF proxy or subarray controller
            "input_labels": stream.input_labels,
            # Copies of our own sensors, with transformations normally applied by cam2telstate
            "adc_sample_rate": stream.adc_sample_rate,
            "n_inputs": len(stream.src_streams),
            "scale_factor_timestamp": stream.adc_sample_rate,
            "sync_time": sync_time,
            "ticks_between_spectra": stream.n_samples_between_spectra,
            "n_chans": stream.n_chans,
            "bandwidth": stream.bandwidth,
            "center_freq": stream.centre_frequency,
        }
        for key, value in telstate_data.items():
            init_telstate[(stream.normalised_name, key)] = value

    for i in range(0, n_engines):
        srcs = streams[0].sources(i)
        input_labels = (streams[0].input_labels[2 * i], streams[0].input_labels[2 * i + 1])
        fgpu = ProductLogicalTask(f"f.{base_name}.{i}", streams=streams, index=i)
        fgpu.subsystem = "cbf"
        fgpu.image = "katgpucbf"
        fgpu.fake_katcp_server_cls = FakeFgpuDeviceServer
        fgpu.mem = 1700  # Actual use is currently around 1.6GB when using narrowband
        if not configuration.options.develop.less_resources:
            fgpu.cpus = 3
            fgpu.cores = ["recv", "send", "python"]
            fgpu.numa_nodes = 1.0  # It's easily starved of bandwidth
            taskset = ["taskset", "-c", "{cores[python]}"]
        else:
            fgpu.cpus = 3 * stream.adc_sample_rate / _MAX_ADC_SAMPLE_RATE
            taskset = []
        fgpu.ports = ["port", "prometheus", "aiomonitor", "aiomonitor_webui", "aioconsole"]
        fgpu.wait_ports = ["port", "prometheus"]
        # TODO: could specify separate interface requests for input and
        # output. Currently that's not possible because interfaces are looked
        # up by network name.
        fgpu.interfaces = [
            scheduler.InterfaceRequest(
                "gpucbf",
                infiniband=ibv,
                multicast_in={src.name for src in srcs},
                multicast_out={stream.name for stream in streams},
            )
        ]
        fgpu.interfaces[0].bandwidth_in = sum(src.data_rate() for src in srcs)
        # stream.data_rate() is sum over all the engines
        fgpu.interfaces[0].bandwidth_out = sum(stream.data_rate() for stream in streams) / n_engines
        fgpu.gpus = [scheduler.GPURequest()]
        # Run at least 4 per GPU, scaling with bandwidth. This will
        # likely need to be revised based on the GPU model selected.
        fgpu.gpus[0].compute = 0.25 * stream.adc_sample_rate / _MAX_ADC_SAMPLE_RATE
        fgpu.gpus[0].mem = 1600  # With 1/8 narrowband, actual use is 1572 MiB
        fgpu.command = (
            ["schedrr"]
            + taskset
            + [
                "fgpu",
                "--recv-interface",
                "{interfaces[gpucbf].name}",
                "--send-interface",
                "{interfaces[gpucbf].name}",
                "--send-packet-payload",
                str(GPUCBF_PACKET_PAYLOAD_BYTES),
                "--adc-sample-rate",
                str(srcs[0].adc_sample_rate),
                "--feng-id",
                str(i),
                "--array-size",
                str(n_engines),
                "--sync-time",
                str(srcs[0].sync_time),
                "--katcp-port",
                "{ports[port]}",
                "--prometheus-port",
                "{ports[prometheus]}",
                "--aiomonitor",
                "--aiomonitor-port",
                "{ports[aiomonitor]}",
                "--aiomonitor-webui-port",
                "{ports[aiomonitor_webui]}",
                "--aioconsole-port",
                "{ports[aioconsole]}",
            ]
        )
        for stream in streams:
            output_config = {
                "name": escape_format(stream.name),
                "channels": stream.n_chans,
                "taps": stream.pfb_taps,
                "w_cutoff": stream.w_cutoff,
                "jones_per_batch": stream.n_jones_per_batch,
                "dst": f"{{endpoints[multicast.{stream.name}_spead]}}",
                "dither": stream.dither,
                "window_function": stream.window_function.value,
            }
            if stream.narrowband is not None:
                output_config["decimation"] = stream.narrowband.decimation_factor
                output_config["centre_frequency"] = stream.narrowband.centre_frequency
                output_config["ddc_taps"] = stream.narrowband.ddc_taps
                output_config["weight_pass"] = stream.narrowband.weight_pass
                if stream.narrowband.vlbi is not None:
                    output_config["pass_bandwidth"] = stream.narrowband.vlbi.pass_bandwidth
                output_arg_name = "narrowband"
            else:
                output_arg_name = "wideband"
            fgpu.command += [
                f"--{output_arg_name}",
                ",".join(
                    f"{key}={value}" for (key, value) in output_config.items() if value is not None
                ),
            ]

        if not configuration.options.develop.less_resources:
            fgpu.command += [
                "--recv-affinity",
                "{cores[recv]}",
                "--send-affinity",
                "{cores[send]}",
            ]
        fgpu.capabilities.append("SYS_NICE")  # For schedrr
        if ibv:
            # Enable cap_net_raw capability for access to raw QPs
            fgpu.capabilities.append("NET_RAW")
            fgpu.command += [
                "--recv-ibv",
                "--send-ibv",
            ]
            if not configuration.options.develop.less_resources:
                # Use the core numbers as completion vectors. This ensures that
                # multiple instances on a machine will use distinct vectors.
                fgpu.command += [
                    "--recv-comp-vector",
                    "{cores[recv]}",
                    "--send-comp-vector",
                    "{cores[send]}",
                ]
        fgpu.command += streams[0].command_line_extra
        # fgpu doesn't use katsdpservices or telstate for config, but does use logging
        fgpu.katsdpservices_config = False
        fgpu.pass_telstate = False
        fgpu.data_suspect_sensors = data_suspect_sensors
        fgpu.data_suspect_range = (2 * i, 2 * i + 2)
        # Identify the antenna, if the input labels are consistent
        if input_labels[0][:-1] == input_labels[1][:-1]:
            fgpu.consul_meta["antenna"] = input_labels[0][:-1]
        fgpu.critical = False  # Can survive losing individual engines
        g.add_node(fgpu)

        # Wire it up to the multicast streams
        src_strs = []
        for src in srcs:
            src_multicast = find_node(g, f"multicast.{src.name}")
            # depends_init is included purely to ensure sensible ordering
            # on the dashboard.
            g.add_edge(
                fgpu,
                src_multicast,
                port="spead",
                depends_resolve=True,
                depends_init=True,
            )
            src_strs.append(f"{{endpoints[multicast.{src.name}_spead]}}")
        fgpu.command.append(",".join(src_strs))
        for dst_multicast in dst_multicasts:
            g.add_edge(fgpu, dst_multicast, port="spead", depends_resolve=True)

        # Link it to the group, so that downstream tasks need only depend on the group.
        g.add_edge(fgpu_group, fgpu, depends_ready=True, depends_init=True)

        # Rename sensors that are relevant to the stream rather than the process
        for j, label in enumerate(input_labels):
            for name in [
                "dig-clip-cnt",
                "dig-rms-dbfs",
                "rx.timestamp",
                "rx.unixtime",
                "rx.missing-unixtime",
            ]:
                # These engine-level sensors get replicated to all the corresponding streams
                fgpu.sensor_renames[f"input{j}.{name}"] = [
                    f"{stream.name}.{label}.{name}" for stream in streams
                ]
            for stream in streams:
                for name in [
                    "eq",
                    "delay",
                    "feng-clip-cnt",
                ]:
                    fgpu.sensor_renames[
                        f"{stream.name}.input{j}.{name}"
                    ] = f"{stream.name}.{label}.{name}"
        for stream in streams:
            for name in [
                "dither-seed",
            ]:
                # These engine-level sensors get replicated as well, but are not per-input
                fgpu.sensor_renames[f"{stream.name}.{name}"] = [
                    f"{stream.name}.{label}.{name}" for label in input_labels
                ]

        # Prepare expected data rates etc
        fgpu.static_gauges["fgpu_expected_input_heaps_per_second"] = sum(
            src.adc_sample_rate / src.samples_per_heap for src in srcs
        )
        fgpu.static_gauges["fgpu_expected_engines"] = 1.0

    return fgpu_group


def _make_xbgpu(
    g: networkx.MultiDiGraph,
    configuration: Configuration,
    sensors: SensorSet,
    streams: Iterable[GpucbfXBStream],
) -> scheduler.LogicalNode:
    # Ensure that streams is a sequence, not just an iterable. Also put
    # baseline correlation products first (typically there will just be one,
    # and this will ensure it becomes base_name below) and sort by name after
    # that.
    streams = sorted(
        streams,
        key=lambda stream: (
            not isinstance(stream, product_config.GpucbfBaselineCorrelationProductsStream),
            stream.name,
        ),
    )

    ibv = not configuration.options.develop.disable_ibverbs
    acv = streams[0].antenna_channelised_voltage
    n_engines = streams[0].n_substreams
    n_inputs = len(acv.src_streams)
    sync_time = acv.sources(0)[0].sync_time

    # Input labels list `h` and `v` pols separately so the reshape is to
    # make the process a bit smoother.
    ants = np.array(acv.input_labels).reshape(-1, 2)
    n_ants = ants.shape[0]

    base_name = streams[0].name
    xbgpu_group = LogicalGroup(f"xbgpu.{base_name}")
    g.add_node(xbgpu_group)
    dst_multicasts = []
    data_suspect_sensors = []
    for stream in streams:
        dst_multicast = LogicalMulticast(
            stream.name, stream.n_substreams, initial_transmit_state=TransmitState.DOWN
        )
        g.add_node(dst_multicast)
        dst_multicasts.append(dst_multicast)
        g.add_edge(dst_multicast, xbgpu_group, depends_init=True, depends_ready=True)

        data_suspect_sensor = Sensor(
            str,
            f"{stream.name}.channel-data-suspect",
            "A bitmask of flags indicating whether each channel should be considered "
            "to be garbage.",
            default="0" * stream.n_chans,
            initial_status=Sensor.Status.NOMINAL,
        )
        data_suspect_sensors.append(data_suspect_sensor)

        escaped_name = re.escape(stream.name)
        stream_sensors: List[Sensor] = [
            # The timestamps are simply ADC sample counts
            Sensor(
                float,
                f"{stream.name}.scale-factor-timestamp",
                "Factor by which to divide instrument timestamps to convert to seconds",
                "Hz",
                default=stream.adc_sample_rate,
                initial_status=Sensor.Status.NOMINAL,
            ),
            Sensor(
                float,
                f"{stream.name}.sync-time",
                "The time at which the digitisers were synchronised. Seconds since the Unix Epoch.",
                "s",
                default=sync_time,
                initial_status=Sensor.Status.NOMINAL,
            ),
            Sensor(
                float,
                f"{stream.name}.bandwidth",
                "The analogue bandwidth of the digitised band",
                "Hz",
                default=acv.bandwidth,
                initial_status=Sensor.Status.NOMINAL,
            ),
            Sensor(
                float,
                f"{stream.name}.pass-bandwidth",
                "Bandwidth over which the CBF produces a flat response",
                "Hz",
                default=acv.pass_bandwidth,
                initial_status=Sensor.Status.NOMINAL,
            ),
            Sensor(
                int,
                f"{stream.name}.n-chans",
                "The number of frequency channels in this stream's overall output",
                default=stream.n_chans,
                initial_status=Sensor.Status.NOMINAL,
            ),
            Sensor(
                int,
                f"{stream.name}.n-chans-per-substream",
                "Number of channels in each substream for this data stream",
                default=stream.n_chans_per_substream,
                initial_status=Sensor.Status.NOMINAL,
            ),
            data_suspect_sensor,
        ]

        if isinstance(stream, product_config.GpucbfBaselineCorrelationProductsStream):

            def get_baseline_index(a1, a2):
                return a2 * (a2 + 1) // 2 + a1

            # Calculating the inputs given the index is hard. So we iterate through
            # combinations of inputs instead, and calculate the index, and update the
            # relevant entry in a LUT.
            bls_ordering = [None] * stream.n_baselines
            for a2 in range(n_ants):
                for a1 in range(a2 + 1):
                    for p1 in range(2):
                        for p2 in range(2):
                            idx = get_baseline_index(a1, a2) * 4 + p1 + p2 * 2
                            bls_ordering[idx] = (ants[a1, p1], ants[a2, p2])
            n_accs = round(
                stream.int_time * acv.adc_sample_rate / acv.n_samples_between_spectra
            )  # type: ignore

            xstream_sensors: List[Sensor] = [
                Sensor(
                    int,
                    f"{stream.name}.n-xengs",
                    "The number of X-engines in the instrument",
                    default=n_engines,
                    initial_status=Sensor.Status.NOMINAL,
                ),
                Sensor(
                    int,
                    f"{stream.name}.n-accs",
                    "The number of spectra that are accumulated per X-engine output",
                    default=n_accs,
                    initial_status=Sensor.Status.NOMINAL,
                ),
                Sensor(
                    float,
                    f"{stream.name}.int-time",
                    "The time, in seconds, for which the X-engines accumulate.",
                    "s",
                    default=stream.int_time,
                    initial_status=Sensor.Status.NOMINAL,
                ),
                Sensor(
                    int,
                    f"{stream.name}.xeng-out-bits-per-sample",
                    "X-engine output bits per sample. Per number, not complex pair- "
                    "Real and imaginary parts are both this wide",
                    default=stream.bits_per_sample,
                    initial_status=Sensor.Status.NOMINAL,
                ),
                Sensor(
                    str,
                    f"{stream.name}.bls-ordering",
                    "A string showing the output ordering of baseline data "
                    "produced by the X-engines in this instrument, as a list "
                    "of correlation pairs given by input label.",
                    default=str(bls_ordering),
                    initial_status=Sensor.Status.NOMINAL,
                ),
                Sensor(
                    int,
                    f"{stream.name}.n-bls",
                    "The number of baselines produced by this correlator instrument.",
                    default=stream.n_baselines,
                    initial_status=Sensor.Status.NOMINAL,
                ),
                Sensor(
                    int,
                    f"{stream.name}.payload-len",
                    "The payload size, in bytes, of the X-engine data stream packets",
                    default=GPUCBF_PACKET_PAYLOAD_BYTES,
                    initial_status=Sensor.Status.NOMINAL,
                ),
                SumSensor(
                    sensors,
                    f"{stream.name}.xeng-clip-cnt",
                    "Number of visibilities that saturated",
                    name_regex=re.compile(rf"{escaped_name}\.[0-9]+\.xeng-clip-cnt"),
                    n_children=stream.n_substreams,
                    auto_strategy=SensorSampler.Strategy.EVENT_RATE,
                    auto_strategy_parameters=(FAST_SENSOR_UPDATE_PERIOD, math.inf),
                ),
                SyncSensor(
                    sensors,
                    f"{stream.name}.rx.synchronised",
                    "For the latest accumulation, was data present from all F-Engines "
                    "for all X-Engines",
                    name_regex=re.compile(rf"{escaped_name}\.[0-9]+\.rx.synchronised"),
                    n_children=stream.n_substreams,
                ),
            ]
            stream_sensors.extend(xstream_sensors)
        elif isinstance(stream, product_config.GpucbfTiedArrayChannelisedVoltageStream):
            source_indices = str(list(range(stream.src_pol, n_inputs, 2)))
            bstream_sensors: List[Sensor] = [
                Sensor(
                    int,
                    f"{stream.name}.n-bengs",
                    "The number of B-engines in the instrument",
                    default=n_engines,
                    initial_status=Sensor.Status.NOMINAL,
                ),
                Sensor(
                    int,
                    f"{stream.name}.beng-out-bits-per-sample",
                    "B-engine output bits per sample. Per number, not complex pair- "
                    "Real and imaginary parts are both this wide",
                    default=stream.bits_per_sample,
                    initial_status=Sensor.Status.NOMINAL,
                ),
                Sensor(
                    int,
                    f"{stream.name}.spectra-per-heap",
                    "Number of spectra per heap of beamformer output",
                    default=stream.spectra_per_heap,
                    initial_status=Sensor.Status.NOMINAL,
                ),
                Sensor(
                    str,
                    f"{stream.name}.source-indices",
                    "The global input indices of the sources summed in this beam",
                    default=f"{source_indices}",
                    initial_status=Sensor.Status.NOMINAL,
                ),
                Sensor(
                    int,
                    f"{stream.name}.payload-len",
                    "The payload size, in bytes, of the B-engine data stream packets",
                    default=GPUCBF_PACKET_PAYLOAD_BYTES,
                    initial_status=Sensor.Status.NOMINAL,
                ),
                SumSensor(
                    sensors,
                    f"{stream.name}.beng-clip-cnt",
                    "Number of complex samples that saturated",
                    name_regex=re.compile(rf"{escaped_name}\.[0-9]+\.beng-clip-cnt"),
                    n_children=stream.n_substreams,
                    auto_strategy=SensorSampler.Strategy.EVENT_RATE,
                    auto_strategy_parameters=(FAST_SENSOR_UPDATE_PERIOD, math.inf),
                ),
                LatestSensor(
                    sensors,
                    float,
                    f"{stream.name}.quantiser-gain",
                    "Non-complex post-summation quantiser gain applied to this beam",
                    name_regex=re.compile(rf"{escaped_name}\.[0-9]+\.quantiser-gain"),
                ),
                LatestSensor(
                    sensors,
                    bytes,
                    f"{stream.name}.delay",
                    "The delay settings of the inputs for this beam",
                    name_regex=re.compile(rf"{escaped_name}\.[0-9]+\.delay"),
                ),
                LatestSensor(
                    sensors,
                    bytes,
                    f"{stream.name}.weight",
                    "The summing weights applied to all the inputs of this beam",
                    name_regex=re.compile(rf"{escaped_name}\.[0-9]+\.weight"),
                ),
            ]
            stream_sensors.extend(bstream_sensors)

        # Add all sensors for all streams
        for ss in stream_sensors:
            g.graph["stream_sensors"].add(ss)

        init_telstate: Dict[Union[str, Tuple[str, ...]], Any] = g.graph["init_telstate"]
        telstate_data = {
            "src_streams": [stream.antenna_channelised_voltage.normalised_name],
            "instrument_dev_name": "gpucbf",  # Made-up instrument name
            "bandwidth": acv.bandwidth,
            "n_chans_per_substream": stream.n_chans_per_substream,
        }
        if isinstance(stream, product_config.GpucbfBaselineCorrelationProductsStream):
            telstate_data.update(
                bls_ordering=bls_ordering,
                int_time=stream.int_time,
                n_accs=n_accs,
            )
        elif isinstance(stream, product_config.GpucbfTiedArrayChannelisedVoltageStream):
            telstate_data.update(
                spectra_per_heap=stream.spectra_per_heap,
            )
        for key, value in telstate_data.items():
            init_telstate[(stream.normalised_name, key)] = value

    # Factor of 2 is because samples are complex
    input_rate = acv.bandwidth * len(acv.src_streams) * acv.bits_per_sample / 8 * 2
    bw_scale = input_rate / (acv.n_substreams * defaults.XBGPU_MAX_SRC_DATA_RATE)

    # Compute how much memory to provide for input

    batch_size = (
        n_inputs
        * acv.n_spectra_per_heap
        * stream.n_chans_per_endpoint
        * COMPLEX
        * acv.bits_per_sample
        // 8
    )
    target_chunk_size = 64 * 1024**2
    # Performance is poor if the number of spectra per chunk is too low,
    # so we ensure at least 128 even if that means overshooting the
    # target chunk size.
    batches_per_chunk = math.ceil(max(128 / acv.n_spectra_per_heap, target_chunk_size / batch_size))
    chunk_size = batches_per_chunk * batch_size
    rx_reorder_tol = 2**29  # Default of --rx-reorder-tol option
    n_tx_items = 2  # Magic number default for xbgpu
    chunk_ticks = acv.n_samples_between_spectra * acv.n_spectra_per_heap * batches_per_chunk
    max_active_chunks = math.ceil(rx_reorder_tol / chunk_ticks) + 1  # Based on calc in xbgpu
    free_chunks = max_active_chunks + 8  # Magic number is from xbgpu
    # Memory allocated for buffering and reordering incoming data
    recv_buffer = free_chunks * chunk_size

    for i in range(n_engines):
        # One engine per section of the band
        xbgpu = ProductLogicalTask(f"xb.{base_name}.{i}", streams=streams, index=i)
        xbgpu.subsystem = "cbf"
        xbgpu.image = "katgpucbf"
        xbgpu.fake_katcp_server_cls = FakeXbgpuDeviceServer
        xbgpu.cpus = 0.5 * bw_scale if configuration.options.develop.less_resources else 1.5
        xbgpu.mem = 1024 + _mb(recv_buffer)
        if not configuration.options.develop.less_resources:
            xbgpu.cores = ["recv", "send"]
            xbgpu.numa_nodes = 0.5 * bw_scale  # It's easily starved of bandwidth
            taskset = ["taskset", "-c", "{cores[send]}"]
        else:
            taskset = []
        xbgpu.ports = ["port", "prometheus", "aiomonitor", "aiomonitor_webui", "aioconsole"]
        xbgpu.wait_ports = ["port", "prometheus"]
        # Note: we use just one name for the input stream, even though we only
        # subscribe to a single multicast group of many. Every xbgpu receives
        # data from every fgpu, so finer-grained tracking is not useful. For
        # the destination it is more useful.
        xbgpu.interfaces = [
            scheduler.InterfaceRequest(
                "gpucbf",
                infiniband=ibv,
                multicast_in={acv.name},
                multicast_out={(stream, i) for stream in streams},
            )
        ]
        xbgpu.interfaces[0].bandwidth_in = acv.data_rate() / n_engines
        xbgpu.interfaces[0].bandwidth_out = (
            sum(stream.data_rate() for stream in streams) / n_engines
        )
        xbgpu.gpus = [scheduler.GPURequest()]
        xbgpu.gpus[0].mem = 300 + _mb(3 * chunk_size)
        # This memory is independent of the number of beams, but is needed
        # only if there is at least one beam. So set it to 0 here then
        # update it if we see a beam.
        rand_state_size = 0
        for stream in streams:
            if isinstance(stream, product_config.GpucbfBaselineCorrelationProductsStream):
                # Compute how much memory to provide for output
                vis_size = (
                    stream.n_baselines
                    * stream.n_chans_per_substream
                    * stream.bits_per_sample
                    // 8
                    * COMPLEX
                )
                # intermediate accumulators (* 2 because they're 64-bit not 32-bit).
                # The code in correlation.py adjusts the number of accumulators
                # in a way that keeps them to roughly 100MB, but there is always at
                # least 1 and that could be larger.
                mid_vis_size = max(100 * 1024 * 1024, vis_size * 2)
                xbgpu.mem += _mb(vis_size * 5)  # Magic number is default in XSend class
                xbgpu.gpus[0].compute += 0.15 * bw_scale
                xbgpu.gpus[0].mem += _mb(n_tx_items * vis_size + mid_vis_size)
                # Minimum capability as a function of bits-per-sample, based on
                # tensor_core_correlation_kernel.mako from katgpucbf.xbgpu.
                min_compute_capability = {4: (7, 3), 8: (7, 2), 16: (7, 0)}
                xbgpu.gpus[0].min_compute_capability = min_compute_capability[acv.bits_per_sample]
            elif isinstance(stream, product_config.GpucbfTiedArrayChannelisedVoltageStream):
                elements = (
                    batches_per_chunk * stream.n_chans_per_substream * stream.spectra_per_heap
                )
                beam_size = elements * COMPLEX
                rand_state_size = elements * 24  # 24 is sizeof(randState_t)
                xbgpu.mem += _mb(n_tx_items * beam_size)
                # Allow 128 single-pol beams + 1 baseline-correlation-products for 80 antennas.
                xbgpu.gpus[0].compute += 0.125 / n_inputs * bw_scale
                xbgpu.gpus[0].mem += _mb(n_tx_items * beam_size)
        xbgpu.gpus[0].mem += _mb(rand_state_size)

        first_dig = acv.sources(0)[0]
        heap_time = acv.n_samples_between_spectra / acv.adc_sample_rate * acv.n_spectra_per_heap
        xbgpu.command = (
            ["schedrr"]
            + taskset
            + [
                "xbgpu",
                "--adc-sample-rate",
                str(first_dig.adc_sample_rate),
                "--array-size",
                str(len(acv.src_streams) // 2),  # 2 pols per antenna
                "--channels",
                str(acv.n_chans),
                "--channels-per-substream",
                str(acv.n_chans_per_substream),
                "--samples-between-spectra",
                str(acv.n_samples_between_spectra),
                "--jones-per-batch",
                str(acv.n_jones_per_batch),
                "--heaps-per-fengine-per-chunk",
                str(batches_per_chunk),
                "--channel-offset-value",
                str(i * acv.n_chans_per_substream),
                "--sample-bits",
                str(acv.bits_per_sample),
                "--recv-interface",
                "{interfaces[gpucbf].name}",
                "--send-interface",
                "{interfaces[gpucbf].name}",
                "--send-packet-payload",
                str(GPUCBF_PACKET_PAYLOAD_BYTES),
                "--sync-time",
                str(sync_time),
                "--katcp-port",
                "{ports[port]}",
                "--prometheus-port",
                "{ports[prometheus]}",
                "--aiomonitor",
                "--aiomonitor-port",
                "{ports[aiomonitor]}",
                "--aiomonitor-webui-port",
                "{ports[aiomonitor_webui]}",
                "--aioconsole-port",
                "{ports[aioconsole]}",
            ]
        )

        for stream in streams:
            if isinstance(stream, product_config.GpucbfBaselineCorrelationProductsStream):
                output_config = {
                    "name": escape_format(stream.name),
                    "heap_accumulation_threshold": round(stream.int_time / heap_time),
                    "dst": f"{{endpoints_vector[multicast.{stream.name}_spead][{i}]}}",
                }
                xbgpu.command += [
                    "--corrprod",
                    ",".join(f"{key}={value}" for (key, value) in output_config.items()),
                ]
            elif isinstance(stream, product_config.GpucbfTiedArrayChannelisedVoltageStream):
                output_config = {
                    "name": escape_format(stream.name),
                    "dst": f"{{endpoints_vector[multicast.{stream.name}_spead][{i}]}}",
                    "pol": stream.src_pol,
                }
                if stream.dither is not None:
                    output_config["dither"] = stream.dither
                xbgpu.command += [
                    "--beam",
                    ",".join(f"{key}={value}" for (key, value) in output_config.items()),
                ]

        if not configuration.options.develop.less_resources:
            xbgpu.command += [
                "--recv-affinity",
                "{cores[recv]}",
                "--send-affinity",
                "{cores[send]}",
            ]
        xbgpu.capabilities.append("SYS_NICE")  # For schedrr
        if ibv:
            # Enable cap_net_raw capability for access to raw QPs
            xbgpu.capabilities.append("NET_RAW")
            xbgpu.command += [
                "--recv-ibv",
                "--send-ibv",
            ]
            if not configuration.options.develop.less_resources:
                # Use the core number as completion vector. This ensures that
                # multiple instances on a machine will use distinct vectors.
                xbgpu.command += [
                    "--recv-comp-vector",
                    "{cores[recv]}",
                    "--send-comp-vector",
                    "{cores[send]}",
                ]
        xbgpu.command += streams[0].command_line_extra
        # xbgpu doesn't use katsdpservices for configuration, or telstate
        xbgpu.katsdpservices_config = False
        xbgpu.pass_telstate = False
        xbgpu.data_suspect_sensors = data_suspect_sensors
        xbgpu.data_suspect_range = (
            i * acv.n_chans_per_substream,
            (i + 1) * acv.n_chans_per_substream,
        )
        xbgpu.critical = False  # Can survive losing individual engines
        g.add_node(xbgpu)

        # Wire it up to the multicast streams
        src_multicast = find_node(g, f"multicast.{acv.name}")
        # depends_init is included purely to ensure sensible ordering
        # on the dashboard.
        g.add_edge(
            xbgpu,
            src_multicast,
            port="spead",
            depends_resolve=True,
            depends_init=True,
        )
        for dst_multicast in dst_multicasts:
            g.add_edge(xbgpu, dst_multicast, port="spead", depends_resolve=True)
        xbgpu.command += [
            f"{{endpoints_vector[multicast.{acv.name}_spead][{i}]}}",
        ]

        # Link it to the group, so that downstream tasks need only depend on the group.
        g.add_edge(xbgpu_group, xbgpu, depends_ready=True, depends_init=True)

        # Rename sensors that are relevant to the stream rather than the process
        for name in [
            "rx.timestamp",
            "rx.unixtime",
            "rx.missing-unixtime",
        ]:
            xbgpu.sensor_renames[name] = [f"{stream.name}.{i}.{name}" for stream in streams]

        # Rename sensors that are relevant to the stream rather than the Pipeline
        for stream in streams:
            if isinstance(stream, product_config.GpucbfBaselineCorrelationProductsStream):
                renames = ["chan-range", "rx.synchronised", "xeng-clip-cnt"]
            elif isinstance(stream, product_config.GpucbfTiedArrayChannelisedVoltageStream):
                renames = [
                    "chan-range",
                    "delay",
                    "quantiser-gain",
                    "weight",
                    "beng-clip-cnt",
                    "tx.next-timestamp",
                    "dither-seed",
                ]
            for name in renames:
                xbgpu.sensor_renames[f"{stream.name}.{name}"] = f"{stream.name}.{i}.{name}"

        xbgpu.static_gauges["xbgpu_expected_input_heaps_per_second"] = (
            acv.adc_sample_rate
            / (acv.n_samples_between_spectra * acv.n_spectra_per_heap)
            * len(acv.src_streams)
            / 2  # / 2 because each heap contains two pols
        )
        xbgpu.static_gauges["xbgpu_expected_engines"] = 1.0

    return xbgpu_group


def _make_cbf_simulator(
    g: networkx.MultiDiGraph,
    configuration: Configuration,
    stream: Union[
        product_config.SimBaselineCorrelationProductsStream,
        product_config.SimTiedArrayChannelisedVoltageStream,
    ],
) -> scheduler.LogicalNode:
    # Determine number of simulators to run.
    n_sim = 1
    if isinstance(stream, product_config.SimBaselineCorrelationProductsStream):
        while stream.n_vis / n_sim > _N32_32:
            n_sim *= 2
    ibv = not configuration.options.develop.disable_ibverbs

    def make_cbf_simulator_config(
        task: ProductPhysicalTask, resolver: "product_controller.Resolver"
    ) -> Dict[str, Any]:
        conf = {
            "cbf_channels": stream.n_chans,
            "cbf_adc_sample_rate": stream.adc_sample_rate,
            "cbf_bandwidth": stream.bandwidth,
            "cbf_substreams": stream.n_substreams,
            "cbf_ibv": ibv,
            "cbf_sync_time": configuration.simulation.start_time or time.time(),
            "cbf_sim_clock_ratio": configuration.simulation.clock_ratio,
            "servers": n_sim,
        }
        sources = configuration.simulation.sources
        if sources:
            conf["cbf_sim_sources"] = [{"description": s.description} for s in sources]
        if isinstance(stream, product_config.SimBaselineCorrelationProductsStream):
            conf.update({"cbf_int_time": stream.int_time, "max_packet_size": 2088})
        else:
            conf.update(
                {
                    "beamformer_timesteps": stream.spectra_per_heap,
                    "beamformer_bits": stream.bits_per_sample,
                }
            )
        conf["cbf_center_freq"] = float(stream.centre_frequency)
        conf["cbf_antennas"] = [
            {"description": ant.description}
            for ant in stream.antenna_channelised_voltage.antenna_objects
        ]
        return conf

    sim_group = LogicalGroup("sim." + stream.name)
    g.add_node(sim_group, config=make_cbf_simulator_config)
    multicast = LogicalMulticast(stream.name, stream.n_endpoints)
    g.add_node(multicast)
    g.add_edge(
        sim_group,
        multicast,
        port="spead",
        depends_resolve=True,
        config=lambda task, resolver, endpoint: {"cbf_spead": str(endpoint)},
    )
    g.add_edge(multicast, sim_group, depends_init=True, depends_ready=True)

    init_telstate: Dict[Union[str, Tuple[str, ...]], Any] = g.graph["init_telstate"]
    init_telstate[(stream.name, "src_streams")] = [stream.antenna_channelised_voltage.name]
    init_telstate[("sub", "band")] = stream.antenna_channelised_voltage.band

    for i in range(n_sim):
        sim = ProductLogicalTask(f"sim.{stream.name}.{i + 1}", streams=[stream], index=i)
        sim.subsystem = "sdp"
        sim.image = "katcbfsim"
        # create-*-stream is passed on the command-line instead of telstate
        # for now due to SR-462.
        if isinstance(stream, product_config.SimBaselineCorrelationProductsStream):
            sim.command = ["cbfsim.py", "--create-fx-stream", escape_format(stream.name)]
            # It's mostly GPU work, so not much CPU requirement. Scale for 2 CPUs for
            # 16 antennas, 32K, and cap it there (threads for compute and network).
            # cbf_vis is an overestimate since the simulator is not constrained to
            # power-of-two antenna counts like the real CBF.
            scale = stream.n_vis / n_sim / _N16_32
            sim.cpus = 2 * min(1.0, scale)
            # 4 entries per Jones matrix, complex64 each
            gains_size = stream.n_antennas * stream.n_chans * 4 * 8
            # Factor of 4 is conservative; only actually double-buffered
            sim.mem = (4 * _mb(stream.size) + _mb(gains_size)) / n_sim + 512
            sim.cores = [None, None]
            sim.gpus = [scheduler.GPURequest()]
            # Scale for 20% at 16 ant, 32K channels
            sim.gpus[0].compute = min(1.0, 0.2 * scale)
            sim.gpus[0].mem = (2 * _mb(stream.size) + _mb(gains_size)) / n_sim + 256
        else:
            sim.command = ["cbfsim.py", "--create-beamformer-stream", escape_format(stream.name)]
            # The beamformer simulator only simulates data shape, not content. The
            # CPU load thus scales only with network bandwidth. Scale for 2 CPUs at
            # L band, and cap it there since there are only 2 threads. Using 1.999
            # instead of 2.0 is to ensure rounding errors don't cause a tiny fraction
            # extra of CPU to be used.
            sim.cpus = min(2.0, 1.999 * stream.size / stream.int_time / 1712000000.0 / n_sim)
            sim.mem = 4 * _mb(stream.size) / n_sim + 512

        sim.ports = ["port"]
        if ibv:
            # The verbs send interface seems to create a large number of
            # file handles per stream, easily exceeding the default of
            # 1024.
            sim.taskinfo.container.docker.parameters = [{"key": "ulimit", "value": "nofile=8192"}]
            # Need to enable cap_net_raw capability if using recent mlx5 kernel module.
            sim.command = ["capambel", "-c", "cap_net_raw+p", "--"] + sim.command
            sim.capabilities.append("NET_RAW")  # For ibverbs raw QPs
        sim.interfaces = [scheduler.InterfaceRequest("cbf", infiniband=ibv)]
        sim.interfaces[0].bandwidth_out = stream.data_rate()
        sim.transitions = {
            CaptureBlockState.CAPTURING: [
                KatcpTransition(
                    "capture-start",
                    stream.name,
                    configuration.simulation.start_time or "{time}",
                    timeout=30,
                )
            ],
            CaptureBlockState.BURNDOWN: [KatcpTransition("capture-stop", stream.name, timeout=60)],
        }

        g.add_node(
            sim,
            config=lambda task, resolver, server_id=i + 1: {
                "cbf_interface": task.interfaces["cbf"].name,
                "server_id": server_id,
            },
        )
        g.add_edge(sim_group, sim, depends_ready=True, depends_init=True)
        g.add_edge(sim, sim_group, depends_resources=True)
    return sim_group


def _make_timeplot(
    g: networkx.MultiDiGraph,
    name: str,
    description: str,
    cpus: float,
    timeplot_buffer_mb: float,
    data_rate: float,
    extra_config: dict,
) -> scheduler.LogicalNode:
    """Common backend code for creating a single timeplot server."""
    multicast = find_node(g, "multicast.timeplot." + name)
    timeplot = ProductLogicalTask("timeplot." + name)
    timeplot.subsystem = "sdp"
    timeplot.image = "katsdpdisp"
    timeplot.command = ["time_plot.py"]
    timeplot.cpus = cpus
    # timeplot_buffer covers only the visibilities, but there are also flags
    # and various auxiliary buffers. Add 50% to give some headroom, and also
    # add a fixed amount since in very small arrays the 20% might not cover
    # the fixed-sized overheads (previously this was 20% but that seems to be
    # insufficient).
    timeplot.mem = timeplot_buffer_mb * 1.5 + 256
    timeplot.interfaces = [scheduler.InterfaceRequest("sdp_10g")]
    timeplot.interfaces[0].bandwidth_in = data_rate
    timeplot.ports = ["html_port"]
    timeplot.volumes = [CONFIG_VOL]
    timeplot.gui_urls = [
        {
            "title": "Signal Display",
            "description": f"Signal displays for {{0.subarray_product_id}} {description}",
            "href": "http://{0.host}:{0.ports[html_port]}/",
            "category": "Plot",
        }
    ]
    timeplot.critical = False

    g.add_node(
        timeplot,
        config=lambda task, resolver: dict(
            {
                "html_host": LOCALHOST if resolver.localhost else "",
                "config_base": os.path.join(CONFIG_VOL.container_path, ".katsdpdisp"),
                "spead_interface": task.interfaces["sdp_10g"].name,
                "memusage": -timeplot_buffer_mb,  # Negative value gives MB instead of %
            },
            **extra_config,
        ),
    )
    g.add_edge(
        timeplot,
        multicast,
        port="spead",
        depends_resolve=True,
        depends_init=True,
        depends_ready=True,
        config=lambda task, resolver, endpoint: {
            "spead": endpoint.host,
            "spead_port": endpoint.port,
        },
    )
    return timeplot


def _make_timeplot_correlator(
    g: networkx.MultiDiGraph, configuration: Configuration, stream: product_config.VisStream
) -> scheduler.LogicalNode:
    n_ingest = stream.n_servers

    # Exact requirement not known (also depends on number of users). Give it
    # 2 CPUs (max it can use) for 16 antennas, 32K channels and scale from there.
    # Lower bound (from inspection) to cater for fixed overheads.
    cpus = max(2 * min(1.0, stream.n_spectral_vis / _N16_32), 0.3)

    # Give timeplot enough memory for 256 time samples, but capped at 16GB.
    # This formula is based on data.py in katsdpdisp.
    percentiles = 5 * 8
    timeplot_slot = (stream.n_spectral_vis + stream.n_spectral_chans * percentiles) * 8
    timeplot_buffer = min(256 * timeplot_slot, 16 * 1024**3)

    return _make_timeplot(
        g,
        name=stream.name,
        description=stream.name,
        cpus=cpus,
        timeplot_buffer_mb=timeplot_buffer / 1024**2,
        data_rate=_correlator_timeplot_data_rate(stream) * n_ingest,
        extra_config={
            "l0_name": stream.name,
            "max_custom_signals": defaults.TIMEPLOT_MAX_CUSTOM_SIGNALS,
        },
    )


def _make_timeplot_beamformer(
    g: networkx.MultiDiGraph,
    configuration: Configuration,
    stream: product_config.TiedArrayChannelisedVoltageStreamBase,
) -> Tuple[scheduler.LogicalNode, scheduler.LogicalNode]:
    """Make timeplot server for the beamformer, plus a beamformer capture to feed it."""
    beamformer = _make_beamformer_engineering_pol(
        g, configuration, None, stream, f"bf_ingest_timeplot.{stream.name}", False, 0
    )
    beamformer.critical = False

    # It's a low-demand setup (only one signal). The CPU and memory numbers
    # could potentially be reduced further.
    timeplot = _make_timeplot(
        g,
        name=stream.name,
        description=stream.name,
        cpus=0.5,
        timeplot_buffer_mb=128,
        data_rate=_beamformer_timeplot_data_rate(stream),
        extra_config={"max_custom_signals": 2},
    )

    return beamformer, timeplot


def _correlator_timeplot_frame_size(stream: product_config.VisStream, n_cont_chans: int) -> int:
    """Approximate size of the data sent from one ingest process to timeplot, per heap"""
    # This is based on _init_ig_sd from katsdpingest/ingest_session.py
    n_perc_signals = 5 * 8
    n_spec_chans = stream.n_spectral_chans // stream.n_servers
    n_cont_chans //= stream.n_servers
    n_bls = stream.n_baselines
    # sd_data + sd_flags
    ans = n_spec_chans * defaults.TIMEPLOT_MAX_CUSTOM_SIGNALS * (BYTES_PER_VIS + BYTES_PER_FLAG)
    ans += defaults.TIMEPLOT_MAX_CUSTOM_SIGNALS * 4  # sd_data_index
    # sd_blmxdata + sd_blmxflags
    ans += n_cont_chans * n_bls * (BYTES_PER_VIS + BYTES_PER_FLAG)
    ans += n_bls * (BYTES_PER_VIS + BYTES_PER_VIS // COMPLEX)  # sd_timeseries + sd_timeseriesabs
    # sd_percspectrum + sd_percspectrumflags
    ans += n_spec_chans * n_perc_signals * (BYTES_PER_VIS // COMPLEX + BYTES_PER_FLAG)
    # input names are e.g. m012v -> 5 chars, 2 inputs per baseline
    ans += n_bls * 10  # bls_ordering
    ans += n_bls * 8 * 4  # sd_flag_fraction
    # There are a few scalar values, but that doesn't add up to enough to worry about
    return ans


def _correlator_timeplot_continuum_factor(stream: product_config.VisStream) -> int:
    factor = 1
    # Aim for about 256 signal display coarse channels
    while (
        stream.n_spectral_chans % (factor * stream.n_servers * 2) == 0
        and stream.n_spectral_chans // factor >= 384
    ):
        factor *= 2
    return factor


def _correlator_timeplot_data_rate(stream: product_config.VisStream) -> float:
    """Bandwidth for the correlator timeplot stream from a single ingest"""
    sd_continuum_factor = _correlator_timeplot_continuum_factor(stream)
    sd_frame_size = _correlator_timeplot_frame_size(
        stream, stream.n_spectral_chans // sd_continuum_factor
    )
    # The rates are low, so we allow plenty of padding in case the calculation is
    # missing something.
    return data_rate(sd_frame_size, stream.int_time, ratio=1.2, overhead=4096)


def _beamformer_timeplot_data_rate(
    stream: product_config.TiedArrayChannelisedVoltageStreamBase,
) -> float:
    """Bandwidth for the beamformer timeplot stream.

    Parameters
    ----------
    stream
        The single-pol beam stream.
    """
    # XXX The beamformer signal displays are still in development, but the
    # rates are tiny. Until the protocol is finalised we'll just hardcode a
    # number.
    return 1e6  # 1 MB/s


def n_cal_nodes(configuration: Configuration, stream: product_config.CalStream) -> int:
    """Number of processes used to implement cal for a particular output."""
    # Use single cal for 4K or less: it doesn't need the performance, and
    # a unified cal report is more convenient (revisit once split cal supports
    # a unified cal report).
    if configuration.options.develop.less_resources:
        return 2
    elif stream.vis.n_chans <= 4096:
        return 1
    else:
        return 4


def _adjust_ingest_output_channels(streams: Sequence[product_config.VisStream]) -> None:
    """Modify a group of visibility streams to set the output channels.

    They are widened where necessary to meet alignment requirements.
    """

    def lcm(a, b):
        return a // math.gcd(a, b) * b

    src = streams[0].baseline_correlation_products
    lo, hi = streams[0].output_channels
    alignment = 1
    # This is somewhat stricter than katsdpingest actually requires, which is
    # simply that each ingest node is aligned to the continuum factor.
    for stream in streams:
        alignment = lcm(alignment, stream.n_servers * stream.continuum_factor)
        lo = min(lo, stream.output_channels[0])
        hi = max(hi, stream.output_channels[1])
    assigned = (_round_down(lo, alignment), _round_up(hi, alignment))
    # Should always succeed if validation passed
    assert 0 <= assigned[0] < assigned[1] <= src.n_chans, "Aligning channels caused an overflow"
    for stream in streams:
        if assigned != stream.output_channels:
            logger.info(
                "Rounding output channels for %s from %s to %s",
                stream.name,
                stream.output_channels,
                assigned,
            )
        stream.output_channels = assigned


def _make_ingest(
    g: networkx.MultiDiGraph,
    configuration: Configuration,
    spectral: Optional[product_config.VisStream],
    continuum: Optional[product_config.VisStream],
) -> scheduler.LogicalNode:
    develop = configuration.options.develop

    primary = spectral if spectral is not None else continuum
    if primary is None:
        raise ValueError("At least one of spectral or continuum must be given")
    name = primary.name
    src = primary.baseline_correlation_products
    streams = []
    if spectral is not None:
        streams.append(spectral)
    if continuum is not None:
        streams.append(continuum)
    # Number of ingest nodes.
    n_ingest = primary.n_servers

    # Virtual ingest node which depends on the real ingest nodes, so that other
    # services can declare dependencies on ingest rather than individual nodes.
    ingest_group = LogicalGroup("ingest." + name)

    sd_continuum_factor = _correlator_timeplot_continuum_factor(primary)
    sd_spead_rate = _correlator_timeplot_data_rate(primary)
    output_channels_str = "{}:{}".format(*primary.output_channels)
    group_config = {
        "continuum_factor": continuum.continuum_factor if continuum is not None else 1,
        "sd_continuum_factor": sd_continuum_factor,
        "sd_spead_rate": sd_spead_rate,
        "cbf_ibv": not develop.disable_ibverbs,
        "cbf_name": src.name,
        "servers": n_ingest,
        "antenna_mask": primary.antennas,
        "output_int_time": primary.int_time,
        "sd_int_time": primary.int_time,
        "output_channels": output_channels_str,
        "sd_output_channels": output_channels_str,
        "use_data_suspect": True,
        "excise": primary.excise,
        "aiomonitor": True,
    }
    if spectral is not None:
        group_config.update(l0_spectral_name=spectral.name)
    if continuum is not None:
        group_config.update(l0_continuum_name=continuum.name)
    if isinstance(src, product_config.SimBaselineCorrelationProductsStream):
        group_config.update(clock_ratio=configuration.simulation.clock_ratio)
    g.add_node(ingest_group, config=lambda task, resolver: group_config)

    if spectral is not None:
        spectral_multicast = LogicalMulticast(spectral.name, n_ingest)
        g.add_node(spectral_multicast)
        g.add_edge(
            ingest_group,
            spectral_multicast,
            port="spead",
            depends_resolve=True,
            config=lambda task, resolver, endpoint: {"l0_spectral_spead": str(endpoint)},
        )
        g.add_edge(spectral_multicast, ingest_group, depends_init=True, depends_ready=True)
        # Signal display stream
        timeplot_multicast = LogicalMulticast("timeplot." + name, 1)
        g.add_node(timeplot_multicast)
        g.add_edge(
            ingest_group,
            timeplot_multicast,
            port="spead",
            depends_resolve=True,
            config=lambda task, resolver, endpoint: {"sdisp_spead": str(endpoint)},
        )
        g.add_edge(timeplot_multicast, ingest_group, depends_init=True, depends_ready=True)
    if continuum is not None:
        continuum_multicast = LogicalMulticast(continuum.name, n_ingest)
        g.add_node(continuum_multicast)
        g.add_edge(
            ingest_group,
            continuum_multicast,
            port="spead",
            depends_resolve=True,
            config=lambda task, resolver, endpoint: {"l0_continuum_spead": str(endpoint)},
        )
        g.add_edge(continuum_multicast, ingest_group, depends_init=True, depends_ready=True)

    src_multicast = find_node(g, "multicast." + src.name)
    g.add_edge(
        ingest_group,
        src_multicast,
        port="spead",
        depends_resolve=True,
        config=lambda task, resolver, endpoint: {"cbf_spead": str(endpoint)},
    )

    for i in range(1, n_ingest + 1):
        ingest = ProductLogicalTask(f"ingest.{name}.{i}", streams=streams, index=i - 1)
        ingest.subsystem = "sdp"
        if configuration.options.interface_mode:
            ingest.physical_factory = ProductFakePhysicalTask
        else:
            ingest.physical_factory = IngestTask
        ingest.fake_katcp_server_cls = FakeIngestDeviceServer
        ingest.image = "katsdpingest_" + normalise_gpu_name(defaults.INGEST_GPU_NAME)
        if develop.disable_ibverbs:
            ingest.command = ["ingest.py"]
        else:
            ingest.command = ["capambel", "-c", "cap_net_raw+p", "--", "ingest.py"]
            ingest.capabilities.append("NET_RAW")
        ingest.taskinfo.command.environment.setdefault("variables", []).extend(
            [{"name": "KATSDPSIGPROC_TUNE_MATCH", "value": defaults.KATSDPSIGPROC_TUNE_MATCH}]
        )
        ingest.ports = ["port", "aiomonitor_port", "aiomonitor_webui_port", "aioconsole_port"]
        ingest.wait_ports = ["port"]
        ingest.gpus = [scheduler.GPURequest()]
        if not develop.any_gpu:
            ingest.gpus[-1].name = defaults.INGEST_GPU_NAME
        # Scale for a full GPU for 32 antennas, 32K channels on one node
        scale = src.n_vis / _N32_32 / n_ingest
        ingest.gpus[0].compute = scale
        # Refer to
        # https://docs.google.com/spreadsheets/d/13LOMcUDV1P0wyh_VSgTOcbnhyfHKqRg5rfkQcmeXmq0/edit
        # We use slightly higher multipliers to be safe, as well as
        # conservatively using src_info instead of spectral_info.
        ingest.gpus[0].mem = (70 * src.n_vis + 168 * src.n_chans) / n_ingest / 1024**2 + 128
        # Provide 4 CPUs for 32 antennas, 32K channels (the number in a NUMA
        # node of an ingest machine). It might not actually need this much.
        # Cores are reserved solely to get NUMA affinity with the NIC.
        ingest.cpus = int(math.ceil(4.0 * scale))
        ingest.cores = [None] * ingest.cpus
        # Scale factor of 32 may be overly conservative: actual usage may be
        # only half this. This also leaves headroom for buffering L0 output.
        ingest.mem = 32 * _mb(src.size) / n_ingest + 4096
        ingest.transitions = CAPTURE_TRANSITIONS
        ingest.interfaces = [
            scheduler.InterfaceRequest(
                "cbf",
                affinity=not develop.disable_ibverbs,
                infiniband=not develop.disable_ibverbs,
            ),
            scheduler.InterfaceRequest("sdp_10g"),
        ]
        ingest.interfaces[0].bandwidth_in = src.data_rate() / n_ingest
        data_rate_out = 0.0
        if spectral is not None:
            data_rate_out += spectral.data_rate() + sd_spead_rate * n_ingest
        if continuum is not None:
            data_rate_out += continuum.data_rate()
        ingest.interfaces[1].bandwidth_out = data_rate_out / n_ingest

        def make_ingest_config(
            task: ProductPhysicalTask, resolver: "product_controller.Resolver", server_id: int = i
        ) -> Dict[str, Any]:
            conf = {"cbf_interface": task.interfaces["cbf"].name, "server_id": server_id}
            if spectral is not None:
                conf.update(l0_spectral_interface=task.interfaces["sdp_10g"].name)
                conf.update(sdisp_interface=task.interfaces["sdp_10g"].name)
            if continuum is not None:
                conf.update(l0_continuum_interface=task.interfaces["sdp_10g"].name)
            return conf

        g.add_node(ingest, config=make_ingest_config)
        # Connect to ingest_group. We need a ready dependency of the group on
        # the node, so that other nodes depending on the group indirectly wait
        # for all nodes; the resource dependency is to prevent the nodes being
        # started without also starting the group, which is necessary for
        # config.
        g.add_edge(ingest_group, ingest, depends_ready=True, depends_init=True)
        g.add_edge(ingest, ingest_group, depends_resources=True)
        g.add_edge(
            ingest, src_multicast, depends_resolve=True, depends_ready=True, depends_init=True
        )
    return ingest_group


def _make_cal(
    g: networkx.MultiDiGraph,
    configuration: Configuration,
    stream: product_config.CalStream,
    flags_streams: Sequence[product_config.FlagsStream],
) -> scheduler.LogicalNode:
    vis = stream.vis
    n_cal = n_cal_nodes(configuration, stream)

    # Some of the scaling in cal is non-linear, so we need to give smaller
    # arrays more CPU than might be suggested by linear scaling. In particular,
    # 32K mode is flagged by averaging down to 8K first, so flagging performance
    # (which tends to dominate runtime) scales as if there were 8K channels.
    # Longer integration times are also less efficient (because there are fixed
    # costs per scan, so we scale as if for 4s dumps if longer dumps are used.
    effective_vis = vis.n_baselines * min(8192, vis.n_chans)
    effective_int = min(vis.int_time, 4.0)
    # This scale factor gives 34 total CPUs for 64A, 32K, 4+s integration, which
    # will get clamped down slightly.
    cpus = 2e-6 * effective_vis / effective_int / n_cal
    # Always (except in development mode, with less_resources = True)
    # have at least a whole CPU for the pipeline.
    if not configuration.options.develop.less_resources:
        cpus = max(cpus, 1)
    # Reserve a separate CPU for the accumulator
    cpus += 1
    # Clamp to what the machines can provide
    cpus = min(cpus, 7.9)
    workers = max(1, int(math.ceil(cpus - 1)))
    # Main memory consumer is buffers for
    # - visibilities (complex64)
    # - flags (uint8)
    # - excision flags (single bit each)
    # - weights (float32)
    # There are also timestamps, but they're insignificant compared to the rest.
    slot_size = vis.n_vis * 13.125 / n_cal
    buffer_size = stream.slots * slot_size
    # Processing operations come in a few flavours:
    # - average over time: need O(1) extra slots
    # - average over frequency: needs far less memory than the above
    # - compute flags per baseline: works on 16 baselines at a time.
    # In each case we arbitrarily allow for 4 times the result, per worker.
    # There is also a time- and channel-averaged version (down to 1024 channels)
    # of the HH and VV products for each scan. We allow a factor of 2 for
    # operations that work on it (which cancels the factor of 1/2 for only
    # having 2 of the 4 pol products). The scaling by n_cal is because there
    # are 1024 channels *per node* while vis.n_chans is over all nodes.
    extra = max(workers / stream.slots, min(16 * workers, vis.n_baselines) / vis.n_baselines) * 4
    extra += stream.max_scans * 1024 * n_cal / (stream.slots * vis.n_chans)

    # Extra memory allocation for tasks that deal with bandpass calibration
    # solutions in telescope state. The exact size of these depends on how
    # often bandpass calibrators are visited, so the scale factor is a
    # thumb-suck. The scale factors are
    # - 2 pols per antenna
    # - 8 bytes per value (complex64)
    # - 200 solutions
    # - 3: conservative estimate of bloat from text-based pickling
    telstate_extra = vis.n_chans * vis.n_pols * vis.n_antennas * 8 * 3 * 200

    group_config = {
        "buffer_maxsize": buffer_size,
        "max_scans": stream.max_scans,
        "workers": workers,
        "l0_name": vis.name,
        "servers": n_cal,
        "aiomonitor": True,
    }
    group_config.update(stream.parameters)
    if isinstance(
        vis.baseline_correlation_products, product_config.SimBaselineCorrelationProductsStream
    ):
        group_config.update(clock_ratio=configuration.simulation.clock_ratio)

    # Virtual node which depends on the real cal nodes, so that other services
    # can declare dependencies on cal rather than individual nodes.
    cal_group = LogicalGroup(stream.name)
    g.add_node(cal_group, telstate_extra=telstate_extra, config=lambda task, resolver: group_config)
    src_multicast = find_node(g, "multicast." + vis.name)
    g.add_edge(
        cal_group,
        src_multicast,
        port="spead",
        depends_resolve=True,
        depends_init=True,
        depends_ready=True,
        config=lambda task, resolver, endpoint: {"l0_spead": str(endpoint)},
    )

    # Flags output
    flags_streams_base = []
    flags_multicasts = {}
    for flags_stream in flags_streams:
        flags_vis = flags_stream.vis
        cf = flags_vis.continuum_factor // vis.continuum_factor
        flags_streams_base.append(
            {
                "name": flags_stream.name,
                "src_stream": flags_vis.name,
                "continuum_factor": cf,
                "rate_ratio": flags_stream.rate_ratio,
            }
        )

        flags_multicast = LogicalMulticast(flags_stream.name, n_cal)
        flags_multicasts[flags_stream] = flags_multicast
        g.add_node(flags_multicast)
        g.add_edge(flags_multicast, cal_group, depends_init=True, depends_ready=True)

    dask_prefix = "/gui/{0.subarray_product.subarray_product_id}/{0.name}/cal-diagnostics"
    for i in range(1, n_cal + 1):
        cal = ProductLogicalTask(
            f"{stream.name}.{i}", streams=(stream,) + tuple(flags_streams), index=i - 1
        )
        cal.subsystem = "sdp"
        cal.fake_katcp_server_cls = FakeCalDeviceServer
        cal.image = "katsdpcal"
        cal.command = ["run_cal.py"]
        cal.cpus = cpus
        cal.mem = buffer_size * (1 + extra) / 1024**2 + 512
        cal.volumes = [DATA_VOL]
        cal.interfaces = [scheduler.InterfaceRequest("sdp_10g")]
        # Note: these scale the fixed overheads too, so is not strictly accurate.
        cal.interfaces[0].bandwidth_in = vis.data_rate() / n_cal
        cal.interfaces[0].bandwidth_out = sum(
            flags_stream.data_rate() / n_cal for flags in flags_streams
        )
        cal.ports = [
            "port",
            "dask_diagnostics",
            "aiomonitor_port",
            "aiomonitor_webui_port",
            "aioconsole_port",
        ]
        cal.wait_ports = ["port"]
        cal.gui_urls = [
            {
                "title": "Cal diagnostics",
                "description": "Dask diagnostics for {0.name}",
                "href": "http://{0.host}:{0.ports[dask_diagnostics]}" + dask_prefix + "/status",
                "category": "Plot",
            }
        ]
        cal.transitions = CAPTURE_TRANSITIONS
        cal.final_state = CaptureBlockState.POSTPROCESSING

        def make_cal_config(
            task: ProductPhysicalTask, resolver: "product_controller.Resolver", server_id: int = i
        ) -> Dict[str, Any]:
            cal_config = {
                "l0_interface": task.interfaces["sdp_10g"].name,
                "server_id": server_id,
                "dask_diagnostics": (
                    LOCALHOST if resolver.localhost else "",
                    task.ports["dask_diagnostics"],
                ),
                "dask_prefix": dask_prefix.format(task),
                "flags_streams": copy.deepcopy(flags_streams_base),
            }
            for flags_stream in cal_config["flags_streams"]:
                flags_stream["interface"] = task.interfaces["sdp_10g"].name
                flags_stream["endpoints"] = str(
                    task.flags_endpoints[flags_stream["name"]]  # type: ignore
                )
            return cal_config

        g.add_node(cal, config=make_cal_config)
        # Connect to cal_group. See comments in _make_ingest for explanation.
        g.add_edge(cal_group, cal, depends_ready=True, depends_init=True)
        g.add_edge(cal, cal_group, depends_resources=True)
        g.add_edge(cal, src_multicast, depends_resolve=True, depends_ready=True, depends_init=True)

        for flags_stream, flags_multicast in flags_multicasts.items():
            # A sneaky hack to capture the endpoint information for the flag
            # streams. This is used as a config= function, but instead of
            # returning config we just stash information in the task object
            # which is retrieved by make_cal_config.
            def add_flags_endpoint(
                task: ProductPhysicalTask,
                resolver: "product_controller.Resolver",
                endpoint: Endpoint,
                name: str = flags_stream.name,
            ) -> Dict[str, Any]:
                if not hasattr(task, "flags_endpoints"):
                    task.flags_endpoints = {}  # type: ignore
                task.flags_endpoints[name] = endpoint  # type: ignore
                return {}

            g.add_edge(
                cal, flags_multicast, port="spead", depends_resolve=True, config=add_flags_endpoint
            )

    g.graph["archived_streams"].append(stream.name)
    return cal_group


def _writer_mem_mb(dump_size, obj_size, n_substreams, workers, buffer_dumps, max_accum_dumps):
    """Compute memory requirement (in MB) for vis_writer or flag_writer"""
    # Estimate the size of the memory pool. An extra item is added because
    # the value is copied when added to the rechunker, although this does
    # not actually come from the memory pool.
    heap_size = dump_size / n_substreams
    memory_pool = (3 + 4 * n_substreams) * heap_size
    # Estimate the size of the in-flight objects for the workers.
    write_queue = buffer_dumps * dump_size
    accum_buffers = max_accum_dumps * dump_size if max_accum_dumps > 1 else 0

    # Socket buffer allocated per endpoint, but it is bounded at 256MB by the
    # OS config.
    socket_buffers = min(256 * 1024**2, heap_size) * n_substreams

    # Double the memory allocation to be on the safe side. This gives some
    # headroom for page cache etc.
    return 2 * _mb(memory_pool + write_queue + accum_buffers + socket_buffers) + 512


def _writer_max_accum_dumps(
    stream: product_config.VisStream, bytes_per_vis: int, max_channels: Optional[int]
) -> int:
    """Compute value of --obj-max-dumps for data writers.

    If `max_channels` is not ``None``, it indicates the maximum number of
    channels per chunk.
    """
    # Allow time accumulation up to 5 minutes (to bound data loss) or 32GB
    # (to bound memory usage).
    limit = min(300.0 / stream.int_time, 32 * 1024**3 / (stream.n_vis * bytes_per_vis))
    # Compute how many are needed to allow weights/flags to achieve the target
    # object size. The scaling by n_ingest_nodes is because this is also the
    # number of substreams, and katsdpdatawriter doesn't weld substreams.
    n_chans = stream.n_chans // stream.n_servers
    if max_channels is not None and max_channels < n_chans:
        n_chans = max_channels
    flag_size = stream.n_baselines * n_chans
    needed = defaults.WRITER_OBJECT_SIZE / flag_size
    # katsdpdatawriter only uses powers of two. While it would be legal to
    # pass a non-power-of-two as the max, we would be wasting memory.
    max_accum_dumps = 1
    while max_accum_dumps * 2 <= min(needed, limit):
        max_accum_dumps *= 2
    return max_accum_dumps


def _make_vis_writer(
    g: networkx.MultiDiGraph,
    configuration: Configuration,
    stream: product_config.VisStream,
    s3_name: str,
    local: bool,
    prefix: Optional[str] = None,
    max_channels: Optional[int] = None,
):
    output_name = prefix + "." + stream.name if prefix is not None else stream.name
    g.graph["archived_streams"].append(output_name)
    vis_writer = ProductLogicalTask("vis_writer." + output_name, streams=[stream])
    vis_writer.subsystem = "sdp"
    vis_writer.image = "katsdpdatawriter"
    vis_writer.command = ["vis_writer.py"]
    # Don't yet have a good idea of real CPU usage. For now assume that 32
    # antennas, 32K channels requires two CPUs (one for capture, one for
    # writing) and scale from there, while capping at 3.
    vis_writer.cpus = min(3, 2 * stream.n_vis / _N32_32)

    workers = 4 if local else 50
    max_accum_dumps = _writer_max_accum_dumps(stream, BYTES_PER_VFW, max_channels)
    # Buffer enough data for 45 seconds. We've seen the disk system throw a fit
    # and hang for 30 seconds at a time, and this should allow us to ride that
    # out.
    buffer_dumps = max(max_accum_dumps, int(math.ceil(45.0 / stream.int_time)))
    src_multicast = find_node(g, "multicast." + stream.name)
    assert isinstance(src_multicast, LogicalMulticast)
    n_substreams = src_multicast.n_addresses

    # Double the memory allocation to be on the safe side. This gives some
    # headroom for page cache etc.
    vis_writer.mem = _writer_mem_mb(
        stream.size,
        defaults.WRITER_OBJECT_SIZE,
        n_substreams,
        workers,
        buffer_dumps,
        max_accum_dumps,
    )
    vis_writer.ports = ["port", "aiomonitor_port", "aiomonitor_webui_port", "aioconsole_port"]
    vis_writer.wait_ports = ["port"]
    vis_writer.interfaces = [scheduler.InterfaceRequest("sdp_10g")]
    vis_writer.interfaces[0].bandwidth_in = stream.data_rate()
    if local:
        vis_writer.volumes = [OBJ_DATA_VOL]
    else:
        vis_writer.interfaces[0].backwidth_out = stream.data_rate()
        # Creds are passed on the command-line so that they are redacted from telstate.
        vis_writer.command.extend(
            [
                "--s3-access-key",
                "{resolver.s3_config[%s][write][access_key]}" % s3_name,
                "--s3-secret-key",
                "{resolver.s3_config[%s][write][secret_key]}" % s3_name,
            ]
        )
    vis_writer.transitions = CAPTURE_TRANSITIONS

    def make_vis_writer_config(
        task: ProductPhysicalTask, resolver: "product_controller.Resolver"
    ) -> Dict[str, Any]:
        conf = {
            "l0_name": stream.name,
            "l0_interface": task.interfaces["sdp_10g"].name,
            "obj_size_mb": defaults.WRITER_OBJECT_SIZE / 1e6,
            "obj_max_dumps": max_accum_dumps,
            "workers": workers,
            "buffer_dumps": buffer_dumps,
            "s3_endpoint_url": resolver.s3_config[s3_name]["read"]["url"],
            "s3_expiry_days": resolver.s3_config[s3_name].get("expiry_days", None),
            "direct_write": True,
            "aiomonitor": True,
        }
        if local:
            conf["npy_path"] = OBJ_DATA_VOL.container_path
        else:
            conf["s3_write_url"] = resolver.s3_config[s3_name]["write"]["url"]
        if prefix is not None:
            conf["new_name"] = output_name
        if max_channels is not None:
            conf["obj_max_channels"] = max_channels
        return conf

    g.add_node(vis_writer, config=make_vis_writer_config)
    g.add_edge(
        vis_writer,
        src_multicast,
        port="spead",
        depends_resolve=True,
        depends_init=True,
        depends_ready=True,
        config=lambda task, resolver, endpoint: {"l0_spead": str(endpoint)},
    )
    return vis_writer


def _make_flag_writer(
    g: networkx.MultiDiGraph,
    configuration: Configuration,
    stream: product_config.FlagsStream,
    s3_name: str,
    local: bool,
    prefix: Optional[str] = None,
    max_channels: Optional[int] = None,
) -> scheduler.LogicalNode:
    output_name = prefix + "." + stream.name if prefix is not None else stream.name
    g.graph["archived_streams"].append(output_name)
    flag_writer = ProductLogicalTask("flag_writer." + output_name, streams=[stream])
    flag_writer.subsystem = "sdp"
    flag_writer.image = "katsdpdatawriter"
    flag_writer.command = ["flag_writer.py"]

    flags_src = find_node(g, "multicast." + stream.name)
    assert isinstance(flags_src, LogicalMulticast)
    n_substreams = flags_src.n_addresses
    workers = 4
    max_accum_dumps = _writer_max_accum_dumps(stream.vis, BYTES_PER_FLAG, max_channels)
    # Buffer enough data for 45 seconds of real time. We've seen the disk
    # system throw a fit and hang for 30 seconds at a time, and this should
    # allow us to ride that out.
    buffer_dumps = max(max_accum_dumps, int(math.ceil(45.0 / stream.int_time * stream.rate_ratio)))

    # Don't yet have a good idea of real CPU usage. This formula is
    # copied from the vis writer.
    flag_writer.cpus = min(3, 2 * stream.n_vis / _N32_32)
    flag_writer.mem = _writer_mem_mb(
        stream.size,
        defaults.WRITER_OBJECT_SIZE,
        n_substreams,
        workers,
        buffer_dumps,
        max_accum_dumps,
    )
    flag_writer.ports = ["port", "aiomonitor_port", "aiomonitor_webui_port", "aioconsole_port"]
    flag_writer.wait_ports = ["port"]
    flag_writer.interfaces = [scheduler.InterfaceRequest("sdp_10g")]
    flag_writer.interfaces[0].bandwidth_in = stream.data_rate()
    if local:
        flag_writer.volumes = [OBJ_DATA_VOL]
    else:
        flag_writer.interfaces[0].bandwidth_out = flag_writer.interfaces[0].bandwidth_in
        # Creds are passed on the command-line so that they are redacted from telstate.
        flag_writer.command.extend(
            [
                "--s3-access-key",
                "{resolver.s3_config[%s][write][access_key]}" % s3_name,
                "--s3-secret-key",
                "{resolver.s3_config[%s][write][secret_key]}" % s3_name,
            ]
        )
    flag_writer.final_state = CaptureBlockState.POSTPROCESSING

    # Capture init / done are used to track progress of completing flags
    # for a specified capture block id - the writer itself is free running
    flag_writer.transitions = {
        CaptureBlockState.CAPTURING: [
            KatcpTransition("capture-init", "{capture_block_id}", timeout=30)
        ],
        CaptureBlockState.POSTPROCESSING: [
            KatcpTransition("capture-done", "{capture_block_id}", timeout=60)
        ],
    }

    def make_flag_writer_config(
        task: ProductPhysicalTask, resolver: "product_controller.Resolver"
    ) -> Dict[str, Any]:
        conf = {
            "flags_name": stream.name,
            "flags_interface": task.interfaces["sdp_10g"].name,
            "obj_size_mb": defaults.WRITER_OBJECT_SIZE / 1e6,
            "obj_max_dumps": max_accum_dumps,
            "workers": workers,
            "buffer_dumps": buffer_dumps,
            "s3_endpoint_url": resolver.s3_config[s3_name]["read"]["url"],
            "s3_expiry_days": resolver.s3_config[s3_name].get("expiry_days", None),
            "direct_write": True,
            "aiomonitor": True,
        }
        if local:
            conf["npy_path"] = OBJ_DATA_VOL.container_path
        else:
            conf["s3_write_url"] = resolver.s3_config[s3_name]["write"]["url"]
        if prefix is not None:
            conf["new_name"] = output_name
            conf["rename_src"] = {stream.vis.name: prefix + "." + stream.vis.name}
        if max_channels is not None:
            conf["obj_max_channels"] = max_channels
        return conf

    g.add_node(flag_writer, config=make_flag_writer_config)
    g.add_edge(
        flag_writer,
        flags_src,
        port="spead",
        depends_resolve=True,
        depends_init=True,
        depends_ready=True,
        config=lambda task, resolver, endpoint: {"flags_spead": str(endpoint)},
    )
    return flag_writer


def _make_imager_writers(
    g: networkx.MultiDiGraph,
    configuration: Configuration,
    s3_name: str,
    stream: product_config.ImageStream,
    max_channels: Optional[int] = None,
) -> None:
    """Make vis and flag writers for an imager output"""
    _make_vis_writer(
        g,
        configuration,
        stream.vis,
        s3_name=s3_name,
        local=False,
        prefix=stream.name,
        max_channels=max_channels,
    )
    _make_flag_writer(
        g,
        configuration,
        stream.flags,
        s3_name=s3_name,
        local=False,
        prefix=stream.name,
        max_channels=max_channels,
    )


def _make_beamformer_engineering_pol(
    g: networkx.MultiDiGraph,
    configuration: Configuration,
    stream: Optional[product_config.BeamformerEngineeringStream],
    src_stream: product_config.TiedArrayChannelisedVoltageStreamBase,
    node_name: str,
    ram: bool,
    idx: int,
) -> ProductLogicalTask:
    """Generate node for a single polarisation of beamformer engineering output.

    This handles two cases: either it is a capture to file, associated with a
    particular output in the config dict; or it is for the purpose of
    computing the signal display statistics, in which case it is associated with
    an input but no specific output.

    Parameters
    ----------
    g
        Graph under construction.
    configuration
        Product configuration.
    stream
        The beamformer engineering stream, if applicable. If ``None``, this is
        for signal displays.
    src_stream
        The tied-array-channelised-voltage stream
    node_name
        Name to use for the logical node
    ram
        Whether this node is for writing to ramdisk (ignored if `stream` is ``None``)
    idx
        Number of this source within the output (ignored if `stream` is ``None``)
    """
    timeplot = stream is None
    src_multicast = find_node(g, "multicast." + src_stream.name)
    assert isinstance(src_multicast, LogicalMulticast)
    ibv = not configuration.options.develop.disable_ibverbs

    bf_ingest = ProductLogicalTask(node_name, streams=[stream] if stream is not None else [])
    bf_ingest.subsystem = "sdp"
    bf_ingest.image = "katsdpbfingest"
    bf_ingest.command = ["schedrr", "bf_ingest.py"]
    bf_ingest.cpus = 2
    bf_ingest.cores = ["disk", "network"]
    bf_ingest.capabilities.append("SYS_NICE")
    if ibv:
        bf_ingest.command = ["schedrr", "capambel", "-c", "cap_net_raw+p", "--", "bf_ingest.py"]
        bf_ingest.capabilities.append("NET_RAW")  # For ibverbs raw QPs
    if timeplot or not ram:
        # Actual usage is about 600MB, more-or-less independent of the
        # parameters.
        bf_ingest.mem = 1024
    else:
        # When writing to tmpfs, the file is accounted as memory to our
        # process, so we need more memory allocation than there is
        # space in the ramdisk. This is only used for lab testing, so
        # we just hardcode a number.
        bf_ingest.mem = 220 * 1024
    # In general we want it on the same interface as the NIC, because
    # otherwise there is a tendency to drop packets. But for ramdisk capture
    # this won't cut it because the machine for this is dual-socket and we
    # can't have affinity to both the (single) NIC and to both memory
    # regions.
    bf_ingest.interfaces = [
        scheduler.InterfaceRequest("cbf", infiniband=ibv, affinity=timeplot or not ram)
    ]
    # XXX Even when there is enough network bandwidth, sharing a node with correlator
    # ingest seems to cause lots of dropped packets for both. Just force the
    # data rate up to 20Gb/s to prevent that sharing (two pols then use all 40Gb/s).
    bf_ingest.interfaces[0].bandwidth_in = max(src_stream.data_rate(), 20e9)
    if timeplot:
        bf_ingest.interfaces.append(scheduler.InterfaceRequest("sdp_10g"))
        bf_ingest.interfaces[-1].bandwidth_out = _beamformer_timeplot_data_rate(src_stream)
    else:
        volume_name = "bf_ram{}" if ram else "bf_ssd{}"
        bf_ingest.volumes = [
            scheduler.VolumeRequest(volume_name.format(idx), "/data", "RW", affinity=ram)
        ]
    bf_ingest.ports = ["port", "aiomonitor_port", "aiomonitor_webui_port", "aioconsole_port"]
    bf_ingest.wait_ports = ["port"]
    bf_ingest.transitions = CAPTURE_TRANSITIONS

    def make_beamformer_engineering_pol_config(
        task: ProductPhysicalTask, resolver: "product_controller.Resolver"
    ) -> Dict[str, Any]:
        config = {
            "affinity": [task.cores["disk"], task.cores["network"]],
            "interface": task.interfaces["cbf"].name,
            "ibv": ibv,
            "stream_name": src_stream.name,
            "aiomonitor": True,
        }
        if stream is None:
            config.update(
                {"stats_interface": task.interfaces["sdp_10g"].name, "stats_int_time": 1.0}
            )
        else:
            config.update(
                {
                    "file_base": "/data",
                    "direct_io": not ram,  # Can't use O_DIRECT on tmpfs
                    "channels": "{}:{}".format(*stream.output_channels),
                }
            )
        return config

    g.add_node(bf_ingest, config=make_beamformer_engineering_pol_config)
    g.add_edge(
        bf_ingest,
        src_multicast,
        port="spead",
        depends_resolve=True,
        depends_init=True,
        depends_ready=True,
        config=lambda task, resolver, endpoint: {"cbf_spead": str(endpoint)},
    )
    if timeplot:
        stats_multicast = LogicalMulticast(f"timeplot.{src_stream.name}", 1)
        g.add_edge(
            bf_ingest,
            stats_multicast,
            port="spead",
            depends_resolve=True,
            config=lambda task, resolver, endpoint: {"stats": str(endpoint)},
        )
        g.add_edge(stats_multicast, bf_ingest, depends_init=True, depends_ready=True)
    return bf_ingest


def _make_beamformer_engineering(
    g: networkx.MultiDiGraph,
    configuration: Configuration,
    stream: product_config.BeamformerEngineeringStream,
) -> Sequence[scheduler.LogicalTask]:
    """Generate nodes for beamformer engineering output."""
    ram = stream.store == "ram"
    nodes = []
    for i, src in enumerate(stream.tied_array_channelised_voltage):
        nodes.append(
            _make_beamformer_engineering_pol(
                g, configuration, stream, src, f"bf_ingest.{stream.name}.{i + 1}", ram, i
            )
        )
    return nodes


def _groupby(items: Iterable[_T], key: Callable[[_T], Any]) -> Iterable[Iterable[_T]]:
    """Group items that have a common property.

    Unlike :func:`itertools.groupby`, this does not require the matching
    items to be adjacent, because it sorts them by key (but this requires the
    keys to be comparable). It also does not return the keys for the groups.
    """
    for _, group in itertools.groupby(sorted(items, key=key), key=key):
        yield group


def build_logical_graph(
    configuration: Configuration, config_dict: dict, sensors: SensorSet
) -> networkx.MultiDiGraph:
    # We mutate the configuration to align output channels to requirements.
    configuration = copy.deepcopy(configuration)

    # Note: a few lambdas in this function have default arguments e.g.
    # stream=stream. This is needed because capturing a loop-local variable in
    # a lambda captures only the final value of the variable, not the value it
    # had in the loop iteration where the lambda occurred.

    archived_streams: List[str] = []
    # Immutable keys to add to telstate on startup. There is no telstate yet
    # on which to call telstate.join, so nested keys must be expressed as
    # tuples which are joined later.
    init_telstate: Dict[Union[str, Tuple[str, ...]], Any] = {
        "sdp_archived_streams": archived_streams,
        "sdp_config": config_dict,
    }

    # Sensors that are created for individual streams and managed by the
    # product controller.
    stream_sensors = SensorSet()

    g = networkx.MultiDiGraph(
        archived_streams=archived_streams,  # For access as g.graph['archived_streams']
        init_telstate=init_telstate,  # ditto
        config=lambda resolver: ({"host": LOCALHOST} if resolver.localhost else {}),
        stream_sensors=stream_sensors,
    )

    # Add SPEAD endpoints to the graph.
    for stream in configuration.streams:
        if isinstance(stream, (product_config.CbfStream, product_config.DigBasebandVoltageStream)):
            url = stream.url
            if url.scheme == "spead":
                node = LogicalMulticast(stream.name, endpoint=Endpoint(url.host, url.port))
                g.add_node(node)

    # cam2telstate node (optional: if we're simulating, we don't need it)
    cam2telstate: Optional[scheduler.LogicalNode] = None
    cam_http = configuration.by_class(product_config.CamHttpStream)
    if cam_http:
        cam2telstate = _make_cam2telstate(g, configuration, cam_http[0])

    # Simulators
    def dsim_key(stream: product_config.SimDigBasebandVoltageStream) -> Tuple[str, float]:
        """Key for dsim streams that should be run in the same process."""
        return (stream.antenna.name, stream.adc_sample_rate)

    for dig_streams in _groupby(
        configuration.by_class(product_config.SimDigBasebandVoltageStream), key=dsim_key
    ):
        _make_dsim(g, configuration, dig_streams)
    for stream in configuration.by_class(product_config.SimBaselineCorrelationProductsStream):
        _make_cbf_simulator(g, configuration, stream)
    for stream in configuration.by_class(product_config.SimTiedArrayChannelisedVoltageStream):
        _make_cbf_simulator(g, configuration, stream)
    # If we don't have cam2telstate, try to populate <ant>_observer ourselves
    # if the user has provided antenna descriptions.
    if cam2telstate is None:
        for stream in configuration.by_class(product_config.DigBasebandVoltageStream):
            _make_dig(g, stream)

    # Correlator
    for fgpu_streams in _groupby(
        configuration.by_class(product_config.GpucbfAntennaChannelisedVoltageStream), key=_fgpu_key
    ):
        _make_fgpu(g, configuration, fgpu_streams)
    for xbgpu_streams in _groupby(
        configuration.by_class(product_config.GpucbfTiedArrayChannelisedVoltageStream)
        + configuration.by_class(product_config.GpucbfBaselineCorrelationProductsStream),
        key=_xbgpu_key,
    ):
        _make_xbgpu(
            g,
            configuration,
            streams=xbgpu_streams,
            sensors=sensors,
        )

    # Pair up spectral and continuum L0 outputs
    l0_done = set()
    for stream in configuration.by_class(product_config.VisStream):
        if stream.continuum_factor == 1:
            for stream2 in configuration.by_class(product_config.VisStream):
                if (
                    stream2 not in l0_done
                    and stream2.continuum_factor > 1
                    and stream2.compatible(stream)
                ):
                    _adjust_ingest_output_channels([stream, stream2])
                    _make_ingest(g, configuration, stream, stream2)
                    _make_timeplot_correlator(g, configuration, stream)
                    l0_done.add(stream)
                    l0_done.add(stream2)
                    break

    l0_spectral_only = False
    l0_continuum_only = False
    for stream in set(configuration.by_class(product_config.VisStream)) - l0_done:
        is_spectral = stream.continuum_factor == 1
        if is_spectral:
            l0_spectral_only = True
        else:
            l0_continuum_only = True
        _adjust_ingest_output_channels([stream])
        if is_spectral:
            _make_ingest(g, configuration, stream, None)
            _make_timeplot_correlator(g, configuration, stream)
        else:
            _make_ingest(g, configuration, None, stream)
    if l0_continuum_only and l0_spectral_only:
        logger.warning(
            "Both continuum-only and spectral-only L0 streams found - "
            "perhaps they were intended to be matched?"
        )

    for stream in configuration.by_class(product_config.VisStream):
        if stream.archive:
            _make_vis_writer(g, configuration, stream, "archive", local=True)

    for stream in configuration.by_class(product_config.CalStream):
        # Check for all corresponding flags outputs
        flags = []
        for flags_stream in configuration.by_class(product_config.FlagsStream):
            if flags_stream.cal is stream:
                flags.append(flags_stream)
        _make_cal(g, configuration, stream, flags)
        for flags_stream in flags:
            if flags_stream.archive:
                _make_flag_writer(g, configuration, flags_stream, "archive", local=True)

    for stream in configuration.by_class(product_config.BeamformerEngineeringStream):
        _make_beamformer_engineering(g, configuration, stream)

    # Collect all tied-array-channelised-voltage streams and make signal displays for them
    for stream in configuration.by_class(product_config.TiedArrayChannelisedVoltageStream):
        _make_timeplot_beamformer(g, configuration, stream)
    for stream in configuration.by_class(product_config.SimTiedArrayChannelisedVoltageStream):
        _make_timeplot_beamformer(g, configuration, stream)

    for stream in configuration.by_class(product_config.ContinuumImageStream):
        _make_imager_writers(g, configuration, "continuum", stream)
    for stream in configuration.by_class(product_config.SpectralImageStream):
        _make_imager_writers(
            g, configuration, "spectral", stream, defaults.SPECTRAL_OBJECT_CHANNELS
        )
    # Imagers are mostly handled in build_postprocess_logical_graph, but we create
    # capture block-independent metadata here.
    image_classes: List[Type[product_config.Stream]] = [
        product_config.ContinuumImageStream,
        product_config.SpectralImageStream,
    ]
    for cls_ in image_classes:
        for stream in configuration.by_class(cls_):
            archived_streams.append(stream.name)
            init_telstate[(stream.name, "src_streams")] = [s.name for s in stream.src_streams]
            init_telstate[(stream.name, "stream_type")] = stream.stream_type

    # telstate node. If no other node takes a reference to telstate, then we
    # don't create it.
    need_telstate = False
    for node in g:
        if isinstance(node, ProductLogicalTask):
            if node.pass_telstate or node.katsdpservices_config:
                need_telstate = True
    if need_telstate:
        telstate: Optional[scheduler.LogicalNode] = _make_telstate(g, configuration)
    else:
        telstate = None
    # XXX Technically meta_writer expects archived streams of type sdp.vis,
    # so this check is necessary but not sufficient (to skip RDB files in the lab)
    if need_telstate and archived_streams:
        meta_writer: Optional[scheduler.LogicalNode] = _make_meta_writer(g, configuration)
    else:
        meta_writer = None

    # Count large allocations in telstate, which affects memory usage of
    # telstate itself and any tasks that dump the contents of telstate.
    telstate_extra = 0
    for _node, data in g.nodes(True):
        telstate_extra += data.get("telstate_extra", 0)
    seen = set()
    for node in g:
        if isinstance(node, ProductLogicalTask):
            assert node.name not in seen, f"{node.name} appears twice in graph"
            seen.add(node.name)
            assert node.image in IMAGES, f"{node.image} missing from IMAGES"
            # Connect every task that needs it to telstate.
            # Also make them wait for cam2telstate.
            if telstate is not None and node is not telstate:
                if node.pass_telstate:
                    node.command.extend(
                        ["--telstate", "{endpoints[telstate_telstate]}", "--name", node.name]
                    )
                node.wrapper = configuration.options.wrapper
                g.add_edge(node, telstate, port="telstate", depends_ready=True, depends_kill=True)
                if cam2telstate is not None and node is not cam2telstate:
                    g.add_edge(node, cam2telstate, depends_ready=True)
            # Make sure meta_writer is the last task to be handled in capture-done
            if meta_writer is not None and node is not meta_writer:
                g.add_edge(meta_writer, node, depends_init=True)
            # Increase memory allocation where it depends on telstate content
            if node is telstate or node is meta_writer:
                node.mem += _mb(telstate_extra)
            # MESOS-7197 causes the master to crash if we ask for too little of
            # a resource. Enforce some minima.
            node.cpus = max(node.cpus, 0.01)
            for request in node.gpus:
                request.compute = max(request.compute, 0.01)
            # Bandwidths are typically large values. If they get large enough
            # then MESOS-8129 can bite (although this has only happened due to
            # misconfiguration). We don't need sub-bps precision, so just round
            # things off, which mitigates the problem.
            for request in node.interfaces:
                request.bandwidth_in = round(request.bandwidth_in)
                request.bandwidth_out = round(request.bandwidth_out)
            # Apply host overrides
            force_host: Optional[str] = None
            try:
                service_override = configuration.options.service_overrides[node.name]
                force_host = service_override.host
            except KeyError:
                pass
            if force_host is not None:
                node.host = force_host
    # For any tasks that don't pick a more specific fake implementation, use
    # either ProductFakePhysicalTask or FakePhysicalTask depending on
    # the original.
    if configuration.options.interface_mode:
        for node in g:
            if node.physical_factory == scheduler.PhysicalTask:
                node.physical_factory = scheduler.FakePhysicalTask
            elif node.physical_factory == ProductPhysicalTask:
                node.physical_factory = ProductFakePhysicalTask
            elif issubclass(node.physical_factory, scheduler.PhysicalTask):
                raise TypeError(f"{node.name} needs to specify a fake physical factory")

    return g


def _continuum_imager_cpus(configuration: Configuration) -> int:
    return 24 if not configuration.options.develop.less_resources else 2


def _spectral_imager_cpus(configuration: Configuration) -> int:
    # Fairly arbitrary number, based on looking at typical usage during a run.
    # In practice the number of spectral imagers per box is limited by GPUs,
    # so the value doesn't make a huge difference.
    return 3 if not configuration.options.develop.less_resources else 1


def _stream_url(capture_block_id: str, stream_name: str) -> str:
    url = "redis://{endpoints[telstate_telstate]}/"
    url += f"?capture_block_id={escape_format(urllib.parse.quote_plus(capture_block_id))}"
    url += f"&stream_name={escape_format(urllib.parse.quote_plus(stream_name))}"
    return url


def _sky_model_url(data_url: str, continuum_name: str, target: katpoint.Target) -> str:
    # data_url must have been returned by stream_url
    url = data_url
    url += f"&continuum={escape_format(urllib.parse.quote_plus(continuum_name))}"
    url += f"&target={escape_format(urllib.parse.quote_plus(target.description))}"
    url += "&format=katdal"
    return url


class TargetMapper:
    """Assign normalised names to targets.

    Each target passed to :meth:`__call__` is given a unique name containing
    only alphanumeric characters, dashes and underscores. Passing the same
    target will return the same name.

    This class is not thread-safe.
    """

    def __init__(self) -> None:
        self._cache: Dict[katpoint.Target, str] = {}
        self._used: Set[str] = set()

    def __call__(self, target: katpoint.Target) -> str:
        if target in self._cache:
            return self._cache[target]
        name = re.sub(r"[^-A-Za-z0-9_]", "_", target.name)
        if name in self._used:
            i = 1
            while f"{name}_{i}" in self._used:
                i += 1
            name = f"{name}_{i}"
        self._used.add(name)
        self._cache[target] = name
        return name


def _get_targets(
    configuration: Configuration,
    capture_block_id: str,
    stream: product_config.ImageStream,
    telstate_endpoint: str,
) -> Tuple[Sequence[Tuple[katpoint.Target, float]], katdal.DataSet]:
    """Identify all the targets to image.

    Parameter
    ---------
    configuration
        Configuration object
    capture_block_id
        Capture block ID
    stream
        The ``sdp.continuum_image`` or ``sdp.spectral_image`` stream
    telstate
        Root view of the telescope state
    min_time
        Skip targets that don't have at least this much observation time (in seconds).

    Returns
    -------
    targets
        Each key is the unique normalised target name, and the value is the
        target and the observation time on the target.
    data_set
        A katdal data set corresponding to the arguments.
    """
    l0_stream = stream.name + "." + stream.vis.name
    data_set = katdal.open(
        f"redis://{telstate_endpoint}",
        capture_block_id=capture_block_id,
        stream_name=l0_stream,
        chunk_store=None,
    )
    tracking = data_set.sensor.get("Observation/scan_state") == "track"
    target_sensor = data_set.sensor.get("Observation/target_index")
    targets = []
    for i, target in enumerate(data_set.catalogue):
        if target.body_type != "radec" or "target" not in target.tags:
            continue
        observed = tracking & (target_sensor == i)
        obs_time = np.sum(observed) * data_set.dump_period
        if obs_time >= stream.min_time:
            targets.append((target, obs_time))
        else:
            logger.info(
                "Skipping target %s: observed for %.1f seconds, threshold is %.1f",
                target.name,
                obs_time,
                stream.min_time,
            )
    return targets, data_set


def _render_continuum_parameters(parameters: Dict[str, Any]) -> str:
    """Turn a dictionary into a sequence of Python assignment statements.

    The keys must be valid Python identifiers.
    """
    return "; ".join(f"{key}={value!r}" for key, value in parameters.items())


async def _make_continuum_imager(
    g: networkx.MultiDiGraph,
    configuration: Configuration,
    capture_block_id: str,
    stream: product_config.ContinuumImageStream,
    telstate: katsdptelstate.aio.TelescopeState,
    telstate_endpoint: str,
    target_mapper: TargetMapper,
) -> None:
    l0_stream = stream.name + "." + stream.vis.name
    data_url = _stream_url(capture_block_id, l0_stream)
    cpus = _continuum_imager_cpus(configuration)
    targets = _get_targets(configuration, capture_block_id, stream, telstate_endpoint)[0]

    for target, obs_time in targets:
        target_name = target_mapper(target)
        imager = ProductLogicalTask(
            f"continuum_image.{stream.name}.{target_name}", streams=[stream]
        )
        imager.subsystem = "sdp"
        imager.cpus = cpus
        # These resources are very rough estimates
        imager.mem = 50000 if not configuration.options.develop.less_resources else 8000
        imager.disk = _mb(1000 * stream.vis.size + 1000)
        imager.max_run_time = 86400  # 24 hours
        imager.volumes = [DATA_VOL]
        imager.gpus = [scheduler.GPURequest()]
        # Just use a whole GPU - no benefit in time-sharing for batch tasks (unless
        # it can help improve parallelism). There is no memory enforcement and I
        # have no idea how much would be needed, so don't bother reserving memory.
        imager.gpus[0].compute = 1.0
        imager.image = "katsdpcontim"
        mfimage_parameters = dict(nThreads=cpus, **stream.mfimage_parameters)
        format_args = [  # Args to pass through str.format
            "run-and-cleanup",
            "/mnt/mesos/sandbox/{capture_block_id}_aipsdisk",
            "--",
            "continuum_pipeline.py",
            "--telstate",
            "{endpoints[telstate_telstate]}",
            "--access-key",
            "{resolver.s3_config[continuum][read][access_key]}",
            "--secret-key",
            "{resolver.s3_config[continuum][read][secret_key]}",
        ]
        no_format_args = [  # Args to protect from str.format
            "--select",
            f'scans="track"; corrprods="cross"; targets=[{target.description!r}]',
            "--capture-block-id",
            capture_block_id,
            "--output-id",
            stream.name,
            "--telstate-id",
            telstate.join(stream.name, target_name),
            "--outputdir",
            DATA_VOL.container_path,
            "--mfimage",
            _render_continuum_parameters(mfimage_parameters),
            "-w",
            "/mnt/mesos/sandbox",
        ]
        if stream.uvblavg_parameters:
            no_format_args.extend(
                ["--uvblavg", _render_continuum_parameters(stream.uvblavg_parameters)]
            )
        imager.command = format_args + [escape_format(arg) for arg in no_format_args] + [data_url]
        imager.katsdpservices_config = False
        imager.metadata_katcp_sensors = False
        imager.batch_data_time = obs_time
        g.add_node(imager)

    if not targets:
        logger.info("No continuum imager targets found for %s", capture_block_id)
    else:
        logger.info(
            "Continuum imager targets for %s: %s",
            capture_block_id,
            ", ".join(target.name for target, _ in targets),
        )
    view = telstate.view(telstate.join(capture_block_id, stream.name))
    await view.set("targets", {target.description: target_mapper(target) for target, _ in targets})


async def _make_spectral_imager(
    g: networkx.MultiDiGraph,
    configuration: Configuration,
    capture_block_id: str,
    stream: product_config.SpectralImageStream,
    telstate: katsdptelstate.aio.TelescopeState,
    telstate_endpoint: str,
    target_mapper: TargetMapper,
) -> Tuple[str, Sequence[scheduler.LogicalNode]]:
    dump_bytes = stream.vis.n_baselines * defaults.SPECTRAL_OBJECT_CHANNELS * BYTES_PER_VFW_SPECTRAL
    data_url = _stream_url(capture_block_id, stream.name + "." + stream.vis.name)
    targets, data_set = _get_targets(configuration, capture_block_id, stream, telstate_endpoint)
    band = data_set.spectral_windows[data_set.spw].band
    channel_freqs = data_set.channel_freqs * u.Hz
    del data_set  # Allow Python to recover the memory

    async with katsdpmodels.fetch.aiohttp.TelescopeStateFetcher(telstate) as fetcher:
        rfi_mask = await fetcher.get("model_rfi_mask_fixed", RFIMask)
        max_baseline = rfi_mask.max_baseline_length(channel_freqs)
        channel_mask = max_baseline > 0 * u.m
        acv = stream.vis.baseline_correlation_products.antenna_channelised_voltage
        telstate_acv = telstate.view(acv.name)
        band_mask = await fetcher.get("model_band_mask_fixed", BandMask, telstate=telstate_acv)
        channel_mask |= band_mask.is_masked(
            SpectralWindow(acv.bandwidth * u.Hz, acv.centre_frequency * u.Hz), channel_freqs
        )

    nodes = []
    for target, obs_time in targets:
        for i in range(0, stream.vis.n_chans, defaults.SPECTRAL_OBJECT_CHANNELS):
            first_channel = max(i, stream.output_channels[0])
            last_channel = min(
                i + defaults.SPECTRAL_OBJECT_CHANNELS, stream.vis.n_chans, stream.output_channels[1]
            )
            if first_channel >= last_channel:
                continue
            if np.all(channel_mask[first_channel:last_channel]):
                continue

            target_name = target_mapper(target)
            continuum_task: Optional[scheduler.LogicalNode] = None
            if stream.continuum is not None:
                continuum_name = stream.continuum.name + "." + target_name
                try:
                    continuum_task = find_node(g, "continuum_image." + continuum_name)
                except KeyError:
                    logger.warning(
                        "Skipping %s for %s because it was not found for %s",
                        target.name,
                        stream.name,
                        continuum_name,
                        extra=dict(capture_block_id=capture_block_id),
                    )
                    continue

            imager = ProductLogicalTask(
                "spectral_image.{}.{:05}-{:05}.{}".format(
                    stream.name, first_channel, last_channel, target_name
                ),
                streams=[stream],
                index=i,
            )
            imager.subsystem = "sdp"
            imager.cpus = _spectral_imager_cpus(configuration)
            # TODO: these resources are very rough estimates. The disk
            # estimate is conservative since it assumes no compression.
            dumps = int(round(obs_time / stream.vis.int_time))
            # 8 GB is usually enough, but there seem to be some rare cases
            # where memory usage keeps going up.
            imager.mem = 12 * 1024
            imager.disk = _mb(dump_bytes * dumps) + 1024
            imager.max_run_time = 6 * 3600  # 6 hours
            imager.volumes = [DATA_VOL]
            imager.gpus = [scheduler.GPURequest()]
            # Actual GPU utilisation is fairly low overall, and performance
            # could be improved by running two (or more) per GPU. However, the
            # imaging machines also run Ceph, and too many imagers can starve
            # Ceph of resources (with catastrophic consequences e.g. see
            # SPR1-904). Requesting a whole GPU is a quick-n-dirty way to limit
            # the number of imager tasks running.
            imager.gpus[0].compute = 1.0
            # There is no memory enforcement, so this doesn't have a lot of
            # padding.
            imager.gpus[0].memory = 3.0
            imager.image = "katsdpimager"
            imager.command = [
                "run-and-cleanup",
                "--create",
                "--tmp",
                "/mnt/mesos/sandbox/tmp",
                "--",
                "imager-mkat-pipeline.py",
                "-i",
                escape_format(f"target={target.description}"),
                "-i",
                "access-key={resolver.s3_config[spectral][read][access_key]}",
                "-i",
                "secret-key={resolver.s3_config[spectral][read][secret_key]}",
                "-i",
                "rfi-mask=fixed",
                "--stokes",
                "I",
                "--start-channel",
                str(first_channel),
                "--stop-channel",
                str(last_channel),
                "--major",
                "5",
                "--weight-type",
                "robust",
                "--channel-batch",
                str(defaults.SPECTRAL_OBJECT_CHANNELS),
                data_url,
                escape_format(DATA_VOL.container_path),
                escape_format(f"{capture_block_id}_{stream.name}_{target_name}"),
                escape_format(stream.name),
            ]
            if stream.continuum is not None:
                sky_model_url = _sky_model_url(data_url, stream.continuum.name, target)
                imager.command += ["--subtract", sky_model_url]
            if band in {"L", "UHF"}:  # Models are not available for other bands yet
                imager.command += ["--primary-beam", "meerkat"]
            for key, value in stream.parameters.items():
                imager.command += [f"--{key}".replace("_", "-"), escape_format(str(value))]

            imager.katsdpservices_config = False
            imager.metadata_katcp_sensors = False
            imager.batch_data_time = obs_time
            g.add_node(imager)
            nodes.append(imager)
            if continuum_task is not None:
                g.add_edge(imager, continuum_task, depends_finished=True)
    if not targets:
        logger.info("No spectral imager targets found for %s", capture_block_id)
    else:
        logger.info(
            "Spectral imager targets for %s: %s",
            capture_block_id,
            ", ".join(target.name for target, _ in targets),
        )
    view = telstate.view(telstate.join(capture_block_id, stream.name))
    await view.set("targets", {target.description: target_mapper(target) for target, _ in targets})
    await view.set(
        "output_channels",
        [
            channel
            for channel in range(stream.output_channels[0], stream.output_channels[1])
            if not channel_mask[channel]
        ],
    )
    return data_url, nodes


def _make_spectral_imager_report(
    g: networkx.MultiDiGraph,
    configuration: Configuration,
    capture_block_id: str,
    stream: product_config.SpectralImageStream,
    data_url: str,
    spectral_nodes: Sequence[scheduler.LogicalNode],
) -> scheduler.LogicalNode:
    report = ProductLogicalTask(f"spectral_image_report.{stream.name}", streams=[stream])
    report.subsystem = "sdp"
    report.cpus = 1.0
    # Memory is a guess - but since we don't open the chunk store it should be lightweight
    report.mem = 4 * 1024
    report.volumes = [DATA_VOL]
    report.image = "katsdpimager"
    report.katsdpservices_config = False
    report.metadata_katcp_sensors = False
    report.command = [
        "imager-mkat-report.py",
        data_url,
        escape_format(DATA_VOL.container_path),
        escape_format(f"{capture_block_id}_{stream.name}"),
        escape_format(stream.name),
    ]
    g.add_node(report)
    for node in spectral_nodes:
        g.add_edge(report, node, depends_finished=True, depends_finished_critical=False)
    return report


async def build_postprocess_logical_graph(
    configuration: Configuration,
    capture_block_id: str,
    telstate: katsdptelstate.aio.TelescopeState,
    telstate_endpoint: str,
) -> networkx.MultiDiGraph:
    g = networkx.MultiDiGraph()
    telstate_node = scheduler.LogicalExternal("telstate")
    g.add_node(telstate_node)
    # The postprocessing steps don't work well with interface mode because
    # they depend on examining state written by the containers, and this is
    # not currently simulated to the necessary level.
    if configuration.options.interface_mode:
        return g

    target_mapper = TargetMapper()

    for cstream in configuration.by_class(product_config.ContinuumImageStream):
        await _make_continuum_imager(
            g, configuration, capture_block_id, cstream, telstate, telstate_endpoint, target_mapper
        )

    # Note: this must only be run after all the sdp.continuum_image nodes have
    # been created, because spectral imager nodes depend on continuum imager
    # nodes.
    for sstream in configuration.by_class(product_config.SpectralImageStream):
        data_url, nodes = await _make_spectral_imager(
            g, configuration, capture_block_id, sstream, telstate, telstate_endpoint, target_mapper
        )
        if nodes:
            _make_spectral_imager_report(
                g, configuration, capture_block_id, sstream, data_url, nodes
            )

    seen = set()
    for node in g:
        if isinstance(node, ProductLogicalTask):
            # TODO: most of this code is shared by _build_logical_graph
            assert node.name not in seen, f"{node.name} appears twice in graph"
            seen.add(node.name)
            assert node.image in IMAGES, f"{node.image} missing from IMAGES"
            # Connect every task to telstate
            g.add_edge(node, telstate_node, port="telstate", depends_ready=True, depends_kill=True)

    return g
