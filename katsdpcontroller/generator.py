import math
import logging
import re
import time
import copy
import urllib.parse
import os.path
from typing import List, Dict, Tuple, Set, Sequence, Type, Union, Optional, Any, TYPE_CHECKING

import addict
import networkx
import numpy as np
import astropy.units as u

import katdal
import katdal.datasources
import katpoint
import katsdptelstate
from katsdptelstate.endpoint import Endpoint
import katsdpmodels.fetch.aiohttp
from katsdpmodels.rfi_mask import RFIMask

from . import scheduler, product_config, defaults
from .tasks import (
    SDPLogicalTask, SDPPhysicalTask, LogicalGroup,
    CaptureBlockState, KatcpTransition)
from .product_config import (
    BYTES_PER_VIS, BYTES_PER_FLAG, BYTES_PER_VFW, data_rate,
    Configuration)
# Import just for type annotations, but avoid at runtime due to circular imports
if TYPE_CHECKING:
    from . import product_controller      # noqa


def normalise_gpu_name(name):
    # Turn spaces and dashes into underscores, remove anything that isn't
    # alphanumeric or underscore, and lowercase (because Docker doesn't
    # allow uppercase in image names).
    mangled = re.sub('[- ]', '_', name.lower())
    mangled = re.sub('[^a-z0-9_]', '', mangled)
    return mangled


def escape_format(s: str) -> str:
    """Escape a string for :meth:`str.format`."""
    return s.replace('{', '{{').replace('}', '}}')


# Docker doesn't support IPv6 out of the box, and on some systems 'localhost'
# resolves to ::1, so force an IPv4 localhost.
LOCALHOST = '127.0.0.1'
CAPTURE_TRANSITIONS = {
    CaptureBlockState.CAPTURING: [
        KatcpTransition('capture-init', '{capture_block_id}', timeout=30)
    ],
    CaptureBlockState.BURNDOWN: [
        KatcpTransition('capture-done', timeout=240)
    ]
}
#: Docker images that may appear in the logical graph (used set to Docker image metadata)
IMAGES = frozenset([
    'katcbfsim',
    'katsdpbfingest',
    'katsdpcal',
    'katsdpcam2telstate',
    'katsdpcontim',
    'katsdpdisp',
    'katsdpimager',
    'katsdpingest_' + normalise_gpu_name(defaults.INGEST_GPU_NAME),
    'katsdpdatawriter',
    'katsdpmetawriter',
    'katsdptelstate'
])
#: Number of bytes used by spectral imager per visibility
BYTES_PER_VFW_SPECTRAL = 14.5       # 58 bytes for 4 polarisation products
#: Number of visibilities in a 32 antenna 32K channel dump, for scaling.
_N32_32 = 32 * 33 * 2 * 32768
#: Number of visibilities in a 16 antenna 32K channel dump, for scaling.
_N16_32 = 16 * 17 * 2 * 32768
#: Volume serviced by katsdptransfer to transfer results to the archive
DATA_VOL = scheduler.VolumeRequest('data', '/var/kat/data', 'RW')
#: Like DATA_VOL, but for high speed data to be transferred to an object store
OBJ_DATA_VOL = scheduler.VolumeRequest('obj_data', '/var/kat/data', 'RW')
#: Volume for persisting user configuration
CONFIG_VOL = scheduler.VolumeRequest('config', '/var/kat/config', 'RW')

logger = logging.getLogger(__name__)


class LogicalMulticast(scheduler.LogicalExternal):
    def __init__(self, name, n_addresses=None, endpoint=None):
        super().__init__(name)
        self.physical_factory = PhysicalMulticast
        self.n_addresses = n_addresses
        self.endpoint = endpoint
        if (self.n_addresses is None) == (self.endpoint is None):
            raise ValueError('Exactly one of n_addresses and endpoint must be specified')


class PhysicalMulticast(scheduler.PhysicalExternal):
    async def resolve(self, resolver, graph):
        await super().resolve(resolver, graph)
        if self.logical_node.endpoint is not None:
            self.host = self.logical_node.endpoint.host
            self.ports = {'spead': self.logical_node.endpoint.port}
        else:
            self.host = await resolver.resources.get_multicast_groups(self.logical_node.n_addresses)
            self.ports = {'spead': await resolver.resources.get_port()}


class TelstateTask(SDPPhysicalTask):
    async def resolve(self, resolver, graph):
        await super().resolve(resolver, graph)
        # Add a port mapping
        self.taskinfo.container.docker.network = 'BRIDGE'
        host_port = self.ports['telstate']
        if not resolver.localhost:
            portmap = addict.Dict()
            portmap.host_port = host_port
            portmap.container_port = 6379
            portmap.protocol = 'tcp'
            self.taskinfo.container.docker.port_mappings = [portmap]
        else:
            # Mesos doesn't provide a way to specify a port mapping with a
            # host-side binding, so we have to provide docker parameters
            # directly.
            parameters = self.taskinfo.container.docker.setdefault('parameters', [])
            parameters.append({'key': 'publish', 'value': f'{LOCALHOST}:{host_port}:6379'})


class IngestTask(SDPPhysicalTask):
    async def resolve(self, resolver, graph, image_path=None):
        # In develop mode, the GPU can be anything, and we need to pick a
        # matching image.
        if image_path is None:
            gpu = self.agent.gpus[self.allocation.gpus[0].index]
            gpu_name = normalise_gpu_name(gpu.name)
            image_path = await resolver.image_resolver('katsdpingest_' + gpu_name)
            if gpu != defaults.INGEST_GPU_NAME:
                logger.info('Develop mode: using %s for ingest', image_path)
        await super().resolve(resolver, graph, image_path)


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
    raise KeyError('no node called ' + name)


def _make_telstate(g: networkx.MultiDiGraph,
                   configuration: Configuration) -> scheduler.LogicalNode:
    # Pointing sensors can account for substantial memory per antennas over a
    # long-lived subarray.
    n_antennas = 0
    for stream in configuration.streams:
        # Note: counts both real and simulated antennas
        if isinstance(stream, product_config.AntennaChannelisedVoltageStreamBase):
            n_antennas += len(stream.antennas)

    telstate = SDPLogicalTask('telstate')
    # redis is nominally single-threaded, but has some helper threads
    # for background tasks so can occasionally exceed 1 CPU.
    telstate.cpus = 1.2 if not configuration.options.develop else 0.2
    telstate.mem = 2048 + 400 * n_antennas
    telstate.disk = telstate.mem
    telstate.image = 'katsdptelstate'
    telstate.ports = ['telstate']
    # Run it in /mnt/mesos/sandbox so that the dump.rdb ends up there.
    telstate.taskinfo.container.docker.setdefault('parameters', []).append(
        {'key': 'workdir', 'value': '/mnt/mesos/sandbox'})
    telstate.command = ['redis-server', '/usr/local/etc/redis/redis.conf']
    telstate.physical_factory = TelstateTask
    telstate.katsdpservices_logging = False
    telstate.katsdpservices_config = False
    telstate.final_state = CaptureBlockState.DEAD
    g.add_node(telstate)
    return telstate


def _make_cam2telstate(g: networkx.MultiDiGraph,
                       configuration: Configuration,
                       stream: product_config.CamHttpStream) -> scheduler.LogicalNode:
    cam2telstate = SDPLogicalTask('cam2telstate')
    cam2telstate.image = 'katsdpcam2telstate'
    cam2telstate.command = ['cam2telstate.py']
    cam2telstate.cpus = 0.75
    cam2telstate.mem = 256
    cam2telstate.ports = ['port', 'aiomonitor_port', 'aioconsole_port']
    cam2telstate.wait_ports = ['port']
    url = stream.url
    antennas = set()
    for input_ in configuration.by_class(product_config.AntennaChannelisedVoltageStream):
        antennas.update(input_.antennas)
    g.add_node(cam2telstate, config=lambda task, resolver: {
        'url': str(url),
        'aiomonitor': True,
        'receptors': ','.join(sorted(antennas))
    })
    return cam2telstate


def _make_meta_writer(g: networkx.MultiDiGraph,
                      configuration: Configuration) -> scheduler.LogicalNode:
    meta_writer = SDPLogicalTask('meta_writer')
    meta_writer.image = 'katsdpmetawriter'
    meta_writer.command = ['meta_writer.py']
    meta_writer.cpus = 0.2
    # Only a base allocation: it also gets telstate_extra added
    # meta_writer.mem = 256
    # Temporary workaround for SR-1695, until the machines can have their
    # kernels upgraded: give it the same memory as telescore state
    telstate = find_node(g, 'telstate')
    assert isinstance(telstate, scheduler.LogicalTask)
    meta_writer.mem = telstate.mem
    meta_writer.ports = ['port']
    meta_writer.volumes = [OBJ_DATA_VOL]
    meta_writer.interfaces = [scheduler.InterfaceRequest('sdp_10g')]
    # Actual required data rate is minimal, but bursty. Use 1 Gb/s,
    # except in development mode where it might not be available.
    meta_writer.interfaces[0].bandwidth_out = 1e9 if not configuration.options.develop else 10e6
    meta_writer.transitions = {
        CaptureBlockState.BURNDOWN: [
            KatcpTransition('write-meta', '{capture_block_id}', True, timeout=120)    # Light dump
        ],
        CaptureBlockState.DEAD: [
            KatcpTransition('write-meta', '{capture_block_id}', False, timeout=300)   # Full dump
        ]
    }
    meta_writer.final_state = CaptureBlockState.DEAD

    g.add_node(meta_writer, config=lambda task, resolver: {
        'rdb_path': OBJ_DATA_VOL.container_path
    })
    return meta_writer


def _make_cbf_simulator(g: networkx.MultiDiGraph,
                        configuration: Configuration,
                        stream: Union[
                            product_config.SimBaselineCorrelationProductsStream,
                            product_config.SimTiedArrayChannelisedVoltageStream
                        ]) -> scheduler.LogicalNode:
    # Determine number of simulators to run.
    n_sim = 1
    if isinstance(stream, product_config.SimBaselineCorrelationProductsStream):
        while stream.n_vis / n_sim > _N32_32:
            n_sim *= 2
    ibv = not configuration.options.develop

    def make_cbf_simulator_config(task: SDPPhysicalTask,
                                  resolver: 'product_controller.Resolver') -> Dict[str, Any]:
        conf = {
            'cbf_channels': stream.n_chans,
            'cbf_adc_sample_rate': stream.adc_sample_rate,
            'cbf_bandwidth': stream.bandwidth,
            'cbf_substreams': stream.n_substreams,
            'cbf_ibv': ibv,
            'cbf_sync_time': configuration.simulation.start_time or time.time(),
            'cbf_sim_clock_ratio': configuration.simulation.clock_ratio,
            'servers': n_sim
        }
        sources = configuration.simulation.sources
        if sources:
            conf['cbf_sim_sources'] = [{'description': s.description} for s in sources]
        if isinstance(stream, product_config.SimBaselineCorrelationProductsStream):
            conf.update({
                'cbf_int_time': stream.int_time,
                'max_packet_size': 2088
            })
        else:
            conf.update({
                'beamformer_timesteps': stream.spectra_per_heap,
                'beamformer_bits': stream.bits_per_sample
            })
        conf['cbf_center_freq'] = float(stream.centre_frequency)
        conf['cbf_antennas'] = [{'description': ant.description}
                                for ant in stream.antenna_channelised_voltage.antenna_objects]
        return conf

    sim_group = LogicalGroup('sim.' + stream.name)
    g.add_node(sim_group, config=make_cbf_simulator_config)
    multicast = LogicalMulticast('multicast.' + stream.name, stream.n_endpoints)
    g.add_node(multicast)
    g.add_edge(sim_group, multicast, port='spead', depends_resolve=True,
               config=lambda task, resolver, endpoint: {'cbf_spead': str(endpoint)})
    g.add_edge(multicast, sim_group, depends_init=True, depends_ready=True)

    for i in range(n_sim):
        sim = SDPLogicalTask('sim.{}.{}'.format(stream.name, i + 1))
        sim.image = 'katcbfsim'
        # create-*-stream is passed on the command-line instead of telstate
        # for now due to SR-462.
        if isinstance(stream, product_config.SimBaselineCorrelationProductsStream):
            sim.command = ['cbfsim.py', '--create-fx-stream', escape_format(stream.name)]
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
            sim.command = ['cbfsim.py', '--create-beamformer-stream', escape_format(stream.name)]
            # The beamformer simulator only simulates data shape, not content. The
            # CPU load thus scales only with network bandwidth. Scale for 2 CPUs at
            # L band, and cap it there since there are only 2 threads. Using 1.999
            # instead of 2.0 is to ensure rounding errors don't cause a tiny fraction
            # extra of CPU to be used.
            sim.cpus = min(2.0, 1.999 * stream.size / stream.int_time / 1712000000.0 / n_sim)
            sim.mem = 4 * _mb(stream.size) / n_sim + 512

        sim.ports = ['port']
        if ibv:
            # The verbs send interface seems to create a large number of
            # file handles per stream, easily exceeding the default of
            # 1024.
            sim.taskinfo.container.docker.parameters = [{"key": "ulimit", "value": "nofile=8192"}]
        sim.interfaces = [scheduler.InterfaceRequest('cbf', infiniband=ibv)]
        sim.interfaces[0].bandwidth_out = stream.data_rate()
        sim.transitions = {
            CaptureBlockState.CAPTURING: [
                KatcpTransition('capture-start', stream.name,
                                configuration.simulation.start_time or '{time}',
                                timeout=30)
            ],
            CaptureBlockState.BURNDOWN: [
                KatcpTransition('capture-stop', stream.name, timeout=60)
            ]
        }

        g.add_node(sim, config=lambda task, resolver, server_id=i + 1: {
            'cbf_interface': task.interfaces['cbf'].name,
            'server_id': server_id
        })
        g.add_edge(sim_group, sim, depends_ready=True, depends_init=True)
        g.add_edge(sim, sim_group, depends_resources=True)
    return sim_group


def _make_timeplot(g: networkx.MultiDiGraph, name: str, description: str,
                   cpus: float,
                   timeplot_buffer_mb: float,
                   data_rate: float,
                   extra_config: dict) -> scheduler.LogicalNode:
    """Common backend code for creating a single timeplot server."""
    multicast = find_node(g, 'multicast.timeplot.' + name)
    timeplot = SDPLogicalTask('timeplot.' + name)
    timeplot.image = 'katsdpdisp'
    timeplot.command = ['time_plot.py']
    timeplot.cpus = cpus
    # timeplot_buffer covers only the visibilities, but there are also flags
    # and various auxiliary buffers. Add 50% to give some headroom, and also
    # add a fixed amount since in very small arrays the 20% might not cover
    # the fixed-sized overheads (previously this was 20% but that seems to be
    # insufficient).
    timeplot.mem = timeplot_buffer_mb * 1.5 + 256
    timeplot.interfaces = [scheduler.InterfaceRequest('sdp_10g')]
    timeplot.interfaces[0].bandwidth_in = data_rate
    timeplot.ports = ['html_port']
    timeplot.volumes = [CONFIG_VOL]
    timeplot.gui_urls = [{
        'title': 'Signal Display',
        'description': 'Signal displays for {0.subarray_product_id} %s' % (description,),
        'href': 'http://{0.host}:{0.ports[html_port]}/',
        'category': 'Plot'
    }]
    timeplot.critical = False

    g.add_node(timeplot, config=lambda task, resolver: dict({
        'html_host': LOCALHOST if resolver.localhost else '',
        'config_base': os.path.join(CONFIG_VOL.container_path, '.katsdpdisp'),
        'spead_interface': task.interfaces['sdp_10g'].name,
        'memusage': -timeplot_buffer_mb     # Negative value gives MB instead of %
    }, **extra_config))
    g.add_edge(timeplot, multicast, port='spead',
               depends_resolve=True, depends_init=True, depends_ready=True,
               config=lambda task, resolver, endpoint: {
                   'spead': endpoint.host,
                   'spead_port': endpoint.port
               })
    return timeplot


def _make_timeplot_correlator(g: networkx.MultiDiGraph,
                              configuration: Configuration,
                              stream: product_config.VisStream) -> scheduler.LogicalNode:
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
        g, name=stream.name, description=stream.name,
        cpus=cpus, timeplot_buffer_mb=timeplot_buffer / 1024**2,
        data_rate=_correlator_timeplot_data_rate(stream) * n_ingest,
        extra_config={
            'l0_name': stream.name,
            'max_custom_signals': defaults.TIMEPLOT_MAX_CUSTOM_SIGNALS
        }
    )


def _make_timeplot_beamformer(
        g: networkx.MultiDiGraph,
        configuration: Configuration,
        stream: product_config.TiedArrayChannelisedVoltageStreamBase) -> \
            Tuple[scheduler.LogicalNode, scheduler.LogicalNode]:
    """Make timeplot server for the beamformer, plus a beamformer capture to feed it."""
    beamformer = _make_beamformer_engineering_pol(
        g, configuration, None, stream, f'bf_ingest_timeplot.{stream.name}', False, 0)
    beamformer.critical = False

    # It's a low-demand setup (only one signal). The CPU and memory numbers
    # could potentially be reduced further.
    timeplot = _make_timeplot(
        g, name=stream.name, description=stream.name,
        cpus=0.5, timeplot_buffer_mb=128,
        data_rate=_beamformer_timeplot_data_rate(stream),
        extra_config={'max_custom_signals': 2})

    return beamformer, timeplot


def _correlator_timeplot_frame_size(stream: product_config.VisStream,
                                    n_cont_chans: int) -> int:
    """Approximate size of the data sent from one ingest process to timeplot, per heap"""
    # This is based on _init_ig_sd from katsdpingest/ingest_session.py
    n_perc_signals = 5 * 8
    n_spec_chans = stream.n_spectral_chans // stream.n_servers
    n_cont_chans //= stream.n_servers
    n_bls = stream.n_baselines
    # sd_data + sd_flags
    ans = n_spec_chans * defaults.TIMEPLOT_MAX_CUSTOM_SIGNALS * (BYTES_PER_VIS + BYTES_PER_FLAG)
    ans += defaults.TIMEPLOT_MAX_CUSTOM_SIGNALS * 4          # sd_data_index
    # sd_blmxdata + sd_blmxflags
    ans += n_cont_chans * n_bls * (BYTES_PER_VIS + BYTES_PER_FLAG)
    ans += n_bls * (BYTES_PER_VIS + BYTES_PER_VIS // 2)    # sd_timeseries + sd_timeseriesabs
    # sd_percspectrum + sd_percspectrumflags
    ans += n_spec_chans * n_perc_signals * (BYTES_PER_VIS // 2 + BYTES_PER_FLAG)
    # input names are e.g. m012v -> 5 chars, 2 inputs per baseline
    ans += n_bls * 10                               # bls_ordering
    ans += n_bls * 8 * 4                            # sd_flag_fraction
    # There are a few scalar values, but that doesn't add up to enough to worry about
    return ans


def _correlator_timeplot_continuum_factor(stream: product_config.VisStream) -> int:
    factor = 1
    # Aim for about 256 signal display coarse channels
    while (stream.n_spectral_chans % (factor * stream.n_servers * 2) == 0
           and stream.n_spectral_chans // factor >= 384):
        factor *= 2
    return factor


def _correlator_timeplot_data_rate(stream: product_config.VisStream) -> float:
    """Bandwidth for the correlator timeplot stream from a single ingest"""
    sd_continuum_factor = _correlator_timeplot_continuum_factor(stream)
    sd_frame_size = _correlator_timeplot_frame_size(
        stream, stream.n_spectral_chans // sd_continuum_factor)
    # The rates are low, so we allow plenty of padding in case the calculation is
    # missing something.
    return data_rate(sd_frame_size, stream.int_time, ratio=1.2, overhead=4096)


def _beamformer_timeplot_data_rate(
        stream: product_config.TiedArrayChannelisedVoltageStreamBase) -> float:
    """Bandwidth for the beamformer timeplot stream.

    Parameters
    ----------
    stream
        The single-pol beam stream.
    """
    # XXX The beamformer signal displays are still in development, but the
    # rates are tiny. Until the protocol is finalised we'll just hardcode a
    # number.
    return 1e6    # 1 MB/s


def n_cal_nodes(configuration: Configuration,
                stream: product_config.CalStream) -> int:
    """Number of processes used to implement cal for a particular output."""
    # Use single cal for 4K or less: it doesn't need the performance, and
    # a unified cal report is more convenient (revisit once split cal supports
    # a unified cal report).
    if configuration.options.develop:
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
            logger.info('Rounding output channels for %s from %s to %s',
                        stream.name, stream.output_channels, assigned)
        stream.output_channels = assigned


def _make_ingest(g: networkx.MultiDiGraph, configuration: Configuration,
                 spectral: Optional[product_config.VisStream],
                 continuum: Optional[product_config.VisStream]) -> scheduler.LogicalNode:
    develop = configuration.options.develop

    primary = spectral if spectral is not None else continuum
    if primary is None:
        raise ValueError('At least one of spectral or continuum must be given')
    name = primary.name
    src = primary.baseline_correlation_products
    # Number of ingest nodes.
    n_ingest = primary.n_servers

    # Virtual ingest node which depends on the real ingest nodes, so that other
    # services can declare dependencies on ingest rather than individual nodes.
    ingest_group = LogicalGroup('ingest.' + name)

    sd_continuum_factor = _correlator_timeplot_continuum_factor(primary)
    sd_spead_rate = _correlator_timeplot_data_rate(primary)
    output_channels_str = '{}:{}'.format(*primary.output_channels)
    group_config = {
        'continuum_factor': continuum.continuum_factor if continuum is not None else 1,
        'sd_continuum_factor': sd_continuum_factor,
        'sd_spead_rate': sd_spead_rate,
        'cbf_ibv': not develop,
        'cbf_name': src.name,
        'servers': n_ingest,
        'antenna_mask': primary.antennas,
        'output_int_time': primary.int_time,
        'sd_int_time': primary.int_time,
        'output_channels': output_channels_str,
        'sd_output_channels': output_channels_str,
        'use_data_suspect': True,
        'aiomonitor': True
    }
    if spectral is not None:
        group_config.update(l0_spectral_name=spectral.name)
    if continuum is not None:
        group_config.update(l0_continuum_name=continuum.name)
    if isinstance(src, product_config.SimBaselineCorrelationProductsStream):
        group_config.update(clock_ratio=configuration.simulation.clock_ratio)
    g.add_node(ingest_group, config=lambda task, resolver: group_config)

    if spectral is not None:
        spectral_multicast = LogicalMulticast('multicast.' + spectral.name, n_ingest)
        g.add_node(spectral_multicast)
        g.add_edge(ingest_group, spectral_multicast, port='spead', depends_resolve=True,
                   config=lambda task, resolver, endpoint: {'l0_spectral_spead': str(endpoint)})
        g.add_edge(spectral_multicast, ingest_group, depends_init=True, depends_ready=True)
        # Signal display stream
        timeplot_multicast = LogicalMulticast('multicast.timeplot.' + name, 1)
        g.add_node(timeplot_multicast)
        g.add_edge(ingest_group, timeplot_multicast, port='spead', depends_resolve=True,
                   config=lambda task, resolver, endpoint: {'sdisp_spead': str(endpoint)})
        g.add_edge(timeplot_multicast, ingest_group, depends_init=True, depends_ready=True)
    if continuum is not None:
        continuum_multicast = LogicalMulticast('multicast.' + continuum.name, n_ingest)
        g.add_node(continuum_multicast)
        g.add_edge(ingest_group, continuum_multicast, port='spead', depends_resolve=True,
                   config=lambda task, resolver, endpoint: {'l0_continuum_spead': str(endpoint)})
        g.add_edge(continuum_multicast, ingest_group, depends_init=True, depends_ready=True)

    src_multicast = find_node(g, 'multicast.' + src.name)
    g.add_edge(ingest_group, src_multicast, port='spead', depends_resolve=True,
               config=lambda task, resolver, endpoint: {'cbf_spead': str(endpoint)})

    for i in range(1, n_ingest + 1):
        ingest = SDPLogicalTask('ingest.{}.{}'.format(name, i))
        ingest.physical_factory = IngestTask
        ingest.image = 'katsdpingest_' + normalise_gpu_name(defaults.INGEST_GPU_NAME)
        ingest.command = ['ingest.py']
        ingest.ports = ['port', 'aiomonitor_port', 'aioconsole_port']
        ingest.wait_ports = ['port']
        ingest.gpus = [scheduler.GPURequest()]
        if not develop:
            ingest.gpus[-1].name = defaults.INGEST_GPU_NAME
        # Scale for a full GPU for 32 antennas, 32K channels on one node
        scale = src.n_vis / _N32_32 / n_ingest
        ingest.gpus[0].compute = scale
        # Refer to
        # https://docs.google.com/spreadsheets/d/13LOMcUDV1P0wyh_VSgTOcbnhyfHKqRg5rfkQcmeXmq0/edit
        # We use slightly higher multipliers to be safe, as well as
        # conservatively using src_info instead of spectral_info.
        ingest.gpus[0].mem = \
            (70 * src.n_vis + 168 * src.n_chans) / n_ingest / 1024**2 + 128
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
            scheduler.InterfaceRequest('cbf', affinity=not develop, infiniband=not develop),
            scheduler.InterfaceRequest('sdp_10g')]
        ingest.interfaces[0].bandwidth_in = src.data_rate() / n_ingest
        data_rate_out = 0.0
        if spectral is not None:
            data_rate_out += spectral.data_rate() + sd_spead_rate * n_ingest
        if continuum is not None:
            data_rate_out += continuum.data_rate()
        ingest.interfaces[1].bandwidth_out = data_rate_out / n_ingest

        def make_ingest_config(task: SDPPhysicalTask,
                               resolver: 'product_controller.Resolver',
                               server_id: int = i) -> Dict[str, Any]:
            conf = {
                'cbf_interface': task.interfaces['cbf'].name,
                'server_id': server_id
            }
            if spectral is not None:
                conf.update(l0_spectral_interface=task.interfaces['sdp_10g'].name)
                conf.update(sdisp_interface=task.interfaces['sdp_10g'].name)
            if continuum is not None:
                conf.update(l0_continuum_interface=task.interfaces['sdp_10g'].name)
            return conf
        g.add_node(ingest, config=make_ingest_config)
        # Connect to ingest_group. We need a ready dependency of the group on
        # the node, so that other nodes depending on the group indirectly wait
        # for all nodes; the resource dependency is to prevent the nodes being
        # started without also starting the group, which is necessary for
        # config.
        g.add_edge(ingest_group, ingest, depends_ready=True, depends_init=True)
        g.add_edge(ingest, ingest_group, depends_resources=True)
        g.add_edge(ingest, src_multicast,
                   depends_resolve=True, depends_ready=True, depends_init=True)
    return ingest_group


def _make_cal(g: networkx.MultiDiGraph,
              configuration: Configuration,
              stream: product_config.CalStream,
              flags_streams: Sequence[product_config.FlagsStream]) -> scheduler.LogicalNode:
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
    # Always (except in development mode) have at least a whole CPU for the
    # pipeline.
    if not configuration.options.develop:
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
        'buffer_maxsize': buffer_size,
        'max_scans': stream.max_scans,
        'workers': workers,
        'l0_name': vis.name,
        'servers': n_cal,
        'aiomonitor': True
    }
    group_config.update(stream.parameters)
    if isinstance(vis.baseline_correlation_products,
                  product_config.SimBaselineCorrelationProductsStream):
        group_config.update(clock_ratio=configuration.simulation.clock_ratio)

    # Virtual node which depends on the real cal nodes, so that other services
    # can declare dependencies on cal rather than individual nodes.
    cal_group = LogicalGroup(stream.name)
    g.add_node(cal_group, telstate_extra=telstate_extra,
               config=lambda task, resolver: group_config)
    src_multicast = find_node(g, 'multicast.' + vis.name)
    g.add_edge(cal_group, src_multicast, port='spead',
               depends_resolve=True, depends_init=True, depends_ready=True,
               config=lambda task, resolver, endpoint: {
                   'l0_spead': str(endpoint)
               })

    # Flags output
    flags_streams_base = []
    flags_multicasts = {}
    for flags_stream in flags_streams:
        flags_vis = flags_stream.vis
        cf = flags_vis.continuum_factor // vis.continuum_factor
        flags_streams_base.append({
            'name': flags_stream.name,
            'src_stream': flags_vis.name,
            'continuum_factor': cf,
            'rate_ratio': flags_stream.rate_ratio
        })

        flags_multicast = LogicalMulticast('multicast.' + flags_stream.name, n_cal)
        flags_multicasts[flags_stream] = flags_multicast
        g.add_node(flags_multicast)
        g.add_edge(flags_multicast, cal_group, depends_init=True, depends_ready=True)

    dask_prefix = '/gui/{0.subarray_product.subarray_product_id}/{0.name}/cal-diagnostics'
    for i in range(1, n_cal + 1):
        cal = SDPLogicalTask('{}.{}'.format(stream.name, i))
        cal.image = 'katsdpcal'
        cal.command = ['run_cal.py']
        cal.cpus = cpus
        cal.mem = buffer_size * (1 + extra) / 1024**2 + 512
        cal.volumes = [DATA_VOL]
        cal.interfaces = [scheduler.InterfaceRequest('sdp_10g')]
        # Note: these scale the fixed overheads too, so is not strictly accurate.
        cal.interfaces[0].bandwidth_in = vis.data_rate() / n_cal
        cal.interfaces[0].bandwidth_out = sum(
            flags_stream.data_rate() / n_cal
            for flags in flags_streams
        )
        cal.ports = ['port', 'dask_diagnostics', 'aiomonitor_port', 'aioconsole_port']
        cal.wait_ports = ['port']
        cal.gui_urls = [{
            'title': 'Cal diagnostics',
            'description': 'Dask diagnostics for {0.name}',
            'href': 'http://{0.host}:{0.ports[dask_diagnostics]}' + dask_prefix + '/status',
            'category': 'Plot'
        }]
        cal.transitions = CAPTURE_TRANSITIONS
        cal.final_state = CaptureBlockState.POSTPROCESSING

        def make_cal_config(task: SDPPhysicalTask,
                            resolver: 'product_controller.Resolver',
                            server_id: int = i) -> Dict[str, Any]:
            cal_config = {
                'l0_interface': task.interfaces['sdp_10g'].name,
                'server_id': server_id,
                'dask_diagnostics': (LOCALHOST if resolver.localhost else '',
                                     task.ports['dask_diagnostics']),
                'dask_prefix': dask_prefix.format(task),
                'flags_streams': copy.deepcopy(flags_streams_base)
            }
            for flags_stream in cal_config['flags_streams']:
                flags_stream['interface'] = task.interfaces['sdp_10g'].name
                flags_stream['endpoints'] = str(
                        task.flags_endpoints[flags_stream['name']]   # type: ignore
                )
            return cal_config

        g.add_node(cal, config=make_cal_config)
        # Connect to cal_group. See comments in _make_ingest for explanation.
        g.add_edge(cal_group, cal, depends_ready=True, depends_init=True)
        g.add_edge(cal, cal_group, depends_resources=True)
        g.add_edge(cal, src_multicast,
                   depends_resolve=True, depends_ready=True, depends_init=True)

        for flags_stream, flags_multicast in flags_multicasts.items():
            # A sneaky hack to capture the endpoint information for the flag
            # streams. This is used as a config= function, but instead of
            # returning config we just stash information in the task object
            # which is retrieved by make_cal_config.
            def add_flags_endpoint(task: SDPPhysicalTask,
                                   resolver: 'product_controller.Resolver',
                                   endpoint: Endpoint,
                                   name: str = flags_stream.name) -> Dict[str, Any]:
                if not hasattr(task, 'flags_endpoints'):
                    task.flags_endpoints = {}                # type: ignore
                task.flags_endpoints[name] = endpoint        # type: ignore
                return {}

            g.add_edge(cal, flags_multicast, port='spead', depends_resolve=True,
                       config=add_flags_endpoint)

    g.graph['archived_streams'].append(stream.name)
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


def _writer_max_accum_dumps(stream: product_config.VisStream,
                            bytes_per_vis: int,
                            max_channels: Optional[int]) -> int:
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


def _make_vis_writer(g: networkx.MultiDiGraph,
                     configuration: Configuration,
                     stream: product_config.VisStream,
                     s3_name: str,
                     local: bool,
                     prefix: Optional[str] = None,
                     max_channels: Optional[int] = None):
    output_name = prefix + '.' + stream.name if prefix is not None else stream.name
    g.graph['archived_streams'].append(output_name)
    vis_writer = SDPLogicalTask('vis_writer.' + output_name)
    vis_writer.image = 'katsdpdatawriter'
    vis_writer.command = ['vis_writer.py']
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
    src_multicast = find_node(g, 'multicast.' + stream.name)
    assert isinstance(src_multicast, LogicalMulticast)
    n_substreams = src_multicast.n_addresses

    # Double the memory allocation to be on the safe side. This gives some
    # headroom for page cache etc.
    vis_writer.mem = _writer_mem_mb(stream.size, defaults.WRITER_OBJECT_SIZE, n_substreams,
                                    workers, buffer_dumps, max_accum_dumps)
    vis_writer.ports = ['port', 'aiomonitor_port', 'aioconsole_port']
    vis_writer.wait_ports = ['port']
    vis_writer.interfaces = [scheduler.InterfaceRequest('sdp_10g')]
    vis_writer.interfaces[0].bandwidth_in = stream.data_rate()
    if local:
        vis_writer.volumes = [OBJ_DATA_VOL]
    else:
        vis_writer.interfaces[0].backwidth_out = stream.data_rate()
        # Creds are passed on the command-line so that they are redacted from telstate.
        vis_writer.command.extend([
            '--s3-access-key', '{resolver.s3_config[%s][write][access_key]}' % s3_name,
            '--s3-secret-key', '{resolver.s3_config[%s][write][secret_key]}' % s3_name
        ])
    vis_writer.transitions = CAPTURE_TRANSITIONS

    def make_vis_writer_config(task: SDPPhysicalTask,
                               resolver: 'product_controller.Resolver') -> Dict[str, Any]:
        conf = {
            'l0_name': stream.name,
            'l0_interface': task.interfaces['sdp_10g'].name,
            'obj_size_mb': defaults.WRITER_OBJECT_SIZE / 1e6,
            'obj_max_dumps': max_accum_dumps,
            'workers': workers,
            'buffer_dumps': buffer_dumps,
            's3_endpoint_url': resolver.s3_config[s3_name]['read']['url'],
            's3_expiry_days': resolver.s3_config[s3_name].get('expiry_days', None),
            'direct_write': True,
            'aiomonitor': True
        }
        if local:
            conf['npy_path'] = OBJ_DATA_VOL.container_path
        else:
            conf['s3_write_url'] = resolver.s3_config[s3_name]['write']['url']
        if prefix is not None:
            conf['new_name'] = output_name
        if max_channels is not None:
            conf['obj_max_channels'] = max_channels
        return conf

    g.add_node(vis_writer, config=make_vis_writer_config)
    g.add_edge(vis_writer, src_multicast, port='spead',
               depends_resolve=True, depends_init=True, depends_ready=True,
               config=lambda task, resolver, endpoint: {'l0_spead': str(endpoint)})
    return vis_writer


def _make_flag_writer(g: networkx.MultiDiGraph,
                      configuration: Configuration,
                      stream: product_config.FlagsStream,
                      s3_name: str,
                      local: bool,
                      prefix: Optional[str] = None,
                      max_channels: Optional[int] = None) -> scheduler.LogicalNode:
    output_name = prefix + '.' + stream.name if prefix is not None else stream.name
    g.graph['archived_streams'].append(output_name)
    flag_writer = SDPLogicalTask('flag_writer.' + output_name)
    flag_writer.image = 'katsdpdatawriter'
    flag_writer.command = ['flag_writer.py']

    flags_src = find_node(g, 'multicast.' + stream.name)
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
    flag_writer.mem = _writer_mem_mb(stream.size, defaults.WRITER_OBJECT_SIZE, n_substreams,
                                     workers, buffer_dumps, max_accum_dumps)
    flag_writer.ports = ['port', 'aiomonitor_port', 'aioconsole_port']
    flag_writer.wait_ports = ['port']
    flag_writer.interfaces = [scheduler.InterfaceRequest('sdp_10g')]
    flag_writer.interfaces[0].bandwidth_in = stream.data_rate()
    if local:
        flag_writer.volumes = [OBJ_DATA_VOL]
    else:
        flag_writer.interfaces[0].bandwidth_out = flag_writer.interfaces[0].bandwidth_in
        # Creds are passed on the command-line so that they are redacted from telstate.
        flag_writer.command.extend([
            '--s3-access-key', '{resolver.s3_config[%s][write][access_key]}' % s3_name,
            '--s3-secret-key', '{resolver.s3_config[%s][write][secret_key]}' % s3_name
        ])
    flag_writer.final_state = CaptureBlockState.POSTPROCESSING

    # Capture init / done are used to track progress of completing flags
    # for a specified capture block id - the writer itself is free running
    flag_writer.transitions = {
        CaptureBlockState.CAPTURING: [
            KatcpTransition('capture-init', '{capture_block_id}', timeout=30)
        ],
        CaptureBlockState.POSTPROCESSING: [
            KatcpTransition('capture-done', '{capture_block_id}', timeout=60)
        ]
    }

    def make_flag_writer_config(task: SDPPhysicalTask,
                                resolver: 'product_controller.Resolver') -> Dict[str, Any]:
        conf = {
            'flags_name': stream.name,
            'flags_interface': task.interfaces['sdp_10g'].name,
            'obj_size_mb': defaults.WRITER_OBJECT_SIZE / 1e6,
            'obj_max_dumps': max_accum_dumps,
            'workers': workers,
            'buffer_dumps': buffer_dumps,
            's3_endpoint_url': resolver.s3_config[s3_name]['read']['url'],
            's3_expiry_days': resolver.s3_config[s3_name].get('expiry_days', None),
            'direct_write': True,
            'aiomonitor': True
        }
        if local:
            conf['npy_path'] = OBJ_DATA_VOL.container_path
        else:
            conf['s3_write_url'] = resolver.s3_config[s3_name]['write']['url']
        if prefix is not None:
            conf['new_name'] = output_name
            conf['rename_src'] = {stream.vis.name: prefix + '.' + stream.vis.name}
        if max_channels is not None:
            conf['obj_max_channels'] = max_channels
        return conf

    g.add_node(flag_writer, config=make_flag_writer_config)
    g.add_edge(flag_writer, flags_src, port='spead',
               depends_resolve=True, depends_init=True, depends_ready=True,
               config=lambda task, resolver, endpoint: {'flags_spead': str(endpoint)})
    return flag_writer


def _make_imager_writers(g: networkx.MultiDiGraph,
                         configuration: Configuration,
                         s3_name: str,
                         stream: product_config.ImageStream,
                         max_channels: Optional[int] = None) -> None:
    """Make vis and flag writers for an imager output"""
    _make_vis_writer(g, configuration, stream.vis, s3_name=s3_name,
                     local=False, prefix=stream.name, max_channels=max_channels)
    _make_flag_writer(g, configuration, stream.flags, s3_name=s3_name,
                      local=False, prefix=stream.name, max_channels=max_channels)


def _make_beamformer_engineering_pol(
        g: networkx.MultiDiGraph,
        configuration: Configuration,
        stream: Optional[product_config.BeamformerEngineeringStream],
        src_stream: product_config.TiedArrayChannelisedVoltageStreamBase,
        node_name: str,
        ram: bool,
        idx: int) -> SDPLogicalTask:
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
    develop
        Whether this is a develop-mode config
    """
    timeplot = stream is None
    src_multicast = find_node(g, 'multicast.' + src_stream.name)
    assert isinstance(src_multicast, LogicalMulticast)

    bf_ingest = SDPLogicalTask(node_name)
    bf_ingest.image = 'katsdpbfingest'
    bf_ingest.command = ['schedrr', 'bf_ingest.py']
    bf_ingest.cpus = 2
    bf_ingest.cores = ['disk', 'network']
    bf_ingest.capabilities.append('SYS_NICE')
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
        scheduler.InterfaceRequest('cbf',
                                   infiniband=not configuration.options.develop,
                                   affinity=timeplot or not ram)
    ]
    # XXX Even when there is enough network bandwidth, sharing a node with correlator
    # ingest seems to cause lots of dropped packets for both. Just force the
    # data rate up to 20Gb/s to prevent that sharing (two pols then use all 40Gb/s).
    bf_ingest.interfaces[0].bandwidth_in = max(src_stream.data_rate(), 20e9)
    if timeplot:
        bf_ingest.interfaces.append(scheduler.InterfaceRequest('sdp_10g'))
        bf_ingest.interfaces[-1].bandwidth_out = _beamformer_timeplot_data_rate(src_stream)
    else:
        volume_name = 'bf_ram{}' if ram else 'bf_ssd{}'
        bf_ingest.volumes = [
            scheduler.VolumeRequest(volume_name.format(idx), '/data', 'RW', affinity=ram)]
    bf_ingest.ports = ['port', 'aiomonitor_port', 'aioconsole_port']
    bf_ingest.wait_ports = ['port']
    bf_ingest.transitions = CAPTURE_TRANSITIONS

    def make_beamformer_engineering_pol_config(
            task: SDPPhysicalTask,
            resolver: 'product_controller.Resolver') -> Dict[str, Any]:
        config = {
            'affinity': [task.cores['disk'], task.cores['network']],
            'interface': task.interfaces['cbf'].name,
            'ibv': not configuration.options.develop,
            'stream_name': src_stream.name,
            'aiomonitor': True
        }
        if stream is None:
            config.update({
                'stats_interface': task.interfaces['sdp_10g'].name,
                'stats_int_time': 1.0
            })
        else:
            config.update({
                'file_base': '/data',
                'direct_io': not ram,       # Can't use O_DIRECT on tmpfs
                'channels': '{}:{}'.format(*stream.output_channels)
            })
        return config

    g.add_node(bf_ingest, config=make_beamformer_engineering_pol_config)
    g.add_edge(bf_ingest, src_multicast, port='spead',
               depends_resolve=True, depends_init=True, depends_ready=True,
               config=lambda task, resolver, endpoint: {'cbf_spead': str(endpoint)})
    if timeplot:
        stats_multicast = LogicalMulticast('multicast.timeplot.{}'.format(src_stream.name), 1)
        g.add_edge(bf_ingest, stats_multicast, port='spead',
                   depends_resolve=True,
                   config=lambda task, resolver, endpoint: {'stats': str(endpoint)})
        g.add_edge(stats_multicast, bf_ingest, depends_init=True, depends_ready=True)
    return bf_ingest


def _make_beamformer_engineering(
        g: networkx.MultiDiGraph,
        configuration: Configuration,
        stream: product_config.BeamformerEngineeringStream) -> Sequence[scheduler.LogicalTask]:
    """Generate nodes for beamformer engineering output.

    If `timeplot` is true, it generates services that only send data to
    timeplot servers, without capturing the data. In this case `name` may
    refer to an ``sdp.beamformer`` rather than an
    ``sdp.beamformer_engineering`` stream.
    """
    ram = stream.store == 'ram'
    nodes = []
    for i, src in enumerate(stream.tied_array_channelised_voltage):
        nodes.append(_make_beamformer_engineering_pol(
            g, configuration, stream, src,
            'bf_ingest.{}.{}'.format(stream.name, i + 1), ram, i))
    return nodes


def build_logical_graph(configuration: Configuration,
                        config_dict: dict) -> networkx.MultiDiGraph:
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
        'sdp_archived_streams': archived_streams,
        'sdp_config': config_dict
    }
    g = networkx.MultiDiGraph(
        archived_streams=archived_streams,  # For access as g.graph['archived_streams']
        init_telstate=init_telstate,        # ditto
        config=lambda resolver: ({'host': LOCALHOST} if resolver.localhost else {})
    )

    # telstate node
    telstate = _make_telstate(g, configuration)

    # Add SPEAD endpoints to the graph.
    input_multicast = []
    for stream in configuration.streams:
        if isinstance(stream, product_config.CbfStream):
            url = stream.url
            if url.scheme == 'spead':
                node = LogicalMulticast('multicast.' + stream.name,
                                        endpoint=Endpoint(url.host, url.port))
                g.add_node(node)
                input_multicast.append(node)

    # cam2telstate node (optional: if we're simulating, we don't need it)
    cam2telstate: Optional[scheduler.LogicalNode] = None
    cam_http = configuration.by_class(product_config.CamHttpStream)
    if cam_http:
        cam2telstate = _make_cam2telstate(g, configuration, cam_http[0])
        for node in input_multicast:
            g.add_edge(node, cam2telstate, depends_ready=True)

    # meta_writer node
    meta_writer = _make_meta_writer(g, configuration)

    # Simulators
    for stream in configuration.by_class(product_config.SimBaselineCorrelationProductsStream):
        _make_cbf_simulator(g, configuration, stream)
    for stream in configuration.by_class(product_config.SimTiedArrayChannelisedVoltageStream):
        _make_cbf_simulator(g, configuration, stream)

    # Pair up spectral and continuum L0 outputs
    l0_done = set()
    for stream in configuration.by_class(product_config.VisStream):
        if stream.continuum_factor == 1:
            for stream2 in configuration.by_class(product_config.VisStream):
                if (stream2 not in l0_done and stream2.continuum_factor > 1
                        and stream2.compatible(stream)):
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
        logger.warning('Both continuum-only and spectral-only L0 streams found - '
                       'perhaps they were intended to be matched?')

    for stream in configuration.by_class(product_config.VisStream):
        if stream.archive:
            _make_vis_writer(g, configuration, stream, 'archive', local=True)

    for stream in configuration.by_class(product_config.CalStream):
        # Check for all corresponding flags outputs
        flags = []
        for flags_stream in configuration.by_class(product_config.FlagsStream):
            if flags_stream.cal is stream:
                flags.append(flags_stream)
        _make_cal(g, configuration, stream, flags)
        for flags_stream in flags:
            if flags_stream.archive:
                _make_flag_writer(g, configuration, flags_stream, 'archive', local=True)

    for stream in configuration.by_class(product_config.BeamformerEngineeringStream):
        _make_beamformer_engineering(g, configuration, stream)

    # Collect all tied-array-channelised-voltage streams and make signal displays for them
    for stream in configuration.by_class(product_config.TiedArrayChannelisedVoltageStream):
        _make_timeplot_beamformer(g, configuration, stream)

    for stream in configuration.by_class(product_config.ContinuumImageStream):
        _make_imager_writers(g, configuration, 'continuum', stream)
    for stream in configuration.by_class(product_config.SpectralImageStream):
        _make_imager_writers(g, configuration, 'spectral', stream,
                             defaults.SPECTRAL_OBJECT_CHANNELS)
    # Imagers are mostly handled in build_postprocess_logical_graph, but we create
    # capture block-independent metadata here.
    image_classes: List[Type[product_config.Stream]] = [
        product_config.ContinuumImageStream,
        product_config.SpectralImageStream
    ]
    for cls_ in image_classes:
        for stream in configuration.by_class(cls_):
            archived_streams.append(stream.name)
            init_telstate[(stream.name, 'src_streams')] = [s.name for s in stream.src_streams]
            init_telstate[(stream.name, 'stream_type')] = stream.stream_type

    # Count large allocations in telstate, which affects memory usage of
    # telstate itself and any tasks that dump the contents of telstate.
    telstate_extra = 0
    for _node, data in g.nodes(True):
        telstate_extra += data.get('telstate_extra', 0)
    seen = set()
    for node in g:
        if isinstance(node, SDPLogicalTask):
            assert node.name not in seen, "{} appears twice in graph".format(node.name)
            seen.add(node.name)
            assert node.image in IMAGES, "{} missing from IMAGES".format(node.image)
            # Connect every task to telstate
            if node is not telstate:
                node.command.extend([
                    '--telstate', '{endpoints[telstate_telstate]}',
                    '--name', node.name])
                node.wrapper = configuration.options.wrapper
                g.add_edge(node, telstate, port='telstate',
                           depends_ready=True, depends_kill=True)
            # Make sure meta_writer is the last task to be handled in capture-done
            if node is not meta_writer:
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
    return g


def _continuum_imager_cpus(configuration: Configuration) -> int:
    return 24 if not configuration.options.develop else 2


def _spectral_imager_cpus(configuration: Configuration) -> int:
    # Fairly arbitrary number, based on looking at typical usage during a run.
    # In practice the number of spectral imagers per box is limited by GPUs,
    # so the value doesn't make a huge difference.
    return 2 if not configuration.options.develop else 1


def _stream_url(capture_block_id: str, stream_name: str) -> str:
    url = 'redis://{endpoints[telstate_telstate]}/'
    url += '?capture_block_id={}'.format(escape_format(urllib.parse.quote_plus(capture_block_id)))
    url += '&stream_name={}'.format(escape_format(urllib.parse.quote_plus(stream_name)))
    return url


def _sky_model_url(data_url: str, continuum_name: str, target: katpoint.Target) -> str:
    # data_url must have been returned by stream_url
    url = data_url
    url += '&continuum={}'.format(escape_format(urllib.parse.quote_plus(continuum_name)))
    url += '&target={}'.format(escape_format(urllib.parse.quote_plus(target.description)))
    url += '&format=katdal'
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
        name = re.sub(r'[^-A-Za-z0-9_]', '_', target.name)
        if name in self._used:
            i = 1
            while '{}_{}'.format(name, i) in self._used:
                i += 1
            name = '{}_{}'.format(name, i)
        self._used.add(name)
        self._cache[target] = name
        return name


def _get_data_set(telstate: katsdptelstate.TelescopeState,
                  capture_block_id: str, stream_name: str) -> katdal.DataSet:
    """Open a :class:`katdal.DataSet` from a live telescope state."""
    view, _, _ = katdal.datasources.view_l0_capture_stream(telstate, capture_block_id, stream_name)
    datasource = katdal.datasources.TelstateDataSource(
        view, capture_block_id, stream_name, chunk_store=None)
    dataset = katdal.VisibilityDataV4(datasource)
    return dataset


def _get_targets(configuration: Configuration,
                 capture_block_id: str,
                 stream: product_config.ImageStream,
                 telstate: katsdptelstate.TelescopeState) -> \
        Tuple[Sequence[Tuple[katpoint.Target, float]], Optional[katdal.DataSet]]:
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
        A katdal data set corresponding to the arguments. If `targets` is empty
        then this may be ``None``.
    """
    l0_stream = stream.name + '.' + stream.vis.name
    data_set = _get_data_set(telstate, capture_block_id, l0_stream)
    tracking = data_set.sensor.get('Observation/scan_state') == 'track'
    target_sensor = data_set.sensor.get('Observation/target_index')
    targets = []
    for i, target in enumerate(data_set.catalogue):
        if 'target' not in target.tags:
            continue
        observed = tracking & (target_sensor == i)
        obs_time = np.sum(observed) * data_set.dump_period
        if obs_time >= stream.min_time:
            targets.append((target, obs_time))
        else:
            logger.info('Skipping target %s: observed for %.1f seconds, threshold is %.1f',
                        target.name, obs_time, stream.min_time)
    return targets, data_set


def _render_continuum_parameters(parameters: Dict[str, Any]) -> str:
    """Turn a dictionary into a sequence of Python assignment statements.

    The keys must be valid Python identifiers.
    """
    return '; '.join('{}={!r}'.format(key, value) for key, value in parameters.items())


def _make_continuum_imager(g: networkx.MultiDiGraph,
                           configuration: Configuration,
                           capture_block_id: str,
                           stream: product_config.ContinuumImageStream,
                           telstate: katsdptelstate.TelescopeState,
                           target_mapper: TargetMapper) -> None:
    l0_stream = stream.name + '.' + stream.vis.name
    data_url = _stream_url(capture_block_id, l0_stream)
    cpus = _continuum_imager_cpus(configuration)
    targets = _get_targets(configuration, capture_block_id, stream, telstate)[0]

    for target, obs_time in targets:
        target_name = target_mapper(target)
        imager = SDPLogicalTask(f'continuum_image.{stream.name}.{target_name}')
        imager.cpus = cpus
        # These resources are very rough estimates
        imager.mem = 50000 if not configuration.options.develop else 8000
        imager.disk = _mb(1000 * stream.vis.size + 1000)
        imager.max_run_time = 86400     # 24 hours
        imager.volumes = [DATA_VOL]
        imager.gpus = [scheduler.GPURequest()]
        # Just use a whole GPU - no benefit in time-sharing for batch tasks (unless
        # it can help improve parallelism). There is no memory enforcement and I
        # have no idea how much would be needed, so don't bother reserving memory.
        imager.gpus[0].compute = 1.0
        imager.image = 'katsdpcontim'
        mfimage_parameters = dict(nThreads=cpus, **stream.mfimage_parameters)
        format_args = [         # Args to pass through str.format
            'run-and-cleanup', '/mnt/mesos/sandbox/{capture_block_id}_aipsdisk', '--',
            'continuum_pipeline.py',
            '--telstate', '{endpoints[telstate_telstate]}',
            '--access-key', '{resolver.s3_config[continuum][read][access_key]}',
            '--secret-key', '{resolver.s3_config[continuum][read][secret_key]}'
        ]
        no_format_args = [      # Args to protect from str.format
            '--select', f'scans="track"; corrprods="cross"; targets=[{target.description!r}]',
            '--capture-block-id', capture_block_id,
            '--output-id', stream.name,
            '--telstate-id', telstate.join(stream.name, target_name),
            '--outputdir', DATA_VOL.container_path,
            '--mfimage', _render_continuum_parameters(mfimage_parameters),
            '-w', '/mnt/mesos/sandbox',
        ]
        if stream.uvblavg_parameters:
            no_format_args.extend([
                '--uvblavg', _render_continuum_parameters(stream.uvblavg_parameters)
            ])
        imager.command = format_args + [escape_format(arg) for arg in no_format_args] + [data_url]
        imager.katsdpservices_config = False
        imager.batch_data_time = obs_time
        g.add_node(imager)

    if not targets:
        logger.info('No continuum imager targets found for %s', capture_block_id)
    else:
        logger.info('Continuum imager targets for %s: %s', capture_block_id,
                    ', '.join(target.name for target, _ in targets))
    view = telstate.view(telstate.join(capture_block_id, stream.name))
    view['targets'] = {target.description: target_mapper(target) for target, _ in targets}


async def _make_spectral_imager(g: networkx.MultiDiGraph,
                                configuration: Configuration,
                                capture_block_id: str,
                                stream: product_config.SpectralImageStream,
                                telstate: katsdptelstate.TelescopeState,
                                target_mapper: TargetMapper) -> \
        Tuple[str, Sequence[scheduler.LogicalNode]]:
    dump_bytes = stream.vis.n_baselines * defaults.SPECTRAL_OBJECT_CHANNELS * BYTES_PER_VFW_SPECTRAL
    data_url = _stream_url(capture_block_id, stream.name + '.' + stream.vis.name)
    targets, data_set = _get_targets(configuration, capture_block_id, stream, telstate)
    band = data_set.spectral_windows[data_set.spw].band if data_set is not None else ''
    channel_freqs = data_set.channel_freqs * u.Hz
    del data_set    # Allow Python to recover the memory

    rfi_mask_model_url = urllib.parse.urljoin(
        telstate['sdp_model_base_url'], telstate['model_rfi_mask_fixed']
    )
    with await katsdpmodels.fetch.aiohttp.fetch_model(rfi_mask_model_url, RFIMask) as rfi_mask:
        max_baseline = rfi_mask.max_baseline_length(channel_freqs)
        channel_mask = max_baseline > 0 * u.m

    nodes = []
    for target, obs_time in targets:
        for i in range(0, stream.vis.n_chans, defaults.SPECTRAL_OBJECT_CHANNELS):
            first_channel = max(i, stream.output_channels[0])
            last_channel = min(i + defaults.SPECTRAL_OBJECT_CHANNELS,
                               stream.vis.n_chans,
                               stream.output_channels[1])
            if first_channel >= last_channel:
                continue
            if np.all(channel_mask[first_channel:last_channel]):
                continue

            target_name = target_mapper(target)
            continuum_task: Optional[scheduler.LogicalNode] = None
            if stream.continuum is not None:
                continuum_name = stream.continuum.name + '.' + target_name
                try:
                    continuum_task = find_node(g, 'continuum_image.' + continuum_name)
                except KeyError:
                    logger.warning('Skipping %s for %s because it was not found for %s',
                                   target.name, stream.name, continuum_name,
                                   extra=dict(capture_block_id=capture_block_id))
                    continue

            imager = SDPLogicalTask('spectral_image.{}.{:05}-{:05}.{}'.format(
                stream.name, first_channel, last_channel, target_name))
            imager.cpus = _spectral_imager_cpus(configuration)
            # TODO: these resources are very rough estimates. The disk
            # estimate is conservative since it assumes no compression.
            dumps = int(round(obs_time / stream.vis.int_time))
            imager.mem = 15 * 1024
            imager.disk = _mb(dump_bytes * dumps) + 1024
            imager.max_run_time = 6 * 3600     # 6 hours
            imager.volumes = [DATA_VOL]
            imager.gpus = [scheduler.GPURequest()]
            # Just use a whole GPU - no benefit in time-sharing for batch tasks (unless
            # it can help improve parallelism). There is no memory enforcement, so
            # don't bother reserving memory.
            imager.gpus[0].compute = 1.0
            imager.image = 'katsdpimager'
            imager.command = [
                'run-and-cleanup', '--create', '--tmp', '/mnt/mesos/sandbox/tmp', '--',
                'imager-mkat-pipeline.py',
                '-i', escape_format('target={}'.format(target.description)),
                '-i', 'access-key={resolver.s3_config[spectral][read][access_key]}',
                '-i', 'secret-key={resolver.s3_config[spectral][read][secret_key]}',
                '-i', 'rfi-mask=fixed',
                '--stokes', 'IQUV',
                '--start-channel', str(first_channel),
                '--stop-channel', str(last_channel),
                '--major', '5',
                '--weight-type', 'robust',
                '--channel-batch', str(defaults.SPECTRAL_OBJECT_CHANNELS),
                data_url,
                escape_format(DATA_VOL.container_path),
                escape_format('{}_{}_{}'.format(capture_block_id, stream.name, target_name)),
                escape_format(stream.name)
            ]
            if stream.continuum is not None:
                sky_model_url = _sky_model_url(data_url, stream.continuum.name, target)
                imager.command += ['--subtract', sky_model_url]
            if band in {'L', 'UHF'}:      # Models are not available for other bands yet
                imager.command += ['--primary-beam', 'meerkat']
            for key, value in stream.parameters.items():
                imager.command += [f'--{key}'.replace('_', '-'), escape_format(str(value))]

            imager.katsdpservices_config = False
            imager.batch_data_time = obs_time
            g.add_node(imager)
            nodes.append(imager)
            if continuum_task is not None:
                g.add_edge(imager, continuum_task, depends_finished=True)
    if not targets:
        logger.info('No spectral imager targets found for %s', capture_block_id)
    else:
        logger.info('Spectral imager targets for %s: %s', capture_block_id,
                    ', '.join(target.name for target, _ in targets))
    view = telstate.view(telstate.join(capture_block_id, stream.name))
    view['targets'] = {target.description: target_mapper(target) for target, _ in targets}
    view['output_channels'] = [
        channel for channel in range(stream.output_channels[0], stream.output_channels[1])
        if not channel_mask[channel]
    ]
    return data_url, nodes


def _make_spectral_imager_report(
        g: networkx.MultiDiGraph,
        configuration: Configuration,
        capture_block_id: str,
        stream: product_config.SpectralImageStream,
        data_url: str,
        spectral_nodes: Sequence[scheduler.LogicalNode]) -> scheduler.LogicalNode:
    report = SDPLogicalTask(f'spectral_image_report.{stream.name}')
    report.cpus = 1.0
    # Memory is a guess - but since we don't open the chunk store it should be lightweight
    report.mem = 4 * 1024
    report.volumes = [DATA_VOL]
    report.image = 'katsdpimager'
    report.katsdpservices_config = False
    report.command = [
        'imager-mkat-report.py',
        data_url,
        escape_format(DATA_VOL.container_path),
        escape_format(f'{capture_block_id}_{stream.name}'),
        escape_format(stream.name)
    ]
    g.add_node(report)
    for node in spectral_nodes:
        g.add_edge(report, node, depends_finished=True, depends_finished_critical=False)
    return report


async def build_postprocess_logical_graph(
        configuration: Configuration,
        capture_block_id: str,
        telstate: katsdptelstate.TelescopeState) -> networkx.MultiDiGraph:
    g = networkx.MultiDiGraph()
    telstate_node = scheduler.LogicalExternal('telstate')
    g.add_node(telstate_node)

    target_mapper = TargetMapper()

    for cstream in configuration.by_class(product_config.ContinuumImageStream):
        _make_continuum_imager(g, configuration, capture_block_id, cstream, telstate, target_mapper)

    # Note: this must only be run after all the sdp.continuum_image nodes have
    # been created, because spectral imager nodes depend on continuum imager
    # nodes.
    for sstream in configuration.by_class(product_config.SpectralImageStream):
        data_url, nodes = _make_spectral_imager(g, configuration, capture_block_id, sstream,
                                                telstate, target_mapper)
        if nodes:
            await _make_spectral_imager_report(g, configuration, capture_block_id, sstream,
                                               data_url, nodes)

    seen = set()
    for node in g:
        if isinstance(node, SDPLogicalTask):
            # TODO: most of this code is shared by _build_logical_graph
            assert node.name not in seen, "{} appears twice in graph".format(node.name)
            seen.add(node.name)
            assert node.image in IMAGES, "{} missing from IMAGES".format(node.image)
            # Connect every task to telstate
            g.add_edge(node, telstate_node, port='telstate',
                       depends_ready=True, depends_kill=True)

    return g
