import math
import logging
import re
import time
import copy
import urllib
import os.path

import networkx

import addict

from katsdptelstate.endpoint import Endpoint, endpoint_list_parser

from katsdpcontroller import scheduler
from katsdpcontroller.tasks import (
    SDPLogicalTask, SDPPhysicalTask, SDPPhysicalTaskBase, LogicalGroup,
    CaptureBlockState, KatcpTransition)


INGEST_GPU_NAME = 'GeForce GTX TITAN X'
CAPTURE_TRANSITIONS = {
    CaptureBlockState.CAPTURING: [
        KatcpTransition('capture-init', '{capture_block_id}', timeout=30)
    ],
    CaptureBlockState.POSTPROCESSING: [
        KatcpTransition('capture-done', timeout=120)
    ]
}
#: Docker images that may appear in the logical graph (used set to Docker image metadata)
IMAGES = frozenset([
    'beamform',
    'katcbfsim',
    'katsdpbfingest',
    'katsdpcal',
    'katsdpcam2telstate',
    'katsdpcontim',
    'katsdpdisp',
    'katsdpingest_titanx',
    'katsdpdatawriter',
    'katsdpmetawriter',
    'katsdptelstate'
])
#: Number of visibilities in a 32 antenna 32K channel dump, for scaling.
_N32_32 = 32 * 33 * 2 * 32768
#: Number of visibilities in a 16 antenna 32K channel dump, for scaling.
_N16_32 = 16 * 17 * 2 * 32768
#: Maximum number of custom signals requested by (correlator) timeplot
TIMEPLOT_MAX_CUSTOM_SIGNALS = 256
#: Speed at which flags are transmitted, relative to real time
FLAGS_RATE_RATIO = 8.0
#: Volume serviced by katsdptransfer to transfer results to the archive
DATA_VOL = scheduler.VolumeRequest('data', '/var/kat/data', 'RW')
#: Like DATA_VOL, but for high speed data to be transferred to an object store
OBJ_DATA_VOL = scheduler.VolumeRequest('obj_data', '/var/kat/data', 'RW')
#: Volume for persisting user configuration
CONFIG_VOL = scheduler.VolumeRequest('config', '/var/kat/config', 'RW')
#: Target size of objects in the object store
WRITER_OBJECT_SIZE = 10e6    # 10 MB

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
    async def resolve(self, resolver, graph, loop):
        await super().resolve(resolver, graph, loop)
        if self.logical_node.endpoint is not None:
            self.host = self.logical_node.endpoint.host
            self.ports = {'spead': self.logical_node.endpoint.port}
        else:
            self.host = resolver.resources.get_multicast_ip(self.logical_node.n_addresses)
            self.ports = {'spead': resolver.resources.get_port()}


class TelstateTask(SDPPhysicalTaskBase):
    async def resolve(self, resolver, graph, loop):
        await super().resolve(resolver, graph, loop)
        # Add a port mapping
        self.taskinfo.container.docker.network = 'BRIDGE'
        portmap = addict.Dict()
        portmap.host_port = self.ports['telstate']
        portmap.container_port = 6379
        portmap.protocol = 'tcp'
        self.taskinfo.container.docker.port_mappings = [portmap]


class IngestTask(SDPPhysicalTask):
    async def resolve(self, resolver, graph, loop):
        await super().resolve(resolver, graph, loop)
        # In develop mode, the GPU can be anything, and we need to pick a
        # matching image. If it is the standard GPU, don't try to override
        # anything, but otherwise synthesize an image name by mangling the
        # GPU name.
        gpu = self.agent.gpus[self.allocation.gpus[0].index]
        if gpu.name != INGEST_GPU_NAME:
            # Turn spaces and dashes into underscores, remove anything that isn't
            # alphanumeric or underscore, and lowercase (because Docker doesn't
            # allow uppercase in image names).
            mangled = re.sub('[- ]', '_', gpu.name.lower())
            mangled = re.sub('[^a-z0-9_]', '', mangled)
            image_path = await resolver.image_resolver('katsdpingest_' + mangled, loop)
            self.taskinfo.container.docker.image = image_path
            logger.info('Develop mode: using %s for ingest', image_path)


def is_develop(config):
    # This gets called as part of validation, so it cannot assume that
    # normalise() has been called.
    return config['config'].get('develop', False)


def _mb(value):
    """Convert bytes to mebibytes"""
    return value / 1024**2


def _round_down(value, period):
    return value // period * period


def _round_up(value, period):
    return _round_down(value - 1, period) + period


def _bandwidth(size, time, ratio=1.05, overhead=128):
    """Convert a heap size to a bandwidth in bits per second.

    Parameters
    ----------
    size : int
        Size in bytes
    time : float
        Time between heaps in seconds
    ratio : float
        Relative overhead
    overhead : int
        Absolute overhead, in bytes
    """
    return (size * ratio + overhead) * 8 / time


class CBFStreamInfo:
    """Wraps a CBF stream from the config to provide convenient access to
    calculated quantities.

    It only supports streams that are ``cbf.antenna_channelised_voltage`` or
    have such a stream upstream.
    """
    def __init__(self, config, name):
        self.name = name
        self.raw = config['inputs'][name]
        self.config = config
        acv = self.raw
        while acv['type'] != 'cbf.antenna_channelised_voltage':
            src = acv['src_streams'][0]
            acv = config['inputs'][src]
        self.antenna_channelised_voltage = acv

    @property
    def antennas(self):
        return self.antenna_channelised_voltage['antennas']

    @property
    def n_antennas(self):
        return len(self.antennas)

    @property
    def n_channels(self):
        return self.antenna_channelised_voltage['n_chans']

    @property
    def n_pols(self):
        return self.antenna_channelised_voltage['n_pols']

    @property
    def bandwidth(self):
        return self.antenna_channelised_voltage['bandwidth']

    @property
    def adc_sample_rate(self):
        return self.antenna_channelised_voltage['adc_sample_rate']

    @property
    def n_samples_between_spectra(self):
        return self.antenna_channelised_voltage['n_samples_between_spectra']

    @property
    def n_endpoints(self):
        url = self.raw['url']
        parts = urllib.parse.urlsplit(url)
        return len(endpoint_list_parser(None)(parts.hostname))


class VisInfo:
    """Mixin for info classes to compute visibility info from baselines and channels."""
    @property
    def n_vis(self):
        return self.n_baselines * self.n_channels


class BaselineCorrelationProductsInfo(VisInfo, CBFStreamInfo):
    @property
    def n_baselines(self):
        return self.raw['n_bls']

    @property
    def int_time(self):
        return self.raw['int_time']

    @property
    def n_channels_per_substream(self):
        return self.raw['n_chans_per_substream']

    @property
    def size(self):
        """Size of frame in bytes"""
        return self.n_vis * 8     # size of complex64

    @property
    def net_bandwidth(self, ratio=1.05, overhead=128):
        return _bandwidth(self.size, self.int_time, ratio, overhead)


class TiedArrayChannelisedVoltageInfo(CBFStreamInfo):
    @property
    def n_channels_per_substream(self):
        return self.raw['n_chans_per_substream']

    @property
    def n_substreams(self):
        return self.n_channels // self.n_channels_per_substream

    @property
    def size(self):
        """Size of frame in bytes"""
        return (self.raw['beng_out_bits_per_sample'] * 2
                * self.raw['spectra_per_heap']
                * self.n_channels) // 8

    @property
    def int_time(self):
        """Interval between heaps, in seconds"""
        return self.raw['spectra_per_heap'] * self.n_samples_between_spectra / self.adc_sample_rate

    @property
    def net_bandwidth(self, ratio=1.05, overhead=128):
        """Network bandwidth in bits per second"""
        heap_size = self.size / self.n_substreams
        return _bandwidth(heap_size, self.int_time, ratio, overhead) * self.n_substreams


class L0Info(VisInfo):
    """Query properties of an L0 data stream"""
    def __init__(self, config, name):
        self.raw = config['outputs'][name]
        self.src_info = BaselineCorrelationProductsInfo(config, self.raw['src_streams'][0])

    @property
    def output_channels(self):
        return tuple(self.raw['output_channels'])

    @property
    def n_channels(self):
        rng = self.output_channels
        return (rng[1] - rng[0]) // self.raw['continuum_factor']

    @property
    def n_pols(self):
        """Number of polarisations per antenna."""
        return self.src_info.n_pols

    @property
    def n_antennas(self):
        return self.src_info.n_antennas

    @property
    def n_baselines(self):
        a = self.n_antennas
        return a * (a + 1) // 2 * self.n_pols**2

    @property
    def int_time(self):
        # Round to nearest multiple of CBF integration time
        ratio = max(1, int(round(self.raw['output_int_time'] / self.src_info.int_time)))
        return ratio * self.src_info.int_time

    @property
    def size(self):
        """Size of single frame in bytes"""
        # complex64 for vis, uint8 for weights and flags, float32 for weights_channel
        return self.n_vis * 10 + self.n_channels * 4

    @property
    def flag_size(self):
        return self.n_vis

    @property
    def net_bandwidth(self, ratio=1.05, overhead=128):
        return _bandwidth(self.size, self.int_time, ratio, overhead)

    @property
    def flag_bandwidth(self, ratio=1.05, overhead=128):
        return _bandwidth(self.flag_size, self.int_time, ratio, overhead)


class BeamformerInfo:
    """Query properties of an SDP beamformer stream.

    Each stream corresponds to a single-pol beam. It supports both the
    ``sdp.beamformer_engineering`` and ``sdp.beamformer`` stream types.
    """
    def __init__(self, config, name, src_name):
        self.raw = config['outputs'][name]
        if src_name not in self.raw['src_streams']:
            raise KeyError('{} not a source of {}'.format(src_name, name))
        self.src_info = TiedArrayChannelisedVoltageInfo(config, src_name)
        requested_channels = tuple(self.raw.get('output_channels', (0, self.src_info.n_channels)))
        # Round to fit the endpoints, as required by bf_ingest.
        channels_per_endpoint = self.src_info.n_channels // self.src_info.n_endpoints
        self.output_channels = (_round_down(requested_channels[0], channels_per_endpoint),
                                _round_up(requested_channels[1], channels_per_endpoint))
        if self.output_channels != requested_channels:
            logger.info('Rounding output channels for %s from %s to %s',
                        name, requested_channels, self.output_channels)

    @property
    def n_channels(self):
        return self.output_channels[1] - self.output_channels[0]

    @property
    def size(self):
        """Size of single frame in bytes"""
        return self.src_info.size * self.n_channels // self.src_info.n_channels

    @property
    def int_time(self):
        """Interval between heaps, in seconds"""
        return self.src_info.int_time

    @property
    def n_substreams(self):
        return self.n_channels // self.src_info.n_channels_per_substream

    @property
    def net_bandwidth(self, ratio=1.05, overhead=128):
        """Network bandwidth in bits per second"""
        heap_size = self.size / self.n_substreams
        return _bandwidth(heap_size, self.int_time, ratio, overhead) * self.n_substreams


def find_node(g, name):
    for node in g:
        if node.name == name:
            return node
    raise KeyError('no node called ' + name)


def _make_telstate(g, config):
    # Pointing sensors can account for substantial memory per antennas over a
    # long-lived subarray.
    n_antennas = 0
    for stream in config['inputs'].values():
        if stream['type'] == 'cbf.antenna_channelised_voltage':
            n_antennas += len(stream['antennas'])

    telstate = SDPLogicalTask('telstate')
    telstate.cpus = 0.4
    telstate.mem = 2048 + 400 * n_antennas
    telstate.disk = telstate.mem
    telstate.image = 'katsdptelstate'
    telstate.ports = ['telstate']
    # Run it in /mnt/mesos/sandbox so that the dump.rdb ends up there.
    telstate.taskinfo.container.docker.setdefault('parameters', []).append(
        {'key': 'workdir', 'value': '/mnt/mesos/sandbox'})
    telstate.command = ['redis-server', '/usr/local/etc/redis/redis.conf']
    telstate.physical_factory = TelstateTask
    telstate.deconfigure_wait = False
    telstate.wait_capture_blocks_dead = True
    g.add_node(telstate)
    return telstate


def _make_cam2telstate(g, config, name):
    cam2telstate = SDPLogicalTask('cam2telstate')
    cam2telstate.image = 'katsdpcam2telstate'
    cam2telstate.command = ['cam2telstate.py']
    cam2telstate.cpus = 0.75
    cam2telstate.mem = 256
    cam2telstate.ports = ['port']
    url = config['inputs'][name]['url']
    g.add_node(cam2telstate, config=lambda task, resolver: {
        'url': url
    })
    return cam2telstate


def _make_meta_writer(g, config):
    def make_meta_writer_config(task, resolver):
        s3_url = urllib.parse.urlsplit(resolver.s3_config['archive']['url'])
        return {
            's3_host': s3_url.hostname,
            's3_port': s3_url.port,
            'rdb_path': OBJ_DATA_VOL.container_path
        }

    meta_writer = SDPLogicalTask('meta_writer')
    meta_writer.image = 'katsdpmetawriter'
    # The keys are passed on the command-line rather than through config so that
    # they are redacted from telstate
    meta_writer.command = [
        'meta_writer.py',
        '--access-key', '{resolver.s3_config[archive][write][access_key]}',
        '--secret-key', '{resolver.s3_config[archive][write][secret_key]}'
    ]
    meta_writer.cpus = 0.2
    # Only a base allocation: it also gets telstate_extra added
    meta_writer.mem = 256
    meta_writer.ports = ['port']
    meta_writer.volumes = [OBJ_DATA_VOL]
    meta_writer.interfaces = [scheduler.InterfaceRequest('sdp_10g')]
    meta_writer.deconfigure_wait = False
    # Actual required bandwidth is minimal, but bursty. Use 1 Gb/s,
    # except in development mode where it might not be available.
    bandwidth = 1e9 if not is_develop(config) else 10e6
    meta_writer.interfaces[0].bandwidth_out = bandwidth
    meta_writer.transitions = {
        CaptureBlockState.POSTPROCESSING: [
            KatcpTransition('write-meta', '{capture_block_id}', True, timeout=120)    # Light dump
        ],
        CaptureBlockState.DEAD: [
            KatcpTransition('write-meta', '{capture_block_id}', False, timeout=300)   # Full dump
        ]
    }

    g.add_node(meta_writer, config=make_meta_writer_config)
    return meta_writer


def _make_cbf_simulator(g, config, name):
    type_ = config['inputs'][name]['type']
    settings = config['inputs'][name]['simulate']
    assert type_ in ('cbf.baseline_correlation_products', 'cbf.tied_array_channelised_voltage')
    # Determine number of simulators to run.
    n_sim = 1
    if type_ == 'cbf.baseline_correlation_products':
        info = BaselineCorrelationProductsInfo(config, name)
        while info.n_vis / n_sim > _N32_32:
            n_sim *= 2
    else:
        info = TiedArrayChannelisedVoltageInfo(config, name)
    ibv = not is_develop(config)

    def make_cbf_simulator_config(task, resolver):
        substreams = info.n_channels // info.n_channels_per_substream
        conf = {
            'cbf_channels': info.n_channels,
            'cbf_adc_sample_rate': info.adc_sample_rate,
            'cbf_bandwidth': info.bandwidth,
            'cbf_substreams': substreams,
            'cbf_ibv': ibv,
            'cbf_sync_time': time.time(),
            'servers': n_sim
        }
        sources = settings.get('sources', [])
        if sources:
            conf['cbf_sim_sources'] = [{'description': description} for description in sources]
        if type_ == 'cbf.baseline_correlation_products':
            conf.update({
                'cbf_int_time': info.int_time,
                'max_packet_size': 2088
            })
        else:
            conf.update({
                'beamformer_timesteps': info.raw['spectra_per_heap'],
                'beamformer_bits': info.raw['beng_out_bits_per_sample']
            })
        if 'center_freq' in settings:
            conf['cbf_center_freq'] = float(settings['center_freq'])
        if 'antennas' in settings:
            conf['cbf_antennas'] = [{'description': value} for value in settings['antennas']]
        else:
            conf['antenna_mask'] = ','.join(info.antennas)
        return conf

    sim_group = LogicalGroup('sim.' + name)
    g.add_node(sim_group, config=make_cbf_simulator_config)
    multicast = find_node(g, 'multicast.' + name)
    g.add_edge(sim_group, multicast, port='spead', depends_resolve=True,
               config=lambda task, resolver, endpoint: {'cbf_spead': str(endpoint)})
    g.add_edge(multicast, sim_group, depends_init=True, depends_ready=True)

    for i in range(n_sim):
        sim = SDPLogicalTask('sim.{}.{}'.format(name, i + 1))
        sim.image = 'katcbfsim'
        # create-*-stream is passed on the command-line instead of telstate
        # for now due to SR-462.
        if type_ == 'cbf.baseline_correlation_products':
            sim.command = ['cbfsim.py', '--create-fx-stream', name]
            # It's mostly GPU work, so not much CPU requirement. Scale for 2 CPUs for
            # 16 antennas, 32K, and cap it there (threads for compute and network).
            # cbf_vis is an overestimate since the simulator is not constrained to
            # power-of-two antenna counts like the real CBF.
            scale = info.n_vis / n_sim / _N16_32
            sim.cpus = 2 * min(1.0, scale)
            # 4 entries per Jones matrix, complex64 each
            gains_size = info.n_antennas * info.n_channels * 4 * 8
            # Factor of 4 is conservative; only actually double-buffered
            sim.mem = (4 * _mb(info.size) + _mb(gains_size)) / n_sim + 512
            sim.cores = [None, None]
            sim.gpus = [scheduler.GPURequest()]
            # Scale for 20% at 16 ant, 32K channels
            sim.gpus[0].compute = min(1.0, 0.2 * scale)
            sim.gpus[0].mem = (2 * _mb(info.size) + _mb(gains_size)) / n_sim + 256
        else:
            info = TiedArrayChannelisedVoltageInfo(config, name)
            sim.command = ['cbfsim.py', '--create-beamformer-stream', name]
            # The beamformer simulator only simulates data shape, not content. The
            # CPU load thus scales only with network bandwidth. Scale for 2 CPUs at
            # L band, and cap it there since there are only 2 threads. Using 1.999
            # instead of 2.0 is to ensure rounding errors don't cause a tiny fraction
            # extra of CPU to be used.
            sim.cpus = min(2.0, 1.999 * info.size / info.int_time / 1712000000.0 / n_sim)
            sim.mem = 4 * _mb(info.size) / n_sim + 512

        sim.ports = ['port']
        if ibv:
            # The verbs send interface seems to create a large number of
            # file handles per stream, easily exceeding the default of
            # 1024.
            sim.taskinfo.container.docker.parameters = [{"key": "ulimit", "value": "nofile=8192"}]
        sim.interfaces = [scheduler.InterfaceRequest('cbf', infiniband=ibv)]
        sim.interfaces[0].bandwidth_out = info.net_bandwidth
        sim.transitions = {
            CaptureBlockState.CAPTURING: [
                KatcpTransition('capture-start', name, '{time}', timeout=30)
            ],
            CaptureBlockState.POSTPROCESSING: [
                KatcpTransition('capture-stop', name, timeout=60)
            ]
        }

        g.add_node(sim, config=lambda task, resolver, server_id=i + 1: {
            'cbf_interface': task.interfaces['cbf'].name,
            'server_id': server_id
        })
        g.add_edge(sim_group, sim, depends_ready=True, depends_init=True)
        g.add_edge(sim, sim_group, depends_resources=True)
    return sim_group


def _make_timeplot(g, name, description,
                   cpus, timeplot_buffer_mb, bandwidth, extra_config):
    """Common backend code for creating a single timeplot server."""
    multicast = find_node(g, 'multicast.timeplot.' + name)
    timeplot = SDPLogicalTask('timeplot.' + name)
    timeplot.image = 'katsdpdisp'
    timeplot.command = ['time_plot.py']
    timeplot.cpus = cpus
    # timeplot_buffer covers only the visibilities, but there are also flags
    # and various auxiliary buffers. Add 20% to give some headroom, and also
    # add a fixed amount since in very small arrays the 20% might not cover
    # the fixed-sized overheads.
    timeplot.mem = timeplot_buffer_mb * 1.2 + 256
    timeplot.interfaces = [scheduler.InterfaceRequest('sdp_10g')]
    timeplot.interfaces[0].bandwidth_in = bandwidth
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


def _make_timeplot_correlator(g, config, spectral_name):
    spectral_info = L0Info(config, spectral_name)
    n_ingest = n_ingest_nodes(config, spectral_name)

    # Exact requirement not known (also depends on number of users). Give it
    # 2 CPUs (max it can use) for 16 antennas, 32K channels and scale from there.
    cpus = 2 * min(1.0, spectral_info.n_vis / _N16_32)
    # Give timeplot enough memory for 256 time samples, but capped at 16GB.
    # This formula is based on data.py in katsdpdisp.
    percentiles = 5 * 8
    timeplot_slot = spectral_info.n_channels * (spectral_info.n_baselines + percentiles) * 8
    timeplot_buffer = min(256 * timeplot_slot, 16 * 1024**3)

    return _make_timeplot(
        g, name=spectral_name, description=spectral_name,
        cpus=cpus, timeplot_buffer_mb=timeplot_buffer / 1024**2,
        bandwidth=_correlator_timeplot_bandwidth(spectral_info, n_ingest) * n_ingest,
        extra_config={'l0_name': spectral_name,
                      'max_custom_signals': TIMEPLOT_MAX_CUSTOM_SIGNALS})


def _make_timeplot_beamformer(g, config, name):
    """Make timeplot server for the beamformer, plus a beamformer capture to feed it."""
    info = TiedArrayChannelisedVoltageInfo(config, name)
    develop = is_develop(config)
    beamformer = _make_beamformer_engineering_pol(
        g, info, 'bf_ingest_timeplot.{}'.format(name), name, True, False, 0, develop)
    beamformer.critical = False

    # It's a low-demand setup (only one signal). The CPU and memory numbers
    # could potentially be reduced further.
    timeplot = _make_timeplot(
        g, name=name, description=name,
        cpus=0.5, timeplot_buffer_mb=128,
        bandwidth=_beamformer_timeplot_bandwidth(info),
        extra_config={'max_custom_signals': 2})

    return beamformer, timeplot


def _correlator_timeplot_frame_size(spectral_info, n_cont_channels, n_ingest):
    """Approximate size of the data sent from one ingest process to timeplot, per heap"""
    # This is based on _init_ig_sd from katsdpingest/ingest_session.py
    n_perc_signals = 5 * 8
    n_spec_channels = spectral_info.n_channels // n_ingest
    n_cont_channels //= n_ingest
    n_bls = spectral_info.n_baselines
    ans = n_spec_channels * TIMEPLOT_MAX_CUSTOM_SIGNALS * 9  # sd_data + sd_flags
    ans += TIMEPLOT_MAX_CUSTOM_SIGNALS * 4          # sd_data_index
    ans += n_cont_channels * n_bls * 9              # sd_blmxdata + sd_blmxflags
    ans += n_bls * 12                               # sd_timeseries + sd_timeseriesabs
    ans += n_spec_channels * n_perc_signals * 5     # sd_percspectrum + sd_percspectrumflags
    # input names are e.g. m012v -> 5 chars, 2 inputs per baseline
    ans += n_bls * 10                               # bls_ordering
    ans += n_bls * 8 * 4                            # sd_flag_fraction
    # There are a few scalar values, but that doesn't add up to enough to worry about
    return ans


def _correlator_timeplot_continuum_factor(spectral_info, n_ingest):
    factor = 1
    # Aim for about 256 signal display coarse channels
    while (spectral_info.n_channels % (factor * n_ingest * 2) == 0
           and spectral_info.n_channels // factor >= 384):
        factor *= 2
    return factor


def _correlator_timeplot_bandwidth(spectral_info, n_ingest):
    """Bandwidth for the correlator timeplot stream from a single ingest"""
    sd_continuum_factor = _correlator_timeplot_continuum_factor(spectral_info, n_ingest)
    sd_frame_size = _correlator_timeplot_frame_size(
        spectral_info, spectral_info.n_channels // sd_continuum_factor, n_ingest)
    # The rates are low, so we allow plenty of padding in case the calculation is
    # missing something.
    return _bandwidth(sd_frame_size, spectral_info.int_time, ratio=1.2, overhead=4096)


def _beamformer_timeplot_bandwidth(info):
    """Bandwidth for the beamformer timeplot stream.

    Parameters
    ----------
    info : :class:`TiedArrayChannelisedVoltageInfo`
        Info about the single-pol beam.
    """
    # XXX The beamformer signal displays are still in development, but the
    # rates are tiny. Until the protocol is finalised we'll just hardcode a
    # number.
    return 1e6    # 1 MB/s


def n_ingest_nodes(config, name):
    """Number of processes used to implement ingest for a particular output."""
    # TODO: adjust based on the number of antennas and channels
    return 4 if not is_develop(config) else 2


def n_cal_nodes(config, name, l0_name):
    """Number of processes used to implement cal for a particular output."""
    # Use single cal for 4K or less: it doesn't need the performance, and
    # a unified cal report is more convenient (revisit once split cal supports
    # a unified cal report).
    info = L0Info(config, l0_name)
    if is_develop(config):
        return 2
    elif info.n_channels <= 4096:
        return 1
    else:
        return 4


def _adjust_ingest_output_channels(config, names):
    """Modify the config dictionary to set output channels for ingest.

    It is widened where necessary to meet alignment requirements.

    Parameters
    ----------
    config : dict
        Configuration dictionary
    names : list of str
        The names of the ingest output products. These must match i.e. be the
        same other than for continuum_factor.
    """
    def lcm(a, b):
        return a // math.gcd(a, b) * b

    n_ingest = n_ingest_nodes(config, names[0])
    outputs = [config['outputs'][name] for name in names]
    src = config['inputs'][outputs[0]['src_streams'][0]]
    acv = config['inputs'][src['src_streams'][0]]
    n_chans = acv['n_chans']
    requested = outputs[0]['output_channels']
    alignment = 1
    # This is somewhat stricter than katsdpingest actually requires, which is
    # simply that each ingest node is aligned to the continuum factor.
    for output in outputs:
        alignment = lcm(alignment, n_ingest * output['continuum_factor'])
    assigned = [_round_down(requested[0], alignment), _round_up(requested[1], alignment)]
    # Should always succeed if product_config.validate passed
    assert 0 <= assigned[0] < assigned[1] <= n_chans, "Aligning channels caused an overflow"
    for name, output in zip(names, outputs):
        if assigned != requested:
            logger.info('Rounding output channels for %s from %s to %s',
                        name, requested, assigned)
        output['output_channels'] = assigned


def _make_ingest(g, config, spectral_name, continuum_name):
    develop = is_develop(config)

    if not spectral_name and not continuum_name:
        raise ValueError('At least one of spectral_name or continuum_name must be given')
    continuum_info = L0Info(config, continuum_name) if continuum_name else None
    spectral_info = L0Info(config, spectral_name) if spectral_name else None
    if spectral_info is None:
        spectral_info = copy.copy(continuum_info)
        # Make a copy of the info, then override continuum_factor
        spectral_info.raw = dict(spectral_info.raw)
        spectral_info.raw['continuum_factor'] = 1
    name = spectral_name if spectral_name else continuum_name
    src = spectral_info.raw['src_streams'][0]
    src_info = spectral_info.src_info
    # Number of ingest nodes.
    n_ingest = n_ingest_nodes(config, name)

    # Virtual ingest node which depends on the real ingest nodes, so that other
    # services can declare dependencies on ingest rather than individual nodes.
    ingest_group = LogicalGroup('ingest.' + name)

    sd_continuum_factor = _correlator_timeplot_continuum_factor(spectral_info, n_ingest)
    sd_spead_rate = _correlator_timeplot_bandwidth(spectral_info, n_ingest)
    output_channels_str = '{}:{}'.format(*spectral_info.output_channels)
    group_config = {
        'continuum_factor': continuum_info.raw['continuum_factor'] if continuum_name else 1,
        'sd_continuum_factor': sd_continuum_factor,
        'sd_spead_rate': sd_spead_rate,
        'cbf_ibv': not develop,
        'cbf_name': src,
        'servers': n_ingest,
        'l0_spectral_name': spectral_name,
        'l0_continuum_name': continuum_name,
        'antenna_mask': src_info.antennas,
        'output_int_time': spectral_info.int_time,
        'sd_int_time': spectral_info.int_time,
        'output_channels': output_channels_str,
        'sd_output_channels': output_channels_str
    }
    if spectral_name:
        group_config.update(l0_spectral_name=spectral_name)
    if continuum_name:
        group_config.update(l0_continuum_name=continuum_name)
    g.add_node(ingest_group, config=lambda task, resolver: group_config)

    if spectral_name:
        spectral_multicast = LogicalMulticast('multicast.' + spectral_name, n_ingest)
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
    if continuum_name:
        continuum_multicast = LogicalMulticast('multicast.' + continuum_name, n_ingest)
        g.add_node(continuum_multicast)
        g.add_edge(ingest_group, continuum_multicast, port='spead', depends_resolve=True,
                   config=lambda task, resolver, endpoint: {'l0_continuum_spead': str(endpoint)})
        g.add_edge(continuum_multicast, ingest_group, depends_init=True, depends_ready=True)

    src_multicast = find_node(g, 'multicast.' + src)
    g.add_edge(ingest_group, src_multicast, port='spead', depends_resolve=True,
               config=lambda task, resolver, endpoint: {'cbf_spead': str(endpoint)})

    for i in range(1, n_ingest + 1):
        ingest = SDPLogicalTask('ingest.{}.{}'.format(name, i))
        ingest.physical_factory = IngestTask
        ingest.image = 'katsdpingest_titanx'
        ingest.command = ['ingest.py']
        ingest.ports = ['port', 'aiomonitor_port', 'aioconsole_port']
        ingest.wait_ports = ['port']
        ingest.gpus = [scheduler.GPURequest()]
        if not develop:
            ingest.gpus[-1].name = INGEST_GPU_NAME
        # Scale for a full GPU for 32 antennas, 32K channels on one node
        scale = src_info.n_vis / _N32_32 / n_ingest
        ingest.gpus[0].compute = scale
        # Refer to
        # https://docs.google.com/spreadsheets/d/13LOMcUDV1P0wyh_VSgTOcbnhyfHKqRg5rfkQcmeXmq0/edit
        # We use slightly higher multipliers to be safe, as well as
        # conservatively using src_info instead of spectral_info.
        ingest.gpus[0].mem = \
            (70 * src_info.n_vis + 168 * src_info.n_channels) / n_ingest / 1024**2 + 128
        # Provide 4 CPUs for 32 antennas, 32K channels (the number in a NUMA
        # node of an ingest machine). It might not actually need this much.
        # Cores are reserved solely to get NUMA affinity with the NIC.
        ingest.cpus = int(math.ceil(4.0 * scale))
        ingest.cores = [None] * ingest.cpus
        # Scale factor of 32 may be overly conservative: actual usage may be
        # only half this. This also leaves headroom for buffering L0 output.
        ingest.mem = 32 * _mb(src_info.size) / n_ingest + 4096
        ingest.transitions = CAPTURE_TRANSITIONS
        ingest.interfaces = [
            scheduler.InterfaceRequest('cbf', affinity=not develop, infiniband=not develop),
            scheduler.InterfaceRequest('sdp_10g')]
        ingest.interfaces[0].bandwidth_in = src_info.net_bandwidth / n_ingest
        net_bandwidth = 0.0
        if spectral_name:
            net_bandwidth += spectral_info.net_bandwidth + sd_spead_rate * n_ingest
        if continuum_name:
            net_bandwidth += continuum_info.net_bandwidth
        ingest.interfaces[1].bandwidth_out = net_bandwidth / n_ingest

        def make_ingest_config(task, resolver, server_id=i):
            conf = {
                'cbf_interface': task.interfaces['cbf'].name,
                'server_id': server_id
            }
            if spectral_name:
                conf.update(l0_spectral_interface=task.interfaces['sdp_10g'].name)
                conf.update(sdisp_interface=task.interfaces['sdp_10g'].name)
            if continuum_name:
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


def _make_cal(g, config, name, l0_name, flags_names):
    info = L0Info(config, l0_name)
    if info.n_antennas < 4:
        # Only possible to calibrate with at least 4 antennas
        return None

    settings = config['outputs'][name]
    # We want ~25 min of data in the buffer, to allow for a single batch of
    # 15 minutes.
    buffer_time = settings.get('buffer_time', 25.0 * 60.0)
    # Some pathological observations use more than this, but they can override
    # the default.
    max_scans = settings.get('max_scans', 1000)
    parameters = settings['parameters']
    models = settings['models']
    if models:
        raise NotImplementedError('cal models are not yet supported')
    n_cal = n_cal_nodes(config, name, l0_name)

    # Some of the scaling in cal is non-linear, so we need to give smaller
    # arrays more CPU than might be suggested by linear scaling. In particular,
    # 32K mode is flagged by averaging down to 8K first, so flagging performance
    # (which tends to dominate runtime) scales as if there were 8K channels.
    # Longer integration times are also less efficient (because there are fixed
    # costs per scan, so we scale as if for 4s dumps if longer dumps are used.
    effective_vis = info.n_baselines * min(8192, info.n_channels)
    effective_int = min(info.int_time, 4.0)
    # This scale factor gives 34 total CPUs for 64A, 32K, 4+s integration, which
    # will get clamped down slightly.
    cpus = 2e-6 * effective_vis / effective_int / n_cal
    # Always (except in development mode) have at least a whole CPU for the
    # pipeline.
    if not is_develop(config):
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
    slots = int(math.ceil(buffer_time / info.int_time))
    slot_size = info.n_vis * 13.125 / n_cal
    buffer_size = slots * slot_size
    # Processing operations come in a few flavours:
    # - average over time: need O(1) extra slots
    # - average over frequency: needs far less memory than the above
    # - compute flags per baseline: works on 16 baselines at a time.
    # In each case we arbitrarily allow for 4 times the result, per worker.
    # There is also a time- and channel-averaged version (down to 1024 channels)
    # of the HH and VV products for each scan. We allow a factor of 2 for
    # operations that work on it (which cancels the factor of 1/2 for only
    # having 2 of the 4 pol products). The scaling by n_cal is because there
    # are 1024 channels *per node* while info.n_channels is over all nodes.
    extra = max(workers / slots, min(16 * workers, info.n_baselines) / info.n_baselines) * 4
    extra += max_scans * 1024 * n_cal / (slots * info.n_channels)

    # Extra memory allocation for tasks that deal with bandpass calibration
    # solutions in telescope state. The exact size of these depends on how
    # often bandpass calibrators are visited, so the scale factor is a
    # thumb-suck. The scale factors are
    # - 2 pols per antenna
    # - 8 bytes per value (complex64)
    # - 200 solutions
    # - 3: conservative estimate of bloat from text-based pickling
    telstate_extra = info.n_channels * info.n_pols * info.n_antennas * 8 * 3 * 200

    group_config = {
        'buffer_maxsize': buffer_size,
        'max_scans': max_scans,
        'workers': workers,
        'l0_name': l0_name,
        'servers': n_cal
    }
    group_config.update(parameters)

    # Virtual node which depends on the real cal nodes, so that other services
    # can declare dependencies on cal rather than individual nodes.
    cal_group = LogicalGroup(name)
    g.add_node(cal_group, telstate_extra=telstate_extra,
               config=lambda task, resolver: group_config)
    src_multicast = find_node(g, 'multicast.' + l0_name)
    g.add_edge(cal_group, src_multicast, port='spead',
               depends_resolve=True, depends_init=True, depends_ready=True,
               config=lambda task, resolver, endpoint: {
                   'l0_spead': str(endpoint)
               })

    # Flags output
    flags_streams_base = []
    flags_multicasts = {}
    for flags_name in flags_names:
        flags_config = config['outputs'][flags_name]
        flags_src_stream = flags_config['src_streams'][0]
        cf = config['outputs'][flags_src_stream]['continuum_factor']
        cf //= config['outputs'][l0_name]['continuum_factor']
        flags_streams_base.append({
            'name': flags_name,
            'src_stream': flags_src_stream,
            'continuum_factor': cf,
            'rate_ratio': FLAGS_RATE_RATIO
        })

        flags_multicast = LogicalMulticast('multicast.' + flags_name, n_cal)
        flags_multicasts[flags_name] = flags_multicast
        g.add_node(flags_multicast)
        g.add_edge(flags_multicast, cal_group, depends_init=True, depends_ready=True)

    for i in range(1, n_cal + 1):
        cal = SDPLogicalTask('{}.{}'.format(name, i))
        cal.image = 'katsdpcal'
        cal.command = ['run_cal.py']
        cal.cpus = cpus
        cal.mem = buffer_size * (1 + extra) / 1024**2 + 512
        cal.volumes = [DATA_VOL]
        cal.interfaces = [scheduler.InterfaceRequest('sdp_10g')]
        # Note: these scale the fixed overheads too, so is not strictly accurate.
        cal.interfaces[0].bandwidth_in = info.net_bandwidth / n_cal
        cal.interfaces[0].bandwidth_out = info.flag_bandwidth * FLAGS_RATE_RATIO / n_cal
        cal.ports = ['port', 'dask_diagnostics']
        cal.wait_ports = ['port']
        cal.gui_urls = [{
            'title': 'Cal diagnostics',
            'description': 'Dask diagnostics for {0.name}',
            'href': 'http://{0.host}:{0.ports[dask_diagnostics]}/status',
            'category': 'Plot'
        }]
        cal.transitions = CAPTURE_TRANSITIONS
        cal.deconfigure_wait = False

        def make_cal_config(task, resolver, server_id=i):
            cal_config = {
                'l0_interface': task.interfaces['sdp_10g'].name,
                'server_id': server_id,
                'dask_diagnostics': ('', task.ports['dask_diagnostics']),
                'flags_streams': copy.deepcopy(flags_streams_base)
            }
            for flags_stream in cal_config['flags_streams']:
                flags_stream['interface'] = task.interfaces['sdp_10g'].name
                flags_stream['endpoints'] = str(task.flags_endpoints[flags_stream['name']])
            return cal_config

        g.add_node(cal, config=make_cal_config)
        # Connect to cal_group. See comments in _make_ingest for explanation.
        g.add_edge(cal_group, cal, depends_ready=True, depends_init=True)
        g.add_edge(cal, cal_group, depends_resources=True)
        g.add_edge(cal, src_multicast,
                   depends_resolve=True, depends_ready=True, depends_init=True)

        for flags_name, flags_multicast in flags_multicasts.items():
            # A sneaky hack to capture the endpoint information for the flag
            # streams. This is used as a config= function, but instead of
            # returning config we just stash information in the task object
            # which is retrieved by make_cal_config.
            def add_flags_endpoint(task, resolver, endpoint, name=flags_name):
                if not hasattr(task, 'flags_endpoints'):
                    task.flags_endpoints = {}
                task.flags_endpoints[name] = endpoint
                return {}

            g.add_edge(cal, flags_multicast, port='spead', depends_resolve=True,
                       config=add_flags_endpoint)

    return cal_group


def _writer_mem_mb(dump_size, obj_size, n_substreams, workers, buffer_dumps):
    """Compute memory requirement (in MB) for vis_writer or flag_writer"""
    # Estimate the size of the memory pool. An extra item is added because
    # the value is copied when added to the rechunker, although this does
    # not actually come from the memory pool.
    heap_size = dump_size / n_substreams
    memory_pool = (3 + 4 * n_substreams) * heap_size
    # Estimate the size of the in-flight objects for the workers.
    # TODO: this doesn't account for any time accumulation
    write_queue = buffer_dumps * dump_size

    # Socket buffer allocated per endpoint, but it is bounded at 256MB by the
    # OS config.
    socket_buffers = min(256 * 1024**2, heap_size) * n_substreams

    # Double the memory allocation to be on the safe side. This gives some
    # headroom for page cache etc.
    return 2 * _mb(memory_pool + write_queue + socket_buffers) + 256


def _make_vis_writer(g, config, name, s3_name, local, prefix=None):
    info = L0Info(config, name)

    output_name = prefix + '.' + name if prefix is not None else name
    g.graph['archived_streams'].append(output_name)
    vis_writer = SDPLogicalTask('vis_writer.' + output_name)
    vis_writer.image = 'katsdpdatawriter'
    vis_writer.command = ['vis_writer.py']
    # Don't yet have a good idea of real CPU usage. For now assume that 32
    # antennas, 32K channels requires two CPUs (one for capture, one for
    # writing) and scale from there, while capping at 3.
    vis_writer.cpus = min(3, 2 * info.n_vis / _N32_32)

    workers = 4
    # Buffer enough dumps for 45 seconds
    buffer_dumps = int(math.ceil(45.0 / info.int_time))
    src_multicast = find_node(g, 'multicast.' + name)
    n_substreams = src_multicast.n_addresses

    # Double the memory allocation to be on the safe side. This gives some
    # headroom for page cache etc.
    vis_writer.mem = _writer_mem_mb(info.size, WRITER_OBJECT_SIZE, n_substreams,
                                    workers, buffer_dumps)
    vis_writer.ports = ['port', 'aiomonitor_port', 'aioconsole_port']
    vis_writer.wait_ports = ['port']
    vis_writer.interfaces = [scheduler.InterfaceRequest('sdp_10g')]
    vis_writer.interfaces[0].bandwidth_in = info.net_bandwidth
    if local:
        vis_writer.volumes = [OBJ_DATA_VOL]
    else:
        vis_writer.interfaces[0].backwidth_out = info.net_bandwidth
        # Creds are passed on the command-line so that they are redacted from telstate.
        vis_writer.command.extend([
            '--s3-access-key', '{resolver.s3_config[%s][write][access_key]}' % s3_name,
            '--s3-secret-key', '{resolver.s3_config[%s][write][secret_key]}' % s3_name
        ])
    vis_writer.transitions = CAPTURE_TRANSITIONS

    def make_vis_writer_config(task, resolver):
        conf = {
            'l0_name': name,
            'l0_interface': task.interfaces['sdp_10g'].name,
            'obj_size_mb': _mb(WRITER_OBJECT_SIZE),
            'workers': workers,
            'buffer_dumps': buffer_dumps,
            's3_endpoint_url': resolver.s3_config[s3_name]['url'],
            'direct_write': True
        }
        if local:
            conf['npy_path'] = OBJ_DATA_VOL.container_path
        if prefix is not None:
            conf['new_name'] = output_name
        return conf

    g.add_node(vis_writer, config=make_vis_writer_config)
    g.add_edge(vis_writer, src_multicast, port='spead',
               depends_resolve=True, depends_init=True, depends_ready=True,
               config=lambda task, resolver, endpoint: {'l0_spead': str(endpoint)})
    return vis_writer


def _make_flag_writer(g, config, name, l0_name, s3_name, local, prefix=None):
    info = L0Info(config, l0_name)

    output_name = prefix + '.' + name if prefix is not None else name
    g.graph['archived_streams'].append(output_name)
    flag_writer = SDPLogicalTask('flag_writer.' + output_name)
    flag_writer.image = 'katsdpdatawriter'
    flag_writer.command = ['flag_writer.py']

    flags_src = find_node(g, 'multicast.' + name)
    n_substreams = flags_src.n_addresses
    workers = 4
    # Buffer enough data for 45 seconds of real time. We've seen the disk
    # system throw a fit and hang for 30 seconds at a time, and this should
    # allow us to ride that out.
    buffer_dumps = int(math.ceil(45.0 / info.int_time * FLAGS_RATE_RATIO))

    # Don't yet have a good idea of real CPU usage. This formula is
    # copied from the vis writer.
    flag_writer.cpus = min(3, 2 * info.n_vis / _N32_32)
    flag_writer.mem = _writer_mem_mb(info.flag_size, WRITER_OBJECT_SIZE, n_substreams,
                                     workers, buffer_dumps)
    flag_writer.ports = ['port', 'aiomonitor_port', 'aioconsole_port']
    flag_writer.wait_ports = ['port']
    flag_writer.interfaces = [scheduler.InterfaceRequest('sdp_10g')]
    flag_writer.interfaces[0].bandwidth_in = info.flag_bandwidth * FLAGS_RATE_RATIO
    if local:
        flag_writer.volumes = [OBJ_DATA_VOL]
    else:
        flag_writer.interfaces[0].bandwidth_out = flag_writer.interfaces[0].bandwidth_in
        # Creds are passed on the command-line so that they are redacted from telstate.
        flag_writer.command.extend([
            '--s3-access-key', '{resolver.s3_config[%s][write][access_key]}' % s3_name,
            '--s3-secret-key', '{resolver.s3_config[%s][write][secret_key]}' % s3_name
        ])
    flag_writer.deconfigure_wait = False

    # Capture init / done are used to track progress of completing flags
    # for a specified capture block id - the writer itself is free running
    flag_writer.transitions = {
        CaptureBlockState.CAPTURING: [
            KatcpTransition('capture-init', '{capture_block_id}', timeout=30)
        ],
        CaptureBlockState.DEAD: [
            KatcpTransition('capture-done', '{capture_block_id}', timeout=60)
        ]
    }

    def make_flag_writer_config(task, resolver):
        conf = {
            'flags_name': name,
            'flags_interface': task.interfaces['sdp_10g'].name,
            'obj_size_mb': _mb(WRITER_OBJECT_SIZE),
            'workers': workers,
            'buffer_dumps': buffer_dumps,
            's3_endpoint_url': resolver.s3_config[s3_name]['url'],
            'direct_write': True
        }
        if local:
            conf['npy_path'] = OBJ_DATA_VOL.container_path
        if prefix is not None:
            conf['new_name'] = output_name
            conf['rename_src'] = {l0_name: prefix + '.' + l0_name}
        return conf

    g.add_node(flag_writer, config=make_flag_writer_config)
    g.add_edge(flag_writer, flags_src, port='spead',
               depends_resolve=True, depends_init=True, depends_ready=True,
               config=lambda task, resolver, endpoint: {'flags_spead': str(endpoint)})
    return flag_writer


def _make_beamformer_ptuse(g, config, name):
    output = config['outputs'][name]
    srcs = output['src_streams']
    info = [BeamformerInfo(config, name, src) for src in srcs]

    bf_ingest = SDPLogicalTask('bf_ingest.' + name)
    bf_ingest.image = 'beamform'
    bf_ingest.command = ['ptuse_ingest.py']
    bf_ingest.ports = ['port']
    bf_ingest.gpus = [scheduler.GPURequest()]
    # TODO: revisit once coherent dedispersion is in use, when multiple
    # GPUs may be needed.
    bf_ingest.gpus[0].compute = 1.0
    bf_ingest.gpus[0].mem = 7 * 1024  # Overkill for now, but may be needed for dedispersion
    bf_ingest.cpus = 4
    bf_ingest.cores = ['capture0', 'capture1', 'processing', 'python']
    # 32GB of psrdada buffers, regardless of channels
    # 4GB to handle general process stuff
    bf_ingest.mem = 36 * 1024
    bf_ingest.interfaces = [scheduler.InterfaceRequest('cbf', infiniband=True, affinity=True)]
    bf_ingest.interfaces[0].bandwidth_in = sum(x.net_bandwidth for x in info)
    bf_ingest.volumes = [scheduler.VolumeRequest('data', '/data', 'RW')]
    # If the kernel is < 3.16, the default values are very low. Set
    # the values to the default from more recent Linux versions
    # (https://github.com/torvalds/linux/commit/060028bac94bf60a65415d1d55a359c3a17d5c31)
    bf_ingest.taskinfo.container.docker.parameters = [
        {'key': 'sysctl', 'value': 'kernel.shmmax=18446744073692774399'},
        {'key': 'sysctl', 'value': 'kernel.shmall=18446744073692774399'}
    ]
    bf_ingest.transitions = CAPTURE_TRANSITIONS
    g.add_node(bf_ingest, config=lambda task, resolver: {
        'cbf_channels': info[0].n_channels,
        'affinity': [task.cores['capture0'],
                     task.cores['capture1'],
                     task.cores['processing'],
                     task.cores['python']],
        'interface': task.interfaces['cbf'].name
    })
    for src, pol in zip(srcs, 'xy'):
        g.add_edge(bf_ingest, find_node(g, 'multicast.' + src), port='spead',
                   depends_resolve=True, depends_init=True, depends_ready=True,
                   config=lambda task, resolver, endpoint, pol=pol: {
                       'cbf_spead{}'.format(pol): str(endpoint)})
    return bf_ingest


def _make_beamformer_engineering_pol(g, info, node_name, src_name, timeplot, ram, idx, develop):
    """Generate node for a single polarisation of beamformer engineering output.

    This handles two cases: either it is a capture to file, associated with a
    particular output in the config dict; or it is for the purpose of
    computing the signal display statistics, in which case it is associated with
    an input but no specific output.

    Parameters
    ----------
    info : :class:`TiedArrayChannelisedVoltageInfo` or :class:`BeamformerInfo`
        Info about the stream.
    node_name : str
        Name to use for the logical node
    src_name : str
        Name of the tied-array-channelised-voltage stream
    timeplot : bool
        Whether this node is for computing signal display statistics
    ram : bool
        Whether this node is for writing to ramdisk (ignored if `timeplot` is true)
    idx : int
        Number of this source within the output (ignored if `timeplot` is true)
    develop : bool
        Whether this is a develop-mode config
    """
    src_multicast = find_node(g, 'multicast.' + src_name)

    bf_ingest = SDPLogicalTask(node_name)
    bf_ingest.image = 'katsdpbfingest'
    bf_ingest.command = ['schedrr', 'bf_ingest.py']
    bf_ingest.cpus = 2
    bf_ingest.cores = ['disk', 'network']
    bf_ingest.capabilities.append('SYS_NICE')
    if timeplot or not ram:
        # bf_ingest accumulates 128 frames in the ring buffer. It's not a
        # lot of memory, so to be on the safe side we double everything.
        # Values are int8*2.  Allow 512MB for various buffers.
        bf_ingest.mem = 256 * info.size / 1024**2 + 512
    else:
        # When writing to tmpfs, the file is accounted as memory to our
        # process, so we need more memory allocation than there is
        # space in the ramdisk. This is only used for lab testing, so
        # we just hardcode a number.
        bf_ingest.ram = 220 * 1024
    bf_ingest.interfaces = [scheduler.InterfaceRequest('cbf', infiniband=not develop)]
    bf_ingest.interfaces[0].bandwidth_in = info.net_bandwidth
    if timeplot:
        bf_ingest.interfaces.append(scheduler.InterfaceRequest('sdp_10g'))
        bf_ingest.interfaces[-1].bandwidth_out = _beamformer_timeplot_bandwidth(info)
    else:
        volume_name = 'bf_ram{}' if ram else 'bf_ssd{}'
        bf_ingest.volumes = [
            scheduler.VolumeRequest(volume_name.format(idx), '/data', 'RW', affinity=ram)]
    bf_ingest.ports = ['port']
    bf_ingest.transitions = CAPTURE_TRANSITIONS

    def make_beamformer_engineering_pol_config(task, resolver):
        config = {
            'affinity': [task.cores['disk'], task.cores['network']],
            'interface': task.interfaces['cbf'].name,
            'ibv': not develop,
            'stream_name': src_name,
        }
        if timeplot:
            config.update({
                'stats_interface': task.interfaces['sdp_10g'].name,
                'stats_int_time': 1.0
            })
        else:
            config.update({
                'file_base': '/data',
                'direct_io': not ram,       # Can't use O_DIRECT on tmpfs
                'channels': '{}:{}'.format(*info.output_channels)
            })
        return config

    g.add_node(bf_ingest, config=make_beamformer_engineering_pol_config)
    g.add_edge(bf_ingest, src_multicast, port='spead',
               depends_resolve=True, depends_init=True, depends_ready=True,
               config=lambda task, resolver, endpoint: {'cbf_spead': str(endpoint)})
    if timeplot:
        stats_multicast = LogicalMulticast('multicast.timeplot.{}'.format(src_name), 1)
        g.add_edge(bf_ingest, stats_multicast, port='spead',
                   depends_resolve=True,
                   config=lambda task, resolver, endpoint: {'stats': str(endpoint)})
        g.add_edge(stats_multicast, bf_ingest, depends_init=True, depends_ready=True)
    return bf_ingest


def _make_beamformer_engineering(g, config, name):
    """Generate nodes for beamformer engineering output.

    If `timeplot` is true, it generates services that only send data to
    timeplot servers, without capturing the data. In this case `name` may
    refer to an ``sdp.beamformer`` rather than an
    ``sdp.beamformer_engineering`` stream.
    """
    output = config['outputs'][name]
    srcs = output['src_streams']
    ram = output.get('store') == 'ram'
    develop = is_develop(config)

    nodes = []
    for i, src in enumerate(srcs):
        info = BeamformerInfo(config, name, src)
        nodes.append(_make_beamformer_engineering_pol(
            g, info, 'bf_ingest.{}.{}'.format(name, i + 1), src, False, ram, i, develop))
    return nodes


def build_logical_graph(config):
    # Note: a few lambdas in this function have default arguments e.g.
    # stream=stream. This is needed because capturing a loop-local variable in
    # a lambda captures only the final value of the variable, not the value it
    # had in the loop iteration where the lambda occurred.

    # Copy the config, because we make some changes to it as we go
    config = copy.deepcopy(config)

    archived_streams = []   # Streams with connected vis_writers/flag_writers
    g = networkx.MultiDiGraph(
        archived_streams=archived_streams,  # For access as g.graph['archived_streams']
        config=lambda resolver: {'sdp_archived_streams': archived_streams}
    )

    # telstate node
    telstate = _make_telstate(g, config)

    # Make a list of input streams of each type and add SPEAD endpoints to
    # the graph.
    inputs = {}
    input_multicast = []
    for name, input_ in config['inputs'].items():
        inputs.setdefault(input_['type'], []).append(name)
        url = input_['url']
        parts = urllib.parse.urlsplit(url)
        if parts.scheme == 'spead':
            node = LogicalMulticast('multicast.' + name,
                                    endpoint=Endpoint(parts.hostname, parts.port))
            g.add_node(node)
            input_multicast.append(node)

    # cam2telstate node (optional: if we're simulating, we don't need it)
    if 'cam.http' in inputs:
        cam2telstate = _make_cam2telstate(g, config, inputs['cam.http'][0])
        for node in input_multicast:
            g.add_edge(node, cam2telstate, depends_ready=True)
    else:
        cam2telstate = None

    # meta_writer node
    meta_writer = _make_meta_writer(g, config)

    # Find all input streams that are actually used. At the moment only the
    # direct dependencies are gathered because those are the only ones that
    # can have the simulate flag anyway.
    inputs_used = set()
    for output in config['outputs'].values():
        inputs_used.update(output['src_streams'])

    # Simulators for input streams where requested
    cbf_streams = inputs.get('cbf.baseline_correlation_products', [])
    cbf_streams.extend(inputs.get('cbf.tied_array_channelised_voltage', []))
    for name in cbf_streams:
        if name in inputs_used and config['inputs'][name]['simulate'] is not False:
            _make_cbf_simulator(g, config, name)

    # Group outputs by type
    outputs = {}
    for name, output in config['outputs'].items():
        outputs.setdefault(output['type'], []).append(name)

    # Pair up spectral and continuum L0 outputs
    l0_done = set()
    for name in outputs.get('sdp.vis', []):
        if config['outputs'][name]['continuum_factor'] == 1:
            for name2 in outputs['sdp.vis']:
                if name2 not in l0_done and config['outputs'][name2]['continuum_factor'] > 1:
                    # Temporarily alter it to check if they're the same other
                    # than continuum_factor and archive.
                    cont_factor = config['outputs'][name2]['continuum_factor']
                    archive = config['outputs'][name2]['archive']
                    config['outputs'][name2]['continuum_factor'] = 1
                    config['outputs'][name2]['archive'] = config['outputs'][name]['archive']
                    match = (config['outputs'][name] == config['outputs'][name2])
                    config['outputs'][name2]['continuum_factor'] = cont_factor
                    config['outputs'][name2]['archive'] = archive
                    if match:
                        _adjust_ingest_output_channels(config, [name, name2])
                        _make_ingest(g, config, name, name2)
                        _make_timeplot_correlator(g, config, name)
                        l0_done.add(name)
                        l0_done.add(name2)
                        break

    l0_spectral_only = False
    l0_continuum_only = False
    for name in set(outputs.get('sdp.vis', [])) - l0_done:
        is_spectral = config['outputs'][name]['continuum_factor'] == 1
        if is_spectral:
            l0_spectral_only = True
        else:
            l0_continuum_only = True
        _adjust_ingest_output_channels(config, [name])
        if is_spectral:
            _make_ingest(g, config, name, None)
            _make_timeplot_correlator(g, config, name)
        else:
            _make_ingest(g, config, None, name)
    if l0_continuum_only and l0_spectral_only:
        logger.warning('Both continuum-only and spectral-only L0 streams found - '
                       'perhaps they were intended to be matched?')

    for name in outputs.get('sdp.vis', []):
        if config['outputs'][name]['archive']:
            _make_vis_writer(g, config, name, 'archive', local=True)

    have_cal = set()
    for name in outputs.get('sdp.cal', []):
        src_name = config['outputs'][name]['src_streams'][0]
        # Check for a corresponding flags outputs
        flags_names = []
        for name2 in outputs.get('sdp.flags', []):
            if config['outputs'][name2]['calibration'][0] == name:
                flags_names.append(name2)
        if _make_cal(g, config, name, src_name, flags_names):
            have_cal.add(name)
            for flags_name in flags_names:
                if config['outputs'][flags_name]['archive']:
                    # Pass l0 name to flag writer to allow calc of bandwidths and sizes
                    flags_l0_name = config['outputs'][flags_name]['src_streams'][0]
                    _make_flag_writer(g, config, flags_name, flags_l0_name, 'archive', local=True)
    for name in outputs.get('sdp.beamformer', []):
        _make_beamformer_ptuse(g, config, name)
    for name in outputs.get('sdp.beamformer_engineering', []):
        _make_beamformer_engineering(g, config, name)

    # Collect all tied-array-channelised-voltage streams and make signal displays for them
    # XXX Temporarily disabled due to MKAIV-1331.
    # for name in inputs.get('cbf.tied_array_channelised_voltage', []):
    #     if name in inputs_used:
    #         _make_timeplot_beamformer(g, config, name)

    for name in outputs.get('sdp.continuum_image', []):
        orig_flags_name = config['outputs'][name]['src_streams'][0]
        orig_l0_name = config['outputs'][orig_flags_name]['src_streams'][0]
        cal = config['outputs'][orig_flags_name]['calibration'][0]
        if cal not in have_cal:
            continue      # If fewer than 4 antennas for example
        _make_vis_writer(g, config, orig_l0_name,
                         s3_name='continuum', local=False, prefix=name)
        _make_flag_writer(g, config, orig_flags_name, orig_l0_name,
                          s3_name='continuum', local=False, prefix=name)

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
                node.wrapper = config['config'].get('wrapper')
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
            force_host = config['config']['service_overrides'].get(node.name, {}).get('host')
            if force_host is not None:
                node.host = force_host
    return g


def _make_continuum_imager(g, config, name):
    output = config['outputs'][name]
    l1_flags_name = output['src_streams'][0]
    l0_name = config['outputs'][l1_flags_name]['src_streams'][0]
    l0_info = L0Info(config, l0_name)
    if l0_info.n_antennas < 4:
        # Won't be any calibration solutions
        return None

    data_url = 'redis://{endpoints[telstate_telstate]}/?capture_block_id={capture_block_id}'
    data_url += '&stream_name={}.{}'.format(name, urllib.parse.quote_plus(l0_name))
    cpus = 24 if not is_develop(config) else 2

    imager = SDPLogicalTask('continuum_image.' + name)
    imager.cpus = cpus
    # These resources are very rough estimates
    imager.mem = 50000 if not is_develop(config) else 8000
    imager.disk = _mb(1000 * l0_info.size + 1000)
    imager.max_run_time = 86400     # 24 hours
    imager.image = 'katsdpcontim'
    imager.command = [
        'run-and-cleanup', '/mnt/mesos/sandbox/{capture_block_id}_aipsdisk', '--',
        'continuum_pipeline.py',
        '--telstate', '{endpoints[telstate_telstate]}',
        '--access-key', '{resolver.s3_config[continuum][read][access_key]}',
        '--secret-key', '{resolver.s3_config[continuum][read][secret_key]}',
        '--select', 'scans="track"; corrprods="cross"',
        '--mfimage', 'nThreads={}'.format(cpus),
        '-w', '/mnt/mesos/sandbox', data_url
    ]
    g.add_node(imager)
    return imager


def build_postprocess_logical_graph(config):
    g = networkx.MultiDiGraph()
    telstate = scheduler.LogicalExternal('telstate')
    g.add_node(telstate)

    for name, output in config['outputs'].items():
        if output['type'] == 'sdp.continuum_image':
            _make_continuum_imager(g, config, name)

    seen = set()
    for node in g:
        if isinstance(node, SDPLogicalTask):
            # TODO: most of this code is shared by _build_logical_graph
            assert node.name not in seen, "{} appears twice in graph".format(node.name)
            seen.add(node.name)
            assert node.image in IMAGES, "{} missing from IMAGES".format(node.image)
            # Connect every task to telstate
            g.add_edge(node, telstate, port='telstate',
                       depends_ready=True, depends_kill=True)

    return g
