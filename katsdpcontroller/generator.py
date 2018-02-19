import math
import logging
import re
import time
import copy
import fractions
import urllib

import networkx

import addict

from katsdptelstate.endpoint import Endpoint, endpoint_list_parser

from katsdpcontroller import scheduler
from katsdpcontroller.tasks import (
    SDPLogicalTask, SDPPhysicalTask, SDPPhysicalTaskBase, LogicalGroup, State)


INGEST_GPU_NAME = 'GeForce GTX TITAN X'
CAPTURE_TRANSITIONS = {
    (State.IDLE, State.CAPTURING): ['capture-init', '{capture_block_id}'],
    (State.CAPTURING, State.IDLE): ['capture-done']
}
#: Docker images that may appear in the logical graph (used set to Docker image metadata)
IMAGES = frozenset([
    'beamform',
    'katcbfsim',
    'katsdpcal',
    'katsdpdisp',
    'katsdpingest',
    'katsdpingest_titanx',
    'katsdpfilewriter',
    'redis'
])
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
    async def resolve(self, resolver, graph, loop):
        await super().resolve(resolver, graph, loop)
        if self.logical_node.endpoint is not None:
            self.host = self.logical_node.endpoint.host
            self.ports = {'spead': self.logical_node.endpoint.port}
        else:
            self.host = resolver.resources.get_multicast_ip(self.logical_node.n_addresses)
            self.ports = {'spead': resolver.resources.get_port()}


class TelstateTask(SDPPhysicalTaskBase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.capture_blocks = None   #: Set by sdpcontroller to mirror the subarray product

    async def resolve(self, resolver, graph, loop):
        await super().resolve(resolver, graph, loop)
        # Add a port mapping
        self.taskinfo.container.docker.network = 'BRIDGE'
        portmap = addict.Dict()
        portmap.host_port = self.ports['telstate']
        portmap.container_port = 6379
        portmap.protocol = 'tcp'
        self.taskinfo.container.docker.port_mappings = [portmap]

    async def graceful_kill(self, driver, **kwargs):
        try:
            for capture_block in list(self.capture_blocks.values()):
                await capture_block.dead_event.wait()
        except Exception:
            logger.exception('Exception in graceful shutdown of %s, killing it', self.name)
        await super().graceful_kill(driver, **kwargs)


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
    return config['config'].get('develop', False)


def _mb(value):
    """Convert bytes to mebibytes"""
    return value / 1024**2


def _round_down(value, period):
    return value // period * period


def _round_up(value, period):
    return _round_down(value - 1, period) + period


def _bandwidth(size, time, ratio=1.05, overhead=2048):
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
    def net_bandwidth(self, ratio=1.05, overhead=2048):
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
    def net_bandwidth(self, ratio=1.05, overhead=2048):
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
        if 'output_channels' in self.raw:
            return tuple(self.raw['output_channels'])
        else:
            return (0, self.src_info.n_channels)

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
    def net_bandwidth(self, ratio=1.05, overhead=2048):
        return _bandwidth(self.size, self.int_time, ratio, overhead)


def find_node(g, name):
    for node in g:
        if node.name == name:
            return node
    raise KeyError('no node called ' + name)


def _make_telstate(g, config):
    telstate = SDPLogicalTask('telstate')
    telstate.cpus = 0.1
    telstate.mem = 8192
    telstate.disk = telstate.mem
    telstate.image = 'redis'
    telstate.ports = ['telstate']
    # Run it in /mnt/mesos/sandbox so that the dump.rdb ends up there.
    telstate.command = ['sh', '-c', 'cd /mnt/mesos/sandbox && exec redis-server']
    telstate.physical_factory = TelstateTask
    telstate.deconfigure_wait = False
    g.add_node(telstate)
    return telstate


def _make_cam2telstate(g, config, name):
    cam2telstate = SDPLogicalTask('cam2telstate')
    cam2telstate.image = 'katsdpingest'
    cam2telstate.command = ['cam2telstate.py']
    cam2telstate.cpus = 0.2
    cam2telstate.mem = 256
    cam2telstate.ports = ['port']
    url = config['inputs'][name]['url']
    g.add_node(cam2telstate, config=lambda task, resolver: {
        'url': url
    })
    return cam2telstate


def _make_cbf_simulator(g, config, name):
    type_ = config['inputs'][name]['type']
    settings = config['inputs'][name].get('simulate', {})
    if settings is True:
        settings = {}
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

    def make_config(task, resolver):
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
                'beamformer-timesteps': info.raw['spectra_per_heap'],
                'beamformer-bits': info.raw['beng_out_bits_per_sample']
            })
        if 'center_freq' in settings:
            conf['cbf_center_freq'] = float(settings['center_freq'])
        if 'antennas' in settings:
            conf['cbf_antennas'] = [{'description': value} for value in settings['antennas']]
        else:
            conf['antenna_mask'] = ','.join(info.antennas)
        return conf

    sim_group = LogicalGroup('sim.' + name)
    g.add_node(sim_group, config=make_config)
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
            sim.container.docker.parameters = [{"key": "ulimit", "value": "nofile=8192"}]
        sim.interfaces = [scheduler.InterfaceRequest('cbf', infiniband=ibv)]
        sim.interfaces[0].bandwidth_out = info.net_bandwidth
        sim.transitions = {
            (State.IDLE, State.CAPTURING): ['capture-start', name, '{time}'],
            (State.CAPTURING, State.IDLE): ['capture-stop', name]
        }

        g.add_node(sim, config=lambda task, resolver, server_id=i + 1: {
            'cbf_interface': task.interfaces['cbf'].name,
            'server_id': server_id
        })
        g.add_edge(sim_group, sim, depends_ready=True, depends_init=True)
        g.add_edge(sim, sim_group, depends_resources=True)
    return sim_group


def _make_timeplot(g, config, spectral_name):
    spectral_info = L0Info(config, spectral_name)

    # signal display node
    timeplot = SDPLogicalTask('timeplot.' + spectral_name)
    timeplot.image = 'katsdpdisp'
    timeplot.command = ['time_plot.py']
    # Exact requirement not known (also depends on number of users). Give it
    # 2 CPUs (max it can use) for 32 antennas, 32K channels and scale from there.
    timeplot.cpus = 2 * min(1.0, spectral_info.n_vis / _N32_32)
    # Give timeplot enough memory for 256 time samples, but capped at 16GB.
    # This formula is based on data.py in katsdpdisp.
    percentiles = 5 * 8
    timeplot_slot = spectral_info.n_channels * (spectral_info.n_baselines + percentiles) * 8
    timeplot_buffer = min(256 * timeplot_slot, 16 * 1024**3)
    timeplot_buffer_mb = timeplot_buffer / 1024**2
    # timeplot_buffer covers only the visibilities, but there are also flags
    # and various auxiliary buffers. Add 20% to give some headroom, and also
    # add a fixed amount since in very small arrays the 20% might not cover
    # the fixed-sized overheads.
    timeplot.mem = timeplot_buffer_mb * 1.2 + 256
    timeplot.ports = ['spead_port', 'html_port']
    timeplot.wait_ports = ['html_port']
    timeplot.volumes = [CONFIG_VOL]
    timeplot.gui_urls = [{
        'title': 'Signal Display',
        'description': 'Signal displays for {0.subarray_product_id}',
        'href': 'http://{0.host}:{0.ports[html_port]}/',
        'category': 'Plot'
    }]
    g.add_node(timeplot, config=lambda task, resolver: {
        'config_base': '/var/kat/config/.katsdpdisp',
        'l0_name': spectral_name,
        'memusage': -timeplot_buffer_mb     # Negative value gives MB instead of %
    })
    return timeplot


def _timeplot_frame_size(spectral_info, n_cont_channels):
    """Approximate size of the data sent from all ingest processes to timeplot per heap"""
    # This is based on _init_ig_sd from katsdpingest/ingest_session.py
    max_custom_signals = 128      # From katsdpdisp/scripts/time_plot.py
    n_perc_signals = 5 * 8
    n_spec_channels = spectral_info.n_channels
    n_bls = spectral_info.n_baselines
    ans = n_spec_channels * max_custom_signals * 9  # sd_data + sd_flags
    ans += max_custom_signals * 4                   # sd_data_index
    ans += n_cont_channels * n_bls * 9              # sd_blmx_data + sd_blmx_flags
    ans += n_bls * 12                               # sd_timeseries + sd_timeseriesabs
    ans += n_spec_channels * n_perc_signals * 5     # sd_percspectrum + sd_percspectrumflags
    # input names are e.g. m012v -> 5 chars, 2 inputs per baseline
    ans += n_bls * 10                               # bls_ordering
    # There are a few scalar values, but that doesn't add up to enough to worry about
    return ans


def n_ingest_nodes(config, name):
    """Number of processes used to implement ingest for a particular output."""
    # TODO: adjust based on the number of antennas and channels
    return 4 if not config['config'].get('develop', False) else 2


def _adjust_ingest_output_channels(config, names):
    """Modify the config dictionary to set output channels for ingest.

    The range is set to the full band if not currently set; otherwise, it
    is widened where necessary to meet alignment requirements.

    Parameters
    ----------
    config : dict
        Configuration dictionary
    names : list of str
        The names of the ingest output products. These must match i.e. be the
        same other than for continuum_factor.
    """
    def lcm(a, b):
        return a // fractions.gcd(a, b) * b

    n_ingest = n_ingest_nodes(config, names[0])
    outputs = [config['outputs'][name] for name in names]
    src = config['inputs'][outputs[0]['src_streams'][0]]
    acv = config['inputs'][src['src_streams'][0]]
    n_chans = acv['n_chans']
    requested = outputs[0].get('output_channels', [0, n_chans])
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
    sd_continuum_factor = 1
    # Aim for about 256 signal display coarse channels
    while (spectral_info.n_channels % (sd_continuum_factor * n_ingest * 2) == 0
           and spectral_info.n_channels // sd_continuum_factor >= 384):
        sd_continuum_factor *= 2

    sd_frame_size = _timeplot_frame_size(
        spectral_info, spectral_info.n_channels // sd_continuum_factor)
    # The rates are low, so we allow plenty of padding in case the calculation is
    # missing something.
    sd_spead_rate = _bandwidth(sd_frame_size, spectral_info.int_time, ratio=1.2, overhead=4096)
    output_channels_str = '{}:{}'.format(*spectral_info.output_channels)
    group_config = {
        'continuum_factor': continuum_info.raw['continuum_factor'] if continuum_name else 1,
        'sd_continuum_factor': sd_continuum_factor,
        'sd_spead_rate': sd_spead_rate / n_ingest,
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
    if continuum_name:
        continuum_multicast = LogicalMulticast('multicast.' + continuum_name, n_ingest)
        g.add_node(continuum_multicast)
        g.add_edge(ingest_group, continuum_multicast, port='spead', depends_resolve=True,
                   config=lambda task, resolver, endpoint: {'l0_continuum_spead': str(endpoint)})
        g.add_edge(continuum_multicast, ingest_group, depends_init=True, depends_ready=True)
    src_multicast = find_node(g, 'multicast.' + src)
    g.add_edge(ingest_group, src_multicast, port='spead', depends_resolve=True,
               config=lambda task, resolver, endpoint: {'cbf_spead': str(endpoint)})

    # TODO: get timeplot to use multicast
    if spectral_name:
        timeplot = find_node(g, 'timeplot.' + spectral_name)
        g.add_edge(ingest_group, timeplot, port='spead_port',
                   config=lambda task, resolver, endpoint: {'sdisp_spead': str(endpoint)})
        g.add_edge(timeplot, ingest_group, depends_ready=True)  # Attributes passed via telstate
    for i in range(1, n_ingest + 1):
        ingest = SDPLogicalTask('ingest.{}.{}'.format(name, i))
        ingest.physical_factory = IngestTask
        ingest.image = 'katsdpingest_titanx'
        ingest.command = ['ingest.py']
        ingest.ports = ['port']
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
            net_bandwidth += spectral_info.net_bandwidth
        if continuum_name:
            net_bandwidth += continuum_info.net_bandwidth
        ingest.interfaces[1].bandwidth_out = net_bandwidth / n_ingest

        def make_config(task, resolver, server_id=i):
            conf = {
                'cbf_interface': task.interfaces['cbf'].name,
                'server_id': server_id
            }
            if spectral_name:
                conf.update(l0_spectral_interface=task.interfaces['sdp_10g'].name)
            if continuum_name:
                conf.update(l0_continuum_interface=task.interfaces['sdp_10g'].name)
            return conf
        g.add_node(ingest, config=make_config)
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


def _make_cal(g, config, name, l0_name):
    info = L0Info(config, l0_name)
    if info.n_antennas < 4:
        # Only possible to calibrate with at least 4 antennas
        return None

    if name is None:
        name = 'cal.' + l0_name
        settings = {}
    else:
        settings = config['outputs'][name]
    # We want ~30 min of data in the buffer, to allow for a single batch of
    # 15 minutes.
    buffer_time = settings.get('buffer_time', 30.0 * 60.0)
    parameters = settings.get('parameters', {})
    models = settings.get('models', {})
    if parameters:
        raise NotImplementedError('cal parameters are not yet supported')
    if models:
        raise NotImplementedError('cal models are not yet supported')

    cal = SDPLogicalTask(name)
    cal.image = 'katsdpcal'
    cal.command = ['run_cal.py']
    # Some of the scaling in cal is non-linear, so we need to give smaller
    # arrays more CPU than might be suggested by linear scaling. As a
    # heuristic, we start with linear scaling for 4 CPUs for 32K, 16 antennas,
    # 2s, but clamp to just below 8 CPUs (we use 1.99s instead of 2s since
    # actual integration times are just under 2s).
    # However, don't go below 2 CPUs (except in development mode) because we
    # can't have less than 1 pipeline worker.
    cal.cpus = 4 * info.n_vis / _N16_32 * 1.99 / info.int_time
    if not is_develop(config):
        cal.cpus = max(cal.cpus, 2)
    cal.cpus = min(cal.cpus, 7.9)
    workers = max(1, int(math.ceil(cal.cpus - 1)))
    # Main memory consumer is buffers for
    # - visibilities (complex64)
    # - flags (uint8)
    # - weights (float32)
    # There are also timestamps, but they're insignificant compared to the rest.
    slots = int(math.ceil(buffer_time / info.int_time))
    slot_size = info.n_vis * 13
    buffer_size = slots * slot_size
    # Processing operations come in a few flavours:
    # - average over time: need O(1) extra slots
    # - average over frequency: needs far less memory than the above
    # - compute flags per baseline: works on 16 baselines at a time.
    # In each case we arbitrarily allow for 4 times the result, per worker.
    # There is also a time- and channel-averaged version (down to 1024 channels)
    # of the HH and VV products for each scan. The number of scans is unknown,
    # but should always be less than 1000, and we allow a factor of 2 for
    # operations that work on it (which cancels the factor of 1/2 for only
    # having 2 of the 4 pol products).
    extra = max(workers / slots, min(16 * workers, info.n_baselines) / info.n_baselines) * 4
    extra += 1000 * 1024 / (slots * info.n_channels)

    # Extra memory allocation for tasks that deal with bandpass calibration
    # solutions in telescope state. The exact size of these depends on how
    # often bandpass calibrators are visited, so the scale factor is a
    # thumb-suck. The scale factors are
    # - 2 pols per antenna
    # - 8 bytes per value (complex64)
    # - 200 solutions
    # - 3: conservative estimate of bloat from text-based pickling
    telstate_extra = info.n_channels * info.n_pols * info.n_antennas * 8 * 3 * 200

    cal.mem = buffer_size * (1 + extra) / 1024**2 + 256
    cal.volumes = [DATA_VOL]
    cal.interfaces = [scheduler.InterfaceRequest('sdp_10g')]
    cal.interfaces[0].bandwidth_in = info.net_bandwidth
    cal.ports = ['port']
    cal.transitions = CAPTURE_TRANSITIONS
    cal.deconfigure_wait = False
    g.add_node(cal, telstate_extra=telstate_extra, config=lambda task, resolver: {
        'buffer_maxsize': buffer_size,
        'workers': workers,
        'l0_spectral_interface': task.interfaces['sdp_10g'].name,
        'l0_spectral_name': l0_name,
        # cal should change to remove "spectral"; both are provided for now
        'l0_interface': task.interfaces['sdp_10g'].name,
        'l0_name': l0_name
    })
    src_multicast = find_node(g, 'multicast.' + l0_name)
    g.add_edge(cal, src_multicast, port='spead',
               depends_resolve=True, depends_init=True, depends_ready=True,
               config=lambda task, resolver, endpoint: {
                   'l0_spectral_spead': str(endpoint),
                   'l0_spead': str(endpoint)
               })

    return cal


def _make_filewriter(g, config, name):
    info = L0Info(config, name)

    filewriter = SDPLogicalTask('filewriter.' + name)
    filewriter.image = 'katsdpfilewriter'
    filewriter.command = ['file_writer.py']
    # Don't yet have a good idea of real CPU usage. For now assume that 16
    # antennas, 32K channels requires two CPUs (one for capture, one for
    # writing) and scale from there.
    filewriter.cpus = 2 * info.n_vis / _N16_32
    # Memory pool has 8 entries, plus the socket buffer, but allocate 12 to be
    # safe.
    filewriter.mem = 12 * _mb(info.size) + 256
    filewriter.ports = ['port']
    filewriter.volumes = [DATA_VOL]
    filewriter.interfaces = [scheduler.InterfaceRequest('sdp_10g')]
    filewriter.interfaces[0].bandwidth_in = info.net_bandwidth
    filewriter.transitions = CAPTURE_TRANSITIONS
    g.add_node(filewriter, config=lambda task, resolver: {
        'file_base': '/var/kat/data',
        'l0_name': name,
        'l0_interface': task.interfaces['sdp_10g'].name
    })
    src_multicast = find_node(g, 'multicast.' + name)
    g.add_edge(filewriter, src_multicast, port='spead',
               depends_resolve=True, depends_init=True, depends_ready=True,
               config=lambda task, resolver, endpoint: {'l0_spead': str(endpoint)})
    return filewriter


def _make_vis_writer(g, config, name):
    info = L0Info(config, name)

    vis_writer = SDPLogicalTask('vis_writer.' + name)
    vis_writer.image = 'katsdpfilewriter'
    vis_writer.command = ['vis_writer.py']
    # Don't yet have a good idea of real CPU usage. For now assume that 32
    # antennas, 32K channels requires two CPUs (one for capture, one for
    # writing) and scale from there.
    vis_writer.cpus = 2 * info.n_vis / _N32_32

    # Estimate the size of the memory pool. The actual code is pretty complex,
    # so this is just to get the right scaling.
    target_obj_size = 10e6
    src_multicast = find_node(g, 'multicast.' + name)
    n_substreams = src_multicast.n_addresses
    # Number of chunks per dump (at minimum one for vis, flags, weights, weights_channel)
    n_chunks = max(4 * n_substreams, info.size / target_obj_size)
    GRAPH_SIZE_TO_WRITE = 200   # From vis_writer.py
    n_dumps_per_write = int(math.ceil(GRAPH_SIZE_TO_WRITE / n_chunks))
    n_dumps_to_buffer = 2 * (n_dumps_per_write + 1) + 4
    memory_pool = n_dumps_to_buffer * info.size

    # It's only a rough estimate, so double it to be on the safe side
    vis_writer.mem = _mb(2 * memory_pool) + 256
    vis_writer.ports = ['port']
    vis_writer.volumes = [OBJ_DATA_VOL]
    vis_writer.interfaces = [scheduler.InterfaceRequest('sdp_10g')]
    vis_writer.interfaces[0].bandwidth_in = info.net_bandwidth
    vis_writer.transitions = CAPTURE_TRANSITIONS
    g.add_node(vis_writer, config=lambda task, resolver: {
        'l0_name': name,
        'l0_interface': task.interfaces['sdp_10g'].name,
        'obj_size_mb': _mb(target_obj_size),
        'npy_path': '/var/kat/data',
        's3_endpoint_url': resolver.s3_config['url']
    })
    g.add_edge(vis_writer, src_multicast, port='spead',
               depends_resolve=True, depends_init=True, depends_ready=True,
               config=lambda task, resolver, endpoint: {'l0_spead': str(endpoint)})
    return vis_writer


def _make_beamformer_ptuse(g, config, name):
    output = config['outputs'][name]
    srcs = output['src_streams']
    info = [TiedArrayChannelisedVoltageInfo(config, src) for src in srcs]

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
    bf_ingest.container.docker.parameters = [
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


def _make_beamformer_engineering(g, config, name):
    output = config['outputs'][name]
    srcs = output['src_streams']
    ram = output['store'] == 'ram'

    nodes = []
    for i, src in enumerate(srcs):
        info = TiedArrayChannelisedVoltageInfo(config, src)
        requested_channels = output.get('output_channels', [0, info.n_channels])
        # Round to fit the endpoints, as required by bf_ingest.
        src_multicast = find_node(g, 'multicast.' + src)
        n_endpoints = len(endpoint_list_parser(None)(src_multicast.endpoint.host))
        channels_per_endpoint = info.n_channels // n_endpoints
        output_channels = [_round_down(requested_channels[0], channels_per_endpoint),
                           _round_up(requested_channels[1], channels_per_endpoint)]
        if output_channels != requested_channels:
            logger.info('Rounding output channels for %s from %s to %s',
                        name, requested_channels, output_channels)

        fraction = (output_channels[1] - output_channels[0]) / info.n_channels

        bf_ingest = SDPLogicalTask('bf_ingest.{}.{}'.format(name, i + 1))
        bf_ingest.image = 'katsdpingest'
        bf_ingest.command = ['schedrr', 'bf_ingest.py']
        bf_ingest.cpus = 2
        bf_ingest.cores = ['disk', 'network']
        bf_ingest.capabilities.append('SYS_NICE')
        if not ram:
            # bf_ingest accumulates 128 frames in the ring buffer. It's not a
            # lot of memory, so to be on the safe side we double everything.
            # Values are int8*2.  Allow 512MB for various buffers.
            bf_ingest.mem = 256 * info.size * fraction / 1024**2 + 512
        else:
            # When writing to tmpfs, the file is accounted as memory to our
            # process, so we need more memory allocation than there is
            # space in the ramdisk. This is only used for lab testing, so
            # we just hardcode a number.
            bf_ingest.ram = 220 * 1024
        bf_ingest.interfaces = [scheduler.InterfaceRequest('cbf', infiniband=True)]
        bf_ingest.interfaces[0].bandwidth_in = info.net_bandwidth * fraction
        volume_name = 'bf_ram{}' if ram else 'bf_ssd{}'
        bf_ingest.volumes = [
            scheduler.VolumeRequest(volume_name.format(i), '/data', 'RW', affinity=ram)]
        bf_ingest.ports = ['port']
        bf_ingest.transitions = CAPTURE_TRANSITIONS
        g.add_node(bf_ingest, config=lambda task, resolver, src=src: {
            'file_base': '/data',
            'affinity': [task.cores['disk'], task.cores['network']],
            'interface': task.interfaces['cbf'].name,
            'ibv': True,
            'direct_io': not ram,       # Can't use O_DIRECT on tmpfs
            'stream_name': src,
            'channels': '{}:{}'.format(*output_channels)
        })
        g.add_edge(bf_ingest, src_multicast, port='spead',
                   depends_resolve=True, depends_init=True, depends_ready=True,
                   config=lambda task, resolver, endpoint: {'cbf_spead': str(endpoint)})
        nodes.append(bf_ingest)
    return nodes


def build_logical_graph(config):
    # Note: a few lambdas in this function have default arguments e.g.
    # stream=stream. This is needed because capturing a loop-local variable in
    # a lambda captures only the final value of the variable, not the value it
    # had in the loop iteration where the lambda occurred.

    # Copy the config, because we make some changes to it as we go
    config = copy.deepcopy(config)

    g = networkx.MultiDiGraph(config=lambda resolver: {})

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
        if name in inputs_used and config['inputs'][name].get('simulate', False) is not False:
            _make_cbf_simulator(g, config, name)

    # Group outputs by type
    outputs = {}
    for name, output in config['outputs'].items():
        outputs.setdefault(output['type'], []).append(name)

    # Pair up spectral and continuum L0 outputs
    implicit_cal = (config['version'] == '1.0')
    l0_done = set()
    for name in outputs.get('sdp.l0', []):
        if config['outputs'][name]['continuum_factor'] == 1:
            for name2 in outputs['sdp.l0']:
                if name2 not in l0_done and config['outputs'][name2]['continuum_factor'] > 1:
                    # Temporarily alter it to check if they're the same other
                    # than continuum_factor
                    cont_factor = config['outputs'][name2]['continuum_factor']
                    config['outputs'][name2]['continuum_factor'] = 1
                    match = (config['outputs'][name] == config['outputs'][name2])
                    config['outputs'][name2]['continuum_factor'] = cont_factor
                    if match:
                        _adjust_ingest_output_channels(config, [name, name2])
                        _make_timeplot(g, config, name)
                        _make_ingest(g, config, name, name2)
                        if implicit_cal:
                            _make_cal(g, config, None, name)
                        _make_filewriter(g, config, name)
                        _make_vis_writer(g, config, name)
                        l0_done.add(name)
                        l0_done.add(name2)
    l0_spectral_only = False
    l0_continuum_only = False
    for name in set(outputs.get('sdp.l0', [])) - l0_done:
        is_spectral = config['outputs'][name]['continuum_factor'] == 1
        if is_spectral:
            l0_spectral_only = True
        else:
            l0_continuum_only = True
        _adjust_ingest_output_channels(config, [name])
        if is_spectral:
            _make_timeplot(g, config, name)
            _make_ingest(g, config, name, None)
            if implicit_cal:
                _make_cal(g, config, None, name)
            _make_filewriter(g, config, name)
        else:
            _make_ingest(g, config, None, name)
    if l0_continuum_only and l0_spectral_only:
        logger.warning('Both continuum-only and spectral-only L0 streams found - '
                       'perhaps they were intended to be matched?')

    for name in outputs.get('sdp.cal', []):
        _make_cal(g, config, name, config['outputs'][name]['src_streams'][0])
    for name in outputs.get('sdp.beamformer', []):
        _make_beamformer_ptuse(g, config, name)
    for name in outputs.get('sdp.beamformer_engineering', []):
        _make_beamformer_engineering(g, config, name)

    # Count large allocations in telstate, which affects memory usage of
    # telstate itself and any tasks that dump the contents of telstate.
    telstate_extra = 0
    for _node, data in g.nodes(True):
        telstate_extra += data.get('telstate_extra', 0)
    for node in g:
        if node is not telstate and isinstance(node, SDPLogicalTask):
            node.command.extend([
                '--telstate', '{endpoints[telstate_telstate]}',
                '--name', node.name])
            node.wrapper = config['config'].get('wrapper')
            g.add_edge(node, telstate, port='telstate',
                       depends_ready=True, depends_kill=True)
        if isinstance(node, SDPLogicalTask):
            assert node.image in IMAGES, "{} missing from IMAGES".format(node.image)
            # Increase memory allocation where it depends on telstate content
            if node is telstate or node.name.startswith('filewriter.'):
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
    return g


def build_postprocess_logical_graph(config):
    # This is just a stub implementation, until we have some postprocessing
    # tasks to run.
    g = networkx.MultiDiGraph()
    telstate = scheduler.LogicalExternal('telstate')
    g.add_node(telstate)

    for node in g:
        if isinstance(node, SDPLogicalTask):
            assert node.image in IMAGES, "{} missing from IMAGES".format(node.image)
    return g
