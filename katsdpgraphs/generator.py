from __future__ import print_function, division, absolute_import
import logging
import networkx as nx
import re
from collections import OrderedDict
import trollius
from trollius import From
import six
import addict
from katsdpcontroller import scheduler
from katsdpcontroller.tasks import SDPLogicalTask, SDPPhysicalTask, SDPPhysicalTaskBase


INGEST_GPU_NAME = 'GeForce GTX TITAN X'
logger = logging.getLogger(__name__)


class LogicalMulticast(scheduler.LogicalExternal):
    def __init__(self, name):
        super(LogicalMulticast, self).__init__(name)
        self.physical_factory = PhysicalMulticast


class PhysicalMulticast(scheduler.PhysicalExternal):
    @trollius.coroutine
    def resolve(self, resolver, graph, loop):
        yield From(super(PhysicalMulticast, self).resolve(resolver, graph, loop))
        self.host = resolver.resources.get_multicast_ip(self.logical_node.name)
        self.ports = {'spead': resolver.resources.get_port(self.logical_node.name)}


class TelstateTask(SDPPhysicalTaskBase):
    @trollius.coroutine
    def resolve(self, resolver, graph, loop):
        yield From(super(TelstateTask, self).resolve(resolver, graph, loop))
        # Add a port mapping
        self.taskinfo.container.docker.network = 'BRIDGE'
        portmap = addict.Dict()
        portmap.host_port = self.ports['telstate']
        portmap.container_port = 6379
        portmap.protocol = 'tcp'
        self.taskinfo.container.docker.port_mappings = [portmap]


class IngestTask(SDPPhysicalTask):
    @trollius.coroutine
    def resolve(self, resolver, graph, loop):
        yield From(super(IngestTask, self).resolve(resolver, graph, loop))
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
            image_path = yield From(resolver.image_resolver('katsdpingest_' + mangled, loop))
            self.taskinfo.container.docker.image = image_path
            logger.info('Develop mode: using %s for ingest', image_path)


def build_logical_graph(beamformer_mode, simulate, develop, cbf_channels, l0_antennas, dump_rate):
    from katsdpcontroller.sdpcontroller import State

    # CBF only does power-of-two numbers of antennas, with 4 being the minimum
    cbf_antennas = 4
    while cbf_antennas < l0_antennas:
        cbf_antennas *= 2

    cbf_baselines = cbf_antennas * (cbf_antennas + 1) * 2
    cbf_vis = cbf_baselines * cbf_channels    # visibilities per frame
    cbf_vis_size = cbf_vis * 8                # 8 is size of complex64
    cbf_vis_mb = cbf_vis_size / 1024**2
    cbf_gains_mb = cbf_channels * cbf_antennas * 4 * 8 / 1024**2

    # Except where otherwise noted, l0 values are for the spectral product
    l0_channels = cbf_channels                # could differ in future
    l0_baselines = l0_antennas * (l0_antennas + 1) * 2
    l0_vis = l0_baselines * l0_channels
    # vis, flags, weights, plus per-channel float32 weight
    l0_size = l0_vis * 10 + l0_channels * 4
    l0_cont_factor = 32
    l0_cont_size = l0_size // l0_cont_factor
    l0_mb = l0_size / 1024**2
    l0_cont_mb = l0_cont_size / 1024**2
    l0_gains_mb = l0_channels * l0_antennas * 4 * 8 / 1024**2
    # Extra memory allocation for tasks that deal with bandpass calibration
    # solutions in telescope state. The exact size of these depends on how
    # often bandpass calibrators are visited, so the scale factor is a
    # thumb-suck. The scale factors are
    # - 2 pols per antenna
    # - 8 bytes per value (complex64)
    # - 200 solutions
    # - 3: conservative estimate of bloat from text-based pickling
    # - /1024**2 to convert to megabytes
    bp_mb = l0_channels * (2 * l0_antennas) * 8 * 3 * 200 / 1024**2
    # Bandwidths, in bits per second
    # * 8 for bytes -> bits, * 1.05 + 2048 to add room for network overheads
    cbf_vis_bandwidth = (cbf_vis_size * 8 * 1.05 + 2048) * dump_rate
    l0_bandwidth = (l0_size * 8 * 1.05 + 2048) * dump_rate
    l0_cont_bandwidth = (l0_cont_size * 8 * 1.05 + 2048) * dump_rate

    g = nx.MultiDiGraph(config=lambda resolver: {
        'sdp_cbf_channels': cbf_channels,
        'cal_refant': '',
        'cal_g_solint': 10,
        'cal_bp_solint': 10,
        'cal_k_solint': 10,
        'cal_k_chan_sample': 10
    })

    # Volume serviced by katsdptransfer to transfer results to the archive
    data_vol = scheduler.VolumeRequest('data', '/var/kat/data', 'RW')
    # Volume for persisting user configuration
    config_vol = scheduler.VolumeRequest('config', '/var/kat/config', 'RW')
    # Volume for writing results that are not archived, but do not have
    # strong requirements on performance or capacity.
    local_data_vol = scheduler.VolumeRequest('local_data', '/var/kat/data', 'RW')

    capture_transitions = {State.INITIALISED: 'capture-init', State.DONE: 'capture-done'}

    # Multicast groups
    bcp_spead = LogicalMulticast('i0.baseline-correlation-products')
    g.add_node(bcp_spead)
    beams_spead = OrderedDict()
    if beamformer_mode != 'none':
        for suffix in ['0x', '0y']:
            beam = 'i0.tied-array-channelised-voltage.{}'.format(suffix)
            beams_spead[beam] = LogicalMulticast(beam)
            g.add_node(beams_spead[beam])
    l0_spectral = LogicalMulticast('l0_spectral')
    g.add_node(l0_spectral)
    l0_continuum = LogicalMulticast('l0_continuum')
    g.add_node(l0_continuum)
    l1_spectral = LogicalMulticast('l1_spectral')
    g.add_node(l1_spectral)

    # telstate node
    telstate = SDPLogicalTask('sdp.telstate')
    telstate.cpus = 0.1
    telstate.mem = 1024 + bp_mb
    telstate.disk = telstate.mem
    telstate.image = 'redis'
    telstate.ports = ['telstate']
    telstate.physical_factory = TelstateTask
    g.add_node(telstate)

    # cam2telstate node
    if not simulate:
        cam2telstate = SDPLogicalTask('sdp.cam2telstate.1')
        cam2telstate.image = 'katsdpingest'
        cam2telstate.command = ['cam2telstate.py']
        cam2telstate.cpus = 0.2
        cam2telstate.mem = 256
        streams = {
            'i0.baseline-correlation-products': 'visibility',
            'i0.antenna-channelised-voltage': 'fengine'
        }
        for beam in six.iterkeys(beams_spead):
            streams[beam] = 'beamformer'
        streams_arg = ','.join("{}:{}".format(key, value) for key, value in streams.items())
        g.add_node(cam2telstate, config=lambda task, resolver: {
            'url': resolver.resources.get_url('camdata'),
            'streams': streams_arg,
            'collapse_streams': True
        })

    # signal display node
    timeplot = SDPLogicalTask('sdp.timeplot.1')
    timeplot.image = 'katsdpdisp'
    timeplot.command = ['time_plot.py']
    # Exact requirement not known (also depends on number of users). Give it
    # 2 CPUs (max it can use) for 32 antennas, 32K channels and scale from there.
    timeplot.cpus = 2 * min(1.0, l0_vis / (32 * 33 * 2 * 32768))
    # Give timeplot enough memory for 256 time samples, but capped at 16GB.
    # This formula is based on data.py in katsdpdisp.
    percentiles = 5 * 8
    timeplot_slot = cbf_channels * (l0_baselines + percentiles) * 8
    timeplot_buffer = min(256 * timeplot_slot, 16 * 1024**3)
    timeplot_buffer_mb = timeplot_buffer / 1024**2
    # timeplot_buffer covers only the visibilities, but there are also flags
    # and various auxiliary buffers. Add 20% to give some headroom, and also
    # add a fixed amount since in very small arrays the 20% might not cover
    # the fixed-sized overheads.
    timeplot.mem = timeplot_buffer_mb * 1.2 + 256
    timeplot.ports = ['spead_port', 'html_port', 'data_port']
    timeplot.wait_ports = ['html_port', 'data_port']
    timeplot.volumes = [config_vol]
    timeplot.gui_urls = [{
        'title': 'Signal Display',
        'description': 'Signal displays for {0.subarray_name}',
        'href': 'http://{0.host}:{0.ports[html_port]}/',
        'category': 'Plot'
    }]
    g.add_node(timeplot, config=lambda task, resolver: {
        'cbf_channels': cbf_channels,
        'config_base': '/var/kat/config/.katsdpdisp',
        'memusage': -timeplot_buffer_mb     # Negative value gives MB instead of %
    })

    # ingest nodes
    n_ingest = 1
    for i in range(1, n_ingest + 1):
        ingest = SDPLogicalTask('sdp.ingest.{}'.format(i))
        ingest.physical_factory = IngestTask
        ingest.image = 'katsdpingest_titanx'
        ingest.command = ['ingest.py']
        ingest.ports = ['port']
        ingest.gpus = [scheduler.GPURequest()]
        if not develop:
            ingest.gpus[-1].name = INGEST_GPU_NAME
        # Scale for a full GPU for 32 antennas, 32K channels
        scale = cbf_vis / (32 * 33 * 2 * 32768)
        ingest.gpus[0].compute = scale
        # Refer to https://docs.google.com/spreadsheets/d/13LOMcUDV1P0wyh_VSgTOcbnhyfHKqRg5rfkQcmeXmq0/edit
        # We use slightly higher multipliers to be safe, as well as
        # conservatively using cbf_* instead of l0_*.
        ingest.gpus[0].mem = (70 * cbf_vis + 168 * cbf_channels) // 1024**2 + 128
        # Actual requirements haven't been measured. Scale things so that
        # 32 antennas, 32K channels uses a bit less than 8 CPUs (the number
        # in an ingest machine).
        ingest.cpus = 7.5 * scale
        # Scale factor of 32 may be overly conservative: actual usage may be
        # only half this. This also leaves headroom for buffering L0 output.
        ingest.mem = 32 * cbf_vis_mb + 4096
        ingest.transitions = capture_transitions
        ingest.networks = [scheduler.InterfaceRequest('cbf'), scheduler.InterfaceRequest('sdp_10g')]
        ingest.networks[0].bandwidth_in = cbf_vis_bandwidth
        ingest.networks[1].bandwidth_out = l0_bandwidth + l0_cont_bandwidth
        g.add_node(ingest, config=lambda task, resolver: {
            'continuum_factor': l0_cont_factor,
            'sd_continuum_factor': cbf_channels // 256,
            'cbf_channels': cbf_channels,
            'sd_spead_rate': 3e9    # local machine, so crank it up a bit (TODO: no longer necessarily true)
        })
        g.add_edge(ingest, bcp_spead, port='spead', config=lambda task, resolver, endpoint: {
            'cbf_spead': str(endpoint)})
        g.add_edge(ingest, l0_spectral, port='spead', config=lambda task, resolver, endpoint: {
            'l0_spectral_spead': str(endpoint)})
        g.add_edge(ingest, l0_continuum, port='spead', config=lambda task, resolver, endpoint: {
            'l0_continuum_spead': str(endpoint)})
        g.add_edge(ingest, timeplot, port='spead_port', config=lambda task, resolver, endpoint: {
            'sdisp_spead': str(endpoint)})

    # TODO: this hard-codes L band - the ADC rate for other bands may be
    # different. It also hard-codes the bits-per-sample. The 1.05 is to account
    # for network overheads.
    bf_bandwidth_1pol = 1712000000.0 * 8 * 1.05
    if beamformer_mode == 'ptuse':
        bf_ingest = SDPLogicalTask('sdp.bf_ingest.1')
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
        bf_ingest.networks = [scheduler.InterfaceRequest('cbf', infiniband=True, affinity=True)]
        bf_ingest.networks[0].bandwidth_in = bf_bandwidth_1pol * 2
        bf_ingest.volumes = [scheduler.VolumeRequest('data', '/data', 'RW')]
        bf_ingest.container.docker.parameters = [{'key': 'ipc', 'value': 'host'}]
        bf_ingest.transitions = capture_transitions
        g.add_node(bf_ingest, config=lambda task, resolver: {
            'cbf_channels': cbf_channels
        })
        for stream in six.itervalues(beams_spead):
            g.add_edge(bf_ingest, stream, port='spead', config=lambda task, resolver, endpoint: {
                'cbf_spead{}'.format(stream.name[-1]): str(endpoint)})
    elif beamformer_mode != 'none':
        ram = beamformer_mode == 'hdf5_ram'
        for i, stream in enumerate(six.itervalues(beams_spead)):
            bf_ingest = SDPLogicalTask('sdp.bf_ingest.{}'.format(i + 1))
            bf_ingest.image = 'katsdpingest'
            bf_ingest.command = ['schedrr', 'bf_ingest.py']
            bf_ingest.cpus = 2
            bf_ingest.cores = ['disk', 'network']
            bf_ingest.capabilities.append('SYS_NICE')
            if not ram:
                # CBF sends 256 time samples per heap, and bf_ingest accumulates
                # 128 of these in the ring buffer. It's not a lot of memory, so
                # to be on the safe side we double everything. Values are int8*2.
                # Allow 512MB for various buffers.
                bf_ingest.mem = 256 * 256 * 2 * cbf_channels / 1024**2 + 512
            else:
                # When writing to tmpfs, the file is accounted as memory to our
                # process, so we need more memory allocation than there is
                # space in the ramdisk. This is only used for lab testing, so
                # we just hardcode a number.
                bf_ingest.ram = 210 * 1024
            bf_ingest.interfaces = [scheduler.InterfaceRequest('cbf', infiniband=True)]
            bf_ingest.interfaces[0].bandwidth_in = bf_bandwidth_1pol
            volume_name = 'bf_ram{}' if ram else 'bf_ssd{}'
            bf_ingest.volumes = [
                scheduler.VolumeRequest(volume_name.format(i), '/data', 'RW', affinity=ram)]
            bf_ingest.ports = ['port']
            bf_ingest.transitions = capture_transitions
            g.add_node(bf_ingest, config=lambda task, resolver: {
                'file_base': '/data',
                'affinity': [task.cores['disk'], task.cores['network']],
                'interface': task.interfaces['cbf'].name,
                'ibv': True,
                'direct_io': beamformer_mode == 'hdf5_ssd',   # Can't use O_DIRECT on tmpfs
                'stream_name': stream.name
            })
            g.add_edge(bf_ingest, stream, port='spead', config=lambda task, resolver, endpoint: {
                'cbf_spead': str(endpoint)})

    # Calibration node (only possible to calibrate with at least 4 antennas)
    if l0_antennas >= 4:
        cal = SDPLogicalTask('sdp.cal.1')
        cal.image = 'katsdpcal'
        cal.command = ['run_cal.py']
        # TODO: not clear exactly how much CPU cal needs, although currently
        # it doesn't make full use of multi-core. Assume 2 CPUs for 16
        # antennas, 32K channels, and assume linear scale in antennas and
        # channels.
        cal.cpus = max(0.1, 2 * (l0_antennas * l0_channels) / (16 * 32768))
        # Main memory consumer is buffers for
        # - visibilities (complex64)
        # - flags (uint8)
        # - weights (float32)
        # There are also timestamps, but they're insignificant compared to the rest.
        # We want ~30 min of data per buffer
        slots = 30 * 60 * dump_rate
        buffer_size = slots * l0_vis * 13
        # There are two buffers, and also some arrays that are reduced versions
        # of the main buffers. The reduction factors are variable, but 10%
        # overhead should be enough. Finally, allow 256MB for general use.
        cal.mem = 2 * buffer_size * 1.1 / 1024**2 + 256
        cal.volumes = [data_vol]
        cal.interfaces = [scheduler.InterfaceRequest('sdp_10g')]
        cal.interfaces[0].bandwidth_in = l0_bandwidth
        g.add_node(cal, config=lambda task, resolver: {
            'cbf_channels': cbf_channels,
            'buffer_maxsize': buffer_size
        })
        g.add_edge(cal, l0_spectral, port='spead', config=lambda task, resolver, endpoint: {
            'l0_spectral_spead': str(endpoint)})
        g.add_edge(cal, l1_spectral, port='spead', config=lambda task, resolver, endpoint: {
            'l1_spectral_spead': str(endpoint)})

    # filewriter node
    filewriter = SDPLogicalTask('sdp.filewriter.1')
    filewriter.image = 'katsdpfilewriter'
    filewriter.command = ['file_writer.py']
    # Don't yet have a good idea of real CPU usage. For now assume that 16
    # antennas, 32K channels requires two CPUs (one for capture, one for
    # writing) and scale from there.
    filewriter.cpus = 2 * l0_vis / (16 * 17 * 2 * 32768)
    # Memory pool has 8 entries, but allocate 16 to be safe.
    # Filewriter also uses this (incorrect) formula for heap size.
    filewriter.mem = 16 * (16 * 17 * 2 * 32768 * 9) / 1024**2 + bp_mb + 256
    filewriter.ports = ['port']
    filewriter.volumes = [data_vol]
    filewriter.interfaces = [scheduler.InterfaceRequest('sdp_10g')]
    filewriter.interfaces[0].bandwidth_in = l0_bandwidth
    filewriter.transitions = capture_transitions
    g.add_node(filewriter, config=lambda task, resolver: {'file_base': '/var/kat/data'})
    g.add_edge(filewriter, l0_spectral, port='spead', config=lambda task, resolver, endpoint: {
        'l0_spectral_spead': str(endpoint)})

    # Simulator node
    if simulate:
        sim = SDPLogicalTask('sdp.sim.1')
        sim.image = 'katcbfsim'
        # create-fx-stream is passed on the command-line instead of telstate
        # for now due to SR-462.
        sim.command = ['cbfsim.py', '--create-fx-stream', 'i0.baseline-correlation-products']
        # It's mostly GPU work, so not much CPU requirement. Scale for 2 CPUs for
        # 16 antennas, 32K, and cap it there (threads for compute and network).
        # cbf_vis is an overestimate since the simulator is not constrained to
        # power-of-two antenna counts like the real CBF.
        scale = cbf_vis / (16 * 17 * 2 * 32768)
        sim.cpus = 2 * min(1.0, scale)
        # Factor of 4 is conservative; only actually double-buffered
        sim.mem = 4 * cbf_vis_mb + cbf_gains_mb + 512
        sim.cores = [None, None]
        sim.gpus = [scheduler.GPURequest()]
        # Scale for 20% at 16 ant, 32K channels
        sim.gpus[0].compute = min(1.0, 0.2 * scale)
        sim.gpus[0].mem = 2 * cbf_vis_mb + cbf_gains_mb + 256
        sim.ports = ['port']
        sim.interfaces = [scheduler.InterfaceRequest('cbf')]
        sim.interfaces[0].bandwidth_out = cbf_vis_bandwidth
        g.add_node(sim, config=lambda task, resolver: {
            'cbf_channels': cbf_channels
        })
        g.add_edge(sim, bcp_spead, port='spead', config=lambda task, resolver, endpoint: {
            'cbf_spead': str(endpoint)
        })

    for node in g:
        if node is not telstate and isinstance(node, SDPLogicalTask):
            node.command.extend([
                '--telstate', '{endpoints[sdp.telstate_telstate]}',
                '--name', node.name])
            g.add_edge(node, telstate, port='telstate', order='strong')
    return g


def graph_parameters(graph_name):
    """Convert a product name into a dictionary of keyword parameters to pass to
    :func:`build_physical_graph`.

    Parameters
    ----------
    graph_name : str
        Logical graph name such as ``bc856M4k``

    Returns
    -------
    parameters : dict
        Key-value pairs to pass as parameters to :func:`build_physical_graph`
    """
    match = re.match('^(?P<mode>c|bc)856M(?P<channels>4|32)k$', graph_name)
    if not match:
        match = re.match('^bec856M(?P<channels>4|32)k(?P<mode>ssd|ram)$', graph_name)
        if not match:
            raise ValueError('Unsupported graph ' + graph_name)
    beamformer_modes = {'c': 'none', 'bc': 'ptuse', 'ssd': 'hdf5_ssd', 'ram': 'hdf5_ram'}
    beamformer_mode = beamformer_modes[match.group('mode')]
    return dict(beamformer_mode=beamformer_mode,
                cbf_channels=int(match.group('channels')) * 1024)
