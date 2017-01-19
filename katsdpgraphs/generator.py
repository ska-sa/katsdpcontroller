from __future__ import print_function, division, absolute_import
import networkx as nx
import re
import addict
from katsdpcontroller import scheduler
from katsdpcontroller.tasks import SDPLogicalTask, SDPPhysicalTask, SDPPhysicalTaskBase


class LogicalMulticast(scheduler.LogicalExternal):
    def __init__(self, name):
        super(LogicalMulticast, self).__init__(name)
        self.physical_factory = PhysicalMulticast


class PhysicalMulticast(scheduler.PhysicalExternal):
    def resolve(self, resolver, graph):
        super(PhysicalMulticast, self).resolve(resolver, graph)
        self.host = resolver.resources.get_multicast_ip(self.logical_node.name)
        self.ports = {'spead': resolver.resources.get_port(self.logical_node.name)}


class GPURequestByName(scheduler.GPURequest):
    def __init__(self, name):
        super(GPURequestByName, self).__init__()
        self.name = name

    def matches(self, agent_gpu, numa_node):
        if agent_gpu.name != self.name:
            return False
        return super(GPURequestByName, self).matches(agent_gpu, numa_node)


class TelstateTask(SDPPhysicalTaskBase):
    def resolve(self, resolver, graph):
        super(TelstateTask, self).resolve(resolver, graph)
        # Add a port mapping
        self.taskinfo.container.docker.network = 'BRIDGE'
        portmap = addict.Dict()
        portmap.host_port = self.ports['telstate']
        portmap.container_port = 6379
        portmap.protocol = 'tcp'
        self.taskinfo.container.docker.port_mappings = [portmap]


def build_logical_graph(beamformer_mode, cbf_channels, simulate):
    from katsdpcontroller.sdpcontroller import State
    # TODO: missing network requests
    # TODO: all resources allocations are just guesses

    g = nx.MultiDiGraph(config=lambda resolver: {
        'sdp_cbf_channels': cbf_channels,
        'cal_refant': '',
        'cal_g_solint': 10,
        'cal_bp_solint': 10,
        'cal_k_solint': 10,
        'cal_k_chan_sample': 10
    })

    data_vol = scheduler.VolumeRequest('data', '/var/kat/data', 'RW')
    config_vol = scheduler.VolumeRequest('config', '/var/kat/config', 'RW')
    scratch_vol = scheduler.VolumeRequest('scratch', '/data', 'RW')

    capture_transitions = {State.INIT_WAIT: 'capture-init', State.DONE: 'capture-done'}

    # Multicast groups
    bcp_spead = LogicalMulticast('corr.baseline-correlation-products_spead')
    g.add_node(bcp_spead)
    if beamformer_mode != 'none':
        beams_spead = {}
        for beam in ['0x', '0y']:
            beams_spead[beam] = LogicalMulticast('corr.tied-array-channelised-voltage.{}_spead'.format(beam))
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
    telstate.mem = 1024
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
            'corr.baseline-correlation-products': 'visibility',
            'corr.antenna-channelised-voltage': 'fengine'
        }
        streams_arg = ','.join("{}:{}".format(key, value) for key, value in streams.items())
        g.add_node(cam2telstate, config=lambda resolver: {
            'url': resolver.resources.get_url('CAMDATA'),
            'streams': streams_arg,
            'collapse_streams': True
        })

    # signal display node
    timeplot = SDPLogicalTask('sdp.timeplot.1')
    timeplot.image = 'katsdpdisp'
    timeplot.command = ['time_plot.py']
    timeplot.cpus = 0.2          # TODO: uses more in reality
    timeplot.mem = 16384.0       # TODO: tie in with timeplot's memory allocation logic
    timeplot.cores = [None] * 2
    timeplot.ports = ['spead_port', 'html_port', 'data_port']
    timeplot.wait_ports = ['html_port', 'data_port']
    timeplot.volumes = [config_vol]

    # ingest nodes
    n_ingest = 1
    for i in range(1, n_ingest + 1):
        ingest = SDPLogicalTask('sdp.ingest.{}'.format(i))
        ingest.image = 'katsdpingest_titanx'
        ingest.command = ['ingest.py']
        ingest.ports = ['port']
        ingest.cores = [None] * 2
        ingest.gpus = [GPURequestByName('GeForce GTX TITAN X')]
        ingest.gpus[0].compute = 0.5
        ingest.gpus[0].mem = 4096.0
        ingest.cpus = 2
        ingest.mem = 16384.0
        ingest.transitions = capture_transitions
        g.add_node(ingest, config=lambda resolver: {
            'continuum_factor': 32,
            'sd_continuum_factor': cbf_channels // 256,
            'cbf_channels': cbf_channels,
            'sd_spead_rate': 3e9    # local machine, so crank it up a bit (TODO: no longer necessarily true)
        })
        g.add_edge(ingest, bcp_spead, port='spead', config=lambda resolver, endpoint: {
            'cbf_spead': str(endpoint)})
        g.add_edge(ingest, l0_spectral, port='spead', config=lambda resolver, endpoint: {
            'l0_spectral_spead': str(endpoint)})
        g.add_edge(ingest, l0_continuum, port='spead', config=lambda resolver, endpoint: {
            'l0_continuum_spead': str(endpoint)})
        g.add_edge(ingest, timeplot, port='spead_port', config=lambda resolver, endpoint: {
            'sdisp_spead': str(endpoint)})

    if beamformer_mode == 'ptuse':
        bf_ingest = SDPLogicalTask('sdp.bf_ingest.1')
        bf_ingest.image = 'beamform'
        bf_ingest.command = ['ptuse_ingest.py']
        bf_ingest.ports = ['port']
        bf_ingest.gpus = [scheduler.GPURequest()]
        bf_ingest.gpus[0].compute = 0.4
        bf_ingest.gpus[0].mem = 4096.0
        bf_ingest.cpus = 2
        bf_ingest.mem = 16384.0
        bf_ingest.networks = [scheduler.NetworkRequest('cbf', infiniband=True)]
        bf_ingest.volumes = [scratch_vol]
        bf_ingest.container.docker.parameters = [{'key': 'ipc', 'value': 'host'}]
        bf_ingest.transitions = capture_transitions
        g.add_node(bf_ingest, config=lambda resolver: {
            'cbf_channels': cbf_channels
        })
        g.add_edge(bf_ingest, beams_spead['0x'], port='spead', config=lambda resolver, endpoint: {
            'cbf_speadx': str(endpoint)})
        g.add_edge(bf_ingest, beams_spead['0y'], port='spead', config=lambda resolver, endpoint: {
            'cbf_speady': str(endpoint)})
    elif beamformer_mode != 'none':
        ram = beamformer_mode == 'hdf5_ram'
        for i, beam in enumerate(['0x', '0y']):
            bf_ingest = SDPLogicalTask('sdp.bf_ingest.{}'.format(i + 1))
            bf_ingest.image = 'katsdpingest'
            bf_ingest.command = ['bf_ingest.py',
                                 '--affinity={cores[disk]},{cores[network]}',
                                 '--interface={interfaces[cbf].name}']
            bf_ingest.cpus = 2
            bf_ingest.cores = ['disk', 'network']
            bf_ingest.mem = 4096.0
            bf_ingest.networks = [scheduler.NetworkRequest('cbf', infiniband=True)]
            volume_name = 'bf_ram{}' if ram else 'bf_ssd{}'
            bf_ingest.volumes = [
                scheduler.VolumeRequest(volume_name.format(i), '/data', 'RW', affinity=ram)]
            bf_ingest.ports = ['port']
            bf_ingest.transitions = capture_transitions
            g.add_node(bf_ingest, config=lambda resolver: {
                'file_base': '/data',
                'ibv': True,
                'direct_io': beamformer_mode == 'hdf5_ssd',   # Can't use O_DIRECT on tmpfs
                'stream_name': 'corr.tied-array-channelised-voltage.' + beam
            })
            g.add_edge(bf_ingest, beams_spead[beam], port='spead', config=lambda resolver, endpoint: {
                'cbf_spead': str(endpoint)})

    # calibration node
    cal = SDPLogicalTask('sdp.cal.1')
    cal.image = 'katsdpcal'
    cal.command = ['run_cal.py']
    cal.cpus = 0.2          # TODO: uses more in reality
    cal.mem = 65536.0
    cal.volumes = [data_vol]
    g.add_node(cal, config=lambda resolver: {
        'cbf_channels': cbf_channels
    })
    g.add_edge(cal, l0_spectral, port='spead', config=lambda resolver, endpoint: {
        'l0_spectral_spead': str(endpoint)})
    g.add_edge(cal, l1_spectral, port='spead', config=lambda resolver, endpoint: {
        'l1_spectral_spead': str(endpoint)})

    # filewriter node
    filewriter = SDPLogicalTask('sdp.filewriter.1')
    filewriter.image = 'katsdpfilewriter'
    filewriter.command = ['file_writer.py']
    filewriter.cpus = 0.2     # TODO: uses more in reality
    filewriter.mem = 2048.0
    filewriter.ports = ['port']
    filewriter.volumes = [data_vol]
    filewriter.transitions = capture_transitions
    g.add_node(filewriter, config=lambda resolver: {'file_base': '/var/kat/data'})
    g.add_edge(filewriter, l0_spectral, port='spead', config=lambda resolver, endpoint: {
        'l0_spectral_spead': str(endpoint)})

    # Simulator node
    if simulate:
        # create-fx-stream is passed on the command-line instead of telstate
        # for now due to SR-462.
        sim = SDPLogicalTask('sdp.sim.1')
        sim.image = 'katcbfsim'
        sim.command = ['cbfsim.py', '--create-fx-stream', 'baseline-correlation-products']  # TODO: stream name
        sim.cpus = 2
        sim.mem = 2048.0             # TODO
        sim.cores = [None, None]
        sim.gpus = [scheduler.GPURequest()]
        sim.gpus[0].compute = 0.5
        sim.gpus[0].mem = 2048.0     # TODO
        sim.ports = ['port']
        g.add_node(sim, config=lambda resolver: {
            'cbf_channels': cbf_channels
        })
        g.add_edge(sim, bcp_spead, port='spead', config=lambda resolver, endpoint: {
            'cbf_spead': str(endpoint)
        })

    for node in g:
        if node is not telstate and isinstance(node, SDPLogicalTask):
            node.command.extend([
                '--telstate', '{endpoints[sdp.telstate_telstate]}',
                '--name', node.name])
            node.physical_factory = SDPPhysicalTask
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
    match = re.match('^(?P<mode>c|bc)856M(?P<channels>4|32)k(?P<sim>(?:sim)?)$', graph_name)
    if not match:
        match = re.match('^bec856M(?P<channels>4|32)k(?P<mode>ssd|ram)(?P<sim>(?:sim)?)$', graph_name)
        if not match:
            raise ValueError('Unsupported graph ' + graph_name)
    beamformer_modes = {'c': 'none', 'bc': 'ptuse', 'ssd': 'hdf5_ssd', 'ram': 'hdf5_ram'}
    beamformer_mode = beamformer_modes[match.group('mode')]
    return dict(beamformer_mode=beamformer_mode,
                cbf_channels=int(match.group('channels')) * 1024,
                simulate=bool(match.group('sim')))
