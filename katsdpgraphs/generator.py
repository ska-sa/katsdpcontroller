from __future__ import print_function, division, absolute_import
import networkx as nx
import re
import addict
from katsdpcontroller import scheduler


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


class TelstateTask(scheduler.PhysicalTask):
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
    # TODO: refactor to avoid circular imports
    from katsdpcontroller.sdpcontroller import SDPLogicalTask, SDPPhysicalTask, State

    g = nx.MultiDiGraph(config=lambda resolver: {
        'sdp_cbf_channels': cbf_channels,
        'cal_refant': '',
        'cal_g_solint': 10,
        'cal_bp_solint': 10,
        'cal_k_solint': 10,
        'cal_k_chan_sample': 10
    })

    data_vol = addict.Dict()
    data_vol.mode = 'RW'
    data_vol.container_path = '/var/kat/data'
    data_vol.host_path = '/tmp'

    config_vol = addict.Dict()
    config_vol.mode = 'RW'
    config_vol.container_path = '/var/kat/config'
    config_vol.host_path = '/var/kat/config'

    capture_transitions = {State.INIT_WAIT: 'capture-init', State.DONE: 'capture-done'}

    # Multicast groups
    bcp_spead = LogicalMulticast('corr.baseline-correlation-products_spead')
    g.add_node(bcp_spead)
    l0_spectral = LogicalMulticast('l0_spectral')
    g.add_node(l0_spectral)
    l0_continuum = LogicalMulticast('l0_continuum')
    g.add_node(l0_continuum)
    l1_spectral = LogicalMulticast('l1_spectral')
    g.add_node(l1_spectral)

    # telstate node
    telstate = scheduler.LogicalTask('sdp.telstate')
    telstate.cpus = 0.1
    telstate.mem = 1024
    telstate.image = 'redis'
    telstate.ports = ['telstate']
    telstate.physical_factory = TelstateTask

    g.add_node(telstate)

    # cam2telstate node
    cam2telstate = SDPLogicalTask('sdp.cam2telstate.1')
    cam2telstate.image = 'katsdpingest'
    cam2telstate.command = ['cam2telstate.py']
    cam2telstate.cpus = 0.5
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
    timeplot.cpus = 2
    timeplot.mem = 16384.0       # TODO: tie in with timeplot's memory allocation logic
    timeplot.cores = [None] * 2
    timeplot.ports = ['spead_port', 'html_port', 'data_port']
    timeplot.wait_ports = ['html_port', 'data_port']
    timeplot.container.volumes = [config_vol]

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
        ingest.gpus[0].mem = 4096.0      # TODO: compute from channels and antennas
        ingest.cpus = 2
        ingest.mem = 16384.0             # TODO: compute
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
        # TODO: network interfaces

    # TODO: beamformer ingest

    # calibration node
    cal = SDPLogicalTask('sdp.cal.1')
    cal.image = 'katsdpcal'
    cal.command = ['run_cal.py']
    cal.cpus = 2          # TODO: uses more in reality
    cal.mem = 65536.0     # TODO: how much does cal need?
    cal.container.volumes = [data_vol]
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
    filewriter.cpus = 2
    filewriter.mem = 2048.0   # TODO: Too small for real system
    filewriter.ports = ['port']
    filewriter.container.volumes = [data_vol]
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
