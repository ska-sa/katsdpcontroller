from __future__ import print_function, division, absolute_import
import networkx as nx
import re

def build_physical_graph(beamformer_mode, cbf_channels, simulate, resources):
    """Generate a physical graph.

    Parameters
    ----------
    beamformer_mode : {"none", "hdf5_ram", "hdf5_ssd", "ptuse"}
        Which nodes (if any) to provide for beamformer capture
    channels : int
        Number of correlator channels
    simulate : boolean
        If true, run the CBF correlator simulator
    resources : :class:`katsdpcontroller.SDPResources`
        Resources for the graph
    """
    if beamformer_mode not in ['none', 'ptuse', 'hdf5_ssd', 'hdf5_ram']:
        raise ValueError('Unrecognised beamformer_mode ' + beamformer_mode)
    r = resources
    c_stream = 'c856M{}k_spead'.format(cbf_channels // 1024)
    telstate = '{}:{}'.format(r.get_host_ip('sdpmc'), r.get_port('redis'))

    streams = "corr.{}:visibility".format(c_stream[:-6].lower())
     # string containing a mapping from stream_name to stream_type.
     # This is temporary for AR1/1.5 and should be replaced by a
     # per stream sensor indicating type directly from the CBF
     # The .lower() is needed because CBF uses lower case in stream
     # specific sensor names, but reports stream names to CAM in mixed case.
     # The [:-6] strips off the _spead suffix that sdpcontroller.py added.
    if beamformer_mode != 'none':
        beams = ['corr.beam_0x', 'corr.beam_0y']
    else:
        beams = []
    for beam in beams:
        streams += ",{}:beamformer".format(beam)
     # we also include a reference to the fengine stream so
     # we can get n_samples_between_spectra
    streams += ",fengine_stream:fengine"

    G = nx.DiGraph()

     # top level attributes of this graph, used by all nodes
    attributes = {'sdp_cbf_channels': cbf_channels,
                  'cal_refant':'', 'cal_g_solint':10, 'cal_bp_solint':10, 'cal_k_solint':10, 'cal_k_chan_sample':10}

    G.graph.update(attributes)

     # list of nodes in the graph. Typically this includes a specification for the docker
     # image, and required parameters to be run.

    G.add_node('sdp.telstate',{'db_key':0, 'docker_image':r.get_image_path('redis'), 'docker_params':\
        {"port_bindings":{6379:r.get_port('redis')}}, 'docker_host_class':'sdpmc'})
     # launch redis node to hold telescope state for this graph

    # cam2telstate node
    G.add_node('sdp.cam2telstate.1', {
        'url': r.get_url('CAMDATA'),
        'streams': streams,
        'collapse_streams': True,
        'docker_image': r.get_image_path('katsdpingest'),
        'docker_cmd': 'cam2telstate.py',
        'docker_host_class': 'sdpmc',
        'docker_params': {
            'network': 'host'
        }
    })

    G.add_node('sdp.ingest.1',{
        'port': r.get_port('sdp_ingest_1_katcp'),
        'continuum_factor': 32,
        'sd_continuum_factor': cbf_channels // 256,
        'cbf_channels': cbf_channels,
        'sd_spead_rate': 3e9,                # local machine, so crank it up a bit
        'docker_image':r.get_image_path('katsdpingest_titanx'),
        'docker_host_class':'nvidia_gpu',
        'docker_cmd':'taskset -c 1,3,5,7 ingest.py',
        'docker_params': {
            "network":"host",
            "devices":["/dev/nvidiactl:/dev/nvidiactl",
                       "/dev/nvidia-uvm:/dev/nvidia-uvm","/dev/nvidia0:/dev/nvidia0"]
        },
        'state_transitions':{2:'capture-init',5:'capture-done'}\
    })
     # ingest node for ar1

    if beamformer_mode == 'ptuse':
        G.add_node('sdp.bf_ingest.1',{'port': r.get_port('sdp_bf_ingest_1_katcp'), 'cbf_channels': cbf_channels, \
             'docker_image':r.get_image_path("beamform"),'docker_host_class':'bf_ingest', 'docker_cmd':'ptuse_ingest.py',\
             'docker_params': {"network":"host", "binds": {"/scratch":{"bind":"/data","ro":False}},
                 "devices":["/dev/nvidiactl:/dev/nvidiactl","/dev/nvidia-uvm:/dev/nvidia-uvm","/dev/nvidia0:/dev/nvidia0"],\
                 "privileged":True, "ulimits":[{"Name":"memlock","Soft":33554432,"Hard":-1}],\
                 "ipc_mode":"host"},\
             'state_transitions':{2:'capture-init',5:'capture-done'}\
            })
    elif beamformer_mode != 'none':
        for i, beam in enumerate(beams):
            if beamformer_mode == 'hdf5_ram':
                affinity = [[4, 6], [5, 7]][i]
                interface = 'p4p1'
                file_base = '/mnt/ramdisk{}'.format(i)
                host_class = 'bf_ingest'
                devices = ['/dev/infiniband/rdma_cm', '/dev/infiniband/uverbs0', '/dev/infiniband/uverbs1']
            else:
                affinity = [[0, 1], [2, 3]][i]
                interface = 'p7p1'
                file_base = '/mnt/data{}'.format(i)
                host_class = 'ssd_pod'
                devices = ['/dev/infiniband/rdma_cm', '/dev/infiniband/uverbs0']
            cpuset = ','.join(map(str, affinity))
            G.add_node('sdp.bf_ingest.{}'.format(i+1), {
                'port': r.get_port('sdp_bf_ingest_{}_katcp'.format(i+1)),
                'file_base': '/data',
                'interface': interface,
                'ibv': True,
                'direct_io': beamformer_mode == 'hdf5_ssd',   # Can't use O_DIRECT on tmpfs
                'affinity': affinity,
                'stream_name': beam,
                'docker_image': r.get_image_path('katsdpingest'),
                'docker_host_class': host_class,
                'docker_cmd': 'taskset -c {} bf_ingest.py'.format(cpuset),
                'docker_params': {
                    "ulimits": [{"Name": "memlock", "Soft": 1024**3, "Hard": 1024**3}],
                    "devices": devices,
                    "cpuset": cpuset,
                    "network": "host",
                    "binds": {file_base: {"bind": "/data", "ro": False}}
                },
                'state_transitions': {2: 'capture-init', 5: 'capture-done'}
            })
         # beamformer ingest node for ar1


    G.add_node('sdp.filewriter.1',{'port': r.get_port('sdp_filewriter_1_katcp'),'file_base':'/var/kat/data',\
         'docker_image':r.get_image_path('katsdpfilewriter'),'docker_host_class':'generic', 'docker_cmd':'file_writer.py',\
         'docker_params': {"network":"host","volumes":"/var/kat/data",\
          "binds": {"/var/kat/data":{"bind":"/var/kat/data","ro":False}}},\
         'state_transitions':{2:'capture-init',5:'capture-done'}\
        })
     # filewriter

    G.add_node('sdp.cal.1',{'docker_image':r.get_image_path('katsdpcal'),'docker_host_class':'calib', 'docker_cmd':'run_cal.py',\
               'docker_params': {"network":"host","volumes":"/var/kat/data",\
                 "binds": {"/var/kat/data":{"bind":"/var/kat/data","ro":False}}}, 'cbf_channels': cbf_channels, \
              })
     # calibration node

    if simulate:
        # create-fx-product is passed on the command-line insteead of telstate
        # for now due to SR-462.
        G.add_node('sdp.sim.1',{'port': r.get_port('sdp_sim_1_katcp'), 'cbf_channels': cbf_channels,
             'docker_image':r.get_image_path('katcbfsim'),'docker_host_class':'nvidia_gpu', 'docker_cmd':'cbfsim.py --create-fx-product ' + r.prefix,\
             'docker_params': {"network":"host", "devices":["/dev/nvidiactl:/dev/nvidiactl",\
                              "/dev/nvidia-uvm:/dev/nvidia-uvm","/dev/nvidia0:/dev/nvidia0"]}
            })

     # simulation node

    G.add_node('sdp.timeplot.1',{
        'spead_port': r.get_port('spead_sdisp'),
        'html_port': r.get_sdisp_port_pair('timeplot')[0],
        'cbf_channels': cbf_channels,
        'capture_server': '{}:{}'.format('127.0.0.1', r.get_port('sdp_ingest_1_katcp')),
        'config_base': '/var/kat/config/.katsdpdisp',
        'data_port': r.get_sdisp_port_pair('timeplot')[1],
        'docker_image':r.get_image_path('katsdpdisp'),
        'docker_host_class':'nvidia_gpu',
        'docker_cmd':'taskset -c 0,2,4,6 time_plot.py --rts',
        'docker_params': {
            "network":"host",
            "binds": {"/var/kat/config":{"bind":"/var/kat/config","ro":False}}
        }
    })
     # timeplot

    # establish node connections

    G.add_edge('sdp.telstate','sdp.ingest.1',{'telstate': telstate})
    G.add_edge('sdp.telstate','sdp.filewriter.1',{'telstate': telstate})
    G.add_edge('sdp.telstate','sdp.cal.1',{'telstate': telstate})
    G.add_edge('sdp.telstate','sdp.cam2spead.1',{'telstate':telstate})
     # connections to the telescope state. 

    c_node = 'sdp.sim.1' if simulate else 'cbf.output.1'
    G.add_edge(c_node,'sdp.ingest.1',{'cbf_spead':'{}:{}'.format(r.get_multicast_ip(c_stream), r.get_port(c_stream))})
     # spead data from correlator to ingest node

    G.add_edge('cam.camtospead.1','sdp.cam2spead.1',{'cam_spead':r.get_multicast('CAM_spead')})
     # spead data from camtospead to ingest node. For simulation this is hardcoded, as we have no control over camtospead

    if beamformer_mode == 'ptuse':
        G.add_edge('cbf.bf_output.1','sdp.bf_ingest.1',{'cbf_speadx':r.get_multicast('beam_0x_spead'),
                                                        'cbf_speady':r.get_multicast('beam_0y_spead')})
    elif beamformer_mode != 'none':
        for i in range(2):
            stream = 'beam_0{}_spead'.format('xy'[i])
            G.add_edge('cbf.bf_output.{}'.format(i+1), 'sdp.bf_ingest.{}'.format(i+1), {
                'cbf_spead': '{}:{}'.format(r.get_multicast_ip(stream), r.get_port(stream))
            })
     # spead data from beamformer to ingest node

    G.add_edge('sdp.ingest.1','sdp.cal.1',{'l0_spectral_spead':r.get_multicast('l0_spectral_spead')})
     # ingest to cal node transfers L0 visibility data (no calibration)

    G.add_edge('sdp.ingest.1','sdp.timeplot.1',{'sdisp_spead':'127.0.0.1:{}'.format(r.get_port('spead_sdisp'))})
     # cal to file writer transfers L1 visibility data (cal tables applied)

    G.add_edge('sdp.ingest.1','sdp.filewriter.1',{'l0_spectral_spead':r.get_multicast('l0_spectral_spead')})
     # cal to file writer transfers L1 visibility data (cal tables applied)

    G.add_edge('sdp.cal.1','null',{'l1_spectral_spead':r.get_multicast('l1_spectral_spead')})
     # cal to file writer transfers L1 visibility data (cal tables applied)

    return G

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
