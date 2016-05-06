import networkx as nx

def define_hosts():
     # in the future this will be discovered at master controller boot time
     # by some form of zeroconf discovery.
    available_hosts = {'sdp-ingest1.mkat':\
                          {'ip':'127.0.0.1','host_class':'nvidia_gpu'},
                       'sdp-spdmc.mkat':\
                          {'ip':'127.0.0.1','host_class':'sdpmc'}}
    return available_hosts

def build_physical_graph(r):

    telstate = '{}:{}'.format(r.get_host_ip('sdpmc'), r.get_port('redis'))

    G = nx.DiGraph()

     # top level attributes of this graph, used by all nodes
    attributes = {'sdp_cbf_channels': 4096,
                  'cal_refant':'', 'cal_g_solint':10, 'cal_bp_solint':10, 'cal_k_solint':10, 'cal_k_chan_sample':10}

    G.graph.update(attributes)

     # list of nodes in the graph. Typically this includes a specification for the docker
     # image, and required parameters to be run.

    G.add_node('sdp.telstate',{'db_key':0, 'docker_image':r.get_image_path('redis'), 'docker_params':\
        {"port_bindings":{6379:r.get_port('redis')}}, 'docker_host_class':'sdpmc'})
     # launch redis node to hold telescope state for this graph

    G.add_node('sdp.ingest.1',{'port': r.get_port('sdp_ingest_1_katcp'), 'sd_int_time':2.0, 'output_int_time':2.0, 'antennas':4, 'continuum_factor': 32, 'cbf_channels': 4096,\
         'docker_image':r.get_image_path('katsdpingest_k40'),'docker_host_class':'nvidia_gpu', 'docker_cmd':'ingest.py',\
         'docker_params': {"network":"host", "devices":["/dev/nvidiactl:/dev/nvidiactl",\
                          "/dev/nvidia-uvm:/dev/nvidia-uvm","/dev/nvidia0:/dev/nvidia0"]},\
         'state_transitions':{2:'capture-init',5:'capture-done'}\
        })
     # ingest node for ar1

    for i in range(2):
        G.add_node('sdp.bf_ingest.{}'.format(i+1),{'port': r.get_port('sdp_bf_ingest_{}_katcp'.format(i+1)), 'file_base':'/data', 'cbf_channels': 4096, 'affinity': [i+2, i+4, i+6], 'buffer': True,\
             'docker_image':r.get_image_path('katsdpingest'),'docker_host_class':'bf_ingest', 'docker_cmd':'bf_ingest.py',\
             'docker_params': {"network":"host", "binds": {"/var/kat/data/pol{}".format(i):{"bind":"/data","ro":False}}},\
             'state_transitions':{2:'capture-init',5:'capture-done'}\
            })
         # beamformer ingest node for ar1

    G.add_node('sdp.filewriter.1',{'port': r.get_port('sdp_filewriter_1_katcp'),'file_base':'/var/kat/data',\
         'docker_image':r.get_image_path('katsdpfilewriter'),'docker_host_class':'generic', 'docker_cmd':'file_writer.py',\
         'docker_params': {"network":"host","volumes":"/var/kat/data",\
          "binds": {"/var/kat/data":{"bind":"/var/kat/data","ro":False}}},\
         'state_transitions':{2:'capture-init',5:'capture-done'}\
        })
     # filewriter

    G.add_node('sdp.cal.1',{'docker_image':r.get_image_path('katsdpcal'),'docker_host_class':'nvidia_gpu', 'docker_cmd':'run_cal.py',\
               'docker_params': {"network":"host","volumes":"/var/kat/data",\
                 "binds": {"/var/kat/data":{"bind":"/var/kat/data","ro":False}}}, 'cbf_channels': 4096, \
              })
     # calibration node

    G.add_node('sdp.timeplot.1',{'spead_port': r.get_port('spead_sdisp'), 'html_port': r.get_sdisp_port_pair('timeplot')[0],\
         'capture_server': '{}:{}'.format('127.0.0.1', r.get_port('sdp_ingest_1_katcp')),\
         'config_base': '/var/kat/config/.katsdpdisp','data_port': r.get_sdisp_port_pair('timeplot')[1],\
         'docker_image':r.get_image_path('katsdpdisp'), 'docker_host_class':'nvidia_gpu', 'docker_cmd':'time_plot.py --rts',\
         'docker_params': {"network":"host", "binds": {"/var/kat/config":{"bind":"/var/kat/config","ro":False}}}})
     # timeplot

    # establish node connections

    G.add_edge('sdp.telstate','sdp.ingest.1',{'telstate': telstate})
    G.add_edge('sdp.telstate','sdp.filewriter.1',{'telstate': telstate})
    G.add_edge('sdp.telstate','sdp.cal.1',{'telstate': telstate})
     # connections to the telescope state. 

    G.add_edge('cbf.output.1','sdp.ingest.1',{'cbf_spead':'{}:{}'.format(r.get_multicast_ip('c856M4k_spead'), r.get_port('c856M4k_spead'))})
     # spead data from correlator to ingest node

    G.add_edge('cam.camtospead.1','sdp.ingest.1',{'cam_spead':'{}:{}'.format(r.get_multicast_ip('CAM_spead'),r.get_port('CAM_spead'))})
     # spead data from camtospead to ingest node. For simulation this is hardcoded, as we have no control over camtospead

    G.add_edge('cbf.bf_output.1','sdp.bf_ingest.1',{'cbf_spead':'{}:{}'.format(r.get_multicast_ip('beam_0x_spead'),r.get_port('beam_0x_spead'))})
     # spead data from beamformer to ingest node

    G.add_edge('cbf.bf_output.1','sdp.bf_ingest.2',{'cbf_spead':'{}:{}'.format(r.get_multicast_ip('beam_0y_spead'),r.get_port('beam_0y_spead'))})
     # spead data from beamformer to ingest node

    G.add_edge('sdp.ingest.1','sdp.cal.1',{'l0_spectral_spead':'{}:{}'.format(r.get_multicast_ip('l0_spectral_spead'), r.get_port('l0_spectral_spead'))})
     # ingest to cal node transfers L0 visibility data (no calibration)

    G.add_edge('sdp.ingest.1','sdp.timeplot.1',{'sdisp_spead':'127.0.0.1:{}'.format(r.get_port('spead_sdisp'))})
     # cal to file writer transfers L1 visibility data (cal tables applied)

    G.add_edge('sdp.ingest.1','sdp.filewriter.1',{'l0_spectral_spead':'{}:{}'.format(r.get_multicast_ip('l0_spectral_spead'),r.get_port('l0_spectral_spead'))})
     # cal to file writer transfers L1 visibility data (cal tables applied)

    G.add_edge('sdp.cal.1','null',{'l1_spectral_spead':'{}:{}'.format(r.get_multicast_ip('l1_spectral_spead'),r.get_port('l1_spectral_spead'))})
     # cal to file writer transfers L1 visibility data (cal tables applied)

    return G
