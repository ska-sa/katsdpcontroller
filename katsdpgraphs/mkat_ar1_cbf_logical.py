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

     # list of nodes in the graph. Typically this includes a specification for the docker
     # image, and required parameters to be run.

    G.add_node('sdp.telstate',{'db_key':0, 'docker_image':'redis', 'docker_params':\
        {"port_bindings":{6379:r.get_port('redis')}}, 'docker_host_class':'sdpmc'})
     # launch redis node to hold telescope state for this graph

    G.add_node('sdp.ingest.1',{'port':2040, 'output_int_time':2, 'antennas':2, 'antenna-mask':'m0062,m0063', 'continuum_factor': 32,\
        'docker_image':r.get_image_path('katsdpingest_k40'),'docker_host_class':'nvidia_gpu', 'docker_cmd':'ingest.py',\
        'docker_params': {"network":"host", "devices":["/dev/nvidiactl:/dev/nvidiactl",\
                          "/dev/nvidia-uvm:/dev/nvidia-uvm","/dev/nvidia0:/dev/nvidia0"]}
        })
     # ingest node for ar1

    #G.add_node('cam.camtospead.1',{'docker_image':'katsdp/camtospead','docker_params':\
    #    {"port_bindings":{5100:5100}}, 'docker_host_class':'generic'})
     # launch cam to spead to provide metadata 

    #G.add_node('sdp.file_writer.1',{'output_dir':'/var/kat/data', 'docker_image':'katsdp/file_writer',\
    #    'docker_host_class':'tape_archive', 'docker_params':'{"network":"host"}'})
     # file writer to capture cal output data to tape

    # establish node connections

    G.add_edge('sdp.telstate','sdp.ingest.1',{'telstate': telstate})
    G.add_edge('sdp.telstate','sdp.file_writer.1',{'telstate': telstate})
     # connections to the telescope state. 

    G.add_edge('cbf.sim.1','sdp.ingest.1',{'cbf_spead':'{}:{}'.format(r.get_multicast_ip('cbf_spead'),r.get_port('cbf_spead'))\
               , 'input_rate':10e6})
     # spead data from simulator to ingest node

    G.add_edge('sdp.ingest.1','sdp.file_writer.1',{'l0_spead':'{}:{}'.format(r.get_multicast_ip('l0_spead'),r.get_port('l0_spead'))})
     # spead data from ingest node to file writer

    G.add_edge('cam.camtospead.1','sdp.ingest.1',{'cam_spead':'{}:{}'.format(r.get_multicast_ip('cam_spead'),r.get_port('cam_spead'))})
     # spead metadata from cam to ingest

    return G
