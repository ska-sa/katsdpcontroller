"""Core classes for the SDP Controller.

"""

import os
import time
import logging
import subprocess
import shlex
import importlib
import json
import signal
import socket

import netifaces
import faulthandler

from katcp import DeviceServer, Sensor, Message, BlockingClient
from katcp.kattypes import request, return_reply, Str, Int, Float

try:
    import docker
except ImportError:
    docker = None
     # docker is not needed when running in interface only mode.
     # when not running in this mode we check to make sure that
     # docker has been imported or we exit.

try:
    import katsdptelstate
    import redis
except ImportError:
    katsdptelstate = None
     # as above - katsdptelstate not needed in interface only mode

faulthandler.register(signal.SIGUSR2, all_threads=True)

SA_STATES = {0:'unconfigured',1:'idle',2:'init_wait',3:'capturing',4:'capture_complete',5:'done'}
TASK_STATES = {0:'init',1:'running',2:'killed'}

INGEST_BASE_PORT = 2040
 # base port to use for ingest processes

logger = logging.getLogger("katsdpcontroller.katsdpcontroller")

class CallbackSensor(Sensor):
    """KATCP Sensor that uses a callback to obtain the next sensor value."""
    def __init__(self, *args, **kwargs):
        self._read_callback = None
        self._busy_updating = False
        super(CallbackSensor, self).__init__(*args, **kwargs)

    def set_read_callback(self, callback=None):
        self._read_callback = callback

    def read(self):
        """Provide a callback function that is executed when read is called.

        Returns
        -------
        reading : :class:`katcp.core.Reading` object
            Sensor reading as a (timestamp, status, value) tuple

        """
        if self._read_callback and not self._busy_updating:
            self._busy_updating = True
            self.set_value(self._read_callback())
        # Having this outside the above if-statement ensures that we cannot get
        # stuck with busy_updating=True if read callback crashes in one thread
        self._busy_updating = False
        return self._current_reading

class SDPResources(object):
    """Helper class to allocate and track assigned IP and ports from a predefined range."""
    def __init__(self, safe_port_range=range(30000,31000), sdisp_port_range=range(8100,7999,-1), safe_multicast_cidr='225.100.100.0/24',local_resources=False, image_tag='latest', interface_mode=False):
        self.local_resources = local_resources
        self.image_tag = image_tag
        self.safe_ports = safe_port_range
        self.sdisp_ports = sdisp_port_range
        self.safe_multicast_range = self._ip_range(safe_multicast_cidr)
        self.private_registry = None
        self.allocated_ports = {}
        self.allocated_sdisp_ports = {}
        self.allocated_mip = {}
        self.hosts = {}
        self.hosts_by_class = {}
        self.interface_mode = interface_mode
        self.prefix = None
        if not self.interface_mode:
            self._discover_hosts()
            try:
                self.allocated_ip = {'sdpmc':self.get_sdpmc_ip()}
            except KeyError:
                logger.error("Failed to retrieve IP address of SDP Master Controller. Unable to continue.")
                raise
        else:
            self.allocated_ip = {'sdpmc':'127.0.0.1'}

    def get_sdpmc_ip(self):
         # returns the IP address of the SDPMC host.
        return self.hosts_by_class['sdpmc'][0].ip

    def get_host(self, host_class='sdpmc'):
         # retrieve a host of the specified class to use for launching resources
         # TODO: This should manage resource availability and round robin
         # services across multiple hosts of the same type. For now it just
         # serves the first available host of the correct type
        return self.hosts_by_class[host_class][0]

    def get_image_path(self, image):
         # return a fully qualified docker images path from the base
         # docker image name. Uses private registry if currently defined
        return "{}/{}:{}".format(self.private_registry if self.private_registry is not None else 'sdp',image,self.image_tag)

    def _discover_hosts(self):
        # TODO: Eventually this will contain the docker host autodiscovery code
        # For AR1 purposes, especially in the lab, we harcode some hosts to use
        # in our resource pool
        available_hosts = {'ingest1':\
                               {'ip':'ingest1.local','host_class':'nvidia_gpu'},
                           'mc1':\
                               {'ip':'mc1.local', 'host_class':'generic'}}
        self.private_registry = 'sdp-ingest5.kat.ac.za:5000'

         # add the SDPMC as a non docker host
        sdpmc_local_ip = '127.0.0.1'
        for iface in netifaces.interfaces():
            if not iface.startswith('e'): continue # Fix to handle strange docker interface enumeration
            for addr in netifaces.ifaddresses(iface).get(netifaces.AF_INET, []):
             # Skip point-to-point links (includes loopback)
                if 'peer' in addr:
                    continue
                sdpmc_local_ip = addr['addr']

        available_hosts['localhost.localdomain'] = \
                        {'ip':sdpmc_local_ip,'host_class':'sdpmc'}

        if self.local_resources:
            for param in available_hosts.itervalues():
                param['ip'] = sdpmc_local_ip
                param['docker_engine_url'] = 'unix:///var/run/docker.sock'
                 # no network connection for local simulation
        else:
            docker_port = 2376
            for param in available_hosts.itervalues():
                param['docker_engine_url'] = 'https://{}:{}'.format(param['ip'], docker_port)
                 # construct Docker URLs from IP addresses

        for host, param in available_hosts.iteritems():
            try:
                self.hosts[host] = SDPHost(private_registry=self.private_registry, **param)
            except docker.errors.requests.ConnectionError:
                logger.error("Host {} ({}) not added.".format(host, param['docker_engine_url']))
                continue
            self.hosts_by_class[param['host_class']] = self.hosts_by_class.get(param['host_class'],[])
            self.hosts_by_class[param['host_class']].append(self.hosts[host])

    def _ip_range(self, ip_cidr):
        (start_ip, cidr) = ip_cidr.split('/')
        start = list(map(int,start_ip.split('.')))
        iprange=[]
        for x in range(2**(32-int(cidr))):
            for i in range(len(start)-1,-1,-1):
                if start[i]<255:
                    start[i]+=1
                    break
                else:
                    start[i]=0
            iprange.append('.'.join(map(str,start)))
        return iprange

    def get_host_ip(self, host_class):
         # for the specified host class
         # return an available / assigned ip
        #if self.prefix: host_class = "{}_{}".format(self.prefix, host_class)
        return self.allocated_ip.get(host_class, None)

    def get_sdisp_port_pair(self, host_class):
         # we want to handle signal displays differently
         # to avoid fully dynamic port assignment
         # this returns a pair of (html_port, data_port)
        if self.prefix: host_class = "{}_{}".format(self.prefix, host_class)
	(html_port, data_port) = self.allocated_sdisp_ports.get(host_class, (None,None))
        if html_port is None:
            html_port = self.sdisp_ports.pop()
            data_port = self.sdisp_ports.pop()
            self.allocated_sdisp_ports[host_class] = (html_port, data_port)
        return (html_port, data_port)

    def _new_mip(self, host_class):
        mip = self.safe_multicast_range.pop()
        self.allocated_mip[host_class] = mip
        return mip

    def set_multicast_ip(self, host_class, ip):
         # override system generated mip with specified one
        if self.prefix: host_class = "{}_{}".format(self.prefix, host_class)
        self.allocated_mip[host_class] = ip

    def get_multicast_ip(self, host_class):
         # for the specified host class
         # return an available / assigned multicast address
        if self.prefix: host_class = "{}_{}".format(self.prefix, host_class)
        mip = self.allocated_mip.get(host_class, None)
        if mip is None: mip = self._new_mip(host_class)
        return mip

    def _new_port(self, host_class):
        port = self.safe_ports.pop()
        self.allocated_ports[host_class] = port
        return port

    def set_port(self, host_class, port):
         # override system generated port with the specified one
        if self.prefix: host_class = "{}_{}".format(self.prefix, host_class)
        self.allocated_ports[host_class] = port

    def get_port(self, host_class):
         # for the specified host class
         # return an available / assigned port]
        if self.prefix: host_class = "{}_{}".format(self.prefix, host_class)
        port = self.allocated_ports.get(host_class, None)
        if port is None: port = self._new_port(host_class)
        return port

class SDPNode(object):
    """Lightweight container for various node related objects."""
    def __init__(self, name, data, host, container_id):
        self.name = name
        self.host = host
        self.ip = host.ip
        self.container_id = container_id
        self.katcp_connection = None
        self.data = data

    def is_alive(self):
         # is this node alive
         # TODO: basic check to make sure the container is still running
        return True

    def get_transition(self, state):
         # if this node has a specified state transition action return it
        return self.data.get('state_transitions',{}).get(state,None)

    def shutdown(self):
         # shutdown this node
        if self.katcp_connection is not None:
            try:
                self.katcp_connection.stop()
                self.katcp_connection.join()
            except RuntimeError:
                pass # best effort
        self.host.stop_container(self.container_id)

    def establish_katcp_connection(self):
         # establish katcp connection to this node if appropriate
        if 'port' in self.data:
            logger.info("Attempting to establish katcp connection to {}:{} for node {}".format(self.ip, self.data['port'], self.name))
            self.katcp_connection = BlockingClient(self.ip, self.data['port'])
            try:
                self.katcp_connection.start(timeout=20)
                self.katcp_connection.wait_connected(timeout=20)
                 # some katcp connections, particularly to ingest can take a while to establish
                return self.katcp_connection
            except RuntimeError:
                self.katcp_connection.stop()
                 # no need for these to lurk around
                retmsg = "Failed to connect to ingest process via katcp on host {0} and port {1}. Check to see if networking issues could be to blame.".format(self.ip, self.data['port'])
                logger.error(retmsg)
                raise
        else:
            return None

class SDPGraph(object):
    """Wrapper around a physical graph used to instantiate
    a particular SDP product/capability/subarray."""
    def __init__(self, graph_name, resources, telstate_node='sdp.telstate'):
        self.telstate_node = telstate_node
        self.resources = resources
        gp = importlib.import_module(graph_name)
        self.graph = gp.build_physical_graph(resources)
        self.telstate = None
        self.telstate_endpoint = ""
        self.nodes = {}
        self.node_details = {}
        self._katcp = {}
         # node keyed dict of established katcp connections

    def _launch(self, node, data):
        for edge in self.graph.in_edges_iter('sdp.ingest.1',data=True):
            data.update(edge[2])
             # update the data dict with any edge information (in and out)
        try:
            host_class = data['docker_host_class']
        except KeyError:
            logger.error("Failed to launch node {} as no docker_host_class is specified.".format(node))
            return 0
        cmd = None
        if data.has_key('docker_cmd'):
            cmd = "{} --telstate {} --name {}".format(data['docker_cmd'], data['telstate'], node)
         # cmd always includes telstate connection and name of process
        img = SDPImage(data['docker_image'], cmd=cmd, image_class=host_class, **data.get('docker_params',{}))
         # prepare a docker image and pass through any override parameters specified
        logger.info("Preparing to launch image {} on node class {}".format(img, host_class))
        try:
            host = self.resources.get_host(host_class)
        except KeyError:
            logger.error("Tried to launch a container on host_class {}, but no hosts of this type are available in the resource pool.".format(host_class))
            return 0
        container = host.launch(img)
         # launch the specified image in a new container
        if container is None:
            ret_msg = "Failed to launch image {} on host {}.".format(data['docker_image'], host.ip)
            logger.error(ret_msg)
            raise docker.errors.DockerException(ret_msg)
        else:
            self.node_details[container.names[0][1:]] = host.get_container_details(container.id)
        self.nodes[node] = SDPNode(node, data, host, container.id)
        logger.info("Successfully launched image {} on host {}. Container ID is {}".format(data['docker_image'], host.ip, container.id))
        return container.id

    def get_json(self):
        from networkx.readwrite import json_graph
        return json_graph.node_link_data(self.graph)

    def launch_telstate(self, additional_config={}, base_params={}):

         # make sure the telstate node is launched
        data = self.graph.node.pop(self.telstate_node)
        telstate_cid = self._launch(self.telstate_node, data)

         # encode metadata into the telescope state for use
         # in component configuration
        config = {}
        for (node, data) in self.graph.nodes_iter(data=True):
            config[node] = data
             # place node config items into config
            edges = self.graph.in_edges(node,data=True)
            edges += self.graph.out_edges(node,data=True)
            for edge in edges:
                try:
                    config[node].update(edge[2])
                        # place config items from in and out edges to this node into config
                except IndexError:
                     # must be an edgeless node
                    continue

        config.update(additional_config)

         # connect to telstate store
        self.telstate_endpoint = '{}:{}'.format(self.resources.get_host_ip('sdpmc'), self.resources.get_port('redis'))
        try:
            self.telstate = katsdptelstate.TelescopeState(endpoint=self.telstate_endpoint)
        except redis.ConnectionError:
            logger.warning("redis backend not reachable. Waiting 1s for it to settle and trying again")
            time.sleep(1.0)
             # we may need some time for the redis container to launch
            self.telstate = katsdptelstate.TelescopeState(endpoint=self.telstate_endpoint)
             # if it fails again we have a deeper malaise

        self.telstate.delete('config')
         # TODO: needed for now as redis tends to save config between sessions at the moment
        h_config = self.to_hierarchical_dict(config)
        logger.debug("CONFIG:{}".format(h_config))
        self.telstate.add('config', h_config, immutable=True)
         # set the configuration
        for k,v in base_params.iteritems():
            self.telstate.add(k,v, immutable=True)

    def to_hierarchical_dict(self, config):
         # take a flat dict of key:values where some of the keys
         # may have form x.y.z and turn these into a nested hierarchy
         # of dicts.
        d = {}
        for k,v in config.iteritems():
            last = d
            key_parts = k.split(".")
            for ks in key_parts[:-1]:
                last[ks] = last.get(ks,{})
                last = last[ks]
            last[key_parts[-1]] = v
        return d

    def execute_graph(self):
         # traverse all nodes in the graph looking for those that
         # require docker containers to be launched.
        for (node, data) in self.graph.nodes_iter(data=True):
            if node == self.telstate_node: continue
             # make sure we don't launch the telstate node again
            if 'docker_image' in data:
                self._launch(node, data)

    def shutdown(self):
        for node in self.nodes.itervalues():
            logger.info("Shutting down node {}".format(node.name))
            node.shutdown()

    def check_nodes(self):
         # check if all requested nodes are actually running
         # TODO: Health state sensors should be controlled from here
        alive = True
        return alive

    def establish_katcp_connections(self):
         # traverse all nodes in the graph that provide katcp connections
        for node in self.nodes.itervalues():
            katcp = node.establish_katcp_connection()
            if katcp is not None: self._katcp[node.name] = katcp

class SDPContainer(object):
    """Wrapper around a docker container"""
    def __init__(self, docker_client, descriptor_json):
        self._docker_client = docker_client
        self.id = None
        for (k,v) in descriptor_json.iteritems():
            setattr(self, str(k).lower(), v)
        if self.id is None:
            logger.warning("Container created without valid ID. This is likely a mistake.")

    def log(self, tail='5'):
        """Print a portion of the STDOUT/STDERR log"""
        print self._docker_client.logs(self.id, tail=tail)

    def is_alive(self):
        c_state = self._docker_client.inspect_container(self.id)
        return c_state['State']['Running']

    def diff(self):
        """Print a list of changes to the containers filesystem since launch."""
        print self._docker_client.diff(self.id)

    def __repr__(self):
        return "{}\t\t{}\t{}\t\t\t{}".format(self.names[0][1:], self.status, self.image, self.command)

class SDPHost(object):
    """A host compute device that is running an accessible docker engine.

    Parameters
    ----------
    ip : str
        Hostname or IP address for other containers to reach this container.
    docker_engine_url : str
        URL for connecting to the docker engine on this host
    host_class : str
        Class of machine
    private_registry : str, optional
        URL for the registry holding Docker images
    """
    def __init__(self, ip='127.0.0.1', docker_engine_url='https://127.0.0.1:2375', host_class='no_docker', private_registry=None):
        logger.debug("New host object on {} with class {}".format(ip,host_class))
        self.ip = ip
        self.url = docker_engine_url
        self.container_list = {}
        self.private_registry = private_registry
        self._docker_client = None
        if host_class != 'no_docker':
            user_home = os.path.expanduser("~")
            if docker_engine_url.startswith('https://'):
                tls_config = docker.tls.TLSConfig(client_cert=('{}/.docker/cert.pem'.format(user_home), '{}/.docker/key.pem'.format(user_home)), verify=False)
                self._docker_client = docker.Client(base_url=docker_engine_url, tls=tls_config)
            else:
                self._docker_client = docker.Client(base_url=docker_engine_url)
             # seems to return an object regardless of connect status
            try:
                info = self._docker_client.info()
                if self.private_registry is not None:
                    logger.debug("Docker login on host to private registry https://{}".format(self.private_registry))
                    self._docker_client.login('kat',password='kat',registry='https://{}'.format(self.private_registry))
                     # authenticate to the specified private registry
            except docker.errors.requests.ConnectionError:
                logger.error("Failed to connect to docker engine on {}".format(docker_engine_url))
                raise
            for (k,v) in info.iteritems():
                setattr(self, str(k).lower(), v)
            self.update_containers()
        self.host_class = host_class

    def shutdown(self):
        """Terminates any running containers on this host,
        and cleans up any connections still active."""
        for cid in self.container_list.iterkeys():
            logger.debug("Terminating container id {}.".format(cid))
            self._docker_client.stop(cid)
        self.container_list = {}
        if self._docker_client is not None: self._docker_client.close()

    def get_container(self, container_name):
        """Get an active container by name."""
        _container = None
        for c in self.container_list.itervalues():
            if c.names[0][1:] == container_name:
                _container = c
                break
        return _container

    def get_container_details(self, cid):
        """Return detailed inspect information for the specified container."""
        if cid in self.container_list:
            if self._docker_client is not None: return self._docker_client.inspect_container(cid)
        return None

    def update_containers(self):
         # update our container list
        containers = self._docker_client.containers()
        for descriptor_json in containers:
            self.container_list[descriptor_json['Id']] = SDPContainer(self._docker_client, descriptor_json)

    def ps(self):
        self.update_containers()
        print "NAME\t\t\tSTATUS\t\tIMAGE\t\t\tCOMMAND\n====\t\t\t======\t\t=====\t\t\t======="
        for c in self.container_list.itervalues():
            print c

    def stop_container(self, container_name):
        """Stop the specified container name."""
        logger.debug("Stopping container {} on host {}".format(container_name, self.ip))
        if container_name.rfind("_") > 0:
            _container = self.get_container(container_name)
        else:
            _container = self.container_list[container_name]

        if _container is None:
            logger.error("Invalid container name specified")
            return None
        self._docker_client.stop(_container.id)

    def launch(self, image):
        """Launch this image as a container on the host."""
         # we may need to investigate container reuse at this point

         # create a container from the specied image, using the build context provided
        try:
            logger.info("Pulling image {} to host {} - this may take some time...".format(image.image, self.ip))
            if image.image != "redis":
                 # TODO: Skip redis pull for now as it is very slow...
                for pull_result in self._docker_client.pull(image.image, insecure_registry=True, stream=True):
                    print ".",
                    #logger.debug(json.dumps(json.loads(pull_result), indent=4))
            _container = self._docker_client.create_container(image=image.image, command=image.cmd, volumes=image.volumes)
        except docker.errors.APIError, e:
            logger.error("Failed to build container ({})".format(e))
            return None

        logger.debug("Built container {}".format(_container['Id']))

         # launch
        try:
            logger.debug("Starting container {}. Port: {}, Devices: {}, Network: {}, Volumes: {}, Cmd: {}".format(_container['Id'], image.port_bindings, image.devices, image.network, image.volumes, image.cmd))
            self._docker_client.start(_container['Id'], port_bindings=image.port_bindings, devices=image.devices,\
                                  network_mode=image.network, binds=image.binds)
        except docker.errors.APIError,e:
            logger.error("Failed to launch container ({})".format(e))
            return None

         # check to see if we launched correctly
        self.update_containers()
        try:
            return self.container_list[_container['Id']]
        except KeyError:
            logger.error("Failed to launch container")
            return None

    def __repr__(self):
        try:
            t = "{}\t{}\t{}\t{}\n".format(self.name, self.operatingsystem, self.ncpu, \
                                                          self.memtotal)
        except AttributeError:
             # likely not a docker host...
            t = "{}\t{}".format(self.ip, self.host_class)
        return t

class SDPImage(object):
    """Wrapper around a docker image.
    
    In lieu of a better method of controlling the launch environment, 
    we rely on an img_class variable pulled as the description of the image.
    This sets up ports, network and device pass through.
    
    """
    def __init__(self, image, port_bindings=None, network=None, devices=None, volumes=None, cmd=None, image_class=None, binds=None):
        self.image = image
        self.port_bindings = port_bindings
        self.network = network
        self.devices = devices
        self.volumes = volumes
        self.binds = binds
        self.cmd = cmd

        #if image_class == 'sdpmc':
        #    self.port_bindings = {5000:5000}

        #if image_class == 'nvidia_gpu':
        #    self.network = 'host'
        #    self.devices = ['/dev/nvidiactl:/dev/nvidiactl','/dev/nvidia-uvm:/dev/nvidia-uvm',\
        #                    '/dev/nvidia0:/dev/nvidia0']
    def __repr__(self):
        return "Image: {}, Port Bindings: {}, Network: {}, Devices: {}, Volumes: {}, Cmd: {}".format(self.image,\
                self.port_bindings, self.network, self.devices, self.volumes, self.cmd)

class SDPArray(object):
    """SDP Array wrapper.

    Allows management of tasks / processes within the scope of the SDP, 
    particularly in the context of a subarray instance.
    
    Each Array will have a dedicated redis backed TelescopeModel which
    will hold static configuration and dynamic values (like sensor data).
    
    This is used by all components launched as part of the Array to pull
    initial configuration (such as ip and port settings).
    
    Launch management is handled by launching docker container instances
    on relevant hardware platforms.
    
    Images are looked for in the referenced registry under the repository
    name 'katsdp'.
    """
    def __init__(self, docker_engine_url='127.0.0.1:2375', docker_registry='127.0.0.1:4500', docker_hosts=['192.168.1.164']):
        self.docker_registry = docker_registry
        self.docker_engine_url = docker_engine_url
        self._docker_client = docker.Client(base_url=self.docker_engine_url)
        self.image_list = {}
        self.refresh_image_list()
        self.host_list = {}
        self.refresh_host_list(docker_hosts)
        self._containers = {}

    def refresh_host_list(self, docker_hosts):
        if docker_hosts is None:
            pass
            # todo - auto discovery - perhaps nmap or zeroconf
        else:
            for host in docker_hosts:
                _host = SDPHost(host)
                self.host_list[_host.name] = _host

    def refresh_image_list(self):
        try:
            self._image_list = self._docker_client.search('{}/{}'.format(self.docker_registry,'kat'))
            for image in self._image_list:
                (null, base_name) = image['name'].split("/")
                _image = SDPImage('{}/{}'.format(self.docker_registry,base_name),image_class=image['description'])
                self.image_list[base_name] = _image
        except docker.errors.APIError, e:
            logger.warning("Failed to retrieve image list ({})".format(e))

    def hosts(self):
        print "Name\t\tOS\t\t#CPU\tMemory\n====\t\t==\t\t====\t======"
        for h in self.host_list.itervalues():
            print h

    def images(self):
        print "Name\t\t\tDescription\n=====\t\t\t==========="
        for (k,v) in self.image_list.iteritems():
            print "{}\t{}".format(k,v)

    def launch(self, host_name, image_name):
        try:
            _host = self.host_list[host_name]
            _image = self.image_list[image_name]
        except KeyError:
            logger.error("Invalid host or image specified")
            raise KeyError
        return _host.launch(_image)

    def __repr__(self):
        retval = ""
        for host in self.host_list.itervalues():
            retval += "{}\n====================\n".format(host.name)
            host.update_containers()
            for container in host.container_list.itervalues():
                retval += "{}\n".format(container)
        return retval

class SDPTask(object):
    """SDP Task wrapper.

    Represents an executing task within the scope of the SDP.

    Eventually this management will be fairly intelligent and will
    deploy and provision tasks automatically based on the available
    resources within the SDP.

    It is expected that SDPTask will be subclassed for specific types
    of execution.

    This is a very thin wrapper for now to support RTS.
    """
    def __init__(self, task_id, task_cmd, host):
        self.task_id = task_id
        self.task_cmd = task_cmd
        self._task_cmd_array = shlex.split(task_cmd)
        self.host = host
        self._task = None
        self.state = TASK_STATES[0]
        self.start_time = None

    def launch(self):
        try:
            self._task = subprocess.Popen(self._task_cmd_array)
            self.state = TASK_STATES[1]
            self.start_time = time.time()
            logger.info("Launched task ({0}): {1}".format(self.task_id, self.task_cmd))
        except OSError, err:
            retmsg = "Failed to launch SDP task. {0}".format(err)
            logger.error(retmsg)
            return ('fail',retmsg)
        return ('ok',"New task launched successfully")

    def halt(self):
        self._task.terminate()
        self.state = TASK_STATES[2]
        return ('ok',"Task terminated successfully.")

    def uptime(self):
        if self.start_time is None: return 0
        else: return time.time() - self.start_time

    def __repr__(self):
        return "SDP Task: status => {0}, uptime => {1:.2f}, cmd => {2}".format(self.state, self.uptime(), self._task_cmd_array[0])

class SDPSubarrayProductBase(object):
    """SDP Subarray Product Base

    Represents an instance of an SDP subarray product. This includes ingest, an appropriate
    telescope model, and any required post-processing.

    In general each telescope subarray product is handled in a completely parallel fashion by the SDP.
    This class encapsulates these instances, handling control input and sensor feedback to CAM.

    ** This can be used directly as a stubbed interface for use in standalone testing and validation. 
    It conforms to the functional interface, but does not launch tasks or generate data **
    """
    def __init__(self, subarray_product_id, antennas, n_channels, dump_rate, n_beams, graph, simulate):
        self.subarray_product_id = subarray_product_id
        self.antennas = antennas
        self.n_antennas = len(antennas.split(","))
        self.n_channels = n_channels
        self.dump_rate = dump_rate
        self.n_beams = n_beams
        self._state = 0
        self.state = SA_STATES[self._state]
        self.set_state(1)
        self.psb_id = 0
         # TODO: Most of the above parameters are now deprecated - remove
        self.simulate = simulate
        self.graph = graph
        if self.n_beams == 0:
           self.data_rate = (((self.n_antennas*(self.n_antennas+1))/2) * 4 * dump_rate * n_channels * 64) / 1e9
        else:
           self.data_rate = (n_beams * dump_rate * n_channels * 32) / 1e9
        logger.info("Created: {0}".format(self.__repr__()))

    def set_psb(self, psb_id):
        if self.psb_id > 0:
            return ('fail', 'An existing processing schedule block is already active. Please stop the subarray product before adding a new one.')
        if self._state < 2:
            return ('fail','The subarray product specified has not yet be inited. Please do this before init post processing.')
        self.psb_id = psb_id
        time.sleep(2) # simulation
        return ('ok','Post processing has been initialised')

    def get_psb(self, psb_id):
        if self.psb_id > 0:
            return ('ok','Post processing id %i is configured and active on this subarray product' % self.psb_id)
        return ('fail','No post processing block is active on this subarray product')

    def _set_state(self, state_id):
        if state_id == 5: state_id = 1
         # handle capture done in simulator
        self._state = state_id
        self.state = SA_STATES[self._state]
        return ('ok','')

    def deconfigure(self, force=False):
        if self._state == 1 or force:
            self._deconfigure()
            return ('ok', 'Subarray product has been deconfigured')
        else:
            return ('fail','Subarray product is not idle and thus cannot be deconfigured. Please issue capture_done first.')

    def _deconfigure(self):
        self._set_state(0)

    def set_state(self, state_id):
        # TODO: check that state change is allowed.
        if state_id == 5:
            if self._state < 2:
                return ('fail','Can only halt subarray_products that have been inited')

        if state_id == 2:
            if self._state != 1:
                return ('fail','Subarray product is currently in state %s, not %s as expected. Cannot be inited.' % (self.state,SA_STATES[1]))

        rcode, rval = self._set_state(state_id)
        if rcode == 'fail': return ('fail',rval)
        else:
            if rval == '': return ('ok','State changed to %s' % self.state)
        return ('ok', rval)

    def __repr__(self):
        return "Subarray product %s: %s antennas, %i channels, %.2f dump_rate ==> %.2f Gibps (State: %s, PSB ID: %i)" % (self.subarray_product_id, self.antennas, self.n_channels, self.dump_rate, self.data_rate, self.state, self.psb_id)

class SDPSubarrayProduct(SDPSubarrayProductBase):
    def __init__(self, *args, **kwargs):
        super(SDPSubarrayProduct, self).__init__(*args, **kwargs)

    def deconfigure(self, force=False):
        if self._state == 1 or force:
            if self._state != 1:
                logger.warning("Forcing capture_done on external request.")
                self._issue_req('capture-done')
            self._deconfigure()
            return ('ok', 'Subarray product has been deconfigured')
        else:
            return ('fail','Subarray product is not idle and thus cannot be deconfigured. Please issue capture_done first.')

    def _deconfigure(self):
         # handle shutdown of this subarray product in as graceful a fashion as possible.
         # TODO: be graceful :)
        logger.info("Deconfiguring subarray product")
         # issue shutdown commands for individual nodes via katcp
         # then terminate katcp connection
        self._issue_req('capture-done',node_type='ingest')
        self.graph.shutdown()

    def _issue_req(self, req, args=[], node_type='ingest'):
         # issue a request against all nodes of a particular type.
         # typically usage is to issue a command such as 'capture-init'
         # to all ingest nodes. A single failure is treated as terminal.
        logger.debug("Issuing request {} to node_type {}".format(req, node_type))
        ret_args = ""
        for (node, katcp) in self.graph._katcp.iteritems():
            try:
                node.index(node_type)
                 # filter out node_type(s) we don't want
                 # TODO: probably needs a regexp
                logger.info("Issued request {} {} to node {}".format(req, args, node))
                if args == []:
                    reply, informs = katcp.blocking_request(Message.request(req))
                else:
                    reply, informs = katcp.blocking_request(Message.request(req, args))
                if not reply.reply_ok():
                    retmsg = "Failed to issue req {} to node {}. {}".format(req, node, reply.arguments[-1])
                    logger.warning(retmsg)
                    return ('fail', retmsg)
                ret_args += "," + reply.arguments[-1]
            except ValueError:
                 # node name does not match requested node_type so ignore
                continue
        if ret_args == "":
            ret_args = "Note: Req {} not issued as no nodes of type {} found.".format(req, node_type)
        return ('ok', ret_args)

    def exec_transitions(self, state):
        """Check for nodes that require action on state transitions."""
        for node in self.graph.nodes.itervalues():
             # TODO: For now this is a dumb loop through all nodes.
             # when we have more this will need to be improved
            req = node.get_transition(state)
            if req is not None:
                self._issue_req(req, node_type=node.name)
             # failure not necessarily catastrophic, follow the schwardtian approach of bumble on...

    def _set_state(self, state_id):
        """The meat of the problem. Handles starting and stopping ingest processes and echo'ing requests."""
        rcode = 'ok'
        rval = ''
        logger.info("Switching state to {} from state {}".format(SA_STATES[state_id],self.state))
        if state_id == 2:
            self.exec_transitions(2)
            if self.simulate:
                logger.info("SIMULATE: Issuing a capture-start to the simulator")
                time.sleep(2) # only needed for simulator...
                rcode, rval = self._issue_req('capture-start', args=['k7'], node_type='cbf.sim')
        if state_id == 5:
            if self.simulate:
                logger.info("SIMULATE: Issuing a capture-stop to the simulator")
                time.sleep(2) # only needed for simulator...
                rcode, rval = self._issue_req('capture-stop', args=['k7'], node_type='cbf.sim')
            self.exec_transitions(5)
             # called in an explicit fashion (above as well) so we can manage
             # execution order correctly when dealing with a simulator
             # all other commands can execute in arbitrary order

        if state_id == 5 or rcode == 'ok':
            if state_id == 5: state_id = 1
             # make sure that we dont get stuck if capture-done is failing...
            self._state = state_id
            self.state = SA_STATES[self._state]
        if rval == '': rval = "State changed to {0}".format(self.state)
        return (rcode, rval)

class SDPControllerServer(DeviceServer):

    VERSION_INFO = ("sdpcontroller", 0, 1)
    BUILD_INFO = ("sdpcontroller", 0, 1, "rc2")

    def __init__(self, *args, **kwargs):
        logging.basicConfig(level=logging.INFO)
         # setup sensors
        self._build_state_sensor = Sensor(Sensor.STRING, "build-state", "SDP Controller build state.", "")
        self._api_version_sensor = Sensor(Sensor.STRING, "api-version", "SDP Controller API version.", "")
        self._device_status_sensor = Sensor(Sensor.DISCRETE, "device-status", "Devices status of the Antenna Positioner", "", ["ok", "degraded", "fail"])
        self._fmeca_sensors = {}
        self._fmeca_sensors['FD0001'] = Sensor(Sensor.BOOLEAN, "fmeca.FD0001", "Sub-process limits", "")
         # example FMECA sensor. In this case something to keep track of issues arising from launching to many processes.
         # TODO: Add more sensors exposing resource usage and currently executing graphs
        self._ntp_sensor = CallbackSensor(Sensor.BOOLEAN, "time-synchronised","SDP Controller container (and host) is synchronised to NTP", "")

        self.simulate = kwargs['simulate']
        if self.simulate: logger.warning("Note: Running in simulation mode. This will simulate certain external components such as the CBF.")
        self.interface_mode = kwargs['interface_mode']
        if self.interface_mode: logger.warning("Note: Running master controller in interface mode. This allows testing of the interface only, no actual command logic will be enacted.")
        self.components = {}
         # dict of currently managed SDP components
        self.local_resources = kwargs.get('local_resources', False)
         # TODO: part of implementing unittest mode

        logger.debug("Building initial resource pool")
        self.resources = SDPResources(local_resources=self.local_resources, image_tag=kwargs['tag'], interface_mode=self.interface_mode)
         # create a new resource pool. 

        self.subarray_products = {}
         # dict of currently configured SDP subarray_products
        self.subarray_product_graphs = {}
         # shortcut dict to established graphs
        self.ingest_ports = {}
        self.tasks = {}
         # dict of currently managed SDP tasks

        super(SDPControllerServer, self).__init__(*args)

    def setup_sensors(self):
        """Add sensors for processes."""
        self._build_state_sensor.set_value(self.build_state())
        self.add_sensor(self._build_state_sensor)
        self._api_version_sensor.set_value(self.version())
        self.add_sensor(self._api_version_sensor)
        self._device_status_sensor.set_value('ok')
        self.add_sensor(self._device_status_sensor)

        self._ntp_sensor.set_value(False)
        self._ntp_sensor.set_read_callback(self._check_ntp_status)
        self.add_sensor(self._ntp_sensor)

          # until we know any better, failure modes are all inactive
        for s in self._fmeca_sensors.itervalues():
            s.set_value(0)
            self.add_sensor(s)

    def _check_ntp_status(self):
        try:
            return subprocess.check_output(["/usr/bin/ntpq","-p"]).find('*') > 0
        except OSError:
            return False

    def request_halt(self, req, msg):
        """Halt the device server.

        Returns
        -------
        success : {'ok', 'fail'}
            Whether scheduling the halt succeeded.

        Examples
        --------
        ::

            ?halt
            !halt ok
        """
        self.stop()
        logger.warning("Halt requested. Attempting cleanup...")
        # this message makes it through because stop
        # only registers in .run(...) after the reply
        # has been sent.
        return req.make_reply("ok")

    @request(Str())
    @return_reply(Str())
    def request_task_terminate(self, req, task_id):
        """Terminate the specified SDP task.
        
        Inform Arguments
        ----------------
        task_id : string
            The ID of the task to terminate

        Returns
        -------
        success : {'ok', 'fail'}
        """
        if not task_id in self.tasks: return ('fail',"Specified task ID ({0}) is unknown".format(task_id))
        task = self.tasks.pop(task_id)
        rcode, rval = task.halt()
        return (rcode, rval)


    @request(Str(optional=True),Str(optional=True),Str(optional=True))
    @return_reply(Str())
    def request_task_launch(self, req, task_id, task_cmd, host):
        """Launch a task within the SDP.
        This command allows tasks to be listed and launched within the SDP. Specification of a desired host
        is optional, as in general the master controller will decide on the most appropriate location on
        which to run the task.
        
        Inform Arguments
        ----------------
        task_id : string
            The unique ID used to identify this task.
            If empty then all managed tasks are listed.
        task_cmd : string
            The complete command to run including fully qualified executable and arguments
            If empty then the status of the specified id is shown
        host : string
            Force the controller to launch the task on the specified host

        Returns
        -------
        success : {'ok', 'fail'}
        host,port : If appropriate, the host/port pair to connect to the task via katcp is returned.
        """
        if not task_id:
            for (task_id, task) in self.tasks.iteritems():
                req.inform(task_id, task)
            return ('ok', "{0}".format(len(self.tasks)))

        if task_id in self.tasks:
            if not task_cmd: return ('ok',"{0}: {1}".format(task_id, self.tasks[task_id]))
            else: return ('fail',"A task with the specified ID is already running and cannot be reconfigured.")

        if task_id not in self.tasks and not task_cmd: return ('fail',"You must specify a command line to run for a new task")

        self.tasks[task_id] = SDPTask(task_id, task_cmd, host)
        rcode, rval = self.tasks[task_id].launch()
        if rcode == 'fail': self.tasks.pop(task_id)
         # launch failed, discard task
        return (rcode, rval)

    def deregister_product(self,subarray_product_id,force=False):
        """Deregister a subarray product.

        This first checks to make sure the product is in an appropriate state
        (ideally idle), and then shuts down the ingest and plotting
        processes associated with it.

        Forcing skips the check on state and is basically used in an emergency."""
        dp_handle = self.subarray_products[subarray_product_id]
        rcode, rval = dp_handle.deconfigure(force=force)
        if rcode == 'fail': return (rcode, rval)
        self.subarray_products.pop(subarray_product_id)
        return (rcode, rval)

    def handle_exit(self):
        """Try to shutdown as gracefully as possible when interrupted."""
        logger.warning("SDP Master Controller interrupted.")
        for subarray_product_id in self.subarray_products.keys():
            rcode, rval = self.deregister_product(subarray_product_id,force=True)
            logger.info("Deregistered subarray product {0} ({1},{2})".format(subarray_product_id, rcode, rval))

    @request(Str(optional=True),Str(optional=True),Int(min=1,max=65535,optional=True),Float(optional=True),Int(min=0,max=16384,optional=True),Str(optional=True),Str(optional=True),include_msg=True)
    @return_reply(Str())
    def request_data_product_configure(self, req, req_msg, subarray_product_id, antennas, n_channels, dump_rate, n_beams, cbf_source, cam_source):
        """Configure a SDP subarray product instance.

        A subarray product instance is comprised of a telescope state, a collection of
        containers running required SDP services, and a networking configuration 
        appropriate for the required data movement.

        On configuring a new product, several steps occur:
         * Build initial static configuration. Includes elements such as IP addresses of deployment machines, multicast subscription details, etc...
         * Launch a new Telescope State Repository (redis instance) for this product and copy in static config.
         * Launch service containers as described in the static configuration.
         * Verify all services are running and reachable.


        Inform Arguments
        ----------------
        subarray_product_id : string
            The ID to use for this subarray product, in the form [<subarray_name>_]<data_product_name>.
        antennas : string
            A space-separated list of antenna names to use in this subarray product.
            These will be matched to the CBF output and used to pull only the specific
            data. If antennas is "0" or "", then this subarray product is de-configured.
            Trailing arguments can be omitted.
        n_channels : int
            Number of channels used in this subarray product (based on CBF config)
        dump_rate : float
            Dump rate of subarray product in Hz
        n_beams : int
            Number of beams in the subarray product (0 = Correlator output, 1+ = Beamformer)
        cbf_source : string
            A specification of the multicast/unicast sources from which to receive the CBF spead stream in the form <ip>[+<count>]:<port>
        cam_source : string
            A specification of the multicast/unicast sources from which to receive the CAM spead stream in the form <ip>[+<count>]:<port>
        
        Returns
        -------
        success : {'ok', 'fail'}
            If ok, returns the port on which the ingest process for this product is running.
        """
        if not subarray_product_id:
            for (subarray_product_id,subarray_product) in self.subarray_products.iteritems():
                req.inform(subarray_product_id,subarray_product)
            return ('ok',"%i" % len(self.subarray_products))

        if antennas is None:
            if subarray_product_id in self.subarray_products:
                return ('ok',"%s is currently configured: %s" % (subarray_product_id,repr(self.subarray_products[subarray_product_id])))
            else: return ('fail',"This subarray product id has no current configuration.")

        if antennas == "0" or antennas == "":
            try:
                (rcode, rval) = self.deregister_product(subarray_product_id)
                return (rcode, rval)
            except KeyError:
                return ('fail',"Deconfiguration of subarray product %s requested, but no configuration found." % subarray_product_id)

        if subarray_product_id in self.subarray_products:
            dp = self.subarray_products[subarray_product_id]
            if dp.antennas == antennas and dp.n_channels == n_channels and dp.dump_rate == dump_rate and dp.n_beams == n_beams:
                return ('ok',"Subarray product with this configuration already exists. Pass.")
            else:
                return ('fail',"A subarray product with this id ({0}) already exists, but has a different configuration. Please deconfigure this product or choose a new product id to continue.".format(subarray_product_id))

         # all good so far, lets check arguments for validity
        if not(antennas and n_channels >= 0 and dump_rate >= 0 and n_beams >= 0 and cbf_source and cam_source):
            return ('fail',"You must specify antennas, n_channels, dump_rate, n_beams and the CBF and CAM spead stream sources to configure a subarray product")

        try:
            (cbf_host,cbf_port) = cbf_source.split(":",2)
            (cam_host,cam_port) = cam_source.split(":",2)
        except ValueError:
            retmsg = "Failed to parse source stream specifiers ({0} / {1}), should be in the form <ip>[+<count>]:port".format(cbf_source, cam_source)
            logger.error(retmsg)
            return ('fail',retmsg)

         # determine graph name
        name_parts = subarray_product_id.split("_")
         # expect subarray product name to be of form [<subarray_name>_]<data_product_name>
        sane_name = name_parts[-1]
        graph_name = "katsdpgraphs.{}{}_logical".format(sane_name, "sim" if self.simulate else "")
        logger.info("Launching graph {}.".format(graph_name))

	self.resources.prefix = subarray_product_id
         # use the full subarray identifier to avoid any namespace collisions
        logger.info("Setting resources prefix to {}".format(self.resources.prefix))

	self.resources.set_multicast_ip('cbf_spead',cbf_host)
        self.resources.set_multicast_ip('cam_spead',cam_host)
        self.resources.set_port('cbf_spead',cbf_port)
        self.resources.set_port('cam_spead',cam_port)
         # TODO: For now we encode the cam and cbf spead specification directly into the resource object.
         # Once we have multiple ingest nodes we need to factor this out into appropriate addreses for each ingest process

        graph = SDPGraph(graph_name, self.resources)
         # create graph object and build physical graph from specified resources

        logger.debug(graph.get_json())
         # determine additional configuration
        config = graph.graph.graph
         # it is very graphy - indeed...
         # parameters such as antennas and channels are encoded in the logical graphs
        config['subarray_product_id'] = subarray_product_id
	 # used to identify file type
        additional_config = {'antenna_mask':antennas}
         # holds additional config that must reside within the config dict in the telstate 

        if self.interface_mode:
            logger.debug("Telstate configured. Base parameters {}".format(config))
            logger.warning("No components will be started - running in interface mode")
            self.subarray_products[subarray_product_id] = SDPSubarrayProductBase(subarray_product_id, antennas, n_channels, dump_rate, n_beams, graph, self.simulate)
            self.subarray_product_graphs[subarray_product_id] = graph
            return ('ok',"")

        if docker is None:
             # from here onwards we require the docker module to be installed.
            retmsg = "You must have the docker python library installed to use the master controller in non interface only mode."
            logger.error(retmsg)
            return ('fail',retmsg)

        if katsdptelstate is None:
             # from here onwards we require the katsdptelstate module to be installed.
            retmsg = "You must have the katsdptelstate library installed to use the master controller in non interface only mode."
            logger.error(retmsg)
            return ('fail',retmsg)

        logger.debug("Launching telstate. Base parameters {}".format(config))
        graph.launch_telstate(additional_config=additional_config, base_params=config)
         # launch the telescope state for this graph

        logger.debug("Executing graph {}".format(graph_name))
        try:
            graph.execute_graph()
             # launch containers for those nodes that require them
        except docker.errors.DockerException, e:
            graph.shutdown()
            return ('fail',e)

        alive = graph.check_nodes()
         # is everything we asked for alive
        if not alive:
            ret_msg = "Some nodes in the graph failed to start. Check the error log for specific details."
            graph.shutdown()
            logger.error(ret_msg)
            return ('fail', ret_msg)

        logger.debug("Establishing katcp connections to appropriate nodes.")
        try:
            graph.establish_katcp_connections()
             # connect to all nodes we need
        except RuntimeError:
            ret_msg = "Failed to establish katcp connections as needed. Check error log for details."
            graph.shutdown()
            logger.error(ret_msg)
            return ('fail', ret_msg)

         # at this point telstate is up, nodes have been launched, katcp connections established
         # TODO: The subarray product class will need to be reworked
        self.subarray_products[subarray_product_id] = SDPSubarrayProduct(subarray_product_id, antennas, n_channels, dump_rate, n_beams, graph, self.simulate)
        self.subarray_product_graphs[subarray_product_id] = graph

         # finally we insert detail on all running nodes into telstate
        if graph.telstate is not None:
            graph.telstate.add('sdp_node_detail',graph.node_details)
        return ('ok',"")

    @request(Str())
    @return_reply(Str())
    def request_capture_init(self, req, subarray_product_id):
        """Request capture of the specified subarray product to start.

        Note: This command is used to prepare the SDP for reception of data
        as specified by the subarray product provided. It is necessary to call this
        command before issuing a start command to the CBF. Essentially the SDP
        will, once this command has returned 'OK', be in a wait state until
        reception of the stream control start packet.

        Inform Arguments
        ----------------
        subarray_product_id : string
            The id of the subarray product to initialise. This must have already been
            configured via the data-product-configure command.

        Returns
        -------
        success : {'ok', 'fail'}
            Whether the system is ready to capture or not.
        """
        if subarray_product_id not in self.subarray_products:
            return ('fail','No existing subarray product configuration with this id found')
        sa = self.subarray_products[subarray_product_id]

        rcode, rval = sa.set_state(2)
        if rcode == 'fail': return (rcode, rval)
         # attempt to set state to init
        return ('ok','SDP ready')

    @request(Str(optional=True))
    @return_reply(Str())
    def request_telstate_endpoint(self, req, subarray_product_id):
        """Returns the endpoint for the telescope state repository of the 
	specified subarray product.

        Inform Arguments
        ----------------
        subarray_product_id : string
            The id of the subarray product whose state we wish to return.

        Returns
        -------
        success : {'ok', 'fail'}
        state : str
        """
        if not subarray_product_id:
            for (subarray_product_id,subarray_product) in self.subarray_products.iteritems():
                req.inform(subarray_product_id, subarray_product.graph.telstate_endpoint)
            return ('ok',"%i" % len(self.subarray_products))

        if subarray_product_id not in self.subarray_products:
            return ('fail','No existing subarray product configuration with this id found')
        return ('ok',self.subarray_products[subarray_product_id].graph.telstate_endpoint)

    @request(Str(optional=True))
    @return_reply(Str())
    def request_capture_status(self, req, subarray_product_id):
        """Returns the status of the specified subarray product.

        Inform Arguments
        ----------------
        subarray_product_id : string
            The id of the subarray product whose state we wish to return.

        Returns
        -------
        success : {'ok', 'fail'}
        state : str
        """
        if not subarray_product_id:
            for (subarray_product_id,subarray_product) in self.subarray_products.iteritems():
                req.inform(subarray_product_id,subarray_product.state)
            return ('ok',"%i" % len(self.subarray_products))

        if subarray_product_id not in self.subarray_products:
            return ('fail','No existing subarray product configuration with this id found')
        return ('ok',self.subarray_products[subarray_product_id].state)

    @request(Str(),Int(optional=True))
    @return_reply(Str())
    def request_postproc_init(self, req, subarray_product_id, psb_id):
        """Returns the status of the specified subarray product.

        Inform Arguments
        ----------------
        subarray_product_id : string
            The id of the subarray product that will provide data to the post processor
        psb_id : integer
            The id of the post processing schedule block to retrieve
            from the observations database that containts the configuration
            to apply to the post processor.

        Returns
        -------
        success : {'ok', 'fail'}
        """
        if subarray_product_id not in self.subarray_products:
            return ('fail','No existing subarray product configuration with this id found')
        sa = self.subarray_products[subarray_product_id]

        if not psb_id >= 0:
            rcode, rval = sa.get_psb(psb_id)
            return (rcode, rval)

        rcode, rval = sa.set_psb(psb_id)
        return (rcode, rval)

    @request(Str())
    @return_reply(Str())
    def request_capture_done(self, req, subarray_product_id):
        """Halts the currently specified subarray product

        Inform Arguments
        ----------------
        subarray_product_id : string
            The id of the subarray product whose state we wish to halt.

        Returns
        -------
        success : {'ok', 'fail'}
        state : str
        """
        if subarray_product_id not in self.subarray_products:
            return ('fail','No existing subarray product configuration with this id found')
        rcode, rval = self.subarray_products[subarray_product_id].set_state(5)
        return (rcode, rval)

    @request()
    @return_reply(Str())
    def request_sdp_shutdown(self, req):
        """Shut down the SDP master controller and all controlled nodes.

        Returns
        -------
        success : {'ok', 'fail'}
            Whether the shutdown sequence of all other nodes succeeded.
        hosts : str
            Comma separated lists of hosts that have been shutdown (excl mc host)
        """
        logger.info("SDP Shutdown called.")
        for subarray_product_id in self.subarray_products.keys():
            rcode, rval = self.deregister_product(subarray_product_id,force=True)
            logger.info("Terminated and deconfigured subarray product {0} ({1},{2})".format(subarray_product_id, rcode, rval))

        self.resources.hosts.pop('localhost.localdomain')
         # remove this to prevent accidental shutdown of master controller whilst handling remotes

        kibisis = SDPImage(self.resources.get_image_path('docker-base'), cmd="/sbin/poweroff", network='host')
         # prepare a docker image to halt remote hosts

        shutdown_hosts = ""
        for (host_name, host) in self.resources.hosts.iteritems():
            if socket.gethostbyname(host.ip) != '127.0.0.1':
                 # make sure a localhost hasn't snuck in to spoil the party
                logger.debug("Launching halt image on host {}".format(host_name))
                container = host.launch(kibisis)
                if container is None: logger.error("Failed to launch shutdown container on host {}".format(host_name))
                shutdown_hosts += "{}{},".format(host_name,"" if container else "(failed)")

        logger.warning("Shutting down master controller host...")
        retval = os.system('sudo --non-interactive /sbin/poweroff')
         # finally shutdown localhost - relying on upstart to shutdown the master controller
        if retval != 0:
            retmsg = "Failed to issue /sbin/poweroff on MC host. This is most likely a sudoers permission issue."
            logger.error(retmsg)
            return ("fail", retmsg)
        return ("ok", shutdown_hosts[:shutdown_hosts.rfind(',')])


    @request(include_msg=True)
    @return_reply(Int(min=0))
    def request_sdp_status(self, req, reqmsg):
        """Request status of SDP components.

        Inform Arguments
        ----------------
        process : str
            Name of a registered process.

        Returns
        -------
        success : {'ok', 'fail'}
            Whether retrieving component status succeeded.
        informs : int
            Number of #sdp_status informs sent
        """
        for (component_name,component) in self.components:
            req.inform("%s:%s",component_name,component.status)
        return ("ok", len(self.components))



