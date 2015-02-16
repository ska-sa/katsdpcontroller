"""Core classes for the SDP Controller.

"""

import time
import logging
import subprocess
import shlex
import docker
import importlib
import netifaces
import katsdptelstate
from katcp import DeviceServer, Sensor, Message, BlockingClient
from katcp.kattypes import request, return_reply, Str, Int, Float

SA_STATES = {0:'unconfigured',1:'idle',2:'init_wait',3:'capturing',4:'capture_complete',5:'done'}
TASK_STATES = {0:'init',1:'running',2:'killed'}

INGEST_BASE_PORT = 2040
 # base port to use for ingest processes

MAX_DATA_PRODUCTS = 255
 # the maximum possible number of simultaneously configured data products
 # pretty arbitrary, could be increased, but we need some limits

logger = logging.getLogger("katsdpcontroller.katsdpcontroller")

class SDPResources(object):
    """Helper class to allocate and track assigned IP and ports from a predefined range."""
    def __init__(self, safe_port_range=range(30000,31000), safe_multicast_cidr='225.100.100.0/24',simulate=False):
        self.safe_ports = safe_port_range
        self.safe_multicast_range = self._ip_range(safe_multicast_cidr)

        self.allocated_ports = {}
        self.allocated_mip = {}
        self.hosts = {}
        self.hosts_by_class = {}
        self._discover_hosts()

        try:
            self.allocated_ip = {'sdpmc':self.get_sdpmc_ip()}
        except KeyError:
            logge.error("Failed to retrieve IP address of SDP Master Controller. Unable to continue.")
            raise

    def get_sdpmc_ip(self):
         # returns the IP address of the SDPMC host.
        return self.hosts_by_class['sdpmc'][0].ip

    def get_host(self, host_class='sdpmc'):
         # retrieve a host of the specified class to use for launching resources
         # TODO: This should manage resource availability and round robin
         # services across multiple hosts of the same type. For now it just
         # serves the first available host of the correct type
        return self.hosts_by_class[host_class][0]

    def _discover_hosts(self):
        # TODO: Eventually this will contain the docker host autodiscovery code
        # For AR1 purposes, especially in the lab, we harcode some hosts to use
        # in our resource pool
        available_hosts = {'sdp-ingest4.kat.ac.za':\
                             {'ip':'127.0.0.1','host_class':'nvidia_gpu'},
                           'sdp-ingest5.kat.ac.za':\
                             {'ip':'127.0.0.1','host_class':'generic'}}

         # add the SDPMC as a non docker host
        sdpmc_local_ip = '127.0.0.1'
        for iface in netifaces.interfaces():
            for addr in netifaces.ifaddresses(iface).get(netifaces.AF_INET, []):
             # Skip point-to-point links (includes loopback)
                if 'peer' in addr:
                    continue
                sdpmc_local_ip = addr['addr']

        available_hosts['localhost.localdomain'] = \
                        {'ip':sdpmc_local_ip,'host_class':'sdpmc'}

        for host, param in available_hosts.iteritems():
            try:
                self.hosts[host] = SDPHost(**param)
            except docker.errors.requests.ConnectionError:
                logger.error("Host {} not added.".format(host))
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
        return self.allocated_ip.get(host_class, None)

    def _new_mip(self, host_class):
        mip = self.safe_multicast_range.pop()
        self.allocated_mip[host_class] = mip
        return mip

    def get_multicast_ip(self, host_class):
         # for the specified host class
         # return an available / assigned multicast address
        mip = self.allocated_mip.get(host_class, None)
        if mip is None: mip = self._new_mip(host_class)
        return mip

    def _new_port(self, host_class):
        port = self.safe_ports.pop()
        self.allocated_ports[host_class] = port
        return port

    def get_port(self, host_class):
         # for the specified host class
         # return an available / assigned port]
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
         # basic check to make sure the container is still running

    def establish_katcp_connection(self):
         # establish katcp connection to this node if appropriate
        if 'port' in self.data:
            self.katcp_connection = BlockingClient(self.ip, self.data['port'])
            try:
                self.katcp_connection.start(timeout=5)
                self.katcp_connection.wait_connected(timeout=5)
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
        self.nodes = {}
        self._katcp = {}
         # node keyed dict of established katcp connections

    def _launch(self, node, data):
        for edge in self.graph.in_edges_iter('sdp.ingest.1',data=True):
            data.update(edge[2])
             # update the data dict with any edge information (in and out)
        host_class = data['docker_host_class']
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
        container_id = host.launch(img)
         # launch the specified image in a new container
        if container_id is None:
            logger.error("Failed to launch image {} on host {}.".format(data['docker_image'], host.ip))
            raise docker.errors.DockerException("Failed to launch image")
        self.nodes[node] = SDPNode(node, data, host, container_id)
        logger.info("Successfully launched image {} on host {}. Container ID is {}".format(data['docker_image'], host.ip, container_id))
        return container_id

    def launch_telstate(self, additional_config={}):

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
        telstate_host = '{}:{}'.format(self.resources.get_host_ip('sdpmc'), self.resources.get_port('redis'))
        self.telstate = katsdptelstate.TelescopeState(host=telstate_host)

        self.telstate.delete('config')
         # TODO: needed for now as redis tends to save config between sessions at the moment
        self.telstate.add('config',config, immutable=True)
         # set the configuration

    def execute_graph(self):
         # traverse all nodes in the graph looking for those that
         # require docker containers to be launched.
        for (node, data) in self.graph.nodes_iter(data=True):
            if node == self.telstate_node: continue
             # make sure we don't launch the telstate node again
            if 'docker_image' in data:
                self._launch(node, data)

    def check_nodes(self):
         # check if all requested nodes are actually running
        alive = True
        for node in self.nodes:
            alive = alive and node.is_alive()
        return alive

    def establish_katcp_connections(self):
         # traverse all nodes in the graph that provide katcp connections
        for node in self.nodes:
            katcp = node.establish_katcp_connection()
            if katcp is not None: self._katcp[node] = katcp

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
    """A host compute device that is running an accessible docker engine."""
    def __init__(self, ip='127.0.0.1', docker_port=2375, host_class='no_docker'):
        logger.debug("New host object on {} with class {}".format(ip,host_class))
        self.ip = ip
        self.container_list = {}
        if host_class != 'no_docker':
            self._docker_client = docker.Client(base_url='{}:{}'.format(ip,docker_port))
             # seems to return an object regardless of connect status
            try:
                info = self._docker_client.info()
            except docker.errors.requests.ConnectionError:
                logger.error("Failed to connect to docker engine on {}:{}".format(ip,docker_port))
                raise
            for (k,v) in info.iteritems():
                setattr(self, str(k).lower(), v)
            self.update_containers()
        self.host_class = host_class

    def get_container(self, container_name):
        """Get an active container by name."""
        _container = None
        for c in self.container_list.itervalues():
            if c.names[0][1:] == container_name:
                _container = c
                break
        return _container

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

    def terminate(self, container_name):
        """Terminate the specified container name."""
        _container = get_container(container_name)

        if _container is None:
            logger.error("Invalid container name specified")
            return None
        self._docker_client.stop(_container.id)

    def launch(self, image):
        """Launch this image as a container on the host."""
         # we may need to investigate container reuse at this point

         # create a container from the specied image, using the build context provided
        try:
            _container = self._docker_client.create_container(image=image.image, command=image.cmd)
        except docker.errors.APIError, e:
            logger.error("Failed to build container ({})".format(e))
            return None

        logger.debug("Built container {}".format(_container['Id']))

         # launch
        try:
            self._docker_client.start(_container['Id'], port_bindings=image.port_bindings, devices=image.devices,\
                                  network_mode=image.network)
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
    def __init__(self, image, port_bindings=None, network=None, devices=None, volumes=None, cmd=None, image_class=None):
        self.image = image
        self.port_bindings = port_bindings
        self.network = network
        self.devices = devices
        self.volumes = volumes
        self.cmd = cmd

        #if image_class == 'sdpmc':
        #    self.port_bindings = {5000:5000}

        #if image_class == 'nvidia_gpu':
        #    self.network = 'host'
        #    self.devices = ['/dev/nvidiactl:/dev/nvidiactl','/dev/nvidia-uvm:/dev/nvidia-uvm',\
        #                    '/dev/nvidia0:/dev/nvidia0']
    def __repr__(self):
        return "Image: {}, Port Bindings: {}, Network: {}, Devices: {}, Volumes: {}".format(self.image,\
                self.port_bindings, self.network, self.devices, self.volumes)

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

class SDPDataProductBase(object):
    """SDP Data Product Base

    Represents an instance of an SDP data product. This includes ingest, an appropriate
    telescope model, and any required post-processing.

    In general each telescope data product is handled in a completely parallel fashion by the SDP.
    This class encapsulates these instances, handling control input and sensor feedback to CAM.

    ** This can be used directly as a stubbed simulator for use in standalone testing and validation. 
    It conforms to the functional interface, but does not launch tasks or generate data **
    """
    def __init__(self, data_product_id, antennas, n_channels, dump_rate, n_beams, cbf_source, cam_source, ingest_port, graph):
        self.data_product_id = data_product_id
        self.antennas = antennas
        self.n_antennas = len(antennas.split(","))
        self.n_channels = n_channels
        self.dump_rate = dump_rate
        self.n_beams = n_beams
        self.cam_source = cam_source
        self.cbf_source = cbf_source
        self._state = 0
        self.set_state(1)
        self.psb_id = 0
        self.ingest_port = ingest_port
        self.ingest_host = 'localhost'
        self.ingest_process = None
        self.ingest_katcp = None
         # TODO: Most of the above parameters are now deprecated - remove
        self.graph = graph
        if self.n_beams == 0:
           self.data_rate = (((self.n_antennas*(self.n_antennas+1))/2) * 4 * dump_rate * n_channels * 64) / 1e9
        else:
           self.data_rate = (n_beams * dump_rate * n_channels * 32) / 1e9
        logger.info("Created: {0}".format(self.__repr__()))

    def set_psb(self, psb_id):
        if self.psb_id > 0:
            return ('fail', 'An existing processing schedule block is already active. Please stop the data product before adding a new one.')
        if self._state < 2:
            return ('fail','The data product specified has not yet be inited. Please do this before init post processing.')
        self.psb_id = psb_id
        time.sleep(2) # simulation
        return ('ok','Post processing has been initialised')

    def get_psb(self, psb_id):
        if self.psb_id > 0:
            return ('ok','Post processing id %i is configured and active on this data product' % self.psb_id)
        return ('fail','No post processing block is active on this data product')

    def _set_state(self, state_id):
        if state_id == 5: state_id = 1
         # handle capture done in simulator
        self._state = state_id
        self.state = SA_STATES[self._state]
        return ('ok','')

    def deconfigure(self, force=False):
        if self._state == 1 or force:
            self._deconfigure()
            return ('ok', 'Data product has been deconfigured')
        else:
            return ('fail','Data product is not idle and thus cannot be deconfigured. Please issue capture_done first.')

    def _deconfigure(self):
        self._set_state(0)

    def set_state(self, state_id):
        # TODO: check that state change is allowed.
        if state_id == 5:
            if self._state < 2:
                return ('fail','Can only halt data_products that have been inited')

        if state_id == 2:
            if self._state != 1:
                return ('fail','Data product is currently in state %s, not %s as expected. Cannot be inited.' % (self.state,SA_STATES[1]))

        rcode, rval = self._set_state(state_id)
        if rcode == 'fail': return ('fail',rval)
        else:
            if rval == '': return ('ok','State changed to %s' % self.state)
        return ('ok', rval)

    def __repr__(self):
        return "Data product %s: %s antennas, %i channels, %.2f dump_rate ==> %.2f Gibps (State: %s, PSB ID: %i)" % (self.data_product_id, self.antennas, self.n_channels, self.dump_rate, self.data_rate, self.state, self.psb_id)

class SDPDataProduct(SDPDataProductBase):
    def __init__(self, *args, **kwargs):
        super(SDPDataProduct, self).__init__(*args, **kwargs)

    def deconfigure(self, force=False):
        if self._state == 1 or force:
            if self._state != 1:
                logger.warning("Forcing capture_done on external request.")
                self._issue_req('capture-done')
            self._deconfigure()
            return ('ok', 'Data product has been deconfigured')
        else:
            return ('fail','Data product is not idle and thus cannot be deconfigured. Please issue capture_done first.')

    def _deconfigure(self):
        if self.ingest_katcp is not None:
            self.ingest_katcp.stop()
            self.ingest_katcp.join()
        if self.ingest is not None:
            self.ingest.terminate()

    def _issue_req(self, req, node_type='ingest'):
         # issue a request against all nodes of a particular type.
         # typically usage is to issue a command such as 'capture-init'
         # to all ingest nodes. A single failure is treated as terminal.
        logger.debug("Issuing request {} to node_type {}".format(req, node_type))
        for (node, katcp) in self._katcp:
            try:
                node.index(node_type)
                 # filter out node_type(s) we don't want
                 # TODO: probably needs a regexp
                reply, informs = katcp.blocking_request(Message.request(req))
                if not reply.reply_ok():
                    retmsg = "Failed to issue req {} to node {}. {}".format(req, node, reply.arguments[-1])
                    logger.warning(retmsg)
                    return ('fail', retmsg)
                ret_args += "," + reply.arguments[-1]
        return ('ok', reply.arguments[-1])

    def _set_state(self, state_id):
        """The meat of the problem. Handles starting and stopping ingest processes and echo'ing requests."""
        rcode = 'ok'
        rval = ''
        logger.debug("Switching state to {} from state {}".format(SA_STATES[state_id],self.state))
        if state_id == 2: rcode, rval = self._issue_req('capture-init')
        if state_id == 5:
            rcode, rval = self._issue_req('capture-done')

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

        self.simulate = args[2]
        if self.simulate: logger.warning("Note: Running in simulation mode...")
        self.components = {}
         # dict of currently managed SDP components

        self.resources = SDPResources(simulate=self.simulate)
         # create a new resource pool. 

        self.data_products = {}
         # dict of currently configured SDP data_products
        self.ingest_ports = {}
        self.tasks = {}
         # dict of currently managed SDP tasks

        super(SDPControllerServer, self).__init__(*args, **kwargs)

    def setup_sensors(self):
        """Add sensors for processes."""
        self._build_state_sensor.set_value(self.build_state())
        self.add_sensor(self._build_state_sensor)
        self._api_version_sensor.set_value(self.version())
        self.add_sensor(self._api_version_sensor)
        self._device_status_sensor.set_value('ok')
        self.add_sensor(self._device_status_sensor)

          # until we know any better, failure modes are all inactive
        for s in self._fmeca_sensors.itervalues():
            s.set_value(0)
            self.add_sensor(s)

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

    def deregister_product(self,data_product_id,force=False):
        """Deregister a data product.

        This first checks to make sure the product is in an appropriate state
        (ideally idle), and then shuts down the ingest and plotting
        processes associated with it.

        Forcing skips the check on state and is basically used in an emergency."""
        dp_handle = self.data_products[data_product_id]
        rcode, rval = dp_handle.deconfigure(force=force)
        if rcode == 'fail': return (rcode, rval)
             # cleanup signal displays (if any)
        disp_id = "{0}_disp".format(data_product_id)
        if disp_id in self.tasks:
            disp_task = self.tasks.pop(disp_id)
            disp_task.halt()
        self.data_products.pop(data_product_id)
        self.ingest_ports.pop(data_product_id)
        return (rcode, rval)

    def handle_exit(self):
        """Try to shutdown as gracefully as possible when interrupted."""
        logger.warning("SDP Master Controller interrupted.")
        for data_product_id in self.data_products.keys():
            rcode, rval = self.deregister_product(data_product_id,force=True)
            logger.info("Deregistered data product {0} ({1},{2})".format(data_product_id, rcode, rval))

    @request(Str(optional=True),Str(optional=True),Int(min=1,max=65535,optional=True),Float(optional=True),Int(min=0,max=16384,optional=True),Str(optional=True),Str(optional=True),include_msg=True)
    @return_reply(Str())
    def request_data_product_configure(self, req, req_msg, data_product_id, antennas, n_channels, dump_rate, n_beams, cbf_source, cam_source):
        """Configure a SDP data product instance.

        A data product instance is comprised of a telescope state, a collection of
        containers running required SDP services, and a networking configuration 
        appropriate for the required data movement.

        On configuring a new product, several steps occur:
         * Build initial static configuration. Includes elements such as IP addresses of deployment machines, multicast subscription details, etc...
         * Launch a new Telescope State Repository (redis instance) for this product and copy in static config.
         * Launch service containers as described in the static configuration.
         * Verify all services are running and reachable.


        Inform Arguments
        ----------------
        data_product_id : string
            The ID to use for this data product.
        antennas : string
            A space seperated list of antenna names to use in this data product.
            These will be matched to the CBF output and used to pull only the specific
            data.
            If antennas == "", then this data product is de-configured. Trailing arguments can be omitted.
        n_channels : int
            Number of channels used in this data product (based on CBF config)
        dump_rate : float
            Dump rate of data product in Hz
        n_beams : int
            Number of beams in the data product (0 = Correlator output, 1+ = Beamformer)
        cbf_source : string
            A specification of the multicast/unicast sources from which to receive the CBF spead stream in the form <ip>[+<count>]:<port>
        cam_source : string
            A specification of the multicast/unicast sources from which to receive the CAM spead stream in the form <ip>[+<count>]:<port>
        
        Returns
        -------
        success : {'ok', 'fail'}
            If ok, returns the port on which the ingest process for this product is running.
        """
        if not data_product_id:
            for (data_product_id,data_product) in self.data_products.iteritems():
                req.inform(data_product_id,data_product)
            return ('ok',"%i" % len(self.data_products))

        if antennas is None:
            if data_product_id in self.data_products:
                return ('ok',"%s is currently configured: %s" % (data_product_id,repr(self.data_products[data_product_id])))
            else: return ('fail',"This data product id has no current configuration.")

        if antennas == "":
            try:
                (rcode, rval) = self.deregister_product(data_product_id)
                return (rcode, rval)
            except KeyError:
                return ('fail',"Deconfiguration of data product %s requested, but no configuration found." % data_product_id)

        if data_product_id in self.data_products:
            dp = self.data_products[data_product_id]
            if dp.antennas == antennas and dp.n_channels == n_channels and dp.dump_rate == dump_rate and dp.n_beams == n_beams and dp.cbf_source == cbf_source and dp.cam_source == cam_source:
                return ('ok',"Data product with this configuration already exists. Pass.")
            else:
                return ('fail',"A data product with this id ({0}) already exists, but has a different configuration. Please deconfigure this product or choose a new product id to continue.".format(data_product_id))

         # all good so far, lets check arguments for validity
        if not(antennas and n_channels >= 0 and dump_rate >= 0 and n_beams >= 0 and cbf_source and cam_source):
            return ('fail',"You must specify antennas, n_channels, dump_rate, n_beams and the CBF and CAM spead stream sources to configure a data product")

        try:
            (cbf_host,cbf_port) = self.cbf_source.split(":",2)
            (cam_host,cam_port) = self.cam_source.split(":",2)
        except ValueError:
            retmsg = "Failed to parse source stream specifiers ({0} / {1}), should be in the form <ip>[+<count>]:port".format(self.cbf_source,self.cam_source)
            logger.error(retmsg)
            return ('fail',retmsg)

        self.resources.set_ip('cbf_spead',cbf_host)
        self.resources.set_ip('cam_spead',cam_host)
        self.resources.set_port('cbf_spead',cbf_port)
        self.resources.set_port('cam_spead',cam_port)
         # TODO: For now we encode the cam and cbf spead specification directly into the resource object.
         # Once we have multiple ingest nodes we need to factor this out into appropriate addreses for each ingest process


         # determine graph name
        graph_name = "{}{}_logical.py".format("mkat_ar1_cbf", "sim" if self.simulate else "")
        logger.info("Launching graph {}.".format(graph))

        graph = SDPGraph(graph_name, self.resources)
         # create graph object and build physical graph from specified resources

         # determine additional configuration
        config = {'antennas':antennas, 'n_channels':n_channels, 'dump_rate':dump_rate}

        logger.debug("Launching telstate. Additional_config {}".format(config))
        graph.launch_telstate(additional_config=config)
         # launch the telescope state for this graph

        graph.execute_graph()
         # launch containers for those nodes that require them

        alive = graph.check_nodes()
         # is everything we asked for alive
        if not alive:
            ret_msg = "Some nodes in the graph failed to start. Check the error log for specific details."
            logger.error(ret_msg)
            return ('fail', ret_msg)

        try:
            graph.establish_katcp_connections()
             # connect to all nodes we need
        except RuntimeError:
            ret_msg = "Failed to establish katcp connections as needed. Check error log for details."
            logger.error(ret_msg)
            return ('fail', ret_msg)

        self.data_products[data_product_id] = SDPDataProduct(data_product_id, antennas, n_channels, dump_rate, n_beams, graph)
        self.data_product_graphs[data_product_id] = graph

        #if self.simulate: self.data_products[data_product_id] = SDPDataProductBase(data_product_id, antennas, n_channels, dump_rate, n_beams, cbf_source, cam_source, ingest_port)
        #else:
        #    self.tasks[disp_id] = SDPTask(disp_id,"time_plot.py","127.0.0.1")
        #    self.tasks[disp_id].launch()
        #    self.data_products[data_product_id] = SDPDataProduct(data_product_id, antennas, n_channels, dump_rate, n_beams, cbf_source, cam_source, ingest_port)
        #    rcode, rval = self.data_products[data_product_id].connect()
        #    if rcode == 'fail':
        #        disp_task = self.tasks.pop(disp_id)
        #        if disp_task: disp_task.halt()
        #        self.data_products.pop(data_product_id)
        #        self.ingest_ports.pop(data_product_id)
        #        return (rcode, rval)
        return ('ok',str(ingest_port))

    @request(Str())
    @return_reply(Str())
    def request_capture_init(self, req, data_product_id):
        """Request capture of the specified data product to start.

        Note: This command is used to prepare the SDP for reception of data
        as specified by the data product provided. It is necessary to call this
        command before issuing a start command to the CBF. Essentially the SDP
        will, once this command has returned 'OK', be in a wait state until
        reception of the stream control start packet.

        Inform Arguments
        ----------------
        data_product_id : string
            The id of the data product to initialise. This must have already been 
            configured via the data-product-configure command.

        Returns
        -------
        success : {'ok', 'fail'}
            Whether the system is ready to capture or not.
        """
        if data_product_id not in self.data_products:
            return ('fail','No existing data product configuration with this id found')
        sa = self.data_products[data_product_id]

        rcode, rval = sa.set_state(2)
        if rcode == 'fail': return (rcode, rval)
         # attempt to set state to init
        return ('ok','SDP ready')

    @request(Str(optional=True))
    @return_reply(Str())
    def request_capture_status(self, req, data_product_id):
        """Returns the status of the specified data product.

        Inform Arguments
        ----------------
        data_product_id : string
            The id of the data product whose state we wish to return.

        Returns
        -------
        success : {'ok', 'fail'}
        state : str
        """
        if not data_product_id:
            for (data_product_id,data_product) in self.data_products.iteritems():
                req.inform(data_product_id,data_product.state)
            return ('ok',"%i" % len(self.data_products))

        if data_product_id not in self.data_products:
            return ('fail','No existing data product configuration with this id found')
        return ('ok',self.data_products[data_product_id].state)

    @request(Str(),Int(optional=True))
    @return_reply(Str())
    def request_postproc_init(self, req, data_product_id, psb_id):
        """Returns the status of the specified data product.

        Inform Arguments
        ----------------
        data_product_id : string
            The id of the data product that will provide data to the post processor
        psb_id : integer
            The id of the post processing schedule block to retrieve
            from the observations database that containts the configuration
            to apply to the post processor.

        Returns
        -------
        success : {'ok', 'fail'}
        """
        if data_product_id not in self.data_products:
            return ('fail','No existing data product configuration with this id found')
        sa = self.data_products[data_product_id]

        if not psb_id >= 0:
            rcode, rval = sa.get_psb(psb_id)
            return (rcode, rval)

        rcode, rval = sa.set_psb(psb_id)
        return (rcode, rval)

    @request(Str())
    @return_reply(Str())
    def request_capture_done(self, req, data_product_id):
        """Halts the currently specified data product

        Inform Arguments
        ----------------
        data_product_id : string
            The id of the data product whose state we wish to halt.

        Returns
        -------
        success : {'ok', 'fail'}
        state : str
        """
        if data_product_id not in self.data_products:
            return ('fail','No existing data product configuration with this id found')
        rcode, rval = self.data_products[data_product_id].set_state(5)
        return (rcode, rval)

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



