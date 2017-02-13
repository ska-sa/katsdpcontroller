#!/usr/bin/env python
from __future__ import print_function, division, absolute_import
import logging
import argparse
import trollius
import signal
import sys
import six
import networkx
import ipaddress
from trollius import From, Return
import pymesos
import addict
from decorator import decorator
import katcp
from katcp.kattypes import request, return_reply, Str, Float
import katsdptelstate
from katsdptelstate.endpoint import Endpoint
from tornado.platform.asyncio import AsyncIOMainLoop
import katsdpcontroller
from katsdpcontroller.scheduler import (
    LogicalExternal, PhysicalExternal,
    LogicalTask, PhysicalTask,
    TaskState, Scheduler, Resolver, ImageResolver, TaskIDAllocator,
    GPURequest, InterfaceRequest,
    instantiate, RANGE_RESOURCES)
from katsdpcontroller.sdpcontroller import MulticastIPResources


logger = logging.getLogger(__name__)


class TelstateTask(PhysicalTask):
    def resolve(self, resolver, graph):
        super(TelstateTask, self).resolve(resolver, graph)
        # Add a port mapping
        self.taskinfo.container.docker.network = 'BRIDGE'
        portmap = addict.Dict()
        portmap.host_port = self.ports['telstate']
        portmap.container_port = 6379
        portmap.protocol = 'tcp'
        self.taskinfo.container.docker.port_mappings = [portmap]


class GPURequestByName(GPURequest):
    def __init__(self, name):
        super(GPURequestByName, self).__init__()
        self.name = name

    def matches(self, agent_gpu, numa_node):
        if agent_gpu.name != self.name:
            return False
        return super(GPURequestByName, self).matches(agent_gpu, numa_node)


class SDPTask(PhysicalTask):
    """Task that stores configuration in telstate"""
    def resolve(self, resolver, graph):
        super(SDPTask, self).resolve(resolver, graph)
        config = graph.node[self].get('config', lambda resolver_: {})(resolver)
        for r in RANGE_RESOURCES:
            for name, value in six.iteritems(getattr(self, r)):
                config[name] = value
        for src, trg, attr in graph.out_edges_iter(self, data=True):
            endpoint = None
            if 'port' in attr and trg.state >= TaskState.STARTING:
                port = attr['port']
                endpoint = Endpoint(trg.host, trg.ports[port])
            config.update(attr.get('config', lambda resolver_, endpoint_: {})(resolver, endpoint))
        resolver.telstate.add('config.' + self.logical_node.name, config, immutable=True)


class LogicalMulticast(LogicalExternal):
    def __init__(self, name):
        super(LogicalMulticast, self).__init__(name)
        self.physical_factory = PhysicalMulticast


class PhysicalMulticast(PhysicalExternal):
    def resolve(self, resolver, graph):
        super(PhysicalMulticast, self).resolve(resolver, graph)
        self.host = resolver.multicast.get_ip(self.logical_node.name)
        self.ports = {'spead': 7148}


def make_graph(beamformer_mode, cbf_channels, simulate):
    g = networkx.MultiDiGraph(config=lambda resolver: {
        'sdp_cbf_channels': cbf_channels,
        'cal_refant': '',
        'cal_g_solint': 10,
        'cal_bp_solint': 10,
        'cal_k_solint': 10,
        'cal_k_chan_sample': 10,
        'subarray_numeric_id': resolver.subarray_numeric_id,
        'antenna_mask': ','.join(resolver.antennas),
        #'output_int_time': resolver.calculated_int_time,
        #'sd_int_time': resolver.calculated_int_time,
        #'stream_sources': resolver.stream_sources
    })

    data_vol = addict.Dict()
    data_vol.mode = 'RW'
    data_vol.container_path = '/var/kat/data'
    data_vol.host_path = '/tmp'

    config_vol = addict.Dict()
    config_vol.mode = 'RW'
    config_vol.container_path = '/var/kat/config'
    config_vol.host_path = '/var/kat/config'

    # Multicast groups
    cbf_spead = LogicalMulticast('cbf')
    g.add_node(cbf_spead)
    l0_spectral = LogicalMulticast('l0_spectral')
    g.add_node(l0_spectral)
    l0_continuum = LogicalMulticast('l0_continuum')
    g.add_node(l0_continuum)
    l1_spectral = LogicalMulticast('l1_spectral')
    g.add_node(l1_spectral)

    # telstate node
    telstate = LogicalTask('sdp.telstate')
    telstate.cpus = 0.1
    telstate.mem = 1024
    telstate.image = 'redis'
    telstate.ports = ['telstate']
    telstate.physical_factory = TelstateTask
    g.add_node(telstate)

    # cam2telstate node
    cam2telstate = LogicalTask('sdp.cam2telstate.1')
    cam2telstate.image = 'katsdpingest'
    cam2telstate.command = ['cam2telstate.py']
    cam2telstate.cpus = 0.5
    cam2telstate.mem = 256
    g.add_node(cam2telstate, config=lambda resolver: {
        'url': resolver.urls['CAMDATA'],
        'streams': 'corr.c856M4k:visibility',
        'collapse_streams': True
    })

    # signal display node
    timeplot = LogicalTask('sdp.timeplot.1')
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
        ingest = LogicalTask('sdp.ingest.{}'.format(i))
        ingest.image = 'katsdpingest_titanx'
        ingest.command = ['ingest.py']
        ingest.ports = ['port']
        ingest.cores = [None] * 2
        ingest.gpus = [GPURequestByName('GeForce GTX TITAN X')]
        ingest.gpus[0].compute = 0.5
        ingest.gpus[0].mem = 4096.0      # TODO: compute from channels and antennas
        ingest.cpus = 2
        ingest.mem = 16384.0             # TODO: compute
        g.add_node(ingest, config=lambda resolver: {
            'continuum_factor': 32,
            'sd_continuum_factor': cbf_channels // 256,
            'cbf_channels': cbf_channels,
            'sd_spead_rate': 3e9    # local machine, so crank it up a bit (TODO: no longer necessarily true)
        })
        g.add_edge(ingest, cbf_spead, port='spead', config=lambda resolver, endpoint: {
            'cbf_spead': str(endpoint)})
        g.add_edge(ingest, l0_spectral, port='spead', config=lambda resolver, endpoint: {
            'l0_spectral_spead': str(endpoint)})
        g.add_edge(ingest, l0_continuum, port='spead', config=lambda resolver, endpoint: {
            'l0_continuum_spead': str(endpoint)})
        g.add_edge(ingest, timeplot, port='spead_port', config=lambda resolver, endpoint: {
            'sdisp_spead': str(endpoint)})
        # TODO: network interfaces

    # calibration node
    cal = LogicalTask('sdp.cal.1')
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
    filewriter = LogicalTask('sdp.filewriter.1')
    filewriter.image = 'katsdpfilewriter'
    filewriter.command = ['file_writer.py']
    filewriter.cpus = 2
    filewriter.mem = 2048.0   # TODO: Too small for real system
    filewriter.ports = ['port']
    filewriter.container.volumes = [data_vol]
    g.add_node(filewriter, config=lambda resolver: {'file_base': '/var/kat/data'})
    g.add_edge(filewriter, l0_spectral, port='spead', config=lambda resolver, endpoint: {
        'l0_spectral_spead': str(endpoint)})

    # Simulator node
    if simulate:
        # create-fx-product is passed on the command-line instead of telstate
        # for now due to SR-462.
        sim = LogicalTask('sdp.sim.1')
        sim.image = 'katcbfsim'
        sim.command = ['cbfsim.py', '--create-fx-stream', 'c856M4k']  # TODO: stream name
        sim.cpus = 2
        sim.mem = 2048.0             # TODO
        sim.cores = [None, None]
        sim.gpus = [GPURequest()]
        sim.gpus[0].compute = 0.5
        sim.gpus[0].mem = 2048.0     # TODO
        sim.ports = ['port']
        g.add_node(sim, config=lambda resolver: {
            'cbf_channels': cbf_channels
        })
        g.add_edge(sim, cbf_spead, port='spead', config=lambda resolver, endpoint: {
            'cbf_spead': str(endpoint)
        })

    for node in g:
        if node is not telstate and isinstance(node, LogicalTask):
            node.command.extend([
                '--telstate', '{endpoints[sdp.telstate_telstate]}',
                '--name', node.name])
            node.physical_factory = SDPTask
            g.add_edge(node, telstate, port='telstate', order='strong')
    return g


def get_node(graph, logical_name):
    for node in graph:
        if node.logical_node.name == logical_name:
            return node
    raise KeyError('no node with logical name {}'.format(logical_name))


@decorator
def async_request(func, *args, **kwargs):
    func = trollius.coroutine(func)
    trollius.async(func(*args, **kwargs))
    raise katcp.AsyncReply()


class Server(katcp.DeviceServer):
    VERSION_INFO = ('dummy', 0, 1)
    BUILD_INFO = ('katsdpcontroller',) + tuple(katsdpcontroller.__version__.split('.', 1)) + ('',)

    def __init__(self, framework, master, resolver, loop, *args, **kwargs):
        super(Server, self).__init__(*args, **kwargs)
        self._loop = loop
        self._scheduler = Scheduler(loop)
        self._driver = pymesos.MesosSchedulerDriver(
            self._scheduler, framework, master, use_addict=True,
            implicit_acknowledgements=False)
        self._scheduler.set_driver(self._driver)
        self._driver.start()
        self._resolver = resolver
        self._physical = {}      #: Physical graphs indexed by name

    def setup_sensors(self):
        pass

    def _to_hierarchical_dict(self, config):
        """Take a flat dict of key:values where some of the keys
        may have form x.y.z and turn these into a nested hierarchy
        of dicts."""
        d = {}
        for k, v in six.iteritems(config):
            last = d
            key_parts = k.split(".")
            for ks in key_parts[:-1]:
                last[ks] = last.get(ks, {})
                last = last[ks]
            last[key_parts[-1]] = v
        return d

    @trollius.coroutine
    def _launch(self, physical):
        telstate_node = get_node(physical, 'sdp.telstate')
        boot = [node for node in physical if not isinstance(node, PhysicalTask)]
        boot.append(telstate_node)
        yield From(self._scheduler.launch(physical, self._resolver, boot))

        telstate_endpoint = Endpoint(telstate_node.host, telstate_node.ports['telstate'])
        telstate = katsdptelstate.TelescopeState(telstate_endpoint)
        self._resolver.telstate = telstate
        config = physical.graph.get('config', lambda resolver: {})(self._resolver)
        telstate.add('config', config, immutable=True)
        yield From(self._scheduler.launch(physical, self._resolver))
        raise Return(physical)

    @request(Str(), Float(optional=True))
    @async_request
    def request_launch(self, req, name, timeout=None):
        """Launch a graph.

        Parameters
        ----------
        name : str
            Name for the physical graph
        timeout : float, optional
            Time to wait for the resources to become available
        """
        try:
            logical = make_graph('none', 4096, True)
            physical = instantiate(logical, self._loop)
            for node in physical:
                node.name += '-' + name
            if timeout is None:
                timeout = 300.0
            yield From(trollius.wait_for(self._launch(physical), timeout, loop=self._loop))
            self._physical[name] = physical
        except trollius.TimeoutError:
            req.reply('fail', 'timed out waiting for resources')
            yield From(self._scheduler.kill(physical))
        except Exception as e:
            logger.debug('launch failed', exc_info=True)
            req.reply('fail', str(e))
        else:
            req.reply('ok')

    @request(Str())
    @async_request
    def request_kill(self, req, name):
        """Destroy a running graph.

        Parameters
        ----------
        name : str
            Name of the physical graph
        """
        try:
            physical = self._physical[name]
        except KeyError:
            req.reply('fail', 'no such graph ' + name)
        else:
            try:
                yield From(self._scheduler.kill(physical))
            except Exception as e:
                logger.debug('kill failed', exc_info=True)
                req.reply('fail', str(e))
            else:
                try:
                    del self._physical[name]
                except KeyError:
                    pass  # Protects against simultaneous deletions
                req.reply('ok')

    @request(Str())
    @return_reply()
    def request_graph_status(self, req, name):
        """Print the status of tasks in a graph."""
        try:
            physical = self._physical[name]
        except KeyError:
            return ('fail', 'no such graph ' + name)
        else:
            for node in physical:
                req.inform('status', node.name, node.state)
            return ('ok',)

    @request(Str())
    @return_reply()
    def request_ports(self, req, name):
        """Print the port numbers for services in the graph."""
        try:
            physical = self._physical[name]
        except KeyError:
            return ('fail', 'no such graph ' + name)
        else:
            for node in physical:
                for name, value in six.iteritems(node.ports):
                    req.inform('port', node.name, name, node.host, value)
            return ('ok',)

    @trollius.coroutine
    def async_stop(self):
        super(Server, self).stop()
        status = yield From(self._scheduler.close())
        raise Return(status)


def main():
    logging.basicConfig(level='DEBUG')

    parser = argparse.ArgumentParser()
    parser.add_argument('master', type=str)
    parser.add_argument('-p', '--port', type=int, default=5002, metavar='N', help='katcp host port')
    parser.add_argument('-a', '--host', type=str, default='', metavar='HOST', help='katcp host address')
    args = parser.parse_args()

    framework = addict.Dict()
    framework.user = 'root'
    framework.name = 'katsdpcontroller'
    framework.checkpoint = True
    framework.principal = 'sdp-sample-framework'
    framework.role = 'sdp-sample-framework'

    image_resolver = ImageResolver('sdp-docker-registry.kat.ac.za:5000')
    task_id_allocator = TaskIDAllocator()
    resolver = Resolver(image_resolver, task_id_allocator)
    resolver.multicast = MulticastIPResources(ipaddress.ip_network(u'226.100.0.0/16'))
    resolver.urls = {'CAMDATA': 'ws://10.8.67.235/katmetadata/subarray-1/custom/websocket'}
    resolver.subarray_numeric_id = 1
    resolver.antennas = ['m001', 'm002', 'm003', 'm004']

    loop = trollius.get_event_loop()
    ioloop = AsyncIOMainLoop()
    ioloop.install()
    server = Server(framework, args.master, resolver, loop, args.host, args.port)
    server.set_concurrency_options(thread_safe=False, handler_thread=False)
    server.set_ioloop(ioloop)
    ioloop.add_callback(server.start)
    finished = trollius.Future(loop=loop)

    @trollius.coroutine
    def shutdown():
        logging.info('Signal receiving, shutting down')
        loop.remove_signal_handler(signal.SIGINT)
        status = yield From(server.async_stop())
        ioloop.stop()
        loop.stop()
        finished.set_result(status)

    loop.add_signal_handler(signal.SIGINT, lambda: trollius.async(shutdown()))
    status = loop.run_until_complete(finished)
    if status != 'DRIVER_STOPPED':
        sys.exit(1)


if __name__ == '__main__':
    main()
