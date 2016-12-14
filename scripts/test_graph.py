#!/usr/bin/env python
from __future__ import print_function, division, absolute_import
import logging
import argparse
import trollius
import signal
import sys
import six
import networkx
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
    instantiate, RANGE_RESOURCES)


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


class SDPTask(PhysicalTask):
    """Task that stores configuration in telstate"""
    def resolve(self, resolver, graph):
        super(SDPTask, self).resolve(resolver, graph)
        config = {}
        fconfig = {}   # Convert processed through subst_args
        fconfig.update(graph.node[self].get('config', {}))
        for r in RANGE_RESOURCES:
            for name, value in six.iteritems(getattr(self, r)):
                config[name] = value
        for src, trg, attr in graph.out_edges_iter(self, data=True):
            fconfig.update(attr.get('config', {}))
            if 'port' in attr and trg.state >= TaskState.STARTED:
                port = attr['port']
                config[port] = trg.host + ':' + str(trg.ports[port])
        for src, trg, attr in graph.in_edges_iter(self, data=True):
            fconfig.update(attr.get('config', {}))
        subst = self.subst_args()
        for key, value in six.iteritems(fconfig):
            config[key] = value.format(**subst)
        resolver.telstate.add('config.' + self.logical_node.name, config, immutable=True)


class Multicast(PhysicalExternal):
    def resolve(self, resolver, graph):
        super(Multicast, self).resolve(resolver, graph)
        # TODO: use the resolver
        self.host = '226.1.2.3'
        self.ports = {'SPEAD': 7148}


def make_graph():
    g = networkx.MultiDiGraph(config={'test_toplevel': 'hello'})

    # Multicast group example
    l0_spectral = LogicalExternal('l0_spectral')
    l0_spectral.physical_factory = Multicast
    g.add_node(l0_spectral, config={'test_node': 'world'})

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
    cam2telstate.cpus = 0.5
    cam2telstate.mem = 256
    cam2telstate.image = 'katsdpingest'
    cam2telstate.command = ['cam2telstate.py']
    g.add_node(cam2telstate)

    # filewriter node
    filewriter = LogicalTask('sdp.filewriter.1')
    filewriter.cpus = 2
    filewriter.mem = 2048   # TODO: Too small for real system
    filewriter.image = 'katsdpfilewriter'
    filewriter.command = ['file_writer.py', '--file-base', '/var/kat/data']
    filewriter.ports = ['port']
    data_vol = addict.Dict()
    data_vol.mode = 'RW'
    data_vol.container_path = '/var/kat/data'
    data_vol.host_path = '/tmp'
    filewriter.container.volumes = [data_vol]
    g.add_node(filewriter)
    g.add_edge(filewriter, l0_spectral, port='SPEAD')

    for node in g:
        if node is not telstate and isinstance(node, LogicalTask):
            node.command.extend([
                '--telstate', '{endpoints[sdp.telstate_telstate]}',
                '--name', node.name])
            node.physical_factory = SDPTask
            g.add_edge(node, telstate, port='telstate', order='strong',
                       config={'test_edge': 'batman'})

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


class Driver(pymesos.MesosSchedulerDriver):
    """Overrides :class:`pymesos.MesosSchedulerDriver` to make acknowledgements
    explicit. This is a bit fragile and should eventually be pushed to pymesos.
    """
    def on_update(self, event):
        status = event['status']
        self.sched.statusUpdate(self, self._dict_cls(status))


class Server(katcp.DeviceServer):
    VERSION_INFO = ('dummy', 0, 1)
    BUILD_INFO = ('katsdpcontroller',) + tuple(katsdpcontroller.__version__.split('.', 1)) + ('',)

    def __init__(self, framework, master, resolver, loop, *args, **kwargs):
        super(Server, self).__init__(*args, **kwargs)
        self._loop = loop
        self._scheduler = Scheduler(loop)
        self._driver = Driver(
            self._scheduler, framework, master, use_addict=True)
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
        config = physical.graph.get('config', {})
        telstate.add('config', config, immutable=True)
        self._resolver.telstate = telstate
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
            logical = make_graph()
            physical = instantiate(logical, self._loop)
            for node in physical:
                node.name += '-' + name
            if timeout is None:
                timeout = 30.0
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
