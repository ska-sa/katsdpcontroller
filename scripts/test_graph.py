#!/usr/bin/env python
from __future__ import print_function, division, absolute_import
import logging
import argparse
import trollius
import signal
from trollius import From, Return
from mesos.interface import mesos_pb2
import mesos.scheduler
import katcp
from decorator import decorator
from katcp.kattypes import request, return_reply, Str, Float
import katsdpcontroller
from katsdpcontroller.scheduler import *
import tornado.gen
from tornado.platform.asyncio import AsyncIOMainLoop


logger = logging.getLogger(__name__)


class TelstateTask(PhysicalTask):
    def taskinfo(self, *args, **kwargs):
        taskinfo = super(TelstateTask, self).taskinfo(*args, **kwargs)
        # Add a port mapping
        taskinfo.container.docker.network = mesos_pb2.ContainerInfo.DockerInfo.BRIDGE
        portmap = taskinfo.container.docker.port_mappings.add()
        portmap.host_port = self.ports['redis']
        portmap.container_port = 6379
        portmap.protocol = 'tcp'
        return taskinfo


def make_graph():
    g = LogicalGraph()

    # telstate node
    telstate = LogicalTask('sdp.telstate')
    telstate.cpus = 0.1
    telstate.mem = 1024
    telstate.image = 'redis'
    telstate.ports = ['redis']
    telstate.physical_factory = TelstateTask
    g.add_node(telstate)

    # cam2telstate node
    cam2telstate = LogicalTask('sdp.cam2telstate.1')
    cam2telstate.cpus = 0.5
    cam2telstate.mem = 256
    cam2telstate.image = 'katsdpingest'
    cam2telstate.command = ['cam2telstate.py']
    #g.add_node(cam2telstate)

    # filewriter node
    filewriter = LogicalTask('sdp.filewriter.1')
    filewriter.cpus = 2
    filewriter.mem = 2048   # TODO: Too small for real system
    filewriter.image = 'katsdpfilewriter'
    filewriter.command = ['file_writer.py']
    filewriter.ports = ['katcp']
    data_vol = filewriter.container.volumes.add()
    data_vol.mode = mesos_pb2.Volume.RW
    data_vol.container_path = '/var/kat/data'
    data_vol.host_path = '/var/kat/data'
    #g.add_node(filewriter)

    return g


@decorator
def async_request(func, *args, **kwargs):
    func = trollius.coroutine(func)
    trollius.async(func(*args, **kwargs))
    raise katcp.AsyncReply()


class Server(katcp.DeviceServer):
    VERSION_INFO = ('dummy', 0, 1)
    BUILD_INFO = ('katsdpcontroller',) + tuple(katsdpcontroller.__version__.split('.', 1)) + ('',)

    def __init__(self, framework, master, image_resolver, loop, *args, **kwargs):
        super(Server, self).__init__(*args, **kwargs)
        self._loop = loop
        self._scheduler = Scheduler(image_resolver, loop)
        self._driver = mesos.scheduler.MesosSchedulerDriver(
            self._scheduler, framework, master, False)
        self._scheduler.set_driver(self._driver)
        self._driver.start()
        self._physical = {}      #: Physical graphs indexed by name

    def setup_sensors(self):
        pass

    @request(Str(), Float(optional=True))
    @async_request
    def request_launch(self, req, name, timeout=30.0):
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
            physical = yield From(trollius.wait_for(self._scheduler.launch(logical),
                                                    timeout, loop=self._loop))
            self._physical[name] = physical
        except trollius.TimeoutError:
            req.reply('fail', 'timed out waiting for resources')
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

    framework = mesos_pb2.FrameworkInfo()
    framework.user = ''      # Let Mesos work it out
    framework.name = 'SDP sample framework'
    framework.checkpoint = True
    framework.principal = 'sdp-sample-framework'

    image_resolver = ImageResolver(
        'sdp-docker-registry.kat.ac.za:5000')

    loop = trollius.get_event_loop()
    ioloop = AsyncIOMainLoop()
    ioloop.install()
    server = Server(framework, args.master, image_resolver, loop, args.host, args.port)
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
    if status != mesos_pb2.DRIVER_STOPPED:
        sys.exit(1)


if __name__ == '__main__':
    main()
