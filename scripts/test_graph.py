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
from katcp.kattypes import request, return_reply, Str, Float
import katsdpcontroller.scheduler
import tornado.gen
from tornado.platform.asyncio import AsyncIOMainLoop


logger = logging.getLogger(__name__)


def make_graph():
    g = katsdpcontroller.scheduler.LogicalGraph()
    telstate = katsdpcontroller.scheduler.Node('sdp.telstate')
    telstate.cpus = 0.1
    telstate.mem = 1024
    telstate.image = 'redis'
    telstate.ports = 1
    portmap = telstate.container.docker.port_mappings.add()
    portmap.container_port = 6379
    portmap.protocol = 'tcp'
    g.add_node(telstate)
    return g


class Server(katcp.DeviceServer):
    VERSION_INFO = ('dummy', 0, 1)
    BUILD_INFO = ('katsdpcontroller',) + tuple(katsdpcontroller.__version__.split('.', 1)) + ('',)

    def __init__(self, framework, master, loop, *args, **kwargs):
        super(Server, self).__init__(*args, **kwargs)
        self._loop = loop
        self._scheduler = katsdpcontroller.scheduler.Scheduler(loop)
        self._driver = mesos.scheduler.MesosSchedulerDriver(
            self._scheduler, framework, master, False)
        self._scheduler.set_driver(self._driver)
        self._driver.start()

    def setup_sensors(self):
        pass

    @trollius.coroutine
    def _launch(self, req, name, timeout):
        logical = make_graph()
        try:
            yield From(trollius.wait_for(self._scheduler.launch(logical, name),
                                         timeout, loop=self._loop))
        except trollius.TimeoutError:
            req.reply('fail', 'timed out waiting for resources')
        except Exception as e:
            logger.debug('launch failed', exc_info=True)
            req.reply('fail', str(e))
        else:
            req.reply('ok')

    @request(Str(), Float(optional=True))
    def request_launch(self, req, name, timeout=30.0):
        """Launch a graph.

        Parameters
        ----------
        name : str
            Name for the physical graph
        timeout : float, optional
            Time to wait for the resources to become available
        """
        trollius.async(self._launch(req, name, timeout))
        raise katcp.AsyncReply()

    @trollius.coroutine
    def _kill(self, req, name):
        try:
            yield From(self._scheduler.kill(name))
        except Exception as e:
            logger.debug('kill failed', exc_info=True)
            req.reply('fail', str(e))
        else:
            req.reply('ok')

    @request(Str())
    def request_kill(self, req, name):
        """Destroy a running graph.

        Parameters
        ----------
        name : str
            Name of the physical graph
        """
        trollius.async(self._kill(req, name))
        raise katcp.AsyncReply()

    @trollius.coroutine
    def async_stop(self):
        super(Server, self).stop()
        status = yield From(self._scheduler.close())
        raise Return(status)


@trollius.coroutine
def run(scheduler):
    logical = make_graph()
    physical = yield From(scheduler.launch(logical, 'test-physical1'))
    yield From(trollius.sleep(5))
    yield From(scheduler.stop('test-physical1'))


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

    loop = trollius.get_event_loop()
    ioloop = AsyncIOMainLoop()
    ioloop.install()
    server = Server(framework, args.master, loop, args.host, args.port)
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
