#!/usr/bin/env python
from __future__ import print_function, division, absolute_import
import logging
import argparse
import trollius
import signal
from trollius import From
from mesos.interface import mesos_pb2
import mesos.scheduler
import katsdpcontroller.scheduler


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


@trollius.coroutine
def run(scheduler):
    logical = make_graph()
    physical = yield From(scheduler.launch(logical, 'test-physical1'))
    yield From(trollius.sleep(5))
    yield From(scheduler.stop('test-physical1'))


@trollius.coroutine
def main():
    logging.basicConfig(level='DEBUG')

    parser = argparse.ArgumentParser()
    parser.add_argument('master', type=str)
    args = parser.parse_args()

    framework = mesos_pb2.FrameworkInfo()
    framework.user = ''      # Let Mesos work it out
    framework.name = 'SDP sample framework'
    framework.checkpoint = True
    framework.principal = 'sdp-sample-framework'

    loop = trollius.get_event_loop()
    scheduler = katsdpcontroller.scheduler.Scheduler(loop)
    driver = mesos.scheduler.MesosSchedulerDriver(
        scheduler, framework, args.master, False)
    scheduler.set_driver(driver)
    driver.start()

    run_task = trollius.async(run(scheduler))
    loop.add_signal_handler(signal.SIGINT, run_task.cancel)
    try:
        yield From(run_task)
    except trollius.CancelledError:
        logging.warn('Aborted by Ctrl-C')

    loop.remove_signal_handler(signal.SIGINT)
    status = yield From(scheduler.close())
    if status != mesos_pb2.DRIVER_STOPPED:
        sys.exit(1)


if __name__ == '__main__':
    trollius.get_event_loop().run_until_complete(main())
