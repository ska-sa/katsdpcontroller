#!/usr/bin/env python

"""Script for launching the Science Data Processor Master Controller.

   Copyright (c) 2013 SKA/KAT. All Rights Reserved.
"""

import sys
import os
import Queue
import signal
from optparse import OptionParser
import logging
import logging.handlers

try:
    import manhole
except ImportError:
    manhole = None

if __name__ == "__main__":

    usage = "usage: %prog [options]"
    parser = OptionParser(usage=usage)
    parser.add_option('-a', '--host', dest='host', type="string", default="", metavar='HOST',
                      help='attach to server HOST (default="%default" - localhost)')
    parser.add_option('-p', '--port', dest='port', type="int", default=5000, metavar='N',
                      help='katcp listen port (default=%default)')
    parser.add_option('-w', '--working-folder', dest='workpath',
                      default=os.path.join("/", "var", "kat", "sdpcontroller"), metavar='WORKING_PATH',
                      help='folder to write process standard out logs into (default=%default)')
    parser.add_option('-l', '--loglevel', dest='loglevel', type="string",
                      default="info", metavar='LOGLEVEL',
                      help='set the Python logging level (default=%default)')
    parser.add_option('-s', '--simulate', dest='simulate', default=False,
                      action='store_true', metavar='SIMULATE',
                      help='run controller in simulation mode suitable for lab testing (default: %default)')
    parser.add_option('-i', '--interface_mode', dest='interface_mode', default=False,
                      action='store_true', metavar='INTERFACE',
                      help='run the controller in interface only mode for testing integration and ICD compliance. (default: %default)')
    parser.add_option('--local-resources', dest='local_resources', default=False,
                      action='store_true', metavar='LOCALRESOURCES',
                      help='launch all containers on local machine via /var/run/docker.sock (default: %default)')
    parser.add_option('--tag', dest='tag', type="string",
                      default='latest', metavar='TAG',
                      help='Docker image tag from which to create containers (default: %default)')
    parser.add_option('-v', '--verbose', dest='verbose', default=False,
                      action='store_true', metavar='VERBOSE',
                      help='print verbose output (default: %default)')
    (opts, args) = parser.parse_args()

    def die(msg=None):
        if msg:
            print msg
        else:
            parser.print_help()
        sys.exit(1)

    if len(logging.root.handlers) > 0: logging.root.removeHandler(logging.root.handlers[0])
    formatter = logging.Formatter("%(asctime)s.%(msecs)dZ - %(filename)s:%(lineno)s - %(levelname)s - %(message)s",
                                      datefmt="%Y-%m-%d %H:%M:%S")
    sh = logging.StreamHandler()
    sh.setFormatter(formatter)
    logging.root.addHandler(sh)

    if isinstance(opts.loglevel, basestring):
        opts.loglevel = getattr(logging, opts.loglevel.upper())

    logging.root.setLevel(opts.loglevel)

    logger = logging.getLogger('sdpcontroller')

    from katsdpcontroller import sdpcontroller

    logger.info("Starting SDP Controller...")
    server = sdpcontroller.SDPControllerServer(
        opts.host, opts.port,
        simulate=opts.simulate, interface_mode=opts.interface_mode,
        local_resources=opts.local_resources, tag=opts.tag)

    restart_queue = Queue.Queue()
    server.set_restart_queue(restart_queue)

    running = True
    def stop_running(signum, frame):
        """Stop the global server."""
        global running
        running = False
    signal.signal(signal.SIGQUIT, stop_running)
    signal.signal(signal.SIGTERM, stop_running)

    server.start()
    logger.info("Started SDP Controller.")

    if manhole:
        manhole.install(oneshot_on='USR1', locals={'logger':logger, 'server':server, 'opts':opts})
         # allow remote debug connections and expose server and opts
    try:
        while running:
            try:
                device = restart_queue.get(timeout=0.5)
            except Queue.Empty:
                device = None
            if device is not None:
                logger.info("Stopping...")
                device.stop()
                device.join()
                logger.info("Restarting...")
                device.start()
                logger.info("Started.")
    except KeyboardInterrupt:
        pass
     # handle all exit conditions, including keyboard interrupt, katcp halt and sigterm
    server.handle_exit()
    server.stop()
    server.join()
