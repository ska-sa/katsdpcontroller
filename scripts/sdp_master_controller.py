#!/usr/bin/env python

"""Script for launching the Science Data Processor Master Controller.

   Copyright (c) 2013 SKA/KAT. All Rights Reserved.
"""

import sys
import os
import time
import Queue
import signal
from optparse import OptionParser
import logging
import logging.handlers

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
                      help='run controller in simulation mode (default: %default)')
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

    logger = logging.getLogger('sdpcontroller')
    if isinstance(opts.loglevel, basestring):
        opts.loglevel = getattr(logging, opts.loglevel.upper())
    logger.setLevel(opts.loglevel)
    fh = logging.handlers.RotatingFileHandler(os.path.join(opts.workpath, 'sdpcontroller.log'), maxBytes=1e6, backupCount=10)
    formatter = logging.Formatter(("%(asctime)s.%(msecs)dZ - %(name)s - %(filename)s:%(lineno)s"
        " - %(levelname)s - %(message)s"), datefmt="%Y-%m-%d %H:%M:%S")
    fh.setFormatter(formatter)
    logger.addHandler(fh)
     # we assume this is the SDP ur process and so we setup logging in a fairly manual fashion

    from katsdpcontroller import sdpcontroller

    if opts.simulate:
        logger.info("Starting SDP Controller (**Simulation Mode**)...")
        server = sdpcontroller.SDPControllerSimulationServer(opts.host, opts.port)
    else:
        logger.info("Starting SDP Controller...")
        server = sdpcontroller.SDPControllerServer(opts.host, opts.port)

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
    logger.info("Started.")

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
    finally:
        logger.info("Shutting down...")
        server.stop()
        server.join()
