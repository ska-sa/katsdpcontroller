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
    parser.add_option('--registry', dest='private_registry', type="string",
                      default='sdp-ingest5.kat.ac.za:5000', metavar='HOST:PORT',
                      help='registry from which to pull images (use empty string to disable) (default: %default)')
    parser.add_option('--image-override', dest='image_override', action='append',
                      default=[], metavar='NAME:IMAGE',
                      help='Override an image name lookup (default: none)')
    parser.add_option('--image-tag', dest='image_tag', type='string',
                      metavar='TAG', help='Image tag to use for resolving images')
    parser.add_option('--no-pull', action='store_true', default=False,
                      help='Skip pulling images from the registry')
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
    image_resolver = sdpcontroller.ImageResolver(
            private_registry=opts.private_registry or None,
            tag=opts.image_tag or None,
            pull=not opts.no_pull)
    for override in opts.image_override:
        fields = override.split(':', 1)
        if len(fields) < 2:
            die("--image-override option must have a colon")
        image_resolver.override(fields[0], fields[1])
    server = sdpcontroller.SDPControllerServer(
        opts.host, opts.port,
        simulate=opts.simulate,
        interface_mode=opts.interface_mode,
        local_resources=opts.local_resources,
        image_resolver=image_resolver)

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
