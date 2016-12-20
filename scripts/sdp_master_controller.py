#!/usr/bin/env python

"""Script for launching the Science Data Processor Master Controller.

   Copyright (c) 2013 SKA/KAT. All Rights Reserved.
"""

import sys
import os
import Queue
import signal
import tornado
import tornado.gen
from optparse import OptionParser
import logging
import logging.handlers

try:
    import manhole
except ImportError:
    manhole = None

@tornado.gen.coroutine
def on_shutdown(ioloop, server):
    si = signal.signal(signal.SIGINT, signal.SIG_IGN)
    st = signal.signal(signal.SIGTERM, signal.SIG_IGN)
     # avoid any further interruptions whilst we handle shutdown
    server.handle_exit()
    yield server.stop()
    ioloop.stop()
    signal.signal(signal.SIGINT, si)
    signal.signal(signal.SIGTERM, st)

if __name__ == "__main__":

    usage = "usage: %prog [options]"
    parser = OptionParser(usage=usage)
    parser.add_option('-a', '--host', dest='host', type="string", default="", metavar='HOST',
                      help='attach to server HOST (default="%default" - localhost)')
    parser.add_option('-p', '--port', dest='port', type="int", default=5001, metavar='N',
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
                      default='sdp-docker-registry.kat.ac.za:5000', metavar='HOST:PORT',
                      help='registry from which to pull images (use empty string to disable) (default: %default)')
    parser.add_option('--image-override', dest='image_override', action='append',
                      default=[], metavar='NAME:IMAGE',
                      help='Override an image name lookup (default: none)')
    parser.add_option('--image-tag-file', dest='image_tag_file', type='string',
                      metavar='FILE', help='Load image tag to run from file (on each configure)')
    parser.add_option('--safe-multicast-cidr', dest='safe_multicast_cidr', type='string', default='225.100.0.0/16',
                      metavar='MULTICAST-CIDR', help='Block of multicast addresses from which to draw internal allocation. Needs to be at least /16. (default: %default)')
    parser.add_option('--graph-override', dest='graph_override', action='append',
                      default=[], metavar='SUBARRAY_PRODUCT_ID:NEW_GRAPH',
                      help='Override the graph to be used for the specified subarray product id (default: none)')
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
    logging.captureWarnings(True)

    logger = logging.getLogger('sdpcontroller')

    from katsdpcontroller import sdpcontroller

    logger.info("Starting SDP Controller...")
    image_resolver = sdpcontroller.ImageResolver(
            private_registry=opts.private_registry or None,
            tag_file=opts.image_tag_file,
            pull=not opts.no_pull)
    for override in opts.image_override:
        fields = override.split(':', 1)
        if len(fields) < 2:
            die("--image-override option must have a colon")
        image_resolver.override(fields[0], fields[1])

    for override in opts.graph_override:
        if len(override.split(':', 1)) < 2:
            die("--graph-override option must be in the form <subarray_product_id>:<graph_name>")

    graph_resolver = sdpcontroller.GraphResolver(overrides=opts.graph_override, simulate=opts.simulate)

    ioloop = tornado.ioloop.IOLoop.current()
    server = sdpcontroller.SDPControllerServer(
        opts.host, opts.port,
        simulate=opts.simulate,
        interface_mode=opts.interface_mode,
        local_resources=opts.local_resources,
        image_resolver=image_resolver,
        graph_resolver=graph_resolver,
        safe_multicast_cidr=opts.safe_multicast_cidr)

    if manhole:
        manhole.install(oneshot_on='USR1', locals={'logger':logger, 'server':server, 'opts':opts})
         # allow remote debug connections and expose server and opts

    logger.info("Starting SDP...")

    server.set_ioloop(ioloop)
    signal.signal(signal.SIGINT, lambda sig, frame: ioloop.add_callback_from_signal(on_shutdown, ioloop, server))
    signal.signal(signal.SIGTERM, lambda sig, frame: ioloop.add_callback_from_signal(on_shutdown, ioloop, server))
    ioloop.add_callback(server.start)
    ioloop.start()
