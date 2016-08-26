#!/usr/bin/env python
from __future__ import print_function
import sys
try:
    import katsdpgraphs.generator
except ImportError as e:
    print("Failed to import katsdpgraphs.generator. Please install katsdpcontroller first.",
          file=sys.stderr)
    sys.exit(1)


class DummyResources(object):
    """Dummy implementation of :class:`katsdpcontroller.SDPResources` that
    just collects a list of images.
    """
    def __init__(self):
        self.images = set()
        self.prefix = ''

    def get_image_path(self, image):
        self.images.add(image)

    def get_host_ip(self, host_class):
        return '127.0.0.1'

    def get_port(self, host_class):
        return 0

    def get_sdisp_port_pair(self, host_class):
        return (0, 1)

    def get_multicast_ip(self, host_class):
        return '127.0.0.1'

    def get_multicast(self, host_class):
        return '127.0.0.1:0'

    def get_url(self, host_class):
        return 'ws://host.domain:port/path'


def find_images():
    resources = DummyResources()
    # Add some repositories that do not form part of the graph
    resources.images.add('katsdpcontroller')
    for beamformer_mode in ["none", "hdf5_ram", "hdf5_ssd", "ptuse"]:
        for channels in [4096, 32768]:
            for simulate in [False, True]:
                katsdpgraphs.generator.build_physical_graph(beamformer_mode, channels, simulate, resources)
    return resources.images


if __name__ == '__main__':
    print(' '.join(sorted(find_images())))
