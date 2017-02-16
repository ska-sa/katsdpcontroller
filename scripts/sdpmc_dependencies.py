#!/usr/bin/env python
from __future__ import print_function
import sys
try:
    import katsdpgraphs.generator
except ImportError as e:
    print("Failed to import katsdpgraphs.generator. Please install katsdpcontroller first.",
          file=sys.stderr)
    sys.exit(1)


def find_images():
    images = set()
    # Add some repositories that do not form part of the graph
    images.add('katsdpcontroller')
    images.add('docker-base')
    for beamformer_mode in ["none", "hdf5_ram", "hdf5_ssd", "ptuse"]:
        for channels in [4096, 32768]:
            # There is a special case for small numbers of antennas
            for antennas in [1, 2, 3, 4, 16]:
                for simulate in [False, True]:
                    # Unlike antennas, dump rate doesn't affect the set of
                    # images used
                    graph = katsdpgraphs.generator.build_logical_graph(
                        beamformer_mode, simulate, False, channels, antennas, 1.0)
                    for node in graph:
                        if hasattr(node, 'image'):
                            images.add(node.image)
    return images


if __name__ == '__main__':
    print(' '.join(sorted(find_images())))
