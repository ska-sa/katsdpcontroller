#!/usr/bin/env python
from __future__ import print_function
import sys
try:
    import katsdpcontroller.generator
except ImportError as e:
    print("Failed to import katsdpcontroller.generator. Please install katsdpcontroller first.",
          file=sys.stderr)
    sys.exit(1)


def find_images():
    images = set(katsdpcontroller.generator.IMAGES)
    # Add some repositories that do not form part of the graph
    images.add('katsdpcontroller')
    images.add('docker-base')
    return images


if __name__ == '__main__':
    print(' '.join(sorted(find_images())))
