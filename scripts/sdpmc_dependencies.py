#!/usr/bin/env python3

import sys

try:
    import katsdpcontroller.generator
except ImportError:
    print(
        "Failed to import katsdpcontroller.generator. Please install katsdpcontroller first.",
        file=sys.stderr,
    )
    sys.exit(1)


def find_images():
    images = set(katsdpcontroller.generator.IMAGES)
    # Add self to the image list
    images.add("katsdpcontroller")
    return images


if __name__ == "__main__":
    print(" ".join(sorted(find_images())))
