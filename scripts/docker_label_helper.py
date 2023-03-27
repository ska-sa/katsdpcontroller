#!/usr/bin/env python3

import argparse
import sys

try:
    import katsdpcontroller.generator
    import katsdpcontroller.schemas
except ImportError:
    print(
        "Failed to import katsdpcontroller. Please install katsdpcontroller first.",
        file=sys.stderr,
    )
    sys.exit(1)


def find_images():
    images = set(katsdpcontroller.generator.IMAGES)
    # Add self to the image list
    images.add("katsdpcontroller")
    return images


def product_configure_versions():
    return [str(v) for v in katsdpcontroller.schemas.PRODUCT_CONFIG.versions]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("variable", choices=["dependencies", "product_configure_versions"])
    args = parser.parse_args()

    variable = args.variable
    if variable == "dependencies":
        print(" ".join(sorted(find_images())))
    elif variable == "product_configure_versions":
        print(" ".join(product_configure_versions()))
    else:
        assert False, f"Unexpected argument {variable}"


if __name__ == "__main__":
    main()
