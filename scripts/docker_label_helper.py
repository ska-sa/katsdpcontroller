#!/usr/bin/env python3

################################################################################
# Copyright (c) 2013-2023, National Research Foundation (SARAO)
#
# Licensed under the BSD 3-Clause License (the "License"); you may not use
# this file except in compliance with the License. You may obtain a copy
# of the License at
#
#   https://opensource.org/licenses/BSD-3-Clause
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

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
