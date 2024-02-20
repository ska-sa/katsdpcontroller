################################################################################
# Copyright (c) 2013-2024, National Research Foundation (SARAO)
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

ARG KATSDPDOCKERBASE_REGISTRY=quay.io/ska-sa

FROM $KATSDPDOCKERBASE_REGISTRY/docker-base-build as build

# Switch to Python 3 environment
ENV PATH="$PATH_PYTHON3" VIRTUAL_ENV="$VIRTUAL_ENV_PYTHON3"

# Install Python dependencies
COPY requirements.txt /tmp/install/requirements.txt
RUN pip install -r /tmp/install/requirements.txt

# Install the current package
COPY --chown=kat:kat . /tmp/install/katsdpcontroller
WORKDIR /tmp/install/katsdpcontroller
RUN python ./setup.py clean
RUN pip install --no-deps .
RUN pip check

#######################################################################

FROM $KATSDPDOCKERBASE_REGISTRY/docker-base-runtime
LABEL maintainer="sdpdev+katsdpcontroller@ska.ac.za"

# Label the image with a list of images it uses
ARG dependencies
RUN test -n "$dependencies" || (echo "Please build with scripts/docker_build.sh" 1>&2; exit 1)
LABEL za.ac.kat.sdp.image-depends $dependencies

# Label the image with a list of supported product-configure schema versions
ARG product_configure_versions
RUN test -n "$product_configure_versions" || (echo "Please build with scripts/docker_build.sh" 1>&2; exit 1)
LABEL za.ac.kat.sdp.katsdpcontroller.product-configure-versions $product_configure_versions

# Install haproxy for --haproxy support and graphviz for --write-graphs
USER root
RUN apt-get update && \
    apt-get -y --no-install-recommends install haproxy graphviz && \
    rm -rf /var/lib/apt/lists/*
USER kat

COPY --from=build --chown=kat:kat /home/kat/ve3 /home/kat/ve3
ENV PATH="$PATH_PYTHON3" VIRTUAL_ENV="$VIRTUAL_ENV_PYTHON3"

# Network setup
EXPOSE 5001
EXPOSE 5004
