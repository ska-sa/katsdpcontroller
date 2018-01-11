FROM sdp-docker-registry.kat.ac.za:5000/docker-base

MAINTAINER Simon Ratcliffe "simonr@ska.ac.za"

# Label the image with a list of images it uses
ARG dependencies
RUN test -n "$dependencies" || (echo "Please build with scripts/docker_build.sh" 1>&2; exit 1)
LABEL za.ac.kat.sdp.image-depends $dependencies

# Switch to Python 3 environment
ENV PATH="$PATH_PYTHON3" VIRTUAL_ENV="$VIRTUAL_ENV_PYTHON3"

# Install haproxy for haproxy_disp and graphviz for --write-graphs support
USER root
RUN apt-get -y update && apt-get -y --no-install-recommends install haproxy graphviz
USER kat

# Install Python dependencies
COPY requirements.txt /tmp/install/requirements.txt
RUN install-requirements.py --default-versions ~/docker-base/base-requirements.txt -r /tmp/install/requirements.txt

# Install the current package
COPY . /tmp/install/katsdpcontroller
WORKDIR /tmp/install/katsdpcontroller
RUN python ./setup.py clean && pip install --no-deps . && pip check

# Network setup
EXPOSE 5001
