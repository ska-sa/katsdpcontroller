FROM sdp-docker-registry.kat.ac.za:5000/docker-base

MAINTAINER Simon Ratcliffe "simonr@ska.ac.za"

# Label the image with a list of images it uses
ARG dependencies
RUN test -n "$dependencies" || (echo "Please build with scripts/docker_build.sh" 1>&2; exit 1)
LABEL za.ac.kat.sdp.image-depends $dependencies

# Install Python dependencies
COPY requirements.txt /tmp/install/requirements.txt
RUN install-requirements.py --default-versions ~/docker-base/base-requirements.txt -r /tmp/install/requirements.txt

# Install the current package
COPY . /tmp/install/katsdpcontroller
WORKDIR /tmp/install/katsdpcontroller
RUN python ./setup.py clean && pip install --no-deps . && pip check

# Network setup
EXPOSE 5001

# Create directory into which config.json can be volume-mounted
RUN mkdir /home/kat/.docker
