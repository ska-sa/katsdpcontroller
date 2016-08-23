FROM sdp-docker-registry.kat.ac.za:5000/docker-base

MAINTAINER Simon Ratcliffe "simonr@ska.ac.za"

# Prevent the user from accidentally running without the wrapper script
ARG dependencies_set=no
RUN test "$dependencies_set" = "yes" || (echo "Please build with scripts/docker_build.sh" 1>&2; exit 1)

# Install Python dependencies
COPY requirements.txt /tmp/install/requirements.txt
RUN install-requirements.py --default-versions ~/docker-base/base-requirements.txt -r /tmp/install/requirements.txt

# Install the current package
COPY . /tmp/install/katsdpcontroller
WORKDIR /tmp/install/katsdpcontroller
RUN python ./setup.py clean && pip install --no-index .

# Network setup
EXPOSE 5000

# Launch configuration
WORKDIR /home/kat

RUN mkdir /home/kat/.docker
COPY ./conf/docker_keys /home/kat/.docker/
