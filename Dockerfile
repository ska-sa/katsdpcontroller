FROM sdp-ingest5.kat.ac.za:5000/docker-base:docker-refactor

MAINTAINER Simon Ratcliffe "simonr@ska.ac.za"

# Install system packages
USER root
RUN apt-get -y update && apt-get -y install \
    supervisor
USER kat

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
