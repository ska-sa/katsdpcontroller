FROM sdp-ingest5.kat.ac.za:5000/docker-base

MAINTAINER Simon Ratcliffe "simonr@ska.ac.za"

# Install system packages
RUN apt-get -y update && apt-get -y install \
    build-essential software-properties-common wget git-core \
    python python-dev python-pip python-numpy \
    supervisor python-software-properties

# Install Python dependencies
COPY requirements.txt /tmp/install/requirements.txt
RUN pip install -r /tmp/install/requirements.txt

# Install the current package
COPY . /tmp/install/katsdpcontroller
WORKDIR /tmp/install/katsdpcontroller
RUN pip install --no-deps .

# Network setup
EXPOSE 5000

# Launch configuration
USER root
WORKDIR /root

RUN mkdir /root/.docker
COPY ./conf/docker_keys /root/.docker/
