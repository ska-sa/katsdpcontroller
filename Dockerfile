FROM ubuntu:14.04

MAINTAINER Simon Ratcliffe "simonr@ska.ac.za"

# Try and supress debconf warnings
ENV DEBIAN_FRONTEND noninteractive
ENV TMPDIR /tmp

# Install system packages
RUN apt-get -y update && apt-get -y install \
    build-essential software-properties-common wget git-core \
    python python-dev python-pip \
    libhdf5-serial-dev libblas-dev gfortran liblapack-dev \
    supervisor python-software-properties

# Add a PPA for redis, since we need a newer version than 14.04 default (2.6.13)
RUN apt-add-repository ppa:chris-lea/redis-server
RUN apt-get -y update && apt-get -y install redis-server

# Install Python dependencies
RUN pip install -U pip setuptools
COPY requirements.txt /tmp/install/requirements.txt
RUN pip install numpy && pip install -r /tmp/install/requirements.txt

# Install the current package
COPY . /tmp/install/katsdpingest
WORKDIR /tmp/install/katsdpingest
RUN pip install .

# Network setup
EXPOSE 5000

# Launch configuration
COPY ./conf/supervisord.conf /etc/supervisor/conf.d/sdpmc.conf

CMD ["/usr/bin/supervisord","-c /etc/supervisor/supervisord.conf"]
