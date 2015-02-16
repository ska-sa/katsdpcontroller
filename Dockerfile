FROM ubuntu:14.04

MAINTAINER Simon Ratcliffe "simonr@ska.ac.za"

# Try and suppress debconf warnings
ENV DEBIAN_FRONTEND noninteractive

# Install system packages
RUN apt-get -y update && apt-get -y install \
    build-essential software-properties-common wget git-core \
    python python-dev python-pip python-numpy \
    supervisor python-software-properties

# Add a PPA for redis, since we need a newer version than 14.04 default (2.6.13)
RUN apt-add-repository ppa:chris-lea/redis-server
RUN apt-get -y update && apt-get -y install redis-server

# Install Python dependencies
COPY requirements.txt /tmp/install/requirements.txt
RUN pip install -r /tmp/install/requirements.txt

# Install the current package
COPY . /tmp/install/katsdpcontroller
WORKDIR /tmp/install/katsdpcontroller
RUN pip install .

# Network setup
EXPOSE 5000

# Launch configuration
COPY ./conf/supervisord.conf /etc/supervisor/conf.d/sdpmc.conf

CMD ["/usr/bin/supervisord","-c","/etc/supervisor/supervisord.conf"]
