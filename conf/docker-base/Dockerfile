# Base Dockerfile for SDP images

FROM ubuntu:14.04

MAINTAINER Bruce Merry "bmerry@ska.ac.za"

# Suppress debconf warnings
ENV DEBIAN_FRONTEND noninteractive

# Install some system packages used by multiple images.
# These are required by:
# - katsdptelstate: netifaces
# - katcp: python-ply python-twisted python-zope.interface
RUN apt-get -y update && apt-get -y install \
    build-essential git-core subversion software-properties-common wget \
    python python-dev python-pip \
    python-netifaces \
    python-numpy \
    python-ply \
    python-twisted \
    python-zope.interface

# Set up access to github private repositories
COPY id_rsa /root/.ssh/
RUN echo "Host *\n\tStrictHostKeyChecking no\n" >> ~/.ssh/config
RUN chmod -R go-rwx ~/.ssh

# Set up access to internal svn repositories
# svn 1.8 has an issue with embedding the password into the URL
# (see http://stackoverflow.com/questions/22451147/svn-1-8-gets-confused-with-password-in-repo-string-using-python-pip),
# so we have to run a command to pre-populate the authentication cache. We also need to
# - Download the CA certificate (Ubuntu does not trust cacert)
# - Set up the configuration file to allow passwords to be stored in plaintext
# - Put the internal IP for katfs into /etc/hosts (since the external version doesn't do SSL).
#   Docker ensures that this change is not persistent.
ENV KATFS_IP 192.168.1.21
RUN wget -q http://www.cacert.org/certs/root.crt -O /usr/local/share/ca-certificates/cacert-root.crt && \
    wget -q http://www.cacert.org/certs/class3.crt -O /usr/local/share/ca-certificates/cacert-class3.crt && \
    update-ca-certificates && \
    mkdir -p $HOME/.subversion && \
    /bin/echo -e '[global]\nstore-passwords = yes\nstore-plaintext-passwords = yes\n' > $HOME/.subversion/servers && \
    echo "$KATFS_IP katfs.kat.ac.za" >> /etc/hosts && \
    svn ls --username kat --password kat https://katfs.kat.ac.za/svnDS/code/katconf > /dev/null

# Installs dependencies needed by PySPEAD, katsdptelstate, katcp and katpoint.
# The packages themselves are not installed, in case a downstream image needs
# to install from a specific version/branch.
# The versions are the latest at the time; there is no known reason they can't
# be updated.
RUN pip install --no-deps \
    ProxyTypes==0.9 \
    pyephem==3.7.5.3 \
    redis==2.10.3 \
    six==1.9.0

# Create a user which will be used to run commands with reduced privileges.
# The user has a password and is in the sudo group so that after running
# "docker exec" it is possible to regain root privileges while debugging.
RUN adduser --disabled-password --gecos 'unprivileged user' kat && \
    (echo kat; echo kat) | passwd kat && \
    adduser kat sudo
