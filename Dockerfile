ARG KATSDPDOCKERBASE_REGISTRY=quay.io/ska-sa

FROM $KATSDPDOCKERBASE_REGISTRY/docker-base-build as build

USER root
RUN apt-get update && \
    apt-get -y --no-install-recommends install liblua5.3-dev libssl-dev
USER kat

# We need to build haproxy from source because the version in Ubuntu 18.04 has
# a bug that causes it to run out of file handles and the PPA at
# https://launchpad.net/~vbernat/+archive/ubuntu/haproxy-1.8 deletes non-latest
# versions.
#
# The make flags are based on debian/rules from the Ubuntu 18.04 package.
RUN VERSION=1.8.22 && \
    wget http://www.haproxy.org/download/1.8/src/haproxy-$VERSION.tar.gz -O /tmp/haproxy-$VERSION.tar.gz && \
    tar -C /tmp -zxf /tmp/haproxy-$VERSION.tar.gz && \
    cd /tmp/haproxy-$VERSION && \
    make -j PREFIX=/usr \
        USE_PCRE=1 PCREDIR= USE_PCRE_JIT=1 \
        USE_OPENSSL=1 USE_ZLIB=1 USE_LUA=1 LUA_INC=/usr/include/lua5.3 \
        USE_GETADDRINFO=1 \
        USE_NS=1 TARGET=linux2628 USE_REGPARM=1 DEBUG=-s && \
    make install PREFIX=/usr DESTDIR=/tmp/haproxy-install

# Switch to Python 3 environment
ENV PATH="$PATH_PYTHON3" VIRTUAL_ENV="$VIRTUAL_ENV_PYTHON3"

# Install Python dependencies
COPY requirements.txt /tmp/install/requirements.txt
RUN install-requirements.py --default-versions ~/docker-base/base-requirements.txt -r /tmp/install/requirements.txt

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

# Install haproxy for --haproxy support and graphviz for --write-graphs
USER root
COPY --from=build /tmp/haproxy-install/usr/sbin/haproxy /usr/sbin/haproxy
RUN apt-get update && \
    apt-get -y --no-install-recommends install liblua5.3 graphviz && \
    rm -rf /var/lib/apt/lists/*
USER kat

COPY --from=build --chown=kat:kat /home/kat/ve3 /home/kat/ve3
ENV PATH="$PATH_PYTHON3" VIRTUAL_ENV="$VIRTUAL_ENV_PYTHON3"

# Network setup
EXPOSE 5001
EXPOSE 5004
