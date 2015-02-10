Docker registry with authentication
===================================

This is a modification to the basic docker-registry image that uses nginx to
provide HTTPS and basic authentication.

Server setup
------------

You will need the following files on the server side (the directory can be changed, but the
filenames are baked in):

 - /etc/ssl/private/ca.pem: CA certificate
 - /etc/ssl/private/server-cert.pem: server certificate
 - /etc/ssl/private/server-key.pem: server private key

Also, create a directory to store the persistent data, such as `/var/kat/docker-registry`.
With that in place, run it as:

    docker run -p 5000:443 -v /var/kat/docker-registry:/tmp/registry -v /etc/ssl/private:/etc/ssl/private -d <imagename>

This can, for example, be added to `/etc/rc.local`.

Client setup
------------

The clients will also need to be configured with the CA certificate. There are two options:

 1. Pass --insecure-registry=<host>:5000 to the Docker daemon. On Ubuntu, this can be done
    by editing `/etc/default/docker` and adding it to DOCKER_OPTS.

 2. Add the certificate to the client machine. On Ubuntu, one can do this by copying it to
    `/usr/local/share/ca-certificates/ska_sa_sdp.crt`, running `update-ca-certificates`,
    and then restarting Docker.

Once the client is configured and the server is running, one should be able to run:

    docker login https://<server>:5000/

using `kat` as the username and password, and an arbitrary email address. If
that succeeds, then one can use the `push` and `pull` commands with image names
such as `<server>:5000/image`.

Caveats
-------
Due to a Docker [bug](https://github.com/docker/docker/issues/8849), it does
not work to put the certificates in `/etc/docker/certs.d/<registry>`. While
other Docker commands work, `docker login` will give an error about the
certificate being signed by an unknown authority.

References
----------
Most of the setup is based on [these
instructions](https://www.digitalocean.com/community/tutorials/how-to-set-up-a-private-docker-registry-on-ubuntu-14-04).
The nginx configuration files were taken from the [docker-registry contrib directory](https://github.com/docker/docker-registry/tree/master/contrib/nginx).
