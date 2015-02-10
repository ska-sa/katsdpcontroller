FROM registry:0.9.1

ENV DEBIAN_FRONTEND noninteractive

RUN sudo apt-get update && sudo apt-get -y install nginx-full apache2-utils
COPY nginx.conf /etc/nginx/sites-available/docker-registry
COPY docker-registry.conf /etc/nginx/docker-registry.conf
RUN htpasswd -b -c /etc/nginx/docker-registry.htpasswd kat kat
RUN rm /etc/nginx/sites-enabled/default && ln -s /etc/nginx/sites-available/docker-registry /etc/nginx/sites-enabled/

# Prevents a race condition in initial database setup
COPY preload.patch /docker-registry/preload.patch
RUN cd /usr/local/lib/python2.7/dist-packages && patch -p1 < /docker-registry/preload.patch

EXPOSE 443

CMD service nginx start && exec docker-registry
