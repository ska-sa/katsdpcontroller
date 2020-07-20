# SDP Sandbox

This is a docker-compose environment for testing on a local machine. It runs
local versions of much of the infrastructure present on a production system.

It can be used either inside or outside the SARAO firewall, but in the latter
case some extra steps are needed to create your own Docker registry with built
images.

## Requirements

Hardware-wise, you will need an NVIDIA GPU. Lots of RAM and a beefy CPU are
also recommended for simulating more than a few antennas, and AVX2 is required
for the continuum imager.

Software-wise, you will need:
- Python 3 (see [setup.py](../setup.py) for specific minimum version)
- docker
- nvidia-container-runtime
- curl
- haproxy (optional)
- [docker-compose](https://docs.docker.com/compose/)
- [kattelmod](https://github.com/ska-sa/kattelmod)
- [katsdpingest](https://github.com/ska-sa/katsdpingest) checked out, but it
  doesn't need to be installed
- [katsdpcontroller](https://github.com/ska-sa/katsdpcontroller) checked out,
  but it doesn't need to be installed (unless you want to run your local copy
  instead of the Docker image)
- Python packages `netifaces`, `psutil`, `py3nvml` and `pycuda`.

You'll also need certain ports to be available on your machine:
- Zookeeper: 2181
- Mesos master: 5050
- Mesos agent: 5051
- Singularity: 7099
- Logstash: 9600, 12201 (UDP)
- Elasticsearch: 9200, 9300
- Kibana: 5601
- Prometheus: 9090
- Grafana: 3000
- Master controller: 5001, 8080
- Docker registry: 5000

Prometheus, Grafana, Elasticsearch, Logstash and Kibana are all just for
monitoring, so you could disable them in `docker-compose.yml` if they are
causing problems.

To use the SDP Docker registry at sdp-docker-registry.kat.ac.za, you'll need
read-access credentials stored (unencrypted) in your `~/.docker/config.json`.
Note that on some systems, `docker login` stores credentials in a key store,
which will not work with the Mesos agent.

## Populating a local Docker registry

If you don't have access to sdp-docker-registry.kat.ac.za (outside the SARAO firewall)
or want to be independent of it, you can create your own registry. The steps to
do this, starting at the root of katsdpcontroller, and with a Python 3.6+
virtual environment, are:

```sh
pip install -e .
pip install docker
cd sandbox
docker-compose up -d registry
./update-local-registry.py
```

The script has some command-line options. In particular, you can use
`--include` and `--exclude` to control which images are built, and you can use
`--downstream` to use a different registry for the built images (for example, a
registry shared between multiple people). Note that some of the built images
(for example, those bundling CUDA) might not be suitable for public
redistribution due to licensing restrictions.

If you're inside the SARAO firewall (and have used `docker login` to
authenticate to the registry), you can replace the last step with
```sh
./update-local-registry.py --copy-all --upstream sdp-docker-registry.kat.ac.za:5000
```
which will be faster since it will avoid building a number of images.

## Starting up the sandbox

1. Run `./prepare.sh`. This will query your system for information
   about the GPU etc, and prepare configuration files for Mesos. This normally
   only needs to be done once. If you need to change the configurations, you
   might need to delete the files in /var/tmp/mesos to clear the previous state.
   You can also edit the files generated in `./etc/mesos` e.g., to increase the
   amount of memory or CPU that Mesos thinks it has so that you can simulate
   larger arrays (at the risk of gobbling up all the RAM in your machine!)

2. Run `docker-compose up -d`. Use `docker-compose ps` to check that the
   processes are all starting up.

3. After the system has been left to settle for a bit, run `./elk-setup.py`.
   This will configure the logging systems. If it fails, give it another
   minute for the services to start up and try again. This step is only
   needed once, unless you destroy the Docker volumes from the sandbox.

4. Run `./grafana-setup.sh`, which will install a plugin needed for one of the
   Grafana plots. Again, this step is only needed once, unless you destroy
   the Docker volumes.

5. Sometimes Singularity doesn't realise that it should be the master if it is
   started too soon after Zookeeper. To be on the safe side, run
   `docker-compose restart singularity` to get it going.

## Preparing an image tag

*This step is only applicable when using the SARAO SDP registry.*

If you just want to run the master branch of all the SDP code you can skip
this step. Otherwise, use
[Jenkins](https://sdp-jenkins.kat.ac.za/view/Deployment/job/deployment/job/generic/)
to create a custom tag (use your username as the tag or part of the tag to
avoid collisions with other developers) using your chosen branches.

Write the chosen tag into a file e.g., called `sdp_image_tag` (you can call it
whatever you want, as long as you remember to use the actual name when copying
and pasting examples below).

## Creating an auto-tuned ingest container

*If you're using `update-local-registry.py` to populate your own registry it
will take care of this step for you.*

The ingest container needs to be pre-tuned for your specific GPU. Unless you
have one of the GPUs that is pre-tuned, you will need to run the following
steps. To determine the GPU name to use, run `nvidia-smi` to see the canonical
name, then convert to lowercase, convert spaces and dashes to underscores, and
delete anything else that isn't alphanumeric. For example, for `Quadro
K2100M`, use `quadro_k2100m` in place of GPUNAME the example below. **Remember
to replace GPUNAME with the actual name and TAG with your custom tag when
using the commands below.**
```sh
docker pull sdp-docker-registry.kat.ac.za:5000/katsdpingest:TAG
cd ~/katsdpingest    # or wherever you have it checked out
scripts/autotune_mkimage.py sdp-docker-registry.kat.ac.za:5000/katsdpingest_GPUNAME:TAG sdp-docker-registry.kat.ac.za:5000/katsdpingest:TAG
docker push sdp-docker-registry.kat.ac.za:5000/katsdpingest_GPUNAME:TAG
```
This will take about 10 minutes to do. If you've done this before but need to
update it for a new upstream katsdpingest image, you can add `--copy` to the
command to seed the autotuning with the previous results, which is usually
much faster.

## Starting the master controller

There are two ways to do this, depending on whether you want to use a
pre-built container or run it directly on the host (which is useful when
developing the master controller itself). **Note: even when running the
master controller from source, the product controller is still run from a
Docker image.**

In either case, add `--registry http://localhost:5000` if you are using a
local registry rather than the SARAO SDP registry, and omit the `--haproxy`
option if you do not have haproxy installed.

### Docker image

```sh
docker run --net=host -v $PWD/sandbox:/sandbox:ro -e KATSDP_LOG_GELF_ADDRESS=127.0.0.1 sdp-docker-registry.kat.ac.za:5000/katsdpcontroller sdp_master_controller.py --gui-urls /sandbox/gui-urls/ --localhost --image-tag-file /sandbox/sdp_image_tag --s3-config-file /sandbox/s3_config.json --haproxy localhost:2181 http://localhost:7099/singularity
```

### Local machine

```sh
KATSDP_LOG_GELF_ADDRESS=127.0.0.1 scripts/sdp_master_controller.py --gui-urls sandbox/gui-urls/ --localhost --image-tag-file sandbox/sdp_image_tag --s3-config-file sandbox/s3_config.json --haproxy localhost:2181 http://localhost:7099/singularity
```

## Running kattelmod

In the `kattelmod` repository, run
`kattelmod/systems/mkat/generator_sim_config.py` with arguments for the system
you want to simulate. The important option is `-m localhost` to run the
simulation on your local machine. This will output a text file which you
should save to a local file (and edit if you wish).

Now run an imaging script, such as 
```sh
python kattelmod/test/basic_track.py --config=sim_16ant_local.cfg 'Zenith, azel, 0, 90' -t 100
```
or
```sh
trg="PKS 0408-65, radec target, 4:08:20.38, -65:45:09.1, (800.0 8400.0 -3.708 3.807 -0.7202)"
python ./scripts/image.py -t 900 "$trg" -m 1000 --config sim-local-spectral8.cfg
```

## Tips and tricks

### Making local changes for debugging
In an edit-debug cycle, it's not always convenient to rebuild the container
and the subarray on every edit. If the service uses `setup_restart` from
[katsdpservices](https://github.com/ska-sa/katsdpservices), then you can send
the process SIGHUP to have it restart itself. Use `docker exec` to enter the
container and modify the code in `/home/kat/ve3/lib/python3.6/site-packages`,
use `ps` to find the PID, and then `kill -HUP <PID>`.

Note that `docker restart` is *not* your friend any more: Mesos thinks the
container has died, and gets very confused when it comes back to life; and the
task will still be dead to the master controller. It may end up being
necessary to manually kill it off again.

### Locally-modified Docker images

By default the master controller will always use the registry to determine the
versions of images to run, which means that locally-built images will be
ignored. Use `--no-pull` when starting the container to always use the
local copy of the image if it exists. Note that this can easily lead to using
very old copies of images.

For overriding individual images, one can use `--image-override NAME:IMAGE` to
specify the specific image to use for NAME. This will not be force-pulled, so a
locally-built image will be used.
