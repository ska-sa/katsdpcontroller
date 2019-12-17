# XXX Temporary hack to disable spectral imaging - DO NOT MERGE

FROM sdp-docker-registry.kat.ac.za:5000/katsdpcontroller:production-00116-20191127

COPY --chown=kat:kat katsdpcontroller/generator.py /home/kat/ve3/lib/python3.6/site-packages/katsdpcontroller/generator.py
