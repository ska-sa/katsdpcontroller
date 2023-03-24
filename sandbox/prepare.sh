#!/bin/bash
set -e

dir="$(realpath $(dirname $0))"
mkdir -p "$dir/logs"
PYTHONPATH="$dir/../src:$PYTHONPATH" python -m katsdpcontroller.agent_mkconfig \
    --attributes-dir "$dir/etc/mesos-agent/attributes" \
    --resources-dir "$dir/etc/mesos-agent/resources" \
    --network "lo:cbf" \
    --network "lo:sdp_10g" \
    --volume "data:$dir/data" \
    --volume "data_local:$dir/data" \
    --volume "obj_data:$dir/data" \
    --volume "bf_ram0:$dir/data" \
    --volume "bf_ram1:$dir/data" \
    --volume "config:$dir/config"
chmod a+w "$dir/data" "$dir/config" "$dir/logs"
