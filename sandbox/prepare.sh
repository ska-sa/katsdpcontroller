#!/bin/bash
set -e

dir="$(realpath $(dirname $0))"
"$dir/../scripts/agent_mkconfig.py" \
    --attributes-dir "$dir/etc/mesos-slave/attributes" \
    --resources-dir "$dir/etc/mesos-slave/resources" \
    --network "lo:cbf" \
    --network "lo:sdp_10g" \
    --volume "data:$dir/data" \
    --volume "data_local:$dir/data" \
    --volume "obj_data:$dir/data" \
    --volume "config:$dir/config"
chmod a+w "$dir/data" "$dir/config"
