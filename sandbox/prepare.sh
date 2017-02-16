#!/bin/bash
set -e

cbf_interface="$(ip -o route get 239.9.3.1 | awk '{print $4}')"
sdp_interface="$(ip -o route get 225.100.1.1 | awk '{print $4}')"
dir="$(realpath $(dirname $0))"
"$dir/../scripts/agent_mkconfig.py" \
    --attributes-dir "$dir/mesos-slave/attributes" \
    --resources-dir "$dir/mesos-slave/resources" \
    --network "$cbf_interface:cbf" \
    --network "$sdp_interface:sdp_10g" \
    --volume "data:$dir/data" \
    --volume "data_local:$dir/data" \
    --volume "config:$dir/config"
chmod a+w "$dir/data" "$dir/config"
