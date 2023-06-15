#!/bin/bash

################################################################################
# Copyright (c) 2013-2023, National Research Foundation (SARAO)
#
# Licensed under the BSD 3-Clause License (the "License"); you may not use
# this file except in compliance with the License. You may obtain a copy
# of the License at
#
#   https://opensource.org/licenses/BSD-3-Clause
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

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
