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
data_root="${KATSDP_SANDBOX_DATA_ROOT:-$dir/data}"
config_root="${KATSDP_SANDBOX_CONFIG_ROOT:-$dir/config}"
mkdir -p "$dir/logs" "$data_root" "$config_root"
echo "Using sandbox data root: $data_root"
echo "Using sandbox config root: $config_root"
if [[ -n "${PYTHON:-}" ]]; then
    python_cmd="$PYTHON"
elif [[ -x "$dir/sandbox_venv/bin/python" ]]; then
    python_cmd="$dir/sandbox_venv/bin/python"
elif command -v python >/dev/null 2>&1; then
    python_cmd=python
else
    python_cmd=python3
fi
echo "Using Python interpreter: $python_cmd"
PYTHONPATH="$dir/../src:$PYTHONPATH" "$python_cmd" -m katsdpcontroller.agent_mkconfig \
    --attributes-dir "$dir/etc/mesos-agent/attributes" \
    --resources-dir "$dir/etc/mesos-agent/resources" \
    --network "lo:cbf" \
    --network "lo:gpucbf" \
    --network "lo:sdp_10g" \
    --volume "data:$data_root" \
    --volume "data_local:$data_root" \
    --volume "obj_data:$data_root" \
    --volume "bf_ram0:$data_root" \
    --volume "bf_ram1:$data_root" \
    --volume "config:$config_root"
if [[ -n "${KATSDP_SANDBOX_CPUS:-}" ]]; then
    echo "Overriding Mesos CPU resource: ${KATSDP_SANDBOX_CPUS}"
    echo "${KATSDP_SANDBOX_CPUS}" > "$dir/etc/mesos-agent/resources/cpus"
fi
for path in "$data_root" "$config_root" "$dir/logs"; do
    if [[ -O "$path" ]]; then
        chmod a+w "$path"
    elif [[ ! -w "$path" ]]; then
        echo "Error: path is not writable: $path" >&2
        exit 1
    else
        echo "Path not owned; skipping chmod: $path"
    fi
done
