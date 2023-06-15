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

# Usage: delay_run.sh url command [args...]
# Request the URL. If return status is 200 (OK) then exec the command.
# Otherwise, abort with an error.

set -e -u

code="$(curl --silent --show-error --output /dev/stderr --write-out '%{http_code}\n' -- "$1")"
if [ "$code" != "200" ]; then
    exit 1
fi
shift    # Shift away the URL
exec "$@"
