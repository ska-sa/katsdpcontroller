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

set -e -u

dependencies="$(scripts/docker_label_helper.py dependencies)"
product_configure_versions="$(scripts/docker_label_helper.py product_configure_versions)"
exec docker build --build-arg "dependencies=$dependencies" --build-arg "product_configure_versions=$product_configure_versions" "$@"
