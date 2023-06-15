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

trap 'echo Failed' ERR

echo "Setting Elasticsearch pipeline"
curl -s --fail -X PUT -H 'Content-type: application/json' -d '@etc/elk/elasticsearch-pipeline.json' 'http://127.0.0.1:9200/_ingest/pipeline/timestamp_precise' > /dev/null
echo "Setting Kibana settings"
curl -s --fail -X POST -H 'Content-type: application/json' -H 'kbn-xsrf: true' -d '@etc/elk/kibana-settings.json' 'http://127.0.0.1:5601/api/saved_objects/config/7.3.1?overwrite=true' > /dev/null
echo "Setting Kibana index pattern"
curl -s --fail -X POST -H 'Content-type: application/json' -H 'kbn-xsrf: true' -d '@etc/elk/kibana-index-pattern.json' 'http://127.0.0.1:5601/api/saved_objects/index-pattern/logstash?overwrite=true' > /dev/null
echo "Setting Kibana search"
curl -s --fail -X POST -H 'Content-type: application/json' -H 'kbn-xsrf: true' -d '@etc/elk/sdp-messages.json' 'http://127.0.0.1:5601/api/saved_objects/search/sdp-messages?overwrite=true' > /dev/null

echo
echo "Success"
