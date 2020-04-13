#!/bin/bash
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
