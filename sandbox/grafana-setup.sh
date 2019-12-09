#!/bin/bash
set -e -u

trap 'echo Failed' ERR

echo 'Installing plotly plugin'
docker-compose exec grafana grafana-cli plugins install natel-plotly-panel 0.0.6
docker-compose restart grafana
