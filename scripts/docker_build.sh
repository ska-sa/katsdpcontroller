#!/bin/bash
set -e -u

dependencies="$(scripts/sdpmc_dependencies.py)"
exec docker build --build-arg "dependencies=$dependencies" "$@"
