#!/bin/bash
set -e -u

dependencies="$(scripts/sdpmc_dependencies.py)"
exec docker build --build-arg 'dependencies_set=yes' --label "za.ac.kat.sdp.image-depends=$dependencies" "$@"
