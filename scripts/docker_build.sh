#!/bin/bash
set -e -u

dependencies="$(scripts/docker_label_helper.py dependencies)"
product_configure_versions="$(scripts/docker_label_helper.py product_configure_versions)"
exec docker build --build-arg "dependencies=$dependencies" --build-arg "product_configure_versions=$product_configure_versions" "$@"
