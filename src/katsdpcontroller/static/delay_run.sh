#!/bin/bash

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
