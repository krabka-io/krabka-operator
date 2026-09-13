#!/usr/bin/env bash
# Load the operator image into the local Docker daemon and run its entrypoint.
#
# Argument 1 is the `image_load` runner. Argument 2 is the tag that it loads
# the image under. //packaging:image_docker_test passes both.
set -euo pipefail

loader="$1"
tag="$2"

if ! docker info >/dev/null 2>&1; then
    echo "image_docker_test: no reachable Docker daemon" >&2
    exit 1
fi

"${loader}"

# The image entrypoint with no override: what a bare `docker run` starts.
if ! docker run --rm "${tag}" --help >/dev/null; then
    echo "image_docker_test: the entrypoint of ${tag} did not answer --help" >&2
    exit 1
fi

echo "image_docker_test: /usr/bin/krabka-operator --help ran in ${tag}"
