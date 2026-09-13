#!/usr/bin/env bash
# Check that the Bazel-built operator reports the workspace version.
#
# Argument 1 is the operator binary. Argument 2 is the root Cargo.toml.
# //packaging:version_test passes both.
set -euo pipefail

binary="$1"
manifest="$2"

expected="krabka-operator $(sed -n 's/^version = "\([^"]*\)"/\1/p' "${manifest}" | head -n 1)"
actual="$("${binary}" --version)"

if [[ "${actual}" != "${expected}" ]]; then
    echo "version_test: the binary reports '${actual}', Cargo.toml says '${expected}'." >&2
    echo "  Set WORKSPACE_VERSION in //bazel:defs.bzl to the Cargo.toml version." >&2
    exit 1
fi

echo "version_test: ${actual}"
