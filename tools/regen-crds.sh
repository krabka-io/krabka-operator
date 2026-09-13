#!/usr/bin/env bash
# Regenerate the CRD manifests that the chart installs.
#
# The `#[derive(CustomResource)]` types in crates/krabka-operator/src/crd are
# the source of truth. `src/gen_crds.rs` writes one `<group>_<plural>.yaml` per
# kind into the output directory. CI runs this script and fails when the
# working tree changes -- see the `crd drift` job in .github/workflows/ci.yml.
#
# The script deletes every manifest in the directory before it generates. The
# directory then holds exactly what the generator writes. A manifest for a kind
# that no longer has a Rust type shows as a deleted file, and the drift job
# fails on it.
set -euo pipefail

root="${BUILD_WORKSPACE_DIRECTORY:-$(cd "$(dirname "$0")/.." && pwd)}"
out="${root}/charts/krabka-operator/crds"

find "${out}" -maxdepth 1 -type f -name '*.yaml' -delete
bazel run //:krabka-operator -- gen-crds "${out}"
echo "Regenerated. Review the diff with: git diff ${out}"
