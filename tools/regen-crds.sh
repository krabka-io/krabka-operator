#!/usr/bin/env bash
# Regenerate the CRD manifests that the chart installs.
#
# The `#[derive(CustomResource)]` types in crates/krabka-operator/src/crd are
# the source of truth. `src/gen_crds.rs` writes one `<group>_<plural>.yaml` per
# kind into the output directory. CI runs this script and fails when the
# working tree changes -- see the `crd drift` job in .github/workflows/ci.yml.
#
# The script overwrites the manifests of the kinds this operator defines. It
# does not delete other files in the directory, so krabka.io_greses.yaml and
# krabka.io_grestenants.yaml survive. Those two kinds have no Rust type in this
# repository yet; the chart installs them, and this script does not keep them
# in step. See the Scope section of the README.
set -euo pipefail

root="${BUILD_WORKSPACE_DIRECTORY:-$(cd "$(dirname "$0")/.." && pwd)}"
out="${root}/charts/krabka-operator/crds"

bazel run //:krabka-operator -- gen-crds "${out}"
echo "Regenerated. Review the diff with: git diff ${out}"
