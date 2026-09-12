# krabka-operator

The Kubernetes operator for [krabka](https://github.com/krabka-io): a Kafka
cluster, its node pools, listeners, topics, users, connectors, schema registry
and gRPC gateway, reconciled from custom resources.

## Custom resources

All in group `krabka.io`, version `v1alpha1`:

| Kind | What it declares |
| --- | --- |
| `Kafka` | A cluster: brokers, controllers, storage, tuning, authorization |
| `KafkaNodePool` | A pool of nodes with its own roles and resources |
| `KafkaTopic` | A topic and its configuration |
| `KafkaUser` | A principal, its ACLs, and its credentials |
| `KafkaListener` | An advertised listener and its authentication |
| `KafkaConnector` | A Connect connector |
| `KafkaRebalance` | A partition-reassignment plan |
| `KafkaSchemaRegistry` | A schema registry deployment |
| `KafkaGrpcGateway` | A gRPC gateway deployment |
| `KafkaClusterCa` | The cluster CA and its renewal policy |
| `KafkaLogging`, `KafkaMetrics`, `KafkaNetworkPolicy` | Cross-cutting policy |

The manifests live in
[`charts/krabka-operator/crds`](charts/krabka-operator/crds), one
`krabka.io_<plural>.yaml` per kind. Regenerate them from the Rust types with:

```bash
tools/regen-crds.sh
```

CI runs the same script and fails when the working tree changes, so a change to
a CRD type has to reach the manifests in the same commit.

## Chart

[`charts/krabka-operator`](charts/krabka-operator) installs the operator, its
RBAC, and the CRDs. Helm creates the files in `crds/` on install. It does not
touch them on upgrade, so a schema change needs a `kubectl apply` of that
directory.

## Scope

This operator reconciles Kafka. Gres — the Postgres-compatible layer — is not
here: its controllers reached about 21k lines of storage engine across
`gres-substrate`, `pgexec`, `pgkv` and `gres-ranges`, none of which have been
extracted, and it is under active development. It returns once those crates
land in the organisation.

The `Gres` and `GresTenant` manifests still ship in `charts/krabka-operator/crds`,
because the operator owns every CRD in the `krabka.io` group. This operator does
not reconcile those two kinds and has no Rust type for either, so
`tools/regen-crds.sh` leaves them untouched and the drift job does not cover
them. They come back under the generator with the Gres controllers.

## Layering

Depends on three sibling repositories, pinned by revision in
[`Cargo.toml`](Cargo.toml)'s `[patch.crates-io]`:

| Repository | What it supplies |
| --- | --- |
| [`krabka-protocol`](https://github.com/krabka-io/krabka-protocol) | Wire types, security, metadata, units |
| [`krabka-client-rs`](https://github.com/krabka-io/krabka-client-rs) | The admin, core and producer clients |
| [`krabka-broker`](https://github.com/krabka-io/krabka-broker) | Object store, kraft-core, logfmt |

## Build

```bash
cargo test --workspace
```

```bash
bazel test //...
```

Both are supported and both are gated in CI. Cargo stays the dependency source
of truth; Bazel reads the same `Cargo.toml` and `Cargo.lock`.
