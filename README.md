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

## Seccomp profile and io_uring

Broker pods use the `RuntimeDefault` seccomp profile of the container
runtime. A broker that writes `preallocate` segments with `O_DIRECT` sends
these writes through io_uring when the kernel permits it. When
`io_uring_setup` fails, the broker logs `no io_uring; direct writes use the
write threads` at info level and uses its write threads. Both paths are
correct. io_uring can be faster for small direct writes.

The default profiles of current runtimes block io_uring:

| Runtime | Versions checked | io_uring in the default profile |
| --- | --- | --- |
| containerd | 2.0.0 to 2.4.2 | Blocked, `EPERM` ([containerd#9320](https://github.com/containerd/containerd/pull/9320)) |
| containerd | 1.7.36 | Permitted. 1.7 is end of life, and the containerd support matrix does not list it for Kubernetes 1.36 or later |
| CRI-O | 1.34.15 to 1.37.2 | Blocked, `ENOSYS` (profile from `containers/common`) |
| Docker | 25.0.0 and later | Blocked, `EPERM` ([moby#46762](https://github.com/moby/moby/pull/46762)) |

io_uring adds kernel attack surface, so the operator does not change the
default. To let the brokers of one pool use io_uring, give the pool a
`Localhost` profile:

```yaml
apiVersion: krabka.io/v1alpha1
kind: KafkaNodePool
spec:
  template:
    seccompProfile:
      type: Localhost
      localhostProfile: operator/krabka-io-uring.json
```

`localhostProfile` is a path relative to the seccomp directory of the
kubelet, `/var/lib/kubelet/seccomp` by default. The file must be on every
node that can run a pod of the pool. If the file is missing, the pod does
not start. The operator does not write files on nodes.

Use the profile that krabka-broker ships,
[`packaging/k8s/profiles/krabka-io-uring.json`](https://github.com/krabka-io/krabka-broker/blob/main/packaging/k8s/profiles/krabka-io-uring.json).
It is the containerd 2.4.2 `RuntimeDefault` profile for a container with
no capabilities, in OCI format, plus one rule that permits
`io_uring_setup`, `io_uring_enter` and `io_uring_register`. Install it on
each node with your node configuration tool, or with the
[Security Profiles Operator](https://github.com/kubernetes-sigs/security-profiles-operator).
A Security Profiles Operator `SeccompProfile` gives the path for the pod in
its `status.localhostProfile`, for example `operator/krabka-io-uring.json`.

Do not use a Docker-format profile (with `archMap`, `includes` or
`excludes`) as a `Localhost` profile. containerd reads the file as an OCI
`LinuxSeccomp` and ignores those fields, so rules that need a capability
become rules that always permit the call.

On Linux 6.6 and later, the `kernel.io_uring_disabled` sysctl can also
block io_uring for every process on the node.

## Scope

This operator reconciles Kafka. Gres — the Postgres-compatible layer — is not
here: its controllers reached about 21k lines of storage engine across
`gres-substrate`, `pgexec`, `pgkv` and `gres-ranges`, none of which have been
extracted, and it is under active development. It returns once those crates
land in the organisation.

The chart does not install the `Gres` and `GresTenant` CRDs. They come back,
from the generator, with the Gres controllers.

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
