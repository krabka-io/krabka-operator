# krabka-operator chart

This chart installs the krabka operator, its RBAC and the CRDs. The
[repository README](../../README.md#chart) tells how Helm handles the CRDs,
and [`values.yaml`](values.yaml) describes each value.

## Broker seccomp profile and io_uring

A broker sends the `O_DIRECT` writes of `preallocate=true` segments through an
io_uring on its `log-direct-uring` thread
([krabka-broker#1582](https://github.com/krabka-io/krabka-broker/pull/1582)).
If the kernel refuses the io_uring, the broker logs
`no io_uring; direct writes use the write threads` at info level and uses its
write threads. A broker with `preallocate=false`, the default, does not use
io_uring.

The operator gives the broker pods the `RuntimeDefault` seccomp profile. The
default profiles of containerd 2.0 and later and of CRI-O refuse
`io_uring_setup`. The default profile of containerd 1.7 allows it. The
[krabka-broker deploy guide](https://github.com/krabka-io/krabka-broker/blob/main/docs/operations/deploy.md#seccomp-and-io_uring)
gives the sources. That section comes with
[krabka-broker#1596](https://github.com/krabka-io/krabka-broker/pull/1596).

[`files/krabka-io-uring.json`](files/krabka-io-uring.json) is a profile that
allows io_uring: containerd v2.4.2's `RuntimeDefault` profile for a container
with no capabilities, plus `io_uring_setup`, `io_uring_enter` and
`io_uring_register`. It is a copy of
`packaging/k8s/profiles/krabka-io-uring.json` in krabka-broker. Change the two
files together.

### Install the profile with the Security Profiles Operator

If the cluster runs the
[Security Profiles Operator](https://github.com/kubernetes-sigs/security-profiles-operator)
v1.0.0 or later, the chart can create the profile as a `SeccompProfile`:

```yaml
brokerSeccompProfile:
  securityProfilesOperator:
    enabled: true
    name: krabka-io-uring
```

The `SeccompProfile` is cluster-scoped, so give each release that enables it
its own `name`. The Security Profiles Operator writes the file on each node.
Its `status.localhostProfile` gives the path for a pod:

```
kubectl get seccompprofile krabka-io-uring --output wide
```

For the default name the path is `operator/krabka-io-uring.json`. Without the
Security Profiles Operator, copy the file to the kubelet seccomp directory of
each node, `/var/lib/kubelet/seccomp` by default. The path for a pod is then
relative to that directory.

### Plan: select the profile for broker pods

This value only installs the profile. The operator does not yet have a setting
that gives the broker pods a `Localhost` profile: it always sets
`seccompProfile.type: RuntimeDefault` in the pods that it creates. A change to
the operator is in progress that adds a field to the `KafkaNodePool` pod
template for the seccomp profile, with the types `RuntimeDefault` and
`Localhost`. When that change merges, add an example here of a
`KafkaNodePool` that uses `operator/krabka-io-uring.json`. The field belongs
to each pool, so it is not a chart value.

The operator pod itself does not use io_uring. Its `securityContext` keeps
`RuntimeDefault`.

### Check the result

Let the broker open a `preallocate=true` segment: create a topic with
`preallocate=true` and produce one record to it. Then:

- `kubectl logs <broker pod> | grep 'no io_uring'` prints nothing when
  io_uring works.
- On the node, `grep -l log-direct-uring /proc/<broker pid>/task/*/comm`
  prints one path when io_uring works. The broker image has no shell, so look
  from the node. `crictl inspect --output go-template --template '{{.info.pid}}' <container id>`
  gives the process id.
