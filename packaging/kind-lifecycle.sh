#!/usr/bin/env bash
set -euo pipefail

broker_image="${BROKER_IMAGE:-ghcr.io/krabka-io/krabka-broker@sha256:15851611a7d5df6e20d3f9bd85b3821ca2dd52d5ca6f4042f5a4cfe0a3a1ab89}"
operator_image="${OPERATOR_IMAGE:-krabka-operator:e2e}"
cluster="${KIND_CLUSTER:-krabka-operator-e2e}"
evidence="${EVIDENCE_DIR:-${TEST_TMPDIR:-/tmp}/krabka-operator-evidence}"
if [[ -n "${BUILD_WORKSPACE_DIRECTORY:-}" ]]; then
    root="${BUILD_WORKSPACE_DIRECTORY}"
elif [[ -n "${RUNFILES_DIR:-}" ]]; then
    root="${RUNFILES_DIR}/${TEST_WORKSPACE:-_main}"
else
    root="$(cd "$(dirname "$0")/.." && pwd)"
fi

for tool in docker kind kubectl helm jq sha256sum; do
    command -v "${tool}" >/dev/null || { echo "missing ${tool}" >&2; exit 1; }
done
docker info >/dev/null
mkdir -p "${evidence}" "${evidence}/crds"
chmod 0777 "${evidence}/crds"
cleanup() { kind delete cluster --name "${cluster}" >/dev/null 2>&1 || true; }
trap cleanup EXIT

docker image inspect "${operator_image}" >/dev/null
docker pull "${broker_image}"
kind create cluster --name "${cluster}" --wait 120s
kind load docker-image --name "${cluster}" "${operator_image}"
docker run --rm --user 0:0 -v "${evidence}/crds:/crds" "${operator_image}" gen-crds /crds
kubectl apply -f "${evidence}/crds"

broker_repository="${broker_image%@*}"
broker_digest="${broker_image#*@}"
helm install krabka-operator "${root}/charts/krabka-operator" \
    --namespace krabka-system --create-namespace --wait --timeout 5m \
    --set image.repository="${operator_image%:*}" \
    --set image.tag="${operator_image##*:}" \
    --set image.pullPolicy=Never \
    --set brokerImage.repository="${broker_repository}" \
    --set brokerImage.digest="${broker_digest}"

kubectl apply -f - <<EOF
apiVersion: krabka.io/v1alpha1
kind: Kafka
metadata:
  name: m20
spec:
  kafkaVersion: "4.0.0"
  metricsConfig: {}
---
apiVersion: krabka.io/v1alpha1
kind: KafkaNodePool
metadata:
  name: brokers
  labels:
    krabka.io/cluster: m20
spec:
  roles: [Controller, Broker]
  replicas: 3
  nodeIdStart: 0
EOF

kubectl wait --for=create statefulset/m20-brokers --timeout=2m
kubectl rollout status statefulset/m20-brokers --timeout=10m
kubectl wait kafka/m20 --for=jsonpath='{.status.conditions[?(@.type=="Ready")].status}'=True --timeout=10m
kubectl wait kafkanodepool/brokers --for=jsonpath='{.status.conditions[?(@.type=="Ready")].status}'=True --timeout=10m
kubectl get secret m20-operator-admin >/dev/null
broker_config="$(kubectl get configmap m20-broker-config -o jsonpath='{.data.broker-0\.toml}')"
grep -q 'name = "OPERATOR"' <<<"${broker_config}"
grep -q 'protocol = "Ssl"' <<<"${broker_config}"
grep -q 'client_auth = "Required"' <<<"${broker_config}"
! grep -q '"ANONYMOUS"' <<<"${broker_config}"

kubectl apply -f - <<EOF
apiVersion: krabka.io/v1alpha1
kind: KafkaTopic
metadata:
  name: secured-admin-before-rotation
  labels:
    krabka.io/cluster: m20
spec:
  partitions: 1
  replicas: 3
EOF
kubectl wait kafkatopic/secured-admin-before-rotation --for=jsonpath='{.status.conditions[?(@.type=="Ready")].status}'=True --timeout=5m

old_operator_cert="$(kubectl get secret m20-operator-admin -o jsonpath='{.data.user\.crt}')"
kubectl annotate kafka m20 krabka.io/force-replace-clients-ca-key="$(date -u +%FT%TZ)" --overwrite
for _ in $(seq 1 120); do
    new_operator_cert="$(kubectl get secret m20-operator-admin -o jsonpath='{.data.user\.crt}')"
    if [[ -n "${new_operator_cert}" && "${new_operator_cert}" != "${old_operator_cert}" ]]; then
        break
    fi
    sleep 5
done
[[ "${new_operator_cert:-}" != "${old_operator_cert}" ]]
kubectl wait kafka/m20 --for=jsonpath='{.status.conditions[?(@.type=="Ready")].status}'=True --timeout=10m
kubectl apply -f - <<EOF
apiVersion: krabka.io/v1alpha1
kind: KafkaTopic
metadata:
  name: secured-admin-after-rotation
  labels:
    krabka.io/cluster: m20
spec:
  partitions: 1
  replicas: 3
EOF
kubectl wait kafkatopic/secured-admin-after-rotation --for=jsonpath='{.status.conditions[?(@.type=="Ready")].status}'=True --timeout=5m
kubectl get kafka m20 -o json >"${evidence}/kafka.json"
kubectl get kafkanodepool brokers -o json >"${evidence}/pool.json"
kubectl get statefulset m20-brokers -o json >"${evidence}/statefulset.json"
kubectl get pods -o wide >"${evidence}/pods.txt"
kubectl logs -n krabka-system -l app.kubernetes.io/name=krabka-operator --all-containers >"${evidence}/operator.log"
printf '%s\n' "${old_operator_cert}" >"${evidence}/operator-cert-before.base64"
printf '%s\n' "${new_operator_cert}" >"${evidence}/operator-cert-after.base64"
(cd "${evidence}" && sha256sum kafka.json pool.json statefulset.json pods.txt operator.log operator-cert-*.base64 >SHA256SUMS)
echo "PASS: mTLS operator admin survived clients-CA credential rotation"
