#!/usr/bin/env bash
set -euo pipefail

broker_image="${BROKER_IMAGE:-ghcr.io/krabka-io/krabka-broker@sha256:886fbe511a0cadacec0c352fe10b295063b3807c0df133d2fce4ea804f4fffcd}"
operator_image="${OPERATOR_IMAGE:-krabka-operator:e2e}"
cluster="${KIND_CLUSTER:-krabka-operator-e2e}"
evidence="${EVIDENCE_DIR:-${TEST_TMPDIR:-/tmp}/krabka-operator-evidence}"
root="${BUILD_WORKSPACE_DIRECTORY:-$(cd "$(dirname "$0")/.." && pwd)}"

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
kubectl get kafka m20 -o json >"${evidence}/kafka.json"
kubectl get kafkanodepool brokers -o json >"${evidence}/pool.json"
kubectl get statefulset m20-brokers -o json >"${evidence}/statefulset.json"
kubectl get pods -o wide >"${evidence}/pods.txt"
kubectl logs -n krabka-system deployment/krabka-operator >"${evidence}/operator.log"
(cd "${evidence}" && sha256sum kafka.json pool.json statefulset.json pods.txt operator.log >SHA256SUMS)
echo "PASS: operator-backed Kind lifecycle reconciled three pinned-image brokers"
