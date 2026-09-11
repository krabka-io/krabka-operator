#!/usr/bin/env bash
set -euo pipefail

broker_image="${BROKER_IMAGE:-krabka-io/krabka-broker:dev}"
rebalancer_image="${REBALANCER_IMAGE:-crabka-rebalancer:m20}"
rebalancer_chart="${REBALANCER_CHART:?set REBALANCER_CHART to the crabka-rebalancer chart}"
operator_image="${OPERATOR_IMAGE:-krabka-operator:e2e}"
kafka_tools_image="mirror.gcr.io/apache/kafka:4.0.0@sha256:01b9a4030e54c6068e66eb3ba4cb82c0d89238629ef1c30d79b86036bf89b1b7"
cluster="${KIND_CLUSTER:-krabka-operator-e2e}"
evidence="${EVIDENCE_DIR:-${TEST_TMPDIR:-/tmp}/krabka-operator-evidence}"
if [[ -n "${BUILD_WORKSPACE_DIRECTORY:-}" ]]; then
    root="${BUILD_WORKSPACE_DIRECTORY}"
elif [[ -n "${RUNFILES_DIR:-}" ]]; then
    root="${RUNFILES_DIR}/${TEST_WORKSPACE:-_main}"
else
    root="$(cd "$(dirname "$0")/.." && pwd)"
fi

for tool in docker kind kubectl helm jq sha256sum diff sort; do
    command -v "${tool}" >/dev/null || { echo "missing ${tool}" >&2; exit 1; }
done
docker info >/dev/null
mkdir -p "${evidence}" "${evidence}/crds"
chmod 0777 "${evidence}/crds"
workspace_version="$(sed -n 's/^version = "\([^"]*\)"/\1/p' "${root}/Cargo.toml" | head -n 1)"
[[ -n "${workspace_version}" ]]
helm package "${root}/charts/krabka-operator" --destination "${evidence}" \
    --version "${workspace_version}" --app-version "${workspace_version}"
helm package "${rebalancer_chart}" --destination "${evidence}" \
    --version "${workspace_version}" --app-version "${workspace_version}"

capture() {
    local exit_code=$?
    set +e
    if kubectl cluster-info >/dev/null 2>&1; then
        kubectl get kafka m20 -o json >"${evidence}/kafka.json"
        kubectl get kafkanodepools -o json >"${evidence}/pools.json"
        kubectl get kafkarebalances -o json >"${evidence}/rebalances.json"
        kubectl get kafkatopics -o json >"${evidence}/topics.json"
        kubectl get statefulsets -o json >"${evidence}/statefulsets.json"
        kubectl get persistentvolumeclaims -o json >"${evidence}/pvcs.json"
        kubectl get pods -A -o wide >"${evidence}/pods.txt"
        kubectl get events -A --sort-by=.lastTimestamp >"${evidence}/events.txt"
        kubectl logs -n krabka-system -l app.kubernetes.io/name=krabka-operator \
            --all-containers --prefix >"${evidence}/operator.log"
        kubectl logs -l app.kubernetes.io/name=crabka-rebalancer \
            --all-containers --prefix >"${evidence}/rebalancer.log"
    fi
    jq -n \
        --arg operator_revision "${OPERATOR_REVISION:-local}" \
        --arg broker_revision "${BROKER_REVISION:-local}" \
        --arg rebalancer_revision "${REBALANCER_REVISION:-local}" \
        --arg broker_image "${broker_image}" \
        --arg rebalancer_image "${rebalancer_image}" \
        --arg operator_image "${operator_image}" \
        --argjson exit_code "${exit_code}" \
        '{operator_revision: $operator_revision, broker_revision: $broker_revision,
          rebalancer_revision: $rebalancer_revision,
          images: {broker: $broker_image, rebalancer: $rebalancer_image,
                   operator: $operator_image},
          exit_code: $exit_code}' >"${evidence}/revisions.json"
    find "${evidence}" -type f ! -name SHA256SUMS -printf '%P\0' | sort -z |
        (cd "${evidence}" && xargs -0 sha256sum) >"${evidence}/SHA256SUMS"
    kind delete cluster --name "${cluster}" >/dev/null 2>&1
    exit "${exit_code}"
}
trap capture EXIT

for image in "${operator_image}" "${broker_image}" \
        "${rebalancer_image}" "${kafka_tools_image}"; do
    docker image inspect "${image}" >/dev/null 2>&1 || docker pull "${image}"
done
kind create cluster --name "${cluster}" --wait 120s
for image in "${operator_image}" "${broker_image}" \
        "${rebalancer_image}" "${kafka_tools_image}"; do
    if [[ "${image}" != *@sha256:* ]]; then
        kind load docker-image --name "${cluster}" "${image}"
    fi
done
docker run --rm --user 0:0 -v "${evidence}/crds:/crds" "${operator_image}" gen-crds /crds
kubectl apply -f "${evidence}/crds"

broker_repository="${broker_image%@*}"
broker_digest="${broker_image#*@}"
broker_values=(--set brokerImage.repository="${broker_repository}")
if [[ "${broker_image}" == *@sha256:* ]]; then
    broker_values+=(--set brokerImage.digest="${broker_digest}")
else
    broker_values+=(--set brokerImage.repository="${broker_image%:*}")
    broker_values+=(--set brokerImage.tag="${broker_image##*:}")
    broker_values+=(--set brokerImage.digest="")
fi
helm install krabka-operator "${root}/charts/krabka-operator" \
    --namespace krabka-system --create-namespace --wait --timeout 5m \
    --set image.repository="${operator_image%:*}" \
    --set image.tag="${operator_image##*:}" \
    --set image.pullPolicy=Never \
    "${broker_values[@]}"

kubectl apply -f - <<EOF
apiVersion: krabka.io/v1alpha1
kind: Kafka
metadata:
  name: m20
spec:
  kafkaVersion: "3.9.0"
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
  replicas: 4
  nodeIdStart: 0
  image: "${broker_image}"
  storage:
    type: PersistentClaim
    size: 256Mi
    deleteClaim: true
EOF

kubectl wait --for=create statefulset/m20-brokers --timeout=3m
kubectl rollout status statefulset/m20-brokers --timeout=12m
kubectl wait kafkanodepool/brokers \
    --for=jsonpath='{.status.conditions[?(@.type=="Ready")].status}'=True --timeout=10m
kubectl wait kafka/m20 --for=jsonpath='{.status.conditions[?(@.type=="Ready")].status}'=True --timeout=10m
[[ "$(kubectl get kafka m20 -o jsonpath='{.status.metadataVersion}')" == "3.9" ]]

kubectl run kafka-tools --image="${kafka_tools_image}" --restart=Never --command -- sleep 7200
kubectl wait --for=condition=Ready pod/kafka-tools --timeout=3m
kubectl apply -f - <<'EOF'
apiVersion: krabka.io/v1alpha1
kind: KafkaTopic
metadata:
  name: lifecycle
  labels:
    krabka.io/cluster: m20
spec:
  partitions: 12
  replicas: 3
  config:
    min.insync.replicas: "2"
EOF
kubectl wait kafkatopic/lifecycle \
    --for=jsonpath='{.status.conditions[?(@.type=="Ready")].status}'=True --timeout=5m
kubectl exec kafka-tools -- /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server m20-broker-headless:9092 --topic lifecycle --describe \
    >"${evidence}/assignments-before.txt"
grep -Eq 'Replicas:.*3' "${evidence}/assignments-before.txt"

traffic_pid=
traffic_json=
traffic_prefix=0
start_traffic() {
    local phase=$1 count=$2
    local prefix=$traffic_prefix
    traffic_prefix=$((traffic_prefix + 1))
    traffic_json="${evidence}/${phase}-producer.jsonl"
    kubectl exec kafka-tools -- /opt/kafka/bin/kafka-verifiable-producer.sh \
        --bootstrap-server m20-broker-headless:9092 --topic lifecycle \
        --max-messages "${count}" --throughput 10 --acks -1 \
        --value-prefix "${prefix}" \
        >"${traffic_json}" 2>"${evidence}/${phase}-producer.log" &
    traffic_pid=$!
}
finish_traffic() {
    local exit_code=0 acknowledged
    wait "${traffic_pid}" || exit_code=$?
    printf '%s\n' "${exit_code}" >"${traffic_json%.jsonl}-exit-code.txt"
    acknowledged="$(jq -s '[.[] | select(.name == "producer_send_success")] | length' "${traffic_json}")"
    ((acknowledged > 0))
    jq -r 'select(.name == "producer_send_success") | [.partition, .offset, .value] | @tsv' \
        "${traffic_json}" >>"${evidence}/acknowledged-offsets.tsv"
}

wait_rollout() {
    local pool=$1 minimum_ready=$2 expected_image=$3 old_revision=${4:-} deadline=$((SECONDS + 900))
    while ((SECONDS < deadline)); do
        read -r desired ready updated current_revision update_revision image < <(
            kubectl get "statefulset/m20-${pool}" -o json | jq -r '[
                .spec.replicas, (.status.readyReplicas // 0), (.status.updatedReplicas // 0),
                (.status.currentRevision // "missing"), (.status.updateRevision // "missing"),
                .spec.template.spec.containers[0].image
            ] | @tsv'
        )
        printf '%s\t%s\t%s\t%s\t%s\t%s\n' "$(date -u +%FT%TZ)" "${pool}" \
            "${desired}" "${ready}" "${current_revision}" "${update_revision}" \
            >>"${evidence}/rollout-ledger.tsv"
        ((ready >= minimum_ready))
        if [[ "${image}" == "${expected_image}" \
                && ( -z "${old_revision}" || "${update_revision}" != "${old_revision}" ) ]]; then
            if [[ "${ready}" == "${desired}" && "${updated}" == "${desired}" \
                && "${current_revision}" == "${update_revision}" ]]; then
                return
            fi
        fi
        sleep 2
    done
    echo "timed out waiting for ${pool} rollout" >&2
    return 1
}

start_traffic upgrade 900
pre_upgrade_revision="$(kubectl get statefulset m20-brokers -o jsonpath='{.status.currentRevision}')"
kubectl patch kafka m20 --type=merge -p '{"spec":{"kafkaVersion":"4.0.0"}}'
for _ in $(seq 1 180); do
    kafka_status="$(kubectl get kafka m20 -o json)"
    if [[ "$(jq -r '.status.metadataVersion // ""' <<<"${kafka_status}")" == "3.9" \
            && "$(jq -r '.status.targetMetadataVersion // ""' <<<"${kafka_status}")" == "4.0" \
            && "$(jq -r '.status.conditions[] | select(.type == "KafkaVersionUpgrade") | .reason' <<<"${kafka_status}")" == "RollingImages" ]]; then
        printf '%s\n' "${kafka_status}" >"${evidence}/metadata-before-finalization.json"
        break
    fi
    sleep 1
done
[[ -s "${evidence}/metadata-before-finalization.json" ]]
wait_rollout brokers 3 "${broker_image}" "${pre_upgrade_revision}"
finish_traffic
kubectl wait kafka/m20 --for=jsonpath='{.status.conditions[?(@.type=="KafkaVersionUpgrade")].reason}'=Finalized --timeout=10m
[[ "$(kubectl get kafka m20 -o jsonpath='{.status.metadataVersion}')" == "4.0" ]]
kubectl get kafka m20 -o json >"${evidence}/metadata-after-finalization.json"

start_traffic disruption 450
old_uid="$(kubectl get pod m20-brokers-0 -o jsonpath='{.metadata.uid}')"
kubectl delete pod m20-brokers-0 --wait=false
for _ in $(seq 1 300); do
    new_uid="$(kubectl get pod m20-brokers-0 --ignore-not-found -o jsonpath='{.metadata.uid}')"
    [[ -n "${new_uid}" && "${new_uid}" != "${old_uid}" ]] && break
    sleep 1
done
[[ -n "${new_uid:-}" && "${new_uid}" != "${old_uid}" ]]
kubectl wait pod/m20-brokers-0 --for=condition=Ready --timeout=10m
finish_traffic
printf 'old_uid=%s\nnew_uid=%s\n' "${old_uid}" "${new_uid}" >"${evidence}/disruption.txt"

kubectl create secret generic m20-rebalancer-auth --from-literal=token=m20-evacuation-token
rebalancer_repository="${rebalancer_image%@*}"
rebalancer_digest="${rebalancer_image#*@}"
rebalancer_values=(--set image.repository="${rebalancer_repository}")
if [[ "${rebalancer_image}" == *@sha256:* ]]; then
    rebalancer_values+=(--set image.digest="${rebalancer_digest}")
else
    rebalancer_values+=(--set image.repository="${rebalancer_image%:*}")
    rebalancer_values+=(--set image.tag="${rebalancer_image##*:}")
    rebalancer_values+=(--set image.pullPolicy=Never)
fi
helm install m20-rebalancer "${rebalancer_chart}" --wait --timeout 5m \
    --set fullnameOverride=m20-rebalancer \
    --set bootstrapServers=m20-broker-headless.default.svc.cluster.local:9092 \
    --set brokerEvacuationAuth.existingSecret=m20-rebalancer-auth \
    --set detector.tickIntervalSecs=0 \
    "${rebalancer_values[@]}"

start_traffic scale-down 1350
kubectl patch kafkanodepool/brokers --type=merge -p '{"spec":{"replicas":3}}'
kubectl wait --for=create kafkarebalance/m20-brokers-drain-to-3 --timeout=3m
[[ "$(kubectl get statefulset m20-brokers -o jsonpath='{.spec.replicas}')" == "4" ]]
kubectl get pod m20-brokers-3 >/dev/null
kubectl get pvc data-m20-brokers-3 >/dev/null
kubectl wait kafkarebalance/m20-brokers-drain-to-3 \
    --for=jsonpath='{.status.conditions[?(@.type=="Ready")].status}'=True --timeout=12m
kubectl rollout status statefulset/m20-brokers --timeout=10m
kubectl wait kafkanodepool/brokers \
    --for=jsonpath='{.status.conditions[?(@.type=="Ready")].status}'=True --timeout=10m
kubectl wait --for=delete pod/m20-brokers-3 --timeout=5m
kubectl wait --for=delete pvc/data-m20-brokers-3 --timeout=5m
kubectl exec kafka-tools -- /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server m20-broker-headless:9092 --topic lifecycle --describe \
    >"${evidence}/assignments-after-evacuation.txt"
! grep -Eq 'Replicas:.*3' "${evidence}/assignments-after-evacuation.txt"
[[ "$(kubectl get kafkanodepool brokers -o jsonpath='{.metadata.annotations.krabka\.io/unregistered-brokers}')" == "3" ]]
finish_traffic

served_cert() {
    kubectl exec kafka-tools -- keytool -printcert \
        -sslserver m20-brokers-0.m20-broker-headless.default.svc.cluster.local:9091 2>/dev/null |
        sed -n 's/^[[:space:]]*SHA256: //p' | head -n 1
}
start_traffic certificate-rotation 900
old_cert="$(served_cert)"
[[ -n "${old_cert}" ]]
old_secret_cert="$(kubectl get secret m20-kafka-brokers -o jsonpath='{.data.0\.crt}')"
old_broker_revision="$(kubectl get statefulset m20-brokers -o jsonpath='{.status.currentRevision}')"
kubectl patch secret m20-kafka-brokers --type=json -p='[{"op":"remove","path":"/data/0.crt"}]'
for _ in $(seq 1 300); do
    new_secret_cert="$(kubectl get secret m20-kafka-brokers -o jsonpath='{.data.0\.crt}' 2>/dev/null || true)"
    [[ -n "${new_secret_cert}" && "${new_secret_cert}" != "${old_secret_cert}" ]] && break
    sleep 1
done
[[ -n "${new_secret_cert:-}" && "${new_secret_cert}" != "${old_secret_cert}" ]]
wait_rollout brokers 2 "${broker_image}" "${old_broker_revision}"
new_cert="$(served_cert)"
[[ -n "${new_cert}" && "${new_cert}" != "${old_cert}" ]]
printf 'before=%s\nafter=%s\n' "${old_cert}" "${new_cert}" >"${evidence}/served-certificates.txt"
kubectl apply -f - <<'EOF'
apiVersion: krabka.io/v1alpha1
kind: KafkaTopic
metadata:
  name: after-leaf-rotation
  labels:
    krabka.io/cluster: m20
spec:
  partitions: 1
  replicas: 3
EOF
kubectl wait kafkatopic/after-leaf-rotation \
    --for=jsonpath='{.status.conditions[?(@.type=="Ready")].status}'=True --timeout=5m
finish_traffic

record_count="$(wc -l <"${evidence}/acknowledged-offsets.tsv")"
kubectl exec kafka-tools -- /opt/kafka/bin/kafka-get-offsets.sh \
    --bootstrap-server m20-broker-headless:9092 --topic lifecycle --time latest \
    >"${evidence}/final-offsets.txt"
stored_count="$(awk -F: '{ total += $3 } END { print total + 0 }' "${evidence}/final-offsets.txt")"
((stored_count >= record_count))
kubectl exec kafka-tools -- /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server m20-broker-headless:9092 --topic lifecycle \
    --from-beginning --max-messages "${stored_count}" --timeout-ms 120000 \
    --property print.partition=true --property print.offset=true \
    >"${evidence}/consumed-with-offsets.txt" 2>"${evidence}/consumer.log"
sed -n -E $'s/^Partition:([0-9-]+)\\tOffset:([0-9-]+)\\t(.*)$/\\1\\t\\2\\t\\3/p' \
    "${evidence}/consumed-with-offsets.txt" >"${evidence}/consumed-offsets.tsv"
[[ "$(wc -l <"${evidence}/consumed-offsets.tsv")" == "${stored_count}" ]]
LC_ALL=C sort "${evidence}/acknowledged-offsets.tsv" >"${evidence}/acknowledged-offsets.sorted"
LC_ALL=C sort "${evidence}/consumed-offsets.tsv" >"${evidence}/consumed-offsets.sorted"
comm -23 "${evidence}/acknowledged-offsets.sorted" "${evidence}/consumed-offsets.sorted" \
    >"${evidence}/missing-acknowledged-records.txt"
[[ ! -s "${evidence}/missing-acknowledged-records.txt" ]]

echo "PASS: RF=3/minISR=2 survived upgrade, disruption, broker evacuation and leaf rotation; ${record_count} acknowledged records reconciled"
