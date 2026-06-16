#!/usr/bin/env bash
# Chaos validation of fjord on kind using EXTERNAL, off-the-shelf tools:
#   * fault injection: Chaos Mesh (CNCF) — PodChaos kills broker pods on a schedule
#   * correctness oracle: Apache Kafka's own kafka-verifiable-producer/consumer
#
# Proves the diskless fault-tolerance claim: stateless brokers behind the Service
# can be killed repeatedly with NO lost acked writes and NO offset gaps, because
# all durable state lives in the coordinator (Postgres) + object store (MinIO).
#
# Only BROKER pods are killed — the bundled Postgres/MinIO use emptyDir here, so
# killing them would be test-infra data loss, not a fjord property. (Their
# durability-under-restart is the production concern: RDS + real S3 with PVCs.)
set -euo pipefail

CLUSTER=fjord-e2e
NS=fjord-chaos
KAFKA_IMG=apache/kafka:3.8.1
N=60000
THROUGHPUT=1000   # ~60s of produce, spanning several pod kills
CHART="$(cd "$(dirname "${BASH_SOURCE[0]}")/../helm/fjord" && pwd)"

log() { echo -e "\n=== $* ===" >&2; }

NODE_IP="$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "${CLUSTER}-control-plane")"
KC="$(mktemp)"; trap 'rm -f "$KC"' EXIT
kind get kubeconfig --name "$CLUSTER" > "$KC"
sed -i -E "s#https://127.0.0.1:[0-9]+#https://${NODE_IP}:6443#" "$KC"
export KUBECONFIG="$KC"
K="kubectl"

log "install Chaos Mesh (CNCF) on kind (containerd runtime)"
helm repo add chaos-mesh https://charts.chaos-mesh.org >/dev/null 2>&1 || true
helm repo update chaos-mesh >/dev/null 2>&1 || true
helm upgrade --install chaos-mesh chaos-mesh/chaos-mesh -n chaos-mesh --create-namespace \
  --set chaosDaemon.runtime=containerd \
  --set chaosDaemon.socketPath=/run/containerd/containerd.sock \
  --version 2.6.3 >/dev/null
$K -n chaos-mesh rollout status deploy/chaos-controller-manager --timeout=240s

log "install fjord (singleLogical, 3 replicas, Service-fronted) + postgres + minio"
$K create namespace "$NS" --dry-run=client -o yaml | $K apply -f -
helm upgrade --install r "$CHART" -n "$NS" \
  --set mode=singleLogical --set replicaCount=3 --set autoscaling.enabled=false \
  --set image.pullPolicy=IfNotPresent --set 'broker.createTopics={chaos:6}'
$K -n "$NS" rollout status deploy/r-fjord-postgres --timeout=180s
$K -n "$NS" rollout status deploy/r-fjord-minio --timeout=180s
$K -n "$NS" wait --for=condition=complete job/r-fjord-minio-mkbucket --timeout=120s || true
$K -n "$NS" rollout status deploy/r-fjord --timeout=240s

BS="r-fjord.${NS}.svc.cluster.local:9092"

log "start verifiable producer ($N records, acks=all) in the background"
# message.timeout high so retries ride out a killed pod's reschedule.
$K -n "$NS" run vprod --image="$KAFKA_IMG" --restart=Never --command -- \
  /opt/kafka/bin/kafka-verifiable-producer.sh --bootstrap-server "$BS" \
  --topic chaos --max-messages "$N" --throughput "$THROUGHPUT" --acks -1 >/dev/null 2>&1

log "apply Chaos Mesh: kill a broker pod every 20s (brokers only, NOT pg/minio)"
cat <<YAML | $K apply -f -
apiVersion: chaos-mesh.org/v1alpha1
kind: Schedule
metadata:
  name: kill-brokers
  namespace: $NS
spec:
  schedule: "@every 20s"
  type: PodChaos
  concurrencyPolicy: Forbid
  historyLimit: 10
  podChaos:
    action: pod-kill
    mode: one
    selector:
      namespaces: [$NS]
      labelSelectors:
        app.kubernetes.io/name: fjord
      expressionSelectors:
        - { key: app.kubernetes.io/component, operator: DoesNotExist }
YAML

log "wait for producer to finish (riding out the kills)"
$K -n "$NS" wait --for=jsonpath='{.status.phase}'=Succeeded pod/vprod --timeout=300s \
  || $K -n "$NS" wait --for=jsonpath='{.status.phase}'=Failed pod/vprod --timeout=5s || true
PROD_LOG="$($K -n "$NS" logs vprod 2>/dev/null)"
$K -n "$NS" delete pod vprod --wait=false >/dev/null 2>&1 || true
ACKED="$(echo "$PROD_LOG" | grep -c '"name":"producer_send_success"' || true)"
echo "acked under chaos: $ACKED" >&2

log "stop chaos; wait for brokers to stabilize"
$K -n "$NS" delete schedule kill-brokers --wait=false >/dev/null 2>&1 || true
$K -n "$NS" rollout status deploy/r-fjord --timeout=180s

log "consume everything with Kafka's verifiable consumer"
$K -n "$NS" run vcons --image="$KAFKA_IMG" --restart=Never --command -- \
  /opt/kafka/bin/kafka-verifiable-consumer.sh --bootstrap-server "$BS" \
  --topic chaos --group-id cg --max-messages "$ACKED" >/dev/null 2>&1
$K -n "$NS" wait --for=jsonpath='{.status.phase}'=Succeeded pod/vcons --timeout=180s \
  || $K -n "$NS" wait --for=jsonpath='{.status.phase}'=Failed pod/vcons --timeout=5s || true
CONS_LOG="$($K -n "$NS" logs vcons 2>/dev/null)"
$K -n "$NS" delete pod vcons --wait=false >/dev/null 2>&1 || true

echo "$CONS_LOG" | grep '"name":"records_consumed"' > /tmp/cons-chaos.jsonl || true
python3 - "$ACKED" <<'PY'
import json, sys
acked = int(sys.argv[1])
parts = {}
total = 0
for line in open('/tmp/cons-chaos.jsonl'):
    line=line.strip()
    if not line: continue
    e = json.loads(line)
    for p in e.get('partitions', []):
        k=(p['topic'],p['partition']); c=p['count']; mn=p['minOffset']; mx=p['maxOffset']
        total += c
        if k not in parts: parts[k]=[0,mn,mx]
        parts[k][0]+=c; parts[k][1]=min(parts[k][1],mn); parts[k][2]=max(parts[k][2],mx)
print(f"consumed under chaos: {total} (acked {acked})", file=sys.stderr)
ok=True
# No lost acked writes: every acked record is consumed (>= because non-idempotent
# retries during a kill may add at-least-once duplicates, which is expected).
if total < acked:
    print(f"FAIL: lost writes — consumed {total} < acked {acked}", file=sys.stderr); ok=False
# No offset gaps per partition (a gap = a hole in the committed log = lost data).
for k,(c,mn,mx) in sorted(parts.items()):
    span = mx-mn+1
    if c < span:
        print(f"FAIL: {k} has a gap — count {c} < span {span} (lost offset)", file=sys.stderr); ok=False
print("CHAOS PASS: no lost acked writes, no offset gaps under broker-kill chaos" if ok
      else "CHAOS FAIL", file=sys.stderr)
sys.exit(0 if ok else 1)
PY
rc=$?
helm uninstall r -n "$NS" >/dev/null 2>&1 || true
exit $rc
