#!/usr/bin/env bash
# Stronger chaos validation: EXACTLY-ONCE under broker-kill chaos, using an
# IDEMPOTENT Apache Kafka verifiable-producer (enable.idempotence=true) + Chaos
# Mesh. Where verify-chaos.sh proved no-loss (at-least-once), this proves
# consumed == acked EXACTLY (no value-duplicates): fjord's coordinator dedups a
# producer's retried batch (same producer-id+sequence ⇒ same offset, not a
# second copy), so an ack lost to a killed broker does not duplicate data.
set -euo pipefail

CLUSTER=fjord-e2e
NS=fjord-chaos-eos
KAFKA_IMG=apache/kafka:3.8.1
N="${N:-60000}"
THROUGHPUT="${THROUGHPUT:-1000}"
CHART="$(cd "$(dirname "${BASH_SOURCE[0]}")/../helm/fjord" && pwd)"

log() { echo -e "\n=== $* ===" >&2; }

NODE_IP="$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "${CLUSTER}-control-plane")"
KC="$(mktemp)"; trap 'rm -f "$KC"' EXIT
kind get kubeconfig --name "$CLUSTER" > "$KC"
sed -i -E "s#https://127.0.0.1:[0-9]+#https://${NODE_IP}:6443#" "$KC"
export KUBECONFIG="$KC"
K="kubectl"

log "ensure Chaos Mesh present"
helm repo add chaos-mesh https://charts.chaos-mesh.org >/dev/null 2>&1 || true
helm upgrade --install chaos-mesh chaos-mesh/chaos-mesh -n chaos-mesh --create-namespace \
  --set chaosDaemon.runtime=containerd --set chaosDaemon.socketPath=/run/containerd/containerd.sock \
  --version 2.6.3 >/dev/null
$K -n chaos-mesh rollout status deploy/chaos-controller-manager --timeout=240s

log "install fjord (singleLogical, 3 replicas) + postgres + minio"
$K create namespace "$NS" --dry-run=client -o yaml | $K apply -f -
helm upgrade --install r "$CHART" -n "$NS" \
  --set mode=singleLogical --set replicaCount=3 --set autoscaling.enabled=false \
  --set image.repository=fjord --set image.tag=dev \
  --set image.pullPolicy=IfNotPresent --set 'broker.createTopics={chaos-eos:6}'
$K -n "$NS" rollout status deploy/r-fjord-postgres --timeout=180s
$K -n "$NS" rollout status deploy/r-fjord-minio --timeout=180s
$K -n "$NS" wait --for=condition=complete job/r-fjord-minio-mkbucket --timeout=120s || true
$K -n "$NS" rollout status deploy/r-fjord --timeout=240s

BS="r-fjord.${NS}.svc.cluster.local:9092"

log "start IDEMPOTENT verifiable producer ($N records) in the background"
$K -n "$NS" run vprod --image="$KAFKA_IMG" --restart=Never --command -- sh -c \
  "printf 'enable.idempotence=true\nacks=all\nmax.in.flight.requests.per.connection=5\nretries=2147483647\n' > /tmp/p.properties && \
   /opt/kafka/bin/kafka-verifiable-producer.sh --bootstrap-server $BS --topic chaos-eos \
   --max-messages $N --throughput $THROUGHPUT --producer.config /tmp/p.properties" >/dev/null 2>&1

log "Chaos Mesh: kill a broker pod every 20s (brokers only)"
cat <<YAML | $K apply -f -
apiVersion: chaos-mesh.org/v1alpha1
kind: Schedule
metadata: { name: kill-brokers, namespace: $NS }
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
      labelSelectors: { app.kubernetes.io/name: fjord }
      expressionSelectors:
        - { key: app.kubernetes.io/component, operator: DoesNotExist }
YAML

log "wait for producer to finish"
$K -n "$NS" wait --for=jsonpath='{.status.phase}'=Succeeded pod/vprod --timeout=400s \
  || $K -n "$NS" wait --for=jsonpath='{.status.phase}'=Failed pod/vprod --timeout=5s || true
PROD_LOG="$($K -n "$NS" logs vprod 2>/dev/null)"
$K -n "$NS" delete pod vprod --wait=false >/dev/null 2>&1 || true
ACKED="$(echo "$PROD_LOG" | grep -c '"name":"producer_send_success"' || true)"
echo "idempotent acked under chaos: $ACKED" >&2

log "stop chaos; stabilize"
$K -n "$NS" delete schedule kill-brokers --wait=false >/dev/null 2>&1 || true
$K -n "$NS" rollout status deploy/r-fjord --timeout=180s

log "consume with verifiable consumer"
$K -n "$NS" run vcons --image="$KAFKA_IMG" --restart=Never --command -- \
  /opt/kafka/bin/kafka-verifiable-consumer.sh --bootstrap-server "$BS" \
  --topic chaos-eos --group-id cg --max-messages "$ACKED" >/dev/null 2>&1
$K -n "$NS" wait --for=jsonpath='{.status.phase}'=Succeeded pod/vcons --timeout=180s \
  || $K -n "$NS" wait --for=jsonpath='{.status.phase}'=Failed pod/vcons --timeout=5s || true
CONS_LOG="$($K -n "$NS" logs vcons 2>/dev/null)"
$K -n "$NS" delete pod vcons --wait=false >/dev/null 2>&1 || true

echo "$CONS_LOG" | grep '"name":"records_consumed"' > /tmp/cons-eos.jsonl || true
python3 - "$ACKED" <<'PY'
import json, sys
acked = int(sys.argv[1])
parts = {}; total = 0
for line in open('/tmp/cons-eos.jsonl'):
    line=line.strip()
    if not line: continue
    for p in json.loads(line).get('partitions', []):
        k=(p['topic'],p['partition']); total += p['count']
        if k not in parts: parts[k]=[0,p['minOffset'],p['maxOffset']]
        parts[k][0]+=p['count']; parts[k][1]=min(parts[k][1],p['minOffset']); parts[k][2]=max(parts[k][2],p['maxOffset'])
print(f"consumed under chaos: {total} (acked {acked})", file=sys.stderr)
ok=True
# EXACTLY-ONCE: idempotent producer ⇒ no duplicates AND no loss ⇒ equal.
if total != acked:
    print(f"FAIL: exactly-once violated — consumed {total} != acked {acked}", file=sys.stderr); ok=False
for k,(c,mn,mx) in sorted(parts.items()):
    if c != mx-mn+1:
        print(f"FAIL: {k} not contiguous — count {c} != span {mx-mn+1}", file=sys.stderr); ok=False
print("EOS-CHAOS PASS: exactly-once (consumed==acked, contiguous) under broker-kill chaos" if ok
      else "EOS-CHAOS FAIL", file=sys.stderr)
sys.exit(0 if ok else 1)
PY
rc=$?
helm uninstall r -n "$NS" >/dev/null 2>&1 || true
$K delete namespace "$NS" --wait=false >/dev/null 2>&1 || true
exit $rc
