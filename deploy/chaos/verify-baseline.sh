#!/usr/bin/env bash
# Baseline (no chaos) of the EXTERNAL correctness oracle: Apache Kafka's own
# kafka-verifiable-producer / kafka-verifiable-consumer (the reference
# implementation's no-loss / contiguous-offset verifiers, used in Kafka's
# ducktape system tests) run against a deployed fjord cluster on kind.
#
# This proves the oracle is valid against fjord before Chaos Mesh fault
# injection is layered on (verify-chaos.sh). Correctness checks, from Kafka's
# OWN tool output:
#   * total consumed == total acked        (no lost acked writes)
#   * per partition: count == maxOffset-minOffset+1   (no gaps, no duplicates)
set -euo pipefail

CLUSTER=fjord-e2e
NS=fjord-chaos
KAFKA_IMG=apache/kafka:3.8.1
N="${N:-20000}"
THROUGHPUT="${THROUGHPUT:-8000}"
CHART="$(cd "$(dirname "${BASH_SOURCE[0]}")/../helm/fjord" && pwd)"

log() { echo -e "\n=== $* ===" >&2; }

# OrbStack: reach the kind API via the node container IP (published port is
# unreachable here; the IP is in the apiserver cert SANs).
NODE_IP="$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "${CLUSTER}-control-plane")"
KC="$(mktemp)"; trap 'rm -f "$KC"' EXIT
kind get kubeconfig --name "$CLUSTER" > "$KC"
sed -i -E "s#https://127.0.0.1:[0-9]+#https://${NODE_IP}:6443#" "$KC"
export KUBECONFIG="$KC"
K="kubectl"

log "load fjord image into kind"
kind load docker-image fjord:dev --name "$CLUSTER" 2>&1 | tail -1

log "install fjord (multiBroker, 3 brokers) + bundled postgres + minio"
$K create namespace "$NS" --dry-run=client -o yaml | $K apply -f -
helm upgrade --install r "$CHART" -n "$NS" \
  --set mode=multiBroker --set replicaCount=3 --set autoscaling.enabled=false \
  --set image.repository=fjord --set image.tag=dev \
  --set image.pullPolicy=IfNotPresent --set 'broker.createTopics={chaos:6}'
$K -n "$NS" rollout status deploy/r-fjord-postgres --timeout=180s
$K -n "$NS" rollout status deploy/r-fjord-minio --timeout=180s
$K -n "$NS" wait --for=condition=complete job/r-fjord-minio-mkbucket --timeout=120s || true
$K -n "$NS" rollout status statefulset/r-fjord --timeout=240s

BS="r-fjord.${NS}.svc.cluster.local:9092"

log "produce $N sequenced records with Kafka's verifiable producer (acks=all)"
$K -n "$NS" run vprod --image="$KAFKA_IMG" --restart=Never --command -- \
  /opt/kafka/bin/kafka-verifiable-producer.sh --bootstrap-server "$BS" \
  --topic chaos --max-messages "$N" --throughput "$THROUGHPUT" --acks -1 >/dev/null 2>&1
$K -n "$NS" wait --for=jsonpath='{.status.phase}'=Succeeded pod/vprod --timeout=180s \
  || $K -n "$NS" wait --for=jsonpath='{.status.phase}'=Failed pod/vprod --timeout=5s || true
PROD_LOG="$($K -n "$NS" logs vprod 2>/dev/null)"
$K -n "$NS" delete pod vprod --wait=false >/dev/null 2>&1 || true
ACKED="$(echo "$PROD_LOG" | grep -c '"name":"producer_send_success"' || true)"
echo "acked: $ACKED" >&2

log "consume with Kafka's verifiable consumer"
$K -n "$NS" run vcons --image="$KAFKA_IMG" --restart=Never --command -- \
  /opt/kafka/bin/kafka-verifiable-consumer.sh --bootstrap-server "$BS" \
  --topic chaos --group-id cg --max-messages "$N" >/dev/null 2>&1
$K -n "$NS" wait --for=jsonpath='{.status.phase}'=Succeeded pod/vcons --timeout=180s \
  || $K -n "$NS" wait --for=jsonpath='{.status.phase}'=Failed pod/vcons --timeout=5s || true
CONS_LOG="$($K -n "$NS" logs vcons 2>/dev/null)"
$K -n "$NS" delete pod vcons --wait=false >/dev/null 2>&1 || true

# Correctness from the consumer's records_consumed events (per-partition count +
# offset range). Total consumed == acked (no loss); per partition
# count == max-min+1 (contiguous → no gaps, no duplicates).
echo "$CONS_LOG" | grep '"name":"records_consumed"' > /tmp/cons.jsonl || true
python3 - "$ACKED" <<'PY'
import json, sys
acked = int(sys.argv[1])
parts = {}   # (topic,partition) -> [count, min, max]
total = 0
for line in open('/tmp/cons.jsonl'):
    line=line.strip()
    if not line: continue
    e = json.loads(line)
    for p in e.get('partitions', []):
        k = (p['topic'], p['partition'])
        c = p['count']; mn = p['minOffset']; mx = p['maxOffset']
        total += c
        if k not in parts: parts[k] = [0, mn, mx]
        parts[k][0] += c
        parts[k][1] = min(parts[k][1], mn)
        parts[k][2] = max(parts[k][2], mx)
print(f"consumed total: {total}", file=sys.stderr)
ok = True
if total < acked:
    print(f"FAIL: lost writes — consumed {total} < acked {acked}", file=sys.stderr); ok=False
for k,(c,mn,mx) in sorted(parts.items()):
    span = mx - mn + 1
    if c != span:
        print(f"FAIL: {k} not contiguous — count {c} != span {span} (gap/dup)", file=sys.stderr); ok=False
print("BASELINE PASS" if ok else "BASELINE FAIL", file=sys.stderr)
sys.exit(0 if ok else 1)
PY
rc=$?
helm uninstall r -n "$NS" >/dev/null 2>&1 || true
exit $rc
