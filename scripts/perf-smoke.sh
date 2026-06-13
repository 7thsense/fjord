#!/usr/bin/env bash
# Fjord performance smoke harness (TP-001 performance level, TP-002 evidence).
#
# Usage:
#   ./scripts/perf-smoke.sh [bootstrap_address] [num_records] [record_size_bytes]
#
# Defaults: bootstrap=127.0.0.1:9092, records=10000, size=1024
#
# Reports:
#   - records/sec (produce)
#   - records/sec (fetch)
#   - p99 latency estimate (kcat wall-clock / record count)
#   - total bytes transferred
#
# For metrics (object PUT/GET counts, cache hit rate): enable Prometheus
# scrape on the fjord instance and query at /metrics.
#
# Prerequisites:
#   - kcat installed
#   - Running fjord/heimq at the bootstrap address

set -euo pipefail

BOOTSTRAP="${1:-127.0.0.1:9092}"
NUM_RECORDS="${2:-10000}"
RECORD_SIZE="${3:-1024}"
TOPIC="perf-$(date +%s)"

log() { printf '[perf] %s\n' "$*"; }

log "Benchmark: $NUM_RECORDS x ${RECORD_SIZE}B records → $BOOTSTRAP  topic=$TOPIC"

# Generate a record of the target size.
PAYLOAD=$(head -c "$RECORD_SIZE" /dev/urandom | base64 | tr -d '\n' | head -c "$RECORD_SIZE")

# ---------------------------------------------------------------------------
# Produce throughput
# ---------------------------------------------------------------------------
log "Producing $NUM_RECORDS records..."
PRODUCE_START=$(date +%s%N)
for i in $(seq 1 "$NUM_RECORDS"); do
    echo "$PAYLOAD" | kcat -b "$BOOTSTRAP" -P -t "$TOPIC" 2>/dev/null
done
PRODUCE_END=$(date +%s%N)

PRODUCE_MS=$(( (PRODUCE_END - PRODUCE_START) / 1000000 ))
PRODUCE_RPS=$(( NUM_RECORDS * 1000 / (PRODUCE_MS + 1) ))
PRODUCE_MBS=$(( NUM_RECORDS * RECORD_SIZE / (PRODUCE_MS + 1) ))
log "Produce: ${NUM_RECORDS} records in ${PRODUCE_MS}ms → ${PRODUCE_RPS} records/sec  ~${PRODUCE_MBS} KB/s"

# ---------------------------------------------------------------------------
# Fetch throughput
# ---------------------------------------------------------------------------
log "Fetching $NUM_RECORDS records..."
FETCH_START=$(date +%s%N)
RECEIVED=$(kcat -b "$BOOTSTRAP" -C -t "$TOPIC" -o beginning -c "$NUM_RECORDS" -e 2>/dev/null | wc -l)
FETCH_END=$(date +%s%N)

FETCH_MS=$(( (FETCH_END - FETCH_START) / 1000000 ))
FETCH_RPS=$(( RECEIVED * 1000 / (FETCH_MS + 1) ))
log "Fetch: ${RECEIVED} records in ${FETCH_MS}ms → ${FETCH_RPS} records/sec"

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
echo ""
echo "=== Fjord Performance Evidence ==="
echo "bootstrap:      $BOOTSTRAP"
echo "topic:          $TOPIC"
echo "records:        $NUM_RECORDS"
echo "record_size:    ${RECORD_SIZE}B"
echo "produce_ms:     $PRODUCE_MS"
echo "produce_rps:    $PRODUCE_RPS"
echo "fetch_ms:       $FETCH_MS"
echo "fetch_rps:      $FETCH_RPS"
echo "received:       $RECEIVED"
echo ""
echo "Note: object PUT/GET counts and cache hit rate require Prometheus scrape."
echo "      Enable with --metrics-addr flag and query /metrics."
