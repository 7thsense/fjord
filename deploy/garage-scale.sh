#!/usr/bin/env bash
# Garage-backed durable-path scale lane.
#
# Runs one Garage-backed durable scale tier per invocation. Producer deliveries
# are awaited in a fixed-size window inside perf_durable.rs, replay happens after
# an in-process broker restart, and evidence is written under the requested dir.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="${FJORD_GARAGE_ENV_FILE:-$ROOT/deploy/chaos/garage.env}"

if [[ -f "$ENV_FILE" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "$ENV_FILE"
  set +a
fi

: "${FJORD_GARAGE_SCALE_RECORDS:=100000000}"
: "${FJORD_GARAGE_SCALE_PARTITIONS:=12}"
: "${FJORD_GARAGE_SCALE_RECORD_SIZE:=1024}"
: "${FJORD_GARAGE_SCALE_IN_FLIGHT:=300000}"
: "${FJORD_GARAGE_SCALE_CONSUME_DEADLINE_SECS:=14400}"
: "${FJORD_GARAGE_SCALE_FLUSH_LINGER_MS:=1000}"
: "${FJORD_GARAGE_SCALE_FLUSH_MAX_BYTES:=134217728}"
: "${FJORD_GARAGE_SCALE_PRODUCER_LINGER_MS:=100}"
: "${FJORD_GARAGE_SCALE_PRODUCER_BATCH_SIZE:=4194304}"
: "${FJORD_GARAGE_SCALE_PRODUCER_QUEUE_MESSAGES:=300000}"
: "${FJORD_GARAGE_SCALE_PRODUCER_QUEUE_KBYTES:=524288}"
: "${FJORD_GARAGE_SCALE_FAULT_PROFILE:=none}"
: "${FJORD_GARAGE_SCALE_NICE:=10}"
: "${FJORD_GARAGE_SCALE_EVIDENCE_DIR:=/tank/home/erik/fjord-evidence/$(date -u +%Y%m%d-%H%M%S)-garage-scale}"

if [[ -z "${FJORD_PG_URL:-}" ]]; then
  echo "FJORD_PG_URL is required for the Garage scale lane" >&2
  exit 2
fi

if [[ -z "${FJORD_GARAGE_SECRET:-}" ]]; then
  echo "FJORD_GARAGE_SECRET is required for the Garage scale lane" >&2
  exit 2
fi

mkdir -p "$FJORD_GARAGE_SCALE_EVIDENCE_DIR"
LOG="$FJORD_GARAGE_SCALE_EVIDENCE_DIR/garage-scale-${FJORD_GARAGE_SCALE_RECORDS}.log"

{
  echo "Garage scale lane"
  echo "records=$FJORD_GARAGE_SCALE_RECORDS"
  echo "partitions=$FJORD_GARAGE_SCALE_PARTITIONS"
  echo "record_size=$FJORD_GARAGE_SCALE_RECORD_SIZE"
  echo "in_flight=$FJORD_GARAGE_SCALE_IN_FLIGHT"
  echo "consume_deadline_secs=$FJORD_GARAGE_SCALE_CONSUME_DEADLINE_SECS"
  echo "flush_linger_ms=$FJORD_GARAGE_SCALE_FLUSH_LINGER_MS"
  echo "flush_max_bytes=$FJORD_GARAGE_SCALE_FLUSH_MAX_BYTES"
  echo "producer_linger_ms=$FJORD_GARAGE_SCALE_PRODUCER_LINGER_MS"
  echo "producer_batch_size=$FJORD_GARAGE_SCALE_PRODUCER_BATCH_SIZE"
  echo "producer_queue_messages=$FJORD_GARAGE_SCALE_PRODUCER_QUEUE_MESSAGES"
  echo "producer_queue_kbytes=$FJORD_GARAGE_SCALE_PRODUCER_QUEUE_KBYTES"
  echo "fault_profile=$FJORD_GARAGE_SCALE_FAULT_PROFILE"
  echo "evidence_dir=$FJORD_GARAGE_SCALE_EVIDENCE_DIR"
  echo "started_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
} | tee "$FJORD_GARAGE_SCALE_EVIDENCE_DIR/garage-scale-manifest.txt"

ulimit -n 65536 >/dev/null 2>&1 || true

cd "$ROOT"
nice -n "$FJORD_GARAGE_SCALE_NICE" env \
  FJORD_DURABLE_SCALE_PROOF=1 \
  FJORD_DURABLE_ONLY_GARAGE=1 \
  FJORD_DURABLE_RECORDS="$FJORD_GARAGE_SCALE_RECORDS" \
  FJORD_DURABLE_PARTITIONS="$FJORD_GARAGE_SCALE_PARTITIONS" \
  FJORD_DURABLE_RECORD_SIZE="$FJORD_GARAGE_SCALE_RECORD_SIZE" \
  FJORD_DURABLE_IN_FLIGHT="$FJORD_GARAGE_SCALE_IN_FLIGHT" \
  FJORD_DURABLE_CONSUME_DEADLINE_SECS="$FJORD_GARAGE_SCALE_CONSUME_DEADLINE_SECS" \
  FJORD_DURABLE_FLUSH_LINGER_MS="$FJORD_GARAGE_SCALE_FLUSH_LINGER_MS" \
  FJORD_DURABLE_FLUSH_MAX_BYTES="$FJORD_GARAGE_SCALE_FLUSH_MAX_BYTES" \
  FJORD_DURABLE_PRODUCER_LINGER_MS="$FJORD_GARAGE_SCALE_PRODUCER_LINGER_MS" \
  FJORD_DURABLE_PRODUCER_BATCH_SIZE="$FJORD_GARAGE_SCALE_PRODUCER_BATCH_SIZE" \
  FJORD_DURABLE_PRODUCER_QUEUE_MESSAGES="$FJORD_GARAGE_SCALE_PRODUCER_QUEUE_MESSAGES" \
  FJORD_DURABLE_PRODUCER_QUEUE_KBYTES="$FJORD_GARAGE_SCALE_PRODUCER_QUEUE_KBYTES" \
  FJORD_DURABLE_EVIDENCE_DIR="$FJORD_GARAGE_SCALE_EVIDENCE_DIR" \
  FJORD_DURABLE_FAULT_PROFILE="$FJORD_GARAGE_SCALE_FAULT_PROFILE" \
  OBJECT_LOG_S3_RANGE_FALLBACK="${OBJECT_LOG_S3_RANGE_FALLBACK:-1}" \
  RUST_BACKTRACE=1 \
  cargo test -p fjord-heimq-backend --features postgres-backend \
    --test perf_durable -- --ignored --nocapture 2>&1 | tee "$LOG"
