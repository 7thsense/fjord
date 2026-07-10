#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Garage-backed durable-path scale lane.
#
# Runs one Garage-backed durable scale tier per invocation. Producer deliveries
# are awaited in a fixed-size window inside perf_durable.rs, replay happens after
# an in-process broker restart, and evidence is written under the requested dir.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="${FJORD_GARAGE_ENV_FILE:-$ROOT/deploy/chaos/garage.env}"
CONFIG_FILE="${FJORD_GARAGE_SCALE_CONFIG:-$ROOT/deploy/config/garage-scale.env}"

if [[ -f "$ENV_FILE" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "$ENV_FILE"
  set +a
fi

if [[ ! -f "$CONFIG_FILE" ]]; then
  echo "Garage scale config not found: $CONFIG_FILE" >&2
  exit 2
fi

# shellcheck disable=SC1090
source "$CONFIG_FILE"

required_config=(
  records
  partitions
  record_size
  in_flight
  consume_deadline_secs
  flush_linger_ms
  flush_max_bytes
  flush_inflight
  flush_max_buffered_bytes
  object_log_runtime_threads
  disable_payload_signing
  s3_range_fallback
  s3_multipart_threshold_bytes
  s3_multipart_part_bytes
  producer_count
  producer_linger_ms
  producer_batch_size
  producer_message_max_bytes
  producer_queue_messages
  producer_queue_kbytes
  producer_max_inflight_requests
  producer_message_timeout_ms
  fault_profile
  nice_level
  evidence_root
)

for name in "${required_config[@]}"; do
  if [[ -z "${!name:-}" ]]; then
    echo "Garage scale config missing required key: $name ($CONFIG_FILE)" >&2
    exit 2
  fi
done

evidence_dir="${evidence_dir:-$evidence_root/$(date -u +%Y%m%d-%H%M%S)-garage-scale}"

if [[ -z "${FJORD_PG_URL:-}" ]]; then
  echo "FJORD_PG_URL is required for the Garage scale lane" >&2
  exit 2
fi

if [[ -z "${FJORD_GARAGE_SECRET:-}" ]]; then
  echo "FJORD_GARAGE_SECRET is required for the Garage scale lane" >&2
  exit 2
fi

mkdir -p "$evidence_dir"
LOG="$evidence_dir/garage-scale-${records}.log"

{
  echo "Garage scale lane"
  echo "config_file=$CONFIG_FILE"
  echo "records=$records"
  echo "partitions=$partitions"
  echo "record_size=$record_size"
  echo "in_flight=$in_flight"
  echo "consume_deadline_secs=$consume_deadline_secs"
  echo "flush_linger_ms=$flush_linger_ms"
  echo "flush_max_bytes=$flush_max_bytes"
  echo "flush_inflight=$flush_inflight"
  echo "flush_max_buffered_bytes=$flush_max_buffered_bytes"
  echo "producer_count=$producer_count"
  echo "producer_linger_ms=$producer_linger_ms"
  echo "producer_batch_size=$producer_batch_size"
  echo "producer_message_max_bytes=$producer_message_max_bytes"
  echo "producer_queue_messages=$producer_queue_messages"
  echo "producer_queue_kbytes=$producer_queue_kbytes"
  echo "producer_max_inflight_requests=$producer_max_inflight_requests"
  echo "producer_message_timeout_ms=$producer_message_timeout_ms"
  echo "object_log_runtime_threads=$object_log_runtime_threads"
  echo "disable_payload_signing=$disable_payload_signing"
  echo "s3_range_fallback=$s3_range_fallback"
  echo "s3_multipart_threshold_bytes=$s3_multipart_threshold_bytes"
  echo "s3_multipart_part_bytes=$s3_multipart_part_bytes"
  echo "fault_profile=$fault_profile"
  echo "evidence_dir=$evidence_dir"
  echo "started_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
} | tee "$evidence_dir/garage-scale-manifest.txt"

ulimit -n 65536 >/dev/null 2>&1 || true

cd "$ROOT"
nice -n "$nice_level" env \
  FJORD_DURABLE_SCALE_PROOF=1 \
  FJORD_DURABLE_ONLY_GARAGE=1 \
  FJORD_DURABLE_RECORDS="$records" \
  FJORD_DURABLE_PARTITIONS="$partitions" \
  FJORD_DURABLE_RECORD_SIZE="$record_size" \
  FJORD_DURABLE_IN_FLIGHT="$in_flight" \
  FJORD_DURABLE_CONSUME_DEADLINE_SECS="$consume_deadline_secs" \
  FJORD_DURABLE_FLUSH_LINGER_MS="$flush_linger_ms" \
  FJORD_DURABLE_FLUSH_MAX_BYTES="$flush_max_bytes" \
  FJORD_DURABLE_FLUSH_MAX_INFLIGHT="$flush_inflight" \
  FJORD_DURABLE_FLUSH_MAX_BUFFERED_BYTES="$flush_max_buffered_bytes" \
  FJORD_DURABLE_PRODUCER_COUNT="$producer_count" \
  FJORD_DURABLE_PRODUCER_LINGER_MS="$producer_linger_ms" \
  FJORD_DURABLE_PRODUCER_BATCH_SIZE="$producer_batch_size" \
  FJORD_DURABLE_PRODUCER_MESSAGE_MAX_BYTES="$producer_message_max_bytes" \
  FJORD_DURABLE_PRODUCER_QUEUE_MESSAGES="$producer_queue_messages" \
  FJORD_DURABLE_PRODUCER_QUEUE_KBYTES="$producer_queue_kbytes" \
  FJORD_DURABLE_PRODUCER_MAX_INFLIGHT_REQUESTS="$producer_max_inflight_requests" \
  FJORD_DURABLE_PRODUCER_MESSAGE_TIMEOUT_MS="$producer_message_timeout_ms" \
  FJORD_DURABLE_S3_MULTIPART_THRESHOLD_BYTES="$s3_multipart_threshold_bytes" \
  FJORD_DURABLE_S3_MULTIPART_PART_BYTES="$s3_multipart_part_bytes" \
  FJORD_DURABLE_EVIDENCE_DIR="$evidence_dir" \
  FJORD_DURABLE_FAULT_PROFILE="$fault_profile" \
  OBJECT_LOG_FLUSH_RUNTIME_THREADS="$object_log_runtime_threads" \
  OBJECT_LOG_S3_DISABLE_PAYLOAD_SIGNING="$disable_payload_signing" \
  OBJECT_LOG_S3_RANGE_FALLBACK="$s3_range_fallback" \
  RUST_BACKTRACE=1 \
  cargo test -p fjord-heimq-backend --features postgres-backend \
    --test perf_durable -- --ignored --nocapture 2>&1 | tee "$LOG"
