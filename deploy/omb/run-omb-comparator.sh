#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Run like-for-like OpenMessaging Benchmark profiles for Fjord, Kafka, Redpanda.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
OMB_DIR="$ROOT/deploy/omb"

: "${OMB_WORKLOAD:=$OMB_DIR/workloads/resource-smoke-1topic-12p-1kb.yaml}"
: "${OMB_EVIDENCE_DIR:=/tank/home/erik/fjord-evidence/$(date -u +%Y%m%d-%H%M%S)-omb-comparator}"

if [[ -z "${OMB_HOME:-}" ]]; then
  echo "OMB_HOME is required and must contain bin/benchmark" >&2
  exit 2
fi

if [[ ! -x "$OMB_HOME/bin/benchmark" ]]; then
  echo "OMB_HOME/bin/benchmark is not executable: $OMB_HOME/bin/benchmark" >&2
  exit 2
fi

for var in FJORD_BOOTSTRAP KAFKA_BOOTSTRAP REDPANDA_BOOTSTRAP; do
  if [[ -z "${!var:-}" ]]; then
    echo "$var is required" >&2
    exit 2
  fi
done

mkdir -p "$OMB_EVIDENCE_DIR/drivers" "$OMB_EVIDENCE_DIR/logs" "$OMB_EVIDENCE_DIR/telemetry"

render_driver() {
  local name="$1"
  local bootstrap="$2"
  sed "s|__BOOTSTRAP_SERVERS__|$bootstrap|g" \
    "$OMB_DIR/drivers/$name.yaml.in" > "$OMB_EVIDENCE_DIR/drivers/$name.yaml"
}

render_driver fjord "$FJORD_BOOTSTRAP"
render_driver kafka "$KAFKA_BOOTSTRAP"
render_driver redpanda "$REDPANDA_BOOTSTRAP"

{
  echo "started_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "fjord_git_sha=$(git -C "$ROOT" rev-parse HEAD)"
  echo "workload=$OMB_WORKLOAD"
  echo "omb_home=$OMB_HOME"
  echo "fjord_bootstrap=$FJORD_BOOTSTRAP"
  echo "kafka_bootstrap=$KAFKA_BOOTSTRAP"
  echo "redpanda_bootstrap=$REDPANDA_BOOTSTRAP"
  echo "omb_workers=${OMB_WORKERS:-}"
  echo "docker_stats_containers=${DOCKER_STATS_CONTAINERS:-}"
} > "$OMB_EVIDENCE_DIR/manifest.txt"

cp "$OMB_WORKLOAD" "$OMB_EVIDENCE_DIR/workload.yaml"

capture_docker_stats() {
  local phase="$1"
  if [[ -z "${DOCKER_STATS_CONTAINERS:-}" ]]; then
    return 0
  fi
  if ! command -v docker >/dev/null 2>&1; then
    echo "docker not found; cannot capture docker stats" \
      > "$OMB_EVIDENCE_DIR/telemetry/docker-stats-$phase.txt"
    return 0
  fi
  # shellcheck disable=SC2086
  docker stats --no-stream ${DOCKER_STATS_CONTAINERS//,/ } \
    > "$OMB_EVIDENCE_DIR/telemetry/docker-stats-$phase.txt" || true
}

run_one() {
  local name="$1"
  local driver="$OMB_EVIDENCE_DIR/drivers/$name.yaml"
  local log="$OMB_EVIDENCE_DIR/logs/$name.log"
  local -a cmd=("$OMB_HOME/bin/benchmark" "--drivers" "$driver")
  if [[ -n "${OMB_WORKERS:-}" ]]; then
    cmd+=("--workers" "$OMB_WORKERS")
  fi
  cmd+=("$OMB_WORKLOAD")

  echo "running ${cmd[*]}" | tee "$log"
  capture_docker_stats "$name-before"
  "${cmd[@]}" 2>&1 | tee -a "$log"
  capture_docker_stats "$name-after"
}

run_one fjord
run_one kafka
run_one redpanda

date -u +%Y-%m-%dT%H:%M:%SZ > "$OMB_EVIDENCE_DIR/completed_utc.txt"
echo "evidence: $OMB_EVIDENCE_DIR"
