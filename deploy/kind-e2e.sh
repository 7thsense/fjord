#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# End-to-end test of the fjord Helm chart on a real (kind) cluster, for BOTH
# topology modes:
#   singleLogical — Deployment; metadata advertises 1 broker (the Service).
#   multiBroker   — StatefulSet; metadata advertises N brokers (per-pod DNS).
# For each: install with bundled Postgres + MinIO, then drive it with an
# in-cluster kcat client — verify the advertised broker count, produce 100
# records, and consume them back.
#
#   ./deploy/kind-e2e.sh
#   FJORD_IMAGE=fjord:dev ./deploy/kind-e2e.sh
#   FJORD_BUILD_IMAGE=1 ./deploy/kind-e2e.sh
set -euo pipefail

CLUSTER="${FJORD_CLUSTER:-fjord-e2e}"
IMAGE="${FJORD_IMAGE:-fjord:dev}"
KCAT="${KCAT_IMAGE:-edenhill/kcat:1.7.1}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CHART="${FJORD_CHART:-$ROOT/deploy/helm/fjord}"
BUILD_IMAGE="${FJORD_BUILD_IMAGE:-0}"

log() { echo -e "\n=== $* ===" >&2; }
die() { echo "error: $*" >&2; exit 1; }

need() {
  command -v "$1" >/dev/null 2>&1 || die "required tool not found: $1"
}

need docker
need kind
need kubectl
need helm
docker info >/dev/null 2>&1 || die "docker daemon is not reachable"
[[ -f "$CHART/Chart.yaml" ]] || die "chart not found: $CHART"

split_image() {
  local ref="$1"
  if [[ "$ref" == *:* && "$ref" != *://* ]]; then
    IMAGE_REPO="${ref%:*}"
    IMAGE_TAG="${ref##*:}"
    if [[ "$IMAGE_TAG" == sha256* ]]; then
      die "FJORD_IMAGE must be repository:tag (got digest-only ref: $ref)"
    fi
  else
    IMAGE_REPO="$ref"
    IMAGE_TAG="latest"
  fi
}

if [[ "$BUILD_IMAGE" == "1" ]]; then
  log "building image $IMAGE"
  docker build -t "$IMAGE" -f "$ROOT/Dockerfile" "$ROOT"
fi

if ! docker image inspect "$IMAGE" >/dev/null 2>&1; then
  log "image $IMAGE not present locally; attempting pull"
  docker pull "$IMAGE"
fi

# 1. Cluster + image.
if ! kind get clusters 2>/dev/null | grep -qx "$CLUSTER"; then
  log "creating kind cluster $CLUSTER"
  kind create cluster --name "$CLUSTER" --wait 120s
fi
log "loading image $IMAGE into kind"
kind load docker-image "$IMAGE" --name "$CLUSTER"

KC="$(mktemp)"
trap 'rm -f "$KC"' EXIT
kind get kubeconfig --name "$CLUSTER" >"$KC"
# Prefer stock kubeconfig; rewrite to node IP only when 127.0.0.1 is unreachable
# (OrbStack / some remote-docker setups).
if ! kubectl --kubeconfig="$KC" cluster-info >/dev/null 2>&1; then
  NODE_IP="$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "${CLUSTER}-control-plane")"
  sed -i -E "s#https://127.0.0.1:[0-9]+#https://${NODE_IP}:6443#" "$KC"
  log "API server via node IP https://${NODE_IP}:6443"
fi
export KUBECONFIG="$KC"

split_image "$IMAGE"

# Run a one-shot in-cluster pod and return its stdout. Deterministic: run
# detached, wait for completion, then fetch logs — avoids the `kubectl run -i`
# attach race (fast containers exit before attach, output gets lost).
kcat_run() {
  local ns="$1"; shift
  local name="kcat-${RANDOM}${RANDOM}"
  kubectl -n "$ns" run "$name" --image="$KCAT" --restart=Never \
    --command -- "$@" >/dev/null 2>&1 || true
  kubectl -n "$ns" wait --for=jsonpath='{.status.phase}'=Succeeded "pod/$name" --timeout=60s >/dev/null 2>&1 \
    || kubectl -n "$ns" wait --for=jsonpath='{.status.phase}'=Failed "pod/$name" --timeout=2s >/dev/null 2>&1 || true
  kubectl -n "$ns" logs "$name" 2>/dev/null || true
  kubectl -n "$ns" delete pod "$name" --wait=false >/dev/null 2>&1 || true
}

test_mode() {
  local mode="$1" ns="$2" expect_brokers="$3"
  log "MODE=$mode  ns=$ns  expect_brokers=$expect_brokers"
  kubectl create namespace "$ns" --dry-run=client -o yaml | kubectl apply -f -

  helm upgrade --install r "$CHART" -n "$ns" \
    --set mode="$mode" --set replicaCount=3 --set autoscaling.enabled=false \
    --set image.repository="$IMAGE_REPO" --set image.tag="$IMAGE_TAG" \
    --set image.pullPolicy=IfNotPresent \
    --set 'broker.createTopics={e2e:6}'

  log "waiting for bundled postgres + minio"
  kubectl -n "$ns" rollout status deploy/r-fjord-postgres --timeout=180s
  kubectl -n "$ns" rollout status deploy/r-fjord-minio --timeout=180s
  log "waiting for bucket-create job"
  kubectl -n "$ns" wait --for=condition=complete job/r-fjord-minio-mkbucket --timeout=120s || true

  log "waiting for brokers ($mode)"
  if [[ "$mode" == "multiBroker" ]]; then
    kubectl -n "$ns" rollout status statefulset/r-fjord --timeout=240s
  else
    kubectl -n "$ns" rollout status deploy/r-fjord --timeout=240s
  fi

  local bs="r-fjord.$ns.svc.cluster.local:9092"

  log "metadata: advertised brokers (retry until cluster settles)"
  local meta nbrokers=0
  for _ in $(seq 1 20); do
    meta="$(kcat_run "$ns" sh -c "kcat -b $bs -L -q 2>/dev/null" || true)"
    # kcat -L prints e.g. "  3 brokers:" (indented). Pull that count.
    nbrokers="$(echo "$meta" | sed -n 's/^ *\([0-9]\+\) brokers:.*/\1/p' | head -1)"
    [[ "${nbrokers:-0}" -eq "$expect_brokers" ]] && break
    sleep 3
  done
  echo "$meta" | grep -E "broker [0-9]|topic \"e2e\"|partition .* leader" | head -20 >&2
  echo "advertised broker count: ${nbrokers:-?}" >&2
  if [[ "${nbrokers:-0}" -ne "$expect_brokers" ]]; then
    echo "FAIL($mode): expected $expect_brokers brokers, got ${nbrokers:-0}" >&2
    return 1
  fi

  log "produce 100 records"
  kcat_run "$ns" sh -c "seq 100 | kcat -b $bs -t e2e -P && echo produced"

  log "consume them back"
  local got=0
  for _ in $(seq 1 10); do
    got="$(kcat_run "$ns" sh -c "kcat -b $bs -t e2e -C -e -q -o beginning 2>/dev/null | wc -l")"
    got="$(echo "$got" | tr -dc '0-9')"
    [[ "${got:-0}" -eq 100 ]] && break
    sleep 3
  done
  echo "consumed: ${got:-0}" >&2
  if [[ "${got:-0}" -ne 100 ]]; then
    echo "FAIL($mode): expected 100 records, got ${got:-0}" >&2
    return 1
  fi

  log "PASS($mode): $expect_brokers broker(s), 100/100 records round-tripped"
  helm uninstall r -n "$ns" || true
}

rc=0
test_mode singleLogical fjord-single 1 || rc=1
test_mode multiBroker fjord-multi 3 || rc=1

if [[ $rc -eq 0 ]]; then
  log "ALL MODES PASSED"
else
  log "SOME MODE FAILED"
fi
exit $rc
