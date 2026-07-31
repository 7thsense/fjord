#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# One-liner evaluation install of Fjord on kind (bundled Postgres + MinIO).
#
# From a clone:
#   ./deploy/kind-up.sh
#
# Remote (no prior clone):
#   curl -fsSL https://raw.githubusercontent.com/7thsense/fjord/main/deploy/kind-up.sh | bash
#
# Environment:
#   FJORD_CLUSTER   kind cluster name (default: fjord)
#   FJORD_NS        Kubernetes namespace (default: fjord)
#   FJORD_IMAGE     full image ref to use (optional; otherwise pull release or build)
#   FJORD_RELEASE   release version for public image/chart (default: 0.1.5)
#   FJORD_MODE      singleLogical | multiBroker (default: singleLogical)
#   FJORD_REPLICAS  broker replicas (default: 1 for singleLogical, 3 for multiBroker)
#   FJORD_SKIP_SMOKE  set to 1 to skip produce/consume smoke
#   FJORD_KEEP      set to 1 to leave the release installed without printing teardown hints only
set -euo pipefail

FJORD_CLUSTER="${FJORD_CLUSTER:-fjord}"
FJORD_NS="${FJORD_NS:-fjord}"
FJORD_RELEASE="${FJORD_RELEASE:-0.1.5}"
FJORD_MODE="${FJORD_MODE:-singleLogical}"
FJORD_SKIP_SMOKE="${FJORD_SKIP_SMOKE:-0}"
PUBLIC_IMAGE="ghcr.io/7thsense/fjord:${FJORD_RELEASE}"
PUBLIC_CHART_URL="https://github.com/7thsense/fjord/releases/download/v${FJORD_RELEASE}/fjord-${FJORD_RELEASE}.tgz"
PUBLIC_CHART_OCI="oci://ghcr.io/7thsense/charts/fjord"
KCAT_IMAGE="${KCAT_IMAGE:-edenhill/kcat:1.7.1}"
REPO_URL="${FJORD_REPO_URL:-https://github.com/7thsense/fjord.git}"

log() { printf '\n==> %s\n' "$*" >&2; }
die() { printf 'error: %s\n' "$*" >&2; exit 1; }

need() {
  command -v "$1" >/dev/null 2>&1 || die "required tool not found: $1"
}

resolve_repo() {
  local script="${BASH_SOURCE[0]:-}"
  if [[ -n "$script" && -f "$script" ]]; then
    local here
    here="$(cd "$(dirname "$script")" && pwd)"
    if [[ -f "$here/helm/fjord/Chart.yaml" ]]; then
      REPO_ROOT="$(cd "$here/.." && pwd)"
      return
    fi
    if [[ -f "$here/deploy/helm/fjord/Chart.yaml" ]]; then
      REPO_ROOT="$here"
      return
    fi
  fi

  # Piped from curl / non-checkout invocation: shallow-clone into a temp dir.
  CLONE_DIR="$(mktemp -d "${TMPDIR:-/tmp}/fjord-kind-up.XXXXXX")"
  log "cloning $REPO_URL into $CLONE_DIR"
  git clone --depth 1 "$REPO_URL" "$CLONE_DIR/fjord"
  REPO_ROOT="$CLONE_DIR/fjord"
}

split_image() {
  # IMAGE_REF -> IMAGE_REPO + IMAGE_TAG (tag defaults to latest)
  local ref="$1"
  if [[ "$ref" == *:* && "$ref" != *://* ]]; then
    IMAGE_REPO="${ref%:*}"
    IMAGE_TAG="${ref##*:}"
    # Handle digests: repo@sha256:... has no usable tag for Helm; require tag form.
    if [[ "$IMAGE_TAG" == sha256* ]]; then
      die "FJORD_IMAGE must be repository:tag (got digest-only ref: $ref)"
    fi
  else
    IMAGE_REPO="$ref"
    IMAGE_TAG="latest"
  fi
}

ensure_tools() {
  need docker
  need kind
  need kubectl
  need helm
  need git
  docker info >/dev/null 2>&1 || die "docker daemon is not reachable"
}

ensure_cluster() {
  if ! kind get clusters 2>/dev/null | grep -qx "$FJORD_CLUSTER"; then
    log "creating kind cluster $FJORD_CLUSTER"
    kind create cluster --name "$FJORD_CLUSTER" --wait 180s
  else
    log "using existing kind cluster $FJORD_CLUSTER"
  fi

  local kc node_ip
  kc="$(mktemp)"
  kind get kubeconfig --name "$FJORD_CLUSTER" >"$kc"
  # Prefer the stock kubeconfig; fall back to the node IP (OrbStack and some
  # remote-docker setups publish the API only on the control-plane container IP).
  if ! kubectl --kubeconfig="$kc" cluster-info >/dev/null 2>&1; then
    node_ip="$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "${FJORD_CLUSTER}-control-plane")"
    sed -i -E "s#https://127.0.0.1:[0-9]+#https://${node_ip}:6443#" "$kc"
    log "API server via node IP https://${node_ip}:6443"
  fi
  export KUBECONFIG="$kc"
  KUBECONFIG_FILE="$kc"
}

resolve_image() {
  if [[ -n "${FJORD_IMAGE:-}" ]]; then
    IMAGE_REF="$FJORD_IMAGE"
    log "using FJORD_IMAGE=$IMAGE_REF"
    if ! docker image inspect "$IMAGE_REF" >/dev/null 2>&1; then
      log "pulling $IMAGE_REF"
      docker pull "$IMAGE_REF"
    fi
    return
  fi

  if docker pull "$PUBLIC_IMAGE" >/dev/null 2>&1; then
    IMAGE_REF="$PUBLIC_IMAGE"
    log "using public image $IMAGE_REF"
    return
  fi

  IMAGE_REF="fjord:dev"
  log "public image unavailable; building $IMAGE_REF from source"
  docker build -t "$IMAGE_REF" -f "$REPO_ROOT/Dockerfile" "$REPO_ROOT"
}

load_image() {
  log "loading $IMAGE_REF into kind ($FJORD_CLUSTER)"
  kind load docker-image "$IMAGE_REF" --name "$FJORD_CLUSTER"
}

resolve_chart() {
  if [[ -f "$REPO_ROOT/deploy/helm/fjord/Chart.yaml" ]]; then
    CHART="$REPO_ROOT/deploy/helm/fjord"
    log "using chart from source: $CHART"
    return
  fi
  die "chart not found under $REPO_ROOT/deploy/helm/fjord"
}

install_chart() {
  local replicas="${FJORD_REPLICAS:-}"
  if [[ -z "$replicas" ]]; then
    if [[ "$FJORD_MODE" == "multiBroker" ]]; then
      replicas=3
    else
      replicas=1
    fi
  fi

  split_image "$IMAGE_REF"
  log "installing Fjord ($FJORD_MODE, replicas=$replicas) into namespace $FJORD_NS"
  kubectl create namespace "$FJORD_NS" --dry-run=client -o yaml | kubectl apply -f -

  helm upgrade --install fjord "$CHART" \
    --namespace "$FJORD_NS" \
    --set mode="$FJORD_MODE" \
    --set replicaCount="$replicas" \
    --set autoscaling.enabled=false \
    --set image.repository="$IMAGE_REPO" \
    --set image.tag="$IMAGE_TAG" \
    --set image.pullPolicy=IfNotPresent \
    --set 'broker.createTopics={quickstart:1}' \
    --wait \
    --timeout 10m

  log "waiting for bundled dependencies"
  kubectl -n "$FJORD_NS" rollout status deploy/fjord-fjord-postgres --timeout=180s
  kubectl -n "$FJORD_NS" rollout status deploy/fjord-fjord-minio --timeout=180s
  kubectl -n "$FJORD_NS" wait --for=condition=complete job/fjord-fjord-minio-mkbucket --timeout=120s || true

  if [[ "$FJORD_MODE" == "multiBroker" ]]; then
    kubectl -n "$FJORD_NS" rollout status statefulset/fjord-fjord --timeout=240s
  else
    kubectl -n "$FJORD_NS" rollout status deploy/fjord-fjord --timeout=240s
  fi
}

kcat_run() {
  local name="kcat-${RANDOM}${RANDOM}"
  kubectl -n "$FJORD_NS" run "$name" --image="$KCAT_IMAGE" --restart=Never \
    --command -- "$@" >/dev/null 2>&1 || true
  kubectl -n "$FJORD_NS" wait --for=jsonpath='{.status.phase}'=Succeeded "pod/$name" --timeout=60s >/dev/null 2>&1 \
    || kubectl -n "$FJORD_NS" wait --for=jsonpath='{.status.phase}'=Failed "pod/$name" --timeout=2s >/dev/null 2>&1 || true
  kubectl -n "$FJORD_NS" logs "$name" 2>/dev/null || true
  kubectl -n "$FJORD_NS" delete pod "$name" --wait=false >/dev/null 2>&1 || true
}

smoke() {
  [[ "$FJORD_SKIP_SMOKE" == "1" ]] && return 0
  local bs="fjord-fjord.${FJORD_NS}.svc.cluster.local:9092"
  log "smoke: produce/consume on topic quickstart via $bs"
  kcat_run sh -c "printf 'hello from fjord kind-up\n' | kcat -b $bs -t quickstart -P && echo produced"
  local got=""
  for _ in $(seq 1 15); do
    got="$(kcat_run sh -c "kcat -b $bs -t quickstart -C -e -q -o beginning 2>/dev/null" || true)"
    [[ -n "$got" ]] && break
    sleep 2
  done
  if [[ -z "$got" ]]; then
    die "smoke failed: no records consumed from quickstart"
  fi
  printf 'consumed: %s\n' "$got" >&2
  log "smoke passed"
}

print_next_steps() {
  local bs="fjord-fjord.${FJORD_NS}.svc.cluster.local:9092"
  cat <<EOF

Fjord is up on kind cluster '${FJORD_CLUSTER}' (namespace ${FJORD_NS}).

In-cluster bootstrap:  ${bs}
Pre-created topic:     quickstart
Image:                 ${IMAGE_REF}
Chart:                 ${CHART}
Mode:                  ${FJORD_MODE}

Export kubeconfig for this cluster:
  kind export kubeconfig --name ${FJORD_CLUSTER}

Produce/consume from an in-cluster client:
  kubectl -n ${FJORD_NS} run kcat --rm -it --restart=Never \\
    --image=${KCAT_IMAGE} --command -- sh -c \\
    "printf 'hi\\n' | kcat -b ${bs} -t quickstart -P && kcat -b ${bs} -t quickstart -C -o beginning -e"

Teardown:
  helm uninstall fjord -n ${FJORD_NS}
  kind delete cluster --name ${FJORD_CLUSTER}

Public chart install (outside this script):
  helm install fjord ${PUBLIC_CHART_URL}
  # or: helm install fjord ${PUBLIC_CHART_OCI} --version ${FJORD_RELEASE}
EOF
}

cleanup() {
  if [[ -n "${KUBECONFIG_FILE:-}" && -f "${KUBECONFIG_FILE}" ]]; then
    rm -f "$KUBECONFIG_FILE"
  fi
}
trap cleanup EXIT

main() {
  ensure_tools
  resolve_repo
  resolve_chart
  ensure_cluster
  resolve_image
  load_image
  install_chart
  smoke
  print_next_steps
}

main "$@"
