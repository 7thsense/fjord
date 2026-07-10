#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CHART="${1:-$ROOT/deploy/helm/fjord}"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

if ! command -v helm >/dev/null 2>&1; then
  echo "helm is required" >&2
  exit 2
fi
if [[ ! -f "$CHART/Chart.yaml" ]]; then
  echo "Helm chart not found: $CHART" >&2
  exit 2
fi

helm lint "$CHART"
helm template docs-single "$CHART" \
  --namespace docs \
  --set autoscaling.enabled=false \
  --set image.repository=fjord \
  --set image.tag=docs > "$TMP/single.yaml"
helm template docs-multi "$CHART" \
  --namespace docs \
  --set mode=multiBroker \
  --set replicaCount=3 \
  --set autoscaling.enabled=false \
  --set image.repository=fjord \
  --set image.tag=docs > "$TMP/multi.yaml"

has_workload() {
  local file="$1"
  local expected_kind="$2"
  local expected_name="$3"

  awk -v expected_kind="$expected_kind" -v expected_name="$expected_name" '
    /^---$/ {
      if (kind == expected_kind && name == expected_name) found = 1
      kind = name = ""
      in_metadata = 0
      next
    }
    /^kind:/ { kind = $2 }
    /^metadata:$/ { in_metadata = 1; next }
    in_metadata && /^  name:/ { name = $2; in_metadata = 0 }
    END {
      if (kind == expected_kind && name == expected_name) found = 1
      exit !found
    }
  ' "$file"
}

if ! has_workload "$TMP/single.yaml" Deployment docs-single-fjord; then
  echo "singleLogical render is missing Deployment/docs-single-fjord" >&2
  exit 1
fi
if has_workload "$TMP/single.yaml" StatefulSet docs-single-fjord; then
  echo "singleLogical render unexpectedly contains a StatefulSet" >&2
  exit 1
fi
if ! has_workload "$TMP/multi.yaml" StatefulSet docs-multi-fjord; then
  echo "multiBroker render is missing StatefulSet/docs-multi-fjord" >&2
  exit 1
fi
if grep -q '^kind: HorizontalPodAutoscaler$' "$TMP/multi.yaml"; then
  echo "multiBroker render unexpectedly contains an HPA" >&2
  exit 1
fi

echo "Helm chart checks passed: $CHART"
