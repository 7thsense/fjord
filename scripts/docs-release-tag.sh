#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MANIFEST="${FJORD_CAPABILITIES_MANIFEST:-$ROOT/docs/public/data/capabilities.json}"

if ! command -v jq >/dev/null 2>&1; then
  echo "jq is required to read $MANIFEST" >&2
  exit 2
fi
if [[ ! -f "$MANIFEST" ]]; then
  echo "capability manifest not found: $MANIFEST" >&2
  exit 2
fi

release="$(jq -er '.documented_release | select(type == "string" and length > 0)' "$MANIFEST")"
if [[ ! "$release" =~ ^v[0-9]+\.[0-9]+\.[0-9]+([.-][0-9A-Za-z.-]+)?$ ]]; then
  echo "invalid documented_release in $MANIFEST: $release" >&2
  exit 2
fi

printf '%s\n' "$release"
