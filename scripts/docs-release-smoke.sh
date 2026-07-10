#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RELEASE="$("$ROOT/scripts/docs-release-tag.sh")"
MANIFEST="${FJORD_CAPABILITIES_MANIFEST:-$ROOT/docs/public/data/capabilities.json}"
TMP="$(mktemp -d)"
CHECKOUT="$TMP/release"
TARGET_DIR="${DOCS_CARGO_TARGET_DIR:-$ROOT/target/docs-release-smoke}"
BROKER_PID=""

cleanup() {
  if [[ -n "$BROKER_PID" ]]; then
    kill "$BROKER_PID" >/dev/null 2>&1 || true
    wait "$BROKER_PID" >/dev/null 2>&1 || true
  fi
  if [[ -d "$CHECKOUT" ]]; then
    git -C "$ROOT" worktree remove --force "$CHECKOUT" >/dev/null 2>&1 || true
  fi
  rm -rf "$TMP"
}
trap cleanup EXIT

git -C "$ROOT" rev-parse --verify --quiet "$RELEASE^{commit}" >/dev/null || {
  echo "documented release tag is unavailable locally: $RELEASE" >&2
  echo "fetch tags with: git fetch --tags origin" >&2
  exit 2
}

expected_commit="$(jq -er '.release_commit | select(type == "string" and length > 0)' "$MANIFEST")"
actual_commit="$(git -C "$ROOT" rev-parse "$RELEASE^{commit}")"
if [[ "$actual_commit" != "$expected_commit" ]]; then
  echo "$RELEASE resolves to $actual_commit, capability manifest records $expected_commit" >&2
  exit 1
fi

git -C "$ROOT" worktree add --detach "$CHECKOUT" "$RELEASE" >/dev/null

echo "Running release-tagged memory smoke at $RELEASE ($actual_commit)"
(
  cd "$CHECKOUT"
  CARGO_TARGET_DIR="$TARGET_DIR" \
    cargo test --locked -p fjord --test binary_smoke \
      binary_boots_and_serves_produce_consume -- --exact --nocapture
)

PORT="$(python3 -c 'import socket; sock = socket.socket(); sock.bind(("127.0.0.1", 0)); print(sock.getsockname()[1]); sock.close()')"
BROKER_LOG="$TMP/fjord.log"
RUST_LOG=warn "$TARGET_DIR/debug/fjord" \
  --host 127.0.0.1 \
  --port "$PORT" \
  --coordinator-url memory \
  --object-store memory >"$BROKER_LOG" 2>&1 &
BROKER_PID=$!

if ! "$ROOT/scripts/check-api-versions.py" \
  --bootstrap "127.0.0.1:$PORT" \
  --manifest "$MANIFEST" \
  --wait-seconds 20; then
  echo "release broker log:" >&2
  sed 's/^/  /' "$BROKER_LOG" >&2
  exit 1
fi

kill "$BROKER_PID" >/dev/null 2>&1 || true
wait "$BROKER_PID" >/dev/null 2>&1 || true
BROKER_PID=""
