#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# Best-effort: mark Fjord GHCR packages public so anonymous pulls work.
# Requires a token with packages:write (and package admin on the org package).
#
#   ./deploy/make-packages-public.sh
#   GHCR_OWNER=7thsense ./deploy/make-packages-public.sh
set -euo pipefail

OWNER="${GHCR_OWNER:-7thsense}"
PACKAGES=(
  "fjord"
  "charts/fjord"
)

if ! command -v gh >/dev/null 2>&1; then
  echo "gh is required" >&2
  exit 2
fi

for pkg in "${PACKAGES[@]}"; do
  echo "Setting visibility=public for container package $OWNER/$pkg ..."
  if gh api --method PATCH \
    -H "Accept: application/vnd.github+json" \
    "/orgs/${OWNER}/packages/container/${pkg}/visibility" \
    -f visibility=public >/dev/null; then
    echo "  ok: $pkg is public"
  else
    echo "  warn: could not change visibility for $pkg (need package admin + packages scope)" >&2
  fi
done

echo "Done. Verify with: docker pull ghcr.io/${OWNER}/fjord:0.1.5"
