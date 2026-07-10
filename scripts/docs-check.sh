#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
STRICT="${DOCS_REQUIRE_TOOLS:-${CI:-false}}"

need_or_skip() {
  local tool="$1"
  if command -v "$tool" >/dev/null 2>&1; then
    return 0
  fi
  if [[ "$STRICT" == "true" || "$STRICT" == "1" ]]; then
    echo "$tool is required for documentation checks" >&2
    exit 2
  fi
  echo "Skipping $tool check because $tool is not installed" >&2
  return 1
}

if [[ -f "$ROOT/docs/public/book.toml" ]]; then
  "$ROOT/scripts/render-capabilities.sh" --check
  if need_or_skip mdbook; then
    mdbook build "$ROOT/docs/public"
  fi
else
  echo "Skipping mdBook build because docs/public/book.toml is absent" >&2
fi

if need_or_skip lychee; then
  markdown=(
    "$ROOT/README.md"
    "$ROOT/CHANGELOG.md"
    "$ROOT/CONTRIBUTING.md"
    "$ROOT/SECURITY.md"
  )
  while IFS= read -r -d '' file; do
    markdown+=("$file")
  done < <(find "$ROOT/docs/public/src" -type f -name '*.md' -print0)
  lychee --offline --no-progress "${markdown[@]}"
fi

if need_or_skip helm; then
  "$ROOT/scripts/docs-helm-check.sh"
fi

echo "Documentation checks passed"
