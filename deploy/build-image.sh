#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Build the fjord broker image.
#
# fjord depends on heimq and object-log as git dependencies (public easel repos),
# so the build context is just this repo — no sibling assembly needed. cargo
# fetches the deps during the build. .dockerignore keeps the context lean.
#
#   ./deploy/build-image.sh [tag]      # default tag: fjord:dev
set -euo pipefail

TAG="${1:-fjord:dev}"
FJORD_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

echo "building image $TAG from $FJORD_ROOT ..."
docker build -t "$TAG" -f "$FJORD_ROOT/Dockerfile" "$FJORD_ROOT"
echo "built $TAG"
