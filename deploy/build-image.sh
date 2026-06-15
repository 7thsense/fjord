#!/usr/bin/env bash
# Build the fjord broker image. fjord depends on the sibling heimq and object-log
# repos via path deps, so the Docker context must contain all three. We assemble
# a clean temp context (excluding target/ and .git, which are large/irrelevant)
# rather than building from the parent dir (which holds many unrelated repos).
#
#   ./deploy/build-image.sh [tag]      # default tag: fjord:dev
set -euo pipefail

TAG="${1:-fjord:dev}"
FJORD_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROJECTS="$(cd "$FJORD_ROOT/.." && pwd)"

for repo in fjord heimq object-log; do
    if [[ ! -d "$PROJECTS/$repo" ]]; then
        echo "error: expected sibling repo $PROJECTS/$repo" >&2
        exit 1
    fi
done

CTX="$(mktemp -d)"
trap 'rm -rf "$CTX"' EXIT

echo "assembling build context in $CTX ..."
for repo in fjord heimq object-log; do
    rsync -a --exclude='target/' --exclude='.git/' "$PROJECTS/$repo/" "$CTX/$repo/"
done

echo "building image $TAG ..."
docker build -t "$TAG" -f "$CTX/fjord/Dockerfile" "$CTX"
echo "built $TAG"
