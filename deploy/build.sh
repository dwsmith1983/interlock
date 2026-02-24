#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DIST_DIR="$REPO_ROOT/deploy/dist"
LAMBDA_HANDLERS=(stream-router evaluator orchestrator trigger run-checker watchdog)

echo "==> Cleaning dist directory"
rm -rf "$DIST_DIR"

echo "==> Building Lambda handlers for linux/arm64"
for handler in "${LAMBDA_HANDLERS[@]}"; do
    out="$DIST_DIR/lambda/$handler/bootstrap"
    mkdir -p "$(dirname "$out")"
    CGO_ENABLED=0 GOOS=linux GOARCH=arm64 \
        go build -tags lambda.norpc -ldflags="-s -w" \
        -o "$out" "$REPO_ROOT/cmd/lambda/$handler"
    echo "    built $handler"
done

echo "==> Staging archetype layer"
mkdir -p "$DIST_DIR/layer/archetypes"
if ls "$REPO_ROOT/archetypes/"*.yaml 1>/dev/null 2>&1; then
    cp "$REPO_ROOT/archetypes/"*.yaml "$DIST_DIR/layer/archetypes/"
    echo "    copied $(ls "$DIST_DIR/layer/archetypes/" | wc -l | tr -d ' ') archetype files"
else
    echo "    no archetype YAML files found in $REPO_ROOT/archetypes/"
fi

echo "==> Build complete"
ls -la "$DIST_DIR/lambda/"*/bootstrap
