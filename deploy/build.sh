#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
DIST_DIR="$SCRIPT_DIR/dist"
LAMBDA_HANDLERS=(stream-router orchestrator sla-monitor watchdog event-sink alert-dispatcher)

echo "==> Cleaning dist directory"
rm -rf "$DIST_DIR"

echo "==> Building Lambda handlers for linux/arm64"
for handler in "${LAMBDA_HANDLERS[@]}"; do
    out="$DIST_DIR/$handler/bootstrap"
    mkdir -p "$(dirname "$out")"
    CGO_ENABLED=0 GOOS=linux GOARCH=arm64 \
        go build -tags lambda.norpc -ldflags="-s -w" \
        -o "$out" "$ROOT_DIR/cmd/lambda/$handler"
    echo "    built $handler"
done

echo "==> Build complete"
ls -la "$DIST_DIR/"*/bootstrap
