#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
DIST_DIR="$SCRIPT_DIR/dist"
LAMBDA_HANDLERS=(stream-router orchestrator sla-monitor watchdog event-sink alert-dispatcher)

# Detect host architecture so LocalStack's Lambda executor can run the binaries natively
ARCH="$(uname -m)"
case "$ARCH" in
    arm64|aarch64)
        GOARCH=arm64
        ;;
    *)
        GOARCH=amd64
        ;;
esac

echo "==> Host arch: $ARCH -> GOARCH=$GOARCH"
echo "==> Cleaning dist directory"
rm -rf "$DIST_DIR"
mkdir -p "$DIST_DIR"

echo "==> Building Lambda handlers for linux/$GOARCH"
for handler in "${LAMBDA_HANDLERS[@]}"; do
    out_dir="$DIST_DIR/$handler"
    mkdir -p "$out_dir"
    CGO_ENABLED=0 GOOS=linux GOARCH="$GOARCH" \
        go build -tags lambda.norpc -ldflags="-s -w" \
        -o "$out_dir/bootstrap" "$ROOT_DIR/cmd/lambda/$handler"

    # Zip the bootstrap binary (must be at the root of the zip, not in a subdir)
    (cd "$out_dir" && zip -q "../$handler.zip" bootstrap)
    echo "    built $handler"
done

echo "==> Build complete"
ls -lh "$DIST_DIR"/*.zip
