#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
DIST_DIR="$SCRIPT_DIR/dist"
LAMBDA_HANDLERS=(stream-router orchestrator sla-monitor watchdog event-sink alert-dispatcher)

# EXTRA_TAGS (e.g. EXTRA_TAGS=e2e) adds comma-separated build tags after lambda.norpc.
if [[ "${EXTRA_TAGS:-}" =~ [[:space:]] ]]; then
    echo "build.sh: EXTRA_TAGS must be comma-separated with no whitespace, got '$EXTRA_TAGS'" >&2
    exit 1
fi
BUILD_TAGS="lambda.norpc${EXTRA_TAGS:+,$EXTRA_TAGS}"

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

echo "==> Building Lambda handlers for linux/$GOARCH (tags: $BUILD_TAGS)"
for handler in "${LAMBDA_HANDLERS[@]}"; do
    out_dir="$DIST_DIR/$handler"
    mkdir -p "$out_dir"
    CGO_ENABLED=0 GOOS=linux GOARCH="$GOARCH" \
        go build -tags "$BUILD_TAGS" -ldflags="-s -w" \
        -o "$out_dir/bootstrap" "$ROOT_DIR/cmd/lambda/$handler"

    # Zip the bootstrap binary (must be at the root of the zip, not in a subdir)
    (cd "$out_dir" && zip -q "../$handler.zip" bootstrap)
    echo "    built $handler"
done

echo "==> Build complete"
ls -lh "$DIST_DIR"/*.zip
