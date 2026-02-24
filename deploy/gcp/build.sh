#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
DIST_DIR="$REPO_ROOT/deploy/gcp/dist"
SOURCE_DIR="$DIST_DIR/source"

echo "==> Cleaning dist directory"
rm -rf "$DIST_DIR"
mkdir -p "$SOURCE_DIR"

echo "==> Staging Go source for Cloud Functions"
# Copy module files
cp "$REPO_ROOT/go.mod" "$REPO_ROOT/go.sum" "$SOURCE_DIR/"

# Copy source directories needed by Cloud Functions
for dir in cmd/cloudfunction internal pkg; do
    mkdir -p "$SOURCE_DIR/$(dirname "$dir")"
    cp -r "$REPO_ROOT/$dir" "$SOURCE_DIR/$dir"
done

echo "==> Staging archetypes"
mkdir -p "$SOURCE_DIR/archetypes"
if ls "$REPO_ROOT/archetypes/"*.yaml 1>/dev/null 2>&1; then
    cp "$REPO_ROOT/archetypes/"*.yaml "$SOURCE_DIR/archetypes/"
    echo "    copied $(ls "$SOURCE_DIR/archetypes/" | wc -l | tr -d ' ') archetype files"
else
    echo "    no archetype YAML files found in $REPO_ROOT/archetypes/"
fi

echo "==> Build complete"
echo "    Source staged at: $SOURCE_DIR"
echo ""
echo "    Next steps:"
echo "      cd deploy/gcp/terraform"
echo "      terraform init -backend-config=backend.hcl"
echo "      terraform plan"
echo "      terraform apply"
