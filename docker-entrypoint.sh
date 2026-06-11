#!/bin/bash
set -e
cd /spellbreak-server

PATCH_ENV=${PATCH_ENV:-prod}
PATCH_DIR="/spellbreak-server/BaseServer/g3/Content/Paks"
mkdir -p "$PATCH_DIR"
mkdir -p /spellbreak-server/data

if [ "$PATCH_ENV" = "vanilla" ]; then
    echo "[entrypoint] Skipping patch (PATCH_ENV=vanilla)"
else
    if [ "$PATCH_ENV" = "dev" ]; then
        PATCH_SRC="${PATCH_TEST_URL:-http://cdn.elefrac.com/patch/dev.zip}"
    else
        PATCH_SRC="${PATCH_URL:-http://cdn.elefrac.com/patch/latest.zip}"
    fi
    echo "[entrypoint] Downloading patch from $PATCH_SRC..."
    if curl -fSL "$PATCH_SRC" -o "$PATCH_DIR/patch.zip"; then
        unzip -o "$PATCH_DIR/patch.zip" -d "$PATCH_DIR"
        rm -f "$PATCH_DIR/patch.zip"
        echo "[entrypoint] Patch applied."
    else
        echo "[entrypoint] WARNING: Patch download failed — starting without patch."
    fi
fi

exec python3 -m elefrac
