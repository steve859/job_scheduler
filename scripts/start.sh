#!/usr/bin/env bash
set -euo pipefail
pushd "$(dirname "$0")/.." >/dev/null
( cd infra && docker compose up -d --build )
popd >/dev/null
echo "[start] Stack started"
