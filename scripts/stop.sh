#!/usr/bin/env bash
set -euo pipefail
pushd "$(dirname "$0")/.." >/dev/null
( cd infra && docker compose down -v )
popd >/dev/null
echo "[stop] Stack stopped & volumes removed"
