#!/usr/bin/env bash
set -euo pipefail
# Run after save-run.sh stop. Source and launch helpers are shared on each role's PVC.
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
exec "${PYTHON:-python3}" "$ROOT/my-shell/sync_code.py" "$@"
