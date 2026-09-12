#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
export PIPELINE_APP_DIR="$SCRIPT_DIR"
LAUNCH="$SCRIPT_DIR/launch.py"
if [[ ! -f "$LAUNCH" ]]; then
  LAUNCH="$SCRIPT_DIR/../common/launch.py"
  export PYTHONPATH="$SCRIPT_DIR/../common:${PYTHONPATH:-}"
fi
# exec lets the application finish its own cleanup when it receives SIGTERM.
exec python3 "$LAUNCH" consumer
