#!/usr/bin/env bash
set -euo pipefail
# With no arguments I run one experiment, wait, collect and analyze it.
# Explicit actions (start, stop, status, collect, export, backup) remain for troubleshooting.
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
exec "${PYTHON:-python3}" -u "$SCRIPT_DIR/run_experiment.py" "$@"
