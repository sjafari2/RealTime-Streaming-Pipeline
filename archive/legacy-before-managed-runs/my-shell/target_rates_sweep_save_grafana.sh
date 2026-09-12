#!/usr/bin/env bash
set -euo pipefail
# Pass rates explicitly, for example: --rates 1000 2000 3000 --repetitions 5
exec "${PYTHON:-python3}" "$(dirname "$0")/run_experiment.py" sweep "$@"
