#!/usr/bin/env bash
set -euo pipefail
# Repeat the shared configuration; --rates optionally runs several target rates.
exec "${PYTHON:-python3}" -u "$(dirname "$0")/run_experiment.py" repeat "$@"
