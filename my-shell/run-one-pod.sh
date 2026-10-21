#!/usr/bin/env bash
# Open a shell in a producer or consumer pod.
set -euo pipefail

role=${1:-}
ordinal=${2:-0}
if [[ "$role" != producer && "$role" != consumer ]]; then
  echo "Usage: $0 producer|consumer [ordinal]" >&2
  exit 1
fi
script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
deployment=$(PYTHONPATH="$script_dir" python3 -c 'import sys; from placement_control import APP_NAMES; print(APP_NAMES[sys.argv[1]])' "$role")
kubectl -n "${NAMESPACE:-kafkastreamingdata}" exec -it "${deployment}-${ordinal}" \
  -c "${role}-container" -- bash
