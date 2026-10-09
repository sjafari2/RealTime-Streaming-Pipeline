#!/usr/bin/env bash
# Open a shell in a producer or consumer pod.
set -euo pipefail

role=${1:-}
ordinal=${2:-0}
if [[ "$role" != producer && "$role" != consumer ]]; then
  echo "Usage: $0 producer|consumer [ordinal]" >&2
  exit 1
fi
kubectl -n "${NAMESPACE:-kafkastreamingdata}" exec -it "${role}-sts-${ordinal}" \
  -c "${role}-container" -- bash
