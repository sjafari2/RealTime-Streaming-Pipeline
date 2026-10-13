#!/bin/bash
set -euo pipefail

root_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
role=${1:-}
if [[ "$role" != producer && "$role" != consumer ]] || (( $# > 2 )); then
  echo "Usage: bash my-shell/deploy-sts.sh producer|consumer [--separate-brokers]" >&2
  exit 2
fi
if (( $# == 2 )) && [[ "$2" != --separate-brokers ]]; then
  echo "Unknown option: $2" >&2
  exit 2
fi
shift
# Run between experiments after reviewing the manifest and existing volumes.
python3 "$root_dir/my-shell/node_placement.py" \
  "$root_dir/k8s/statefullsets/$role-sts.yaml" "$@" | kubectl apply -f -
