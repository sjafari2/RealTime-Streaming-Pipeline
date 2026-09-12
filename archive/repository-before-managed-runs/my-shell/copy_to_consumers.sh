#!/usr/bin/env bash
set -euo pipefail

NS="kafkastreamingdata"
POD_PREFIX="consumer-sts"
START=0
END=7

# If your pod has a specific container name, set it here (else leave empty).
CONTAINER="consumer-container"   # e.g., "consumer"

SRC_PY="src/consumer/consumer.py"
SRC_SH="src/consumer/run.sh"
DEST_DIR="/app/consumer-merge-data"

for i in $(seq $START $END); do
  POD="${POD_PREFIX}-${i}"
  echo "==> Copying to ${POD} ..."

  if [[ -n "$CONTAINER" ]]; then
    kubectl -n "$NS" cp "$SRC_PY" "${POD}:${DEST_DIR}/consumer.py" -c "$CONTAINER"
    kubectl -n "$NS" cp "$SRC_SH" "${POD}:${DEST_DIR}/run.sh"      -c "$CONTAINER"
    kubectl -n "$NS" exec -c "$CONTAINER" -n "$NS" "$POD" -- chmod +x "${DEST_DIR}/run.sh"
  else
    kubectl -n "$NS" cp "$SRC_PY" "${POD}:${DEST_DIR}/consumer.py"
    kubectl -n "$NS" cp "$SRC_SH" "${POD}:${DEST_DIR}/run.sh"
    kubectl -n "$NS" exec -n "$NS" "$POD" -- chmod +x "${DEST_DIR}/run.sh"
  fi

  echo "   Done: ${POD}"
done

echo "All pods updated."

