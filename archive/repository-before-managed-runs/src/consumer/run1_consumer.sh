#!/usr/bin/env bash
set -euo pipefail
trap "exit" INT TERM
trap "kill 0" EXIT

cd /app/consumer-merge-data

CONFIG_FILE="${PIPELINE_CONFIG:-/config/pipeline-configmap.yaml}"
[[ -f "$CONFIG_FILE" ]] || { echo "[consumer] ERROR: missing $CONFIG_FILE" >&2; exit 2; }

# export env vars from flat config
eval "$(
  yq eval -r '
    .data
    | to_entries
    | .[]
    | select(.key | test("^[A-Za-z_][A-Za-z0-9_]*$"))
    | "export " + .key + "=" + (.value | @sh)
  ' "$CONFIG_FILE"
)"

# IMPORTANT: consumer does NOT create topics here.
# It just reads RUN_TOPIC_PREFIX that was saved by create_topics.sh (called once elsewhere).
if [[ -z "${RUN_TOPIC_PREFIX:-}" ]]; then
  echo "[consumer] ERROR: RUN_TOPIC_PREFIX is empty. Run create_topics.sh first (once) to set it." >&2
  exit 3
fi

echo "[consumer] RUN_ID=${RUN_ID:-unset}"
echo "[consumer] RUN_TOPIC_PREFIX=${RUN_TOPIC_PREFIX}"
echo "[consumer] TOPIC_COUNT=${TOPIC_COUNT:-unset}"

# optional: kill old python in this container
pkill -f 'consumer\.py' 2>/dev/null || true

python3 consumer.py

