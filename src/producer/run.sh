#!/usr/bin/env bash
set -euo pipefail
trap "exit" INT TERM
trap "kill 0" EXIT

CONFIG_FILE="${PIPELINE_CONFIG:-/config/pipeline-configmap.yaml}"

# Export all config entries under .data as env vars
eval "$(
  yq eval '.data | to_entries | map("export " + .key + "=" + (.value | @sh)) | .[]' "$CONFIG_FILE"
)"

# Log cleanup (optional)
CLEAN_LOGS_ON_START="${CLEAN_LOGS_ON_START:-true}"
if [[ "${CLEAN_LOGS_ON_START}" == "true" ]]; then
  rm -rf ./logs/* || true
fi

CURRENT_DATE=$(TZ=America/Denver date +"%Y-%m-%d")
CURRENT_TIME=$(TZ=America/Denver date +"%H-%M-%S")
LOG_DIR="./logs/producer/${CURRENT_DATE}/${CURRENT_TIME}"
mkdir -p "$LOG_DIR"

POD_NAME="$(hostname)"
echo "[producer] pod=${POD_NAME}"
echo "[producer] config=${CONFIG_FILE}"
echo "[producer] EXP_ID=${EXP_ID:-unset} TRAFFIC_MODE=${TRAFFIC_MODE:-unset} TARGET_RATE=${TARGET_RATE:-unset}"

python3 producer.py 2>&1 | tee "${LOG_DIR}/producer_${POD_NAME}.log"

