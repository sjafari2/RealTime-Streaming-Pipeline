#!/usr/bin/env bash

set -euo pipefail
trap "exit" INT TERM
trap "kill 0" EXIT

# Path to mounted ConfigMap
CONFIG_FILE="/config/pipeline-configmap.yaml"

# Export all entries under .data as plain environment variables
# e.g., TARGET_RATE="1000", EXP_ID="B0", TRAFFIC_MODE="balanced", etc.
eval "$(
  yq eval '.data | to_entries | map("export " + .key + "=" + (.value | @sh)) | .[]' "$CONFIG_FILE"
)"

# Prepare logging directory
CURRENT_DATE=$(TZ=America/Denver date +"%Y-%m-%d")
CURRENT_TIME=$(TZ=America/Denver date +"%H-%M-%S")
log_path="./logs/producer/${CURRENT_DATE}/${CURRENT_TIME}"
mkdir -p "$log_path"

pod_name=$(hostname)

echo "Producer pod: ${pod_name}"
echo "Config: EXP_ID=${EXP_ID:-unknown}, TRAFFIC_MODE=${TRAFFIC_MODE:-balanced}, TARGET_RATE=${TARGET_RATE:-unset}"

# Run producer – all configuration is now read via os.getenv() inside Python
python3 producer.py \
  2>&1 | tee "${log_path}/producer_${pod_name}.log"

