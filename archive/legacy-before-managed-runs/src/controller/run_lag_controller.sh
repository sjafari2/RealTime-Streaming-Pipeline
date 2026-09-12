#!/usr/bin/env bash
set -euo pipefail
trap "exit" INT TERM

# Path to mounted ConfigMap
CONFIG_FILE="/config/pipeline-configmap.yaml"

# Export all entries under .data as environment variables
eval "$(
  yq eval '.data | to_entries | map("export " + .key + "=" + (.value | @sh)) | .[]' "$CONFIG_FILE"
)"

# Optional: controller-specific defaults can also be overridden here via env
# export DELTA_SECONDS=15
# export SKEW_THRESHOLD=10.0
# export COOLDOWN_SECONDS=60

# Prepare logging directory
CURRENT_DATE=$(TZ=America/Denver date +"%Y-%m-%d")
CURRENT_TIME=$(TZ=America/Denver date +"%H-%M-%S")
log_path="/app/controller-data/logs/${CURRENT_DATE}/${CURRENT_TIME}"
mkdir -p "$log_path"

pod_name=$(hostname)

echo "Controller pod: ${pod_name}"
echo "Config: EXP_ID=${EXP_ID:-unknown}, TRAFFIC_MODE=${TRAFFIC_MODE:-balanced}, TARGET_RATE=${TARGET_RATE:-unset}"

# Run controller – all configuration is now read via os.getenv() inside Python
python3 lag_controller.py \
  2>&1 | tee "${log_path}/controller_${pod_name}.log"

