#!/bin/bash

set -euo pipefail

source parseYaml.sh
eval $(parse_yaml /config/pipeline-configmap.yaml)

pod_index=${1:-0}

watch_dir="${CONSUMER_OUTPUT_DIR}"    #:-"/app/consumer-merge-data/consumer-result"}"
processed_dir="${CONSUMER_OUTPUT_DIR}/processed/"
merged_dir="${MERGE_OUTPUT_DIR}"     #:-"./merge-result/"}"
metrics_dir="${MERGE_METRICS_DIR}"   #:-"./merge-result/"}"
min_files="${MERGE_MIN_FILES:-5}"
interval_sec="${MERGE_INTERVAL:-10}"

CURRENT_DATE=$(TZ=America/Denver date +"%Y-%m-%d")
CURRENT_TIME=$(TZ=America/Denver date +"%H-%M-%S")
log_path="./logs/merge/${CURRENT_DATE}/${CURRENT_TIME}"
mkdir -p "$log_path"
mkdir -p "$merged_dir"
mkdir -p "$metrics_dir"

log_file="${log_path}/merge.log"

echo "[INIT] Starting merge process..." | tee -a "$log_file"
echo "[INFO] Watch dir: $watch_dir" | tee -a "$log_file"
echo "[INFO] Processed dir: $processed_dir" | tee -a "$log_file"
echo "[INFO] Merged dir: $merged_dir" | tee -a "$log_file"
echo "[INFO] Minimum files: $min_files, Interval: $interval_sec" | tee -a "$log_file"
echo "[INFO] Logging to $log_file" | tee -a "$log_file"

# Trap clean shutdown on Ctrl+C or SIGTERM
trap "echo '[INFO] Caught interrupt signal. Stopping merger.' | tee -a \"$log_file\"; exit 0" INT TERM

# Use exec to replace shell with Python process for proper signal handling,
# and redirect stdout/stderr to log file.
exec python3 confluent_merge.py \
    --watchDir "$watch_dir" \
    --processedDir "$processed_dir" \
    --mergedDir "$merged_dir" \
    --metricsDir "$metrics_dir" \
    --minFiles "$min_files" \
    --intervalSec "$interval_sec" # \
    #2>&1 | tee -a "$log_file"

