#!/usr/bin/env bash

set -euo pipefail

# ------------------- CONFIG & EXPERIMENT ID -------------------
CONFIG_FILE="/config/pipeline-configmap.yaml"
source parseYaml.sh
eval $(parse_yaml "$CONFIG_FILE")

# Generate experiment ID with date-time and random suffix for uniqueness
EXPERIMENT_ID="exp_$(date +%Y%m%d_%H%M%S_%3N)"
export EXPERIMENT_ID

echo "[INIT] Experiment ID: $EXPERIMENT_ID"

# ------------------- PATHS -------------------
watch_dir="${CONSUMER_OUTPUT_DIR:-/app/consumer-merge-data/consumer-result}"
processed_dir="${watch_dir}/processed/"
merged_dir="${MERGE_OUTPUT_DIR:-/app/merged-data/merge-result}"
metrics_dir="${MERGE_METRICS_DIR:-/app/merged-data/merge-metrics}"

mkdir -p "$processed_dir" "$merged_dir" "$metrics_dir"

# ------------------- LOGGING -------------------
CURRENT_DATE=$(TZ=America/Denver date +"%Y-%m-%d")
CURRENT_TIME=$(TZ=America/Denver date +"%H-%M-%S")
log_path="./logs/merge/${EXPERIMENT_ID}_${CURRENT_DATE}_${CURRENT_TIME}"
mkdir -p "$log_path"
log_file="${log_path}/merge.log"

echo "[INIT] Starting merge process..." | tee -a "$log_file"
echo "[INFO] Experiment ID: $EXPERIMENT_ID" | tee -a "$log_file"
echo "[INFO] Watch Dir: $watch_dir" | tee -a "$log_file"
echo "[INFO] Processed Dir: $processed_dir" | tee -a "$log_file"
echo "[INFO] Merged Dir: $merged_dir" | tee -a "$log_file"
echo "[INFO] Metrics Dir: $metrics_dir" | tee -a "$log_file"

# ------------------- SIGNAL CLEANUP -------------------
trap "echo '[INFO] Caught interrupt signal. Stopping merger.' | tee -a \"$log_file\"; exit 0" INT TERM

# ------------------- EXECUTION -------------------
exec python3 confluent_merge.py \
    --watchDir "$watch_dir" \
    --processedDir "$processed_dir" \
    --mergedDir "$merged_dir" \
    --metricsDir "$metrics_dir" \
    --enableParquet true \
    --gevMetric application_consumer \
    --minBlocksForGEV 20 \
    --fitGEVIntervalSec 60 \
    2>&1 | tee -a "$log_file"

