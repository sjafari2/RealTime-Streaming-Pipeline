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

CURRENT_DATE=$(TZ=America/Denver date +"%Y-%m-%d")
CURRENT_TIME=$(TZ=America/Denver date +"%H-%M-%S")

# ------------------- PATHS -------------------
watch_dir="${CONSUMER_OUTPUT_DIR:-/app/consumer-merge-data/consumer-result}"
processed_dir="${PROCESSED_DIR:-/app/consumer-merge-data/consumer-result/processed}"
merged_dir="${MERGE_OUTPUT_DIR:-/app/merged-data/merge-result}"
metrics_dir="${MERGE_METRICS_DIR:-/app/merged-data/merge-metrics}/${CURRENT_DATE}_${CURRENT_TIME}"
block_scope="${MERGE_BLOCK_SCOPE:- global}"
stop_at_blocks="${STOP_AT_BLOCKS}"
fit_gev_interval_sec="${FIT_GEV_INTERVAL_SEC}"
min_blocks_for_gev="${MIN_BLOCKS_FOR_GEV}"
## Block Scope can be either global or per_pod

mkdir -p "$processed_dir"  "${metrics_dir}" "${merged_dir}"

# ------------------- LOGGING -------------------
log_path="./logs/merge/${CURRENT_DATE}_${CURRENT_TIME}"
mkdir -p "$log_path"
log_file="${log_path}/merge.log"

#echo "[INIT] Starting merge process..." | tee -a "$log_file"
#echo "[INFO] Experiment ID: $EXPERIMENT_ID" | tee -a "$log_file"
#echo "[INFO] Watch Dir: $watch_dir" | tee -a "$log_file"
#echo "[INFO] Processed Dir: $processed_dir" | tee -a "$log_file"
#echo "[INFO] Merged Dir: $merged_dir" | tee -a "$log_file"
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
    #--gevMetric application_consumer \
    --minBlocksForGEV "$min_blocks_for_gev" \
    --fitGEVIntervalSec "$fit_gev_interval_sec" \
    --blockScope "$block_scope" \
    --stopAtBlocks "$stop_at_blocks" \
    2>&1 | tee -a "$log_file"

