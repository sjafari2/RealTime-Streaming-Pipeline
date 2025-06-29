#!/bin/bash

source parseYaml.sh
eval $(parse_yaml /config/pipeline-configmap.yaml)

pod_index=${1:-0}

watch_dir="${data_SHARED_MERGE_DIR:-"../app-merge-data/application-result"}"
processed_dir="${watch_dir}/${data_PROCESSED_CSV_DIR:-"processed/"}"
merged_dir="${data_MERGED_CSV_DIR:-"./merge-result/"}"
metrics_dir=${data_METRICS_CSV_DIR:-"./merge-result/"}
min_files="${data_MERGE_MIN_FILES:-5}"
interval_sec="${data_MERGE_INTERVAL:-10}"

CURRENT_DATE=$(TZ=America/Denver date +"%Y-%m-%d")
CURRENT_TIME=$(TZ=America/Denver date +"%H-%M-%S")
log_path="./logs/merge/${CURRENT_DATE}/${CURRENT_TIME}"
mkdir -p "$log_path"

echo "[INIT] Starting merge process..."
echo "[INFO] Watch dir: $watch_dir"
echo "[INFO] Processed dir: $processed_dir"
echo "[INFO] Merged dir: $merged_dir"
echo "[INFO] Minimum files: $min_files, Interval: $interval_sec"

python3 confluent_merge.py \
    --watchDir "$watch_dir" \
    --processedDir "$processed_dir" \
    --mergedDir "$merged_dir" \
    --metricsDir "$metrics_dir" \
    --minFiles "$min_files" \
    --intervalSec "$interval_sec" \
    >& "${log_path}/merge.log" 2>&1 &

