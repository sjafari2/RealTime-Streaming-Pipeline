#!/bin/bash

source parseYaml.sh
eval $(parse_yaml pipeline-configmap.yaml)

pod_index=${1:-0}
watch_dir=${data_SHARED_MERGE_DIR}/Pod_${pod_index}"
processed_dir=${watch_dir}/${data_PROCESSED_CSV_DIR}
merged_dir=${watch_dir}/${data_MERGED_CSV_DIR}
min_files=${data_MERGE_MIN_FILES:-4}
interval_sec=${data_MERGE_INTERVAL:-10}

CURRENT_DATE=$(TZ=America/Denver date +"%Y-%m-%d")
CURRENT_TIME=$(TZ=America/Denver date +"%H-%M-%S")
log_path="./logs/merge/${CURRENT_DATE}/${CURRENT_TIME}"
mkdir -p "$log_path"

echo "🔁 Starting merge process..."
python3 synthetic_merge_by_time.py \
    --watchDir "$watch_dir" \
    --processedDir "$processed_dir" \
    --mergedDir "$merged_dir" \
    --minFiles "$min_files" \
    --intervalSec "$interval_sec" \
    > "${log_path}/merge.log" 2>&1 &

