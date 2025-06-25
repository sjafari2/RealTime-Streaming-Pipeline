#!/usr/bin/env bash

# === Config: pod/container/src_path → destination subfolder name ===
declare -A result_configs=(
  ["consumer"]="consumer-application-sts-0:consumer-container:/app/consumer-app-data/consumer-result:consumer"
  ["application"]="consumer-application-sts-0:application-container:/app/app-merge-data/application-result:application"
  ["merge"]="merge-sts-0:merge-container:/app/merged-data/merge-result:merge"
  ["request"]="request-sts-0:request-container:/app/request-data/request-producer-data:request"
)

# === Define desired key order explicitly ===
ordered_keys=("consumer" "application" "merge" "request")

# === Create timestamped base results directory ===
timestamp=$(date +"%Y-%m-%d_%H-%M-%S")
base_dir="./results/${timestamp}"
mkdir -p "$base_dir"

copy_results() {
  local pod="$1"
  local container="$2"
  local src_path="$3"
  local dst_subdir="$4"

  local dst_path="${base_dir}/${dst_subdir}"
  mkdir -p "$dst_path"

  echo "Copying from $pod:$src_path (container: $container) to $dst_path"
  kubectl cp "$pod:$src_path" "$dst_path" -c "$container" 2>/dev/null
}

list_and_count_files() {
  local path="$1"
  echo "Files in $path:"
  find "$path" -type f
  count=$(find "$path" -type f | wc -l)
  echo "Total files: $count"
  echo "================================================================================="
}

# === Use ordered_keys to copy results in exact order ===
for key in "${ordered_keys[@]}"; do
  IFS=':' read -r pod container src_path dst_subdir <<< "${result_configs[$key]}"
  copy_results "$pod" "$container" "$src_path" "$dst_subdir"
  list_and_count_files "${base_dir}/${dst_subdir}"
done

echo "All results saved in: $base_dir"

