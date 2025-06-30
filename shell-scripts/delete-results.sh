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

delete_results() {
  local pod="$1"
  local container="$2"
  local target_path="$3"

  echo "Deleting files in $pod:$target_path (container: $container)"
  kubectl exec "$pod" -c "$container" -- rm -rf "$target_path"/*
}

# Confirm before deleting
echo "⚠️ WARNING: This will PERMANENTLY DELETE all result files inside the following paths in the pods:"
for key in "${ordered_keys[@]}"; do
  IFS=':' read -r pod container target_path dst_subdir <<< "${result_configs[$key]}"
  echo " - $pod:$target_path (container: $container)"
done

read -p "Are you sure you want to continue? (y/N): " confirm
if [[ "$confirm" != "y" && "$confirm" != "Y" ]]; then
  echo "Aborted."
  exit 1
fi

# Perform deletion in order
for key in "${ordered_keys[@]}"; do
  IFS=':' read -r pod container target_path dst_subdir <<< "${result_configs[$key]}"
  delete_results "$pod" "$container" "$target_path"
done

echo "✅ Deletion completed."

