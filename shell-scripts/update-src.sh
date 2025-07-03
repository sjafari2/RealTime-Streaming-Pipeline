#!/usr/bin/env bash

# Define config: alias → "pod:container:src_path:dst_path"
declare -A pod_configs=(
  ["producer"]="producer-sts-0:producer-container:/app/producer-data:./src/producer"
  ["consumer"]="consumer-sts-0:consumer-container:/app/consumer-merge-data:./src/consumer"
 # ["application"]="consumer-application-sts-0:application-container:/app/app-merge-data:./src/application"
  ["merge"]="merge-sts-0:merge-container:/app/merged-data:./src/merge"
)

# Define processing order
ordered_keys=("producer" "consumer" "merge")

# Extensions to copy
file_extensions=("py" "sh" "yaml" "yml" "properties")

copy_files() {
  local pod="$1"
  local container="$2"
  local src_path="$3"
  local dst_path="$4"

  mkdir -p "$dst_path"
  for ext in "${file_extensions[@]}"; do
    files=$(kubectl exec "$pod" -c "$container" -- find "$src_path" -type f -name "*.${ext}" 2>/dev/null)
    for file in $files; do
      rel_path="${file#$src_path/}"
      local_dir="$dst_path/$(dirname "$rel_path")"
      mkdir -p "$local_dir"
      kubectl cp "$pod:$file" "$dst_path/$rel_path" -c "$container" 2>/dev/null
    done
  done
}

list_and_count_files() {
  local path="$1"
  echo "Files in $path:"
  find "$path" -type f
  count=$(find "$path" -type f | wc -l)
  echo "Total files: $count"
  echo "================================================================================="
}

# Main loop
for key in "${ordered_keys[@]}"; do
  IFS=':' read -r pod container src_path dst_path <<< "${pod_configs[$key]}"
  echo "🔄 Copying from $pod (container: $container, path: $src_path) to $dst_path"
  copy_files "$pod" "$container" "$src_path" "$dst_path"
  list_and_count_files "$dst_path"
done

