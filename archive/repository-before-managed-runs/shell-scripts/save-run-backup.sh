#!/usr/bin/env bash

set -e

###############################
## Configuration
###############################

pod_labels=("app=producer-sts" "app=consumer-sts" "app=merge-sts")
containers=("producer-container" "consumer-container" "merge-container")
results_paths=("" "/app/consumer-merge-data/consumer-result" "/app/merged-data/merge-result /app/merged-data/merge-metrics")
logs_paths=("/app/producer-data/logs" "/app/consumer-merge-data/logs" "/app/merged-data/logs")

declare -A pod_configs=(
  ["producer"]="producer-sts-0:producer-container:/app/producer-data:./src/producer"
  ["consumer"]="consumer-sts-0:consumer-container:/app/consumer-merge-data:./src/consumer"
  ["merge"]="merge-sts-0:merge-container:/app/merged-data:./src/merge"
)

ordered_keys=("producer" "consumer" "merge")
file_extensions=("py" "sh" "yaml" "yml" "properties")

###############################
## Functions
###############################

save_codes() {
  echo "==================== Saving Codes from All Pods ===================="
  mkdir -p ./src/producer ./src/consumer ./src/merge

  for key in "${ordered_keys[@]}"; do
    IFS=':' read -r pod container src_path dst_path <<< "${pod_configs[$key]}"
    echo "Copying from $pod (container: $container, path: $src_path) to $dst_path"
    mkdir -p "$dst_path"
    for ext in "${file_extensions[@]}"; do
      files=$(kubectl exec "$pod" -c "$container" -- find "$src_path" -type f -name "*.${ext}" 2>/dev/null || true)
      for file in $files; do
        rel_path="${file#$src_path/}"
        local_dir="$dst_path/$(dirname "$rel_path")"
        mkdir -p "$local_dir"
        kubectl cp "$pod:$file" "$dst_path/$rel_path" -c "$container" 2>/dev/null || echo "Warning: Failed to copy $file"
      done
    done
    echo "Completed copying for $key."
  done

  echo "Copying pipeline-configmap.yaml..."
  kubectl cp merge-sts-0:/config/pipeline-configmap.yaml ./src/pipeline-configmap.yaml 2>/dev/null || true

  if [ -f ./src/pipeline-configmap.yaml ]; then
    echo "Saved pipeline-configmap.yaml to ./src/pipeline-configmap.yaml"
  else
    echo "Warning: Failed to copy pipeline-configmap.yaml. Check if it is mounted."
  fi
  echo "==================================================================="
}

process_pods() {
  # Create timestamped folders inside ./results and ./logs
  timestamp=$(date +"%Y%m%d_%H%M%S")
  results_root="./results/$timestamp"
  logs_root="./logs/$timestamp"

  mkdir -p "$results_root/producer" "$results_root/consumer" "$results_root/merge"
  mkdir -p "$logs_root/producer" "$logs_root/consumer" "$logs_root/merge"

  read -n 1 -p "Save results before running the script? (y/n): " save_results
  echo
  [[ "$save_results" != "y" ]] && save_results="n"

  read -n 1 -p "Delete results before running the script? (y/n): " delete_results
  echo
  [[ "$delete_results" != "y" ]] && delete_results="n"

  read -n 1 -p "Save logs before running the script? (y/n): " save_logs
  echo
  [[ "$save_logs" != "y" ]] && save_logs="n"

  read -n 1 -p "Delete logs before running the script? (y/n): " delete_logs
  echo
  [[ "$delete_logs" != "y" ]] && delete_logs="n"

  read -n 1 -p "Run ./runsynthetic.sh inside all pods now? (y/n): " run_scripts
  echo
  [[ "$run_scripts" != "y" ]] && run_scripts="n"

  for i in "${!pod_labels[@]}"; do
    label=${pod_labels[$i]}
    container=${containers[$i]}
    pod_type=$(echo $label | cut -d= -f2 | cut -d- -f1)
    echo "==================== Processing $pod_type Pods ===================="

    pods=$(kubectl get pods -l $label -o jsonpath='{.items[*].metadata.name}')
    pod_id=0
    for pod in $pods; do
      echo "Pod: $pod ($container)"

      if [[ "$save_results" == "y" && -n "${results_paths[$i]}" ]]; then
        for remote_path in ${results_paths[$i]}; do
          if [[ "$remote_path" == *metrics ]]; then
            suffix="-metrics"
          else
            suffix="-result"
          fi
          echo "Saving $remote_path from $pod..."
          kubectl cp "$pod:$remote_path" "$results_root/$pod_type/${pod}${suffix}" -c $container || echo "Warning: Failed to copy $remote_path from $pod"
        done
      else
        echo "Skipping result save for $pod_type."
      fi

      if [[ "$delete_results" == "y" && -n "${results_paths[$i]}" ]]; then
        for remote_path in ${results_paths[$i]}; do
          echo "Deleting $remote_path in $pod..."
          kubectl exec -c $container $pod -- rm -rf "$remote_path" || echo "Warning: Failed to delete $remote_path in $pod"
        done
      else
        echo "Skipping result deletion for $pod_type."
      fi

      if [[ "$save_logs" == "y" ]]; then
        log_remote_path="${logs_paths[$i]}"
        echo "Saving logs from $log_remote_path in $pod..."

        tmp_log_dir=$(mktemp -d)
        kubectl cp "$pod:$log_remote_path/." "$tmp_log_dir" -c $container || echo "Warning: Failed to copy logs from $pod"

        for log_file in "$tmp_log_dir"/*; do
          log_name=$(basename "$log_file")
          mv "$log_file" "$logs_root/$pod_type/${pod}_${log_name}"
        done
        rm -rf "$tmp_log_dir"
      else
        echo "Skipping log save for $pod_type."
      fi

      if [[ "$delete_logs" == "y" ]]; then
        log_remote_path="${logs_paths[$i]}"
        echo "Deleting logs in $log_remote_path in $pod..."
        kubectl exec -c $container $pod -- rm -rf "$log_remote_path/*" || echo "Warning: Failed to delete logs in $pod"
      else
        echo "Skipping log deletion for $pod_type."
      fi

      if [[ "$run_scripts" == "y" ]]; then
        echo "Force-killing any existing Python and shell script processes inside $pod before starting ./runsynthetic.sh ..."
        kubectl exec -c $container $pod -- sh -c "ps -eo pid,args | grep python | grep '\.py' | grep -v grep | awk '{print \$1}' | xargs -r kill -9 || echo 'No python scripts found.'"
        kubectl exec -c $container $pod -- sh -c "ps -eo pid,args | grep '\.sh' | grep -v grep | awk '{print \$1}' | xargs -r kill -9 || echo 'No .sh scripts found.'"

        echo "Starting ./runsynthetic.sh $pod_id inside $pod in detached mode..."
        if ! kubectl exec -c $container $pod -- sh -c "setsid ./runsynthetic.sh '$pod_id' > /dev/null 2>&1 < /dev/null &"; then
          echo "Warning: Failed to start ./runsynthetic.sh in $pod"
        fi
      else
        echo "Skipping ./runsynthetic.sh for $pod_type."
      fi

      pod_id=$((pod_id+1))
    done
    echo "==================================================================="
  done

  echo "✅ Results are saved under: $results_root"
  echo "✅ Logs are saved under: $logs_root"
}

###############################
## Main Execution
###############################

echo "==================== Starting Code Backup and Pod Processing ===================="

read -n 1 -p "Save codes from all pods before running the script? (y/n): " save_codes_choice
echo
[[ "$save_codes_choice" != "y" ]] && save_codes_choice="n"

if [[ "$save_codes_choice" == "y" ]]; then
    save_codes
else
    echo "Skipping code backup step."
fi

process_pods

echo "==================== All operations completed successfully. ===================="

