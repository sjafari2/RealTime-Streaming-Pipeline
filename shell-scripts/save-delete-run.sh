#!/usr/bin/env bash

set -e

###############################
## Configuration
###############################

pod_labels=("app=producer-sts" "app=consumer-sts")
containers=("producer-container" "consumer-container")
results_paths=("" "/app/consumer-merge-data/consumer-result")
logs_paths=("/app/producer-data/logs" "/app/consumer-merge-data/logs")
script_dirs=("/app/producer-data" "/app/consumer-merge-data")

declare -A pod_configs=(
  ["producer"]="producer-sts-0:producer-container:/app/producer-data:./src/producer"
  ["consumer"]="consumer-sts-0:consumer-container:/app/consumer-merge-data:./src/consumer"
)

ordered_keys=("producer" "consumer")
file_extensions=("py" "sh" "yaml" "yml" "properties")

###############################
## Functions
###############################

save_codes() {
  echo "==================== Saving Codes from All Pods ===================="
  mkdir -p ./src/producer ./src/consumer

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

  echo "Copying /config/pipeline-configmap.yaml explicitly from producer pod..."
  if kubectl cp producer-sts-0:/config/pipeline-configmap.yaml ./src/pipeline-configmap.yaml -c producer-container 2>/dev/null; then
    echo "✅ Saved pipeline-configmap.yaml to ./src/pipeline-configmap.yaml"
  else
    echo "⚠️ Warning: Failed to copy /config/pipeline-configmap.yaml from producer pod. Check if it is mounted."
  fi
  echo "==================================================================="
}

process_pods() {
  timestamp=$(date +"%Y%m%d_%H%M%S")
  results_root="./results/$timestamp"
  logs_root="./logs/$timestamp"

  mkdir -p "$results_root/producer" "$results_root/consumer"
  mkdir -p "$logs_root/producer" "$logs_root/consumer"

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

  read -n 1 -p "Run ./runsynthetic.sh inside each pod now? (y/n): " run_scripts
  echo
  [[ "$run_scripts" != "y" ]] && echo "Skipping script execution in pods." && run_scripts="n"

  for i in "${!pod_labels[@]}"; do
    label=${pod_labels[$i]}
    container=${containers[$i]}
    script_dir=${script_dirs[$i]}
    pod_type=$(echo $label | cut -d= -f2 | cut -d- -f1)
    echo "==================== Processing $pod_type Pods ===================="

    pods=$(kubectl get pods -l $label -o jsonpath='{.items[*].metadata.name}')
    pod_id=0
    for pod in $pods; do
      echo "Pod: $pod ($container)"

      if [[ "$save_results" == "y" && -n "${results_paths[$i]}" ]]; then
        for remote_path in ${results_paths[$i]}; do
          folder_name=$(basename $remote_path)
          new_folder_name="${pod}-${folder_name}"
          new_folder_name="${new_folder_name//-data/}"
          echo "Saving $remote_path from $pod..."
          kubectl cp "$pod:$remote_path" "$results_root/$pod_type/${new_folder_name}" -c $container || echo "Warning: Failed to copy $remote_path from $pod"
        done
      fi

      if [[ "$delete_results" == "y" && -n "${results_paths[$i]}" ]]; then
        for remote_path in ${results_paths[$i]}; do
          echo "Deleting $remote_path in $pod..."
          kubectl exec -c $container $pod -- sh -c "rm -rf ${remote_path}" || echo "Warning: Failed to delete $remote_path in $pod"
        done
      fi

      if [[ "$save_logs" == "y" ]]; then
        log_remote_path="${logs_paths[$i]}"
        echo "Saving logs from $log_remote_path in $pod..."
        temp_log_dir=$(mktemp -d)
        kubectl cp "$pod:$log_remote_path" "$temp_log_dir" -c "$container" || echo "Warning: Failed to copy logs from $pod"
        log_files=($(find "$temp_log_dir" -type f))
        if [ ${#log_files[@]} -gt 0 ]; then
          cat "${log_files[@]}" > "$logs_root/$pod_type/${pod}.log"
        fi
        rm -rf "$temp_log_dir"
      fi

      if [[ "$delete_logs" == "y" ]]; then
        log_remote_path="${logs_paths[$i]}"
        echo "Deleting logs in $log_remote_path in $pod..."
        kubectl exec -c $container $pod -- sh -c "rm -rf ${log_remote_path}/*" || echo "Warning: Failed to delete logs in $pod"
      fi

      if [[ "$run_scripts" == "y" ]]; then
        echo "Killing previous Python/shell processes in $pod..."
        kubectl exec -c $container $pod -- sh -c "ps -eo pid,args | grep python | grep '\.py' | grep -v grep | awk '{print \$1}' | xargs -r kill -9 || true"
        kubectl exec -c $container $pod -- sh -c "ps -eo pid,args | grep '\.sh' | grep -v grep | awk '{print \$1}' | xargs -r kill -9 || true"

        echo "Running ./runsynthetic.sh $pod_id from $script_dir ..."
        kubectl exec -c $container $pod -- sh -c "
	echo '--- STARTING SCRIPT ---' > /tmp/start_debug.log;
  	cd $script_dir || { echo 'FAILED TO CD INTO $script_dir' >> /tmp/start_debug.log; exit 1; };
  	chmod +x runsynthetic.sh || echo 'CHMOD FAILED' >> /tmp/start_debug.log;
  	echo 'Launching with nohup' >> /tmp/start_debug.log;
  	nohup ./runsynthetic.sh '$pod_id' > /tmp/runsynthetic_${pod_id}.log 2>&1 &
  	echo \$! > /tmp/pid_$pod_id
"
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
[[ "$save_codes_choice" == "y" ]] && save_codes || echo "Skipping code backup step."

process_pods

echo "==================== All operations completed successfully. ===================="

