#!/usr/bin/env bash
set -euo pipefail

###############################
## Configuration
###############################

pod_labels=("app=producer-sts" "app=consumer-sts" "app=merge-sts")  # order matters for indices
containers=("producer-container" "consumer-container" "merge-container")

# Results dirs (space-separated when multiple); empty string means "none"
results_paths=("" "/app/consumer-merge-data/consumer-result" "/app/merged-data/merge-metrics")

# LOGS: must have same length/order as pod_labels
logs_paths=("/app/producer-data/logs" "/app/consumer-merge-data/logs" "/app/merged-data/logs")

declare -A pod_configs=(
  ["producer"]="producer-sts-0:producer-container:/app/producer-data:./src/producer"
  ["consumer"]="consumer-sts-0:consumer-container:/app/consumer-merge-data:./src/consumer"
  ["merge"]="merge-sts-0:merge-container:/app/merged-data:./src/merge"
)

ordered_keys=("producer" "consumer" "merge")
file_extensions=("py" "sh" "yaml" "yml" "properties")

SINGLE_FOR_COPY="${SINGLE_FOR_COPY:-1}"  # 1 = do save/delete on first pod per type only

###############################
## Helpers
###############################

save_dir_stream() {
  local pod="$1" c="$2" rdir="$3" ldir="$4"
  mkdir -p "$ldir"
  if ! kubectl exec -c "$c" "$pod" -- sh -lc "test -d '$rdir'"; then
    echo "Skip: $rdir not found in $pod"
    return 0
  fi
  echo "Saving $pod:$rdir -> $ldir ..."
  kubectl exec -c "$c" "$pod" -- sh -lc "
    cd '$rdir' 2>/dev/null || exit 3
    if command -v pigz >/dev/null 2>&1; then
      tar -cf - . | pigz -1
    else
      tar -czf - .
    fi
  " | tar -C "$ldir" -xzf - || echo 'Warning: stream copy failed'
}

delete_dir_cephsafe() {
  local pod="$1" c="$2" rdir="$3"
  if ! kubectl exec -c "$c" "$pod" -- sh -lc "test -d '$rdir'"; then
    echo "Skip: $rdir not found in $pod"
    return 0
  fi
  echo "Deleting (Ceph-safe) $pod:$rdir ..."
  kubectl exec -c "$c" "$pod" -- sh -lc "
    find '$rdir' -type f -delete
    find '$rdir' -depth -type d -empty -delete
  " || echo 'Warning: delete failed'
}

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
        mkdir -p "$dst_path/$(dirname "$rel_path")"
        kubectl cp "$pod:$file" "$dst_path/$rel_path" -c "$container" 2>/dev/null || echo "Warning: Failed to copy $file"
      done
    done
    echo "Completed copying for $key."
  done

  echo "Copying pipeline-configmap.yaml..."
  kubectl cp producer-sts-0:/config/pipeline-configmap.yaml ./src/pipeline-configmap.yaml 2>/dev/null || true
  [[ -f ./src/pipeline-configmap.yaml ]] && echo "Saved pipeline-configmap.yaml" || echo "Warning: Failed to copy pipeline-configmap.yaml."
  echo "==================================================================="
}

process_pods() {
  local timestamp results_root logs_root
  timestamp=$(date +"%Y%m%d_%H%M%S")
  results_root="./results/$timestamp"
  logs_root="./logs/$timestamp"
  mkdir -p "$results_root/producer" "$results_root/consumer" "$results_root/merge"
  mkdir -p "$logs_root/producer" "$logs_root/consumer" "$logs_root/merge"

  read -n 1 -p "Save results before running the script? (y/n): " save_results; echo
  [[ "$save_results" != "y" ]] && save_results="n"
  read -n 1 -p "Delete results before running the script? (y/n): " delete_results; echo
  [[ "$delete_results" != "y" ]] && delete_results="n"
  read -n 1 -p "Save logs before running the script? (y/n): " save_logs; echo
  [[ "$save_logs" != "y" ]] && save_logs="n"
  read -n 1 -p "Delete logs before running the script? (y/n): " delete_logs; echo
  [[ "$delete_logs" != "y" ]] && delete_logs="n"
  read -n 1 -p "Run ./runsynthetic.sh inside pods? (y/n): " run_scripts; echo
  [[ "$run_scripts" != "y" ]] && run_scripts="n"

  # ===== Save/Delete phase (per type; once per type if SINGLE_FOR_COPY=1) =====
  for idx in "${!pod_labels[@]}"; do
    local label="${pod_labels[$idx]}"
    local container="${containers[$idx]}"
    local pod_type
    pod_type=$(echo "$label" | cut -d= -f2 | cut -d- -f1)
    echo "==================== Processing $pod_type (save/delete) ===================="

    local pods did_save_results did_delete_results did_save_logs did_delete_logs
    pods=$(kubectl get pods -l "$label" -o jsonpath='{.items[*].metadata.name}')
    did_save_results=0; did_delete_results=0; did_save_logs=0; did_delete_logs=0

    for pod in $pods; do
      [[ -z "$pod" ]] && continue

      if [[ "$save_results" == "y" && -n "${results_paths[$idx]}" ]]; then
        if [[ "$SINGLE_FOR_COPY" == "1" && "$did_save_results" -eq 1 ]]; then
          echo "Skip results save for $pod_type (already saved)."
        else
          for remote_path in ${results_paths[$idx]}; do
            [[ -z "$remote_path" ]] && continue
            local_dir="$results_root/$pod_type/${pod}_$(basename "$remote_path")"
            save_dir_stream "$pod" "$container" "$remote_path" "$local_dir"
          done
          did_save_results=1
        fi
      else
        echo "Skipping result save for $pod_type."
      fi

      if [[ "$delete_results" == "y" && -n "${results_paths[$idx]}" ]]; then
        if [[ "$SINGLE_FOR_COPY" == "1" && "$did_delete_results" -eq 1 ]]; then
          echo "Skip results deletion for $pod_type (already deleted)."
        else
          for remote_path in ${results_paths[$idx]}; do
            [[ -z "$remote_path" ]] && continue
            delete_dir_cephsafe "$pod" "$container" "$remote_path"
          done
          did_delete_results=1
        fi
      else
        echo "Skipping result deletion for $pod_type."
      fi

      if [[ "$save_logs" == "y" && -n "${logs_paths[$idx]}" ]]; then
        if [[ "$SINGLE_FOR_COPY" == "1" && "$did_save_logs" -eq 1 ]]; then
          echo "Skip log save for $pod_type (already saved)."
        else
          local_dir="$logs_root/$pod_type/${pod}_logs"
          save_dir_stream "$pod" "$container" "${logs_paths[$idx]}" "$local_dir"
          did_save_logs=1
        fi
      else
        echo "Skipping log save for $pod_type."
      fi

      if [[ "$delete_logs" == "y" && -n "${logs_paths[$idx]}" ]]; then
        if [[ "$SINGLE_FOR_COPY" == "1" && "$did_delete_logs" -eq 1 ]]; then
          echo "Skip log deletion for $pod_type (already deleted)."
        else
          delete_dir_cephsafe "$pod" "$container" "${logs_paths[$idx]}"
          did_delete_logs=1
        fi
      else
        echo "Skipping log deletion for $pod_type."
      fi

      # Only first pod if SINGLE_FOR_COPY=1
      if [[ "$SINGLE_FOR_COPY" == "1" && ( "$did_save_results" -eq 1 || "$did_delete_results" -eq 1 || "$did_save_logs" -eq 1 || "$did_delete_logs" -eq 1 ) ]]; then
        break
      fi
    done
    echo "==================================================================="
  done

  # ===== Run phase: consumers first, then producers =====
  if [[ "$run_scripts" == "y" ]]; then
    echo "===== Running scripts for all consumer pods first ====="
    for idx in "${!pod_labels[@]}"; do
      if [[ "${pod_labels[$idx]}" == "app=consumer-sts" ]]; then
        local container="${containers[$idx]}"
        local pods pod_id
        pods=$(kubectl get pods -l "${pod_labels[$idx]}" -o jsonpath='{.items[*].metadata.name}')
        pod_id=0
        for pod in $pods; do
          #echo "Force-killing processes in consumer pod: $pod"
          #kubectl exec -c "$container" "$pod" -- sh -lc "ps -eo pid,args | grep '\.py' | grep -v grep | awk '{print \$1}' | xargs -r kill -9 || true"
          #kubectl exec -c "$container" "$pod" -- sh -lc "ps -eo pid,args | grep '\.sh' | grep -v grep | awk '{print \$1}' | xargs -r kill -9 || true"
          echo "Starting ./runsynthetic.sh $pod_id in $pod ..."
          kubectl exec -c "$container" "$pod" -- sh -lc "setsid ./runsynthetic.sh '$pod_id' >/dev/null 2>&1 < /dev/null &"
          pod_id=$((pod_id+1))
        done
      fi
    done

    echo "===== Running scripts for all producer pods next ====="
    for idx in "${!pod_labels[@]}"; do
      if [[ "${pod_labels[$idx]}" == "app=producer-sts" ]]; then
        local container="${containers[$idx]}"
        local pods pod_id
        pods=$(kubectl get pods -l "${pod_labels[$idx]}" -o jsonpath='{.items[*].metadata.name}')
        pod_id=0
        for pod in $pods; do
          #echo "Force-killing processes in producer pod: $pod"
          #kubectl exec -c "$container" "$pod" -- sh -lc "ps -eo pid,args | grep '\.py' | grep -v grep | awk '{print \$1}' | xargs -r kill -9 || true"
          #kubectl exec -c "$container" "$pod" -- sh -lc "ps -eo pid,args | grep '\.sh' | grep -v grep | awk '{print \$1}' | xargs -r kill -9 || true"
          echo "Starting ./runsynthetic.sh $pod_id in $pod ..."
          kubectl exec -c "$container" "$pod" -- sh -lc "setsid ./runsynthetic.sh '$pod_id' >/dev/null 2>&1 < /dev/null &"
          pod_id=$((pod_id+1))
        done
      fi
    done
    echo "===== Running scripts for merge pod next ====="
    for idx in "${!pod_labels[@]}"; do
      if [[ "${pod_labels[$idx]}" == "app=merge-sts" ]]; then
        local container="${containers[$idx]}"
        local pods pod_id
        pods=$(kubectl get pods -l "${pod_labels[$idx]}" -o jsonpath='{.items[*].metadata.name}')
        pod_id=0
        for pod in $pods; do
          #echo "Force-killing processes in producer pod: $pod"
          #kubectl exec -c "$container" "$pod" -- sh -lc "ps -eo pid,args | grep '\.py' | grep -v grep | awk '{print \$1}' | xargs -r kill -9 || true"
          #kubectl exec -c "$container" "$pod" -- sh -lc "ps -eo pid,args | grep '\.sh' | grep -v grep | awk '{print \$1}' | xargs -r kill -9 || true"
          echo "Starting ./runsynthetic.sh $pod_id in $pod ..."
          kubectl exec -c "$container" "$pod" -- sh -lc "setsid ./runsynthetic.sh '$pod_id' >/dev/null 2>&1 < /dev/null &"
          pod_id=$((pod_id+1))
        done
      fi
    done
  else
    echo "Skipping script run."
  fi

  echo "✅ Results saved under: $results_root"
  echo "✅ Logs saved under: $logs_root"
}

###############################
## Main Execution
###############################

echo "==================== Starting Code Backup and Pod Processing ===================="

read -n 1 -p "Save codes from all pods before running the script? (y/n): " save_codes_choice; echo
[[ "$save_codes_choice" != "y" ]] && save_codes_choice="n"
if [[ "$save_codes_choice" == "y" ]]; then
  save_codes
else
  echo "Skipping code backup step."
fi

process_pods

echo "==================== All operations completed successfully. ===================="

