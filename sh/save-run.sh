#!/usr/bin/env bash
set -euo pipefail

########################################
# Config (adjust as needed)
########################################

# Optional: kubectl namespace (leave empty to use current context ns)
NAMESPACE="${NAMESPACE:-}"

# Run save/delete only once per role (first pod found)
SINGLE_FOR_COPY="${SINGLE_FOR_COPY:-1}"

# Roles in processing order for save/delete; run order is set below
roles=("producer" "consumer" "merge")

# Role → label selector
declare -A role_to_label=(
  ["producer"]="app=producer-sts"
  ["consumer"]="app=consumer-sts"
  ["merge"]="app=merge-sts"
)

# Role → container name
declare -A role_to_container=(
  ["producer"]="producer-container"
  ["consumer"]="consumer-container"
  ["merge"]="merge-container"
)

# Role → results paths (space-separated allows multiple) — empty means “none”
declare -A role_to_results=(
  ["producer"]=""
  ["consumer"]="/app/consumer-merge-data/consumer-result"
  ["merge"]="/app/merged-data/merge-metrics"
)

# Role → logs path (single path expected)
declare -A role_to_logs=(
  ["producer"]="/app/producer-data/logs"
  ["consumer"]="/app/consumer-merge-data/logs"
  ["merge"]="/app/merged-data/logs"
)

# For code backup from inside containers → local subfolders
declare -A pod_configs=(
  #   podName           :container         :srcPath                    :dstPath
  ["producer"]="producer-sts-0:producer-container:/app/producer-data:./src/producer"
  ["consumer"]="consumer-sts-0:consumer-container:/app/consumer-merge-data:./src/consumer"
  ["merge"]="merge-sts-0:merge-container:/app/merged-data:./src/merge"
)

file_extensions=("py" "sh" "yaml" "yml" "properties")

########################################
# Helpers
########################################

k() {
  # Wrapper for kubectl to pass namespace automatically if set
  if [[ -n "${NAMESPACE}" ]]; then
    kubectl -n "${NAMESPACE}" "$@"
  else
    kubectl "$@"
  fi
}

save_dir_stream() {
  local pod="$1" c="$2" rdir="$3" ldir="$4"
  mkdir -p "$ldir"
  if ! k exec -c "$c" "$pod" -- sh -lc "test -d '$rdir'"; then
    echo "Skip: $rdir not found in $pod"
    return 0
  fi
  echo "Saving $pod:$rdir -> $ldir ..."
  # Stream-compress inside pod; extract locally
  k exec -c "$c" "$pod" -- sh -lc "
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
  if ! k exec -c "$c" "$pod" -- sh -lc "test -d '$rdir'"; then
    echo "Skip: $rdir not found in $pod"
    return 0
  fi
  echo "Deleting (Ceph-safe) $pod:$rdir ..."
  k exec -c "$c" "$pod" -- sh -lc "
    find '$rdir' -type f -delete
    find '$rdir' -depth -type d -empty -delete
  " || echo 'Warning: delete failed'
}

# Return pods for a role, one per line, sorted "naturally" (…-2 before …-10)
list_pods_by_role() {
  local role="$1"
  local sel="${role_to_label[$role]}"
  [[ -z "$sel" ]] && return 0
  # Prefer -V (version sort); fall back to numeric on the suffix column
  if sort -V </dev/null >/dev/null 2>&1; then
    k get pods -l "$sel" -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' | sort -V
  else
    k get pods -l "$sel" -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' \
      | awk 'match($0,/-([0-9]+)$/,m){print m[1],$0}' \
      | sort -k1,1n \
      | cut -d' ' -f2-
  fi
}

# Extract the ordinal suffix from a StatefulSet pod name (e.g., consumer-sts-11 -> 11)
pod_ordinal() {
  local name="$1"
  if [[ "$name" =~ -([0-9]+)$ ]]; then
    echo "${BASH_REMATCH[1]}"
  else
    echo "0"
  fi
}

########################################
# Code backup (optional)
########################################

save_codes() {
  echo "==================== Saving Codes from All Pods ===================="
  mkdir -p ./src/producer ./src/consumer ./src/merge
  for role in "${roles[@]}"; do
    IFS=':' read -r pod container src_path dst_path <<< "${pod_configs[$role]}"
    echo "Copying from $pod (container: $container, path: $src_path) to $dst_path"
    mkdir -p "$dst_path"
    for ext in "${file_extensions[@]}"; do
      files=$(k exec "$pod" -c "$container" -- sh -lc "find '$src_path' -type f -name '*.$ext' 2>/dev/null" || true)
      for file in $files; do
        rel_path="${file#$src_path/}"
        mkdir -p "$dst_path/$(dirname "$rel_path")"
        k cp "$pod:$file" "$dst_path/$rel_path" -c "$container" 2>/dev/null || echo "Warning: Failed to copy $file"
      done
    done
    echo "Completed copying for $role."
  done

  echo "Copying pipeline-configmap.yaml..."
  if k get pod producer-sts-0 >/dev/null 2>&1; then
    k cp producer-sts-0:/config/pipeline-configmap.yaml ./src/pipeline-configmap.yaml 2>/dev/null || true
    [[ -f ./src/pipeline-configmap.yaml ]] && echo "Saved pipeline-configmap.yaml" || echo "Warning: Failed to copy pipeline-configmap.yaml."
  else
    echo "Warning: producer-sts-0 not found; skipping pipeline-configmap.yaml."
  fi
  echo "==================================================================="
}

########################################
# Main per-run workflow
########################################

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

  # ===== Save/Delete phase =====
  for role in "${roles[@]}"; do
    echo "==================== Processing $role (save/delete) ===================="
    local container="${role_to_container[$role]}"
    local did_save_results=0 did_delete_results=0 did_save_logs=0 did_delete_logs=0
    local pods pod

    pods=$(list_pods_by_role "$role")
    if [[ -z "${pods:-}" ]]; then
      echo "No pods found for role=$role (selector: ${role_to_label[$role]})."
      echo "==================================================================="
      continue
    fi

    for pod in $pods; do
      [[ -z "$pod" ]] && continue

      # Results save
      if [[ "$save_results" == "y" && -n "${role_to_results[$role]}" ]]; then
        if [[ "$SINGLE_FOR_COPY" == "1" && "$did_save_results" -eq 1 ]]; then
          echo "Skip results save for $role (already saved)."
        else
          for remote_path in ${role_to_results[$role]}; do
            [[ -z "$remote_path" ]] && continue
            local_dir="$results_root/$role/${pod}_$(basename "$remote_path")"
            save_dir_stream "$pod" "$container" "$remote_path" "$local_dir"
          done
          did_save_results=1
        fi
      else
        echo "Skipping result save for $role."
      fi

      # Results delete
      if [[ "$delete_results" == "y" && -n "${role_to_results[$role]}" ]]; then
        if [[ "$SINGLE_FOR_COPY" == "1" && "$did_delete_results" -eq 1 ]]; then
          echo "Skip results deletion for $role (already deleted)."
        else
          for remote_path in ${role_to_results[$role]}; do
            [[ -z "$remote_path" ]] && continue
            delete_dir_cephsafe "$pod" "$container" "$remote_path"
          done
          did_delete_results=1
        fi
      else
        echo "Skipping result deletion for $role."
      fi

      # Logs save
      if [[ "$save_logs" == "y" && -n "${role_to_logs[$role]}" ]]; then
        if [[ "$SINGLE_FOR_COPY" == "1" && "$did_save_logs" -eq 1 ]]; then
          echo "Skip log save for $role (already saved)."
        else
          local_dir="$logs_root/$role/${pod}_logs"
          save_dir_stream "$pod" "$container" "${role_to_logs[$role]}" "$local_dir"
          did_save_logs=1
        fi
      else
        echo "Skipping log save for $role."
      fi

      # Logs delete
      if [[ "$delete_logs" == "y" && -n "${role_to_logs[$role]}" ]]; then
        if [[ "$SINGLE_FOR_COPY" == "1" && "$did_delete_logs" -eq 1 ]]; then
          echo "Skip log deletion for $role (already deleted)."
        else
          delete_dir_cephsafe "$pod" "$container" "${role_to_logs[$role]}"
          did_delete_logs=1
        fi
      else
        echo "Skipping log deletion for $role."
      fi

      # Only first pod if SINGLE_FOR_COPY=1 and we performed any op
      if [[ "$SINGLE_FOR_COPY" == "1" && ( "$did_save_results" -eq 1 || "$did_delete_results" -eq 1 || "$did_save_logs" -eq 1 || "$did_delete_logs" -eq 1 ) ]]; then
        break
      fi
    done
    echo "==================================================================="
  done

  # ===== Run phase: consumers → producers → merge =====
  if [[ "$run_scripts" == "y" ]]; then
    echo "===== Running scripts for all consumer pods first ====="
    {
      role="consumer"
      container="${role_to_container[$role]}"
      pods=$(list_pods_by_role "$role")
      for pod in $pods; do
        pod_id="$(pod_ordinal "$pod")"
        echo "Starting ./runsynthetic.sh $pod_id in $pod ..."
        k exec -c "$container" "$pod" -- sh -lc "setsid ./runsynthetic.sh '$pod_id' >/dev/null 2>&1 < /dev/null &"
      done
    }

    echo "===== Running scripts for all producer pods next ====="
    {
      role="producer"
      container="${role_to_container[$role]}"
      pods=$(list_pods_by_role "$role")
      for pod in $pods; do
        pod_id="$(pod_ordinal "$pod")"
        echo "Starting ./runsynthetic.sh $pod_id in $pod ..."
        k exec -c "$container" "$pod" -- sh -lc "setsid ./runsynthetic.sh '$pod_id' >/dev/null 2>&1 < /dev/null &"
      done
    }

    echo "===== Running scripts for merge pod next ====="
    {
      role="merge"
      container="${role_to_container[$role]}"
      pods=$(list_pods_by_role "$role")
      for pod in $pods; do
        pod_id="$(pod_ordinal "$pod")"
        echo "Starting ./runsynthetic.sh $pod_id in $pod ..."
        k exec -c "$container" "$pod" -- sh -lc "setsid ./runsynthetic.sh  >/dev/null 2>&1 < /dev/null &"
      done
    }
  else
    echo "Skipping script run."
  fi
}  # <-- closes process_pods()

########################################
# Main
########################################

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

