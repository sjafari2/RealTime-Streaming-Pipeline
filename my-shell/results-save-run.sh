#!/usr/bin/env bash
set -euo pipefail

########################################
# Config (adjust as needed)
########################################

# kubectl namespace (leave empty to use the current context namespace)
NAMESPACE="${NAMESPACE:-}"

# Copy/delete only once per role (first pod found)
SINGLE_FOR_COPY="${SINGLE_FOR_COPY:-1}"

# Roles to process (save/delete order); run order is enforced later
roles=("producer" "consumer" "merge")

# Pod label selectors per role (change if your labels differ)
# Example assumes your pods have labels like app=producer-sts / app=consumer-sts / app=merge-sts
declare -A role_to_label=(
  ["producer"]="app=producer-sts"
  ["consumer"]="app=consumer-sts"
  ["merge"]="app=merge-sts"
)

# Container name in each pod
declare -A role_to_container=(
  ["producer"]="producer-container"
  ["consumer"]="consumer-container"
  ["merge"]="merge-container"
)

# Remote results path (leave empty to DISABLE saving for that role)
# NOTE: Producer is empty by default, so no results/producer folder will be created.
declare -A role_to_results=(
  ["producer"]=""
  ["consumer"]="/app/consumer-merge-data/processed"
  ["merge"]="/app/merged-data/merge-metrics"
)

# Remote logs path (leave empty to DISABLE saving logs for that role)
declare -A role_to_logs=(
  ["producer"]="/app/producer-data/logs"
  ["consumer"]="/app/consumer-merge-data/logs"
  ["merge"]="/app/merged-data/logs"
)

########################################
# Helpers
########################################

# kubectl wrapper with optional namespace
k() {
  if [[ -n "${NAMESPACE}" ]]; then
    kubectl -n "${NAMESPACE}" "$@"
  else
    kubectl "$@"
  fi
}

# List pods for a role using its label selector
list_pods_by_role() {
  local role="$1"
  local sel="${role_to_label[$role]}"
  [[ -z "$sel" ]] && return 0
  # Prefer -V (version sort) to keep -0, -1, -2 ordering
  if sort -V </dev/null >/dev/null 2>&1; then
    k get pods -l "$sel" -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' | sort -V
  else
    k get pods -l "$sel" -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}'
  fi
}

# Get StatefulSet ordinal from pod name: e.g., consumer-sts-11 -> 11
pod_ordinal() {
  local name="$1"
  if [[ "$name" =~ -([0-9]+)$ ]]; then
    echo "${BASH_REMATCH[1]}"
  else
    echo "0"
  fi
}

# Stream-copy a directory from inside a pod onto local disk
# $1=pod  $2=container  $3=remote_dir  $4=local_dir
save_dir_stream() {
  local pod="$1" container="$2" remote_dir="$3" local_dir="$4"
  echo "[COPY] $pod:$remote_dir -> $local_dir"
  mkdir -p "$local_dir"
  # tar from pod, untar locally; ignore permission errors but fail on others
  k exec -c "$container" "$pod" -- sh -lc "tar cf - -C \"$(dirname "$remote_dir")\" \"$(basename "$remote_dir")\"" \
    | tar xf - -C "$local_dir"
}

# Delete contents *inside* a path in a pod (keeps the directory itself)
# $1=pod  $2=container  $3=remote_dir
delete_inside_pod_dir() {
  local pod="$1" container="$2" remote_dir="$3"
  echo "[DELETE] $pod:$remote_dir/*"
  # Use find to safely remove contents without removing the directory node itself
  k exec -c "$container" "$pod" -- sh -lc '
    set -e
    target_dir="$1"
    if [ -d "$target_dir" ]; then
      find "$target_dir" -mindepth 1 -maxdepth 1 -exec rm -rf {} +
    fi
  ' sh "$remote_dir"
}

########################################
# Main processing: save/delete and runs
########################################
process_pods() {
  local timestamp
  timestamp="$(date +%Y%m%d_%H%M%S)"

  # Ask what to do
  echo
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

  # Roots created lazily (only if something is actually copied)
  local results_root="./results/${timestamp}"
  local logs_root="./logs/${timestamp}"
  local created_results_root=0
  local created_logs_root=0

  # ===== Save/Delete phase (by role) =====
  for role in "${roles[@]}"; do
    echo "==================== PROCESS: $role ===================="
    local container="${role_to_container[$role]}"
    local did_any_op=0
    local did_save_results=0 did_delete_results=0 did_save_logs=0 did_delete_logs=0

    pods="$(list_pods_by_role "$role")"
    if [[ -z "${pods:-}" ]]; then
      echo "No pods found for role=$role (selector: ${role_to_label[$role]})."
      echo "========================================================"
      continue
    fi

    for pod in $pods; do
      [[ -z "$pod" ]] && continue

      # Save results (only if role has a non-empty results path)
      if [[ "$save_results" == "y" && -n "${role_to_results[$role]}" ]]; then
        if [[ "$SINGLE_FOR_COPY" == "1" && "$did_save_results" -eq 1 ]]; then
          echo "Skip results save for $role (already saved from first pod)."
        else
          [[ "$created_results_root" -eq 0 ]] && { mkdir -p "$results_root"; created_results_root=1; }
          local dest="$results_root/$role/$pod"
          save_dir_stream "$pod" "$container" "${role_to_results[$role]}" "$dest"
          did_save_results=1
          did_any_op=1
        fi
      fi

      # Delete results (only if role has a non-empty results path)
      if [[ "$delete_results" == "y" && -n "${role_to_results[$role]}" ]]; then
        if [[ "$SINGLE_FOR_COPY" == "1" && "$did_delete_results" -eq 1 ]]; then
          echo "Skip results delete for $role (already deleted from first pod)."
        else
          delete_inside_pod_dir "$pod" "$container" "${role_to_results[$role]}"
          did_delete_results=1
          did_any_op=1
        fi
      fi

      # Save logs
      if [[ "$save_logs" == "y" && -n "${role_to_logs[$role]}" ]]; then
        if [[ "$SINGLE_FOR_COPY" == "1" && "$did_save_logs" -eq 1 ]]; then
          echo "Skip logs save for $role (already saved from first pod)."
        else
          [[ "$created_logs_root" -eq 0 ]] && { mkdir -p "$logs_root"; created_logs_root=1; }
          local dest="$logs_root/$role/$pod"
          save_dir_stream "$pod" "$container" "${role_to_logs[$role]}" "$dest"
          did_save_logs=1
          did_any_op=1
        fi
      fi

      # Delete logs
      if [[ "$delete_logs" == "y" && -n "${role_to_logs[$role]}" ]]; then
        if [[ "$SINGLE_FOR_COPY" == "1" && "$did_delete_logs" -eq 1 ]]; then
          echo "Skip logs delete for $role (already deleted from first pod)."
        else
          delete_inside_pod_dir "$pod" "$container" "${role_to_logs[$role]}"
          did_delete_logs=1
          did_any_op=1
        fi
      fi

      # Stop at first pod if SINGLE_FOR_COPY and something was done
      if [[ "$SINGLE_FOR_COPY" == "1" && "$did_any_op" -eq 1 ]]; then
        break
      fi
    done
    echo "========================================================"
  done

  # ===== Run phase: consumers → producers → merge =====
  if [[ "$run_scripts" == "y" ]]; then
    echo "===== Launching ./runsynthetic.sh for all CONSUMER pods ====="
    {
      local role="consumer"
      local container="${role_to_container[$role]}"
      pods="$(list_pods_by_role "$role")"
      for pod in $pods; do
        pod_id="$(pod_ordinal "$pod")"
        echo "Starting consumer $pod ./runsynthetic.sh $pod_id ..."
        k exec -c "$container" "$pod" -- sh -lc "setsid ./runsynthetic.sh '$pod_id' >/dev/null 2>&1 < /dev/null &"
      done
    }

    echo "===== Launching ./runsynthetic.sh for all PRODUCER pods ====="
    {
      local role="producer"
      local container="${role_to_container[$role]}"
      pods="$(list_pods_by_role "$role")"
      for pod in $pods; do
        pod_id="$(pod_ordinal "$pod")"
        echo "Starting producer $pod ./runsynthetic.sh $pod_id ..."
        k exec -c "$container" "$pod" -- sh -lc "setsid ./runsynthetic.sh '$pod_id' >/dev/null 2>&1 < /dev/null &"
      done
    }

    echo "===== Launching ./runsynthetic.sh for all MERGE pods ====="
    {
      local role="merge"
      local container="${role_to_container[$role]}"
      pods="$(list_pods_by_role "$role")"
      for pod in $pods; do
        pod_id="$(pod_ordinal "$pod")"
        echo "Starting merge $pod ./runsynthetic.sh $pod_id ..."
        k exec -c "$container" "$pod" -- sh -lc "setsid ./runsynthetic.sh '$pod_id' >/dev/null 2>&1 < /dev/null &"
      done
    }
  else
    echo "Run phase skipped."
  fi
}

########################################
# Entry
########################################
echo "==================== save-run.sh ===================="
echo "Namespace: ${NAMESPACE:-<current>}"
echo "Roles: ${roles[*]}"
echo "SINGLE_FOR_COPY: ${SINGLE_FOR_COPY}"

process_pods

echo "==================== Done. ===================="

