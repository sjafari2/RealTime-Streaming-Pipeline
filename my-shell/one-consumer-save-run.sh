#!/usr/bin/env bash
set -euo pipefail

########################################
# Config (adjust as needed)
########################################

NAMESPACE="${NAMESPACE:-}"
SINGLE_FOR_COPY="${SINGLE_FOR_COPY:-1}"

# Save/delete processing order (run order is consumers then producers below)
roles=("producer" "consumer")

declare -A role_to_label=(
  ["producer"]="app=producer-sts"
  ["consumer"]="app=consumer-sts"
)

declare -A role_to_container=(
  ["producer"]="producer-container"
  ["consumer"]="consumer-container"
)

declare -A role_to_results=(
  ["producer"]=""
  ["consumer"]="/app/consumer-merge-data/metrics"
)

declare -A role_to_logs=(
  ["producer"]="/app/producer-data/logs"
  ["consumer"]="/app/consumer-merge-data/logs"
)

# Correct run.sh paths per role
declare -A role_to_runsh=(
  ["producer"]="/app/producer-data/run.sh"
  ["consumer"]="/app/consumer-merge-data/run.sh"
)

# create_topics.sh location (consumer pod 0 only)
CREATE_TOPICS_POD="consumer-sts-0"
CREATE_TOPICS_CONTAINER="consumer-container"
CREATE_TOPICS_PATH="/app/consumer-merge-data/create_topics.sh"
CONFIG_PATH_IN_POD="/config/pipeline-configmap.yaml"

# Code backup sources
declare -A pod_configs=(
  ["producer"]="producer-sts-0:producer-container:/app/producer-data:./src/producer"
  ["consumer"]="consumer-sts-0:consumer-container:/app/consumer-merge-data:./src/consumer"
)

file_extensions=("py" "sh" "yaml" "yml" "properties")

########################################
# Helpers
########################################

k() {
  if [[ -n "${NAMESPACE}" ]]; then
    kubectl -n "${NAMESPACE}" --request-timeout=0  "$@"
  else
    kubectl --request-timeout=0 "$@"
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
    find '$rdir' -type f -delete 2>/dev/null || true
    find '$rdir' -depth -type d -empty -delete 2>/dev/null || true
  " || echo 'Warning: delete failed'
}

list_pods_by_role() {
  local role="$1"
  local sel="${role_to_label[$role]}"
  [[ -z "$sel" ]] && return 0
  if sort -V </dev/null >/dev/null 2>&1; then
    k get pods -l "$sel" -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' | sort -V
  else
    k get pods -l "$sel" -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' \
      | awk 'match($0,/-([0-9]+)$/,m){print m[1],$0}' \
      | sort -k1,1n \
      | cut -d' ' -f2-
  fi
}

########################################
# Code backup (optional) — EXCLUDES logs/results
########################################

should_skip_code_file() {
  local full_path="$1"
  if [[ "$full_path" == *"/consumer-result/"* ]] || [[ "$full_path" == *"/consumer-result" ]]; then
    return 0
  fi
  if [[ "$full_path" == *"/logs/"* ]] || [[ "$full_path" == *"/logs" ]]; then
    return 0
  fi
  return 1
}

save_codes() {
  echo "==================== Saving Codes from All Pods (no logs/results) ===================="
  mkdir -p ./src/producer ./src/consumer

  for role in "${roles[@]}"; do
    IFS=':' read -r pod container src_path dst_path <<< "${pod_configs[$role]}"
    echo "Copying from $pod (container: $container, path: $src_path) to $dst_path"
    mkdir -p "$dst_path"

    for ext in "${file_extensions[@]}"; do
      files=$(k exec "$pod" -c "$container" -- sh -lc "find '$src_path' -type f -name '*.$ext' 2>/dev/null" || true)
      for file in $files; do
        if should_skip_code_file "$file"; then
          continue
        fi
        rel_path="${file#$src_path/}"
        mkdir -p "$dst_path/$(dirname "$rel_path")"
        k cp "$pod:$file" "$dst_path/$rel_path" -c "$container" 2>/dev/null || echo "Warning: Failed to copy $file"
      done
    done
    echo "Completed copying for $role."
  done

  echo "Copying pipeline-configmap.yaml..."
  if k get pod producer-sts-0 >/dev/null 2>&1; then
    mkdir -p ./src
    k cp producer-sts-0:/config/pipeline-configmap.yaml ./src/pipeline-configmap.yaml 2>/dev/null || true
    [[ -f ./src/pipeline-configmap.yaml ]] && echo "Saved pipeline-configmap.yaml" || echo "Warning: Failed to copy pipeline-configmap.yaml."
  else
    echo "Warning: producer-sts-0 not found; skipping pipeline-configmap.yaml."
  fi
  echo "==================================================================="
}

########################################
# Kill old processes in pods
########################################

kill_processes_in_pod() {
  local pod="$1" container="$2" role="$3"

  # conservative patterns: python3, consumer.py/producer.py, and run.sh
  # also kill "sh -lc /app/.../run.sh" style wrappers
  echo "[KILL] ${pod} (${role}) ..."
  k exec -c "$container" "$pod" -- sh -lc "
    set +e
    # show what we are about to kill (optional)
    # ps -ef | grep -E '[p]ython3|[r]un\.sh|consumer\.py|producer\.py' || true

    # Kill run.sh wrappers first (they may be parent jobs)
    pkill -f 'run\.sh' 2>/dev/null || true
    pkill -f '/app/.*/run\.sh' 2>/dev/null || true

    # Kill python workloads
    pkill -f 'python3 .*consumer\.py' 2>/dev/null || true
    pkill -f 'python3 .*producer\.py' 2>/dev/null || true
    pkill -f 'python3' 2>/dev/null || true

    # Give processes a moment, then hard-kill anything left
    sleep 1
    pkill -9 -f 'run\.sh' 2>/dev/null || true
    pkill -9 -f 'python3' 2>/dev/null || true

    echo '[KILL] done'
    exit 0
  " || true
}

kill_all_pods() {
  echo "==================== Killing old python3/run.sh in ALL pods ===================="
  for role in "consumer" "producer"; do
    local container="${role_to_container[$role]}"
    local pods
    pods=$(list_pods_by_role "$role")
    if [[ -z "${pods:-}" ]]; then
      echo "No pods found for role=$role"
      continue
    fi
    for pod in $pods; do
      [[ -z "$pod" ]] && continue
      kill_processes_in_pod "$pod" "$container" "$role"
    done
  done
  echo "==================================================================="
}

########################################
# Run create_topics once (consumer-sts-0)
########################################

run_create_topics_once() {
  echo "==================== Creating topics once in ${CREATE_TOPICS_POD} ===================="

  k get pod "${CREATE_TOPICS_POD}" >/dev/null 2>&1 || {
    echo "[ERROR] Pod ${CREATE_TOPICS_POD} not found." >&2
    return 1
  }

  k exec -c "${CREATE_TOPICS_CONTAINER}" "${CREATE_TOPICS_POD}" -- sh -lc "
    set -e
    test -f '${CREATE_TOPICS_PATH}' || { echo '[ERROR] missing ${CREATE_TOPICS_PATH}'; exit 2; }
    chmod +x '${CREATE_TOPICS_PATH}' || true
    '${CREATE_TOPICS_PATH}'

    # Verify write-back (prefer yq)
    if command -v yq >/dev/null 2>&1; then
      rid=\$(yq eval -r '.data.RUN_ID // \"\"' '${CONFIG_PATH_IN_POD}' 2>/dev/null || true)
      ttl=\$(yq eval -r '.data.TOPIC_TITLE // \"\"' '${CONFIG_PATH_IN_POD}' 2>/dev/null || true)
    else
      rid=\$(awk '/^[[:space:]]+RUN_ID:/{gsub(/^[[:space:]]+RUN_ID:[[:space:]]*/,\"\",\$0); gsub(/\"/,\"\",\$0); print \$0; exit}' '${CONFIG_PATH_IN_POD}' || true)
      ttl=\$(awk '/^[[:space:]]+TOPIC_TITLE:/{gsub(/^[[:space:]]+TOPIC_TITLE:[[:space:]]*/,\"\",\$0); gsub(/\"/,\"\",\$0); print \$0; exit}' '${CONFIG_PATH_IN_POD}' || true)
    fi

    echo \"[VERIFY] RUN_ID=\$rid\"
    echo \"[VERIFY] TOPIC_TITLE=\$ttl\"
    test -n \"\$rid\" || { echo '[ERROR] RUN_ID empty after create_topics.sh'; exit 10; }
    test -n \"\$ttl\" || { echo '[ERROR] TOPIC_TITLE empty after create_topics.sh'; exit 11; }
  "

  echo "==================================================================="
}

########################################
# Start run.sh in pods (robust + verify)
########################################

start_role_pods() {
  local role="$1"
  local container="${role_to_container[$role]}"
  local runsh="${role_to_runsh[$role]}"
  local pods
  pods=$(list_pods_by_role "$role")

  if [[ -z "${pods:-}" ]]; then
    echo "[ERROR] No pods found for role=$role (selector: ${role_to_label[$role]})." >&2
    return 1
  fi

  echo "==================== Starting ${role} pods ===================="

  for pod in $pods; do
    [[ -z "$pod" ]] && continue

    local log_dir log_file proc_pat
    if [[ "$role" == "consumer" ]]; then
      log_dir="/app/consumer-merge-data/logs"
      proc_pat="consumer.py"
    else
      log_dir="/app/producer-data/logs"
      proc_pat="producer.py"
    fi
    log_file="${log_dir}/run_${pod}.log"

    echo "Starting $runsh in $pod ..."
    
    set +e

    k exec -c "$container" "$pod" -- sh -lc "
      set -e
      test -f '$runsh' || { echo '[ERROR] missing $runsh'; exit 2; }
      mkdir -p '$log_dir'
      chmod +x '$runsh' || true

      # Start in background, detached
      nohup '$runsh' </dev/null > '$log_file' 2>&1 &
      pid=\$!

      # Verify with ps (no pgrep dependency), retry for slow startup
      ok=0
      for i in 1 2 3 4 5; do
        if ps -ef 2>/dev/null | grep -E \"[p]ython3 .*${proc_pat}\" >/dev/null 2>&1; then
          ok=1
          break
        fi
        sleep 1
      done

      if [ \"\$ok\" -eq 1 ]; then
        echo '[OK] started pid='\"\$pid\"' and ${proc_pat} is running'
      else
        echo '[FAIL] ${proc_pat} not detected after start. Last 120 log lines:'
        tail -n 120 '$log_file' 2>/dev/null || true
        echo '[FAIL] process table (python lines):'
        ps -ef 2>/dev/null | grep -E \"[p]ython\" || true
        exit 9
      fi
    "
    rc=$?
    set -e

    if [[ $rc -ne 0 ]]; then
      echo "[WARN] start failed in $pod (rc=$rc). Continuing to next pod..."
      # best-effort: try to show log tail (separate exec so the main one can fail)
      k exec -c "$container" "$pod" -- sh -lc "tail -n 80 '$log_file' 2>/dev/null || true" || true
      continue
    fi
  done

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

  mkdir -p "$results_root/consumer"
  mkdir -p "$logs_root/producer" "$logs_root/consumer"

  read -n 1 -p "Save results before running the script? (y/n): " save_results; echo
  [[ "$save_results" != "y" ]] && save_results="n"

  read -n 1 -p "Delete results before running the script? (y/n): " delete_results; echo
  [[ "$delete_results" != "y" ]] && delete_results="n"

  read -n 1 -p "Save logs before running the script? (y/n): " save_logs; echo
  [[ "$save_logs" != "y" ]] && save_logs="n"

  read -n 1 -p "Delete logs before running the script? (y/n): " delete_logs; echo
  [[ "$delete_logs" != "y" ]] && delete_logs="n"

  read -n 1 -p "Kill all running python3/run.sh in pods before starting? (y/n): " kill_first; echo
  [[ "$kill_first" != "y" ]] && kill_first="n"

  read -n 1 -p "Run run.sh inside pods? (y/n): " run_scripts; echo
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

      if [[ "$SINGLE_FOR_COPY" == "1" && ( "$did_save_results" -eq 1 || "$did_delete_results" -eq 1 || "$did_save_logs" -eq 1 || "$did_delete_logs" -eq 1 ) ]]; then
        break
      fi
    done

    echo "==================================================================="
  done

  # ===== Run phase: kill → create topics → consumers → producers =====
  if [[ "$run_scripts" == "y" ]]; then
    if [[ "$kill_first" == "y" ]]; then
      kill_all_pods
    else
      echo "Skipping kill step."
    fi

    echo "===== Step 1: create topics (consumer-sts-0) ====="
    run_create_topics_once

    echo "===== Step 2: start consumers ====="
    start_role_pods "consumer"

    echo "===== Step 3: start producers ====="
    start_role_pods "producer"
  else
    echo "Skipping script run."
  fi
}

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

