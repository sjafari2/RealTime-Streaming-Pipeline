#!/usr/bin/env bash
set -Eeuo pipefail

# ------------------- CONFIG -------------------
CONFIG_FILE="${CONFIG_FILE:-/config/pipeline-configmap.yaml}"

# Optional: parseYaml.sh (if present). It might produce VARS or data_VARS for ConfigMap.
if [[ -f ./parseYaml.sh ]]; then
  # shellcheck source=/dev/null
  source ./parseYaml.sh
  # Don't fail the whole script if parse_yaml hits something weird
  eval "$(parse_yaml "$CONFIG_FILE" 2>/dev/null || true)"
fi

# Helper: read from top-level VAR, then from data_VAR, else fallback
get_cfg() {
  local key="$1" default="${2-}"
  local val data_key

  # 1) top-level (e.g., CONSUMER_OUTPUT_DIR)
  if eval "[[ \${$key+set} ]]"; then
    eval "val=\${$key}"
    printf '%s' "$val"
    return
  fi

  # 2) ConfigMap-style (e.g., data_CONSUMER_OUTPUT_DIR)
  data_key="data_${key}"
  if eval "[[ \${$data_key+set} ]]"; then
    eval "val=\${$data_key}"
    printf '%s' "$val"
    return
  fi

  # 3) fallback default
  printf '%s' "$default"
}
# ------------------- EXPERIMENT ID & TIME -------------------
EXPERIMENT_ID="exp_$(date +%Y%m%d_%H%M%S)"
export EXPERIMENT_ID
echo "[INIT] Experiment ID: $EXPERIMENT_ID"

CURRENT_DATE=$(TZ=America/Denver date +"%Y-%m-%d")
CURRENT_TIME=$(TZ=America/Denver date +"%H-%M-%S")

# ------------------- PATHS (pull from config with safe fallbacks) -------------------
watch_dir="$(get_cfg CONSUMER_OUTPUT_DIR "/app/consumer-merge-data/consumer-results")"
processed_dir="$(get_cfg PROCESSED_DIR "/app/consumer-merge-data/processed")"

merged_root_default="/app/merge-result"
merged_dir="$(get_cfg MERGE_OUTPUT_DIR "$merged_root_default")"

metrics_root_default="/app/merge-metrics"
metrics_root="$(get_cfg MERGE_METRICS_DIR "$metrics_root_default")"
metrics_dir="${metrics_root%/}/${CURRENT_DATE}_${CURRENT_TIME}"

# Controls
enable_parquet="$(get_cfg ENABLE_PARQUET "true")"
min_blocks_for_gev="$(get_cfg MIN_BLOCKS_FOR_GEV "30")"
fit_gev_interval_sec="$(get_cfg FIT_GEV_INTERVAL_SEC "60")"
stop_at_blocks="$(get_cfg STOP_AT_BLOCKS "0")"
block_scope_raw="$(get_cfg MERGE_BLOCK_SCOPE "global")"

# sanitize block_scope (trim + lowercase + validate)
block_scope="${block_scope_raw,,}"
# trim leading/trailing spaces
block_scope="${block_scope#"${block_scope%%[![:space:]]*}"}"
block_scope="${block_scope%"${block_scope##*[![:space:]]}"}"
if [[ "$block_scope" != "global" && "$block_scope" != "per_pod" ]]; then
  echo "[WARN] Invalid MERGE_BLOCK_SCOPE='$block_scope_raw' → falling back to 'global'"
  block_scope="global"
fi

# Ensure directories exist
mkdir -p "$processed_dir" "$merged_dir" "$metrics_dir"

# ------------------- LOGGING -------------------
log_path="./logs/merge/${CURRENT_DATE}_${CURRENT_TIME}"
mkdir -p "$log_path"
log_file="${log_path}/merge.log"
echo "[INFO] Metrics Dir: $metrics_dir" | tee -a "$log_file"

# ------------------- SIGNAL CLEANUP -------------------
trap 'echo "[INFO] Caught interrupt signal. Stopping merger." | tee -a "$log_file"; exit 0' INT TERM

# ------------------- EXECUTION -------------------
# NOTE: do NOT put a trailing "\" on a commented line; it breaks the pipeline.
python3 confluent_merge.py \
  --watchDir "$watch_dir" \
  --processedDir "$processed_dir" \
  --mergedDir "$merged_dir" \
  --metricsDir "$metrics_dir" \
  --enableParquet "$enable_parquet" \
  --minBlocksForGEV "$min_blocks_for_gev" \
  --fitGEVIntervalSec "$fit_gev_interval_sec" \
  --blockScope "$block_scope" \
  --stopAtBlocks "$stop_at_blocks" \
  2>&1 | tee -a "$log_file"


