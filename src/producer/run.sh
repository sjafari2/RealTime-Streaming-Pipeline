#!/usr/bin/env bash
set -euo pipefail
trap "exit" INT TERM
#trap "kill 0" EXIT

cd /app/producer-data

CONFIG_FILE="${PIPELINE_CONFIG:-/config/pipeline-configmap.yaml}"
[[ -f "$CONFIG_FILE" ]] || { echo "[producer] ERROR: missing $CONFIG_FILE" >&2; exit 2; }

load_config_to_env() {
  eval "$(
    yq eval -r '
      .data
      | to_entries
      | .[]
      | select(.key | test("^[A-Za-z_][A-Za-z0-9_]*$"))
      | "export " + .key + "=" + (.value | @sh)
    ' "$CONFIG_FILE"
  )"
}

load_config_to_env

# ------------------------------------------------------------
# Expect TOPIC_TITLE (run-specific topic prefix) to already be set
# by create_topics.sh running elsewhere (consumer side or a Job).
# ------------------------------------------------------------
if [[ -z "${TOPIC_TITLE:-}" ]]; then
  echo "[producer] ERROR: TOPIC_TITLE is empty." >&2
  echo "[producer] Expected create_topics.sh to set TOPIC_TITLE to something like: B0-R500-YYYYMMDD-HHMMSS" >&2
  echo "[producer] Fix: run create_topics.sh (once) before starting producers, then reload config." >&2
  exit 3
fi

echo "[producer] RUN_ID=${RUN_ID:-unset}"
echo "[producer] TOPIC_TITLE(prefix)=${TOPIC_TITLE}"
echo "[producer] TOPIC_COUNT=${TOPIC_COUNT:-unset}"
echo "[producer] BOOTSTRAP_SERVERS=${BOOTSTRAP_SERVERS:-pip-kafka:9092}"

# Optional log cleanup
CLEAN_LOGS_ON_START="${CLEAN_LOGS_ON_START:-true}"
if [[ "${CLEAN_LOGS_ON_START}" == "true" ]]; then
  rm -rf ./logs/* || true
fi

# Use RUN_ID timestamp for log dir (so it matches topic prefix timestamp)
RUN_TS=""
if [[ -n "${RUN_ID:-}" && "${RUN_ID}" =~ ^run-([0-9]{8}-[0-9]{6})$ ]]; then
  RUN_TS="${BASH_REMATCH[1]}"
else
  # Fallback to Mountain time if RUN_ID is missing/unexpected format
  RUN_TS="$(TZ=America/Denver date +"%Y%m%d-%H%M%S")"
fi

CURRENT_DATE_FMT="${RUN_TS:0:4}-${RUN_TS:4:2}-${RUN_TS:6:2}"   # YYYY-MM-DD
CURRENT_TIME_FMT="${RUN_TS:9:2}-${RUN_TS:11:2}-${RUN_TS:13:2}" # HH-MM-SS

LOG_DIR="./logs/producer/${CURRENT_DATE_FMT}/${CURRENT_TIME_FMT}"
mkdir -p "$LOG_DIR"

POD_NAME="$(hostname)"
echo "[producer] pod=${POD_NAME}"
echo "[producer] EXP_ID=${EXP_ID:-unset} TRAFFIC_MODE=${TRAFFIC_MODE:-unset} TARGET_RATE=${TARGET_RATE:-unset}"
echo "[producer] Logs: ${LOG_DIR}"

# Kill old python in this container (optional)
pkill -f 'producer\.py' 2>/dev/null || true

set -x

python3 producer.py "$@"
rc=$?
set +x
echo "[DEBUG] python exited with code $rc"
exit $rc

#2>&1 | tee "${LOG_DIR}/producer_${POD_NAME}_${EXP_ID:-EXP}_${TARGET_RATE:-RATE}_${RUN_TS}.log"

