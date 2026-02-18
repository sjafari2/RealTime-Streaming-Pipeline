#!/usr/bin/env bash
set -euo pipefail
trap "exit" INT TERM
#trap "kill 0" EXIT

CONFIG_FILE="${PIPELINE_CONFIG:-/config/pipeline-configmap.yaml}"

cd /app/consumer-merge-data

if [[ ! -f "$CONFIG_FILE" ]]; then
  echo "[consumer] ERROR: config file not found: $CONFIG_FILE" >&2
  exit 2
fi

# ============================================================
#  Load ConfigMap keys under .data into env variables
# ============================================================
eval "$(
  yq eval -r '
    .data
    | to_entries
    | .[]
    | select(.key | test("^[A-Za-z_][A-Za-z0-9_]*$"))
    | "export " + .key + "=" + (.value | @sh)
  ' "$CONFIG_FILE"
)"

# ============================================================
#  Hard requirements (must be set by your "general shell script")
# ============================================================
if [[ -z "${RUN_ID:-}" ]]; then
  echo "[consumer] ERROR: RUN_ID is empty. Run create_topics.sh first (outside this script) and reload config." >&2
  exit 3
fi
if [[ -z "${TOPIC_TITLE:-}" ]]; then
  echo "[consumer] ERROR: TOPIC_TITLE is empty. Run create_topics.sh first (outside this script) and reload config." >&2
  exit 3
fi

echo "[consumer] RUN_ID=${RUN_ID}"
echo "[consumer] TOPIC_TITLE(prefix)=${TOPIC_TITLE}"
echo "[consumer] TOPIC_COUNT=${TOPIC_COUNT:-unset}"
echo "[consumer] BOOTSTRAP_SERVERS=${BOOTSTRAP_SERVERS:-pip-kafka:9092}"

# ============================================================
#  Prepare logging/output directories (use RUN_ID timestamp)
# ============================================================
export CONSUMER_HTTP_PORT="${CONSUMER_HTTP_PORT:-8002}"
POD_NAME="${POD_NAME:-$(hostname)}"

if [[ -z "${CONSUMER_OUTPUT_DIR:-}" ]]; then
  echo "[consumer] ERROR: CONSUMER_OUTPUT_DIR is empty in config." >&2
  exit 1
fi

RUN_TS=""
if [[ "${RUN_ID}" =~ ^run-([0-9]{8}-[0-9]{6})$ ]]; then
  RUN_TS="${BASH_REMATCH[1]}"
else
  # fallback: still Mountain time, but ideally RUN_ID always matches format
  RUN_TS="$(TZ=America/Denver date +"%Y%m%d-%H%M%S")"
fi

CURRENT_DATE_FMT="${RUN_TS:0:4}-${RUN_TS:4:2}-${RUN_TS:6:2}"   # YYYY-MM-DD
CURRENT_TIME_FMT="${RUN_TS:9:2}-${RUN_TS:11:2}-${RUN_TS:13:2}"  # HH-MM-SS

mkdir -p "${CONSUMER_OUTPUT_DIR}/${CURRENT_DATE_FMT}"
LOG_DIR="./logs/consumer/${CURRENT_DATE_FMT}/${CURRENT_TIME_FMT}"
mkdir -p "${LOG_DIR}"

echo "[consumer] pod=${POD_NAME}"
echo "[consumer] EXP_ID=${EXP_ID:-unset} TRAFFIC_MODE=${TRAFFIC_MODE:-unset} TARGET_RATE=${TARGET_RATE:-unset}"
echo "[consumer] Logs: ${LOG_DIR}"

# ============================================================
#  Stop leftover python processes in this container
# ============================================================
echo "[consumer] Terminating any running .py scripts in this container..."
SELF_PID=$$
PARENT_PID=$(ps -o ppid= -p "$SELF_PID" | tr -d ' ')
for pid in $(pgrep -f '\.py' || true); do
  if [[ "$pid" != "$SELF_PID" && "$pid" != "$PARENT_PID" ]]; then
    kill -9 "$pid" 2>/dev/null || true
  fi
done

# ============================================================
#  Launch consumer (ENV-only)
# ============================================================
echo "[consumer] Starting consumer..."

set -x
python3 consumer.py "$@"
rc=$?
set +x
echo "[DEBUG] python exited with code $rc"
exit $rc

#2>&1 | tee "${LOG_DIR}/${POD_NAME}_${EXP_ID}_${TARGET_RATE}.log"

