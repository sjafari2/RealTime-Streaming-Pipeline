#!/usr/bin/env bash
set -euo pipefail

trap "exit" INT TERM
trap "kill 0" EXIT

CONFIG_FILE="${PIPELINE_CONFIG:-/config/pipeline-configmap.yaml}"

cd /app/consumer-merge-data

if [[ ! -f "$CONFIG_FILE" ]]; then
  echo "[consumer] ERROR: config file not found: $CONFIG_FILE" >&2
  exit 2
fi

# ============================================================
#  Load ConfigMap keys under .data into env variables (flat format)
# ============================================================

# (Optional debug) show keys
#echo "[DEBUG] ConfigMap data keys (first 40):"
#yq eval -r '.data | keys | .[0:40] | .[]' "$CONFIG_FILE" || true

# Export only VALID env var keys
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
#  Prepare logging/output directories
# ============================================================

export CONSUMER_HTTP_PORT="${CONSUMER_HTTP_PORT:-8002}"

CURRENT_DATE=$(TZ=America/Denver date +"%Y-%m-%d")
CURRENT_TIME=$(TZ=America/Denver date +"%H-%M-%S")

POD_NAME="${POD_NAME:-$(hostname)}"

if [[ -z "${CONSUMER_OUTPUT_DIR:-}" ]]; then
  echo "[consumer] ERROR: CONSUMER_OUTPUT_DIR is empty in ConfigMap." >&2
  exit 1
fi

CONSUMER_DIR="${CONSUMER_OUTPUT_DIR}/${CURRENT_DATE}"
mkdir -p "${CONSUMER_DIR}"

LOG_DIR="./logs/consumer/${CURRENT_DATE}/${CURRENT_TIME}"
mkdir -p "${LOG_DIR}"

echo "[INFO] Consumer pod: ${POD_NAME}"
echo "[INFO] EXP_ID=${EXP_ID:-unset} TRAFFIC_MODE=${TRAFFIC_MODE:-unset} TARGET_RATE=${TARGET_RATE:-unset}"

# ============================================================
#  Topic discovery
# ============================================================

server_uri="${BOOTSTRAP_SERVERS:-pip-kafka:9092}"
echo "[INFO] Brokers: ${server_uri}"

if [[ -z "${TOPIC_TITLE:-}" ]]; then
  echo "[ERROR] TOPIC_TITLE is empty in ConfigMap."
  exit 1
fi

chmod +x ./kafka-list-topics.sh ./create_topics.sh || true

readarray -t TOPICS < <(./kafka-list-topics.sh)

echo "[INFO] All topics:"
printf "%s\n" "${TOPICS[@]}"

MATCHED_TOPICS=()
for topic in "${TOPICS[@]}"; do
  if [[ "$topic" == ${TOPIC_TITLE}_* ]]; then
    MATCHED_TOPICS+=("$topic")
  fi
done

if [[ ${#MATCHED_TOPICS[@]} -eq 0 ]]; then
  echo "[WARN] No topics matched '${TOPIC_TITLE}', creating..."
  ./create_topics.sh || true

  readarray -t TOPICS < <(./kafka-list-topics.sh)

  MATCHED_TOPICS=()
  for topic in "${TOPICS[@]}"; do
    if [[ "$topic" == ${TOPIC_TITLE}_* ]]; then
      MATCHED_TOPICS+=("$topic")
    fi
  done
fi

if [[ ${#MATCHED_TOPICS[@]} -eq 0 ]]; then
  echo "[ERROR] Still no topics found even after creating."
  exit 1
fi

JOINED_TOPICS=$(IFS=, ; echo "${MATCHED_TOPICS[*]}")
echo "[INFO] Matched topics: ${JOINED_TOPICS}"

# ============================================================
#  Stop leftover scripts (unchanged logic, but safer if pgrep finds nothing)
# ============================================================

echo "[INFO] Terminating any running .py or .sh scripts in this container..."

SELF_PID=$$
PARENT_PID=$(ps -o ppid= -p "$SELF_PID" | tr -d ' ')

for pid in $(pgrep -f '\.py' || true); do
  if [[ "$pid" != "$SELF_PID" && "$pid" != "$PARENT_PID" ]]; then
    kill -9 "$pid" || true
  fi
done

for pid in $(pgrep -f '\.sh' || true); do
  if [[ "$pid" != "$SELF_PID" && "$pid" != "$PARENT_PID" ]]; then
    kill -9 "$pid" || true
  fi
done

# ============================================================
#  Launch consumer (ENV-only — no CLI args)
# ============================================================

echo "[INFO] Starting consumer..."
set -x
python3 consumer.py
#\
#  2>&1 | tee "${LOG_DIR}/${POD_NAME}_${EXP_ID}_${TARGET_RATE}.log"
set +x

