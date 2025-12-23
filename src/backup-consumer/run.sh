#!/usr/bin/env bash
set -euo pipefail

trap "exit" INT TERM
trap "kill 0" EXIT

CONFIG_FILE="/config/pipeline-configmap.yaml"

# ============================================================
#  Load ALL ConfigMap keys under .data into plain env variables
# ============================================================

eval "$(
  yq eval '.data | to_entries | map("export " + .key + "=" + (.value | @sh)) | .[]' "$CONFIG_FILE"
)"

# ============================================================
#  Prepare logging/output directories
# ============================================================

CURRENT_DATE=$(TZ=America/Denver date +"%Y-%m-%d")
CURRENT_TIME=$(TZ=America/Denver date +"%H-%M-%S")

CONSUMER_DIR="${CONSUMER_OUTPUT_DIR}/${CURRENT_DATE}"
mkdir -p "${CONSUMER_DIR}"

LOG_DIR="./logs/consumer/${CURRENT_DATE}"
mkdir -p "${LOG_DIR}"

pod_name=$(hostname)

echo "[INFO] Consumer pod: ${pod_name}"
echo "[INFO] EXP_ID=${EXP_ID:-unknown}, TRAFFIC_MODE=${TRAFFIC_MODE:-balanced}"

# ============================================================
#  Topic discovery (unchanged)
# ============================================================

server_uri='pip-kafka:9092'   # static DNS used in your cluster
echo "[INFO] Brokers: ${server_uri}"

if [ -z "${TOPIC_TITLE}" ]; then
  echo "[ERROR] TOPIC_TITLE is empty in ConfigMap."
  exit 1
fi

readarray -t TOPICS < <(./kafka-list-topics.sh)

echo "[INFO] All topics:"
printf "%s\n" "${TOPICS[@]}"

MATCHED_TOPICS=()
for topic in "${TOPICS[@]}"; do
  prefix="${topic%%_*}"
  if [[ "$prefix" == "${TOPIC_TITLE}" ]]; then
    MATCHED_TOPICS+=("$topic")
  fi
done

if [ ${#MATCHED_TOPICS[@]} -eq 0 ]; then
  echo "[WARN] No topics matched '${TOPIC_TITLE}', creating..."
  chmod +x ./create_topics.sh
  ./create_topics.sh
  readarray -t TOPICS < <(./kafka-list-topics.sh)
  for topic in "${TOPICS[@]}"; do
    prefix="${topic%%_*}"
    if [[ "$prefix" == "${TOPIC_TITLE}" ]]; then
      MATCHED_TOPICS+=("$topic")
    fi
  done
fi

if [ ${#MATCHED_TOPICS[@]} -eq 0 ]; then
  echo "[ERROR] Still no topics found even after creating."
  exit 1
fi

JOINED_TOPICS=$(IFS=, ; echo "${MATCHED_TOPICS[*]}")
echo "[INFO] Matched topics: ${JOINED_TOPICS}"

# ============================================================
#  Stop leftover scripts (unchanged logic)
# ============================================================

echo "[INFO] Terminating any running .py or .sh scripts in this container..."

SELF_PID=$$
PARENT_PID=$(ps -o ppid= -p "$SELF_PID" | tr -d ' ')

for pid in $(pgrep -f '\.py'); do
  if [[ "$pid" != "$SELF_PID" && "$pid" != "$PARENT_PID" ]]; then
    kill -9 "$pid" || true
  fi
done

for pid in $(pgrep -f '\.sh'); do
  if [[ "$pid" != "$SELF_PID" && "$pid" != "$PARENT_PID" ]]; then
    kill -9 "$pid" || true
  fi
done

# ============================================================
#  Launch consumer (ENV-only — no CLI args)
# ============================================================

echo "[INFO] Starting consumer..."
set -x
python3 consumer.py \
    2>&1 | tee "${LOG_DIR}/${pod_name}_${EXP_ID}_${TARGET_RATE}.log"
set +x

