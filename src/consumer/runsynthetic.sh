#!/usr/bin/env bash
set -euo pipefail

trap "exit" INT TERM
trap "kill 0" EXIT

CONFIG_FILE="/config/pipeline-configmap.yaml"

# Export all config variables as data_<KEY>=<value>
eval $(
  yq eval '.data | to_entries | map("export data_" + .key + "=" + (.value | @sh)) | .[]' "$CONFIG_FILE"
)

CURRENT_DATE=$(TZ=America/Denver date +"%Y-%m-%d")
CURRENT_TIME=$(TZ=America/Denver date +"%H-%M-%S")
consumer_output_dir="${data_CONSUMER_OUTPUT_DIR}/${CURRENT_DATE}"
mkdir -p "${consumer_output_dir}"

# === Get brokers ===
#server_uri=$(bash get_kafka_consumer_dns.sh | sed 's/\[\|\]//g' | tr -d '"' | tr '\n' ',' | sed 's/,$//')
server_uri='pip-kafka:9092'

echo Servers are $server_uri

if [ -z "${data_TOPIC_TITLE}" ]; then
    echo "ERROR: TOPIC_TITLE is empty in ConfigMap."
    exit 1
fi
# === Get topics ===
readarray -t TOPICS < <(./kafka-list-topics.sh)

echo "All topics:"
for topic in "${TOPICS[@]}"; do
  echo "$topic"
done

# === Match topics by prefix ===
MATCHED_TOPICS=()
for topic in "${TOPICS[@]}"; do
  topic_prefix="${topic%%_*}"  # Extract part before underscore
  if [[ "$topic_prefix" == ${data_TOPIC_TITLE} ]]; then
    MATCHED_TOPICS+=("$topic")
  fi
done

# Join matched topics with comma
if [ ${#MATCHED_TOPICS[@]} -gt 0 ]; then
  JOINED_TOPICS=$(IFS=, ; echo "${MATCHED_TOPICS[*]}")
  echo "Matched topics: $JOINED_TOPICS"
else
  echo "ERROR: No topics matched with '${data_TOPIC_TITLE}'"
  echo "Creating topics now..."
  chmod +x ./create_topics.sh
  ./create_topics.sh
  
  # Retry topic listing
  readarray -t TOPICS < <(./kafka-list-topics.sh)
  for topic in "${TOPICS[@]}"; do
	  topic_prefix="${topic%%_*}"  # Extract part before underscore
	  echo TOPIC PREFIX is $topic_prefix
    if [[ "$topic_prefix" == "${data_TOPIC_TITLE}" ]]; then
      MATCHED_TOPICS+=("$topic")
    fi
  done
fi

# === Logging setup ===
log_path="./logs/consumer/${CURRENT_DATE}/${CURRENT_TIME}"
mkdir -p "${log_path}"

# === Kill any running Python (.py) or Shell (.sh) scripts ===
echo "[INFO] Searching for and terminating any running .py or .sh scripts"

# Get current script PID and parent PID
SELF_PID=$$
PARENT_PID=$(ps -o ppid= -p "$SELF_PID" | tr -d ' ')

# Kill Python (.py) scripts except this one
for pid in $(pgrep -f '\.py'); do
  if [[ "$pid" != "$SELF_PID" && "$pid" != "$PARENT_PID" ]]; then
    echo "[INFO] Killing Python script PID $pid"
    kill -9 "$pid" || echo "[WARN] Failed to kill PID $pid"
  fi
done

# Kill Shell (.sh) scripts except this one
for pid in $(pgrep -f '\.sh'); do
  if [[ "$pid" != "$SELF_PID" && "$pid" != "$PARENT_PID" ]]; then
    echo "[INFO] Killing Shell script PID $pid"
    kill -9 "$pid" || echo "[WARN] Failed to kill PID $pid"
  fi
done
pod_name=$(hostname)
set -x
python3 confluent_consumer.py \
  --topics "${JOINED_TOPICS}" \
  --uris "${server_uri}" \
  --groupId "${data_CONSUMER_GROUP_ID}" \
  --maxMsg "${data_MAX_MESSAGES}" \
  --pollTimeout "${data_POLL_TIMEOUT}" \
  --maxPollIntervalMs "${data_MAX_POLL_INTERVAL_MS}" \
  --heartbeatIntervalMs "${data_HEARTBEAT_INTERVAL_MS}" \
  --sessionTimeoutMs "${data_SESSION_TIMEOUT_MS}" \
  --socketTimeoutMs "${data_SOCKET_TIMEOUT_MS}" \
  --fetchMaxBytes "${data_FETCH_MAX_BYTES}" \
  --fetchMinBytes "${data_FETCH_MIN_BYTES}" \
  --fetchMaxWaitMs "${data_FETCH_MAX_WAIT_MS}" \
  --consumerOutputDir "${consumer_output_dir}" \
  --enableAutoCommit "${data_ENABLE_AUTO_COMMIT}" \
  --autoOffsetReset "${data_AUTO_OFFSET_RESET}" \
  --lagQueryTimeout "${data_LAG_QUERY_TIMEOUT}" \
  --lagQueryInterval "${data_LAG_QUERY_INTERVAL}" \
  --appDelayMinMs "${data_APP_DELAY_MIN_MS}" \
  --appDelayMaxMs "${data_APP_DELAY_MAX_MS}" \
  --appDelayMode "${data_APP_DELAY_MODE}" \
  2>&1 | tee "${log_path}/${pod_name}_tr_${data_TARGET_RATE}.log"
set +x
