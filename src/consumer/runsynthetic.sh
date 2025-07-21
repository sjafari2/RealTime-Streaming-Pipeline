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
consumer_output_dir="${data_CONSUMER_OUTPUT_DIR}/${CURRENT_DATE}"
mkdir -p "${consumer_output_dir}"

# === Get brokers ===
server_uri=$(bash get_kafka_consumer_dns.sh | sed 's/\[\|\]//g' | tr -d '"' | tr '\n' ',' | sed 's/,$//')

if [ -z "${data_TOPIC_TITLE}" ]; then
    echo "ERROR: TOPIC_TITLE is empty in ConfigMap."
    exit 1
fi

# === Get topics matching prefix ===
IFS=$'\n' read -r -d '' -a TOPICS < <(
    ${KAFKA_INSTALL_PATH}/kafka-topics.sh --list --bootstrap-server "${server_uri}" --command-config ./consumer.properties && printf '\0'
)

MATCHED_TOPICS=$(printf "%s\n" "${TOPICS[@]}" | grep -E "^${data_TOPIC_TITLE}" | paste -sd "," -)

if [ -z "${MATCHED_TOPICS}" ]; then
    echo "ERROR: No topics matched with prefix '${data_TOPIC_TITLE}'."
    exit 1
fi

echo "Matched topics: ${MATCHED_TOPICS}"

# === Logging setup ===
log_path="./logs/consumer"
mkdir -p "${log_path}"
pod_name=$(hostname)

# === Launch consumer ===
python3 confluent_consumer.py \
    --topics "${MATCHED_TOPICS}" \
    --uris "${server_uri}" \
    --groupId "${data_CONSUMER_GROUP_ID}" \
    --maxMsg "${data_MAX_MESSAGES}" \
    --maxPoolRecords "${data_MAX_POLL_RECORDS}" \
    --pollTimeout "${data_POLL_TIMEOUT}" \
    --maxPollIntervalMs "${data_MAX_POLL_INTERVAL_MS}" \
    --queuedMinMessages "${data_QUEUED_MIN_MESSAGES}" \
    --socketTimeoutMs "${data_SOCKET_TIMEOUT_MS}" \
    --sessionTimeoutMs "${data_SESSION_TIMEOUT_MS}" \
    --fetchMaxBytes "${data_FETCH_MAX_BYTES}" \
    --fetchMinBytes "${data_FETCH_MIN_BYTES}" \
    --fetchMaxWaitMs "${data_FETCH_MAX_WAIT_MS}" \
    --consumerOutputDir "${consumer_output_dir}" \
    --enableAutoCommit "${data_ENABLE_AUTO_COMMIT}" \
    --autoCommitIntervalMs "${data_AUTO_COMMIT_INTERVAL_MS}" \
    --autoOffsetReset "${data_AUTO_OFFSET_RESET}" \
     2>&1 | tee "${log_path}/consumer_${pod_name}.log" 

