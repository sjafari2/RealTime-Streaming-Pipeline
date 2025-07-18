#!/usr/bin/env bash

set -euo pipefail

trap "exit" INT TERM
trap "kill 0" EXIT

: "$(yq e '.TOPIC_TITLE' /config/pipeline-configmap.yaml)"  # check existence

CURRENT_DATE=$(TZ=America/Denver date +"%Y-%m-%d")

# === Config values ===
topic_title=$(yq e '.TOPIC_TITLE' /config/pipeline-configmap.yaml)
group_id=$(yq e '.CONSUMER_GROUP_ID' /config/pipeline-configmap.yaml)
msg_max=$(yq e '.MAX_MESSAGES' /config/pipeline-configmap.yaml)
poll_timeout=$(yq e '.POLL_TIMEOUT' /config/pipeline-configmap.yaml)
max_pool_records=$(yq e '.MAX_POLL_RECORDS' /config/pipeline-configmap.yaml)
session_timeout_ms=$(yq e '.SESSION_TIMEOUT_MS' /config/pipeline-configmap.yaml)
socket_timeout_ms=$(yq e '.SOCKET_TIMEOUT_MS' /config/pipeline-configmap.yaml)
fetch_max_bytes=$(yq e '.FETCH_MAX_BYTES' /config/pipeline-configmap.yaml)
fetch_min_bytes=$(yq e '.FETCH_MIN_BYTES' /config/pipeline-configmap.yaml)
fetch_max_wait_ms=$(yq e '.FETCH_MAX_WAIT_MS' /config/pipeline-configmap.yaml)
queued_min_msg=$(yq e '.QUEUED_MIN_MESSAGES' /config/pipeline-configmap.yaml)
max_poll_interval_ms=$(yq e '.MAX_POLL_INTERVAL_MS' /config/pipeline-configmap.yaml)
auto_commit=$(yq e '.ENABLE_AUTO_COMMIT' /config/pipeline-configmap.yaml)
auto_commit_interval_ms=$(yq e '.AUTO_COMMIT_INTERVAL_MS' /config/pipeline-configmap.yaml)
offset_reset=$(yq e '.AUTO_OFFSET_RESET' /config/pipeline-configmap.yaml)
consumer_output_dir="$(yq e '.CONSUMER_OUTPUT_DIR' /config/pipeline-configmap.yaml)/${CURRENT_DATE}"

mkdir -p "${consumer_output_dir}"

# === Get brokers ===
server_uri=$(bash get_kafka_consumer_dns.sh | sed 's/\[\|\]//g' | tr -d '"' | tr '\n' ',' | sed 's/,$//')

if [ -z "${topic_title}" ]; then
    echo "ERROR: TOPIC_TITLE is empty in ConfigMap."
    exit 1
fi

# === Get topics matching prefix ===
IFS=$'\n' read -r -d '' -a TOPICS < <(
    ${KAFKA_INSTALL_PATH}/kafka-topics.sh --list --bootstrap-server "${server_uri}" --command-config ./consumer.properties && printf '\0'
)

MATCHED_TOPICS=$(printf "%s\n" "${TOPICS[@]}" | grep -E "^${topic_title}" | paste -sd "," -)

if [ -z "${MATCHED_TOPICS}" ]; then
    echo "ERROR: No topics matched with prefix '${topic_title}'."
    exit 1
fi

echo "Matched topics: ${MATCHED_TOPICS}"

# === Logging setup ===
log_path="./logs/consumer"
mkdir -p "${log_path}"
pod_name=$(hostname)

# === Kill old consumers safely ===
#pkill -f confluent_consumer.py || true

# === Launch consumer ===
python3 confluent_consumer.py \
    --topics ${MATCHED_TOPICS} \
    --uris ${server_uri} \
    --groupId ${group_id} \
    --maxMsg ${msg_max} \
    --maxPoolRecords ${max_pool_records} \
    --pollTimeout ${poll_timeout} \
    --maxPollIntervalMs ${max_poll_interval_ms} \
    --queuedMinMessages ${queued_min_msg}
    --socketTimeoutMs ${socket_timeout_ms} \
    --sessionTimeoutMs ${session_timeout_ms} \
    --fetchMaxBytes ${fetch_max_bytes} \
    --fetchMinBytes ${fetch_min_bytes} \
    --fetchMaxWaitMs ${fetch_max_wait_ms} \
    --consumerOutputDir ${consumer_output_dir} \
    --enableAutoCommit ${auto_commit} \
    --autoCommitIntervalMs ${auto_commit_interval_ms} \
    --autoOffsetReset ${offset_reset} \
     2>&1 | tee "${log_path}/consumer_${pod_name}.log" &

