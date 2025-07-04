#!/usr/bin/env bash

set -euo pipefail

# === Load configuration ===
source parseYaml.sh
eval $(parse_yaml /config/pipeline-configmap.yaml)
: "${TOPIC_TITLE:?TOPIC_TITLE not set in config}"
trap "exit" INT TERM
trap "kill 0" EXIT

CURRENT_DATE=$(TZ=America/Denver date +"%Y-%m-%d")

# === Config values ===
#topic_title="${data_TOPIC_TITLE}"
topic_title="${TOPIC_TITLE}" #:?TOPIC_TITLE not defined in config}"
group_id="${CONSUMER_GROUP_ID}"
msg_max="${MAX_MESSAGES}"
poll_timeout="${POLL_TIMEOUT}"
fetch_max_bytes="${FETCH_MAX_BYTES}"
fetch_min_bytes="${FETCH_MIN_BYTES}"
fetch_max_wait_ms="${FETCH_MAX_WAIT_MS}"
auto_commit="${ENABLE_AUTO_COMMIT}"
offset_reset="${AUTO_OFFSET_RESET}"
consumer_output_dir="${CONSUMER_OUTPUT_DIR}/${CURRENT_DATE}"
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
pkill -f confluent_consumer.py || true

# === Launch consumer ===
python3 confluent_consumer.py \
    --topics "${MATCHED_TOPICS}" \
    --uris "${server_uri}" \
    --groupId "${group_id}" \
    --maxMsg "${msg_max}" \
    --pollTimeout "${poll_timeout}" \
    --fetchMaxBytes "${fetch_max_bytes}" \
    --fetchMinBytes "${fetch_min_bytes}" \
    --fetchMaxWaitMs "${fetch_max_wait_ms}" \
    --consumerOutputDir "${consumer_output_dir}" \
    --enableAutoCommit "${auto_commit}" \
    --autoOffsetReset "${offset_reset}" #\
    #&> "${log_path}/consumer_${pod_name}.log"

