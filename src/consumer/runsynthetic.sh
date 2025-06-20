#!/bin/bash

# Load config
source parseYaml.sh
eval $(parse_yaml pipeline-configmap.yaml)
trap "exit" INT TERM
trap "kill 0" EXIT

# Config values
topic_title=${data_TOPIC_TITLE}
nconsumers=${data_CONSUMER_COUNT}
pod_count=${data_CONSUMER_POD_COUNT}
msg_max=${data_MAX_MESSAGES}
pod_index=${1:-0}

# Optional consumer tuning params from ConfigMap (with defaults if missing)
auto_commit=${data_ENABLE_AUTO_COMMIT:-true}
offset_reset=${data_OFFSET_RESET:-earliest}
poll_timeout=${data_POLL_TIMEOUT:-300}
max_records=${data_MAX_RECORDS:-500}
fetch_max_bytes=${data_FETCH_MAX_BYTES:-10485760}
fetch_min_bytes=${data_FETCH_MIN_BYTES:-1024}
fetch_max_wait_ms=${data_FETCH_MAX_WAIT_MS:-500}
result_path=${data_RESULT_PATH}

# Get list of topics matching the prefix
source kafka-list-topics.sh
TOPICS=$(printf "%s\n" "${TOPICS[@]}" | grep ${topic_title})
TOPICS=($TOPICS)
topics_len=${#TOPICS[@]}
echo Mathced topics are ${TOPICS[@]}

# Get Kafka brokers
server_uri=$(bash get_kafka_consumer_dns.sh | sed 's/\[\|\]//g' | tr -d '"' | tr '\n' ',' | sed 's/,$//')

# Logging setup
CURRENT_DATE=$(TZ=America/Denver date +"%Y-%m-%d")
CURRENT_TIME=$(TZ=America/Denver date +"%H-%M-%S")
log_path="./logs/consumer/Pod_${pod_index}/"
output_path="${result_path}/Pod_${pod_index}"

mkdir -p "${log_path}"
mkdir -p "${output_path}"

# Kill any existing consumer processes
pgrep -f synthetic_consumer.py > /dev/null && pkill -f synthetic_consumer.py

# Calculate topic assignment per process
total_consumers=$((pod_count * nconsumers))
topics_per_consumer=$((topics_len / total_consumers))
extra_topics=$((topics_len % total_consumers))

# Launch consumer processes
for ((i = 0; i < nconsumers; i++)); do
    global_index=$((pod_index * nconsumers + i))

    if [ "$global_index" -lt "$extra_topics" ]; then
        count=$((topics_per_consumer + 1))
        start=$((global_index * count))
    else
        count=$topics_per_consumer
        start=$((extra_topics * (topics_per_consumer + 1) + (global_index - extra_topics) * topics_per_consumer))
    fi

    assigned_topics=("${TOPICS[@]:$start:$count}")
    topic_str=$(IFS=, ; echo "${assigned_topics[*]}")

    echo "Launching consumer [$i] in pod [$pod_index] for topics: $topic_str"

    python3 synthetic_consumer.py \
        --topics "$topic_str" \
        --groupId "consumer-group-${pod_index}-${i}" \
        --outputPath "${output_path}/consumer_pod${pod_index}_proc${i}_${CURRENT_DATE}_${CURRENT_TIME}.csv" \
        --uris "$server_uri" \
        --enableAutoCommit "$auto_commit" \
        --offsetReset "$offset_reset" \
        --maxMsg "$msg_max" \
        --pollTimeout "$poll_timeout" \
        --maxRecords "$max_records" \
        --fetchMaxBytes "$fetch_max_bytes" \
        --fetchMinBytes "$fetch_min_bytes" \
        --fetchMaxWaitMs "$fetch_max_wait_ms" &#\
        #>& "${log_path}/consumer_pod${pod_index}_proc${i}.out" &
done

wait

