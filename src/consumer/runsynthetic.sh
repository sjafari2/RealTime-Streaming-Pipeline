#!/bin/bash

# Load config
source parseYaml.sh
eval $(parse_yaml /config/pipeline-configmap.yaml)
trap "exit" INT TERM
trap "kill 0" EXIT

# Config values
topic_title=${data_TOPIC_TITLE}
nconsumers=${data_CONSUMER_COUNT}
pod_count=${data_CONSUMER_POD_COUNT}
msg_max=${data_MAX_MESSAGES}
pod_index=${1:-0}

# Optional configs for consumer
auto_commit=${data_ENABLE_AUTO_COMMIT:-true}
offset_reset=${data_OFFSET_RESET:-earliest}

# Get list of topics matching the prefix
source kafka-list-topics.sh
TOPICS=$(printf "%s\n" "${TOPICS[@]}" | grep ${topic_title})
TOPICS=($TOPICS)
topics_len=${#TOPICS[@]}

# Get Kafka brokers
server_uri=$(bash get_kafka_consumer_dns.sh | sed 's/\[\|\]//g' | tr -d '"' | tr '\n' ',' | sed 's/,$//')

# Logging setup
CURRENT_DATE=$(TZ=America/Denver date +"%Y-%m-%d")
CURRENT_TIME=$(TZ=America/Denver date +"%H-%M-%S")
log_path="./logs/consumer/Pod_${pod_index}/${CURRENT_DATE}/${CURRENT_TIME}"
output_path="./consumer-result"
# consumer/Pod_${pod_index}/${CURRENT_DATE}/${CURRENT_TIME}"

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

    python3 confluent_consumer.py \
        --topics "$topic_str" \
        --groupId "consumer-group-${pod_index}-${i}" \
        --outputPath "${output_path}/consumer_pod${pod_index}_proc${i}_${CURRENT_DATE}_${CURRENT_TIME}.csv" \
        --uris "$server_uri" \
        --enableAutoCommit "$auto_commit" \
        --offsetReset "$offset_reset" \
        --maxMsg "$msg_max" & #\
        #>& "${log_path}/consumer_pod${pod_index}_proc${i}.out" &
done

wait

