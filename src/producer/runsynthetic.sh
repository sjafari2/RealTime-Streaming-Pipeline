#!/bin/bash

# Load configuration
source parseYaml.sh
eval $(parse_yaml pipeline-configmap.yaml)

trap "exit" INT TERM
trap "kill 0" EXIT

# Extract variables
nproducers=${data_PRODUCER_COUNT}
num_topics=${data_PRODUCER_TOPIC_COUNT}
batch_size=${data_BATCH_SIZE}
delay=${data_DELAY_BETWEEN_MESSAGES}
topic_title=${data_TOPIC_TITLE}
num_partitions=${data_NUM_PARTITIONS}
replica=${data_REPLICATION_FACTOR}
random_range=${data_RANDOM_RANGE}
pod_index=${1:-0}

chmod +x get_kafka_producer_dns.sh
server_uri=$(bash ./get_kafka_producer_dns.sh)

# Create logs
CURRENT_DATE=$(TZ=America/Denver date +"%Y-%m-%d")
CURRENT_TIME=$(TZ=America/Denver date +"%H-%M-%S")
log_path="./logs/producer/${topic_title}/${CURRENT_DATE}/${CURRENT_TIME}/Pod_$pod_index"
mkdir -p "${log_path}"

# Kill old processes
pkill -f synthetic_producer.py || true

# Start producers (forever loops are inside the Python script)
for ((i = 0; i < nproducers; i++)); do
    echo "Starting Producer[$i]..."
    python3 synthetic_producer.py \
        --topicTitle "${topic_title}" \
        --numTopics "${num_topics}" \
        --delay "${delay}" \
        --numPartitions "${num_partitions}" \
        --replica "${replica}" \
        --randomRange "${random_range}" \
        > "${log_path}/producer.$i.log" 2>&1 &
done

wait

