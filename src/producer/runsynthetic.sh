#!/bin/bash

# Load configuration
source parseYaml.sh
eval $(parse_yaml /config/pipeline-configmap.yaml)

trap "exit" INT TERM
trap "kill 0" EXIT

# Extract variables from config
nproducers=${data_PRODUCER_COUNT}
num_topics=${data_PRODUCER_TOPIC_COUNT}
batch_size=${data_BATCH_SIZE}
delay=${data_DELAY_BETWEEN_MESSAGES}
topic_title=${data_TOPIC_TITLE}
num_partitions=${data_NUM_PARTITIONS}
replica=${data_REPLICATION_FACTOR}
random_range=${data_RANDOM_RANGE}
linger_ms=${data_LINGER_MS}
compression_type=${data_COMPRESSION_TYPE}
max_request_size=${data_MAX_REQUEST_SIZE}
acks=${data_ACKS}
pod_index=${1:-0}

# Optional: Get server URIs (currently not passed to Python, used inside Python)
chmod +x get_kafka_producer_dns.sh
server_uri=$(bash ./get_kafka_producer_dns.sh)

# Create logs
CURRENT_DATE=$(TZ=America/Denver date +"%Y-%m-%d")
CURRENT_TIME=$(TZ=America/Denver date +"%H-%M-%S")
log_path="./logs/producer/${CURRENT_DATE}/${CURRENT_TIME}/Pod_$pod_index"
mkdir -p "${log_path}"

# Kill old producer processes
pkill -f synthetic_producer.py || true

# Start producers
for ((i = 0; i < nproducers; i++)); do
    echo "Starting Producer[$i]..."
    python3 confluent_kafka_producer.py \
        --topicTitle "${topic_title}" \
        --numTopics "${num_topics}" \
        --delay "${delay}" \
        --numPartitions "${num_partitions}" \
        --replica "${replica}" \
        --randomRange "${random_range}" \
        --lingerMs "${linger_ms}" \
        --compressionType "${compression_type}" \
        --batchSize "${batch_size}" \
        --maxRequestSize "${max_request_size}" \
        --acks "${acks}" & # \
        #> "${log_path}/producer.$i.log" 2>&1 &
done

wait

