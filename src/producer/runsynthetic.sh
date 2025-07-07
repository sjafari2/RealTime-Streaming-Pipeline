#!/usr/bin/env bash

set -euo pipefail

source parseYaml.sh
eval $(parse_yaml /config/pipeline-configmap.yaml)
trap "exit" INT TERM
trap "kill 0" EXIT

# Extract config
num_topics=${TOPIC_COUNT}
batch_size=${BATCH_SIZE}
delay=${DELAY_BETWEEN_MESSAGES}
topic_title=${TOPIC_TITLE}
num_partitions=${NUM_PARTITIONS}
replica=${REPLICATION_FACTOR}
random_range=${RANDOM_RANGE}
linger_ms=${LINGER_MS}
compression_type=${COMPRESSION_TYPE}
max_request_size=${MAX_REQUEST_SIZE}
acks=${ACKS}
request_timeout_ms=${REQUEST_TIMEOUT_MS}
delivery_timeout_ms=${DELIVERY_TIMEOUT_MS}
queue_buffering_max_messages=${QUEUE_BUFFERING_MAX_MESSAGES}
queue_buffering_max_kbytes=${QUEUE_BUFFERING_MAX_KBYTES}

# Logging
log_path="./logs/producer"
mkdir -p "$log_path"
pod_name=$(hostname)

# Kill old producer
pkill -f confluent_kafka_producer.py || true
#pkill -f runsynthetic.sh || true

# Launch single producer per pod
python3 confluent_kafka_producer.py \
    --topicTitle "$topic_title" \
    --numTopics "$num_topics" \
    --delay "$delay" \
    --numPartitions "$num_partitions" \
    --replica "$replica" \
    --randomRange "$random_range" \
    --lingerMs "$linger_ms" \
    --compressionType "$compression_type" \
    --batchSize "$batch_size" \
    --maxRequestSize "$max_request_size" \
    --acks "$acks" \
    --requestTimeoutMs "$request_timeout_ms" \
    --deliveryTimeoutMs "$delivery_timeout_ms" \
    --queueBufferingMaxMessages "$queue_buffering_max_messages" \
    --queueBufferingMaxKbytes "$queue_buffering_max_kbytes" #\
    #&> "$log_path/producer_${pod_name}.log"
