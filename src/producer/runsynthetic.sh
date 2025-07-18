#!/usr/bin/env bash

set -euo pipefail

trap "exit" INT TERM
trap "kill 0" EXIT

# Extract config from /config/pipeline-configmap.yaml using yq
num_topics=$(yq e '.TOPIC_COUNT' /config/pipeline-configmap.yaml)
batch_size=$(yq e '.BATCH_SIZE' /config/pipeline-configmap.yaml)
delay=$(yq e '.DELAY_BETWEEN_MESSAGES' /config/pipeline-configmap.yaml)
topic_title=$(yq e '.TOPIC_TITLE' /config/pipeline-configmap.yaml)
num_partitions=$(yq e '.NUM_PARTITIONS' /config/pipeline-configmap.yaml)
replica=$(yq e '.REPLICATION_FACTOR' /config/pipeline-configmap.yaml)
random_range=$(yq e '.RANDOM_RANGE' /config/pipeline-configmap.yaml)
linger_ms=$(yq e '.LINGER_MS' /config/pipeline-configmap.yaml)
compression_type=$(yq e '.COMPRESSION_TYPE' /config/pipeline-configmap.yaml)
max_request_size=$(yq e '.MAX_REQUEST_SIZE' /config/pipeline-configmap.yaml)
acks=$(yq e '.ACKS' /config/pipeline-configmap.yaml)
request_timeout_ms=$(yq e '.REQUEST_TIMEOUT_MS' /config/pipeline-configmap.yaml)
delivery_timeout_ms=$(yq e '.DELIVERY_TIMEOUT_MS' /config/pipeline-configmap.yaml)
queue_buffering_max_messages=$(yq e '.QUEUE_BUFFERING_MAX_MESSAGES' /config/pipeline-configmap.yaml)
queue_buffering_max_kbytes=$(yq e '.QUEUE_BUFFERING_MAX_KBYTES' /config/pipeline-configmap.yaml)
retries=$(yq e '.RETRIES' /config/pipeline-configmap.yaml)
retry_backoff_ms=$(yq e '.RETRY_BACKOFF_MS' /config/pipeline-configmap.yaml)
min_insync_replicas=$(yq e '.MIN_INSYNC_REPLICAS' /config/pipeline-configmap.yaml)
target_rate=$(yq e '.TARGET_RATE' /config/pipeline-configmap.yaml)
connections_max_idle_ms=$(yq e '.CONNECTION_MAX_IDLE_MS' /config/pipeline-configmap.yaml)
reconnect_backoff_max_ms=$(yq e '.RECONNECT_BACKOFF_MAX_MS' /config/pipeline-configmap.yaml)
reconnect_backoff_ms=$(yq e '.RECONNECT_BACKOFF_MS' /config/pipeline-configmap.yaml)


# Logging
log_path="./logs/producer"
mkdir -p "$log_path"
pod_name=$(hostname)
echo $pod_name

# Kill old producer
#pkill -f confluent_kafka_producer.py || true
#pkill -f runsynthetic.sh || true

# Launch single producer per pod
python3 confluent_kafka_producer.py \
    --topicTitle "${topic_title}" \
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
    --retries "$retries" \
    --retryBackoffMs "$retry_backoff_ms" \
    --reconnectBackoffMs "$reconnect_backoff_ms" \
    --reconnectBackoffMaxMs "$reconnect_backoff_max_ms" \
    --minInSync "$min_insync_replicas" \
    --requestTimeoutMs "$request_timeout_ms" \
    --deliveryTimeoutMs "$delivery_timeout_ms" \
    --queueBufferingMaxMessages "$queue_buffering_max_messages" \
    --queueBufferingMaxKbytes "$queue_buffering_max_kbytes" \
    --targetRate "$target_rate" \
    --connectionsMaxIdleMs "$connections_max_idle_ms" \
    --socketKeepaliveEnable  \
     2>&1 | tee "$log_path/producer_${pod_name}.log" &
