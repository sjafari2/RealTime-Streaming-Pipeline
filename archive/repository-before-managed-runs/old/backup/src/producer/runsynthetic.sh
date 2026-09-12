#!/usr/bin/env bash

set -euo pipefail
trap "exit" INT TERM
trap "kill 0" EXIT

# Path to mounted ConfigMap
CONFIG_FILE="/config/pipeline-configmap.yaml"

# Export all config entries from .data with prefix data_
eval $(
  yq eval '.data | to_entries | map("export data_" + .key + "=" + (.value | @sh)) | .[]' "$CONFIG_FILE"
)
CURRENT_DATE=$(TZ=America/Denver date +"%Y-%m-%d")
CURRENT_TIME=$(TZ=America/Denver date +"%H-%M-%S")
log_path="./logs/producer/${CURRENT_DATE}/${CURRENT_TIME}"
mkdir -p "$log_path"
pod_name=$(hostname)
echo "$pod_name"

# Launch producer using environment variables
python3 confluent_kafka_producer.py \
    --topicTitle "${data_TOPIC_TITLE}" \
    --numTopics "${data_TOPIC_COUNT}" \
    --delay "${data_DELAY_BETWEEN_MESSAGES}" \
    --numPartitions "${data_NUM_PARTITIONS}" \
    --replica "${data_REPLICATION_FACTOR}" \
    --randomRange "${data_RANDOM_RANGE:-100}" \
    --lingerMs "${data_LINGER_MS}" \
    --compressionType "${data_COMPRESSION_TYPE}" \
    --batchSize "${data_BATCH_SIZE}" \
    --msgMaxBytes "${data_MSG_MAX_BYTES}" \
    --metadataMaxAgeMs "${data_METADATA_MAX_AGE_MS}" \
    --topicMetadataRefreshIntervalMs "${data_TOPIC_METADATA_REFRESH_INTERVAL_MS}" \
    --maxInFlightRequestsPerConnection "${data_MAX_IN_FLIGHT_REQUEST_PER_CONNECTION}" \
    --acks "${data_ACKS}" \
    --retries "${data_RETRIES}" \
    --retryBackoffMs "${data_RETRY_BACKOFF_MS}" \
    --reconnectBackoffMs "${data_RECONNECT_BACKOFF_MS}" \
    --reconnectBackoffMaxMs "${data_RECONNECT_BACKOFF_MAX_MS}" \
    --minInSync "${data_MIN_INSYNC_REPLICAS}" \
    --requestTimeoutMs "${data_REQUEST_TIMEOUT_MS}" \
    --deliveryTimeoutMs "${data_DELIVERY_TIMEOUT_MS}" \
    --queueBufferingMaxMessages "${data_QUEUE_BUFFERING_MAX_MESSAGES}" \
    --queueBufferingMaxKbytes "${data_QUEUE_BUFFERING_MAX_KBYTES}" \
    --targetRate "${data_TARGET_RATE}" \
    --connectionsMaxIdleMs "${data_CONNECTION_MAX_IDLE_MS}" \
    --socketKeepaliveEnable \
    2>&1 | tee "$log_path/producer_${pod_name}.log" 

