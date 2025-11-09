#!/usr/bin/env bash

set -euo pipefail

# === CONFIG ===
CONFIGMAP_PATH="/config/pipeline-configmap.yaml"
#COMMAND_CONFIG="./consumer.properties"         
KAFKA_TOPICS="${KAFKA_INSTALL_PATH}/kafka-topics.sh"

# === Extract values from ConfigMap ===
TOPIC_COUNT=$(grep 'TOPIC_COUNT:' "$CONFIGMAP_PATH" | awk -F '"' '{print $2}')
TOPIC_TITLE=$(grep 'TOPIC_TITLE:' "$CONFIGMAP_PATH" | awk -F '"' '{print $2}')
RETENTION_MS=$(grep 'RETENTION_MS:' "$CONFIGMAP_PATH" | awk -F '"' '{print $2}')
SEGMENT_MS=$(grep 'SEGMENT_MS:' "$CONFIGMAP_PATH" | awk -F '"' '{print $2}')
CLEANUP_POLICY=$(grep 'CLEANUP_POLICY:' "$CONFIGMAP_PATH" | awk -F '"' '{print $2}')
NUM_PARTITIONS=$(grep 'NUM_PARTITIONS:' "$CONFIGMAP_PATH" | awk -F '"' '{print $2}')


# === Get Kafka bootstrap servers ===
echo "[INFO] Resolving Kafka bootstrap servers..."
BOOTSTRAP_SERVERS=$(bash get_kafka_consumer_dns.sh | sed 's/\[\|\]//g' | tr -d '"' | tr '\n' ',' | sed 's/,$//')
echo "[INFO] Bootstrap servers: $BOOTSTRAP_SERVERS"

# === Build topic list
declare -a topic_names
for i in $(seq 0 $((TOPIC_COUNT - 1))); do
  topic_names+=("${TOPIC_TITLE}_${i}")
done

# === First: Create topics
for topic in "${topic_names[@]}"; do
  echo "[INFO] Creating topic: $topic"
  $KAFKA_TOPICS --create \
    --bootstrap-server "$BOOTSTRAP_SERVERS" \
    --topic "$topic" \
    --partitions "$NUM_PARTITIONS" \
    --replication-factor 1 \
    --config retention.ms="$RETENTION_MS" \
    --config segment.ms="$SEGMENT_MS" \
    --config cleanup.policy="$CLEANUP_POLICY" \
    || echo "[WARN] Topic $topic may already exist"
done

# === Then: Describe topics
#echo -e "\n[INFO] Describing all created topics:"
#for topic in "${topic_names[@]}"; do
#  echo -e "\n-----------------------------"
#  echo "[INFO] Topic: $topic"
#  $KAFKA_TOPICS --describe \
#    --bootstrap-server "$BOOTSTRAP_SERVERS" \
#    --topic "$topic"
#done

echo -e "\n[SUCCESS] Topic creation and verification completed."

