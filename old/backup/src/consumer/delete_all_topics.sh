#!/usr/bin/env bash

set -euo pipefail

echo "[INFO] Fetching Kafka broker addresses..."
server_uri=$(bash get_kafka_consumer_dns.sh | sed 's/\[\|\]//g' | tr -d '"' | tr '\n' ',' | sed 's/,$//')

echo "[INFO] Using bootstrap servers: ${server_uri}"

echo "[INFO] Listing all topics..."
topics=$(${KAFKA_INSTALL_PATH}/kafka-topics.sh --list --bootstrap-server "${server_uri}")

if [[ -z "$topics" ]]; then
  echo "[INFO] No topics found. Nothing to delete."
  exit 0
fi

echo "[INFO] Topics found:"
echo "$topics"

# Loop and delete all non-internal topics
for topic in $topics; do
  if [[ "$topic" == __* ]]; then
    echo "[SKIP] Preserving internal/system topic: $topic"
    continue
  fi

  echo "[INFO] Deleting topic: $topic"
  output=$(${KAFKA_INSTALL_PATH}/kafka-topics.sh --bootstrap-server "${server_uri}" \
          --delete --topic "$topic" 2>&1)

  echo "[INFO] Delete command output for $topic:"
  echo "$output"
done

echo "[SUCCESS] All non-internal topics deleted."

