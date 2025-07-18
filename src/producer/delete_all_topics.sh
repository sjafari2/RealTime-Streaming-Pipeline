#!/usr/bin/env bash

set -euo pipefail

echo "[INFO] Fetching Kafka broker addresses..."
server_uri=$(bash get_kafka_producer_dns.sh | sed 's/\[\|\]//g' | tr -d '"' | tr '\n' ',' | sed 's/,$//')

echo "[INFO] Using bootstrap servers: ${server_uri}"

echo "[INFO] Listing all topics..."
mapfile -t topics < <(${KAFKA_INSTALL_PATH}/kafka-topics.sh --list --bootstrap-server "${server_uri}" --command-config ./producer.properties)

if [[ ${#topics[@]} -eq 0 ]]; then
  echo "[INFO] No topics found. Nothing to delete."
  exit 0
fi

echo "[INFO] Topics found:"
printf ' - %s\n' "${topics[@]}"

# Loop and delete
for topic in "${topics[@]}"; do
  echo "[INFO] Deleting topic: $topic"
  output=$(${KAFKA_INSTALL_PATH}/kafka-topics.sh --bootstrap-server "${server_uri}" \
          --command-config ./producer.properties \
          --delete --topic "$topic" 2>&1)

  echo "[INFO] Delete command output for $topic:"
  echo "$output"
done

echo "[SUCCESS] All topics processed."

