#!/bin/bash

# Required config
TOPICS="messaglatency_0,messaglatency_1,messaglatency_2,messaglatency_3,messaglatency_4"
BROKER_URI="pip-kafka-controller-headless.kafkastreamingdata.svc.cluster.local:9092"
GROUP_ID="consgroup"
MAX_MESSAGES=500
POLL_TIMEOUT=500
FETCH_MAX_BYTES=20971520
FETCH_MIN_BYTES=10240
FETCH_MAX_WAIT_MS=20
MAX_POLL_INTERVAL_MS=120000
SOCKET_TIMEOUT_MS=60000
AUTO_COMMIT="false"
AUTO_OFFSET_RESET="earliest"
CONSUMER_OUTPUT_DIR="/app/consumer-merge-data/consumer-result"

# Run the consumer script
python3 simple_consumer.py \
  --topics "$TOPICS" \
  --uris "$BROKER_URI" \
  --groupId "$GROUP_ID" \
  --maxMsg "$MAX_MESSAGES" \
  --pollTimeout "$POLL_TIMEOUT" \
  --fetchMaxBytes "$FETCH_MAX_BYTES" \
  --fetchMinBytes "$FETCH_MIN_BYTES" \
  --fetchMaxWaitMs "$FETCH_MAX_WAIT_MS" \
  --maxPollIntervalMs "$MAX_POLL_INTERVAL_MS" \
  --socketTimeoutMs "$SOCKET_TIMEOUT_MS" \
  --enableAutoCommit "$AUTO_COMMIT" \
  --autoOffsetReset "$AUTO_OFFSET_RESET" \
  --consumerOutputDir "$CONSUMER_OUTPUT_DIR"

