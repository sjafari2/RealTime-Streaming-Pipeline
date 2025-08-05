#!/bin/bash
set -euo pipefail

chmod +x get_kafka_producer_list.sh
server_uri=$(bash get_kafka_producer_list.sh)

# List topics from Kafka broker
${KAFKA_INSTALL_PATH}/kafka-topics.sh --list --bootstrap-server "${server_uri}"  #--command-config ./consumer.properties

#topics_len=${#topics[@]}
