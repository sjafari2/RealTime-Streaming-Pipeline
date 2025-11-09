#!/usr/bin/env bash
set -euo pipefail

# Locate kafka-topics.sh
KAFKA_TOPICS="${KAFKA_TOPICS:-}"
if [[ -z "${KAFKA_TOPICS}" ]]; then
  if [[ -n "${KAFKA_INSTALL_PATH:-}" && -x "${KAFKA_INSTALL_PATH}/kafka-topics.sh" ]]; then
    KAFKA_TOPICS="${KAFKA_INSTALL_PATH}/kafka-topics.sh"
  elif command -v kafka-topics.sh >/dev/null 2>&1; then
    KAFKA_TOPICS="$(command -v kafka-topics.sh)"
  elif [[ -x "/kafka/bin/kafka-topics.sh" ]]; then
    KAFKA_TOPICS="/kafka/bin/kafka-topics.sh"
  else
    echo "[ERROR] kafka-topics.sh not found. Set KAFKA_INSTALL_PATH or add to PATH." >&2
    exit 1
  fi
fi

# Resolve bootstrap servers
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BOOTSTRAP_SERVERS="$("${SCRIPT_DIR}/get_bootstrap_servers.sh")"
echo "[INFO] Bootstrap servers: ${BOOTSTRAP_SERVERS}"

# List topics
"${KAFKA_TOPICS}" --list --bootstrap-server "${BOOTSTRAP_SERVERS}"

