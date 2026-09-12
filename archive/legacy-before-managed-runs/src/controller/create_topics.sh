#!/usr/bin/env bash
set -euo pipefail

CONFIGMAP_PATH="${CONFIGMAP_PATH:-/config/pipeline-configmap.yaml}"

# Read config with yq if available, else grep/awk
get_cfg() {
  local key="$1" default="${2:-}"
  if command -v yq >/dev/null 2>&1 && [[ -f "${CONFIGMAP_PATH}" ]]; then
    val="$(yq -r ".data.${key} // empty" "${CONFIGMAP_PATH}" || true)"
    [[ -n "${val}" ]] && { echo -n "${val}"; return; }
  fi
  if [[ -f "${CONFIGMAP_PATH}" ]]; then
    val="$(grep -E "^ *${key}:" "${CONFIGMAP_PATH}" | awk -F\" '{print $2}' | head -n1 || true)"
    [[ -n "${val}" ]] && { echo -n "${val}"; return; }
  fi
  echo -n "${default}"
}

TOPIC_COUNT="$(get_cfg TOPIC_COUNT 1)"
TOPIC_TITLE="$(get_cfg TOPIC_TITLE topic)"
RETENTION_MS="$(get_cfg RETENTION_MS 1800000)"
SEGMENT_MS="$(get_cfg SEGMENT_MS 600000)"
CLEANUP_POLICY="$(get_cfg CLEANUP_POLICY delete)"
NUM_PARTITIONS="$(get_cfg NUM_PARTITIONS 36)"
REPLICATION_FACTOR="$(get_cfg REPLICATION_FACTOR 1)"

# Locate kafka-topics.sh
KAFKA_TOPICS="${KAFKA_TOPICS:-}"
if [[ -z "${KAFKA_TOPICS}" ]]; then
  if [[ -n "${KAFKA_INSTALL_PATH:-}" && -x "${KAFKA_INSTALL_PATH}/kafka-topics.sh" ]]; then
    KAFKA_TOPICS="${KAFKA_INSTALL_PATH}/kafka-topics.sh"
  elif command -v kafka-topics.sh >/dev/null 2>&1; then
    KAFKA_TOPICS="$(command -v kafka-topics.sh)"
  elif [[ -x "/opt/bitnami/kafka/bin/kafka-topics.sh" ]]; then
    KAFKA_TOPICS="/opt/bitnami/kafka/bin/kafka-topics.sh"
  else
    echo "[ERROR] kafka-topics.sh not found. Set KAFKA_INSTALL_PATH or add to PATH." >&2
    exit 1
  fi
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BOOTSTRAP_SERVERS="$("${SCRIPT_DIR}/get_bootstrap_servers.sh")"
echo "[INFO] Bootstrap servers: ${BOOTSTRAP_SERVERS}"

# Build topic list
declare -a topic_names=()
for i in $(seq 0 $((TOPIC_COUNT - 1))); do
  topic_names+=("${TOPIC_TITLE}_${i}")
done

# Create topics (idempotent)
for topic in "${topic_names[@]}"; do
  echo "[INFO] Creating topic: ${topic}"
  set +e
  "${KAFKA_TOPICS}" --create \
    --bootstrap-server "${BOOTSTRAP_SERVERS}" \
    --topic "${topic}" \
    --partitions "${NUM_PARTITIONS}" \
    --replication-factor "${REPLICATION_FACTOR}" \
    --config retention.ms="${RETENTION_MS}" \
    --config segment.ms="${SEGMENT_MS}" \
    --config cleanup.policy="${CLEANUP_POLICY}"
  rc=$?
  set -e
  if [[ $rc -ne 0 ]]; then
    echo "[WARN] Create returned non-zero (topic may already exist): ${topic}"
  fi
done

echo "[SUCCESS] Topic creation completed."

