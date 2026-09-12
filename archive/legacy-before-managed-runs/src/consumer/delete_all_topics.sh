#!/usr/bin/env bash
set -euo pipefail

# Locate kafka-topics.sh (same logic)
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

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BOOTSTRAP_SERVERS="$("${SCRIPT_DIR}/get_bootstrap_servers.sh")"

echo "dirname(\$0)                = $(dirname "$0")"
echo "dirname(\${BASH_SOURCE[0]}) = $(dirname "${BASH_SOURCE[0]}")"

echo "[INFO] Using bootstrap servers: ${BOOTSTRAP_SERVERS}"

echo "[INFO] Listing all topics..."
# Don't exit on list failure; handle empty gracefully
set +e
topics="$("${KAFKA_TOPICS}" --list --bootstrap-server "${BOOTSTRAP_SERVERS}")"
rc=$?
set -e
if [[ $rc -ne 0 || -z "${topics}" ]]; then
  echo "[INFO] No topics found or cannot connect. Nothing to delete."
  exit 0
fi

echo "[INFO] Topics found:"
echo "${topics}"

# Delete all non-internal topics
while IFS= read -r topic; do
  [[ -z "${topic}" ]] && continue
  if [[ "${topic}" == __* ]]; then
    echo "[SKIP] Preserving internal/system topic: ${topic}"
    continue
  fi
  echo "[INFO] Deleting topic: ${topic}"
  set +e
  out="$("${KAFKA_TOPICS}" --bootstrap-server "${BOOTSTRAP_SERVERS}" --delete --topic "${topic}" 2>&1)"
  rc=$?
  set -e
  echo "[INFO] Delete output for ${topic}:"
  echo "${out}"
  if [[ $rc -ne 0 ]]; then
    echo "[WARN] Delete command returned non-zero for ${topic}."
  fi
done <<< "${topics}"

echo "[SUCCESS] All non-internal topics deleted."

