#!/usr/bin/env bash
set -euo pipefail

CONFIG_FILE="${CONFIG_FILE:-/config/pipeline-configmap.yaml}"

# 1) Try to get RELEASE_NAME/NAMESPACE/BROKER_COUNT via yq, else grep, else defaults
get_val() {
  local key="$1"
  if command -v yq >/dev/null 2>&1 && [[ -f "$CONFIG_FILE" ]]; then
    yq -r ".data.${key} // empty" "$CONFIG_FILE" || true
  elif [[ -f "$CONFIG_FILE" ]]; then
    # fall back to grep (expects: KEY: "value")
    grep -E "^ *${key}:" "$CONFIG_FILE" | awk -F\" '{print $2}' | head -n1 || true
  else
    echo ""
  fi
}

RELEASE_NAME="${RELEASE_NAME:-$(get_val RELEASE_NAME)}"
NAMESPACE="${NAMESPACE:-$(get_val NAMESPACE)}"
BROKER_COUNT="${BROKER_COUNT:-$(get_val BROKER_COUNT)}"

# Sensible defaults if still empty
RELEASE_NAME="${RELEASE_NAME:-pip}"
NAMESPACE="${NAMESPACE:-kafkastreamingdata}"
BROKER_COUNT="${BROKER_COUNT:-5}"

# 2) Build per-pod controller+broker client endpoints (9092)
servers=()
for ((i=0; i<${BROKER_COUNT}; i++)); do
  servers+=("${RELEASE_NAME}-kafka-controller-${i}.${RELEASE_NAME}-kafka-controller-headless.${NAMESPACE}.svc.cluster.local:9092")
done

# Echo comma-separated list (suitable for --bootstrap-server)
(IFS=,; echo "${servers[*]}")

