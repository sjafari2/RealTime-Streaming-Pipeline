#!/usr/bin/env bash
set -euo pipefail

# Config path: from env or default
CONFIG_PATH="${PIPELINE_CONFIG:-/config/pipeline-configmap.yaml}"

LOG_DIR="/app/controller-data/logs"
INTERVAL="${CONTROLLER_INTERVAL_SEC:-15}"

# Heartbeat (for future liveness probes if needed)
HEARTBEAT_DIR="/tmp/lag-controller"
HEARTBEAT_FILE="${HEARTBEAT_DIR}/healthy"

mkdir -p "$LOG_DIR"
mkdir -p "$HEARTBEAT_DIR"

echo "[lag-controller] Starting lag controller loop..."
echo "[lag-controller] Using config: $CONFIG_PATH"
echo "[lag-controller] Interval: ${INTERVAL}s"
echo "[lag-controller] Logs: $LOG_DIR"

while true; do
  # Update heartbeat
  date > "$HEARTBEAT_FILE"

  # One controller iteration
  python /code/lag_controller.py \
    --config "$CONFIG_PATH" \
    >> "${LOG_DIR}/lag_controller.log" 2>&1 || {
      echo "[lag-controller] Controller iteration failed (exit $?), will retry after sleep..." >&2
    }

  sleep "$INTERVAL"
done

