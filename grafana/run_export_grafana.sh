#!/bin/bash

CONFIG_FILE="../src/pipeline-configmap.yaml"

TARGET_RATE=$(yq '.data.TARGET_RATE' "$CONFIG_FILE" | tr -d '"')
PRODUCER_COUNT=$(yq '.data.PRODUCER_POD_COUNT' "$CONFIG_FILE" | tr -d '"')
LOAD_TYPE=$(yq '.data.TRAFFIC_MODE' "$CONFIG_FILE" | tr -d '"')
SKEW_FRACTION=$(yq '.data.SKEW_FRACTION' "$CONFIG_FILE" | tr -d '"')
SKEW_PARTITION=$(yq '.data.HOT_PARTITIONS' "$CONFIG_FILE" | tr -d '"')
RUN_ID=$(yq '.data.RUN_ID' "$CONFIG_FILE" | tr -d '"')
EXP_ID=$(yq '.data.EXP_ID' "$CONFIG_FILE" | tr -d '"')
EXP_START=$(yq '.data.EXP_START_ISO' "$CONFIG_FILE" | tr -d '"')
EXP_END=$(yq '.data.EXP_END_ISO' "$CONFIG_FILE" | tr -d '"')



OUT_PREFIX="${EXP_ID}-${LOAD_TYPE}-R${TARGET_RATE}-${RUN_ID}"

python3 export_grafana_dashboard_to_csv.py \
  --dashboard-json Kafka Dashboard– Per-Consumer Metrics.json \
  --prom-url http://localhost:9090 \
  --start "${EXP_START}" \
  --end "${EXP_END}" \
  --step 10s \
  --target-rate "${TARGET_RATE}" \
  --producer-count "${PRODUCER_COUNT}" \
  --load-type "${LOAD_TYPE}" \
  --skew-fraction "${SKEW_FRACTION}" \
  --skew-partition "${SKEW_PARTITION}" \
  --run-id "${RUN_ID}" \
  --exp-id "${EXP_ID}" \
  --out-prefix "${OUT_PREFIX}"
