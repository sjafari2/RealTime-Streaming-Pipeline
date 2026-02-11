#!/usr/bin/env bash

set -euo pipefail

EXP_ID="${1:-B0_500}"

python ./src/producer/render_configmap.py \
  --config ./src/experiments.yaml \
  --exp-id "$EXP_ID" \
  --name pipeline-configmap \
  --out ./src/"pipeline-configmap.yaml"



