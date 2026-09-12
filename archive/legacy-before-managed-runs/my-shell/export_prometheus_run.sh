#!/usr/bin/env bash
set -euo pipefail

PROM_URL="http://localhost:9090"
CONFIG_FILE="./src/pipeline-configmap.yaml"

START="$1"
END="$2"
WINDOW="${4:-15m}"

RUN_ID=$(awk -F': ' '/RUN_ID:/ {gsub(/"/,"",$2); print $2; exit}' "$CONFIG_FILE")
TARGET_RATE=$(awk -F': ' '/TARGET_RATE:/ {gsub(/"/,"",$2); print $2; exit}' "$CONFIG_FILE")
TRAFFIC_MODE=$(awk -F': ' '/TRAFFIC_MODE:/ {gsub(/"/,"",$2); print $2; exit}' "$CONFIG_FILE")

OUT="${3:-experiment_summary_R${TARGET_RATE}_${RUN_ID}.csv}"

query() {
  local q="$1"
  curl -sG "${PROM_URL}/api/v1/query" \
    --data-urlencode "query=${q}" |
    python3 -c '
import sys, json
d=json.load(sys.stdin)
r=d.get("data", {}).get("result", [])
print(r[0]["value"][1] if r else "")
'
}

# Overall backlog
mean_system_lag=$(query "avg_over_time((sum(consumer_total_lag{run_id=\"$RUN_ID\"}))[${WINDOW}:])")
max_system_lag=$(query "max_over_time((sum(consumer_total_lag{run_id=\"$RUN_ID\"}))[${WINDOW}:])")

# Hot-partition / skew metrics
mean_max_partition_lag=$(query "avg_over_time((max(consumer_lag{run_id=\"$RUN_ID\"}))[${WINDOW}:])")
max_partition_lag=$(query "max_over_time((max(consumer_lag{run_id=\"$RUN_ID\"}))[${WINDOW}:])")
mean_partition_lag_std=$(query "avg_over_time((stddev(consumer_lag{run_id=\"$RUN_ID\"}))[${WINDOW}:])")
max_partition_lag_std=$(query "max_over_time((stddev(consumer_lag{run_id=\"$RUN_ID\"}))[${WINDOW}:])")
mean_lag_skew_ratio=$(query "avg_over_time((max(consumer_lag{run_id=\"$RUN_ID\"}) / avg(consumer_lag{run_id=\"$RUN_ID\"}))[${WINDOW}:])")

# Latency
p50_latency_ms=$(query "1000 * histogram_quantile(0.50, sum by (le) (rate(consumer_e2e_latency_seconds_bucket{run_id=\"$RUN_ID\"}[30s])))")
p95_latency_ms=$(query "1000 * histogram_quantile(0.95, sum by (le) (rate(consumer_e2e_latency_seconds_bucket{run_id=\"$RUN_ID\"}[30s])))")
p99_latency_ms=$(query "1000 * histogram_quantile(0.99, sum by (le) (rate(consumer_e2e_latency_seconds_bucket{run_id=\"$RUN_ID\"}[30s])))")

# Throughput and resources
throughput_msg_s=$(query "sum(rate(consumer_messages_consumed_total{run_id=\"$RUN_ID\"}[30s]))")
throughput_mb_s=$(query "sum(rate(consumer_bytes_consumed_total{run_id=\"$RUN_ID\"}[30s])) / 1024 / 1024")
consumer_cpu_mean=$(query "avg(consumer_cpu_percent{run_id=\"$RUN_ID\"})")
consumer_mem_mean=$(query "avg(consumer_memory_percent{run_id=\"$RUN_ID\"})")

if [ ! -f "$OUT" ]; then
  echo "run_id,target_rate,traffic_mode,start,end,mean_system_lag,max_system_lag,mean_max_partition_lag,max_partition_lag,mean_partition_lag_std,max_partition_lag_std,mean_lag_skew_ratio,p50_latency_ms,p95_latency_ms,p99_latency_ms,throughput_msg_s,throughput_mb_s,consumer_cpu_mean,consumer_mem_mean" > "$OUT"
fi

echo "$RUN_ID,$TARGET_RATE,$TRAFFIC_MODE,$START,$END,$mean_system_lag,$max_system_lag,$mean_max_partition_lag,$max_partition_lag,$mean_partition_lag_std,$max_partition_lag_std,$mean_lag_skew_ratio,$p50_latency_ms,$p95_latency_ms,$p99_latency_ms,$throughput_msg_s,$throughput_mb_s,$consumer_cpu_mean,$consumer_mem_mean" >> "$OUT"

echo "Saved to $OUT"
