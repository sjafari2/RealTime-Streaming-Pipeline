RUN_ID="run-20260622-002804"
mkdir -p results_$RUN_ID

START="2026-06-22T00:30:00Z"
END="2026-06-22T00:40:00Z"
STEP="5s"

mkdir -p results_$RUN

curl -G "http://localhost:9090/api/v1/query_range" \
  --data-urlencode "query=sum(rate(producer_messages_sent_total{exp_id=\"$RUN\"}[30s]))" \
  --data-urlencode "start=$START" \
  --data-urlencode "end=$END" \
  --data-urlencode "step=$STEP" \
  -o results_$RUN/throughput.json

curl -G "http://localhost:9090/api/v1/query_range" \
  --data-urlencode "query=1000 * sum(rate(consumer_e2e_latency_seconds_sum{exp_id=\"$RUN\"}[30s])) / sum(rate(consumer_e2e_latency_seconds_count{exp_id=\"$RUN\"}[30s]))" \
  --data-urlencode "start=$START" \
  --data-urlencode "end=$END" \
  --data-urlencode "step=$STEP" \
  -o results_$RUN/mean_latency_ms.json

curl -G "http://localhost:9090/api/v1/query_range" \
  --data-urlencode "query=sum(consumer_total_lag{exp_id=\"$RUN\"})" \
  --data-urlencode "start=$START" \
  --data-urlencode "end=$END" \
  --data-urlencode "step=$STEP" \
  -o results_$RUN/total_lag.json

curl -G "http://localhost:9090/api/v1/query_range" \
  --data-urlencode "query=max(consumer_max_lag{exp_id=\"$RUN\"})" \
  --data-urlencode "start=$START" \
  --data-urlencode "end=$END" \
  --data-urlencode "step=$STEP" \
  -o results_$RUN/max_lag.json

curl -G "http://localhost:9090/api/v1/query_range" \
  --data-urlencode "query=avg(consumer_lag_skew_ratio{exp_id=\"$RUN\"})" \
  --data-urlencode "start=$START" \
  --data-urlencode "end=$END" \
  --data-urlencode "step=$STEP" \
  -o results_$RUN/lag_skew.json
