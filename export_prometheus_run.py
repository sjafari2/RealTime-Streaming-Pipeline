#!/usr/bin/env python3
import sys, csv, requests, statistics
from datetime import datetime

PROM = "http://localhost:9090"

if len(sys.argv) < 6:
    print("Usage: export_prometheus_run.py RUN_ID TARGET_RATE START_ISO END_ISO OUT.csv")
    sys.exit(1)

run_id, target_rate, start_iso, end_iso, out = sys.argv[1:6]

def ts(x):
    return datetime.fromisoformat(x).timestamp()

start = ts(start_iso)
end = ts(end_iso)
step = "10s"

queries = {
    "lag": f'consumer_total_lag{{run_id="{run_id}"}}',
    "throughput": f'sum(rate(consumer_messages_consumed_total{{run_id="{run_id}"}}[30s]))',
    "p95_latency_ms": f'1000 * histogram_quantile(0.95, sum by (le) (rate(consumer_e2e_latency_seconds_bucket{{run_id="{run_id}"}}[30s])))',
    "cpu": f'avg(consumer_cpu_percent{{run_id="{run_id}"}})',
}

def query_range(q):
    r = requests.get(
        f"{PROM}/api/v1/query_range",
        params={"query": q, "start": start, "end": end, "step": step},
        timeout=20,
    )
    r.raise_for_status()
    data = r.json()["data"]["result"]
    vals = []
    for series in data:
        for _, v in series["values"]:
            try:
                vals.append(float(v))
            except:
                pass
    return vals

row = {
    "run_id": run_id,
    "target_rate": target_rate,
    "start": start_iso,
    "end": end_iso,
}

for name, q in queries.items():
    vals = query_range(q)
    row[f"{name}_mean"] = statistics.mean(vals) if vals else ""
    row[f"{name}_std"] = statistics.stdev(vals) if len(vals) > 1 else ""
    row[f"{name}_min"] = min(vals) if vals else ""
    row[f"{name}_max"] = max(vals) if vals else ""
    row[f"{name}_n"] = len(vals)

fields = list(row.keys())

write_header = False
try:
    open(out).close()
except FileNotFoundError:
    write_header = True

with open(out, "a", newline="") as f:
    w = csv.DictWriter(f, fieldnames=fields)
    if write_header:
        w.writeheader()
    w.writerow(row)

print("Saved:", out)
print(row)
