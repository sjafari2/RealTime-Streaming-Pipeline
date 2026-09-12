#!/usr/bin/env python3
import argparse, csv, json, math, re, sys, urllib.parse, urllib.request
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean, pstdev

def parse_args():
    p = argparse.ArgumentParser(description="Export Grafana dashboard Prometheus panels to one CSV.")
    p.add_argument("--dashboard-json", required=True)
    p.add_argument("--prom-url", default="http://localhost:9090")
    p.add_argument("--start", required=True, help="ISO time with timezone or epoch seconds")
    p.add_argument("--end", required=True, help="ISO time with timezone or epoch seconds")
    p.add_argument("--step", default="10s")
    p.add_argument("--target-rate", required=True, type=float)
    p.add_argument("--producer-count", required=True, type=int)
    p.add_argument("--load-type", required=True)
    p.add_argument("--skew-fraction", default="")
    p.add_argument("--skew-partition", default="")
    p.add_argument("--run-id", default="")
    p.add_argument("--exp-id", default="")
    p.add_argument("--group", default=".*")
    p.add_argument("--client-id", default=".*")
    p.add_argument("--out-prefix", default="grafana_export")
    return p.parse_args()

def to_epoch(t):
    t = str(t).strip()
    if re.fullmatch(r"\d+(\.\d+)?", t):
        return float(t)
    if t.endswith("Z"):
        t = t[:-1] + "+00:00"
    dt = datetime.fromisoformat(t)
    if dt.tzinfo is None:
        raise ValueError("Use timezone in --start/--end, e.g. 2026-06-22T01:48:00-06:00")
    return dt.timestamp()

def range_string(seconds):
    seconds = int(round(seconds))
    if seconds % 86400 == 0:
        return f"{seconds//86400}d"
    if seconds % 3600 == 0:
        return f"{seconds//3600}h"
    if seconds % 60 == 0:
        return f"{seconds//60}m"
    return f"{seconds}s"

def qrange(prom_url, query, start, end, step):
    url = prom_url.rstrip("/") + "/api/v1/query_range?" + urllib.parse.urlencode(
        {"query": query, "start": str(start), "end": str(end), "step": step}
    )
    with urllib.request.urlopen(url, timeout=120) as r:
        data = json.loads(r.read().decode())
    if data.get("status") != "success":
        raise RuntimeError(data)
    return data["data"]["result"]

def extract_panels(dashboard):
    rows = []
    def walk(panels):
        for panel in panels or []:
            if panel.get("panels"):
                walk(panel["panels"])
            for target in panel.get("targets", []) or []:
                expr = (target.get("expr") or "").strip()
                if expr:
                    rows.append({
                        "panel_id": panel.get("id", ""),
                        "panel_title": panel.get("title", ""),
                        "ref_id": target.get("refId", ""),
                        "expr": expr
                    })
    walk(dashboard.get("panels", []))
    return rows

def replace_vars(expr, rng, interval, group, client_id):
    return (expr
        .replace("$__range", rng).replace("${__range}", rng)
        .replace("$__interval", interval).replace("${__interval}", interval)
        .replace("$group", group).replace("${group}", group)
        .replace("$client_id", client_id).replace("${client_id}", client_id)
    )

def series_name(metric):
    if not metric:
        return "value"
    if "pod" in metric and "partition" in metric:
        return f'{metric["pod"]}/partition={metric["partition"]}'
    if "pod" in metric:
        return metric["pod"]
    if "partition" in metric:
        return f'partition={metric["partition"]}'
    parts = [f"{k}={v}" for k, v in sorted(metric.items()) if k != "__name__"]
    return ",".join(parts) if parts else "value"

def stat(vals):
    vals = [v for v in vals if math.isfinite(v)]
    if not vals:
        return {"sample_count":0, "mean":"", "std":"", "min":"", "max":"", "last":""}
    return {"sample_count":len(vals), "mean":mean(vals),
            "std":pstdev(vals) if len(vals)>1 else 0.0,
            "min":min(vals), "max":max(vals), "last":vals[-1]}

def main():
    a = parse_args()
    start, end = to_epoch(a.start), to_epoch(a.end)
    if end <= start:
        raise SystemExit("ERROR: --end must be after --start")
    rng = range_string(end-start)

    with open(a.dashboard_json, "r", encoding="utf-8") as f:
        dash = json.load(f)

    panels = extract_panels(dash)
    if not panels:
        raise SystemExit("ERROR: no panel queries found.")

    meta = {
        "target_rate_per_producer": a.target_rate,
        "producer_count": a.producer_count,
        "aggregate_target_rate": a.target_rate * a.producer_count,
        "load_type": a.load_type,
        "skew_fraction": a.skew_fraction,
        "skew_partition": a.skew_partition,
        "run_id": a.run_id,
        "exp_id": a.exp_id,
        "start": a.start,
        "end": a.end,
        "step": a.step,
        "dashboard_title": dash.get("title",""),
        "dashboard_uid": dash.get("uid",""),
    }

    combined_path = Path(f"{a.out_prefix}_all_panel_data.csv")
    summary_path = Path(f"{a.out_prefix}_summary.csv")
    failed_path = Path(f"{a.out_prefix}_failed_queries.csv")

    combined_fields = ["timestamp_iso_utc","timestamp_epoch","panel_id","panel_title","ref_id","series","value",*meta.keys(),"query"]
    summary_fields = ["panel_id","panel_title","ref_id","series","sample_count","mean","std","min","max","last",*meta.keys(),"query"]

    failed = []
    with combined_path.open("w", newline="", encoding="utf-8") as cf, summary_path.open("w", newline="", encoding="utf-8") as sf:
        cw = csv.DictWriter(cf, fieldnames=combined_fields); cw.writeheader()
        sw = csv.DictWriter(sf, fieldnames=summary_fields); sw.writeheader()

        for i, p in enumerate(panels, 1):
            query = replace_vars(p["expr"], rng, a.step, a.group, a.client_id)
            print(f"[{i}/{len(panels)}] {p['panel_title']}")
            try:
                result = qrange(a.prom_url, query, start, end, a.step)
            except Exception as e:
                failed.append({**p, "query": query, "error": str(e)})
                print(f"[WARN] failed: {p['panel_title']}: {e}", file=sys.stderr)
                continue

            for ts in result:
                s = series_name(ts.get("metric", {}))
                vals = []
                for epoch, val in ts.get("values", []):
                    try:
                        v = float(val)
                    except Exception:
                        continue
                    if not math.isfinite(v):
                        continue
                    vals.append(v)
                    iso = datetime.fromtimestamp(float(epoch), tz=timezone.utc).isoformat()
                    cw.writerow({"timestamp_iso_utc":iso, "timestamp_epoch":float(epoch),
                                 "panel_id":p["panel_id"], "panel_title":p["panel_title"], "ref_id":p["ref_id"],
                                 "series":s, "value":v, **meta, "query":query})
                sw.writerow({"panel_id":p["panel_id"], "panel_title":p["panel_title"], "ref_id":p["ref_id"],
                             "series":s, **stat(vals), **meta, "query":query})

    if failed:
        with failed_path.open("w", newline="", encoding="utf-8") as f:
            fields = ["panel_id","panel_title","ref_id","expr","query","error"]
            w = csv.DictWriter(f, fieldnames=fields); w.writeheader(); w.writerows(failed)
        print(f"[WARN] failed queries saved to {failed_path}")

    print(f"[OK] combined CSV: {combined_path}")
    print(f"[OK] summary CSV: {summary_path}")

if __name__ == "__main__":
    main()
