#!/usr/bin/env python3
import argparse
import csv
import json
import math
import re
import sys
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean, pstdev


def parse_args():
    p = argparse.ArgumentParser(
        description="Export Grafana dashboard Prometheus panels to CSV without repeating experiment metadata."
    )
    p.add_argument("--dashboard-json", required=True)
    p.add_argument("--prom-url", default="http://localhost:9090")
    p.add_argument("--start", required=True, help="ISO time with timezone or epoch seconds")
    p.add_argument("--end", required=True, help="ISO time with timezone or epoch seconds")
    p.add_argument("--step", default="10s")

    # Experiment metadata saved once in *_metadata.csv
    p.add_argument("--target-rate", required=True, type=float)
    p.add_argument("--producer-count", required=True, type=int)
    p.add_argument("--load-type", required=True)
    p.add_argument("--skew-fraction", default="")
    p.add_argument("--skew-partition", default="")
    p.add_argument("--run-id", default="")
    p.add_argument("--exp-id", default="")

    # Grafana variables
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
        return f"{seconds // 86400}d"
    if seconds % 3600 == 0:
        return f"{seconds // 3600}h"
    if seconds % 60 == 0:
        return f"{seconds // 60}m"
    return f"{seconds}s"


def qrange(prom_url, query, start, end, step):
    url = prom_url.rstrip("/") + "/api/v1/query_range?" + urllib.parse.urlencode(
        {"query": query, "start": str(start), "end": str(end), "step": step}
    )
    with urllib.request.urlopen(url, timeout=120) as r:
        data = json.loads(r.read().decode())

    if data.get("status") != "success":
        raise RuntimeError(json.dumps(data, indent=2))

    return data["data"]["result"]


def extract_panels(dashboard):
    """
    Return one row per Grafana target query.
    Panels with no query are also returned with expr='' so panel_status can show them.
    """
    rows = []

    def walk(panels):
        for panel in panels or []:
            if panel.get("panels"):
                walk(panel["panels"])

            title = panel.get("title", "")
            panel_id = panel.get("id", "")
            panel_type = panel.get("type", "")

            targets = panel.get("targets", []) or []
            if not targets:
                rows.append({
                    "panel_id": panel_id,
                    "panel_title": title,
                    "panel_type": panel_type,
                    "ref_id": "",
                    "expr": "",
                })
                continue

            found_expr = False
            for target in targets:
                expr = (target.get("expr") or "").strip()
                ref_id = target.get("refId", "")
                if expr:
                    found_expr = True
                rows.append({
                    "panel_id": panel_id,
                    "panel_title": title,
                    "panel_type": panel_type,
                    "ref_id": ref_id,
                    "expr": expr,
                })

            if not found_expr:
                # Keep the panel visible in the status file even when targets exist but no PromQL exists.
                pass

    walk(dashboard.get("panels", []))
    return rows


def replace_vars(expr, rng, interval, group, client_id):
    return (
        expr
        .replace("$__range", rng).replace("${__range}", rng)
        .replace("$__interval", interval).replace("${__interval}", interval)
        .replace("$group", group).replace("${group}", group)
        .replace("$client_id", client_id).replace("${client_id}", client_id)
    )


def scope_query(expr, run_id):
    if not run_id:
        raise ValueError("--run-id is required for an experiment export")
    quoted = json.dumps(run_id)
    expr = expr.replace('"$run_id"', quoted).replace('"${run_id}"', quoted)
    string_spans = [(m.start(), m.end()) for m in re.finditer(r'"(?:\\.|[^"\\])*"', expr)]
    def scope(match):
        if any(start <= match.start() < end for start, end in string_spans):
            return match.group(0)

        name, labels = match.group(1), match.group(2)
        labels = (labels or "{}")[1:-1].strip()
        # Dashboard variables are replaced before adding the exact selector.
        if re.search(r'\brun_id\s*(=|!=|=~|!~)', labels):
            labels = re.sub(r'run_id\s*(?:=~|!~|!=|=)\s*"[^" ]*"', 'run_id=' + quoted, labels)
        else:
            labels = labels + ("," if labels else "") + 'run_id=' + quoted
        return name + "{" + labels + "}"
    return re.sub(r'\b((?:consumer|producer)_[A-Za-z0-9_]+)(\{[^}]*\})?', scope, expr)


def series_name(metric):
    # Preserve run, topic, group and incarnation even when pod names are reused.
    return json.dumps(metric, sort_keys=True)


def stat(vals):
    vals = [v for v in vals if math.isfinite(v)]
    if not vals:
        return {"sample_count": 0, "mean": "", "std": "", "min": "", "max": "", "last": ""}
    return {
        "sample_count": len(vals),
        "mean": mean(vals),
        "std": pstdev(vals) if len(vals) > 1 else 0.0,
        "min": min(vals),
        "max": max(vals),
        "last": vals[-1],
    }


def write_metadata(path, metadata):
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["key", "value"])
        for k, v in metadata.items():
            w.writerow([k, v])


def main():
    a = parse_args()
    start, end = to_epoch(a.start), to_epoch(a.end)
    if end <= start:
        raise SystemExit("ERROR: --end must be after --start")

    rng = range_string(end - start)

    with open(a.dashboard_json, "r", encoding="utf-8") as f:
        dash = json.load(f)

    panels = extract_panels(dash)
    if not panels:
        raise SystemExit("ERROR: no panels found in dashboard JSON.")

    metadata = {
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
        "duration_seconds": int(round(end - start)),
        "step": a.step,
        "dashboard_title": dash.get("title", ""),
        "dashboard_uid": dash.get("uid", ""),
    }

    out_prefix = Path(a.out_prefix)

    metadata_path = Path(f"{out_prefix}_metadata.csv")
    data_path = Path(f"{out_prefix}_all_panel_data.csv")
    summary_path = Path(f"{out_prefix}_summary.csv")
    status_path = Path(f"{out_prefix}_panel_status.csv")
    failed_path = Path(f"{out_prefix}_failed_queries.csv")

    write_metadata(metadata_path, metadata)

    data_fields = [
        "timestamp_iso_utc",
        "timestamp_epoch",
        "panel_id",
        "panel_title",
        "panel_type",
        "ref_id",
        "series",
        "value",
        "query",
    ]

    summary_fields = [
        "panel_id",
        "panel_title",
        "panel_type",
        "ref_id",
        "series",
        "sample_count",
        "mean",
        "std",
        "min",
        "max",
        "last",
        "query",
    ]

    status_fields = [
        "panel_id",
        "panel_title",
        "panel_type",
        "ref_id",
        "status",
        "series_count",
        "sample_count",
        "query",
        "error",
    ]

    failed = []

    with data_path.open("w", newline="", encoding="utf-8") as df, \
         summary_path.open("w", newline="", encoding="utf-8") as sf, \
         status_path.open("w", newline="", encoding="utf-8") as pf:

        data_writer = csv.DictWriter(df, fieldnames=data_fields)
        summary_writer = csv.DictWriter(sf, fieldnames=summary_fields)
        status_writer = csv.DictWriter(pf, fieldnames=status_fields)

        data_writer.writeheader()
        summary_writer.writeheader()
        status_writer.writeheader()

        for i, panel in enumerate(panels, 1):
            raw_expr = panel["expr"]

            if not raw_expr:
                status_writer.writerow({
                    "panel_id": panel["panel_id"],
                    "panel_title": panel["panel_title"],
                    "panel_type": panel["panel_type"],
                    "ref_id": panel["ref_id"],
                    "status": "no_promql_query",
                    "series_count": 0,
                    "sample_count": 0,
                    "query": "",
                    "error": "",
                })
                continue

            query = scope_query(replace_vars(raw_expr, rng, a.step, a.group, a.client_id), a.run_id)

            print(f"[{i}/{len(panels)}] {panel['panel_title']}")

            try:
                result = qrange(a.prom_url, query, start, end, a.step)
            except Exception as e:
                err = str(e)
                failed.append({**panel, "query": query, "error": err})
                status_writer.writerow({
                    "panel_id": panel["panel_id"],
                    "panel_title": panel["panel_title"],
                    "panel_type": panel["panel_type"],
                    "ref_id": panel["ref_id"],
                    "status": "query_failed",
                    "series_count": 0,
                    "sample_count": 0,
                    "query": query,
                    "error": err,
                })
                print(f"[WARN] failed: {panel['panel_title']}: {err}", file=sys.stderr)
                continue

            total_samples_for_panel = 0

            if not result:
                status_writer.writerow({
                    "panel_id": panel["panel_id"],
                    "panel_title": panel["panel_title"],
                    "panel_type": panel["panel_type"],
                    "ref_id": panel["ref_id"],
                    "status": "no_data",
                    "series_count": 0,
                    "sample_count": 0,
                    "query": query,
                    "error": "",
                })
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
                    total_samples_for_panel += 1

                    iso = datetime.fromtimestamp(float(epoch), tz=timezone.utc).isoformat()
                    data_writer.writerow({
                        "timestamp_iso_utc": iso,
                        "timestamp_epoch": float(epoch),
                        "panel_id": panel["panel_id"],
                        "panel_title": panel["panel_title"],
                        "panel_type": panel["panel_type"],
                        "ref_id": panel["ref_id"],
                        "series": s,
                        "value": v,
                        "query": query,
                    })

                summary_writer.writerow({
                    "panel_id": panel["panel_id"],
                    "panel_title": panel["panel_title"],
                    "panel_type": panel["panel_type"],
                    "ref_id": panel["ref_id"],
                    "series": s,
                    **stat(vals),
                    "query": query,
                })

            status_writer.writerow({
                "panel_id": panel["panel_id"],
                "panel_title": panel["panel_title"],
                "panel_type": panel["panel_type"],
                "ref_id": panel["ref_id"],
                "status": "ok",
                "series_count": len(result),
                "sample_count": total_samples_for_panel,
                "query": query,
                "error": "",
            })

    if failed:
        with failed_path.open("w", newline="", encoding="utf-8") as f:
            fields = ["panel_id", "panel_title", "panel_type", "ref_id", "expr", "query", "error"]
            w = csv.DictWriter(f, fieldnames=fields)
            w.writeheader()
            w.writerows(failed)
        print(f"[WARN] failed queries saved to {failed_path}")

    print(f"[OK] metadata CSV:     {metadata_path}")
    print(f"[OK] all panel data:   {data_path}")
    print(f"[OK] summary CSV:      {summary_path}")
    print(f"[OK] panel status CSV: {status_path}")


if __name__ == "__main__":
    main()
