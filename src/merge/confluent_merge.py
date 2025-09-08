import os
import time
import pandas as pd
from datetime import datetime
import numpy as np
import socket
import psutil
import threading
from flask import Flask
from prometheus_flask_exporter import PrometheusMetrics
from prometheus_client import Gauge
import signal
import sys
import yaml
import shutil

# ---------------- Flask + Prometheus wiring ----------------
app = Flask(__name__)
metrics_exporter = PrometheusMetrics(app, defaults_prefix=None)

# System / pipeline metrics
throughput_gauge = Gauge('merge_throughput_MBps', 'Throughput (MB/s) during merge')
late_rate_gauge = Gauge('merge_late_arrival_rate', 'Fraction of late-arriving records')
out_of_order_gauge = Gauge('merge_out_of_order_rate', 'Fraction of out-of-order records')
duplicate_gauge = Gauge('merge_duplicate_rate', 'Fraction of duplicate records')
cpu_gauge = Gauge('merge_cpu_percent', 'CPU usage during merge (%)')
mem_gauge = Gauge('merge_memory_percent', 'Memory usage during merge (%)')
uptime_gauge = Gauge('merge_uptime_seconds', 'Merger uptime in seconds')
last_merge_time_gauge = Gauge('merge_last_merge_timestamp', 'Unix timestamp of last merge')
last_batch_size_gauge = Gauge('merge_last_batch_size', 'Number of rows in last merge')

# GEV-related gauges (labeled by metric/scope/pod)
# metric ∈ {"producer_consumer", "consumer_application", "producer_application"}
# scope  ∈ {"global", "consumer_pod", "producer_pod"}
# pod    ∈ pod name or "" for global
gev_shape_c_gauge = Gauge('gev_shape_c', "SciPy genextreme shape parameter c", ['metric','scope','pod'])
gev_xi_gauge = Gauge('gev_xi', 'GEV shape xi (xi = -c)', ['metric','scope','pod'])
gev_location_mu_gauge = Gauge('gev_location_mu', 'GEV location (mu)', ['metric','scope','pod'])
gev_scale_beta_gauge = Gauge('gev_scale_beta', 'GEV scale (beta)', ['metric','scope','pod'])
gev_ks_pvalue_gauge = Gauge('gev_ks_pvalue', 'KS test p-value for GEV fit', ['metric','scope','pod'])
block_maxima_count_gauge = Gauge('merge_block_maxima_count', 'Count of maxima rows', ['metric','scope','pod'])

# ---------------- Config loader ----------------
def load_experiment_config(config_path="/config/pipeline-configmap.yaml"):
    try:
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
            data = config.get('data', {})
            return {
                "PRODUCER_COUNT": data.get("PRODUCER_COUNT", "NA"),
                "TARGET_RATE": data.get("TARGET_RATE", "NA"),
                "LINGER_MS": data.get("LINGER_MS", "NA"),
                "BATCH_SIZE": data.get("BATCH_SIZE", "NA"),
                "ACKS": data.get("ACKS", "NA"),
                "NUM_PARTITIONS": data.get("NUM_PARTITIONS", "NA"),
                "REPLICATION_FACTOR": data.get("REPLICATION_FACTOR", "NA"),
                "MIN_INSYNC_REPLICAS": data.get("MIN_INSYNC_REPLICAS", "NA"),
                "CONSUMER_COUNT": data.get("CONSUMER_COUNT", "NA"),
                "FETCH_MAX_WAIT_MS": data.get("FETCH_MAX_WAIT_MS", "NA"),
                "MESSAGE_SIZE_BYTES": data.get("MESSAGE_SIZE_BYTES", "NA")
            }
    except Exception as e:
        print(f"[ERROR] Loading config failed: {e}")
        return {}

# ---------------- Merge Pod ----------------
class HybridMerger:
    """
    CSV merge path: unchanged.
    Parquet block maxima: each .parquet is one block; compute per-file maxima for:
       - producer_consumer
       - consumer_application
       - producer_application
    Scope controlled by --blockScope: "global" or "per_pod".
    Append rows to block_maxima.csv, then fit GEV per (metric, scope[, pod]) if enough data.
    """
    METRICS = ("producer_consumer", "consumer_application", "producer_application")
    SCOPES = ("global", "consumer_pod", "producer_pod")

    def __init__(
        self,
        watch_dir,
        processed_dir,
        merged_dir,
        metrics_dir,
        min_files,
        interval_sec,
        experiment_config,
        enable_parquet=True,
        min_blocks_for_gev=30,
        fit_interval_sec=60,
        block_scope="global"  # "global" or "per_pod"
    ):
        self.watch_dir = watch_dir
        self.processed_dir = processed_dir
        self.merged_dir = merged_dir
        self.metrics_dir = metrics_dir
        self.min_files = int(min_files)
        self.interval = int(interval_sec)
        self.last_merge_time = time.time()
        self.previous_max_producer_timestamp = 0.0
        self.pod_name = socket.gethostname()
        self.start_time = time.time()
        self.running = True
        self.experiment_config = experiment_config

        # Parquet + GEV options
        self.enable_parquet = (str(enable_parquet).lower() == "true")
        self.min_blocks_for_gev = int(min_blocks_for_gev)
        self.fit_interval_sec = int(fit_interval_sec)
        self.last_fit_time = 0.0

        if block_scope not in ("global", "per_pod"):
            raise ValueError("--blockScope must be 'global' or 'per_pod'")
        self.block_scope = block_scope

        # Ensure dirs
        os.makedirs(self.watch_dir, exist_ok=True)
        os.makedirs(self.processed_dir, exist_ok=True)
        os.makedirs(self.merged_dir, exist_ok=True)
        os.makedirs(self.metrics_dir, exist_ok=True)

        # Where we persist block maxima
        self.block_maxima_csv = os.path.join(self.metrics_dir, "block_maxima.csv")

        # Background system metrics updater
        threading.Thread(target=self._update_metrics_periodically, daemon=True).start()

        # Bootstrap: emit block counts (zeros or from disk) at startup and keep refreshed
        self._emit_block_counts_from_disk()
        threading.Thread(target=self._refresh_counts_periodically, daemon=True).start()

    # ---------- periodic system metrics ----------
    def _update_metrics_periodically(self):
        while self.running:
            try:
                cpu_gauge.set(psutil.cpu_percent(interval=1))
                mem_gauge.set(psutil.virtual_memory().percent)
                uptime_gauge.set(time.time() - self.start_time)
            except Exception as e:
                print(f"[ERROR] Metrics update error: {e}")
            time.sleep(5)

    # ---------- CSV flow (unchanged) ----------
    def _get_unprocessed_csv_files(self):
        return sorted(
            os.path.join(root, f)
            for root, _, files in os.walk(self.watch_dir)
            for f in files
            if f.endswith('.csv') and not f.startswith('.tmp_')
        )

    @staticmethod
    def _remove_file(path):
        try:
            os.remove(path)
        except Exception as e:
            print(f"[WARN] Failed to remove {path}: {e}")

    @staticmethod
    def _compute_throughput(batch_mb, duration_s):
        return batch_mb / duration_s if duration_s > 0 else 0.0

    def _save_merge_metrics_row(self, throughput_MBps, late_rate, out_of_order_rate,
                                duplicate_rate, batch_size_rows, batch_size_MB, merge_timestamp):
        """Append one row to metrics_dir/merge_metrics.csv."""
        out_csv = os.path.join(self.metrics_dir, "merge_metrics.csv")
        dt = datetime.utcfromtimestamp(merge_timestamp)
        row = {
            "merge_timestamp": dt.strftime('%Y-%m-%dT%H%M%S'),
            "throughput_MBps": throughput_MBps,
            "late_rate": late_rate,
            "out_of_order_rate": out_of_order_rate,
            "duplicate_rate": duplicate_rate,
            "batch_size_rows": batch_size_rows,
            "batch_size_MB": batch_size_MB,
            "uptime_seconds": (time.time() - self.start_time),
            "cpu_percent": psutil.cpu_percent(),
            "memory_percent": psutil.virtual_memory().percent,
        }
        row.update(self.experiment_config)
        df = pd.DataFrame([row])
        if not os.path.exists(out_csv):
            df.to_csv(out_csv, index=False)
        else:
            df.to_csv(out_csv, mode='a', header=False, index=False)

    def _merge_csv_files(self, files):
        """Original CSV merge logic (kept)."""
        dfs, total_bytes = [], 0
        for f in files:
            try:
                total_bytes += os.path.getsize(f)
                df = pd.read_csv(f)
                if not {'index', 'producer_timestamp', 'consumer_receive_timestamp'}.issubset(df.columns):
                    continue
                df['producer_timestamp'] = pd.to_numeric(df['producer_timestamp'], errors='coerce')
                df['index'] = pd.to_numeric(df['index'], errors='coerce')
                df['consumer_receive_timestamp'] = pd.to_numeric(df['consumer_receive_timestamp'], errors='coerce')
                df['source_file'] = os.path.basename(f)
                dfs.append(df)
            except Exception as e:
                print(f"[ERROR] Failed to process {f}: {e}")

        if not dfs:
            return None, None

        merged_df = pd.concat(dfs, ignore_index=True)
        now = time.time()
        dt = datetime.utcfromtimestamp(now)
        duration = now - self.last_merge_time
        batch_size_MB = total_bytes / (1024 * 1024)
        throughput_MBps = self._compute_throughput(batch_size_MB, duration)

        # Late-arrival rate: producer_ts older than previous observed max
        merged_df['is_late'] = merged_df['producer_timestamp'] < self.previous_max_producer_timestamp
        late_rate = float(merged_df['is_late'].mean())

        # Out-of-order & duplicates by index
        idx_array = merged_df['index'].dropna().astype(int).to_numpy()
        out_of_order_flags = np.zeros(len(idx_array), dtype=bool)
        if len(idx_array) > 1:
            out_of_order_flags[1:] = idx_array[1:] < idx_array[:-1]
        merged_df['is_out_of_order'] = False
        merged_df.loc[merged_df['index'].dropna().index, 'is_out_of_order'] = out_of_order_flags
        out_of_order_rate = float(out_of_order_flags.mean())

        duplicate_flags = merged_df['index'].duplicated()
        merged_df['is_duplicate'] = duplicate_flags
        duplicate_rate = float(duplicate_flags.mean())

        merged_df['merge_timestamp'] = now
        merged_df['end_to_end_delay_ms'] = (merged_df['merge_timestamp'] - merged_df['producer_timestamp']) * 1000

        fname = f"merged_{dt.strftime('%Y%m%dT%H%M%S')}.csv"
        merged_path = os.path.join(self.merged_dir, fname)
        merged_df.to_csv(merged_path, index=False)

        # Gauges
        throughput_gauge.set(throughput_MBps)
        late_rate_gauge.set(late_rate)
        out_of_order_gauge.set(out_of_order_rate)
        duplicate_gauge.set(duplicate_rate)
        last_merge_time_gauge.set(now)
        last_batch_size_gauge.set(len(merged_df))

        self.previous_max_producer_timestamp = float(merged_df['producer_timestamp'].max())

        self._save_merge_metrics_row(
            throughput_MBps, late_rate, out_of_order_rate, duplicate_rate,
            len(merged_df), batch_size_MB, now
        )
        return merged_path, now

    # ---------- Parquet helpers (tolerant schema) ----------
    def _get_unprocessed_parquet_files(self):
        watch = os.path.abspath(self.watch_dir)
        processed = os.path.abspath(self.processed_dir)
        out = []
        for root, dirs, files in os.walk(watch, topdown=True):
            # prune processed subtree
            abs_root = os.path.abspath(root)
            # If processed is under watch, remove it from traversal
            dirs[:] = [d for d in dirs if os.path.abspath(os.path.join(root, d)) != processed]
            # Also skip if we're already inside processed (safety)
            if abs_root.startswith(processed):
                continue
            for f in files:
                if f.endswith(".parquet") and not f.startswith(".tmp_"):
                    out.append(os.path.join(root, f))
        return sorted(out)
    
    
    def _seen_keys(self):
        """Return set of ( metric, scope, pod) already written to block_maxima.csv to avoid duplicates."""
        if not os.path.exists(self.block_maxima_csv):
            return set()
        try:
            df = pd.read_csv(self.block_maxima_csv)
            needed = {'file_name', 'metric','scope','pod'}
            if not needed.issubset(df.columns):
                return set()
            return set(zip(df['file_name'].astype(str),
                           df['metric'].astype(str),
                           df['scope'].astype(str),
                           df['pod'].fillna('').astype(str)))
        except Exception as e:
            print(f"[WARN] Could not read {self.block_maxima_csv}: {e}")
            return set()

    def _append_block_maxima(self, rows):
        if not rows:
            return
        df = pd.DataFrame(rows)
        cols = [ "file_name", "computed_at", "metric", "scope", "pod", "block_max_value", "rows_in_block"]
        for c in cols:
            if c not in df.columns:
                df[c] = np.nan
        df = df[cols]
        if not os.path.exists(self.block_maxima_csv):
            df.to_csv(self.block_maxima_csv, index=False)
        else:
            df.to_csv(self.block_maxima_csv, mode='a', header=False, index=False)

        # Update gauges per (metric,scope,pod)
        try:
            all_df = pd.read_csv(self.block_maxima_csv)
            for m in self.METRICS:
                if not {'metric','scope','pod'}.issubset(all_df.columns):
                    break
                if self.block_scope == 'global':
                    sub = all_df[(all_df['metric']==m) & (all_df['scope']=='global')]
                    block_maxima_count_gauge.labels(m, 'global', '').set(len(sub))
                else:
                    subc = all_df[(all_df['metric']==m) & (all_df['scope']=='consumer_pod')]
                    for pod, g in subc.groupby(subc['pod'].fillna('')):
                        block_maxima_count_gauge.labels(m, 'consumer_pod', str(pod)).set(len(g))
                    subp = all_df[(all_df['metric']==m) & (all_df['scope']=='producer_pod')]
                    for pod, g in subp.groupby(subp['pod'].fillna('')):
                        block_maxima_count_gauge.labels(m, 'producer_pod', str(pod)).set(len(g))
        except Exception:
            pass

    def _load_parquet_columns(self, parquet_path, needed_cols):
        """
        Read only the columns that actually exist in the file.
        If pyarrow is available, inspect schema first; otherwise read-all then subset.
        Returns a pandas DataFrame (possibly missing some of needed_cols).
        """
        needed = set(needed_cols)
        # Try to inspect schema first (fast & safe)
        try:
            import pyarrow.parquet as pq
            schema = pq.read_schema(parquet_path)
            have = set(schema.names)
            cols_to_read = list(needed & have)
            if not cols_to_read:
                return pd.DataFrame()
            return pd.read_parquet(parquet_path, columns=cols_to_read)
        except Exception:
            # Fallback: read whole file, then subset
            try:
                df = pd.read_parquet(parquet_path)
                keep = list(needed & set(df.columns))
                return df[keep] if keep else pd.DataFrame()
            except Exception as e2:
                print(f"[ERROR] Failed to read parquet {os.path.basename(parquet_path)}: {e2}")
                return None

    # ---------- Build metric series ----------
    def _metric_series(self, frame: pd.DataFrame) -> dict:
        """Build metric series dict for the three latencies from the given DataFrame slice."""
        out = {}
        # producer_consumer
        if {"producer_timestamp","consumer_receive_timestamp"}.issubset(frame.columns):
            s = (frame["consumer_receive_timestamp"] - frame["producer_timestamp"]).dropna()
            if not s.empty: out["producer_consumer"] = s
        # consumer_application
        if "application_latency_seconds" in frame.columns:
            s = pd.to_numeric(frame["application_latency_seconds"], errors='coerce').dropna()
            if not s.empty: out["consumer_application"] = s
        elif {"application_timestamp","consumer_receive_timestamp"}.issubset(frame.columns):
            s = (frame["application_timestamp"] - frame["consumer_receive_timestamp"]).dropna()
            if not s.empty: out["consumer_application"] = s
        # producer_application (end-to-end)
        if "end_to_end_latency_seconds" in frame.columns:
            s = pd.to_numeric(frame["end_to_end_latency_seconds"], errors='coerce').dropna()
            if not s.empty: out["producer_application"] = s
        elif {"application_timestamp","producer_timestamp"}.issubset(frame.columns):
            s = (frame["application_timestamp"] - frame["producer_timestamp"]).dropna()
            if not s.empty: out["producer_application"] = s
        return out

    # ---------- SINGLE place to compute maxima rows for one parquet ----------
    def _compute_block_max_from_parquet(self, parquet_path: str) -> list | None:
        """
        Compute block maxima for one parquet file.

        Emits scope values that MATCH the rest of the code:
          - "global"            (single row per metric, pod="")
          - "consumer_pod"      (one row per consumer_pod)
          - "producer_pod"      (one row per producer_pod)
        """
        file_name = os.path.basename(parquet_path)
        #block_id = os.path.splitext(file_name)[0]
        computed_at = time.time()

        needed = {
            "producer_timestamp", "consumer_receive_timestamp",
            "application_timestamp", "application_latency_seconds",
            "end_to_end_latency_seconds",
            "producer_pod", "consumer_pod"
        }

        df = self._load_parquet_columns(parquet_path, needed_cols=needed)
        if df is None or df.empty:
            return None

        # numeric conversions
        for col in ["producer_timestamp","consumer_receive_timestamp","application_timestamp",
                    "application_latency_seconds","end_to_end_latency_seconds"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        rows_out = []

        # --- GLOBAL rows (across pods) ---
        series_map = self._metric_series(df)
        for metric_name, s in series_map.items():
            if not s.empty:
                rows_out.append({
                    #"block_id": block_id,
                    "file_name": file_name, "computed_at": computed_at,
                    "metric": metric_name, "scope": "global", "pod": "",
                    "block_max_value": float(s.max()), "rows_in_block": int(s.shape[0])
                })

        # --- Per-pod rows if requested ---
        if self.block_scope == "per_pod":
            if "consumer_pod" in df.columns and df["consumer_pod"].notna().any():
                for pod, sub in df.dropna(subset=["consumer_pod"]).groupby(df["consumer_pod"].astype(str)):
                    s_map = self._metric_series(sub)
                    for metric_name, s in s_map.items():
                        if not s.empty:
                            rows_out.append({
                                #"block_id": block_id,
                                "file_name": file_name, "computed_at": computed_at,
                                "metric": metric_name, "scope": "consumer_pod", "pod": str(pod),
                                "block_max_value": float(s.max()), "rows_in_block": int(s.shape[0])
                            })
            if "producer_pod" in df.columns and df["producer_pod"].notna().any():
                for pod, sub in df.dropna(subset=["producer_pod"]).groupby(df["producer_pod"].astype(str)):
                    s_map = self._metric_series(sub)
                    for metric_name, s in s_map.items():
                        if not s.empty:
                            rows_out.append({
                                #"block_id": block_id,
                                "file_name": file_name, "computed_at": computed_at,
                                "metric": metric_name, "scope": "producer_pod", "pod": str(pod),
                                "block_max_value": float(s.max()), "rows_in_block": int(s.shape[0])
                            })

        return rows_out or None

    def _process_parquet_blocks(self, parquet_files: list) -> int:
        """
        Treat each Parquet file as one block.
        Compute maxima rows per --blockScope and append them.
        Move processed files to processedDir (mirroring tree).
        """
        if not parquet_files:
            return 0

        seen = self._seen_keys()
        new_rows, processed_files = [], []

        for f in parquet_files:
            rows = self._compute_block_max_from_parquet(f)
            if not rows:
                continue
            # filter out seen keys ( metric, scope, pod)
            filt = []
            for r in rows:
                key = (r["file_name"], r["metric"], r["scope"], (r["pod"] if r["pod"] is not None else ""))
                if key not in seen:
                    filt.append(r)
            if filt:
                new_rows.extend(filt)
                processed_files.append(f)

        if new_rows:
            self._append_block_maxima(new_rows)
            for f in processed_files:
                self._move_to_processed(f)

        return len(processed_files)

    # ---------- Fit GEV per (metric, scope[, pod]) ----------
    def _fit_gev_if_needed(self):
        now = time.time()
        if (now - self.last_fit_time) < self.fit_interval_sec:
            return
        if not os.path.exists(self.block_maxima_csv):
            return

        try:
            df = pd.read_csv(self.block_maxima_csv)
        except Exception as e:
            print(f"[WARN] Could not read block_maxima.csv for GEV fit: {e}")
            return

        needed = {'metric','scope','pod','block_max_value'}
        if not needed.issubset(df.columns):
            return

        try:
            from scipy.stats import genextreme, kstest
        except Exception:
            print("[WARN] SciPy not available. Install with `pip install scipy` to enable GEV fitting.")
            return

        if self.block_scope == "global":
            groups = df[(df['scope']=='global')].groupby(['metric','scope'], dropna=False)
        else:
            groups = df[df['scope'].isin(['consumer_pod','producer_pod'])].groupby(['metric','scope','pod'], dropna=False)

        for key, g in groups:
            if self.block_scope == "global":
                metric, scope = key
                pod = ""
            else:
                metric, scope, pod = key
                pod = "" if pd.isna(pod) else str(pod)

            data = g['block_max_value'].dropna().astype(float).values
            n = len(data)
            if n < self.min_blocks_for_gev:
                block_maxima_count_gauge.labels(metric, scope, pod).set(n)
                continue

            try:
                c, loc, scale = genextreme.fit(data)
                D, p = kstest(data, 'genextreme', args=(c, loc, scale))

                # Save a row with the fit
                out_csv = os.path.join(self.metrics_dir, "gev_fit_results.csv")
                row = pd.DataFrame([{
                    "timestamp": datetime.utcfromtimestamp(now).strftime('%Y-%m-%dT%H%M%S'),
                    "metric": metric,
                    "scope": scope,
                    "pod": pod,
                    "n_blocks": n,
                    "shape_c": c,
                    "xi": -c,
                    "location_mu": loc,
                    "scale_beta": scale,
                    "ks_D": D,
                    "ks_pvalue": p
                }])
                if not os.path.exists(out_csv):
                    row.to_csv(out_csv, index=False)
                else:
                    row.to_csv(out_csv, mode='a', header=False, index=False)

                # Export to Prometheus
                block_maxima_count_gauge.labels(metric, scope, pod).set(n)
                gev_shape_c_gauge.labels(metric, scope, pod).set(c)
                gev_xi_gauge.labels(metric, scope, pod).set(-c)
                gev_location_mu_gauge.labels(metric, scope, pod).set(loc)
                gev_scale_beta_gauge.labels(metric, scope, pod).set(scale)
                gev_ks_pvalue_gauge.labels(metric, scope, pod).set(p)

                print(f"[INFO] GEV fit {metric} | {scope} | {pod or '-'} on {n} blocks: c={c:.4f}, mu={loc:.4f}, beta={scale:.4f}, KS p={p:.4f}")
            except Exception as e:
                print(f"[ERROR] GEV fit failed for {key}: {e}")

        self.last_fit_time = now

    # ---------- bootstrap / periodic refresh of block counts ----------
    def _emit_block_counts_from_disk(self):
        """Publish counts per (metric, scope, pod) even if no new files were processed yet."""
        try:
            if not os.path.exists(self.block_maxima_csv):
                # Emit zeros so panels can bind to series immediately
                if self.block_scope == "global":
                    for m in self.METRICS:
                        block_maxima_count_gauge.labels(m, "global", "").set(0)
                else:
                    for m in self.METRICS:
                        block_maxima_count_gauge.labels(m, "consumer_pod", "").set(0)
                        block_maxima_count_gauge.labels(m, "producer_pod", "").set(0)
                return

            df = pd.read_csv(self.block_maxima_csv)
            need = {'metric','scope','pod'}
            if not need.issubset(df.columns):
                return

            if self.block_scope == "global":
                for m in self.METRICS:
                    sub = df[(df['metric']==m) & (df['scope']=='global')]
                    block_maxima_count_gauge.labels(m, "global", "").set(len(sub))
            else:
                for m in self.METRICS:
                    subc = df[(df['metric']==m) & (df['scope']=='consumer_pod')]
                    if not subc.empty:
                        for pod, g in subc.groupby(subc['pod'].fillna('')):
                            block_maxima_count_gauge.labels(m, "consumer_pod", str(pod)).set(len(g))
                    else:
                        block_maxima_count_gauge.labels(m, "consumer_pod", "").set(0)
                    subp = df[(df['metric']==m) & (df['scope']=='producer_pod')]
                    if not subp.empty:
                        for pod, g in subp.groupby(subp['pod'].fillna('')):
                            block_maxima_count_gauge.labels(m, "producer_pod", str(pod)).set(len(g))
                    else:
                        block_maxima_count_gauge.labels(m, "producer_pod", "").set(0)
        except Exception as e:
            print(f"[WARN] Could not refresh block maxima counts: {e}")

    def _refresh_counts_periodically(self):
        while self.running:
            self._emit_block_counts_from_disk()
            time.sleep(15)

    # ---------- main loop ----------
    def run(self):
        while self.running:
            try:
                # 1) CSV path (unchanged)
                csv_files = self._get_unprocessed_csv_files()
                now = time.time()
                if csv_files and (len(csv_files) >= self.min_files or now - self.last_merge_time >= self.interval):
                    merged_path, merge_ts = self._merge_csv_files(csv_files)
                    if merged_path:
                        for f in csv_files:
                            self._remove_file(f)
                        self.last_merge_time = merge_ts

                # 2) Parquet block-maxima
                if self.enable_parquet:
                    parquet_files = self._get_unprocessed_parquet_files()
                    self._process_parquet_blocks(parquet_files)

                # Always try to fit (time-gated inside)
                self._fit_gev_if_needed()

                time.sleep(2)

            except KeyboardInterrupt:
                self.stop()
            except Exception as e:
                print(f"[ERROR] {e}")
                time.sleep(2)

    def stop(self):
        self.running = False

    def _move_to_processed(self, src_path: str) -> str:
        """
        Move a processed file from watch_dir to processed_dir, preserving its
        relative subfolder structure. Append a microsecond timestamp on collisions.
        """
        src_abs = os.path.abspath(src_path)
        watch_abs = os.path.abspath(self.watch_dir)
        proc_abs  = os.path.abspath(self.processed_dir)

        # If file is already inside processed_dir, don't move it again
        if src_abs.startswith(proc_abs + os.sep):
            return src_abs

        rel = os.path.relpath(src_abs, watch_abs)
        dest = os.path.join(proc_abs, rel)
        os.makedirs(os.path.dirname(dest), exist_ok=True)
        if os.path.exists(dest):
            base, ext = os.path.splitext(dest)
            dest = f"{base}_{int(time.time()*1e6)}{ext}"
        shutil.move(src_abs, dest)
        return dest

# ---------------- runner / wiring ----------------
def start_metrics_server():
    app.run(host='0.0.0.0', port=8000)

def graceful_shutdown(signal_num, frame):
    merger.stop()
    sys.exit(0)

if __name__ == "__main__":
    signal.signal(signal.SIGINT, graceful_shutdown)
    signal.signal(signal.SIGTERM, graceful_shutdown)

    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--watchDir", required=True, help="Directory where consumer drops CSV/Parquet batches")
    parser.add_argument("--processedDir", required=True, help="Where processed Parquet files are moved")
    parser.add_argument("--mergedDir", required=True, help="Where merged CSV outputs go (CSV path only)")
    parser.add_argument("--metricsDir", required=True, help="Where block_maxima.csv and gev_fit_results.csv are stored")
    parser.add_argument("--minFiles", type=int, default=4, help="Min CSV files to trigger a merge")
    parser.add_argument("--intervalSec", type=int, default=10, help="Max seconds between CSV merges")

    # Parquet + GEV controls
    parser.add_argument("--enableParquet", type=str, default="true",
                        help="Enable Parquet block-maxima processing (default: true)")
    parser.add_argument("--minBlocksForGEV", type=int, default=30,
                        help="Minimum number of blocks required to fit GEV (default: 30)")
    parser.add_argument("--fitGEVIntervalSec", type=int, default=60,
                        help="Minimum seconds between GEV fits (default: 60)")
    parser.add_argument("--blockScope", type=str, default="global", choices=["global","per_pod"],
                        help="Scope for block maxima rows: 'global' (across all pods) or 'per_pod' (consumer & producer pod rows)")

    args = parser.parse_args()
    experiment_config = load_experiment_config()

    merger = HybridMerger(
        watch_dir=args.watchDir,
        processed_dir=args.processedDir,
        merged_dir=args.mergedDir,
        metrics_dir=args.metricsDir,
        min_files=args.minFiles,
        interval_sec=args.intervalSec,
        experiment_config=experiment_config,
        enable_parquet=args.enableParquet,
        min_blocks_for_gev=args.minBlocksForGEV,
        fit_interval_sec=args.fitGEVIntervalSec,
        block_scope=args.blockScope
    )
    threading.Thread(target=start_metrics_server, daemon=True).start()
    merger.run()

