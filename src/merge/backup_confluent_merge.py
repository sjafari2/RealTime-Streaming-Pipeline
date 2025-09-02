"""
# Each Prometheus Gauge below corresponds to a tracked metric:
# - merge_throughput_MBps: Measures throughput (MB/s) during each merge.
# - merge_late_arrival_rate: Fraction of late-arriving records in the batch.
# - merge_out_of_order_rate: Fraction of out-of-order records in the batch.
# - merge_duplicate_rate: Fraction of duplicate records in the batch.
# - merge_cpu_percent: CPU usage (%) during merge operations.
# - merge_memory_percent: Memory usage (%) during merge operations.
# - merge_uptime_seconds: Total uptime of the merger process in seconds.
# - merge_last_merge_timestamp: Unix timestamp of the last completed merge.
# - merge_last_batch_size: Number of rows merged in the last batch.

# In addition, the CSV file `merge_metrics.csv` stores:
# - merge_timestamp: UTC timestamp of the merge completion.
# - throughput_MBps: Throughput during merge in MB/s.
# - late_rate: Fraction of late-arriving rows.
# - out_of_order_rate: Fraction of out-of-order rows.
# - duplicate_rate: Fraction of duplicate rows.
# - batch_size_rows: Number of rows in the merged batch.
# - batch_size_MB: Size of merged files in MB.
# - uptime_seconds: Total merger uptime.
# - cpu_percent: CPU usage percentage during merge.
# - memory_percent: Memory usage percentage during merge.


HybridMerger (simplified):
- Keeps your original CSV merge behavior (unchanged).
- Adds Parquet block-maxima pipeline with only TWO metric choices for GEV:
    1) consumer-producer latency:
         consumer_receive_timestamp - producer_timestamp
    2) application-consumer latency:
         application_timestamp - consumer_receive_timestamp
       (or, if present, application_latency_seconds is used directly)

Block definition:
- Each Parquet file written by the consumer is treated as ONE block.
- We compute the maximum of the selected metric within that file and append to:
    <metrics_dir>/block_maxima.csv
- Periodically fit a GEV using SciPy (if available) on accumulated block maxima.
  Writes <metrics_dir>/gev_fit_results.csv and exposes Prometheus gauges.

SciPy note:
- Install SciPy on the merge pod to enable fitting:
    pip install scipy

Prometheus Gauges (existing + GEV):
- merge_throughput_MBps, merge_late_arrival_rate, merge_out_of_order_rate,
  merge_duplicate_rate, merge_cpu_percent, merge_memory_percent,
  merge_uptime_seconds, merge_last_merge_timestamp, merge_last_batch_size
- gev_shape_c (SciPy c), gev_xi (-c), gev_location_mu (loc), gev_scale_beta (scale),
  gev_ks_pvalue (KS goodness-of-fit), merge_block_maxima_count
"""

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

# GEV-related gauges
gev_shape_c_gauge = Gauge('gev_shape_c', "SciPy's genextreme shape parameter c")
gev_xi_gauge = Gauge('gev_xi', 'GEV shape xi (xi = -c in SciPy convention)')
gev_location_mu_gauge = Gauge('gev_location_mu', 'GEV location (mu)')
gev_scale_beta_gauge = Gauge('gev_scale_beta', 'GEV scale (beta)')
gev_ks_pvalue_gauge = Gauge('gev_ks_pvalue', 'KS test p-value for GEV fit')
block_maxima_count_gauge = Gauge('merge_block_maxima_count', 'Count of blocks in maxima dataset')


# ---------------- Config loader (unchanged) ----------------
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
    Modes:
      - CSV merge path: unchanged from your original script.
      - Parquet block maxima: each .parquet is a block; compute max(metric) and append to block_maxima.csv
    Two metric choices ONLY (select via --gevMetric):
      * "consumer_producer": consumer_receive_timestamp - producer_timestamp
      * "application_consumer": application_timestamp - consumer_receive_timestamp
                               (or uses application_latency_seconds directly if present)
    """
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
        gev_metric="consumer_producer",
        min_blocks_for_gev=30,
        fit_interval_sec=60
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
        self.gev_metric = gev_metric  # "consumer_producer" or "application_consumer"
        if self.gev_metric not in ("consumer_producer", "application_consumer"):
            raise ValueError("Invalid --gevMetric. Use 'consumer_producer' or 'application_consumer'.")

        self.min_blocks_for_gev = int(min_blocks_for_gev)
        self.fit_interval_sec = int(fit_interval_sec)
        self.last_fit_time = 0.0

        # Ensure dirs
        os.makedirs(self.watch_dir, exist_ok=True)
        os.makedirs(self.processed_dir, exist_ok=True)
        os.makedirs(self.merged_dir, exist_ok=True)
        os.makedirs(self.metrics_dir, exist_ok=True)

        # Where we persist block maxima
        self.block_maxima_csv = os.path.join(self.metrics_dir, "block_maxima.csv")

        # Background system metrics updater
        threading.Thread(target=self._update_metrics_periodically, daemon=True).start()

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

    # ---------- Parquet block-maxima ----------
    def _get_unprocessed_parquet_files(self):
        return sorted(
            os.path.join(root, f)
            for root, _, files in os.walk(self.watch_dir)
            for f in files
            if f.endswith('.parquet') and not f.startswith('.tmp_')
        )

    def _seen_block_ids(self):
        """Return set of block_ids already written to block_maxima.csv (to avoid double counting)."""
        if not os.path.exists(self.block_maxima_csv):
            return set()
        try:
            df = pd.read_csv(self.block_maxima_csv)
            return set(df['block_id'].astype(str)) if 'block_id' in df.columns else set()
        except Exception as e:
            print(f"[WARN] Could not read {self.block_maxima_csv}: {e}")
            return set()

    def _append_block_maxima(self, rows):
        if not rows:
            return
        df = pd.DataFrame(rows)
        cols = ["block_id", "file_name", "computed_at", "metric", "block_max_value", "rows_in_block"]
        for c in cols:
            if c not in df.columns:
                df[c] = np.nan
        df = df[cols]
        if not os.path.exists(self.block_maxima_csv):
            df.to_csv(self.block_maxima_csv, index=False)
        else:
            df.to_csv(self.block_maxima_csv, mode='a', header=False, index=False)

        # Update gauge
        try:
            all_df = pd.read_csv(self.block_maxima_csv)
            block_maxima_count_gauge.set(len(all_df))
        except Exception:
            pass

    def _compute_block_max_from_parquet(self, parquet_path: str) -> dict | None:
        """
        Compute block maximum for the selected metric from ONE parquet file.
        Metric choices:
          - "consumer_producer": consumer_receive_timestamp - producer_timestamp
          - "application_consumer": application_timestamp - consumer_receive_timestamp
                                    (or application_latency_seconds if available)
        """
        file_name = os.path.basename(parquet_path)
        block_id = os.path.splitext(file_name)[0]
        computed_at = time.time()

        try:
            # Read only the columns we need based on metric
            if self.gev_metric == "consumer_producer":
                cols = ["consumer_receive_timestamp", "producer_timestamp"]
            else:  # "application_consumer"
                cols = ["application_latency_seconds", "application_timestamp", "consumer_receive_timestamp"]

            df = pd.read_parquet(parquet_path, columns=cols)
        except Exception as e:
            print(f"[ERROR] Failed to read parquet {file_name}: {e}")
            return None

        # Normalize numeric
        for col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

        # Build the per-row series for the chosen metric
        if self.gev_metric == "consumer_producer":
            # network / transport latency (seconds)
            missing = {"consumer_receive_timestamp", "producer_timestamp"} - set(df.columns)
            if missing:
                print(f"[WARN] {file_name} missing columns {missing}; skipping.")
                return None
            series = (df["consumer_receive_timestamp"] - df["producer_timestamp"]).dropna()

        else:  # "application_consumer"
            # Prefer the direct column if present (works for simulated or realistic modes)
            if "application_latency_seconds" in df.columns:
                series = pd.to_numeric(df["application_latency_seconds"], errors='coerce').dropna()
            else:
                missing = {"application_timestamp", "consumer_receive_timestamp"} - set(df.columns)
                if missing:
                    print(f"[WARN] {file_name} missing columns {missing}; skipping.")
                    return None
                series = (df["application_timestamp"] - df["consumer_receive_timestamp"]).dropna()

        if series.empty:
            print(f"[WARN] Empty series for metric {self.gev_metric} in {file_name}")
            return None

        block_max = float(series.max())
        rows_in_block = int(series.shape[0])

        return {
            "block_id": block_id,
            "file_name": file_name,
            "computed_at": computed_at,
            "metric": self.gev_metric,
            "block_max_value": block_max,
            "rows_in_block": rows_in_block
        }

    def _process_parquet_blocks(self, parquet_files: list) -> int:
        """
        Treat each Parquet file as one block.
        For each block, compute max(metric) and append to block_maxima.csv.
        Delete processed parquet files to keep the watch dir clean.
        """
        if not parquet_files:
            return 0

        seen = self._seen_block_ids()
        new_rows, processed = [], 0

        for f in parquet_files:
            block_id = os.path.splitext(os.path.basename(f))[0]
            if block_id in seen:
                # already accounted for
                continue
            row = self._compute_block_max_from_parquet(f)
            if row is not None:
                new_rows.append(row)
                processed += 1

        if new_rows:
            self._append_block_maxima(new_rows)
            # remove only those successfully processed
            good_ids = {r["block_id"] for r in new_rows}
            for f in parquet_files:
                bid = os.path.splitext(os.path.basename(f))[0]
                if bid in good_ids:
                    self._move_to_processed(f)
                    #self._remove_file(f)
                    # optional:
                    # self._cleanup_empty_parents(f)

        return processed

    # ---------- Fit GEV on accumulated maxima ----------
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

        if "metric" not in df.columns or "block_max_value" not in df.columns:
            return

        # Use only rows matching the currently selected metric
        data = df.loc[df["metric"] == self.gev_metric, "block_max_value"].dropna().astype(float).values
        n = len(data)
        block_maxima_count_gauge.set(n)
        if n < self.min_blocks_for_gev:
            return

        try:
            from scipy.stats import genextreme, kstest
        except Exception:
            print("[WARN] SciPy not available. Install with `pip install scipy` to enable GEV fitting.")
            return

        try:
            # Fit SciPy GEV: returns (c, loc, scale). Classical xi = -c
            c, loc, scale = genextreme.fit(data)
            D, p = kstest(data, 'genextreme', args=(c, loc, scale))

            # Save a row with the fit
            out_csv = os.path.join(self.metrics_dir, "gev_fit_results.csv")
            row = pd.DataFrame([{
                "timestamp": datetime.utcfromtimestamp(now).strftime('%Y-%m-%dT%H%M%S'),
                "metric": self.gev_metric,
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
            gev_shape_c_gauge.set(c)
            gev_xi_gauge.set(-c)
            gev_location_mu_gauge.set(loc)
            gev_scale_beta_gauge.set(scale)
            gev_ks_pvalue_gauge.set(p)

            self.last_fit_time = now
            print(f"[INFO] GEV fit ({self.gev_metric}) on {n} blocks: c={c:.4f}, mu={loc:.4f}, beta={scale:.4f}, KS p={p:.4f}")
        except Exception as e:
            print(f"[ERROR] GEV fit failed: {e}")

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

                # 2) Parquet block-maxima (two-metric simplified path)
                if self.enable_parquet:
                    parquet_files = self._get_unprocessed_parquet_files()
                    processed = self._process_parquet_blocks(parquet_files)
                    if processed > 0:
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
        relative subfolder structure (e.g., date/time folders). If a file with
        the same name already exists at the destination, append a microsecond
        timestamp to avoid overwriting.
        """
        # Build destination path that mirrors watch_dir structure
        rel = os.path.relpath(src_path, self.watch_dir)
        dest = os.path.join(self.processed_dir, rel)

        # Ensure destination directory exists
        os.makedirs(os.path.dirname(dest), exist_ok=True)

        # Avoid overwrite if a file with same name already exists
        if os.path.exists(dest):
            base, ext = os.path.splitext(dest)
            dest = f"{base}_{int(time.time()*1e6)}{ext}"

        shutil.move(src_path, dest)
        return dest

    def _cleanup_empty_parents(self, path: str):
        """
        Attempt to remove empty parent dirs of `path` up to watch_dir.
        Stops at the first non-empty dir.
        """
        d = os.path.dirname(path)
        while d and os.path.commonpath([d, self.watch_dir]) == self.watch_dir:
            try:
                os.rmdir(d)  # only succeeds if empty
            except OSError:
                break
            d = os.path.dirname(d)

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
    parser.add_argument("--processedDir", required=True, help="(Kept for compatibility) Unused; files are deleted after processing")
    parser.add_argument("--mergedDir", required=True, help="Where merged CSV outputs go (CSV path only)")
    parser.add_argument("--metricsDir", required=True, help="Where block_maxima.csv and gev_fit_results.csv are stored")
    parser.add_argument("--minFiles", type=int, default=4, help="Min CSV files to trigger a merge")
    parser.add_argument("--intervalSec", type=int, default=10, help="Max seconds between CSV merges")

    # ---- Simplified Parquet + GEV controls ----
    parser.add_argument("--enableParquet", type=str, default="true",
                        help="Enable Parquet block-maxima processing (default: true)")
    parser.add_argument(
        "--gevMetric",
        type=str,
        default="consumer_producer",
        help=(
            "Type of latency to use for block maxima (each parquet file = one block):\n"
            "  - 'consumer_producer'   : consumer_receive_timestamp - producer_timestamp (transport latency)\n"
            "  - 'application_consumer': application_timestamp - consumer_receive_timestamp (pure app time).\n"
            "    If 'application_latency_seconds' exists, it is used directly.\n"
        ),
    )
    parser.add_argument("--minBlocksForGEV", type=int, default=30,
                        help="Minimum number of blocks required to fit GEV (default: 30)")
    parser.add_argument("--fitGEVIntervalSec", type=int, default=60,
                        help="Minimum seconds between GEV fits (default: 60)")

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
        gev_metric=args.gevMetric,
        min_blocks_for_gev=args.minBlocksForGEV,
        fit_interval_sec=args.fitGEVIntervalSec
    )
    threading.Thread(target=start_metrics_server, daemon=True).start()
    merger.run()

