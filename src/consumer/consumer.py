# consumer.py
import os
import time
import socket
import threading
import signal
import sys
import csv
from collections import deque
from typing import Deque, Tuple, List, Optional
import random
import psutil
from confluent_kafka import Consumer, KafkaError
from flask import Flask, Response
from prometheus_client import Counter, Gauge, Histogram, generate_latest, CONTENT_TYPE_LATEST

consumer_instance = None
app = Flask(__name__)


@app.route("/")
def hello():
    return "Kafka consumer is running"


@app.route("/metrics")
def metrics():
    return Response(generate_latest(), mimetype=CONTENT_TYPE_LATEST)


def getenv_str(name: str, default: str) -> str:
    val = os.getenv(name)
    return val if val is not None else default


def getenv_int(name: str, default: int) -> int:
    val = os.getenv(name)
    try:
        return int(val) if val is not None else default
    except Exception:
        return default


def getenv_float(name: str, default: float) -> float:
    val = os.getenv(name)
    try:
        return float(val) if val is not None else default
    except Exception:
        return default


def getenv_bool(name: str, default: bool) -> bool:
    val = os.getenv(name)
    if val is None:
        return default
    s = str(val).strip().lower()
    if s in ("true", "1", "yes", "y", "on"):
        return True
    if s in ("false", "0", "no", "n", "off"):
        return False
    return default


def _percentile(sorted_vals: List[float], p: float) -> Optional[float]:
    """
    Compute percentile p in [0, 100] from a sorted list using linear interpolation.
    Returns None if list is empty.
    """
    if not sorted_vals:
        return None
    if p <= 0:
        return float(sorted_vals[0])
    if p >= 100:
        return float(sorted_vals[-1])

    n = len(sorted_vals)
    # rank in [0, n-1]
    r = (p / 100.0) * (n - 1)
    lo = int(r)
    hi = min(lo + 1, n - 1)
    frac = r - lo
    return float(sorted_vals[lo] * (1.0 - frac) + sorted_vals[hi] * frac)


COMMON_LABELS = ["pod", "group", "client_id", "exp_id", "run_id", "traffic_mode"]

consumer_cpu_percent = Gauge("consumer_cpu_percent", "CPU usage percent", COMMON_LABELS)
consumer_memory_percent = Gauge("consumer_memory_percent", "Memory usage percent", COMMON_LABELS)
consumer_uptime_seconds = Gauge("consumer_uptime_seconds", "Consumer uptime in seconds", COMMON_LABELS)
consumer_assigned_partitions = Gauge("consumer_assigned_partitions", "Assigned partitions", COMMON_LABELS)

consumer_messages_consumed_total = Counter("consumer_messages_consumed_total", "Total messages consumed", COMMON_LABELS)
consumer_bytes_consumed_total = Counter("consumer_bytes_consumed_total", "Total bytes consumed", COMMON_LABELS)

consumer_message_rate_window = Gauge("consumer_message_rate_window", "Approx msg/s over sliding window", COMMON_LABELS)

consumer_e2e_latency_seconds = Histogram(
    "consumer_e2e_latency_seconds",
    "End-to-end latency from producer send timestamp to consumer receive (seconds)",
    COMMON_LABELS,
    buckets=[0.001, 0.005, 0.01, 0.02, 0.05, 0.075, 0.099, 0.15, 0.2, 0.3, 0.5, 1, 2],
)

consumer_slo_violations_total = Counter(
    "consumer_slo_violations_total",
    "Total messages with e2e latency above SLO",
    COMMON_LABELS,
)

consumer_slo_violation_rate_window = Gauge(
    "consumer_slo_violation_rate_window",
    "Fraction of messages in the sliding window violating SLO",
    COMMON_LABELS,
)

consumer_lag = Gauge(
    "consumer_lag",
    "Consumer lag per assigned partition (end offset - position/committed)",
    COMMON_LABELS + ["topic", "partition"],
)

consumer_total_lag = Gauge("consumer_total_lag", "Sum of lag over assigned partitions", COMMON_LABELS)
consumer_max_lag = Gauge("consumer_max_lag", "Max lag over assigned partitions", COMMON_LABELS)
consumer_lag_skew_ratio = Gauge("consumer_lag_skew_ratio", "Max lag / total lag over all partitions", COMMON_LABELS)
consumer_hot_partition_lag = Gauge(
    "consumer_hot_partition_lag",
    "Lag of the configured hot partition",
    COMMON_LABELS,
)

consumer_hot_partition_fraction = Gauge(
    "consumer_hot_partition_fraction",
    "Configured hot partition lag divided by total lag",
    COMMON_LABELS,
)


class MetricConsumer:
    """
    Metrics-only consumer:
    - Prometheus metrics
    - e2e latency computed from producer_timestamp header
    - sliding-window SLO violation rate
    - consumer lag metrics
    - PLUS: append run metrics to a local CSV every 30 seconds (background thread)

    Topic naming:
      - TOPIC_TITLE is the run-specific topic prefix, e.g. "B0-R500-YYYYMMDD-HHMMSS"
      - topics: TOPIC_TITLE_0 .. TOPIC_TITLE_{TOPIC_COUNT-1}
    """

    def __init__(self):
        self.pod_name = socket.gethostname()

        self.exp_id = getenv_str("EXP_ID", "B0")
        self.traffic_mode = getenv_str("TRAFFIC_MODE", "balanced").strip().lower()

        tr_env = os.getenv("TARGET_RATE") or os.getenv("STEADY_RATE_MSGS") or "500"
        self.target_rate = float(tr_env)

        self.run_id = getenv_str("RUN_ID", "").strip()

        self.group_id = getenv_str("CONSUMER_GROUP_ID", getenv_str("GROUP_ID", "consumer-group"))

        # IMPORTANT: avoid accidentally using producer CLIENT_ID
        self.client_id = getenv_str("CONSUMER_CLIENT_ID", self.pod_name)
        
        self.hot_partition = getenv_int("HOT_PARTITION", -1)
        self.last_hot_partition_lag = 0.0
        self.last_hot_partition_fraction = 0.0

        self.metric_labels = {
            "pod": self.pod_name,
            "group": self.group_id,
            "client_id": self.client_id,
            "exp_id": self.exp_id,
            "run_id": (self.run_id if self.run_id else "unset"),
            "traffic_mode": self.traffic_mode,
        }

        bootstrap_servers = getenv_str("BOOTSTRAP_SERVERS", "localhost:9092")

        # ---- Topics derived ONLY from TOPIC_TITLE ----
        topic_title = getenv_str("TOPIC_TITLE", "").strip()
        if not topic_title:
            raise RuntimeError("TOPIC_TITLE is required (run-specific topic prefix). Check ConfigMap/write-back.")

        topic_count = max(1, getenv_int("TOPIC_COUNT", 1))
        self.topic_prefix = topic_title
        self.topics = [f"{self.topic_prefix}_{i}" for i in range(topic_count)]

        self.enable_auto_commit = getenv_bool("ENABLE_AUTO_COMMIT", False)
        auto_offset_reset = getenv_str("AUTO_OFFSET_RESET", "latest")

        self.poll_max_msg = getenv_int("POLL_MAX_MSG", 1000)
        self.poll_timeout = getenv_float("POLL_TIMEOUT", 5)

        self.lag_query_interval = getenv_float("LAG_QUERY_INTERVAL", 10.0)
        self.lag_query_timeout = getenv_float("LAG_QUERY_TIMEOUT", 2.0)

        # SLO threshold
        self.slo_threshold_ms = getenv_float("SLO_THRESHOLD_MS", getenv_float("SLA", 99.0))
        self.slo_window_seconds = getenv_float("SLO_WINDOW_SECONDS", 60.0)

        # CSV logging (every 30s) — kept separate from SLO window
        self.csv_log_interval_sec = getenv_float("CSV_LOG_INTERVAL_SEC", 30.0)
        self.csv_latency_window_sec = getenv_float("CSV_LATENCY_WINDOW_SEC", 30.0)

        # window for SLO violation rate (tracks just violation boolean)
        self._window_events: Deque[Tuple[float, bool]] = deque()
        self._window_total = 0
        self._window_viol = 0

        # window for percentiles written to CSV (stores latency values)
        self._lat_window: Deque[Tuple[float, float]] = deque()  # (recv_ts, e2e_ms)
        self._lock = threading.Lock()

        self.start_time = time.time()
        self.last_sys_update = 0.0
        self.last_lag_update = 0.0
        self.running = True

               
        # Debug / heartbeat controls
        self.debug_enabled = getenv_str("CONSUMER_DEBUG", "false").strip().lower() == "true"
        self.debug_every_n = getenv_int("CONSUMER_DEBUG_EVERY_N", 0)
        self.heartbeat_every_sec = getenv_float("CONSUMER_HEARTBEAT_SEC", 10.0)
        if self.heartbeat_every_sec < 1.0:
            self.heartbeat_every_sec = 10.0
        self.last_heartbeat = 0.0
        self.local_consumed = 0

        # Output directory for CSV
        self.consumer_output_dir = getenv_str("CONSUMER_OUTPUT_DIR", "").strip()
        if not self.consumer_output_dir:
            raise RuntimeError("CONSUMER_OUTPUT_DIR is required for CSV logging.")

        self.csv_dir = self.consumer_output_dir
        os.makedirs(self.csv_dir, exist_ok=True)

        run_tag = (self.run_id if self.run_id else "unset")
        self.csv_path = os.path.join(self.csv_dir, f"consumer_metrics_{run_tag}_{self.pod_name}.csv")
        self.partition_lag_csv_path = os.path.join(self.csv_dir,f"consumer_partition_lag_{run_tag}_{self.pod_name}.csv")
        self.partition_lag_enabled = getenv_bool("PARTITION_LAG_CSV_ENABLED", True)
        
        # last known lag summaries (for CSV)
        self.last_total_lag = 0.0
        self.last_max_lag = 0.0
        self.last_skew_ratio = 0.0
        self.last_hot_partition_lag = 0.0
        self.last_hot_partition_fraction = 0.0
        
        self.per_msg_enabled = getenv_str("PER_MSG_LAT_ENABLED", "false").lower() == "true"
        self.per_msg_sample_rate = getenv_float("PER_MSG_LAT_SAMPLE_RATE", 0.01)  # 1%
        self.per_msg_flush_every_sec = getenv_float("PER_MSG_LAT_FLUSH_SEC", 5.0)
        self.per_msg_max_buffer = getenv_int("PER_MSG_LAT_MAX_BUFFER", 20000)

        self.per_msg_path = os.path.join(
             self.csv_dir, f"per_message_latency_{run_tag}_{self.pod_name}.csv"
        )

        self._per_msg_buf = deque()          # holds rows to flush
        self._per_msg_last_flush = time.time()
        
        self._csv_thread = threading.Thread(target=self._csv_logger_loop, daemon=True)

        self.consumer = Consumer(
            {
                "bootstrap.servers": bootstrap_servers,
                "group.id": self.group_id,
                "client.id": self.client_id,
                "enable.auto.commit": "true" if self.enable_auto_commit else "false",
                "auto.offset.reset": auto_offset_reset,
            }
        )
        
        self._init_partition_lag_csv()

        self.consumer.subscribe(self.topics)

        print(f"[INFO] Consumer pod={self.pod_name}")
        print(f"[INFO] EXP_ID={self.exp_id} TARGET_RATE={self.target_rate} RUN_ID={self.run_id or 'unset'}")
        print(f"[INFO] TRAFFIC_MODE={self.traffic_mode}")
        print(f"[INFO] Group={self.group_id}, Client={self.client_id}")
        print(f"[INFO] TOPIC_TITLE(prefix)={self.topic_prefix} Topics={self.topics}")
        print(f"[INFO] SLO={self.slo_threshold_ms} ms, Window={self.slo_window_seconds} s")
        print(f"[INFO] ENABLE_AUTO_COMMIT={self.enable_auto_commit}")
        print(f"[INFO] CSV logging: every {self.csv_log_interval_sec}s -> {self.csv_path}")

        # Start CSV logger thread
        self._csv_thread.start()

    # --------------------- sliding window (SLO) ---------------------

    def _window_evict_old(self, now: float):
        cutoff = now - self.slo_window_seconds
        while self._window_events and self._window_events[0][0] < cutoff:
            _, was_violation = self._window_events.popleft()
            self._window_total -= 1
            if was_violation:
                self._window_viol -= 1

    def _window_add(self, now: float, is_violation: bool):
        self._window_events.append((now, is_violation))
        self._window_total += 1
        if is_violation:
            self._window_viol += 1

        self._window_evict_old(now)

        if self._window_total > 0:
            consumer_slo_violation_rate_window.labels(**self.metric_labels).set(self._window_viol / self._window_total)
            consumer_message_rate_window.labels(**self.metric_labels).set(self._window_total / self.slo_window_seconds)
        else:
            consumer_slo_violation_rate_window.labels(**self.metric_labels).set(0.0)
            consumer_message_rate_window.labels(**self.metric_labels).set(0.0)

    # --------------------- latency window (CSV percentiles) ---------------------

    def _lat_window_evict_old(self, now: float):
        cutoff = now - self.csv_latency_window_sec
        while self._lat_window and self._lat_window[0][0] < cutoff:
            self._lat_window.popleft()

    # --------------------- Initialize CSV file for per-partition lag snapshots ---------------------------------
    def _init_partition_lag_csv(self):
        if not self.partition_lag_enabled:
            return

        if not os.path.exists(self.partition_lag_csv_path):
            with open(self.partition_lag_csv_path, "w", newline="") as f:
                w = csv.writer(f)
                w.writerow([
                    "ts_epoch",
                    "pod",
                    "traffic_mode",
                    "topic",
                    "partition",
                    "current_offset",
                    "high_watermark",
                    "lag",
                ])

    # --------------------- lag update ---------------------

    def update_consumer_lag(self):
        try:
            assignments = self.consumer.assignment()
            if not assignments:
                return

            if self.enable_auto_commit:
                offsets = self.consumer.committed(assignments, timeout=self.lag_query_timeout)
                base_offsets = {tp: o.offset for tp, o in zip(assignments, offsets)}
            else:
                positions = self.consumer.position(assignments)
                base_offsets = {tp: p.offset for tp, p in zip(assignments, positions)}

            lags = []
            hot_lag = 0.0
            rows_to_append = []
            now = time.time()

            for tp in assignments:
                low, high = self.consumer.get_watermark_offsets(tp, timeout=self.lag_query_timeout)
                base = base_offsets.get(tp, KafkaError._NO_OFFSET)
                if base == KafkaError._NO_OFFSET or base < 0:
                    continue

                lag = max(0, high - base)  #lag = high_watermark - current_offset
                lags.append(lag)
                consumer_lag.labels(**self.metric_labels, topic=tp.topic, partition=str(tp.partition)).set(float(lag))
            
                if tp.partition == self.hot_partition:
                    hot_lag = float(lag)
            
                # Save raw per-partition lag for offline analysis
                rows_to_append.append([
                    f"{now:.6f}",
                    self.pod_name,
                    self.traffic_mode,
                    tp.topic,
                    tp.partition,
                    base,
                    high,
                    lag,
                ])

            if lags:
                total = float(sum(lags))
                mx = float(max(lags))
                skew_ratio = (mx / total) if total > 0 else 0.0
                hot_fraction = (hot_lag / total) if total > 0 else 0.0
                
                consumer_total_lag.labels(**self.metric_labels).set(total)
                consumer_max_lag.labels(**self.metric_labels).set(mx)
                consumer_lag_skew_ratio.labels(**self.metric_labels).set(skew_ratio)
                consumer_hot_partition_lag.labels(**self.metric_labels).set(hot_lag)
                consumer_hot_partition_fraction.labels(**self.metric_labels).set(hot_fraction)

                # save for CSV logging (no locks needed; floats are atomic enough in CPython)
                self.last_total_lag = total
                self.last_max_lag = mx
                self.last_skew_ratio = skew_ratio
                self.last_hot_partition_lag = hot_lag
                self.last_hot_partition_fraction = hot_fraction
            
            if self.partition_lag_enabled and rows_to_append:
                with open(self.partition_lag_csv_path, "a", newline="") as f:
                    w = csv.writer(f)
                    w.writerows(rows_to_append)

        except Exception as e:
            if self.debug_enabled:
                print(f"[WARN] Failed to update consumer lag: {e}")

    # --------------------- CSV logger thread ---------------------

    def _csv_logger_loop(self):
        """
        Background thread: every csv_log_interval_sec, append one row to CSV.
        This avoids any disk I/O in the consume loop.
        """
        # Ensure header exists once
        header = [
            #"ts_epoch",
            #"ts_iso",
            "pod",
            #"exp_id",
            #"run_id",
            "traffic_mode",
            #"group_id",
            #"client_id",
            #"topic_prefix",
            "topics",
            #"window_seconds",
            "window_total",
            "window_viol",
            #"slo_ms",
            #"violation_rate_window",
            "msg_rate_window",
            "lat_window_seconds",
            "lat_count_window",
            #"lat_p50_ms",
            "lat_p95_ms",
            "lat_p99_ms",
            "cpu_percent",
            "mem_percent",
            "uptime_sec",
            "total_lag",
            "max_lag",
            "lag_skew_ratio",
            "consumed_total",
            "hot_partition_lag",
            "hot_partition_fraction",

        ]

        file_exists = os.path.exists(self.csv_path)
        try:
            # line-buffered append
            with open(self.csv_path, "a", newline="") as f:
                w = csv.writer(f)
                if not file_exists:
                    w.writerow(header)
                    f.flush()

                while self.running:
                    time.sleep(self.csv_log_interval_sec)
                    now = time.time()

                    # Snapshot sliding SLO window values (no lock; simple ints)
                    wt = int(self._window_total)
                    wv = int(self._window_viol)
                    vr = (float(wv) / float(wt)) if wt > 0 else 0.0
                    mr = (float(wt) / float(self.slo_window_seconds)) if self.slo_window_seconds > 0 else 0.0

                    # Snapshot latency window (lock because deques mutate in consume thread)
                    with self._lock:
                        self._lat_window_evict_old(now)
                        lat_vals = [ms for (_t, ms) in self._lat_window]

                    lat_vals.sort()
                    p50 = _percentile(lat_vals, 50.0)
                    p95 = _percentile(lat_vals, 95.0)
                    p99 = _percentile(lat_vals, 99.0)

                    cpu = psutil.cpu_percent(interval=None)
                    mem = psutil.virtual_memory().percent
                    uptime = now - self.start_time

                    row = [
                        #f"{now:.6f}",
                        #time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(now)),
                        self.pod_name,
                        #self.exp_id,
                        #(self.run_id if self.run_id else "unset"),
                        self.traffic_mode,
                        #self.group_id,
                        #self.client_id,
                        #self.topic_prefix,
                        ";".join(self.topics),
                        #f"{self.slo_window_seconds:.3f}",
                        str(wt),
                        str(wv),
                        #f"{self.slo_threshold_ms:.3f}",
                        #f"{vr:.6f}",
                        f"{mr:.6f}",
                        f"{self.csv_latency_window_sec:.3f}",
                        str(len(lat_vals)),
                        #(f"{p50:.3f}" if p50 is not None else ""),
                        (f"{p95:.3f}" if p95 is not None else ""),
                        (f"{p99:.3f}" if p99 is not None else ""),
                        f"{cpu:.3f}",
                        f"{mem:.3f}",
                        f"{uptime:.3f}",
                        f"{self.last_total_lag:.3f}",
                        f"{self.last_max_lag:.3f}",
                        f"{self.last_skew_ratio:.6f}",
                        str(self.local_consumed),
                        f"{self.last_hot_partition_lag:.3f}",
                        f"{self.last_hot_partition_fraction:.6f}",


                    ]

                    w.writerow(row)

                    # ---- flush per-message latency samples (batched) ----
                    if getattr(self, "per_msg_enabled", False):
                        # flush every PER_MSG_LAT_FLUSH_SEC seconds (default 5s)
                        if (now - self._per_msg_last_flush) >= self.per_msg_flush_every_sec:
                            rows = []
                            while self._per_msg_buf:
                                rows.append(self._per_msg_buf.popleft())

                            if rows:
                                new_file = not os.path.exists(self.per_msg_path)
                                with open(self.per_msg_path, "a", newline="") as pf:
                                    pw = csv.writer(pf)
                                    if new_file:
                                        pw.writerow([
                                            "recv_ts", "producer_ts", "e2e_ms",
                                            "topic", "partition", "offset",
                                            "producer_pod", "index"
                                        ])
                                    pw.writerows(rows)

                            self._per_msg_last_flush = now

                    f.flush()

        except Exception as e:
            # If CSV logging fails, don't kill consumer; just report
            print(f"[WARN] CSV logger stopped due to error: {e}")

    # --------------------- consume loop ---------------------

    def consume(self):
        while self.running:
            try:
                msgs = self.consumer.consume(num_messages=self.poll_max_msg, timeout=self.poll_timeout)
                now = time.time()
                elapsed = now - self.start_time

                if now - self.last_sys_update >= 10.0:
                    consumer_cpu_percent.labels(**self.metric_labels).set(psutil.cpu_percent(interval=None))
                    consumer_memory_percent.labels(**self.metric_labels).set(psutil.virtual_memory().percent)
                    consumer_uptime_seconds.labels(**self.metric_labels).set(elapsed)
                    self.last_sys_update = now

                    self._window_evict_old(now)
                    if self._window_total > 0:
                        consumer_slo_violation_rate_window.labels(**self.metric_labels).set(self._window_viol / self._window_total)
                        consumer_message_rate_window.labels(**self.metric_labels).set(self._window_total / self.slo_window_seconds)
                    else:
                        consumer_slo_violation_rate_window.labels(**self.metric_labels).set(0.0)
                        consumer_message_rate_window.labels(**self.metric_labels).set(0.0)

                if self.debug_enabled and (now - self.last_heartbeat) >= self.heartbeat_every_sec:
                    try:
                        assigns = self.consumer.assignment()
                        consumer_assigned_partitions.labels(**self.metric_labels).set(float(len(assigns)))
                    except Exception:
                        pass
                    print(f"[HEARTBEAT] consumed={self.local_consumed} window_total={self._window_total} window_viol={self._window_viol}")
                    self.last_heartbeat = now

                if now - self.last_lag_update >= self.lag_query_interval:
                    self.update_consumer_lag()
                    self.last_lag_update = now

                for msg in msgs:
                    if msg is None:
                        continue
                    if msg.error():
                        continue

                    headers = dict(msg.headers() or [])
                    producer_ts = headers.get("producer_timestamp")
                    if producer_ts is None:
                        continue

                    try:
                        producer_ts_val = float(
                            producer_ts.decode() if isinstance(producer_ts, (bytes, bytearray)) else producer_ts
                        )
                    except Exception:
                        continue

                    recv_ts = time.time()
                    e2e_seconds = max(0.0, recv_ts - producer_ts_val)
                    e2e_ms = e2e_seconds * 1000.0
                    
                    # ------------------ Simulated application work ------------------
                    delay_ms = int(os.getenv("APP_DELAY_MS", "0"))
                    if delay_ms > 0:
                        time.sleep(delay_ms / 1000.0)
                    # ---------------------------------------------------------------

                    if self.per_msg_enabled and (random.random() < self.per_msg_sample_rate):
                        # optional headers you already send
                        prod_pod = headers.get("producer_pod_name")
                        idx_hdr = headers.get("index")

                        prod_pod_val = (
                            prod_pod.decode() if isinstance(prod_pod, (bytes, bytearray)) else (prod_pod or "")
                        )
                        idx_val = (
                            idx_hdr.decode() if isinstance(idx_hdr, (bytes, bytearray)) else (idx_hdr or "")
                        )

                        # Keep buffer bounded (drop oldest on overflow)
                        if len(self._per_msg_buf) >= self.per_msg_max_buffer:
                            self._per_msg_buf.popleft()

                        self._per_msg_buf.append([
                                recv_ts,                 # consumer receive time (epoch sec)
                                producer_ts_val,         # producer send time (epoch sec)
                                e2e_ms,                  # end-to-end ms
                                msg.topic(),
                                msg.partition(),
                                msg.offset(),
                                prod_pod_val,
                                idx_val,
                        ])


                    consumer_e2e_latency_seconds.labels(**self.metric_labels).observe(e2e_seconds)
                    consumer_messages_consumed_total.labels(**self.metric_labels).inc()
                    self.local_consumed += 1

                    value_size = len(msg.value()) if msg.value() else 0
                    consumer_bytes_consumed_total.labels(**self.metric_labels).inc(value_size)

                    is_violation = e2e_ms > self.slo_threshold_ms
                    if is_violation:
                        consumer_slo_violations_total.labels(**self.metric_labels).inc()

                    # Update SLO sliding window (cheap)
                    self._window_add(recv_ts, is_violation)

                    # Update latency window for CSV percentiles (cheap; compute percentiles in logger thread)
                    with self._lock:
                        self._lat_window.append((recv_ts, e2e_ms))
                        self._lat_window_evict_old(recv_ts)

                    if self.debug_enabled and self.debug_every_n > 0 and (self.local_consumed % self.debug_every_n) == 0:
                        print(f"[SAMPLE] topic={msg.topic()} part={msg.partition()} e2e_ms={e2e_ms:.2f}")

                if msgs and (not self.enable_auto_commit):
                    try:
                        self.consumer.commit(asynchronous=True)
                    except Exception:
                        pass

            except Exception as e:
                print(f"[ERROR] Consume loop error: {e}")
                time.sleep(0.5)

    def shutdown(self):
        self.running = False
        try:
            self.consumer.close()
        except Exception as e:
            print(f"[WARN] Consumer close failed: {e}")


def start_metrics_server():
    port = getenv_int("CONSUMER_HTTP_PORT", 8002)
    print(f"[INFO] Starting consumer metrics server on port {port}")
    app.run(host="0.0.0.0", port=port)


def graceful_shutdown(signal_num, frame):
    print("[INFO] Shutting down consumer gracefully...")
    if consumer_instance:
        consumer_instance.shutdown()
    sys.exit(0)


if __name__ == "__main__":
    signal.signal(signal.SIGINT, graceful_shutdown)
    signal.signal(signal.SIGTERM, graceful_shutdown)
    signal.signal(signal.SIGQUIT, graceful_shutdown)

    consumer_instance = MetricConsumer()
    threading.Thread(target=start_metrics_server, daemon=True).start()
    consumer_instance.consume()

