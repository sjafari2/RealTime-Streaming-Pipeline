# confluent_consumer.py
import json
import time
import os
import socket
import threading
import signal
import sys
import queue
import random
import csv
from datetime import datetime

import pandas as pd
import numpy as np
import psutil
from confluent_kafka import Consumer, KafkaError
from flask import Flask, Response
from prometheus_client import (
    Counter,
    Gauge,
    Histogram,
    Summary,
    generate_latest,
    CONTENT_TYPE_LATEST,
)

# ============================================================
#  Global consumer instance + Flask for Prometheus
# ============================================================

consumer_instance = None

app = Flask(__name__)


@app.route("/")
def hello():
    return "Kafka consumer is running"


@app.route("/metrics")
def metrics():
    return Response(generate_latest(), mimetype=CONTENT_TYPE_LATEST)


# ============================================================
#  Prometheus metrics (with labels)
# ============================================================

COMMON_LABELS = ["pod", "group", "client_id"]

e2e_latency_summary = Summary(
    "end_to_end_latency_ms",
    "End-to-end latency in ms (includes application processing delay)",
    COMMON_LABELS,
)

msg_consumed_counter = Counter(
    "consumer_messages_consumed_total",
    "Total messages consumed",
    COMMON_LABELS,
)

msg_rate_gauge = Gauge(
    "consumer_message_rate",
    "Message consumption rate (msg/sec)",
    COMMON_LABELS,
)

mb_rate_gauge = Gauge(
    "consumer_mb_rate",
    "Message throughput rate (MB/sec)",
    COMMON_LABELS,
)

cpu_gauge = Gauge(
    "consumer_cpu_percent",
    "CPU percent usage",
    COMMON_LABELS,
)

mem_gauge = Gauge(
    "consumer_memory_percent",
    "Memory percent usage",
    COMMON_LABELS,
)

uptime_gauge = Gauge(
    "consumer_uptime_seconds",
    "Consumer uptime in seconds",
    COMMON_LABELS,
)

consumer_latency_histogram = Histogram(
    "consumer_latency_seconds",
    "End-to-end latency (producer to consumer + application delay)",
    COMMON_LABELS,
    buckets=[0.001, 0.005, 0.01, 0.05, 0.1, 0.2, 0.5, 1, 2],
)

out_of_order_rate_gauge = Gauge(
    "consumer_out_of_order_rate",
    "Fraction of out-of-order messages (per batch)",
    COMMON_LABELS,
)

duplicate_rate_gauge = Gauge(
    "consumer_duplicate_rate",
    "Fraction of duplicate messages (per batch)",
    COMMON_LABELS,
)

target_rate_gauge = Gauge(
    "consumer_target_rate",
    "Target message rate (msgs/sec, from producer headers)",
    COMMON_LABELS,
)

consumer_lag_gauge = Gauge(
    "consumer_lag",
    "Consumer lag (latest offset - committed offset)",
    COMMON_LABELS + ["topic", "partition"],
)


# ============================================================
#  Small env helpers
# ============================================================

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


# ============================================================
#  Main Consumer class
# ============================================================

class MetricConsumer:
    """
    ENV-only Kafka consumer for your experiments.

    Reads all configuration from environment variables (ConfigMap):
      - TOPIC_TITLE, TOPIC_COUNT
      - BOOTSTRAP_SERVERS
      - CONSUMER_GROUP_ID, AUTO_OFFSET_RESET, ENABLE_AUTO_COMMIT
      - POLL_TIMEOUT, POLL_MAX_MSG, MAX_MESSAGES, etc.
      - APP_DELAY_MIN_MS, APP_DELAY_MAX_MS, APP_DELAY_MODE
      - LAG_QUERY_TIMEOUT, LAG_QUERY_INTERVAL
    """

    def __init__(self):
        # --- Basic identity / labels ---
        self.pod_name = socket.gethostname()
        self.group_id = getenv_str("CONSUMER_GROUP_ID", "consumer-group")
        self.client_id = self.pod_name
        self.metric_labels = {
            "pod": self.pod_name,
            "group": self.group_id,
            "client_id": self.client_id,
        }

        # --- Topics + bootstrap servers ---
        topic_title = getenv_str("TOPIC_TITLE", "ae")
        topic_count = getenv_int("TOPIC_COUNT", 1)
        if topic_count < 1:
            topic_count = 1

        # topics follow pattern <TITLE>_i, same as producer
        self.topics = [f"{topic_title}_{i}" for i in range(topic_count)]
        bootstrap_servers = getenv_str("BOOTSTRAP_SERVERS", "pip-kafka:9092")

        # --- Consumer tuning parameters ---
        self.poll_timeout = float(getenv_str("POLL_TIMEOUT", "10"))  # seconds
        self.poll_max_msg = getenv_int("POLL_MAX_MSG", 1000)
        self.max_messages = getenv_int("MAX_MESSAGES", 100)
        self.lag_query_timeout = getenv_float("LAG_QUERY_TIMEOUT", 5.0)
        self.lag_query_interval = getenv_int("LAG_QUERY_INTERVAL", 30)

        self.consumer_output_base = getenv_str(
            "CONSUMER_OUTPUT_DIR", "/app/consumer-merge-data/consumer-result"
        )
        # Create date-based subdir so runs are separated per day
        date_str = datetime.now().strftime("%Y-%m-%d")
        self.consumer_output_dir = os.path.join(self.consumer_output_base, date_str)

        enable_auto_commit_str = getenv_str("ENABLE_AUTO_COMMIT", "true").lower()
        enable_auto_commit = enable_auto_commit_str == "true"

        auto_offset_reset = getenv_str("AUTO_OFFSET_RESET", "earliest")

        session_timeout_ms = getenv_int("SESSION_TIMEOUT_MS", 20000)
        socket_timeout_ms = getenv_int("SOCKET_TIMEOUT_MS", 30000)
        heartbeat_interval_ms = getenv_int("HEARTBEAT_INTERVAL_MS", 1000)
        max_poll_interval_ms = getenv_int("MAX_POLL_INTERVAL_MS", 120000)
        fetch_max_bytes = getenv_int("FETCH_MAX_BYTES", 5242880)
        fetch_min_bytes = getenv_int("FETCH_MIN_BYTES", 1)
        fetch_max_wait_ms = getenv_int("FETCH_MAX_WAIT_MS", 1)
        socket_send_buffer_bytes = getenv_int("SOCKET_SEND_BUFFER_BYTES", 524288)
        socket_receive_buffer_bytes = getenv_int("SOCKET_RECEIVE_BUFFER_BYTES", 524288)
        max_partition_fetch_bytes = getenv_int("MAX_PARTITION_FETCH_BYTES", 4194304)
        partition_assignment_strategy = getenv_str(
            "PARTITION_ASSIGNMENT_STRATEGY", "cooperative-sticky"
        )

        # --- Application delay (seconds) ---
        app_delay_min_ms = getenv_float("APP_DELAY_MIN_MS", 1.0)
        app_delay_max_ms = getenv_float("APP_DELAY_MAX_MS", 10.0)
        self.app_delay_mode = getenv_str("APP_DELAY_MODE", "simulated").lower()

        self.app_delay_min_s = app_delay_min_ms / 1000.0
        self.app_delay_max_s = app_delay_max_ms / 1000.0

        if self.app_delay_mode not in ("simulated", "realistic"):
            raise ValueError("Invalid APP_DELAY_MODE, must be 'simulated' or 'realistic'")
        if (
            self.app_delay_min_s < 0
            or self.app_delay_max_s < 0
            or self.app_delay_min_s > self.app_delay_max_s
        ):
            raise ValueError("Invalid app delay range; ensure 0 <= min <= max")

        # --- Internal counters ---
        self.start_time = time.time()
        self.msg_consumed = 0
        self.bytes_consumed = 0
        self.last_log_time = time.time()
        self.last_lag_update = 0
        self.metrics_list = []

        # --- Async saver queue for non-blocking file writes ---
        self._sentinel = object()
        self.save_queue = queue.Queue(maxsize=4096)
        self.save_thread = threading.Thread(
            target=self.save_worker, daemon=True
        )
        self.save_thread.start()

        # --- Create Kafka Consumer ---
        self.consumer = Consumer(
            {
                "bootstrap.servers": bootstrap_servers,
                "group.id": self.group_id,
                "enable.auto.commit": "true" if enable_auto_commit else "false",
                "auto.offset.reset": auto_offset_reset,
                "socket.timeout.ms": int(socket_timeout_ms),
                "fetch.max.bytes": int(fetch_max_bytes),
                "fetch.min.bytes": int(fetch_min_bytes),
                "fetch.wait.max.ms": int(fetch_max_wait_ms),
                "max.poll.interval.ms": int(max_poll_interval_ms),
                "heartbeat.interval.ms": int(heartbeat_interval_ms),
                "session.timeout.ms": int(session_timeout_ms),
                "client.id": self.client_id,
                "socket.send.buffer.bytes": socket_send_buffer_bytes,
                "socket.receive.buffer.bytes": socket_receive_buffer_bytes,
                "max.partition.fetch.bytes": max_partition_fetch_bytes,
                "partition.assignment.strategy": partition_assignment_strategy,
            }
        )

        self.consumer.subscribe(self.topics)

        # Ensure output dir exists
        os.makedirs(self.consumer_output_dir, exist_ok=True)

        print(f"[INFO] Consumer pod={self.pod_name}")
        print(f"[INFO] Group={self.group_id}, Client={self.client_id}")
        print(f"[INFO] Topics={self.topics}")
        print(f"[INFO] Output dir={self.consumer_output_dir}")

    # --------------------------------------------------------
    #  Lag computation
    # --------------------------------------------------------

    def update_consumer_lag(self):
        try:
            assignments = self.consumer.assignment()
            if not assignments:
                return

            committed = self.consumer.committed(
                assignments, timeout=self.lag_query_timeout
            )
            for tp, committed_tp in zip(assignments, committed):
                low, high = self.consumer.get_watermark_offsets(
                    tp, timeout=self.lag_query_timeout
                )
                if committed_tp.offset != KafkaError._NO_OFFSET:
                    lag = max(0, high - committed_tp.offset)
                    consumer_lag_gauge.labels(
                        **self.metric_labels,
                        topic=tp.topic,
                        partition=str(tp.partition),
                    ).set(lag)
        except Exception as e:
            print(f"[WARN] Failed to update consumer lag: {e}")

    # --------------------------------------------------------
    #  Main consume loop
    # --------------------------------------------------------

    def consume(self):
        while True:
            try:
                msgs = self.consumer.consume(
                    num_messages=self.poll_max_msg, timeout=self.poll_timeout
                )
                now = time.time()
                elapsed = now - self.start_time

                # System + throughput metrics every ~10s
                if now - self.last_log_time >= 10:
                    cpu_gauge.labels(**self.metric_labels).set(
                        psutil.cpu_percent(interval=None)
                    )
                    mem_gauge.labels(**self.metric_labels).set(
                        psutil.virtual_memory().percent
                    )
                    uptime_gauge.labels(**self.metric_labels).set(elapsed)

                    if elapsed > 0:
                        msg_rate_gauge.labels(**self.metric_labels).set(
                            self.msg_consumed / elapsed
                        )
                        mb_rate_gauge.labels(**self.metric_labels).set(
                            (self.bytes_consumed / (1024 * 1024)) / elapsed
                        )
                    self.last_log_time = now

                # Lag less frequently
                if now - self.last_lag_update >= self.lag_query_interval:
                    self.update_consumer_lag()
                    self.last_lag_update = now

                for msg in msgs:
                    if msg is None or msg.error():
                        continue

                    headers = dict(msg.headers() or [])
                    index = headers.get("index")
                    producer_ts = headers.get("producer_timestamp")
                    size_bytes = headers.get("size_bytes")
                    target_rate = headers.get("target_rate")
                    exp_id = headers.get("exp_id")
                    traffic_mode = headers.get("traffic_mode")
                    producer_pod_name = headers.get("producer_pod_name")

                    if index is None or producer_ts is None:
                        continue

                    # Decode headers
                    try:
                        index_val = int(index.decode())
                    except Exception:
                        index_val = (
                            index.decode()
                            if isinstance(index, (bytes, bytearray))
                            else str(index)
                        )

                    producer_ts_val = float(
                        producer_ts.decode()
                        if isinstance(producer_ts, (bytes, bytearray))
                        else producer_ts
                    )
                    receive_ts = time.time()

                    network_latency_sec = receive_ts - producer_ts_val
                    app_delay = random.uniform(
                        self.app_delay_min_s, self.app_delay_max_s
                    )

                    if self.app_delay_mode == "realistic":
                        time.sleep(app_delay)

                    application_ts = receive_ts + app_delay
                    end_to_end_latency_sec = network_latency_sec + app_delay

                    # Prometheus recording (e2e)
                    e2e_latency_summary.labels(
                        **self.metric_labels
                    ).observe(end_to_end_latency_sec * 1000.0)
                    consumer_latency_histogram.labels(
                        **self.metric_labels
                    ).observe(end_to_end_latency_sec)

                    if target_rate:
                        try:
                            tr_val = float(
                                target_rate.decode()
                                if isinstance(target_rate, (bytes, bytearray))
                                else target_rate
                            )
                            target_rate_gauge.labels(
                                **self.metric_labels
                            ).set(tr_val)
                        except Exception:
                            pass

                    # Prepare record for async save
                    record = {
                        "index": index_val,
                        "producer_timestamp": producer_ts_val,
                        "consumer_receive_timestamp": receive_ts,
                        "network_latency_seconds": network_latency_sec,
                        "application_timestamp": application_ts,
                        "application_latency_seconds": app_delay,
                        "end_to_end_latency_seconds": end_to_end_latency_sec,
                        "size_bytes": int(size_bytes.decode())
                        if isinstance(size_bytes, (bytes, bytearray))
                        else (int(size_bytes) if size_bytes is not None else None)
                        if size_bytes
                        else None,
                        "target_rate": float(target_rate.decode())
                        if isinstance(target_rate, (bytes, bytearray))
                        else (float(target_rate) if target_rate is not None else None)
                        if target_rate
                        else None,
                        "topic": msg.topic(),
                        "partition": msg.partition(),
                        "offset": msg.offset(),
                        "exp_id": exp_id.decode()
                        if isinstance(exp_id, (bytes, bytearray))
                        else exp_id
                        if exp_id
                        else None,
                        "traffic_mode": traffic_mode.decode()
                        if isinstance(traffic_mode, (bytes, bytearray))
                        else traffic_mode
                        if traffic_mode
                        else None,
                        "producer_pod_name": producer_pod_name.decode()
                        if isinstance(producer_pod_name, (bytes, bytearray))
                        else producer_pod_name
                        if producer_pod_name
                        else None,
                    }

                    self.metrics_list.append(record)

                    # Counters / bytes
                    self.bytes_consumed += len(msg.value()) if msg.value() else 0
                    self.msg_consumed += 1
                    msg_consumed_counter.labels(**self.metric_labels).inc()

                    # Periodic async commit
                    if self.msg_consumed % 100 == 0:
                        try:
                            self.consumer.commit(asynchronous=True)
                        except Exception as e:
                            print(f"[WARN] Commit failed: {e}")

                    # Flush batch to saver
                    if len(self.metrics_list) >= self.max_messages:
                        batch = self.metrics_list
                        self.metrics_list = []
                        try:
                            self.save_queue.put(batch, timeout=0.2)
                        except queue.Full:
                            print("[WARN] Save queue full; retrying...")
                            self.save_queue.put(batch, timeout=1.0)

            except Exception as e:
                print(f"[ERROR] Consume loop error: {e}")
                time.sleep(0.5)

    # --------------------------------------------------------
    #  Async saver: parquet + correctness metrics
    # --------------------------------------------------------

    def save_worker(self):
        while True:
            batch = self.save_queue.get()
            if batch is self._sentinel:
                break
            try:
                self._save_parquet_to_disk(batch)
            except Exception as e:
                print(f"[ERROR] Save worker failed: {e}")

    def _save_parquet_to_disk(self, batch):
        df = pd.DataFrame(batch)
        if df.empty:
            return

        now = datetime.now()
        filename = f"file_{self.pod_name}_{now.strftime('%Y%m%d_%H%M%S_%f')}.parquet"
        tmp_path = os.path.join(self.consumer_output_dir, f".tmp_{filename}")
        final_path = os.path.join(self.consumer_output_dir, filename)

        # Compute out-of-order / duplicate flags based on index
        if "index" in df.columns:
            idx_series = pd.to_numeric(df["index"], errors="coerce")
            idx_array = idx_series.dropna().astype(int).to_numpy()
            out_of_order_flags = np.zeros(len(idx_array), dtype=bool)
            if len(idx_array) > 1:
                out_of_order_flags[1:] = idx_array[1:] < idx_array[:-1]

            df["is_out_of_order"] = False
            df.loc[idx_series.dropna().index, "is_out_of_order"] = out_of_order_flags
            df["is_duplicate"] = idx_series.duplicated()

            # Per-batch rates for Prometheus
            ooo_rate = float(df["is_out_of_order"].mean())
            dup_rate = float(df["is_duplicate"].mean())
            out_of_order_rate_gauge.labels(**self.metric_labels).set(ooo_rate)
            duplicate_rate_gauge.labels(**self.metric_labels).set(dup_rate)

        # Atomic write
        df.to_parquet(tmp_path, index=False)
        os.replace(tmp_path, final_path)
        print(f"[INFO] Saved parquet: {final_path} ({len(df)} records)")

    # --------------------------------------------------------
    #  Shutdown
    # --------------------------------------------------------

    def shutdown(self):
        # Flush remaining batch
        if self.metrics_list:
            try:
                self.save_queue.put(self.metrics_list, timeout=1.0)
                self.metrics_list = []
            except Exception:
                pass

        # Signal saver to stop and join thread
        try:
            self.save_queue.put(self._sentinel)
            self.save_thread.join(timeout=5)
        except Exception:
            pass

        # Final commit and close
        try:
            self.consumer.commit(asynchronous=False)
        except Exception as e:
            print(f"[WARN] Commit on shutdown failed: {e}")
        try:
            self.consumer.close()
        except Exception as e:
            print(f"[WARN] Consumer close failed: {e}")


# ============================================================
#  Metrics server / signal handling
# ============================================================

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

