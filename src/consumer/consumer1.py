# consumer.py
import os
import time
import socket
import threading
import signal
import sys
from collections import deque

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
    """
    Parses common K8s ConfigMap boolean strings: "true"/"false", "1"/"0", "yes"/"no".
    """
    val = os.getenv(name)
    if val is None:
        return default
    s = str(val).strip().lower()
    if s in ("true", "1", "yes", "y", "on"):
        return True
    if s in ("false", "0", "no", "n", "off"):
        return False
    return default


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
consumer_lag_skew_ratio = Gauge("consumer_lag_skew_ratio", "Max lag / mean lag over assigned partitions", COMMON_LABELS)


class MetricConsumer:
    """
    Metrics-only consumer:
    - Prometheus only (no files)
    - e2e latency from producer_timestamp header
    - SLO violation rate over sliding window
    - lag per partition + summaries

    Topic naming (your current design):
      - ConfigMap provides TOPIC_TITLE as the run-specific topic prefix,
        e.g., TOPIC_TITLE="B0-R500-20260216-204006"
      - Topics are derived as: TOPIC_TITLE_0 .. TOPIC_TITLE_{TOPIC_COUNT-1}
      - RUN_ID is used for metrics labeling (optional), not for topic naming.
    """

    def __init__(self):
        self.pod_name = socket.gethostname()

        self.exp_id = getenv_str("EXP_ID", "B0")
        self.traffic_mode = getenv_str("TRAFFIC_MODE", "balanced").strip().lower()

        tr_env = os.getenv("TARGET_RATE") or os.getenv("STEADY_RATE_MSGS") or "500"
        self.target_rate = float(tr_env)

        # Run id is useful for labeling/diagnostics; not required for topic naming.
        self.run_id = getenv_str("RUN_ID", "").strip()

        self.group_id = getenv_str("CONSUMER_GROUP_ID", getenv_str("GROUP_ID", "consumer-group"))
        self.client_id = getenv_str("CONSUMER_CLIENT_ID", self.pod_name)

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

        # ConfigMap uses "true"/"false"
        self.enable_auto_commit = getenv_bool("ENABLE_AUTO_COMMIT", False)
        auto_offset_reset = getenv_str("AUTO_OFFSET_RESET", "latest")

        self.poll_max_msg = getenv_int("POLL_MAX_MSG", 500)
        self.poll_timeout = getenv_float("POLL_TIMEOUT", 0.5)

        self.lag_query_interval = getenv_float("LAG_QUERY_INTERVAL", 10.0)
        self.lag_query_timeout = getenv_float("LAG_QUERY_TIMEOUT", 2.0)

        # SLO threshold: prefer SLO_THRESHOLD_MS, fallback to SLA
        self.slo_threshold_ms = getenv_float("SLO_THRESHOLD_MS", getenv_float("SLA", 99.0))
        self.slo_window_seconds = getenv_float("SLO_WINDOW_SECONDS", 60.0)

        self._window_events = deque()
        self._window_total = 0
        self._window_viol = 0

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

        self.consumer = Consumer(
            {
                "bootstrap.servers": bootstrap_servers,
                "group.id": self.group_id,
                "client.id": self.client_id,
                "enable.auto.commit": "true" if self.enable_auto_commit else "false",
                "auto.offset.reset": auto_offset_reset,
            }
        )
        self.consumer.subscribe(self.topics)

        print(f"[INFO] Consumer pod={self.pod_name}")
        print(f"[INFO] EXP_ID={self.exp_id} TARGET_RATE={self.target_rate} RUN_ID={self.run_id or 'unset'}")
        print(f"[INFO] TRAFFIC_MODE={self.traffic_mode}")
        print(f"[INFO] Group={self.group_id}, Client={self.client_id}")
        print(f"[INFO] TOPIC_TITLE(prefix)={self.topic_prefix} Topics={self.topics}")
        print(f"[INFO] SLO={self.slo_threshold_ms} ms, Window={self.slo_window_seconds} s")
        print(f"[INFO] ENABLE_AUTO_COMMIT={self.enable_auto_commit}")

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

    def update_consumer_lag(self):
        """
        If ENABLE_AUTO_COMMIT=false, committed offsets can lag behind;
        use position() in that case.
        """
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
            for tp in assignments:
                _, high = self.consumer.get_watermark_offsets(tp, timeout=self.lag_query_timeout)
                base = base_offsets.get(tp, KafkaError._NO_OFFSET)
                if base == KafkaError._NO_OFFSET or base < 0:
                    continue

                lag = max(0, high - base)
                lags.append(lag)
                consumer_lag.labels(**self.metric_labels, topic=tp.topic, partition=str(tp.partition)).set(float(lag))

            if lags:
                total = float(sum(lags))
                mx = float(max(lags))
                mean = total / float(len(lags)) if len(lags) > 0 else 0.0
                ratio = (mx / mean) if mean > 0 else 0.0
                consumer_total_lag.labels(**self.metric_labels).set(total)
                consumer_max_lag.labels(**self.metric_labels).set(mx)
                consumer_lag_skew_ratio.labels(**self.metric_labels).set(ratio)

        except Exception as e:
            if self.debug_enabled:
                print(f"[WARN] Failed to update consumer lag: {e}")

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

                    consumer_e2e_latency_seconds.labels(**self.metric_labels).observe(e2e_seconds)
                    consumer_messages_consumed_total.labels(**self.metric_labels).inc()
                    self.local_consumed += 1

                    value_size = len(msg.value()) if msg.value() else 0
                    consumer_bytes_consumed_total.labels(**self.metric_labels).inc(value_size)

                    is_violation = (e2e_seconds * 1000.0) > self.slo_threshold_ms
                    if is_violation:
                        consumer_slo_violations_total.labels(**self.metric_labels).inc()

                    self._window_add(recv_ts, is_violation)

                    if self.debug_enabled and self.debug_every_n > 0 and (self.local_consumed % self.debug_every_n) == 0:
                        print(f"[SAMPLE] topic={msg.topic()} part={msg.partition()} e2e_ms={e2e_seconds*1000.0:.2f}")

                # If auto-commit is off, commit async after each batch (cheap)
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

