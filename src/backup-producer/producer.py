# confluent_kafka_producer.py
import json
import time
import random
import threading
import psutil
from flask import Flask, Response
from prometheus_client import (
    Counter,
    Gauge,
    Histogram,
    generate_latest,
    CONTENT_TYPE_LATEST,
)
from confluent_kafka import Producer, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic
import socket
import signal
import sys
import os
import itertools

# ============================================================
#  Globals / Web App for Prometheus Metrics
# ============================================================

shutdown_event = threading.Event()

app = Flask(__name__)


@app.route("/")
def hello():
    return "Kafka producer is running"


@app.route("/metrics")
def metrics():
    return Response(generate_latest(), mimetype=CONTENT_TYPE_LATEST)


# Prometheus metrics
msg_sent_counter = Counter("producer_messages_sent_total", "Total messages sent")
msg_rate_gauge = Gauge("producer_message_rate", "Message send rate (msg/sec)")
mb_rate_gauge = Gauge("producer_mb_rate", "Throughput (MB/sec)")
cpu_gauge = Gauge("producer_cpu_percent", "CPU percent usage")
mem_gauge = Gauge("producer_memory_percent", "Memory percent usage")
uptime_gauge = Gauge("producer_uptime_seconds", "Producer uptime in seconds")
delivery_latency_histogram = Histogram(
    "producer_delivery_latency_seconds",
    "Per-message delivery latency in seconds",
    buckets=[0.001, 0.005, 0.01, 0.05, 0.1, 0.2, 0.5, 1, 2],
)
target_rate_gauge = Gauge("producer_target_rate", "Target message send rate (msg/sec)")
effective_rate_gauge = Gauge(
    "producer_effective_rate", "Applied setpoint (msg/sec)"
)
observed_rate_gauge = Gauge(
    "producer_observed_rate", "Observed msg/s over runtime"
)
avg_msg_size_gauge = Gauge(
    "producer_avg_msg_size_bytes", "Average message size in bytes"
)

producer_instance = None


# ============================================================
#  Helper functions for env parsing
# ============================================================

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


def getenv_str(name: str, default: str) -> str:
    val = os.getenv(name)
    return val if val is not None else default


# ============================================================
#  Producer Class
# ============================================================

class MyProducer:
    """
    Synthetic Kafka producer for your thesis experiments.

    - Reads ALL configuration from environment variables (ConfigMap).
    - Supports:
        * EXP_ID        (e.g., B0, S0, S1, ...)
        * TRAFFIC_MODE  ("balanced", "skew", "burst" [future])
        * TARGET_RATE   (workload intensity in msgs/sec)
    - Emits:
        * Fixed 32KB messages
        * Headers: index, producer_timestamp, producer_pod_name,
                   size_bytes, target_rate, exp_id, traffic_mode
        * Prometheus metrics via /metrics (Flask)
    """

    def __init__(self):
        self.start_time = time.time()
        self.msg_sent = 0
        self.bytes_sent = 0
        self.pod_name = socket.gethostname()
        self.running = True

        # --------------------------------------------
        #  Core experiment configuration (env-only)
        # --------------------------------------------
        self.topic_title = getenv_str("TOPIC_TITLE", "ae")
        self.num_topics = getenv_int("TOPIC_COUNT", 1)
        self.num_partitions = getenv_int("NUM_PARTITIONS", 1)
        self.replication_factor = getenv_int("REPLICATION_FACTOR", 1)
        self.random_range = getenv_int("RANDOM_RANGE", 1000)

        # Workload intensity (msg/s)
        target_rate = os.getenv("TARGET_RATE")
        if target_rate is None:
            # optional backward-compat: STEADY_RATE_MSGS
            target_rate = os.getenv("STEADY_RATE_MSGS", "1000")
        self.original_target_rate = int(float(target_rate))
        self.effective_rate = self.original_target_rate
        self.dynamic_target_rate = self.effective_rate  # used for pacing

        # Experiment ID (B0, S0, S1, ... or auto)
        exp_id_env = os.getenv("EXP_ID")
        traffic_mode_env = os.getenv("TRAFFIC_MODE", "balanced").lower()

        # Skew parameters (only meaningful when traffic_mode == "skew")
        self.traffic_mode = traffic_mode_env  # "balanced" | "skew" | (future "burst")
        self.skew_partition = getenv_int("SKEW_PARTITION", 0)
        self.skew_fraction = max(
            0.0, min(1.0, getenv_float("SKEW_FRACTION", 0.7))
        )

        # If EXP_ID not explicitly given, build a descriptive one
        if exp_id_env:
            self.exp_id = exp_id_env
        else:
            run_id = (
                os.getenv("RUN_ID")
                or os.getenv("POD_NAME")
                or os.getenv("HOSTNAME")
                or "local"
            )
            if self.traffic_mode == "skew":
                skew_suffix = f"_p{self.skew_partition}_a{int(round(self.skew_fraction * 100))}"
            else:
                skew_suffix = ""
            self.exp_id = f"{run_id}__{self.traffic_mode}__{self.effective_rate}mps{skew_suffix}"

        # Make EXP_ID visible to any downstream tools (consumer, etc.)
        os.environ.setdefault("EXP_ID", self.exp_id)

        # Round-robin partition helpers
        hot = self.skew_partition % max(1, self.num_partitions)
        others = [p for p in range(self.num_partitions) if p != hot] or [hot]
        self._rr_all_parts = itertools.cycle(range(self.num_partitions or 1))
        self._rr_other = itertools.cycle(others)
        self._rng = random.Random(42)

        # Latency tracking (Kafka delivery)
        self.prev_latency = 0.1
        self.total_latency = 0.0
        self.latency_count = 0
        self.last_latency = 0.0

        # --------------------------------------------
        #  Kafka producer configuration from env
        # --------------------------------------------
        bootstrap_servers = getenv_str(
            "BOOTSTRAP_SERVERS",
            "pip-kafka-controller-headless.kafkastreamingdata.svc.cluster.local:9092",
        )

        self.min_in_sync = getenv_int("MIN_INSYNC_REPLICAS", 1)

        self.producer_config = {
            "bootstrap.servers": bootstrap_servers,
            "compression.type": getenv_str("COMPRESSION_TYPE", "none"),
            "linger.ms": getenv_int("LINGER_MS", 0),
            "batch.size": getenv_int("BATCH_SIZE", 524288),
            "message.max.bytes": getenv_int("MSG_MAX_BYTES", 1048576),
            "acks": getenv_str("ACKS", "0"),
            "retries": getenv_int("RETRIES", 2),
            "retry.backoff.ms": getenv_int("RETRY_BACKOFF_MS", 5),
            "reconnect.backoff.ms": getenv_int("RECONNECT_BACKOFF_MS", 100),
            "reconnect.backoff.max.ms": getenv_int("RECONNECT_BACKOFF_MAX_MS", 1000),
            "request.timeout.ms": getenv_int("REQUEST_TIMEOUT_MS", 10000),
            "delivery.timeout.ms": getenv_int("DELIVERY_TIMEOUT_MS", 120000),
            "queue.buffering.max.messages": getenv_int(
                "QUEUE_BUFFERING_MAX_MESSAGES", 200000
            ),
            "queue.buffering.max.kbytes": getenv_int(
                "QUEUE_BUFFERING_MAX_KBYTES", 131072
            ),
            "connections.max.idle.ms": getenv_int(
                "CONNECTION_MAX_IDLE_MS", 30000
            ),
            "socket.keepalive.enable": "true",
            "socket.send.buffer.bytes": 524288,
            "socket.receive.buffer.bytes": 524288,
            "client.id": self.pod_name,
            "metadata.max.age.ms": getenv_int("METADATA_MAX_AGE_MS", 120000),
            "topic.metadata.refresh.interval.ms": getenv_int(
                "TOPIC_METADATA_REFRESH_INTERVAL_MS", 120000
            ),
            "max.in.flight.requests.per.connection": getenv_int(
                "MAX_IN_FLIGHT_REQUEST_PER_CONNECTION", 4
            ),
        }

        self.producer = Producer(self.producer_config)

        # Admin client for topic creation
        self.admin = AdminClient({"bootstrap.servers": bootstrap_servers})
        self.create_topics_if_missing(
            self.topic_title,
            self.num_topics,
            self.num_partitions,
            self.replication_factor,
        )

        # Store a simple manifest for reproducibility
        os.makedirs("./logs", exist_ok=True)
        with open("./logs/producer_manifest.json", "w") as f:
            json.dump(
                {
                    "exp_id": self.exp_id,
                    "traffic_mode": self.traffic_mode,
                    "original_target_rate_msgs_per_s": self.original_target_rate,
                    "effective_rate_msgs_per_s": self.effective_rate,
                    "skew_partition": self.skew_partition,
                    "skew_fraction": self.skew_fraction,
                    "pod_name": self.pod_name,
                    "bootstrap_servers": bootstrap_servers,
                },
                f,
                indent=2,
            )

        # Background metric threads
        threading.Thread(
            target=self.update_metrics_periodically, daemon=True
        ).start()
        threading.Thread(
            target=self.monitor_buffer_metrics, daemon=True
        ).start()

    # --------------------------------------------------------
    #  Topic creation
    # --------------------------------------------------------

    def create_topics_if_missing(
        self, base_topic: str, num_topics: int, num_partitions: int, replication_factor: int
    ):
        topic_cfg = {"min.insync.replicas": str(self.min_in_sync)}
        topics = [
            NewTopic(f"{base_topic}_{i}", num_partitions, replication_factor, config=topic_cfg)
            for i in range(num_topics)
        ]
        fs = self.admin.create_topics(topics)
        for topic, f in fs.items():
            try:
                f.result()
                print(f"[INFO] Created topic {topic}")
            except KafkaException as e:
                print(f"[INFO] Topic {topic} may already exist: {e}")

    # --------------------------------------------------------
    #  Metrics helpers
    # --------------------------------------------------------

    def monitor_buffer_metrics(self, interval: int = 10):
        """Periodically print buffer usage from Kafka producer metrics."""
        while self.running:
            try:
                metrics = self.producer.metrics()
                for _, m in metrics.items():
                    if "producer-metrics" in m:
                        pm = m["producer-metrics"]
                        exhausted = pm.get("buffer-exhausted-records")
                        available = pm.get("bufferpool-available-records")
                        if exhausted is not None and available is not None:
                            print(
                                f"[BUFFER] exhausted={int(exhausted)} available={int(available)}"
                            )
                        else:
                            print("[BUFFER] metrics present but keys missing")
                        break
            except Exception as e:
                print(f"[WARN] Could not read buffer metrics: {e}")
            time.sleep(interval)

    def update_metrics_periodically(self):
        """Update Prometheus metrics once per second."""
        while self.running:
            try:
                cpu_gauge.set(psutil.cpu_percent(interval=None))
                mem_gauge.set(psutil.virtual_memory().percent)
                uptime_gauge.set(time.time() - self.start_time)

                elapsed = time.time() - self.start_time

                target_rate_gauge.set(self.original_target_rate)
                effective_rate_gauge.set(self.effective_rate)

                if elapsed > 0:
                    observed_rate_gauge.set(self.msg_sent / elapsed)

                if elapsed > 0 and self.msg_sent > 0:
                    msg_rate = self.msg_sent / elapsed
                    mb_rate = (self.bytes_sent / (1024 * 1024)) / elapsed
                    avg_msg_size = self.bytes_sent / self.msg_sent
                    msg_rate_gauge.set(msg_rate)
                    mb_rate_gauge.set(mb_rate)
                    avg_msg_size_gauge.set(avg_msg_size)
                    print(
                        f"[METRIC] Elapsed: {elapsed:.2f}s | Sent: {self.msg_sent} msgs | "
                        f"Rate: {msg_rate:.2f} msg/s | Throughput: {mb_rate:.4f} MB/s | "
                        f"Avg Msg Size: {avg_msg_size:.2f} bytes | Target: {self.dynamic_target_rate} msg/s"
                    )
            except Exception as e:
                print(f"[ERROR] Metrics update error: {e}")
            time.sleep(1)

    # --------------------------------------------------------
    #  Sending messages
    # --------------------------------------------------------

    def _choose_partition(self) -> int:
        """Partition selection based on traffic_mode and skew parameters."""
        if not self.num_partitions or self.num_partitions <= 1:
            return 0

        if self.traffic_mode == "skew":
            # With probability skew_fraction, send to hot partition
            if self._rng.random() < self.skew_fraction:
                return self.skew_partition % self.num_partitions
            # Otherwise distribute among non-hot partitions
            return next(self._rr_other)

        # Default / balanced: round-robin across all partitions
        return next(self._rr_all_parts)

    def send_message(self, topic: str, idx: int):
        """Send a single synthetic message with fixed 32KB payload."""
        payload_size = 32 * 1024
        dummy_content = "".join(
            random.choices("ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789", k=payload_size)
        )
        message_bytes = dummy_content.encode("utf-8")

        send_time = time.time()

        headers = [
            ("index", str(idx).encode()),
            ("producer_timestamp", str(send_time).encode()),
            ("producer_pod_name", self.pod_name.encode()),
            ("size_bytes", str(len(message_bytes)).encode()),
            ("target_rate", str(self.dynamic_target_rate).encode()),
            ("exp_id", self.exp_id.encode()),
            ("traffic_mode", self.traffic_mode.encode()),
        ]

        try:
            partition = self._choose_partition()

            self.producer.produce(
                topic=topic,
                key=None,
                value=message_bytes,
                headers=headers,
                partition=partition,
                callback=self.ack_callback,
            )

            # Serve delivery callbacks and free queue space
            self.producer.poll(0)

            self.msg_sent += 1
            self.bytes_sent += len(message_bytes)
            msg_sent_counter.inc()

            if self.msg_sent % 100 == 0:
                print(f"[INFO] Sent #{self.msg_sent}")
        except BufferError:
            print("[WARN] Local queue is full, retrying...")
            self.producer.poll(1)
            time.sleep(0.1)
        except KafkaException as e:
            print(f"[ERROR] Send failed: {e}")

    def ack_callback(self, err, msg):
        """Delivery report callback."""
        if err:
            print(f"[ERROR] Delivery failed: {err}")
            return

        headers = dict(msg.headers() or [])
        try:
            raw = headers.get("producer_timestamp") or headers.get("send_time")
            if raw is None:
                raise ValueError("Missing producer_timestamp/send_time header")
            send_time = float(
                raw.decode() if isinstance(raw, (bytes, bytearray)) else raw
            )
            latency = time.time() - send_time
            delivery_latency_histogram.observe(latency)
            self.total_latency += latency
            self.latency_count += 1
            self.last_latency = latency

            if self.msg_sent % 100 == 0:
                print(
                    f"[ACK] Delivered #{self.msg_sent} | Latency={latency:.5f}s | "
                    f"Target={self.dynamic_target_rate} msg/sec"
                )
        except Exception as e:
            print(f"[WARN] Latency calculation failed: {e}, headers={headers}")

    # --------------------------------------------------------
    #  Main send loop
    # --------------------------------------------------------

    def start_synthetic_stream(self):
        """Main synthetic workload loop."""
        idx = 0
        try:
            while self.running and not shutdown_event.is_set():
                topic = f"{self.topic_title}_{idx % self.num_topics}"

                self.send_message(topic, idx)
                idx += 1

                # Simple pacing based on target_rate (workload intensity)
                time_per_msg = 1.0 / max(self.dynamic_target_rate, 1.0)
                if shutdown_event.wait(timeout=time_per_msg):
                    break
        except KeyboardInterrupt:
            self.stop()

    def stop(self):
        self.running = False
        try:
            self.producer.flush()
        except Exception as e:
            print(f"[WARN] Flush on stop failed: {e}")
        print("[INFO] Producer stopped cleanly.")


# ============================================================
#  Server / Shutdown wiring
# ============================================================

def start_metrics_server():
    port = getenv_int("METRICS_PORT", 8000)
    print(f"[INFO] Starting metrics server on port {port}")
    app.run(host="0.0.0.0", port=port)


def graceful_shutdown(signal_num, frame):
    global producer_instance
    print(f"[INFO] Received shutdown signal ({signal_num}).")
    shutdown_event.set()
    if producer_instance:
        producer_instance.stop()
    sys.exit(0)


if __name__ == "__main__":
    signal.signal(signal.SIGINT, graceful_shutdown)
    signal.signal(signal.SIGTERM, graceful_shutdown)
    signal.signal(signal.SIGQUIT, graceful_shutdown)

    producer_instance = MyProducer()
    threading.Thread(target=start_metrics_server, daemon=True).start()

    try:
        producer_instance.start_synthetic_stream()
    except KeyboardInterrupt:
        print("[INFO] KeyboardInterrupt caught. Shutting down...")
        shutdown_event.set()
        producer_instance.stop()
        sys.exit(0)

