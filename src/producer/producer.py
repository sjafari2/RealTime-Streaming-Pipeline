# producer.py
import os
import time
import random
import threading
import socket
import signal
import sys
import hashlib
from typing import List, Optional

import psutil
from flask import Flask, Response
from prometheus_client import Counter, Gauge, generate_latest, CONTENT_TYPE_LATEST
from confluent_kafka import Producer, KafkaException

producer_instance = None
shutdown_event = threading.Event()
app = Flask(__name__)


@app.route("/")
def hello():
    return "Kafka producer is running"


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


# Prometheus labels
COMMON_LABELS = ["pod", "client_id", "exp_id", "run_id", "traffic_mode"]

producer_cpu_percent = Gauge("producer_cpu_percent", "CPU usage percent", COMMON_LABELS)
producer_memory_percent = Gauge("producer_memory_percent", "Memory usage percent", COMMON_LABELS)
producer_uptime_seconds = Gauge("producer_uptime_seconds", "Producer uptime in seconds", COMMON_LABELS)

producer_messages_sent_total = Counter("producer_messages_sent_total", "Total messages enqueued for send", COMMON_LABELS)
producer_bytes_sent_total = Counter("producer_bytes_sent_total", "Total bytes enqueued for send", COMMON_LABELS)
producer_target_rate = Gauge("producer_target_rate", "Configured target send rate (msg/sec)", COMMON_LABELS)

producer_delivery_ok_total = Counter("producer_delivery_ok_total", "Total deliveries acked by broker", COMMON_LABELS)
producer_delivery_err_total = Counter("producer_delivery_err_total", "Total delivery errors", COMMON_LABELS)
producer_outq_len = Gauge("producer_outq_len", "librdkafka outgoing queue length", COMMON_LABELS)

producer_hot_partition_count = Gauge("producer_hot_partition_count", "Number of hot partitions used (skew mode)", COMMON_LABELS)
producer_skew_fraction = Gauge("producer_skew_fraction", "Fraction of messages sent to hot partitions (skew mode)", COMMON_LABELS)


class MyProducer:
    """
    Producer supports:
      - balanced
      - skew (SKEW_FRACTION messages go to hot partitions)

    Topic naming (your current design):
      - ConfigMap provides TOPIC_TITLE as the run-specific topic prefix,
        e.g., TOPIC_TITLE="B0-R500-20260216-204006"
      - Topics are: TOPIC_TITLE_0 .. TOPIC_TITLE_{TOPIC_COUNT-1}
      - RUN_ID is used for metrics labeling/headers (optional but recommended).
    """

    def __init__(self):
        self.pod_name = socket.gethostname()
        self.client_id = getenv_str("PRODUCER_CLIENT_ID", self.pod_name)

        self.exp_id = getenv_str("EXP_ID", "B0")
        self.traffic_mode = getenv_str("TRAFFIC_MODE", "balanced").strip().lower()
        
        if self.traffic_mode not in {"balanced", "skew"}:
            raise ValueError(f"Unsupported TRAFFIC_MODE='{self.traffic_mode}'. Use 'balanced' or 'skew'.")
        
        self.max_burst = getenv_int("MAX_BURST", 10)
        self.min_burst = max(1, getenv_int("MIN_BURST", 1))
        self.drop_catchup = getenv_str("DROP_CATCHUP", "true").strip().lower() == "true"

        tr_env = os.getenv("TARGET_RATE") or os.getenv("STEADY_RATE_MSGS") or "500"
        self.target_rate = max(1.0, float(tr_env))
        self.interval = 1.0 / self.target_rate

        self.run_id = getenv_str("RUN_ID", "").strip()

        # Producer-only stop condition
        self.max_messages = getenv_int("PRODUCER_MAX_MESSAGES", 0)

        # Producer pacing / batching / transport settings
        self.acks = getenv_str("ACKS", "1")
        self.compression_type = getenv_str("COMPRESSION_TYPE", "none")
        self.linger_ms = getenv_int("LINGER_MS", 0)
        self.batch_size = getenv_int("BATCH_SIZE", 16384)
        self.max_request_size = getenv_int("MAX_REQUEST_SIZE", 1048576)
        self.retries = getenv_int("RETRIES", 3)
        self.retry_backoff_ms = getenv_int("RETRY_BACKOFF_MS", 100)
        self.connection_max_idle_ms = getenv_int("CONNECTION_MAX_IDLE_MS", 30000)
        self.reconnect_backoff_ms = getenv_int("RECONNECT_BACKOFF_MS", 100)
        self.reconnect_backoff_max_ms = getenv_int("RECONNECT_BACKOFF_MAX_MS", 1000)
        self.request_timeout_ms = getenv_int("REQUEST_TIMEOUT_MS", 30000)
        self.delivery_timeout_ms = getenv_int("DELIVERY_TIMEOUT_MS", 120000)
        self.queue_buffering_max_messages = getenv_int("QUEUE_BUFFERING_MAX_MESSAGES", 100000)
        self.queue_buffering_max_kbytes = getenv_int("QUEUE_BUFFERING_MAX_KBYTES", 1048576)
        self.msg_max_bytes = getenv_int("MSG_MAX_BYTES", 1048576)
        self.metadata_max_age_ms = getenv_int("METADATA_MAX_AGE_MS", 300000)
        self.topic_metadata_refresh_interval_ms = getenv_int("TOPIC_METADATA_REFRESH_INTERVAL_MS", 300000)
        self.max_in_flight = getenv_int("MAX_IN_FLIGHT_REQUEST_PER_CONNECTION", 1)
        self.producer_http_port = getenv_int("PRODUCER_HTTP_PORT", 8001)

        # Optional compatibility with old name if ever use PAYLOAD_SIZE_BYTES
        #self.payload_size_bytes = getenv_int("MSG_MAX_BYTES", getenv_int("PAYLOAD_SIZE_BYTES", 16 * 1024))
        self.metric_labels = {
            "pod": self.pod_name,
            "client_id": self.client_id,
            "exp_id": self.exp_id,
            "run_id": (self.run_id if self.run_id else "unset"),
            "traffic_mode": self.traffic_mode,
        }
        
        
        bootstrap_servers = getenv_str("BOOTSTRAP_SERVERS", "localhost:9092")

        producer_conf = {
            "bootstrap.servers": bootstrap_servers,
            "client.id": self.client_id,
            "acks": self.acks,
            "compression.type": self.compression_type,
            "linger.ms": self.linger_ms,
            "batch.size": self.batch_size,
            #"max.request.size": self.max_request_size,
            "retries": self.retries,
            "retry.backoff.ms": self.retry_backoff_ms,
            "connections.max.idle.ms": self.connection_max_idle_ms,
            "reconnect.backoff.ms": self.reconnect_backoff_ms,
            "reconnect.backoff.max.ms": self.reconnect_backoff_max_ms,
            "request.timeout.ms": self.request_timeout_ms,
            "delivery.timeout.ms": self.delivery_timeout_ms,
            "queue.buffering.max.messages": self.queue_buffering_max_messages,
            "queue.buffering.max.kbytes": self.queue_buffering_max_kbytes,
            "message.max.bytes": self.msg_max_bytes,
            "metadata.max.age.ms": self.metadata_max_age_ms,
            "topic.metadata.refresh.interval.ms": self.topic_metadata_refresh_interval_ms,
            "max.in.flight.requests.per.connection": self.max_in_flight,
        }

        self.producer = Producer(producer_conf)
        
        # ---- Topics derived ONLY from TOPIC_TITLE ----
        topic_title = getenv_str("TOPIC_TITLE", "").strip()
        if not topic_title:
            raise RuntimeError("TOPIC_TITLE is required (run-specific topic prefix). Check ConfigMap/write-back.")

        self.topic_prefix = topic_title
        self.num_topics = max(1, getenv_int("TOPIC_COUNT", 1))

        self.payload_size_bytes = getenv_int("PAYLOAD_SIZE_BYTES", 16 * 1024)

        # Debug logging controls
        self.debug_enabled = getenv_str("PRODUCER_DEBUG", "false").strip().lower() == "true"
        self.debug_every_n = getenv_int("PRODUCER_DEBUG_EVERY_N", 0)
        self.heartbeat_every_sec = getenv_float("PRODUCER_HEARTBEAT_SEC", 10.0)
        if self.heartbeat_every_sec < 1.0:
            self.heartbeat_every_sec = 10.0
        self.last_heartbeat = 0.0

        # Skew controls
        self.skew_fraction = float(getenv_float("SKEW_FRACTION", 0.8))
        self.hot_partitions_spec = getenv_str("HOT_PARTITIONS", "0.2").strip()
        self.num_partitions = getenv_int("NUM_PARTITIONS", 0)

        self.hot_partitions: List[int] = []
        if self.traffic_mode == "skew":
            self.hot_partitions = self._resolve_hot_partitions()

        producer_target_rate.labels(**self.metric_labels).set(self.target_rate)
        producer_hot_partition_count.labels(**self.metric_labels).set(len(self.hot_partitions) if self.hot_partitions else 0)
        producer_skew_fraction.labels(**self.metric_labels).set(self.skew_fraction if self.traffic_mode == "skew" else 0.0)

        self.start_time = time.time()
        self.last_sys_update = 0.0
        self.next_send_time = time.time()
        self.idx = 0
        self.running = True

        print(f"[INFO] Producer pod={self.pod_name}")
        print(f"[INFO] EXP_ID={self.exp_id} TARGET_RATE={self.target_rate} RUN_ID={self.run_id or 'unset'}")
        print(f"[INFO] TRAFFIC_MODE={self.traffic_mode}")
        print(f"[INFO] TOPIC_TITLE(prefix)={self.topic_prefix} TOPIC_COUNT={self.num_topics}")
        if self.traffic_mode == "skew":
            print(f"[INFO] SKEW_FRACTION={self.skew_fraction} hot_partitions={self.hot_partitions}")
        print(f"[INFO] Payload={self.payload_size_bytes} bytes")
        print(f"[INFO] PRODUCER_MAX_MESSAGES={self.max_messages if self.max_messages > 0 else 'unlimited'}")

    def _resolve_hot_partitions(self) -> List[int]:
        spec = self.hot_partitions_spec

        if "," in spec:
            parts = []
            for tok in spec.split(","):
                tok = tok.strip()
                if tok:
                    parts.append(int(tok))
            return sorted(list(set(parts)))

        try:
            if spec.isdigit() or (spec.startswith("-") and spec[1:].isdigit()):
                return [int(spec)]
        except Exception:
            pass

        try:
            frac = float(spec)
            if not (0.0 < frac <= 1.0):
                raise ValueError("SKEW_PARTITION fraction must be in (0, 1].")
            if self.num_partitions <= 0:
                raise ValueError("NUM_PARTITIONS must be set when SKEW_PARTITION is a fraction.")

            k = max(1, int(round(frac * self.num_partitions)))

            seed_source = (
                f"{self.exp_id}-"
                f"r{self.target_rate}-"
                f"p{self.num_partitions}-"
                f"alpha{self.hot_partitions_spec}-"
                f"f{self.skew_fraction}-"
                f"run{self.run_id}"
            )
            seed_bytes = hashlib.sha256(seed_source.encode("utf-8")).digest()
            seed_int = int.from_bytes(seed_bytes[:8], byteorder="big", signed=False)

            rng = random.Random(seed_int)
            hot = rng.sample(range(self.num_partitions), k)
            return sorted(hot)

        except Exception as e:
            print(f"[WARN] Could not parse SKEW_PARTITION='{spec}'. Error: {e}. Falling back to [0].")
            return [0]

    def _make_payload(self) -> bytes:
        return bytes(random.getrandbits(8) for _ in range(self.payload_size_bytes))

    def _choose_partition_for_skew(self) -> Optional[int]:
        if not self.hot_partitions:
            return None
        if random.random() < self.skew_fraction:
            return random.choice(self.hot_partitions)
        return None

    def _delivery_report(self, err, msg):
        if err is not None:
            producer_delivery_err_total.labels(**self.metric_labels).inc()
            if self.debug_enabled:
                print(f"[DELIVERY_ERR] topic={msg.topic()} err={err}")
            return

        producer_delivery_ok_total.labels(**self.metric_labels).inc()

        if self.debug_enabled and self.debug_every_n > 0 and (self.idx % self.debug_every_n) == 0:
            print(f"[DELIVERY_OK] topic={msg.topic()} partition={msg.partition()} offset={msg.offset()}")

    def send_one(self, topic: str, idx: int):
        """
        Send one Kafka message safely.
        If the producer queue is full (BufferError),
        the function waits and retries until the message
        is successfully queued.
        """
        
        send_time = time.time()
        payload = self._make_payload()

        headers = [
            ("index", str(idx).encode()),
            ("producer_timestamp", str(send_time).encode()),
            ("producer_pod_name", self.pod_name.encode()),
            ("size_bytes", str(len(payload)).encode()),
            ("target_rate", str(self.target_rate).encode()),
            ("exp_id", self.exp_id.encode()),
            ("run_id", (self.run_id or "unset").encode()),
            ("traffic_mode", self.traffic_mode.encode()),
        ]
        
        while True:

            try:
                if self.traffic_mode == "skew":
                    p = self._choose_partition_for_skew()
                    if p is None:
                        self.producer.produce(topic=topic, value=payload, headers=headers, callback=self._delivery_report)
                    else:
                        self.producer.produce(topic=topic, value=payload, headers=headers, partition=p, callback=self._delivery_report)
                else:
                    self.producer.produce(topic=topic, value=payload, headers=headers, callback=self._delivery_report)

                producer_messages_sent_total.labels(**self.metric_labels).inc()
                producer_bytes_sent_total.labels(**self.metric_labels).inc(len(payload))

                self.producer.poll(0)

                if self.debug_enabled and self.debug_every_n > 0 and (idx % self.debug_every_n) == 0:
                    print(f"[ENQUEUE_OK] idx={idx} topic={topic} mode={self.traffic_mode}")
                
                break

            except BufferError:
                self.producer.poll(1)
            except KafkaException as e:
                print(f"[ERROR] Produce failed: {e}")
                break

    def run(self):
        while self.running and not shutdown_event.is_set():
            
            if self.max_messages > 0 and self.idx >= self.max_messages:
                print(f"[INFO] Reached MAX_MESSAGES={self.max_messages}. Stopping producer.")
                break

            now = time.time()

            if now - self.last_sys_update >= 10.0:
                producer_cpu_percent.labels(**self.metric_labels).set(psutil.cpu_percent(interval=None))
                producer_memory_percent.labels(**self.metric_labels).set(psutil.virtual_memory().percent)
                producer_uptime_seconds.labels(**self.metric_labels).set(now - self.start_time)
                self.last_sys_update = now

            # HEARTBEAT
            if now - self.last_heartbeat >= self.heartbeat_every_sec:
                oq = -1.0
                try:
                    oq = float(self.producer.outq_len())
                    producer_outq_len.labels(**self.metric_labels).set(oq)
                except Exception:
                    pass

                if self.debug_enabled:
                    print(f"[HEARTBEAT] sent={self.idx}") #outq_len={oq}")

                self.last_heartbeat = now

            if now < self.next_send_time:
                time.sleep(min(0.001, self.next_send_time - now))
                continue

            behind = now - self.next_send_time
            
            # If we're behind by more than one interval, do NOT try to catch up.
            if self.drop_catchup and behind > self.interval:

                # latency-first: do NOT try to catch up; reset schedule
                self.next_send_time = now
                behind = 0

            # compute how many sends we're allowed to do right now (catch-up), but cap it
            due = int((now - self.next_send_time) / self.interval) + 1
            # Cap the burst
            if due < 1:
                due = 1
            elif due > self.max_burst:
                due = self.max_burst

            for _ in range(due):
                
                if self.max_messages > 0 and self.idx >= self.max_messages:
                    self.running = False
                    break
                
                topic = f"{self.topic_prefix}_{self.idx % self.num_topics}"
                self.send_one(topic, self.idx)
                self.idx += 1
                self.next_send_time += self.interval
 
        try:
            self.producer.flush(10)
        except Exception:
            pass

    def shutdown(self):
        self.running = False


def start_metrics_server():
    port = getenv_int("PRODUCER_HTTP_PORT", 8001)
    print(f"[INFO] Starting producer metrics server on port {port}")
    app.run(host="0.0.0.0", port=port)


def graceful_shutdown(signal_num, frame):
    print("[INFO] Shutting down producer gracefully...")
    shutdown_event.set()
    if producer_instance:
        producer_instance.shutdown()
    sys.exit(0)


if __name__ == "__main__":
    signal.signal(signal.SIGINT, graceful_shutdown)
    signal.signal(signal.SIGTERM, graceful_shutdown)
    signal.signal(signal.SIGQUIT, graceful_shutdown)

    producer_instance = MyProducer()
    threading.Thread(target=start_metrics_server, daemon=True).start()
    producer_instance.run()

