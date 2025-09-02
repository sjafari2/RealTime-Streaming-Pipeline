import json
import time
import random
import argparse
import threading
import psutil
from flask import Flask, Response
from prometheus_client import (
    Counter, Gauge, Histogram, generate_latest, CONTENT_TYPE_LATEST
)
from confluent_kafka import Producer, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic
import socket
import signal
import sys

shutdown_event = threading.Event()

# -------------------------
# Flask / Prometheus server
# -------------------------
app = Flask(__name__)

@app.route("/")
def hello():
    return "This is my Kafka app"

@app.route("/metrics")
def metrics():
    return Response(generate_latest(), mimetype=CONTENT_TYPE_LATEST)

# -------------------------
# Global labels
# -------------------------
POD = socket.gethostname()

# -------------------------
# Producer metrics (labeled)
# -------------------------
msg_sent_counter = Counter(
    'producer_messages_sent_total', 'Total messages sent', ['pod', 'topic']
)
msg_rate_gauge = Gauge(
    'producer_message_rate', 'Message send rate (msg/sec)', ['pod']
)
mb_rate_gauge = Gauge(
    'producer_mb_rate', 'Throughput (MB/sec)', ['pod']
)
cpu_gauge = Gauge(
    'producer_cpu_percent', 'CPU percent usage', ['pod']
)
mem_gauge = Gauge(
    'producer_memory_percent', 'Memory percent usage', ['pod']
)
uptime_gauge = Gauge(
    'producer_uptime_seconds', 'Producer uptime in seconds', ['pod']
)

# ACK latency (producer -> broker)
delivery_latency_histogram = Histogram(
    'producer_delivery_latency_seconds',
    'Per-message delivery latency in seconds',
    ['pod', 'topic'],
    buckets=[0.001, 0.005, 0.01, 0.05, 0.1, 0.2, 0.5, 1, 2]
)
producer_latency_gauge = Gauge(
    'producer_avg_delivery_latency_ms', 'Average producer delivery latency in milliseconds', ['pod']
)

target_rate_gauge = Gauge(
    'producer_target_rate', 'Target message send rate (msg/sec)', ['pod']
)
avg_msg_size_gauge = Gauge(
    'producer_avg_msg_size_bytes', 'Average message size in bytes', ['pod']
)

# Local queue / buffer telemetry
producer_queue_len_gauge = Gauge(
    'producer_outqueue_len', 'Producer local queue length', ['pod']
)
buffer_available_gauge = Gauge(
    'producer_bufferpool_available_records', 'Bufferpool available records', ['pod']
)
buffer_exhausted_gauge = Gauge(
    'producer_buffer_exhausted_records', 'Buffer exhausted records', ['pod']
)

# Config “freeze” gauges (so each run is self-describing)
linger_gauge = Gauge('producer_cfg_linger_ms', 'linger.ms', ['pod'])
batch_gauge  = Gauge('producer_cfg_batch_bytes', 'batch.size', ['pod'])
acks_gauge   = Gauge('producer_cfg_acks', 'acks (1=1, -1=all)', ['pod'])
mif_gauge    = Gauge('producer_cfg_max_in_flight', 'max.in.flight.requests.per.connection', ['pod'])
comp_gauge   = Gauge('producer_cfg_compression',
                     'compression.type enum: 0=none,1=snappy,2=zstd,3=gzip', ['pod'])


class MyProducer:
    def __init__(self, args):
        self.start_time = time.time()
        self.msg_sent = 0
        self.bytes_sent = 0
        self.pod_name = POD
        self.running = True
        self.dynamic_target_rate = args.targetRate
        self.num_partitions = args.numPartitions

        # ack latency smoothing bucket
        self.total_latency = 0.0
        self.latency_count = 0
        self.last_latency = 0.0

        # Bootstrap for AdminClient (for topic creation)
        bootstrap_servers = ",".join([
            "pip-kafka-controller-0.pip-kafka-controller-headless.kafkastreamingdata.svc.cluster.local:9092",
            "pip-kafka-controller-1.pip-kafka-controller-headless.kafkastreamingdata.svc.cluster.local:9092",
            "pip-kafka-controller-2.pip-kafka-controller-headless.kafkastreamingdata.svc.cluster.local:9092"
        ])

        # Producer config
        self.producer_config = {
            'bootstrap.servers': "pip-kafka-controller-headless.kafkastreamingdata.svc.cluster.local:9092",
            'compression.type': args.compressionType,
            'linger.ms': int(args.lingerMs),
            'batch.size': int(args.batchSize),
            'message.max.bytes': int(args.msgMaxBytes),
            'acks': args.acks,
            'retries': int(args.retries),
            'retry.backoff.ms': int(args.retryBackoffMs),
            'reconnect.backoff.ms': int(args.reconnectBackoffMs),
            'reconnect.backoff.max.ms': int(args.reconnectBackoffMaxMs),
            'request.timeout.ms': int(args.requestTimeoutMs),
            'delivery.timeout.ms': int(args.deliveryTimeoutMs),
            'queue.buffering.max.messages': int(args.queueBufferingMaxMessages),
            'queue.buffering.max.kbytes': int(args.queueBufferingMaxKbytes),
            'connections.max.idle.ms': int(args.connectionsMaxIdleMs),
            'socket.keepalive.enable': args.socketKeepaliveEnable,
            'socket.send.buffer.bytes': 524288,
            'socket.receive.buffer.bytes': 524288,
            'client.id': self.pod_name,
            'metadata.max.age.ms': int(args.metadataMaxAgeMs),
            'topic.metadata.refresh.interval.ms': int(args.topicMetadataRefreshIntervalMs),
            'max.in.flight.requests.per.connection': int(args.maxInFlightRequestsPerConnection),
        }

        # Create producer and admin
        self.producer = Producer(self.producer_config)
        self.admin = AdminClient({'bootstrap.servers': bootstrap_servers})

        # Freeze config gauges once
        linger_gauge.labels(self.pod_name).set(int(self.producer_config['linger.ms']))
        batch_gauge.labels(self.pod_name).set(int(self.producer_config['batch.size']))
        acks_map = {'0': 0, '1': 1, 'all': -1, '-1': -1}
        acks_gauge.labels(self.pod_name).set(acks_map.get(str(self.producer_config['acks']), 1))
        mif_gauge.labels(self.pod_name).set(int(self.producer_config['max.in.flight.requests.per.connection']))
        comp_map = {'none': 0, 'snappy': 1, 'zstd': 2, 'gzip': 3}
        comp_gauge.labels(self.pod_name).set(comp_map.get(self.producer_config['compression.type'], 0))

        # Ensure topics exist
        self.create_topics_if_missing(args.topicTitle, args.numTopics, args.numPartitions, args.replica)

        # Background metric loops
        threading.Thread(target=self.update_metrics_periodically, daemon=True).start()
        threading.Thread(target=self.monitor_buffer_metrics, daemon=True).start()

    def create_topics_if_missing(self, base_topic, num_topics, num_partitions, replication_factor):
        topics = [NewTopic(f"{base_topic}_{i}", num_partitions, replication_factor) for i in range(num_topics)]
        fs = self.admin.create_topics(topics)
        for topic, f in fs.items():
            try:
                f.result()
                print(f"[INFO] Created topic {topic}")
            except KafkaException as e:
                # Topic likely already exists
                print(f"[INFO] Topic {topic} may already exist: {e}")

    def monitor_buffer_metrics(self, interval=10):
        while self.running and not shutdown_event.is_set():
            try:
                # Local outqueue length (use __len__)
                producer_queue_len_gauge.labels(self.pod_name).set(len(self.producer))

                # Producer metrics (when exposed by librdkafka)
                metrics = self.producer.metrics()
                for _, m in metrics.items():
                    if 'producer-metrics' in m:
                        pm = m['producer-metrics']
                        exhausted = pm.get('buffer-exhausted-records') or 0
                        available = pm.get('bufferpool-available-records') or 0
                        buffer_exhausted_gauge.labels(self.pod_name).set(int(exhausted))
                        buffer_available_gauge.labels(self.pod_name).set(int(available))
                        break
            except Exception as e:
                print(f"[WARN] Could not read buffer metrics: {e}")
            time.sleep(interval)

    def update_metrics_periodically(self):
        while self.running and not shutdown_event.is_set():
            try:
                cpu_gauge.labels(self.pod_name).set(psutil.cpu_percent(interval=1))
                mem_gauge.labels(self.pod_name).set(psutil.virtual_memory().percent)
                uptime_gauge.labels(self.pod_name).set(time.time() - self.start_time)
                target_rate_gauge.labels(self.pod_name).set(self.dynamic_target_rate)

                elapsed = time.time() - self.start_time
                if elapsed > 0 and self.msg_sent > 0:
                    msg_rate = self.msg_sent / elapsed
                    mb_rate = (self.bytes_sent / (1024 * 1024)) / elapsed
                    avg_msg_size = self.bytes_sent / self.msg_sent

                    msg_rate_gauge.labels(self.pod_name).set(msg_rate)
                    mb_rate_gauge.labels(self.pod_name).set(mb_rate)
                    avg_msg_size_gauge.labels(self.pod_name).set(avg_msg_size)

                    if self.latency_count > 0:
                        avg_ack_ms = (self.total_latency / self.latency_count) * 1000.0
                        producer_latency_gauge.labels(self.pod_name).set(avg_ack_ms)
                        # reset window
                        self.total_latency = 0.0
                        self.latency_count = 0
            except Exception as e:
                print(f"[ERROR] Metrics update error: {e}")
            time.sleep(1)

    def ack_callback(self, err, msg):
        if err:
            print(f"[ERROR] Delivery failed: {err}")
            return

        try:
            # msg.timestamp() -> (type, ts_ms). type: 0=CreateTime, 1=LogAppendTime
            ts_type, ts_ms = msg.timestamp() or (None, None)
            if ts_ms is None:
                # Fallback: try headers if timestamp is missing
                headers = dict(msg.headers() or [])
                st = headers.get("send_time")
                if st is not None:
                    ts_ms = float(st.decode()) * 1000 if isinstance(st, (bytes, bytearray)) else float(st) * 1000

            if ts_ms is None:
                print(f"[WARN] Ack latency extraction failed: no timestamp; headers={dict(msg.headers() or {})}")
                return

            latency = time.time() - (float(ts_ms) / 1000.0)
            delivery_latency_histogram.labels(self.pod_name, msg.topic()).observe(latency)
            self.total_latency += latency
            self.latency_count += 1
            self.last_latency = latency

            if self.msg_sent % 100 == 0:
                print(f"[ACK] #{self.msg_sent} | topic={msg.topic()} | latency={latency:.5f}s | outq_len={len(self.producer)}")

        except Exception as e:
            print(f"[WARN] Ack latency compute failed: {e}")

    def send_message(self, topic, idx, headers=None):
        # Fixed 32 KB payload
        payload_size = 32 * 1024
        dummy_content = ''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789', k=payload_size))
        message_bytes = dummy_content.encode('utf-8')

        send_time = time.time()
        full_headers = list(headers or []) + [
            ("index", str(idx).encode()),
            ("producer_timestamp", str(send_time).encode()),
            ("size_bytes", str(len(message_bytes)).encode()),
            ("target_rate", str(self.dynamic_target_rate).encode()),
            ("send_time", str(send_time).encode()),  # still useful for consumers
        ]

        try:
            self.producer.produce(
                topic=topic,
                key=None,
                value=message_bytes,
                headers=full_headers,
                callback=self.ack_callback,
                timestamp=int(send_time * 1000)  # <<< set producer CreateTime in ms
            )

            # Service delivery callbacks and free internal queues
            self.producer.poll(0)

            self.msg_sent += 1
            self.bytes_sent += len(message_bytes)
            msg_sent_counter.labels(self.pod_name, topic).inc()

            if self.msg_sent % 100 == 0:
                print(f"[INFO] Sent #{self.msg_sent} | outq_len={len(self.producer)}")

        except BufferError:
            print("[WARN] Local queue is full, retrying...")
            # Let producer drain a bit
            self.producer.poll(1)
            time.sleep(0.1)

        except KafkaException as e:
            print(f"[ERROR] Send failed: {e}")

    def start_synthetic_stream(self, topic_title, num_topics, delay_unused, random_range_unused):
        idx = 0
        try:
            while self.running and not shutdown_event.is_set():
                topic = f"{topic_title}_{idx % num_topics}"
                now = time.time()
                headers = [
                    ("index", str(idx).encode()),
                    ("producer_timestamp", str(now).encode()),
                    ("producer_pod", self.pod_name.encode())
                ]
                self.send_message(topic, idx, headers)
                idx += 1

                # Rate limit to dynamic_target_rate (msgs/sec)
                time_per_msg = 1.0 / float(self.dynamic_target_rate)
                if shutdown_event.wait(timeout=time_per_msg):
                    break

        except KeyboardInterrupt:
            self.stop()

    def stop(self):
        self.running = False
        try:
            # Flush outstanding messages
            self.producer.flush()
        except Exception as e:
            print(f"[WARN] Flush on stop failed: {e}")
        print("[INFO] Producer stopped cleanly.")


def start_metrics_server():
    # Expose /metrics on port 8000
    app.run(host='0.0.0.0', port=8000)


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

    parser = argparse.ArgumentParser()
    parser.add_argument('--topicTitle', type=str, required=True)
    parser.add_argument('--numTopics', type=int, required=True)
    parser.add_argument('--delay', type=float, required=True)  # kept for CLI compat (unused)
    parser.add_argument('--numPartitions', type=int, required=True)
    parser.add_argument('--replica', type=int, required=True)
    parser.add_argument('--randomRange', type=int, required=True)  # kept for CLI compat (unused)
    parser.add_argument('--lingerMs', type=int, required=True)
    parser.add_argument('--compressionType', type=str, required=True)
    parser.add_argument('--batchSize', type=int, required=True)
    parser.add_argument('--msgMaxBytes', type=int, required=True)
    parser.add_argument('--metadataMaxAgeMs', type=int, required=True)
    parser.add_argument('--topicMetadataRefreshIntervalMs', type=int, required=True)
    parser.add_argument('--maxInFlightRequestsPerConnection', type=int, required=True)
    parser.add_argument('--acks', type=str, required=True)
    parser.add_argument('--retries', type=int, required=True)
    parser.add_argument('--retryBackoffMs', type=int, required=True)
    parser.add_argument('--reconnectBackoffMs', type=int, required=True)
    parser.add_argument('--reconnectBackoffMaxMs', type=int, required=True)
    parser.add_argument('--minInSync', type=int, required=True)  # not used by client; kept for CLI symmetry
    parser.add_argument('--requestTimeoutMs', type=int, required=True)
    parser.add_argument('--deliveryTimeoutMs', type=int, required=True)
    parser.add_argument('--queueBufferingMaxMessages', type=int, required=True)
    parser.add_argument('--queueBufferingMaxKbytes', type=int, required=True)
    parser.add_argument('--targetRate', type=int, required=True)
    parser.add_argument('--connectionsMaxIdleMs', type=int, required=True)
    parser.add_argument('--socketKeepaliveEnable', action='store_true', help='Enable TCP socket keepalive')

    args = parser.parse_args()

    producer_instance = MyProducer(args)
    threading.Thread(target=start_metrics_server, daemon=True).start()

    try:
        producer_instance.start_synthetic_stream(args.topicTitle, args.numTopics, args.delay, args.randomRange)
    except KeyboardInterrupt:
        print("[INFO] KeyboardInterrupt caught. Shutting down...")
        shutdown_event.set()
        producer_instance.stop()
        sys.exit(0)

