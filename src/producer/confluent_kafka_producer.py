"""
This Kafka producer simulates real-time message traffic by sending synthetic messages
at a controlled rate. Each message includes metadata in headers and a randomly sized 
payload between 16 KB and 64 KB to mimic real-world load.

Key features:
- Adaptive rate control using latency feedback: the producer measures end-to-end 
  message delivery latency (from send to Kafka acknowledgment) and adjusts the 
  message rate (`target_rate`) accordingly.
- Exponential smoothing is applied to the observed latencies to prevent overreaction 
  to short-term spikes or noise. prev_latency = α * new_latency + (1 - α) * prev_latency, α (alpha) = smoothing factor (between 0 and 1)
  Larger α = faster response, more noise
  Smaller α = slower response, smoother behavior

- Message metadata such as send time, size, and target rate are encoded in Kafka 
  headers for efficient monitoring on the consumer side.
- Real-time metrics (throughput, latency, CPU/memory) are exported via Prometheus.

"""

import json
import time
import random
import argparse
import threading
import psutil
from flask import Flask, Response
#from prometheus_flask_exporter import PrometheusMetrics
from prometheus_client import Counter, Gauge, Histogram, start_http_server, generate_latest, CONTENT_TYPE_LATEST
from confluent_kafka import Producer, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic
import socket
import signal
import sys
import os
import itertools
#from helper import Tools

shutdown_event = threading.Event()

#start_http_server(9100)  # Starts a Prometheus metrics server on port 9100, exposing metrics to Prometheus
app = Flask(__name__)  # Starts Flask app on port 8000

@app.route("/")
def hello():
    return "This is my Kafka app"

@app.route("/metrics")
def metrics():
    return Response(generate_latest(), mimetype=CONTENT_TYPE_LATEST)

#metrics_exporter = PrometheusMetrics(app, defaults_prefix=None)

msg_sent_counter = Counter('producer_messages_sent_total', 'Total messages sent')
msg_rate_gauge = Gauge('producer_message_rate', 'Message send rate (msg/sec)')
mb_rate_gauge = Gauge('producer_mb_rate', 'Throughput (MB/sec)')
cpu_gauge = Gauge('producer_cpu_percent', 'CPU percent usage')
mem_gauge = Gauge('producer_memory_percent', 'Memory percent usage')
uptime_gauge = Gauge('producer_uptime_seconds', 'Producer uptime in seconds')
delivery_latency_histogram = Histogram('producer_delivery_latency_seconds', 'Per-message delivery latency in seconds', buckets=[0.001, 0.005, 0.01, 0.05, 0.1, 0.2, 0.5, 1, 2])
#producer_latency_gauge = Gauge('producer_avg_delivery_latency_ms', 'Average producer delivery latency in milliseconds')
target_rate_gauge = Gauge('producer_target_rate', 'Target message send rate (msg/sec)')
effective_rate_gauge = Gauge('producer_effective_rate', 'Applied setpoint (msg/sec)')
observed_rate_gauge  = Gauge('producer_observed_rate', 'Observed msg/s over run')
avg_msg_size_gauge = Gauge('producer_avg_msg_size_bytes', 'Average message size in bytes')

producer_instance = None

class MyProducer:
    def __init__(self, args):
        self.start_time = time.time()
        self.msg_sent = 0
        self.bytes_sent = 0
        self.pod_name = socket.gethostname()
        self.running = True    
        self.min_in_sync = int(args.minInSync)
        self.original_target_rate = int(args.targetRate)
        self.dynamic_target_rate = self.original_target_rate   # baseline initialization

        # Allow env override for S0 fixed rates (500/1000/5000)
        _sr = os.getenv("STEADY_RATE_MSGS")
        if _sr:
            try:
                self.effective_rate = int(_sr)        # applied setpoint
            except Exception:
                self.effective_rate = self.original_target_rate
        else:
            self.effective_rate = self.original_target_rate

        # Use effective_rate as the pacing setpoint for the existing loop
        self.dynamic_target_rate = self.effective_rate

        self.num_partitions = args.numPartitions
        # --- Skew/balanced + fixed-rate options (env-overridable, minimal change) ---
        self.traffic_mode   = os.getenv("TRAFFIC_MODE", "balanced").lower()  # "balanced" | "skew"
        self.skew_partition = int(os.getenv("SKEW_PARTITION", "0"))
        try:
            self.skew_fraction = float(os.getenv("SKEW_FRACTION", "0.7"))
        except Exception:
            self.skew_fraction = 0.7
        self.skew_fraction = max(0.0, min(1.0, self.skew_fraction))

               
        # ---- Dynamic EXP_ID builder (run id + mode + rate [+ skew]) ----
        # Prefer an explicit RUN_ID (e.g., experiment number), else pod name/hostname, else "local".
        run_id = (
        os.getenv("RUN_ID")
        or os.getenv("POD_NAME")
        or os.getenv("HOSTNAME")
        or "local"
        )

        # Build suffix if skew is active
        if self.traffic_mode == "skew":
            skew_suffix = f"_p{self.skew_partition}_a{int(round(self.skew_fraction*100))}"
        else:
            skew_suffix = ""

        # Compose EXP_ID: e.g., "exp42__skew__1000mps_p0_a70" or "pod-3__balanced__5000mps"
        self.exp_id = f"{run_id}__{self.traffic_mode}__{self.effective_rate}mps{skew_suffix}"

        # Export for downstream tools (consumers, scripts) if not explicitly set
        os.environ.setdefault("EXP_ID", self.exp_id)

        # Write manifest once (clean, not every 100 msgs)
        os.makedirs("./logs", exist_ok=True)
        with open("./logs/producer_manifest.json", "w") as f:
            json.dump({
                "exp_id": self.exp_id,
                "traffic_mode": self.traffic_mode,
                "original_target_rate_msgs_per_s": self.original_target_rate,  # CLI
                "effective_rate_msgs_per_s": self.effective_rate,              # applied setpoint
                "skew_partition": self.skew_partition,
                "skew_fraction": self.skew_fraction,
                "pod_name": self.pod_name,
            }, f, indent=2)

        # Partition round-robin iterators (for balanced / non-hot others)
        hot = self.skew_partition % max(1, self.num_partitions)
        others = [p for p in range(self.num_partitions) if p != hot] or [hot]
        self._rr_all_parts = itertools.cycle(range(self.num_partitions or 1))
        self._rr_other     = itertools.cycle(others)
        self._rng          = random.Random(42)

        self.prev_latency = 0.1  # Initial guess
        self.total_latency = 0.0
        self.latency_count = 0
        self.last_latency = 0
 
        #t = Tools()
        #config = t.read_config('producer.properties')

        #username = config.get('sasl.username', 'user1')
        #password = config.get('sasl.password', '5x4XjjbPod')
        #security_protocol = config.get('security.protocol', 'SASL_PLAINTEXT')
        #sasl_mechanism = config.get('sasl.mechanism', 'PLAIN')

        #'bootstrap.servers': 'pip-kafka-broker:9092'

        bootstrap_servers = ",".join([
            "pip-kafka-controller-0.pip-kafka-controller-headless.kafkastreamingdata.svc.cluster.local:9092",
            "pip-kafka-controller-1.pip-kafka-controller-headless.kafkastreamingdata.svc.cluster.local:9092",
            "pip-kafka-controller-2.pip-kafka-controller-headless.kafkastreamingdata.svc.cluster.local:9092"
        ])

        self.producer_config = {
            'bootstrap.servers': "pip-kafka-controller-headless.kafkastreamingdata.svc.cluster.local:9092",
            'compression.type': args.compressionType,
            'linger.ms': int(args.lingerMs),
            'batch.size': int(args.batchSize),
            'message.max.bytes': int(args.msgMaxBytes),
            'acks': str(args.acks),
            'retries': int(args.retries),
            'retry.backoff.ms': int(args.retryBackoffMs),
            'reconnect.backoff.ms': int(args.reconnectBackoffMs),
            'reconnect.backoff.max.ms': int(args.reconnectBackoffMaxMs),
            'request.timeout.ms': int(args.requestTimeoutMs),
            'delivery.timeout.ms': int(args.deliveryTimeoutMs),
            'queue.buffering.max.messages': int(args.queueBufferingMaxMessages),
            'queue.buffering.max.kbytes': int(args.queueBufferingMaxKbytes),
            'connections.max.idle.ms': int(args.connectionsMaxIdleMs),
            'socket.keepalive.enable': 'true' if args.socketKeepaliveEnable else 'false',
            'socket.send.buffer.bytes': 524288,
            'socket.receive.buffer.bytes': 524288,
            #'socket.request.max.bytes': 32*1024*1024, 
            #'security.protocol': "SASL_PLAINTEXT",
            #'sasl.mechanism': "PLAIN",
            #'sasl.username': "user1",
            #'sasl.password': "5x4XjjbPod",
            #'debug': "protocol,broker",
            'client.id': socket.gethostname(),
            'metadata.max.age.ms': int(args.metadataMaxAgeMs),
            'topic.metadata.refresh.interval.ms': int(args.topicMetadataRefreshIntervalMs),
            'max.in.flight.requests.per.connection': int(args.maxInFlightRequestsPerConnection),
            #'queue.buffering.max.ms': 1  # removed in Confluent Kafka
            #'socket.keepalive.enable': True,

        }

        self.producer = Producer(self.producer_config)
        self.admin = AdminClient({'bootstrap.servers': 'pip-kafka-broker:9092'})
        self.create_topics_if_missing(args.topicTitle, args.numTopics, args.numPartitions, args.replica)

        threading.Thread(target=self.update_metrics_periodically, daemon=True).start()
        #threading.Thread(target=self.auto_flush, daemon=True).start()
        threading.Thread(target=self.monitor_buffer_metrics, daemon=True).start() 

    def create_topics_if_missing(self, base_topic, num_topics, num_partitions, replication_factor):
        topic_cfg = {"min.insync.replicas": str(self.min_in_sync)}  # topic-level config must be strings
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

    def monitor_buffer_metrics(self, interval=10):
        while self.running:
            try:
                metrics = self.producer.metrics()
                for _, m in metrics.items():
                    if 'producer-metrics' in m:
                        pm = m['producer-metrics']
                        exhausted = pm.get('buffer-exhausted-records')
                        available = pm.get('bufferpool-available-records')
                        if exhausted is not None and available is not None:
                            print(f"[BUFFER] exhausted={int(exhausted)} available={int(available)}")
                        else:
                            print("[BUFFER] metrics present but keys missing")
                        break
            except Exception as e:
                print(f"[WARN] Could not read buffer metrics: {e}")
            time.sleep(interval)

    def update_metrics_periodically(self):
        while self.running:
            try:
                cpu_gauge.set(psutil.cpu_percent(interval=None))     # None-Blocking
                mem_gauge.set(psutil.virtual_memory().percent)
                uptime_gauge.set(time.time() - self.start_time)
                
                elapsed = time.time() - self.start_time
                
                target_rate_gauge.set(self.original_target_rate)     # what CLI asked for
                effective_rate_gauge.set(self.effective_rate)        # what we applied
                
                if elapsed > 0:
                    observed_rate_gauge.set(self.msg_sent / elapsed)

                               
                if elapsed > 0 and self.msg_sent > 0:
                    msg_rate = self.msg_sent / elapsed
                    mb_rate = (self.bytes_sent / (1024 * 1024)) / elapsed
                    avg_msg_size = self.bytes_sent / self.msg_sent
                    msg_rate_gauge.set(msg_rate)
                    mb_rate_gauge.set(mb_rate)
                    avg_msg_size_gauge.set(avg_msg_size)
                    print(f" Target rate: {self.dynamic_target_rate} msg/sec")
                    print(f"[METRIC] Elapsed: {elapsed:.2f}s | Sent: {self.msg_sent} msgs | Rate: {msg_rate:.2f} msg/s | Throughput: {mb_rate:.4f} MB/s | Avg Msg Size: {avg_msg_size:.2f} bytes")
            except Exception as e:
                print(f"[ERROR] Metrics update error: {e}")
            time.sleep(1)
    '''
    def auto_flush(self, interval=0.1): # auto flush evry 100 ms
        while self.running:
            try:
                self.producer.flush()
            except Exception as e:
                print(f"[WARN] Periodic flush failed: {e}")
            time.sleep(interval)
    '''
    def send_message(self, topic, idx, headers=None):
    # Fixed 32 KB payload
        payload_size = 32 * 1024
        dummy_content = ''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789', k=payload_size))
        message_bytes = dummy_content.encode('utf-8')

        send_time = time.time()

        full_headers = headers or []
        full_headers.extend([
            #("index", str(idx).encode()),
            ("send_time", str(send_time).encode()),#time just before calling produce(), matches Kafka delivery latency measurement, used by the producer (ack_callback) to compute Kafka-level delivery latency
            #("producer_pod_name", self.pod_name.encode()),
            ("size_bytes", str(len(message_bytes)).encode()),
            ("target_rate", str(self.dynamic_target_rate).encode())
        ])
        #print(f"[DEBUG] Setting headers: {full_headers}")

        try:

            # --- Choose partition (balanced vs skew) ---
            if self.num_partitions and self.num_partitions > 1:
                if self.traffic_mode == "skew" and self._rng.random() < self.skew_fraction:
                    partition = self.skew_partition % self.num_partitions
                else:
                    partition = next(self._rr_all_parts) if self.traffic_mode == "balanced" else next(self._rr_other)
            else:
                partition = 0   

            self.producer.produce(
                topic=topic,
                key=None,
                value=message_bytes,
                headers=full_headers,
                partition=partition,
                callback=self.ack_callback,
            )

            self.producer.poll(0) # immediately serve delivery callbacks and free space in the queue

            # Measure latency as time since send started
            latency = time.time() - send_time
            #self.total_latency += latency
            #self.latency_count += 1
            self.msg_sent += 1
            self.bytes_sent += len(message_bytes)

            if self.msg_sent % 100 == 0:
                print(f"[INFO] Sent #{self.msg_sent} | Latency={latency:.3f}s") # | Target rate={self.dynamic_target_rate}")
                       
        except BufferError:
            print("[WARN] Local queue is full, retrying...")
            self.producer.poll(1)  # block and free space
            time.sleep(0.1)
        
        except KafkaException as e:
            print(f"[ERROR] Send failed: {e}")

    def send_batch_messages(self, topic, idx, headers=None):
        delivered = False
        payload_size = 32 * 1024 #random.randint(64 * 1024)  # Size in bytes
        dummy_content = ''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789', k=payload_size))
        message_bytes = dummy_content.encode('utf-8')
        while not delivered and self.running:
            try:
                #key = str(idx % self.num_partitions).encode()
                send_time = str(time.time()).encode()
                full_headers = list(headers or [])+[
                
                    ("send_time", send_time),
                    ("size_bytes", str(len(message_bytes)).encode()),
                    ("target_rate", str(self.dynamic_target_rate).encode())
                ]
                # --- Choose partition (balanced vs skew) ---
                if self.num_partitions and self.num_partitions > 1:
                    if self.traffic_mode == "skew" and self._rng.random() < self.skew_fraction:
                        partition = self.skew_partition % self.num_partitions
                    else:
                        partition = next(self._rr_all_parts) if self.traffic_mode == "balanced" else next(self._rr_other)
                else:
                    partition = 0

                self.producer.produce(
                    topic=topic,
                    key=None,
                    value=message_bytes,
                    headers=full_headers,
                    callback=self.ack_callback,
                    partition=partition
                    )
                self.producer.poll(0) # 0 = Non-blocking: checks the internal response buffer for any pending delivery reports, processes them and triggers the on_delivery callbacks
                delivered = True
                self.msg_sent += 1
                self.bytes_sent += len(message_bytes)
                msg_sent_counter.inc()
                #print("[DEBUG] Sending headers:", full_headers)

            except BufferError:
                #self.dynamic_target_rate = max(int(self.dynamic_target_rate * 0.9), 10)
                self.producer.poll(1)  # Blocking: wait up to 1s to flush buffer when full
                time.sleep(0.2)
            
            except KafkaException as e:
                print(f"[ERROR] Send failed: {e}")
                break

    def ack_callback(self, err, msg):
        if err:
            print(f"[ERROR] Delivery failed: {err}")
        else:
            headers = dict(msg.headers() or [])
            try:
                
                # Extract and decode the 'send_time' header
                raw = headers.get("send_time")
                send_time = float(raw.decode()) if isinstance(raw, (bytes, bytearray)) else float(raw)

                latency = time.time() - send_time  # time taken from when a message is sent to when it is acknowledged by Kafka

                # Record latency in histogram
                delivery_latency_histogram.observe(latency)
               
                # Update internal tracking for average latency
                self.total_latency += latency  # sum of all message delivery latencies in a time window
                self.latency_count += 1        # number of messages delivered in that window
                self.last_latency = latency  # Store for later use
                #print(f"[ACK] Latency={latency:.3f}s | Target rate={self.dynamic_target_rate} msg/sec | In-flight={self.producer.outq_len()} | Msg #{self.msg_sent}")
                print(f"[ACK] Latency={latency:.5f}s | Target rate={self.dynamic_target_rate} msg/sec")

            except (KeyError, ValueError, AttributeError) as e:
                print(f"[WARN] Latency calculation failed: {e}, headers={headers}")
           
           #queue_size = self.producer.len()
            if self.msg_sent % 100 == 0:
                print(f"[INFO] Delivered #{self.msg_sent} | last_latency={getattr(self, 'last_latency', 0.0):.5f}s")
                #latency_to_log = getattr(self, 'last_latency', 0.000)  # Default to 0 if not set
                #print(f"[INFO] Delivered to {msg.topic()} | Latency={latency:.3f}s ") # | Target rate={self.dynamic_target_rate}")
                #print(f"[INFO] Sent #{self.msg_sent} | Latency={latency_to_log:.5f}s | Callback Latency Available={latency is not None}")
            #excepstit Exception as e:
            #    send_time = time.time()
            #    print("[WARN] Failed to parse send_time header:", e)


    def start_synthetic_stream(self, topic_title, num_topics, delay, random_range):
        idx = 0
        try:
            while self.running and not shutdown_event.is_set():
                now = time.time()
                topic = f"{topic_title}_{idx % num_topics}"
               # payload = json.dumps({
               #     "index": idx,
               #     "producer_timestamp": now,
               #     "pod_name": self.pod_name,
               #     "values": [random.randint(0, random_range) for _ in range(random.randint(3, 6))]
               # })
                headers = [
                    ("index", str(idx).encode()),
                    ("producer_timestamp", str(now).encode()), # time at which the synthetic workload loop reached this message, for end-to-end latency measurement by the consumer
                    ("producer_pod_name", self.pod_name.encode())
                ]
                self.send_message(topic, idx, headers)
                idx += 1
                time_per_msg = 1 / self.dynamic_target_rate
                if shutdown_event.wait(timeout=time_per_msg):  #waits for  time_per_msg sec unless shutdown happens earlier which happens when shutdown flag is set
                    break
        except KeyboardInterrupt:
            self.stop()

    def stop(self):
        self.running = False
        self.producer.flush()
        print("[INFO] Producer stopped cleanly.")

def start_metrics_server():
    app.run(host='0.0.0.0', port=8000) # Starts a Flask web server, serves custom Flask routes 

def graceful_shutdown(signal_num, frame):
    global producer_instance
    print(f"[INFO] Received shutdown signal ({signal_num}).")
    shutdown_event.set() # Triggers shutdown (usually on SIGINT, SIGTERM) by raising the shutdown flag 
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
    parser.add_argument('--delay', type=float, required=True)
    parser.add_argument('--numPartitions', type=int, required=True)
    parser.add_argument('--replica', type=int, required=True)
    parser.add_argument('--randomRange', type=int, required=True)
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
    parser.add_argument('--minInSync', type=int, required=True)
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

