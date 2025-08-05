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
from flask import Flask
from prometheus_flask_exporter import PrometheusMetrics
from prometheus_client import Counter, Gauge, Histogram
from confluent_kafka import Producer, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic
import socket
import signal
import sys
import os
from helper import Tools
import socket

shutdown_event = threading.Event()

app = Flask(__name__)
metrics_exporter = PrometheusMetrics(app, defaults_prefix=None)

msg_sent_counter = Counter('producer_messages_sent_total', 'Total messages sent')
msg_rate_gauge = Gauge('producer_message_rate', 'Message send rate (msg/sec)')
mb_rate_gauge = Gauge('producer_mb_rate', 'Throughput (MB/sec)')
cpu_gauge = Gauge('producer_cpu_percent', 'CPU percent usage')
mem_gauge = Gauge('producer_memory_percent', 'Memory percent usage')
uptime_gauge = Gauge('producer_uptime_seconds', 'Producer uptime in seconds')
delivery_latency_histogram = Histogram('producer_delivery_latency_seconds', 'Per-message delivery latency in seconds', buckets=[0.001, 0.005, 0.01, 0.05, 0.1, 0.2, 0.5, 1, 2])
target_rate_gauge = Gauge('producer_target_rate', 'Target message send rate (msg/sec)')
avg_msg_size_gauge = Gauge('producer_avg_msg_size_bytes', 'Average message size in bytes')

producer_instance = None

class MyProducer:
    def __init__(self, args):
        self.start_time = time.time()
        self.msg_sent = 0
        self.bytes_sent = 0
        self.pod_name = socket.gethostname()
        self.running = True
        self.dynamic_target_rate = args.targetRate
        self.num_partitions = args.numPartitions
        self.prev_latency = 0.1  # Initial guess
        self.total_latency = 0.0
        self.latency_count = 0
 
        #t = Tools()
        #config = t.read_config('producer.properties')

        #username = config.get('sasl.username', 'user1')
        #password = config.get('sasl.password', '5x4XjjbPod')
        #security_protocol = config.get('security.protocol', 'SASL_PLAINTEXT')
        #sasl_mechanism = config.get('sasl.mechanism', 'PLAIN')

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
            #'security.protocol': "SASL_PLAINTEXT",
            #'sasl.mechanism': "PLAIN",
            #'sasl.username': "user1",
            #'sasl.password': "5x4XjjbPod",
            'debug': "protocol,broker",
            'client.id': socket.gethostname(),
            'metadata.max.age.ms': 60000,
            'topic.metadata.refresh.interval.ms': 60000,
            'max.in.flight.requests.per.connection': 1,
            'queue.buffering.max.ms': 1 
            #'socket.keepalive.enable': True,

        }

        self.producer = Producer(self.producer_config)
        self.admin = AdminClient({'bootstrap.servers': bootstrap_servers})
        self.create_topics_if_missing(args.topicTitle, args.numTopics, args.numPartitions, args.replica)

        #threading.Thread(target=self.update_metrics_periodically, daemon=True).start()
        threading.Thread(target=self.auto_flush, daemon=True).start()
        threading.Thread(target=self.monitor_buffer_metrics, daemon=True).start() 

    def create_topics_if_missing(self, base_topic, num_topics, num_partitions, replication_factor):
        topics = [NewTopic(f"{base_topic}_{i}", num_partitions, replication_factor) for i in range(num_topics)]
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
                        print(f"[BUFFER] exhausted={int(exhausted)}, available={int(available)}")
                        break  # Only need to check one broker
            except Exception as e:
                print(f"[WARN] Could not read buffer metrics: {e}")
            time.sleep(interval)

    def update_metrics_periodically(self):
        while self.running:
            try:
                #cpu_gauge.set(psutil.cpu_percent(interval=1))
                #mem_gauge.set(psutil.virtual_memory().percent)
                #uptime_gauge.set(time.time() - self.start_time)
                #target_rate_gauge.set(self.dynamic_target_rate)

                elapsed = time.time() - self.start_time
                if elapsed > 0 and self.msg_sent > 0:
                    msg_rate = self.msg_sent / elapsed
                    mb_rate = (self.bytes_sent / (1024 * 1024)) / elapsed
                    avg_msg_size = self.bytes_sent / self.msg_sent

                    #msg_rate_gauge.set(msg_rate)
                    #mb_rate_gauge.set(mb_rate)
                    #avg_msg_size_gauge.set(avg_msg_size)

                    if self.latency_count > 0:
                        current_latency = self.total_latency / self.latency_count
                        self.prev_latency = 0.8 * self.prev_latency + 0.2 * current_latency
                        print(f"[DEBUG] current_latency={current_latency:.4f}, prev_latency={self.prev_latency:.4f}, target_rate={self.dynamic_target_rate}")

                        # Adjust target rate based on latency feedback
                        if self.prev_latency > 0.5:
                            self.dynamic_target_rate = max(int(self.dynamic_target_rate * 0.8), 50)
                        elif self.prev_latency < 0.1 and self.dynamic_target_rate < 1000:
                            self.dynamic_target_rate = int(self.dynamic_target_rate * 1.1)

                        # Reset for next interval
                        self.total_latency = 0.0  # adds up all latencies in the last interval
                        self.latency_count = 0    # counts how many messages were acknowledged
                    print(f" Target rate: {self.dynamic_target_rate} msg/sec")

                    print(f"[METRIC] Elapsed: {elapsed:.2f}s | Sent: {self.msg_sent} msgs | Rate: {msg_rate:.2f} msg/s | Throughput: {mb_rate:.4f} MB/s | Avg Msg Size: {avg_msg_size:.2f} bytes")
            except Exception as e:
                print(f"[ERROR] Metrics update error: {e}")
            time.sleep(1)

    def auto_flush(self, interval=1):
        while self.running:
            try:
                self.producer.flush()
            except Exception as e:
                print(f"[WARN] Periodic flush failed: {e}")
            time.sleep(interval)

    def send_message(self, topic, idx, headers=None):
    # Fixed 64 KB payload
        payload_size = 64 * 1024
        dummy_content = ''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789', k=payload_size))
        message_bytes = dummy_content.encode('utf-8')

        send_time = time.time()

        full_headers = headers or []
        full_headers.extend([
            ("index", str(idx).encode()),
            ("producer_timestamp", str(send_time).encode()),
            ("producer_pod_name", self.pod_name.encode()),
            ("size_bytes", str(len(message_bytes)).encode()),
            ("target_rate", str(self.dynamic_target_rate).encode())
        ])

        try:
            self.producer.produce(
                topic=topic,
                key=None,
                value=message_bytes,
                headers=full_headers
            )
            self.producer.flush()  # Force immediate delivery

            # Measure latency as time since send started
            latency = time.time() - send_time
            #self.total_latency += latency
            #self.latency_count += 1
            self.msg_sent += 1
            self.bytes_sent += len(message_bytes)

            if self.msg_sent % 100 == 0:
                print(f"[INFO] Sent #{self.msg_sent} | Latency={latency:.3f}s") # | Target rate={self.dynamic_target_rate}")
        except KafkaException as e:
            print(f"[ERROR] Send failed: {e}")

    def send_batch_messages(self, topic, idx, headers=None):
        delivered = False
        # Generate random message body between 16 KB and 64 KB
        payload_size = 64 * 1024 #random.randint(64 * 1024)  # Size in bytes
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
                partition = idx % self.num_partitions
                self.producer.produce(
                    topic=topic,
                    key=None,
                    value=message_bytes,
                    headers=full_headers,
                    callback=self.ack_callback,
                    #partition=partition
                    )
                self.producer.poll(0) # Non-blocking: process delivery reports after produce()
                delivered = True
                self.msg_sent += 1
                self.bytes_sent += len(message_bytes)
                msg_sent_counter.inc()
                print("[DEBUG] Sending headers:", full_headers)

            except BufferError:
                self.dynamic_target_rate = max(int(self.dynamic_target_rate * 0.9), 10)
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
                send_time = float(headers.get("send_time").decode())
            except Exception as e:
                send_time = time.time()
                print("[WARN] Failed to parse send_time header:", e)

            latency = time.time() - send_time  # time taken from when a message is sent to when it is acknowledged by Kafka
            #print(f"[ACK] Latency={latency:.3f}s | Target rate={self.dynamic_target_rate} msg/sec | In-flight={self.producer.outq_len()} | Msg #{self.msg_sent}")

            #self.total_latency += latency
            #self.latency_count += 1
            print("[DEBUG] Received headers:", headers)

            #queue_size = self.producer.len()
            #delivery_latency_histogram.observe(latency)
            if self.msg_sent % 100 == 0:
                print(f"[INFO] Delivered to {msg.topic()} | Latency={latency:.3f}s ") # | Target rate={self.dynamic_target_rate}")

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
                    ("producer_timestamp", str(now).encode()),
                    ("producer_pod_name", self.pod_name.encode())
                ]
                self.send_message(topic, idx, headers)
                idx += 1
                time_per_msg = 1 / self.dynamic_target_rate
                if shutdown_event.wait(timeout=time_per_msg):
                    break
        except KeyboardInterrupt:
            self.stop()

    def stop(self):
        self.running = False
        self.producer.flush()
        print("[INFO] Producer stopped cleanly.")

def start_metrics_server():
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
    parser.add_argument('--delay', type=float, required=True)
    parser.add_argument('--numPartitions', type=int, required=True)
    parser.add_argument('--replica', type=int, required=True)
    parser.add_argument('--randomRange', type=int, required=True)
    parser.add_argument('--lingerMs', type=int, required=True)
    parser.add_argument('--compressionType', type=str, required=True)
    parser.add_argument('--batchSize', type=int, required=True)
    parser.add_argument('--msgMaxBytes', type=int, required=True)
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
    #threading.Thread(target=start_metrics_server, daemon=True).start()

    try:
        producer_instance.start_synthetic_stream(args.topicTitle, args.numTopics, args.delay, args.randomRange)
    except KeyboardInterrupt:
        print("[INFO] KeyboardInterrupt caught. Shutting down...")
        shutdown_event.set()
        producer_instance.stop()
        sys.exit(0)

