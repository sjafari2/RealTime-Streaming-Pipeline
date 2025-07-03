import json
import time
import random
import argparse
import threading
import psutil
from flask import Flask
from prometheus_flask_exporter import PrometheusMetrics
from prometheus_client import Counter, Gauge
from confluent_kafka import Producer, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic
import socket

app = Flask(__name__)
metrics_exporter = PrometheusMetrics(app, defaults_prefix=None)

msg_sent_counter = Counter('producer_messages_sent_total', 'Total messages sent')
msg_rate_gauge = Gauge('producer_message_rate', 'Message send rate (msg/sec)')
mb_rate_gauge = Gauge('producer_mb_rate', 'Throughput (MB/sec)')
cpu_gauge = Gauge('producer_cpu_percent', 'CPU percent usage')
mem_gauge = Gauge('producer_memory_percent', 'Memory percent usage')
uptime_gauge = Gauge('producer_uptime_seconds', 'Producer uptime in seconds')

class MyProducer:
    def __init__(self, args):
        self.start_time = time.time()
        self.msg_sent = 0
        self.bytes_sent = 0
        self.pod_name = socket.gethostname()

        config = Tools.read_config('producer.properties')
        sasl_config = config.get('sasl.jaas.config', '')
        username = sasl_config.split('username=')[1].split(' ')[0].strip('"')
        password = sasl_config.split('password=')[1].strip('";')

        bootstrap_servers = ",".join([
            "pip-kafka-controller-0.pip-kafka-controller-headless.kafkastreamingdata.svc.cluster.local:9092",
            "pip-kafka-controller-1.pip-kafka-controller-headless.kafkastreamingdata.svc.cluster.local:9092",
            "pip-kafka-controller-2.pip-kafka-controller-headless.kafkastreamingdata.svc.cluster.local:9092"
        ])

        self.producer_config = {
            'bootstrap.servers': bootstrap_servers,
            'security.protocol': config.get('security.protocol', 'SASL_PLAINTEXT'),
            'sasl.mechanism': config.get('sasl.mechanism', 'PLAIN'),
            'sasl.username': username,
            'sasl.password': password,
            'compression.type': args.compressionType,
            'linger.ms': args.lingerMs,
            'batch.size': args.batchSize,
            'message.max.bytes': args.maxRequestSize,
            'acks': args.acks,
            'retries': 10,
            'request.timeout.ms': args.requestTimeoutMs,
            'delivery.timeout.ms': args.deliveryTimeoutMs,
            'queue.buffering.max.messages': args.queueBufferingMaxMessages,
            'queue.buffering.max.kbytes': args.queueBufferingMaxKbytes
        }

        self.producer = Producer(self.producer_config)
        self.admin = AdminClient({k: self.producer_config[k] for k in [
            'bootstrap.servers', 'security.protocol', 'sasl.mechanism', 'sasl.username', 'sasl.password']})
        self.create_topics_if_missing(args.topicTitle, args.numTopics, args.numPartitions, args.replica)

    def create_topics_if_missing(self, base_topic, num_topics, num_partitions, replication_factor):
        topics = [NewTopic(f"{base_topic}_{i}", num_partitions, replication_factor) for i in range(num_topics)]
        fs = self.admin.create_topics(topics)
        for topic, f in fs.items():
            try:
                f.result()
                print(f"[INFO] Created topic {topic}")
            except KafkaException as e:
                print(f"[INFO] Topic {topic} may already exist: {e}")

    def send_message(self, topic, message, headers=None):
        delivered = False
        while not delivered:
            try:
                self.producer.produce(
                    topic=topic,
                    value=message.encode('utf-8'),
                    headers=headers or [],
                    callback=self.ack_callback
                )
                self.producer.poll(0)
                delivered = True
                self.msg_sent += 1
                self.bytes_sent += len(message.encode('utf-8'))

                elapsed = time.time() - self.start_time
                msg_rate = self.msg_sent / elapsed if elapsed > 0 else 0
                mb_rate = (self.bytes_sent / (1024 * 1024)) / elapsed if elapsed > 0 else 0

                msg_sent_counter.inc()
                msg_rate_gauge.set(msg_rate)
                mb_rate_gauge.set(mb_rate)
                cpu_gauge.set(psutil.cpu_percent(interval=None))
                mem_gauge.set(psutil.virtual_memory().percent)
                uptime_gauge.set(elapsed)

            except BufferError:
                print("[WARN] Buffer full, waiting...")
                self.producer.poll(1)
                time.sleep(0.1)
            except KafkaException as e:
                print(f"[ERROR] Send failed: {e}")
                break

    def ack_callback(self, err, msg):
        if err:
            print(f"[ERROR] Delivery failed: {err}")
        else:
            print(f"[INFO] Delivered to {msg.topic()} [{msg.partition()}] offset {msg.offset()}")

    def start_synthetic_stream(self, topic_title, num_topics, delay, random_range):
        idx = 0
        try:
            while True:
                topic = f"{topic_title}_{idx % num_topics}"
                payload = json.dumps({
                    "index": idx,
                    "producer_timestamp": time.time(),
                    "pod_name": self.pod_name,
                    "values": [random.randint(0, random_range) for _ in range(random.randint(3, 6))]
                })
                headers = [
                    ("index", str(idx).encode()),
                    ("producer_timestamp", str(time.time()).encode()),
                    ("size_bytes", str(len(payload)).encode()),
                    ("producer_pod_name", self.pod_name.encode())
                ]
                self.send_message(topic, payload, headers)
                idx += 1
                time.sleep(delay)
        except KeyboardInterrupt:
            print("[INFO] Flushing and stopping producer...")
            self.producer.flush()

def start_metrics_server():
    app.run(host='0.0.0.0', port=8000)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--topicTitle', required=True)
    parser.add_argument('--numTopics', type=int, required=True)
    parser.add_argument('--delay', type=float, required=True)
    parser.add_argument('--numPartitions', type=int, required=True)
    parser.add_argument('--replica', type=int, required=True)
    parser.add_argument('--randomRange', type=int, required=True)
    parser.add_argument('--lingerMs', type=int, required=True)
    parser.add_argument('--compressionType', required=True)
    parser.add_argument('--batchSize', type=int, required=True)
    parser.add_argument('--maxRequestSize', type=int, required=True)
    parser.add_argument('--acks', required=True)
    parser.add_argument('--requestTimeoutMs', type=int, required=True)
    parser.add_argument('--deliveryTimeoutMs', type=int, required=True)
    parser.add_argument('--queueBufferingMaxMessages', type=int, required=True)
    parser.add_argument('--queueBufferingMaxKbytes', type=int, required=True)
    args = parser.parse_args()

    producer_instance = MyProducer(args)
    threading.Thread(target=start_metrics_server, daemon=True).start()
    producer_instance.start_synthetic_stream(args.topicTitle, args.numTopics, args.delay, args.randomRange)

