import json
import time
import random
import argparse
import threading
import psutil
from flask import Flask, jsonify
from confluent_kafka import Producer, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic
import socket

app = Flask(__name__)
metrics = {
    "msg_sent": 0,
    "bytes_sent": 0,
    "start_time": time.time(),
    "batch_size": 0,
    "linger_ms": 0,
    "acks": "",
    "cpu_percent": 0,
    "mem_percent": 0,
}

class Tools:
    @staticmethod
    def read_config(filepath):
        config = {}
        with open(filepath, 'r') as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('#') or '=' not in line:
                    continue
                key, value = line.split('=', 1)
                config[key.strip()] = value.strip()
        return config

class MyProducer:
    def __init__(self, args):
        config = Tools.read_config('producer.properties')
        sasl_config = config.get('sasl.jaas.config', '')
        username = sasl_config.split('username=')[1].split(' ')[0].strip('"')
        password = sasl_config.split('password=')[1].strip('";')

        bootstrap_servers = ",".join([
            "pip-kafka-controller-0.pip-kafka-controller-headless.kafkastreamingdata.svc.cluster.local:9092",
            "pip-kafka-controller-1.pip-kafka-controller-headless.kafkastreamingdata.svc.cluster.local:9092",
            "pip-kafka-controller-2.pip-kafka-controller-headless.kafkastreamingdata.svc.cluster.local:9092"
        ])

        self.pod_name = socket.gethostname()

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

        metrics["batch_size"] = args.batchSize
        metrics["linger_ms"] = args.lingerMs
        metrics["acks"] = args.acks

        self.producer = Producer(self.producer_config)
        self.admin = AdminClient({k: self.producer_config[k] for k in [
            'bootstrap.servers', 'security.protocol', 'sasl.mechanism', 'sasl.username', 'sasl.password'
        ]})

        self.create_topics_if_missing(args.topicTitle, args.numTopics, args.numPartitions, args.replica)
        self.msg_sent = 0
        self.bytes_sent = 0

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

    def get_metrics(self):
        elapsed = time.time() - metrics["start_time"]
        msg_rate = self.msg_sent / elapsed if elapsed > 0 else 0
        mb_rate = (self.bytes_sent / (1024*1024)) / elapsed if elapsed > 0 else 0
        metrics.update({
            "msg_rate": msg_rate,
            "mb_rate": mb_rate,
            "cpu_percent": psutil.cpu_percent(interval=1),
            "mem_percent": psutil.virtual_memory().percent,
            "uptime_sec": elapsed,
            "msg_sent": self.msg_sent,
            "bytes_sent": self.bytes_sent
        })
        return metrics

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

producer_instance = None

@app.route('/metrics', methods=['GET'])
def metrics_endpoint():
    return jsonify(producer_instance.get_metrics()) if producer_instance else jsonify({"error": "Producer not initialized"})

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

