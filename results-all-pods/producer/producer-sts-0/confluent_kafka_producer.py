import json
import time
import random
import argparse
from confluent_kafka import Producer, KafkaException, KafkaError
from confluent_kafka.admin import AdminClient, NewTopic

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
        self.args = args
        self.hlpr = Tools()

        config = self.hlpr.read_config('producer.properties')
        sasl_config = config.get('sasl.jaas.config', '')
        if not sasl_config:
            raise ValueError("Missing 'sasl.jaas.config' in producer.properties")

        username = sasl_config.split('username=')[1].split(' ')[0].strip('"')
        password = sasl_config.split('password=')[1].strip('";')

        self.bootstrap_servers = ",".join([
            "pip-kafka-controller-0.pip-kafka-controller-headless.kafkastreamingdata.svc.cluster.local:9092",
            "pip-kafka-controller-1.pip-kafka-controller-headless.kafkastreamingdata.svc.cluster.local:9092",
            "pip-kafka-controller-2.pip-kafka-controller-headless.kafkastreamingdata.svc.cluster.local:9092"
        ])

        self.producer_config = {
            'bootstrap.servers': self.bootstrap_servers,
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
            'request.timeout.ms': 30000,
            'delivery.timeout.ms': 45000,
            'queue.buffering.max.messages': 500000,   # increased buffer size
            'queue.buffering.max.kbytes': 2097152     # ~2GB buffer
        }

        self.producer = Producer(self.producer_config)
        self.admin = AdminClient({
            k: self.producer_config[k] for k in [
                'bootstrap.servers', 'security.protocol', 'sasl.mechanism', 'sasl.username', 'sasl.password'
            ]
        })

        self.create_topics_if_missing(args.topicTitle, args.numTopics, args.numPartitions, args.replica)

    def create_topics_if_missing(self, base_topic, num_topics, num_partitions, replication_factor):
        topics = [NewTopic(f"{base_topic}_{i}", num_partitions, replication_factor) for i in range(num_topics)]
        try:
            fs = self.admin.create_topics(topics)
            for topic, f in fs.items():
                try:
                    f.result()
                    print(f"Topic created: {topic}")
                except KafkaException as e:
                    print(f"Topic already exists for {topic}: {e}")
        except Exception as e:
            print(f"Admin client error: {e}")

    def send_message(self, topic, message, headers=None):
        delivered = False
        while not delivered:
            try:
                self.producer.produce(
                    topic=topic,
                    value=message.encode('utf-8'),
                    headers=headers or [],
                    callback=self.acknowledgment
                )
                self.producer.poll(0)
                delivered = True
            except BufferError as e:
                print("[WARNING] Local producer queue is full, waiting to clear...")
                self.producer.poll(1)  # drain for 1 second to clear buffer
                time.sleep(0.1)
            except KafkaException as e:
                print(f"[ERROR] Send error: {e}")
                break

    def acknowledgment(self, err, msg):
        if err is not None:
            print(f"[ERROR] Delivery failed for record: {err}")
        else:
            print(f"[INFO] Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

    def start_synthetic_stream(self, topic_title, num_topics, delay, random_range):
        index = 0
        try:
            while True:
                topic = f"{topic_title}_{index % num_topics}"
                values = [random.randint(0, random_range) for _ in range(random.randint(3, 6))]
                payload = {
                    "index": index,
                    "timestamp": time.time(),
                    "values": values
                }
                message_json = json.dumps(payload)
                headers = [
                    ("size_bytes", str(len(message_json)).encode('utf-8')),
                    ("index", str(index).encode('utf-8'))
                ]
                self.send_message(topic, message_json, headers=headers)
                index += 1
                time.sleep(delay)
        except KeyboardInterrupt:
            print("[INFO] Stopping producer, flushing remaining messages...")
            self.producer.flush()
            print("[INFO] Producer stopped cleanly.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Synthetic Kafka Data Producer")
    parser.add_argument('--topicTitle', type=str, required=True)
    parser.add_argument('--numTopics', type=int, required=True)
    parser.add_argument('--delay', type=float, required=True)
    parser.add_argument('--numPartitions', type=int, required=True)
    parser.add_argument('--replica', type=int, required=True)
    parser.add_argument('--randomRange', type=int, required=True)
    parser.add_argument('--lingerMs', type=int, required=True)
    parser.add_argument('--compressionType', type=str, required=True)
    parser.add_argument('--batchSize', type=int, required=True)
    parser.add_argument('--maxRequestSize', type=int, required=True)
    parser.add_argument('--acks', type=str, required=True)
    args = parser.parse_args()

    producer = MyProducer(args)
    producer.start_synthetic_stream(
        topic_title=args.topicTitle,
        num_topics=args.numTopics,
        delay=args.delay,
        random_range=args.randomRange
    )

