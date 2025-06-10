import json
import random
import time
import re
import argparse
import socket
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import NoBrokersAvailable, TopicAlreadyExistsError

class Tools:
    def read_config(self, filepath):
        config = {}
        with open(filepath, 'r') as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('#') or '=' not in line:
                    continue
                key, value = line.split('=', 1)
                config[key.strip()] = value.strip()
        return config

class Serializer:
    def str_serializer(self, data):
        if isinstance(data, str):
            return data.encode('utf-8')
        return data

class Producer:
    def __init__(self, args):
        self.hlpr = Tools()
        self.serializer = Serializer()

        self.producer_bootstrap_servers = [
            "pip-kafka-controller-2.pip-kafka-controller-headless.kafkastreamingdata.svc.cluster.local:9092",
            "pip-kafka-controller-1.pip-kafka-controller-headless.kafkastreamingdata.svc.cluster.local:9092",
            "pip-kafka-controller-0.pip-kafka-controller-headless.kafkastreamingdata.svc.cluster.local:9092"
        ]

        for server in self.producer_bootstrap_servers:
            host, port = server.split(":")
            try:
                ip = socket.gethostbyname(host)
                print(f"DNS lookup successful: {host} resolved to {ip}")
                with socket.create_connection((host, int(port)), timeout=5):
                    print(f"Successfully connected to {host}:{port}")
            except Exception as e:
                print(f"Connection error for {host}:{port} — {e}")

        config = self.hlpr.read_config('producer.properties')
        sasl_config = config.get('sasl.jaas.config', '').replace('\\', '')
        username_match = re.search(r'username\s*=\s*\"([^\"]+)\"', sasl_config)
        password_match = re.search(r'password\s*=\s*\"([^\"]+)\"', sasl_config)

        username = username_match.group(1) if username_match else ''
        password = password_match.group(1) if password_match else ''

        self.producer_config_args = {
            'security_protocol': config.get('security.protocol', 'SASL_PLAINTEXT'),
            'sasl_mechanism': config.get('sasl.mechanism', 'PLAIN'),
            'sasl_plain_username': username,
            'sasl_plain_password': password,
            'value_serializer': self.serializer.str_serializer,
            'acks': args.acks,
            'linger_ms': args.lingerMs,
            'compression_type': args.compressionType,
            'batch_size': args.batchSize,
            'max_request_size': args.maxRequestSize
        }

        self.producer = None
        while self.producer is None:
            try:
                self.producer = KafkaProducer(bootstrap_servers=self.producer_bootstrap_servers, **self.producer_config_args)
                print("Kafka Producer connected.")
            except Exception as ex:
                print('Kafka Producer init error:', str(ex))
                time.sleep(5)

    def send_message_no_flush(self, topic, message, headers=None):
        try:
            future = self.producer.send(topic, value=message, headers=headers or [])
            if self.producer_config_args['acks'] != '0':
                future.get(timeout=10)
            print(f"Sent message to topic '{topic}'")
        except Exception as e:
            print(f"Send error: {e}")

    def start_synthetic_stream(self, topic_title, num_topics, delay, random_range):
        index = 0
        while True:
            topic = f"{topic_title}_{index % num_topics}"
            values = [random.randint(0, random_range) for _ in range(random.randint(3, 6))]
            payload = {
                "index": index,
                "timestamp": time.time(),
                "values": values
            }

            message_json = json.dumps(payload)
            message_bytes = message_json.encode('utf-8')
            headers = [
                ('size_bytes', str(len(message_bytes)).encode('utf-8')),
                ('index', str(index).encode('utf-8'))
            ]

            self.send_message_no_flush(topic, message_bytes, headers=headers)
            index += 1
            time.sleep(delay)

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

    producer = Producer(args)
    producer.start_synthetic_stream(
        topic_title=args.topicTitle,
        num_topics=args.numTopics,
        delay=args.delay,
        random_range=args.randomRange
    )

