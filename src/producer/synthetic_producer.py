import json
import random
import time
import logging
import re
import argparse
from kafka import KafkaProducer


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
    def __init__(self):
        self.hlpr = Tools()
        self.serializer = Serializer()

        self.producer_bootstrap_servers = [
           "pip-kafka-controller-0.pip-kafka-controller-headless.kafkastreamingdata.svc.cluster.local:9092",
           "pip-kafka-controller-1.pip-kafka-controller-headless.kafkastreamingdata.svc.cluster.local:9092",
           "pip-kafka-controller-2.pip-kafka-controller-headless.kafkastreamingdata.svc.cluster.local:9092"
        ]

        config = self.hlpr.read_config('producer.properties')
        sasl_config = config.get('sasl.jaas.config', '')
        username_match = re.search(r'username="([^"]+)"', sasl_config)
        password_match = re.search(r'password="([^"]+)"', sasl_config)

        username = username_match.group(1) if username_match else ''
        password = password_match.group(1) if password_match else ''

        producer_config_args = {
            'security_protocol': config.get('security.protocol', 'PLAINTEXT'),
            'sasl_mechanism': config.get('sasl.mechanism', 'PLAIN'),
            'sasl_plain_username': username,
            'sasl_plain_password': password,
            'value_serializer': self.serializer.str_serializer,
            'acks': 1,
            'linger_ms': 100,
            'compression_type': 'lz4',
            'batch_size': 16384,
        }

        try:
            self.producer = KafkaProducer(bootstrap_servers=self.producer_bootstrap_servers, **producer_config_args)
            print("Kafka Producer is running.")
        except Exception as ex:
            logging.error('Exception while creating Kafka Producer: ' + str(ex))
            print('Exception while creating Kafka Producer')
            print(str(ex))

    def send_message_no_flush(self, topic, message, headers=None):

        try:
            print("Sending message...")
            future = self.producer.send(topic, value=message, headers=headers or [])
            print("Waiting for Kafka to acknowledge...")
            future.get(timeout=10)  # will raise if Kafka doesn't respond
            print(f"Sent message to topic '{topic}'")
        except Exception as e:
            print(f"Error sending message: {e}")

    def send_message(self, topic, message, headers=None):
        try:
            self.producer.send(topic, value=message, headers=headers or [])
            self.producer.flush()
            print(f"Sent message to topic '{topic}'")
        except Exception as e:
            print(f"Error sending message: {e}")

    def start_synthetic_stream(self, num_topics=10, delay=0.5, column_range=500000):
        index = 0
        while True:
            topic = f"synthetic_{index % num_topics}"
            values = [random.randint(0, column_range) for _ in range(random.randint(3, 6))]
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

            print(f"Sending index {index} to topic {topic}")
            self.send_message_no_flush(topic, message_bytes, headers=headers)
            index += 1
            time.sleep(delay)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Synthetic Kafka Data Producer")
    parser.add_argument('--topics', type=int, default=10, help='Number of Kafka topics to use')
    parser.add_argument('--delay', type=float, default=0.5, help='Delay between messages in seconds')
    args = parser.parse_args()

    producer = Producer()
    producer.start_synthetic_stream(num_topics=args.topics, delay=args.delay)

