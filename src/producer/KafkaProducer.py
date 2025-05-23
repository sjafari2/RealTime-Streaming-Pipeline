# KafkaProducer.py
import itertools
import logging
import pandas as pd
from kafka import KafkaProducer
import helper
import helper2
from functools import partial
import time
import glob
import json
import random

class Producer:
    def __init__(self, serveruri) -> None:
        self.topic_methods = {
            "random": self.random_structured_data_producer
        }
        self.hlpr = helper.Tools()
        self.serializer = helper.Serializer()
        self.producer_bootstrap_servers = [
            "pip-kafka-controller-0.pip-kafka-controller-headless.kafkastreamingdata.svc.cluster.local:9092",
            "pip-kafka-controller-1.pip-kafka-controller-headless.kafkastreamingdata.svc.cluster.local:9092",
            "pip-kafka-controller-2.pip-kafka-controller-headless.kafkastreamingdata.svc.cluster.local:9092"
        ]

        config = self.hlpr.read_config('producer.properties')
        producer_config_args = {
            'security_protocol': config.get('security.protocol', 'PLAINTEXT'),
            'sasl_mechanism': config.get('sasl.mechanism', 'PLAIN'),
            'sasl_plain_username': config.get('sasl.jaas.config').split('username=')[1].split(' ')[0].replace("\"", "").strip(),
            'sasl_plain_password': config.get('sasl.jaas.config').split('password=')[1].replace("\"", "").replace(";", "").strip(),
        }

        additional_args = {
            'value_serializer': self.serializer.str_serializer,
            'acks': 1,
            'linger_ms': 100,
            'compression_type': 'lz4',
            'batch_size': 16384,
            **producer_config_args
        }

        try:
            self.producer = KafkaProducer(bootstrap_servers=self.producer_bootstrap_servers, **additional_args)
            print("Kafka Producer is running")
        except Exception as ex:
            logging.error('Exception while creating Kafka Producer: ' + str(ex))
            print('Exception while creating Kafka Producer')
            print(str(ex))

    def send_message(self, topic, message, headers=None):
        try:
            self.producer.send(topic, value=message, headers=headers or [])
            self.producer.flush()
            print(f"Message successfully sent to topic '{topic}'")
        except Exception as e:
            print(f"Error sending message to topic '{topic}': {e}")

    def close(self):
        self.producer.close()

    def fetch_files(self, pri, pi, input_path):
        file_names = []
        pattern = f"{input_path}/*-pod-{pi}-prod-{pri}.csv"
        matched_files = glob.glob(pattern)
        file_names.extend(matched_files)
        return file_names

    def stream_data(self, wait_time, topicTitle, **kwargs):
       # print("Streaming Data")
        
        if topicTitle == "random":
            print("Streaming Synthatic Data")
            self.topic_methods["random"](topicTitle, **kwargs)
            return
        
      
    def random_structured_data_producer(self, topicTitle, nprod, num_topics, podindex, prodindex, batchsize, column_range, **kwargs):
        print("\U0001F528 Starting synthatic structured data producer")

        index_start = podindex * batchsize * nprod + prodindex * batchsize
        rangehash = lambda x: hash(x) % column_range

        while True:  # ← Add this loop to continuously send data
            for i in range(batchsize):
                msg_index = index_start + i
                hashed_key = rangehash(f"key_{msg_index}")
                values = [rangehash(f"val_{msg_index}_{j}") for j in range(random.randint(2, 6))]

                payload = {
                    "index": msg_index,
                    "timestamp": time.time(),
                    "values": values
                }

                message_json = json.dumps({hashed_key: payload})
                message_bytes = message_json.encode("utf-8")
                size_bytes = len(message_bytes)
                key = hashed_key % num_topics
                topic = f"{topicTitle}_{key}"

                headers = [
                    ('size_bytes', str(size_bytes).encode('utf-8')),
                    ('index', str(msg_index).encode('utf-8'))
                ]

                print(f"\U0001F4E4 Sending message #{msg_index} to {topic} | size={size_bytes} bytes")
                self.send_message(topic, message_bytes, headers=headers)

            index_start += batchsize  # Move forward for the next loop
            time.sleep(1)  # Optional: pause briefly before next batch

