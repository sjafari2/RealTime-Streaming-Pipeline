"""Paced producer using the same shared run schedule as the consumers."""
import os
import random
import time
import hashlib

import psutil
from confluent_kafka import Producer
from prometheus_client import Counter, Gauge, generate_latest
from pipeline_runtime import Runtime, seed_for

LABELS = ['pod', 'client_id', 'exp_id', 'run_id', 'traffic_mode', 'incarnation']
sent = Counter('producer_messages_sent_total', 'Locally enqueued messages', LABELS)
bytes_sent = Counter('producer_bytes_sent_total', 'Locally enqueued payload bytes', LABELS)
acknowledged = Counter('producer_delivery_ok_total', 'Broker acknowledged messages', LABELS)
errors = Counter('producer_delivery_err_total', 'Delivery or enqueue failures', LABELS)
missed = Counter('producer_pacing_skipped_total', 'Scheduled sends skipped when pacing falls behind', LABELS)
unresolved = Gauge('producer_unresolved_messages', 'Enqueued messages still unresolved after flush', LABELS)
partition_count = Counter('producer_partition_messages_total', 'Broker acknowledged records by actual partition', LABELS + ['topic', 'partition'])
rate_metric = Gauge('producer_target_rate', 'Target records per second for this producer', LABELS)
queue_metric = Gauge('producer_outq_len', 'Messages awaiting delivery', LABELS)
cpu = Gauge('producer_cpu_percent', 'Process CPU percent; 100 percent is one core', LABELS)
memory = Gauge('producer_memory_bytes', 'Process resident memory bytes', LABELS)
uptime = Gauge('producer_uptime_seconds', 'Process uptime', LABELS)
losses = Gauge('producer_evidence_dropped_total', 'Outcome records dropped by the writer', LABELS)


def producer_config(env, client_id):
    config = {'bootstrap.servers': env['BOOTSTRAP_SERVERS'], 'client.id': client_id,
              'acks': env.get('ACKS', 'all')}
    if str(config['acks']) == '0':
        raise ValueError('Set ACKS=1 or all: acknowledged-message experiments cannot use ACKS=0')
    integers = {'LINGER_MS': 'linger.ms', 'BATCH_SIZE': 'batch.size', 'RETRIES': 'retries',
                'RETRY_BACKOFF_MS': 'retry.backoff.ms', 'REQUEST_TIMEOUT_MS': 'request.timeout.ms',
                'DELIVERY_TIMEOUT_MS': 'delivery.timeout.ms', 'QUEUE_BUFFERING_MAX_MESSAGES': 'queue.buffering.max.messages',
                'QUEUE_BUFFERING_MAX_KBYTES': 'queue.buffering.max.kbytes',
                'MAX_IN_FLIGHT_REQUEST_PER_CONNECTION': 'max.in.flight.requests.per.connection'}
    for variable, key in integers.items():
        if variable in env:
            config[key] = int(env[variable])
    config['compression.type'] = env.get('COMPRESSION_TYPE', 'none')
    config['enable.idempotence'] = env.get('ENABLE_IDEMPOTENCE', 'false').lower() == 'true'
    return config  # librdkafka also validates incompatible idempotence settings.


def hot_partitions(spec, count, seed):
    if ',' in spec:
        selected = sorted(set(int(p.strip()) for p in spec.split(',')))
    elif spec.isdigit():
        selected = [int(spec)]
    else:
        fraction = float(spec)
        if not 0 < fraction < 1:
            raise ValueError('Hot partition fraction must be between zero and one')
        selected = sorted(random.Random(seed_for(seed, 'hot')).sample(range(count), max(1, round(count * fraction))))
    if not selected or min(selected) < 0 or max(selected) >= count or len(selected) >= count:
        raise ValueError('Choose valid hot partitions and leave at least one cold partition')
    return selected


class MyProducer:
    def __init__(self):
        self.runtime = Runtime('producer')
        self.labels = dict(pod=self.runtime.pod, client_id=os.getenv('PRODUCER_CLIENT_ID', self.runtime.pod),
                           exp_id=os.getenv('EXP_ID', 'B0'), run_id=self.runtime.run_id,
                           traffic_mode=os.getenv('TRAFFIC_MODE', 'balanced'), incarnation=self.runtime.incarnation)
        self.config = producer_config(os.environ, self.labels['client_id'])
        self.producer = Producer(self.config)
        self.rate = float(os.getenv('TARGET_RATE', '500'))
        self.count = int(os.getenv('NUM_PARTITIONS', '60'))
        self.topic_count = int(os.getenv('TOPIC_COUNT', '1'))
        self.max_messages = int(os.getenv('MAX_MESSAGES', os.getenv('PRODUCER_MAX_MESSAGES', '0')))
        self.max_burst = int(os.getenv('MAX_BURST', '2'))
        self.drop_catchup = os.getenv('DROP_CATCHUP', 'true').lower() == 'true'
        self.seed = os.getenv('WORKLOAD_SEED', '1')
        self.rng = random.Random(seed_for(self.seed, self.runtime.pod))
        self.payload = random.Random(seed_for(self.seed, 'payload')).randbytes(int(os.getenv('PAYLOAD_SIZE_BYTES', '100')))
        self.hot = hot_partitions(os.getenv('SKEW_PARTITION', '.2'), self.count, self.seed) if self.labels['traffic_mode'] == 'skew' else []
        self.cold = [p for p in range(self.count) if p not in self.hot]
        self.fraction = float(os.getenv('SKEW_FRACTION', '.8'))
        if self.rate <= 0 or self.count <= 0 or self.topic_count <= 0 or self.max_burst <= 0 or not 0 <= self.fraction <= 1:
            raise ValueError('Invalid producer rate, partition count or skew settings')
        if self.labels['traffic_mode'] not in ('balanced', 'skew'):
            raise ValueError('TRAFFIC_MODE must be balanced or skew')
        self.index = 0
        self.enqueued = 0
        self.resolved = 0
        self.process = psutil.Process()
        self.runtime.event('producer_config', config=self.config, hot_partitions=self.hot, seed=self.seed,
                           payload_sha256=hashlib.sha256(self.payload).hexdigest())
        rate_metric.labels(**self.labels).set(self.rate)
        for metric in (sent, bytes_sent, acknowledged, errors, missed):
            metric.labels(**self.labels).inc(0)
        self.runtime.phase = 'ready'
        self.runtime.start_http(int(os.getenv('PRODUCER_HTTP_PORT', '8001')), generate_latest)

    def delivered(self, error, msg, identity, produced):
        self.resolved += 1
        if error:
            errors.labels(**self.labels).inc()
            self.runtime.outcome('delivery_failed', message_id=identity, producer_timestamp=produced, error=str(error))
        else:
            acknowledged.labels(**self.labels).inc()
            partition_count.labels(**self.labels, topic=msg.topic(), partition=str(msg.partition())).inc()
            self.runtime.outcome('acknowledged', message_id=identity, producer_timestamp=produced,
                                 topic=msg.topic(), partition=msg.partition(), offset=msg.offset())

    def send_one(self):
        produced = time.time()
        identity = f'{self.runtime.run_id}/{self.runtime.pod}/{self.runtime.incarnation}/{self.index}'
        topic = f"{os.environ['TOPIC_TITLE']}_{self.index % self.topic_count}"
        candidates = self.hot if self.hot and self.rng.random() < self.fraction else self.cold
        partition = self.rng.choice(candidates)
        headers = [('message_id', identity.encode()), ('producer_timestamp', str(produced).encode()),
                   ('run_id', self.runtime.run_id.encode()), ('index', str(self.index).encode()),
                   ('producer_pod_name', self.runtime.pod.encode())]
        self.index += 1
        while not self.runtime.should_stop() and self.runtime.production_open():
            try:
                self.producer.produce(topic=topic, value=self.payload, partition=partition, headers=headers,
                                      callback=lambda err, msg: self.delivered(err, msg, identity, produced))
                self.enqueued += 1
                sent.labels(**self.labels).inc()
                bytes_sent.labels(**self.labels).inc(len(self.payload))
                return
            except BufferError:
                self.producer.poll(.05)
            except Exception as exc:
                errors.labels(**self.labels).inc()
                self.runtime.outcome('enqueue_failed', message_id=identity, producer_timestamp=produced, error=str(exc))
                return
        self.runtime.outcome('enqueue_cancelled', message_id=identity, producer_timestamp=produced)

    def run(self):
        try:
            # All producers expose ready status before the coordinator releases the barrier.
            while not self.runtime.should_stop() and not self.runtime.production_open():
                control = self.runtime.control()
                if control['state'] == 'running' and time.time() >= control['producer_end_epoch']:
                    return
                time.sleep(.05)
            next_send = time.monotonic()
            last_system = 0
            self.runtime.phase = 'running'
            while not self.runtime.should_stop() and self.runtime.production_open():
                if self.max_messages > 0 and self.enqueued >= self.max_messages:
                    break
                now = time.monotonic()
                if now < next_send:
                    time.sleep(min(.001, next_send - now))
                    self.producer.poll(0)
                    continue
                due = int((now - next_send) * self.rate) + 1
                if self.drop_catchup and due > 1:
                    missed.labels(**self.labels).inc(due - 1)
                    next_send = now
                    due = 1
                for _ in range(min(due, self.max_burst)):
                    if self.max_messages > 0 and self.enqueued >= self.max_messages:
                        break
                    self.send_one()
                    next_send += 1 / self.rate
                self.producer.poll(0)
                if now - last_system > 2:
                    cpu.labels(**self.labels).set(self.process.cpu_percent())
                    memory.labels(**self.labels).set(self.process.memory_info().rss)
                    uptime.labels(**self.labels).set(time.time() - self.runtime.started)
                    queue_metric.labels(**self.labels).set(len(self.producer))
                    losses.labels(**self.labels).set(self.runtime.writer.dropped)
                    last_system = now
        except Exception as exc:
            self.runtime.failure = str(exc)
            raise
        finally:
            self.runtime.phase = 'flushing'
            try:
                self.producer.flush(float(os.getenv('PRODUCER_FLUSH_SECONDS', '30')))
                pending = self.enqueued - self.resolved
                unresolved.labels(**self.labels).set(pending)
                self.runtime.event('delivery_summary', enqueued=self.enqueued, resolved=self.resolved, unresolved=pending)
                if pending:
                    self.runtime.failure = self.runtime.failure or 'Unresolved producer deliveries after flush'
            except Exception as exc:
                self.runtime.failure = self.runtime.failure or str(exc)
            finally:
                self.runtime.finish(generate_latest)


if __name__ == '__main__':
    MyProducer().run()
