"""Sequential Kafka consumer with completion metrics and minimal durable evidence."""
import math
import os
import time
import hashlib
import threading
from functools import wraps
from collections import deque

import psutil
from confluent_kafka import Consumer, TopicPartition, KafkaError, KafkaException
from prometheus_client import Counter, Gauge, Histogram, generate_latest
from pipeline_runtime import Runtime, latency_seconds, valid_lag

LABELS = ['pod', 'group', 'client_id', 'exp_id', 'run_id', 'traffic_mode', 'incarnation']
PARTITION_LABELS = LABELS + ['topic', 'partition']
BUCKETS = [.001, .005, .01, .02, .05, .075, .099, .15, .2, .3, .5, 1, 2, 5, 10, 30, 60, 120, 300, 900]

received = Counter('consumer_received_total', 'Records returned for processing', LABELS)
completed = Counter('consumer_messages_consumed_total', 'Successfully completed processing attempts', LABELS)
completed_partition = Counter('consumer_partition_completed_total', 'Completed attempts per partition', PARTITION_LABELS)
bytes_done = Counter('consumer_bytes_consumed_total', 'Bytes in completed attempts', LABELS)
latency = Histogram('consumer_e2e_latency_seconds', 'Producer enqueue timestamp to processing completion, seconds', LABELS, buckets=BUCKETS)
start_latency = Histogram('consumer_processing_start_latency_seconds', 'Producer enqueue timestamp to processing start, seconds', LABELS, buckets=BUCKETS)
service_time = Histogram('consumer_processing_seconds', 'Local processing duration measured with a monotonic clock', LABELS, buckets=BUCKETS)
violations = Counter('consumer_slo_violations_total', 'Valid completion observations above the deadline', LABELS)
invalid_latency = Counter('consumer_invalid_latency_total', 'Invalid or negative completion latency observations', LABELS)
failures = Counter('consumer_processing_failures_total', 'Records that did not complete successfully', LABELS)
commit_failures = Counter('consumer_commit_failures_total', 'Failed offset commits, including group transitions', LABELS)
commit_transitions = Counter('consumer_commit_rebalance_errors_total', 'Commit errors caused by group membership transitions', LABELS)
rebalance = Counter('consumer_rebalances_total', 'Ownership callback events', LABELS + ['event_type'])
freshness_metric = Gauge('consumer_lag_freshness_seconds', 'Maximum accepted offset observation age', LABELS)
expected_metric = Gauge('consumer_expected_partitions', 'Expected partitions in this run', LABELS)
assigned_metric = Gauge('consumer_assigned_partitions', 'Current complete assignment size', LABELS)
lag_errors = Counter('consumer_lag_query_errors_total', 'Invalid or failed offset queries', LABELS + ['reason'])
lag_duration = Histogram('consumer_lag_collection_seconds', 'Time spent collecting lag in one loop', LABELS, buckets=[.001,.005,.01,.02,.05,.1,.2,.5,1,2,5])
lag_metric = Gauge('consumer_lag', 'High offset minus returned-record position; NaN when invalid', PARTITION_LABELS)
backlog_metric = Gauge('consumer_processing_backlog', 'High offset minus sequential completion frontier', PARTITION_LABELS)
observed_metric = Gauge('consumer_lag_observed_timestamp_seconds', 'Time of the last valid offset query', PARTITION_LABELS)
age_metric = Gauge('consumer_lag_observation_age_seconds', 'Monotonic observation age at exporter snapshot', PARTITION_LABELS)
valid_metric = Gauge('consumer_lag_valid', 'One when the partition observation is valid and fresh', PARTITION_LABELS)
high_metric = Gauge('consumer_high_offset', 'Queried high offset', PARTITION_LABELS)
position_metric = Gauge('consumer_position_offset', 'Next offset after returned records', PARTITION_LABELS)
frontier_metric = Gauge('consumer_completion_offset', 'Next offset after completed sequential work', PARTITION_LABELS)
owner_metric = Gauge('consumer_partition_owned', 'Current partition ownership', PARTITION_LABELS)
total_lag = Gauge('consumer_total_lag', 'Sum over complete fresh local partition observations', LABELS)
max_lag = Gauge('consumer_max_lag', 'Maximum valid local partition lag', LABELS)
cpu = Gauge('consumer_cpu_percent', 'Process CPU percent; 100 percent is one CPU core', LABELS)
memory = Gauge('consumer_memory_bytes', 'Process resident memory in bytes', LABELS)
uptime = Gauge('consumer_uptime_seconds', 'Process uptime', LABELS)
losses = Gauge('consumer_evidence_dropped_total', 'Outcome records dropped by the bounded writer', LABELS)
PARTITION_GAUGES = [lag_metric, backlog_metric, observed_metric, age_metric, valid_metric, high_metric, position_metric, frontier_metric, owner_metric]


def synchronized_lag(method):
    @wraps(method)
    def call(self, *args, **kwargs):
        with self.metrics_lock:
            return method(self, *args, **kwargs)
    return call


def membership_settings(pod, environment=None):
    """The shared setting enables membership; each pod supplies its own identity."""
    environment = os.environ if environment is None else environment
    enabled = environment.get('CONSUMER_STATIC_MEMBERSHIP', 'false').lower()
    if enabled not in ('true', 'false'):
        raise ValueError('CONSUMER_STATIC_MEMBERSHIP must be true or false')
    if enabled == 'false':
        return {}
    if not isinstance(pod, str) or not pod or any(c.isspace() for c in pod):
        raise ValueError('Static membership needs a nonempty, unique pod name')
    return {'group.instance.id': pod}


class MetricConsumer:
    def __init__(self):
        self.runtime = Runtime('consumer')
        self.metrics_lock = threading.RLock()
        self.group = os.environ['CONSUMER_GROUP_ID']
        self.labels = dict(pod=self.runtime.pod, group=self.group,
                           client_id=os.getenv('CONSUMER_CLIENT_ID', self.runtime.pod),
                           exp_id=os.getenv('EXP_ID', 'B0'), run_id=self.runtime.run_id,
                           traffic_mode=os.getenv('TRAFFIC_MODE', 'balanced'), incarnation=self.runtime.incarnation)
        prefix = os.environ['TOPIC_TITLE']
        self.topics = [f'{prefix}_{i}' for i in range(int(os.getenv('TOPIC_COUNT', '1')))]
        self.assignments = {}
        self.frontiers = {}
        self.completed_offsets = {}
        self.observations = {}
        self.query_errors = {}
        self.query_queue = deque()
        self.epoch = 0
        self.process = psutil.Process()
        self.last_system = 0
        self.last_commit = time.monotonic()
        self.commit_interval = float(os.getenv('COMMIT_INTERVAL_SECONDS', '2'))
        self.lag_interval = float(os.getenv('LAG_QUERY_INTERVAL', '2'))
        self.lag_timeout = float(os.getenv('LAG_QUERY_TIMEOUT', '.1'))
        self.lag_budget = float(os.getenv('LAG_QUERY_BUDGET_SEC', '.2'))
        self.freshness = float(os.getenv('LAG_FRESHNESS_SECONDS', '10'))
        self.poll_count = int(os.getenv('POLL_MAX_MSG', '25'))
        self.poll_timeout = float(os.getenv('POLL_TIMEOUT', '.1'))
        self.delay = float(os.getenv('APP_DELAY_MS', '0')) / 1000
        self.cpu_iterations = int(os.getenv('APP_CPU_ITERATIONS', '0'))
        self.deadline = float(os.getenv('SLO_THRESHOLD_MS', '99')) / 1000
        if min(self.lag_timeout, self.lag_budget, self.freshness, self.commit_interval) <= 0:
            raise ValueError('Lag and commit timing settings must be positive')
        if self.delay < 0 or self.cpu_iterations < 0 or self.poll_count <= 0:
            raise ValueError('Invalid processing or poll configuration')
        if os.getenv('ENABLE_AUTO_COMMIT', 'false').lower() == 'true':
            raise ValueError('This consumer uses completion-based manual commits; set ENABLE_AUTO_COMMIT=false')
        self.config = {
            'bootstrap.servers': os.environ['BOOTSTRAP_SERVERS'], 'group.id': self.group,
            'client.id': self.labels['client_id'], 'enable.auto.commit': False,
            'enable.auto.offset.store': False, 'group.protocol': 'classic',
            'auto.offset.reset': os.getenv('AUTO_OFFSET_RESET', 'earliest'),
            'partition.assignment.strategy': os.getenv('PARTITION_ASSIGNMENT_STRATEGY', 'cooperative-sticky'),
            'on_commit': self.on_commit,
        }
        self.config.update(membership_settings(self.runtime.pod))
        self.runtime.details['group_instance_id'] = self.config.get('group.instance.id')
        settings = {'FETCH_MAX_BYTES': 'fetch.max.bytes', 'FETCH_MIN_BYTES': 'fetch.min.bytes',
                    'FETCH_MAX_WAIT_MS': 'fetch.wait.max.ms', 'MAX_PARTITION_FETCH_BYTES': 'max.partition.fetch.bytes',
                    'MAX_POLL_INTERVAL_MS': 'max.poll.interval.ms', 'SESSION_TIMEOUT_MS': 'session.timeout.ms',
                    'HEARTBEAT_INTERVAL_MS': 'heartbeat.interval.ms', 'SOCKET_TIMEOUT_MS': 'socket.timeout.ms'}
        for variable, key in settings.items():
            if variable in os.environ:
                self.config[key] = int(os.environ[variable])
        if self.config['partition.assignment.strategy'] != 'cooperative-sticky':
            raise ValueError('The current callbacks require the classic cooperative-sticky assignor')
        freshness_metric.labels(**self.labels).set(self.freshness)
        expected_metric.labels(**self.labels).set(int(os.getenv('NUM_PARTITIONS', '60')) * len(self.topics))
        self.consumer = Consumer(self.config)
        self.runtime.event('consumer_config', config={k: v for k, v in self.config.items() if k != 'on_commit'},
                           task=dict(wait_seconds=self.delay, sha256_iterations=self.cpu_iterations))
        self.consumer.subscribe(self.topics, on_assign=self.on_assign, on_revoke=self.on_revoke, on_lost=self.on_lost)
        self.runtime.phase = 'ready'
        self.runtime.start_http(int(os.getenv('CONSUMER_HTTP_PORT', '8002')), self.metrics_snapshot)

    def metrics_snapshot(self):
        # A scrape must see high, position, backlog, validity and ownership from
        # the same update. Prometheus otherwise reads their families separately.
        with self.metrics_lock:
            self.refresh_lag_validity()
            return generate_latest()

    def partition_labels(self, key):
        return dict(self.labels, topic=key[0], partition=str(key[1]))

    @synchronized_lag
    def on_assign(self, consumer, partitions):
        self.runtime.event('assign_started', partitions=[[p.topic, p.partition] for p in partitions])
        # Resolve the exact starting offsets before a batch can advance position().
        offsets = consumer.committed(partitions, timeout=5)
        for tp, committed in zip(partitions, offsets):
            low, high = consumer.get_watermark_offsets(tp, timeout=5)
            if committed.offset >= 0:
                if not low <= committed.offset <= high:
                    raise RuntimeError('Committed progress is outside retained offsets')
                tp.offset = committed.offset
            else:
                tp.offset = low if self.config['auto.offset.reset'] == 'earliest' else high
            key = (tp.topic, tp.partition)
            self.frontiers[key] = tp.offset
            self.assignments[key] = tp
            self.query_queue.append(key)
            owner_metric.labels(**self.partition_labels(key)).set(1)
            valid_metric.labels(**self.partition_labels(key)).set(0)
        consumer.incremental_assign(partitions)
        self.ownership_event('assign', partitions)

    def ownership_event(self, event, partitions):
        self.epoch += 1
        rebalance.labels(**self.labels, event_type=event).inc()
        assigned_metric.labels(**self.labels).set(len(self.assignments))
        self.runtime.details['assignments'] = [[t, p] for t, p in sorted(self.assignments)]
        self.runtime.details['assignment_epoch'] = self.epoch
        self.runtime.event(event, partitions=[[p.topic, p.partition] for p in partitions], epoch=self.epoch)

    def forget(self, partitions):
        for tp in partitions:
            key = (tp.topic, tp.partition)
            self.assignments.pop(key, None)
            self.frontiers.pop(key, None)
            self.completed_offsets.pop(key, None)
            self.observations.pop(key, None)
            self.query_errors.pop(key, None)
            for metric in PARTITION_GAUGES:
                try:
                    metric.remove(*(self.partition_labels(key)[label] for label in PARTITION_LABELS))
                except KeyError:
                    pass
        self.query_queue = deque(k for k in self.query_queue if k in self.assignments)

    def record_commit_result(self, error, partitions, context):
        errors = ([error] if error else []) + [tp.error for tp in partitions if tp.error]
        offsets = [dict(topic=tp.topic, partition=tp.partition, offset=tp.offset,
                        error=str(tp.error) if tp.error else None) for tp in partitions]
        if not errors:
            self.runtime.event('commit_result', context=context, success=True, offsets=offsets)
            return True
        # A request from an old group generation can fail while ownership changes.
        # Retry only current completed offsets in the normal loop. Revoked/lost
        # partitions are forgotten, so their new owner resumes from Kafka's commit.
        transition = all(isinstance(e, KafkaError) and not e.fatal() and e.code() in
                         (KafkaError.ILLEGAL_GENERATION, KafkaError.UNKNOWN_MEMBER_ID,
                          KafkaError.REBALANCE_IN_PROGRESS) for e in errors)
        commit_failures.labels(**self.labels).inc()
        if transition:
            commit_transitions.labels(**self.labels).inc()
        self.runtime.event('commit_result', context=context, success=False,
                           group_transition=transition, errors=[str(e) for e in errors], offsets=offsets)
        if not transition:
            self.runtime.failure = 'Offset commit failed: ' + '; '.join(str(e) for e in errors)
            self.runtime.stop_event.set()
        return False

    def commit(self, keys=None, asynchronous=False):
        keys = self.assignments if keys is None else keys
        # Assignment frontiers initialize lag even before work begins. They are
        # not new completed work, and must not generate startup commits.
        offsets = [TopicPartition(t, p, self.completed_offsets[(t, p)]) for t, p in keys
                   if (t, p) in self.assignments and (t, p) in self.completed_offsets]
        if not offsets:
            return True
        context = 'asynchronous_request' if asynchronous else 'synchronous_request'
        try:
            result = self.consumer.commit(offsets=offsets, asynchronous=asynchronous)
        except KafkaException as exc:
            self.record_commit_result(exc.args[0], offsets, context)
            if self.runtime.failure:
                raise
            return False
        except Exception as exc:
            self.record_commit_result(exc, offsets, context)
            raise
        if not asynchronous:
            successful = self.record_commit_result(None, result or [], context)
            if self.runtime.failure:
                raise RuntimeError(self.runtime.failure)
            return successful
        return None  # A queued request is not a commit acknowledgment.

    def on_commit(self, error, partitions):
        self.record_commit_result(error, partitions or [], 'asynchronous_callback')

    @synchronized_lag
    def on_revoke(self, consumer, partitions):
        self.runtime.event('revoke_started', partitions=[[p.topic, p.partition] for p in partitions])
        self.commit([(tp.topic, tp.partition) for tp in partitions])
        self.forget(partitions)
        consumer.incremental_unassign(partitions)
        self.ownership_event('revoke', partitions)

    @synchronized_lag
    def on_lost(self, consumer, partitions):
        # A lost owner must not commit offsets for partitions now owned elsewhere.
        self.forget(partitions)
        consumer.incremental_unassign(partitions)
        self.ownership_event('lost', partitions)

    @synchronized_lag
    def update_lag(self):
        # Rotate bounded queries through the assignment instead of blocking on every partition.
        started = time.monotonic()
        deadline = started + self.lag_budget
        for _ in range(len(self.query_queue)):
            key = self.query_queue.popleft()
            self.query_queue.append(key)
            previous = self.observations.get(key)
            if previous and time.monotonic() - previous['monotonic'] < self.lag_interval:
                continue
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            try:
                tp = self.assignments[key]
                position = self.consumer.position([tp])[0].offset
                low, high = self.consumer.get_watermark_offsets(tp, timeout=min(self.lag_timeout, remaining))
                lag = valid_lag(low, high, position)
                unfinished = valid_lag(low, high, self.frontiers[key])
                if lag is None or unfinished is None:
                    raise ValueError('Invalid offset observation')
                labels = self.partition_labels(key)
                self.observations[key] = dict(timestamp=time.time(), monotonic=time.monotonic(), lag=lag, backlog=unfinished)
                self.query_errors.pop(key, None)
                for metric, value in [(lag_metric, lag), (backlog_metric, unfinished), (high_metric, high),
                                      (position_metric, position), (frontier_metric, self.frontiers[key]),
                                      (observed_metric, self.observations[key]['timestamp'])]:
                    metric.labels(**labels).set(value)
            except Exception as exc:
                self.observations.pop(key, None)
                reason = 'invalid_offset' if isinstance(exc, ValueError) else 'query_failed'
                lag_errors.labels(**self.labels, reason=reason).inc()
                if self.query_errors.get(key) != reason:
                    self.runtime.event('lag_invalid', topic=key[0], partition=key[1], reason=reason, error=str(exc))
                self.query_errors[key] = reason
        lag_duration.labels(**self.labels).observe(time.monotonic() - started)
        self.refresh_lag_validity()

    def refresh_lag_validity(self):
        # Called while holding metrics_lock. Age advances even if the main loop
        # stops updating offsets; a successful HTTP scrape alone is not freshness.
        now = time.monotonic()
        values = []
        for key in self.assignments:
            row = self.observations.get(key)
            age = now - row['monotonic'] if row is not None else math.nan
            valid = math.isfinite(age) and 0 <= age <= self.freshness
            labels = self.partition_labels(key)
            age_metric.labels(**labels).set(age)
            valid_metric.labels(**labels).set(int(valid))
            if valid:
                values.append(row['lag'])
            else:
                lag_metric.labels(**labels).set(math.nan)
                backlog_metric.labels(**labels).set(math.nan)
        complete = len(values) == len(self.assignments)
        total_lag.labels(**self.labels).set(sum(values) if complete else math.nan)
        max_lag.labels(**self.labels).set(max(values, default=0) if complete else math.nan)
        self.runtime.details['lag_valid'] = complete

    def process_message(self, msg):
        received.labels(**self.labels).inc()
        headers = dict(msg.headers() or [])
        def header(name):
            value = headers[name]
            return value.decode() if isinstance(value, bytes) else str(value)
        # Missing metadata is a failed record, not something silently committed past.
        message_id = header('message_id')
        if header('run_id') != self.runtime.run_id:
            raise ValueError('Record belongs to another run')
        produced = float(header('producer_timestamp'))
        start = time.time()
        monotonic_start = time.monotonic()
        result = msg.value() or b''
        for _ in range(self.cpu_iterations):
            result = hashlib.sha256(result).digest()
        if self.delay:
            time.sleep(self.delay)
        processing_seconds = time.monotonic() - monotonic_start
        finished = time.time()
        measured = latency_seconds(produced, finished)
        initial = latency_seconds(produced, start)
        key = (msg.topic(), msg.partition())
        self.runtime.outcome('completed', message_id=message_id, producer_timestamp=produced if math.isfinite(produced) else None,
                             completion_timestamp=finished, processing_start_timestamp=start, processing_seconds=processing_seconds, topic=key[0], partition=key[1], offset=msg.offset(),
                             output_sha256=hashlib.sha256(result).hexdigest(), assignment_epoch=self.epoch)
        # Processing is sequential, so offset+1 is safe only after this record succeeds.
        self.frontiers[key] = msg.offset() + 1
        self.completed_offsets[key] = msg.offset() + 1
        self.consumer.store_offsets(message=msg)
        completed.labels(**self.labels).inc()
        completed_partition.labels(**self.partition_labels(key)).inc()
        bytes_done.labels(**self.labels).inc(len(msg.value() or b''))
        service_time.labels(**self.labels).observe(processing_seconds)
        if initial is not None:
            start_latency.labels(**self.labels).observe(initial)
        if measured is None:
            invalid_latency.labels(**self.labels).inc()
        else:
            latency.labels(**self.labels).observe(measured)
            if measured > self.deadline:
                violations.labels(**self.labels).inc()

    def run(self):
        try:
            while not self.runtime.should_stop() and not self.runtime.consumer_finished():
                messages = self.consumer.consume(num_messages=self.poll_count, timeout=self.poll_timeout)
                for msg in messages:
                    if self.runtime.stop_event.is_set() or self.runtime.consumer_finished():
                        break  # Fetched but unfinished records remain eligible for replay.
                    if msg.error():
                        raise RuntimeError(str(msg.error()))
                    try:
                        self.process_message(msg)
                    except Exception as exc:
                        failures.labels(**self.labels).inc()
                        self.runtime.event('processing_failed', topic=msg.topic(), partition=msg.partition(),
                                           offset=msg.offset(), error=str(exc))
                        raise
                control = self.runtime.control()
                if control['state'] == 'running':
                    self.runtime.phase = 'draining' if time.time() >= control['producer_end_epoch'] else 'running'
                self.update_lag()
                if time.monotonic() - self.last_commit >= self.commit_interval:
                    self.commit(asynchronous=True)
                    self.last_commit = time.monotonic()
                if time.monotonic() - self.last_system > 2:
                    cpu.labels(**self.labels).set(self.process.cpu_percent())
                    memory.labels(**self.labels).set(self.process.memory_info().rss)
                    uptime.labels(**self.labels).set(time.time() - self.runtime.started)
                    losses.labels(**self.labels).set(self.runtime.writer.dropped)
                    self.last_system = time.monotonic()
        except Exception as exc:
            self.runtime.failure = str(exc)
            raise
        finally:
            try:
                self.commit()
            except Exception as exc:
                self.runtime.failure = self.runtime.failure or str(exc)
            try:
                self.consumer.close()
            finally:
                self.runtime.finish(self.metrics_snapshot)


if __name__ == '__main__':
    MetricConsumer().run()
