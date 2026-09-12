import importlib.util
import json
import math
from pathlib import Path
import random
import sys
import threading
from types import SimpleNamespace
import uuid

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src/common'))
sys.path.insert(0, str(ROOT / 'my-shell'))
from pipeline_runtime import EvidenceWriter, Runtime, latency_seconds, valid_lag
from run_experiment import validate_readiness


def load(name, relative):
    spec = importlib.util.spec_from_file_location(name, ROOT / relative)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module

consumer = load('measurement_consumer', 'src/consumer/consumer.py')
producer = load('measurement_producer', 'src/producer/producer.py')
evaluator = load('evaluate_run', 'python-scripts/evaluate_run.py')


class FakeRuntime:
    def __init__(self, role):
        self.role = role
        self.pod = 'test-' + role
        self.incarnation = uuid.uuid4().hex
        self.run_id = 'run-test'
        self.details = {}
        self.rows = []
        self.phase = 'starting'
        self.stop_event = threading.Event()
        self.failure = None
        self.started = 100
        self.writer = SimpleNamespace(dropped=0)
    def event(self, event, **fields):
        self.rows.append(dict(event=event, **fields))
    outcome = event
    def start_http(self, *args):
        pass
    def finish(self, metrics):
        self.finished = True


class FakeConsumer:
    def __init__(self, config):
        self.config = config
        self.commits = []
        self.stores = []
        self.owned = []
    def subscribe(self, topics, **callbacks):
        self.callbacks = callbacks
        callbacks['on_assign'](self, [consumer.TopicPartition(topics[0], 0)])
    def committed(self, partitions, timeout):
        return [consumer.TopicPartition(p.topic, p.partition, 0) for p in partitions]
    def get_watermark_offsets(self, tp, timeout):
        return 0, 100
    def incremental_assign(self, partitions):
        self.owned.extend(partitions)
    def incremental_unassign(self, partitions):
        self.owned = []
    def position(self, partitions):
        return [consumer.TopicPartition(p.topic, p.partition, 20) for p in partitions]
    def commit(self, **kwargs):
        self.commits.append(kwargs)
    def store_offsets(self, message):
        self.stores.append(message.offset())
    def close(self):
        pass


class Message:
    def __init__(self, offset=0, headers=None):
        self.number = offset
        self._headers = headers or [('message_id', b'id1'), ('run_id', b'run-test'), ('producer_timestamp', b'100')]
    def headers(self): return self._headers
    def topic(self): return 'topic_0'
    def partition(self): return 0
    def offset(self): return self.number
    def value(self): return b'abc'
    def error(self): return None


@pytest.fixture
def app(monkeypatch):
    for key, value in yaml.safe_load((ROOT / 'src/pipeline-configmap.yaml').read_text())['data'].items():
        monkeypatch.setenv(key, str(value))
    monkeypatch.setenv('RUN_ID', 'run-test')
    monkeypatch.setenv('TOPIC_TITLE', 'topic')
    monkeypatch.setenv('APP_DELAY_MS', '50')
    monkeypatch.setattr(consumer, 'Runtime', FakeRuntime)
    monkeypatch.setattr(consumer, 'Consumer', FakeConsumer)
    return consumer.MetricConsumer()


def test_completion_boundary_and_explicit_progress(app, monkeypatch):
    clock = [100.01]
    def sleep(delay): clock[0] += delay
    monkeypatch.setattr(consumer, 'time', SimpleNamespace(time=lambda: clock[0], monotonic=lambda: clock[0], sleep=sleep))
    app.process_message(Message())
    row = app.runtime.rows[-1]
    assert row['completion_timestamp'] == pytest.approx(100.06)
    assert consumer.latency.labels(**app.labels)._sum.get() == pytest.approx(.06)
    assert consumer.start_latency.labels(**app.labels)._sum.get() == pytest.approx(.01)
    assert app.frontiers[('topic_0', 0)] == 1
    assert app.consumer.config['enable.auto.offset.store'] is False
    app.commit()
    assert app.consumer.commits[-1]['offsets'][0].offset == 1


def test_invalid_record_does_not_advance_progress(app):
    with pytest.raises(KeyError):
        app.process_message(Message(headers=[('run_id', b'run-test')]))
    assert app.frontiers[('topic_0', 0)] == 0
    assert not app.consumer.stores


def test_revoke_removes_old_lag_and_commits_completion(app):
    app.update_lag()
    assert app.observations[('topic_0', 0)]['lag'] == 80
    app.frontiers[('topic_0', 0)] = 7
    app.on_revoke(app.consumer, list(app.assignments.values()))
    assert app.consumer.commits[-1]['offsets'][0].offset == 7
    assert not app.assignments and not app.observations
    assert not any(s.labels.get('incarnation') == app.labels['incarnation'] for m in consumer.lag_metric.collect() for s in m.samples)


def test_lost_owner_never_commits(app):
    app.on_lost(app.consumer, list(app.assignments.values()))
    assert not app.consumer.commits


def test_query_failure_is_invalid_not_zero(app, monkeypatch):
    monkeypatch.setattr(app.consumer, 'get_watermark_offsets', lambda *a, **k: (_ for _ in ()).throw(TimeoutError()))
    app.update_lag()
    assert math.isnan(consumer.total_lag.labels(**app.labels)._value.get())
    assert consumer.valid_metric.labels(**app.partition_labels(('topic_0', 0)))._value.get() == 0


def test_readiness_rejects_duplicate_and_missing_owners():
    row = dict(phase='ready', config_sha256='abc', assignments=[['t', 0]])
    assert validate_readiness([row], [('t', 0)], 'abc')
    assert not validate_readiness([row, row], [('t', 0)], 'abc')
    assert not validate_readiness([row], [('t', 0), ('t', 1)], 'abc')
    assert not validate_readiness([row], [('t', 0)], 'different')


def test_runtime_honors_shared_schedule_and_cancellation():
    runtime = Runtime.__new__(Runtime)
    runtime.stop_event = threading.Event()
    runtime.control = lambda: dict(state='stopped')
    assert runtime.should_stop()


def test_invalid_offsets_and_clocks():
    assert valid_lag(0, 10, -1001) is None
    assert valid_lag(5, 10, 3) is None
    assert valid_lag(0, 10, 11) is None
    assert valid_lag(0, 10, 10) == 0
    assert latency_seconds(2, 1) is None
    assert latency_seconds(float('nan'), 1) is None


def test_writer_flush_and_errors_are_visible(tmp_path):
    writer = EvidenceWriter(tmp_path / 'events.jsonl')
    for i in range(200): writer.put({'index': i})
    writer.close()
    assert not writer.error and writer.dropped == 0
    assert len((tmp_path / 'events.jsonl').read_text().splitlines()) == 200
    bad = EvidenceWriter(tmp_path / 'bad.jsonl')
    bad.put({'value': float('nan')})
    bad.close()
    assert bad.error


def test_producer_config_and_reproducible_hot_set():
    with pytest.raises(ValueError):
        producer.producer_config({'BOOTSTRAP_SERVERS': 'unused', 'ACKS': '0'}, 'id')
    config = producer.producer_config({'BOOTSTRAP_SERVERS': 'unused', 'ACKS': 'all', 'LINGER_MS': '5', 'ENABLE_IDEMPOTENCE': 'true'}, 'id')
    assert config['linger.ms'] == 5 and config['enable.idempotence'] is True
    hot = producer.hot_partitions('.2', 60, 'seed')
    assert len(hot) == 12 and hot == producer.hot_partitions('.2', 60, 'seed')
    assert not set(hot).intersection(set(range(60)) - set(hot))


def test_cohort_counts_unfinished_and_duplicate_messages(tmp_path):
    manifest = dict(run_id='r', evaluation_start_epoch=100, producer_end_epoch=110, drain_end_epoch=120,
                    config={'SLO_THRESHOLD_MS': '1000'}, producer_pods=['p'], consumer_pods=['c'])
    (tmp_path / 'manifest.json').write_text(json.dumps(manifest))
    for role, pod in [('producer', 'p'), ('consumer', 'c')]:
        directory = tmp_path / role
        directory.mkdir()
        (directory / 'final.json').write_text(json.dumps(dict(role=role, pod=pod, run_id='r', incarnation='one')))
    events = [dict(event='acknowledged', message_id=f'm{i}', producer_timestamp=101) for i in range(3)]
    (tmp_path / 'producer/events.jsonl').write_text('\n'.join(map(json.dumps, events)))
    completions = [dict(event='completed', message_id='m0', completion_timestamp=101.1, output_sha256='a'),
                   dict(event='completed', message_id='m0', completion_timestamp=101.2, output_sha256='a'),
                   dict(event='completed', message_id='m1', completion_timestamp=103, output_sha256='a')]
    (tmp_path / 'consumer/events.jsonl').write_text('\n'.join(map(json.dumps, completions)))
    summary = evaluator.evaluate(tmp_path)
    assert summary['admitted_evaluation_cohort'] == 3
    assert summary['incomplete_by_drain'] == 1
    assert summary['deadline_misses'] == 2
    assert summary['duplicate_completion_attempts'] == 1
    assert not summary['validity_failures']


def test_supervised_consumer_is_not_launched_a_second_time(monkeypatch):
    import run_experiment
    commands = []
    monkeypatch.setattr(run_experiment, 'remote', lambda *a, **k: b'True\n')
    monkeypatch.setattr(run_experiment, 'kubectl', lambda *a, **k: commands.append(a))
    run_experiment.start_app('consumer', 'consumer-sts-0')
    assert commands == []


def test_terminated_consumer_commits_only_processed_prefix(app, monkeypatch):
    # consume() can return a batch before SIGTERM; the unprocessed suffix must not be committed.
    calls = [0]
    def consume(**kwargs):
        calls[0] += 1
        return [Message(0), Message(1)]
    app.runtime.should_stop = lambda: app.runtime.stop_event.is_set()
    app.runtime.consumer_finished = lambda: False
    app.runtime.control = lambda: dict(state='running', producer_end_epoch=float('inf'))
    monkeypatch.setattr(app.consumer, 'consume', consume, raising=False)
    actual_process = app.process_message
    def process(msg):
        actual_process(msg)
        app.runtime.stop_event.set()
    monkeypatch.setattr(app, 'process_message', process)
    app.run()
    assert calls[0] == 1
    assert app.consumer.commits[-1]['offsets'][0].offset == 1
    assert app.runtime.finished


def test_producer_finally_flushes_when_shared_window_ends(monkeypatch):
    runtime = FakeRuntime('producer')
    runtime.should_stop = lambda: False
    runtime.production_open = lambda: False
    runtime.control = lambda: dict(state='running', producer_end_epoch=0)
    obj = producer.MyProducer.__new__(producer.MyProducer)
    obj.runtime = runtime
    obj.enqueued = obj.resolved = 3
    obj.labels = dict(pod='p', client_id='c', exp_id='e', run_id='r', traffic_mode='balanced', incarnation=uuid.uuid4().hex)
    flushed = []
    obj.producer = SimpleNamespace(flush=lambda timeout: flushed.append(timeout))
    obj.run()
    assert flushed and runtime.finished
    assert runtime.rows[-1]['unresolved'] == 0


@pytest.mark.parametrize('observed, expected_status', [('one', 'complete'), ('different', 'incomplete')])
def test_active_export_preserves_labels_nan_and_checks_incarnations(tmp_path, monkeypatch, observed, expected_status):
    import io
    import urllib.parse
    import run_experiment
    labels = {'__name__': 'consumer_total_lag', 'run_id': 'r', 'pod': 'c',
              'incarnation': observed, 'topic': 't', 'partition': '0'}
    result = {'status': 'success', 'data': {'result': [{'metric': labels, 'values': [[101, 'NaN']]}]}}
    (tmp_path / 'final.json').write_text(json.dumps({'incarnation': 'one'}))
    requests = []
    def response(url, timeout):
        requests.append(urllib.parse.parse_qs(urllib.parse.urlparse(url).query))
        return io.BytesIO(json.dumps(result).encode())
    monkeypatch.setattr(run_experiment.urllib.request, 'urlopen', response)
    control = dict(run_id='r', start_epoch=100, drain_end_epoch=110, config={})
    if expected_status == 'incomplete':
        with pytest.raises(RuntimeError, match='every process incarnation'):
            run_experiment.export_metrics(tmp_path, control)
    else:
        run_experiment.export_metrics(tmp_path, control)
    import csv
    with (tmp_path / 'prometheus.csv').open() as stream:
        rows = list(csv.DictReader(stream))
    assert json.loads(rows[0]['labels_json']) == labels
    assert rows[0]['value'] == 'NaN'
    assert 'run_id="r"' in requests[0]['query'][0]
    assert json.loads((tmp_path / 'export-status.json').read_text())['status'] == expected_status
