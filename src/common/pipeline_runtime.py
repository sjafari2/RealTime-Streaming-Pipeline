"""Shared run timing and small durable records used by both applications."""
import fcntl
import hashlib
import importlib.metadata
import sys
import json
import math
import os
from pathlib import Path
import queue
import signal
import socket
import threading
import time
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


def atomic_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + '.' + uuid.uuid4().hex + '.tmp')
    with temporary.open('w') as stream:
        json.dump(value, stream, allow_nan=False)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)


def latency_seconds(produced, completed):
    value = completed - produced
    return value if math.isfinite(value) and value >= 0 else None


def valid_lag(low, high, position):
    # A negative/sentinel offset or expired progress is not an empty queue.
    if min(low, high, position) < 0 or not low <= position <= high:
        return None
    return high - position


def seed_for(*parts):
    return int.from_bytes(hashlib.sha256('|'.join(map(str, parts)).encode()).digest()[:8], 'big')


class EvidenceWriter:
    """Write outcomes off the processing thread; failed collection invalidates a run."""
    def __init__(self, path, limit=100000):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.queue = queue.Queue(maxsize=limit)
        self.dropped = 0
        self.error = None
        self.stopping = threading.Event()
        # Open before starting work so an unwritable path fails immediately.
        self.stream = self.path.open('x', buffering=256 * 1024)
        self.thread = threading.Thread(target=self._write, daemon=True)
        self.thread.start()

    def put(self, row):
        try:
            self.queue.put_nowait(row)
        except queue.Full:
            self.dropped += 1

    def _write(self):
        try:
            last_flush = time.monotonic()
            while not self.stopping.is_set() or not self.queue.empty():
                try:
                    row = self.queue.get(timeout=0.1)
                except queue.Empty:
                    row = None
                if row is not None:
                    self.stream.write(json.dumps(row, allow_nan=False, separators=(',', ':')) + '\n')
                if time.monotonic() - last_flush >= 1:
                    self.stream.flush()
                    last_flush = time.monotonic()
            self.stream.flush()
            os.fsync(self.stream.fileno())
        except Exception as exc:
            self.error = str(exc)
        finally:
            self.stream.close()

    def close(self):
        self.stopping.set()
        self.thread.join(timeout=30)
        if self.thread.is_alive():
            self.error = 'Evidence writer did not finish within 30 seconds'


class Runtime:
    def __init__(self, role):
        self.role = role
        self.run_id = os.environ['RUN_ID']
        self.pod = socket.gethostname()
        self.incarnation = uuid.uuid4().hex
        self.stop_event = threading.Event()
        self.phase = 'starting'
        self.failure = None
        self.details = {}
        self.started = time.time()
        self.started_monotonic = time.monotonic()
        self.control_path = os.getenv('PIPELINE_RUN_CONTROL', '')
        self.control_cache = None
        self.control_read_at = 0
        self.config_hash = os.getenv('PIPELINE_CONFIG_SHA256', '')
        # This lock is local to a pod, even though the Python code is shared.
        self.lock = open('/tmp/pipeline-' + role + '.lock', 'a+')
        fcntl.flock(self.lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        self.lock.seek(0)
        self.lock.truncate()
        self.lock.write(str(os.getpid()))
        self.lock.flush()
        default_root = '/app/consumer-merge-data/evidence' if role == 'consumer' else '/app/producer-data/evidence'
        self.directory = Path(os.getenv(role.upper() + '_EVIDENCE_DIR', default_root)) / self.run_id / self.pod / self.incarnation
        self.directory.mkdir(parents=True, exist_ok=False)
        self.writer = EvidenceWriter(self.directory / 'events.jsonl', int(os.getenv('EVIDENCE_MAX_BUFFER', '100000')))
        self.outcomes = os.getenv('OUTCOME_LOG_ENABLED', 'true').lower() == 'true'
        versions = {}
        for package in ('confluent-kafka', 'prometheus-client', 'psutil', 'PyYAML'):
            try:
                versions[package] = importlib.metadata.version(package)
            except importlib.metadata.PackageNotFoundError:
                versions[package] = 'unavailable'
        self.event('started', python_version=sys.version, packages=versions, config_sha256=self.config_hash, pid=os.getpid(), outcomes_enabled=self.outcomes,
                   source_sha256={p.name: hashlib.sha256(p.read_bytes()).hexdigest()
                                  for p in Path(__file__).parent.glob('*.py')})
        for sig in (signal.SIGINT, signal.SIGTERM):
            signal.signal(sig, lambda *_: self.stop_event.set())

    def event(self, event, **fields):
        self.writer.put(dict(event=event, timestamp=time.time(), **fields))

    def outcome(self, event, **fields):
        if self.outcomes:
            self.event(event, **fields)

    def control(self):
        if not self.control_path:
            return {'run_id': self.run_id, 'state': 'running', 'start_epoch': self.started,
                    'producer_end_epoch': self.started + float(os.getenv('EXP_DURATION_SEC', '300')),
                    'drain_end_epoch': self.started + float(os.getenv('EXP_DURATION_SEC', '300')) + float(os.getenv('DRAIN_SECONDS', '60'))}
        if time.monotonic() - self.control_read_at > 0.2:
            with open(self.control_path) as stream:
                value = json.load(stream)
            if value['run_id'] != self.run_id:
                self.stop_event.set()
            self.control_cache = value
            self.control_read_at = time.monotonic()
        return self.control_cache

    def should_stop(self):
        control = self.control()
        if control['state'] in ('stopped', 'failed') or time.time() >= control.get('expires_epoch', float('inf')):
            self.stop_event.set()
        return self.stop_event.is_set()

    def production_open(self):
        control = self.control()
        return (control['state'] == 'running' and
                control['start_epoch'] <= time.time() < control['producer_end_epoch'] and
                not self.stop_event.is_set())

    def consumer_finished(self):
        control = self.control()
        return control['state'] == 'running' and time.time() >= control['drain_end_epoch']

    def status(self):
        return dict(role=self.role, run_id=self.run_id, pod=self.pod, incarnation=self.incarnation,
                    config_sha256=self.config_hash, phase=self.phase, timestamp=time.time(),
                    failure=self.failure, evidence_dropped=self.writer.dropped,
                    evidence_error=self.writer.error, **self.details)

    def start_http(self, port, metrics):
        runtime = self
        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                if self.path == '/metrics':
                    data, content = metrics(), 'text/plain; version=0.0.4'
                elif self.path in ('/', '/status'):
                    data, content = json.dumps(runtime.status(), allow_nan=False).encode(), 'application/json'
                else:
                    self.send_error(404)
                    return
                self.send_response(200)
                self.send_header('Content-Type', content)
                self.send_header('Content-Length', str(len(data)))
                self.end_headers()
                self.wfile.write(data)
            def log_message(self, *_):
                pass
        self.server = ThreadingHTTPServer(('0.0.0.0', port), Handler)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def finish(self, metrics):
        self.phase = 'failed' if self.failure else 'finished'
        self.event('finished', failure=self.failure, lifetime_seconds=time.monotonic()-self.started_monotonic)
        self.writer.close()
        if self.writer.error or self.writer.dropped:
            self.failure = self.failure or 'Incomplete evidence collection'
            self.phase = 'failed'
        (self.directory / 'final.prom').write_bytes(metrics())
        atomic_json(self.directory / 'final.json', self.status())
        atomic_json('/tmp/pipeline-' + self.role + '-final.json', self.status())
        # Keep final counters available for several scrapes, including during scale-down.
        time.sleep(float(os.getenv('FINAL_SCRAPE_SECONDS', '15')))
        if hasattr(self, 'server'):
            self.server.shutdown()
        self.lock.close()
