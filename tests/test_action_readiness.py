"""Action imports must finish before production, including on a fresh process."""
import importlib.util
from pathlib import Path
import sys
import types

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'my-shell'))
spec = importlib.util.spec_from_file_location('runner_readiness', ROOT / 'my-shell/run_experiment.py')
r = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = r
spec.loader.exec_module(r)


def test_unavailable_action_code_cannot_release_workload(monkeypatch):
    started = []
    monkeypatch.setattr(r, 'read_control', lambda: None)
    monkeypatch.setattr(r, 'start', lambda *args: started.append(True))
    def unavailable(name):
        raise ImportError('Action source unavailable')
    monkeypatch.setattr(r.importlib, 'import_module', unavailable)
    with pytest.raises(ImportError, match='unavailable'):
        r.complete_run({'action': 'redistribute'})
    assert started == []


def test_module_preparation_precedes_start_and_has_verifiable_source(monkeypatch, tmp_path):
    events = []
    source = tmp_path / 'action.py'
    source.write_text('# Test action module\n')
    module = types.SimpleNamespace(__file__=str(source), transfer=lambda: None)
    def load(name):
        events.append(('import', name))
        return module
    class BeforeTraffic(Exception):
        pass
    def start(plan):
        assert events == [('import', 'explicit_control')]
        raise BeforeTraffic()
    monkeypatch.setattr(r, 'read_control', lambda: None)
    monkeypatch.setattr(r.importlib, 'import_module', load)
    monkeypatch.setattr(r, 'start', start)
    with pytest.raises(BeforeTraffic):
        r.complete_run({'action': 'redistribute'})
    record = r.prepare_action_code({'action': 'redistribute'})
    assert record['source_sha256'] == r.hashlib.sha256(source.read_bytes()).hexdigest()
    assert record['timestamp'] >= record['preparation_started_epoch']
    assert record['preparation_seconds'] >= 0


def test_builtin_actions_need_no_extra_imports(monkeypatch):
    def unexpected(name):
        pytest.fail('Unexpected external action module: ' + name)
    monkeypatch.setattr(r.importlib, 'import_module', unexpected)
    assert r.prepare_action_code(None) is None
    assert r.prepare_action_code({'action': 'none'}) is None
    assert r.prepare_action_code({'action': 'scale'}) is None


def test_source_checks_return_exact_file_hashes(tmp_path):
    source = tmp_path / 'worker.py'
    source.write_bytes(b'print("source check")\n')
    assert r.local_source_hashes([source]) == {
        source.name: r.hashlib.sha256(source.read_bytes()).hexdigest()}


def test_source_read_timeout_retries_with_fresh_reader(monkeypatch, tmp_path):
    calls = []
    def reader(argv, **kwargs):
        calls.append((argv, kwargs))
        if len(calls) == 1:
            raise r.subprocess.TimeoutExpired(argv, kwargs['timeout'])
        return types.SimpleNamespace(stdout=b'{"worker.py": "verified-hash"}')
    monkeypatch.setattr(r.subprocess, 'run', reader)
    assert r.local_source_hashes([tmp_path/'worker.py'], timeout=2) == {'worker.py':'verified-hash'}
    assert len(calls) == 2
    assert all(call[1]['timeout'] == 2 for call in calls)


def test_repeated_source_timeouts_stop_before_source_admission(monkeypatch, tmp_path):
    calls = []
    def unavailable(argv, **kwargs):
        calls.append(argv)
        raise r.subprocess.TimeoutExpired(argv, kwargs['timeout'])
    monkeypatch.setattr(r.subprocess, 'run', unavailable)
    with pytest.raises(RuntimeError, match='Retryable local source-read timeout before traffic'):
        r.local_source_hashes([tmp_path/'worker.py'], attempts=3)
    assert len(calls) == 3
