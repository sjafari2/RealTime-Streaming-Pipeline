"""Check the complete-run workflow without starting workloads on Nautilus."""
from contextlib import nullcontext
import json
from pathlib import Path
import subprocess
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'my-shell'))
import run_experiment as runner


def test_retention_error_explains_effective_defaults_and_boundary():
    config = dict(EXP_DURATION_SEC='300', RETENTION_MS='180000', ACKS='all')
    with pytest.raises(ValueError, match=r'180000 ms; needs at least 660000 ms') as error:
        runner.validate_config(config)
    assert '300s production + 60s drain + 180s readiness + 120s margin' in str(error.value)
    config['RETENTION_MS'] = '660000'
    assert runner.validate_config(config) == (300, 60, 0, 180)


def evidence(root, run_id):
    directory = root / 'results' / run_id
    directory.mkdir(parents=True)
    control = dict(run_id=run_id, evaluation_start_epoch=100, producer_end_epoch=110,
                   drain_end_epoch=120, producer_pods=['p'], consumer_pods=['c'],
                   config=dict(RUN_ID=run_id, TOPIC_TITLE=run_id, TARGET_RATE='1',
                               TOPIC_COUNT='1', NUM_PARTITIONS='1', SLO_THRESHOLD_MS='99'))
    (directory / 'manifest.json').write_text(json.dumps(control))
    for role, pod in [('producer', 'p'), ('consumer', 'c')]:
        target = directory / role
        target.mkdir()
        (target / 'final.json').write_text(json.dumps(dict(run_id=run_id, role=role, pod=pod, incarnation=pod)))
        event = dict(event='acknowledged', message_id='m', producer_timestamp=101)
        if role == 'consumer':
            event.update(event='completed', completion_timestamp=101.01, output_sha256='same')
        (target / 'events.jsonl').write_text(json.dumps(event) + '\n')
    return directory, control


def test_collect_preserves_previous_evidence_when_transfer_fails(monkeypatch, tmp_path):
    directory, control = evidence(tmp_path, 'run-copy-fails')
    control.update(state='stopped', config_path='/config/frozen.yaml')
    previous = (directory / 'producer/events.jsonl').read_bytes()
    monkeypatch.setattr(runner, 'ROOT', tmp_path)
    monkeypatch.setattr(runner, 'read_control', lambda: control)
    monkeypatch.setattr(runner, 'shared_read', lambda path: b'data: {}\n')
    monkeypatch.setattr(runner, 'pods', lambda role: [role + '-0'])
    monkeypatch.setattr(runner, 'remote', lambda role, *args: json.dumps([role + '-0']).encode())
    def failed_copy(*args, **kwargs):
        staging = Path(args[2])
        staging.mkdir()
        (staging / 'partial').write_text('incomplete transfer')
        raise subprocess.CalledProcessError(1, ['kubectl', 'cp'], stderr=b'unexpected EOF')
    monkeypatch.setattr(runner, 'kubectl', failed_copy)
    with pytest.raises(subprocess.CalledProcessError):
        runner.collect()
    assert (directory / 'producer/events.jsonl').read_bytes() == previous
    assert not list(directory.rglob('partial'))


def test_evidence_copy_has_bounded_retries_and_keeps_api_timeouts(monkeypatch, tmp_path):
    directory, control = evidence(tmp_path, 'run-copy-ok')
    control.update(state='stopped', config_path='/config/frozen.yaml')
    monkeypatch.setattr(runner, 'ROOT', tmp_path)
    monkeypatch.setattr(runner, 'read_control', lambda: control)
    monkeypatch.setattr(runner, 'shared_read', lambda path: b'data: {}\n')
    monkeypatch.setattr(runner, 'pods', lambda role: [role + '-0'])
    # The shared volume also retains evidence for a pod no longer in Kubernetes.
    monkeypatch.setattr(runner, 'remote', lambda role, *args: json.dumps([role + '-0', role + '-retired']).encode())
    calls = []
    def transfer(command, **kwargs):
        calls.append((command, kwargs['timeout']))
        if 'cp' in command:
            position = command.index('cp')
            destination = Path(command[position + 2])
            destination.mkdir()
            (destination / 'copied').write_text('complete')
        return subprocess.CompletedProcess(command, 0, stdout=b'')
    monkeypatch.setattr(runner.subprocess, 'run', transfer)
    runner.kubectl('get', 'pods')
    assert runner.collect() == directory
    assert '--request-timeout=30s' in calls[0][0] and calls[0][1] == 60
    for command, timeout in calls[1:]:
        assert '--request-timeout=0' in command
        assert '--retries=3' in command and timeout == 900
    assert len(calls) == 5
    for role in ('producer', 'consumer'):
        for pod in [role + '-0', role + '-retired']:
            assert (directory / role / pod / 'copied').read_text() == 'complete'
        assert not (directory / role / 'events.jsonl').exists()


def test_collect_retries_error_stream_eof_without_kept_partial_files(monkeypatch, tmp_path):
    directory, control = evidence(tmp_path, 'run-error-stream')
    control.update(state='stopped', config_path='/config/frozen.yaml')
    monkeypatch.setattr(runner, 'ROOT', tmp_path)
    monkeypatch.setattr(runner, 'read_control', lambda: control)
    monkeypatch.setattr(runner, 'shared_read', lambda path: b'data: {}\n')
    monkeypatch.setattr(runner, 'pods', lambda role: [role + '-0'])
    monkeypatch.setattr(runner, 'remote', lambda role, *args: json.dumps([role + '-0']).encode())
    calls = []
    def copy(*args, **kwargs):
        calls.append(args[1])
        destination = Path(args[2])
        assert not destination.exists()
        destination.mkdir()
        if len(calls) == 1:
            (destination / 'partial').write_text('unfinished')
            raise subprocess.CalledProcessError(1, ['kubectl', 'cp'], stderr=b'error reading from error stream: unexpected EOF')
        (destination / 'events.jsonl').write_text('complete')
    monkeypatch.setattr(runner, 'kubectl', copy)
    assert runner.collect() == directory
    assert len(calls) == 3 and calls[0] == calls[1]
    assert not list(directory.rglob('partial'))
    assert (directory / 'producer/producer-0/events.jsonl').read_text() == 'complete'


def test_collect_preserves_role_when_a_later_pod_copy_fails(monkeypatch, tmp_path):
    directory, control = evidence(tmp_path, 'run-later-copy-fails')
    control.update(state='stopped', config_path='/config/frozen.yaml')
    previous = (directory / 'producer/events.jsonl').read_bytes()
    monkeypatch.setattr(runner, 'ROOT', tmp_path)
    monkeypatch.setattr(runner, 'read_control', lambda: control)
    monkeypatch.setattr(runner, 'shared_read', lambda path: b'data: {}\n')
    monkeypatch.setattr(runner, 'pods', lambda role: [role + '-0'])
    monkeypatch.setattr(runner, 'remote', lambda *args: b'["first", "second"]')
    calls = []
    def copy(*args, **kwargs):
        calls.append(args[1])
        destination = Path(args[2])
        destination.mkdir()
        (destination / 'new-data').write_text('copied or partial')
        if destination.name == 'second':
            raise subprocess.CalledProcessError(1, ['kubectl', 'cp'], stderr=b'unexpected EOF')
    monkeypatch.setattr(runner, 'kubectl', copy)
    with pytest.raises(subprocess.CalledProcessError):
        runner.collect()
    assert len(calls) == 4  # One successful directory, then three bounded attempts.
    assert (directory / 'producer/events.jsonl').read_bytes() == previous
    assert not list(directory.rglob('new-data'))


def test_complete_run_waits_before_collecting_and_analyzes(monkeypatch, tmp_path):
    directory, control = evidence(tmp_path, 'run-a')
    calls = []
    monkeypatch.setattr(runner, 'read_control', lambda: control)
    monkeypatch.setattr(runner, 'start', lambda: calls.append('start') or control)
    monkeypatch.setattr(runner, 'wait_for_end', lambda value: calls.append('wait-through-drain'))
    monkeypatch.setattr(runner, 'collect', lambda: calls.append('collect') or directory)
    def export(target, value):
        calls.append('export')
        (target / 'prometheus.json').write_text(json.dumps(dict(status='success', data=dict(result=[]))))
    monkeypatch.setattr(runner, 'export_metrics', export)
    assert runner.complete_run() == directory
    assert calls == ['start', 'wait-through-drain', 'collect', 'export']
    assert json.loads((directory / 'outcome-summary.json').read_text())['admitted_evaluation_cohort'] == 1
    assert (directory / 'lag-summary.json').exists()
    assert json.loads((directory / 'runner-status.json').read_text())['status'] == 'complete'


def test_export_failure_preserves_outcome_analysis(monkeypatch, tmp_path):
    directory, control = evidence(tmp_path, 'run-a')
    def fail(*args):
        raise RuntimeError('connection lost')
    monkeypatch.setattr(runner, 'export_metrics', fail)
    with pytest.raises(RuntimeError, match='connection lost'):
        runner.analyze_collected(directory, control)
    assert (directory / 'outcome-summary.json').exists()
    assert json.loads((directory / 'runner-status.json').read_text())['status'] == 'failed'


def batch_setup(monkeypatch, tmp_path):
    # Load the real analyzers before replacing the location of generated results.
    analysis = runner.analysis_tools()
    monkeypatch.setattr(runner, 'analysis_tools', lambda: analysis)
    monkeypatch.setattr(runner, 'ROOT', tmp_path)
    monkeypatch.setattr(runner, 'preflight', lambda: None)
    monkeypatch.setattr(runner, 'prometheus_connection', nullcontext)
    monkeypatch.setenv('PROM_URL', 'http://test-prometheus:9090')
    monkeypatch.setattr(runner, 'prometheus_ready', lambda url: True)


def test_repeat_same_config_saves_exact_run_list_and_summary(monkeypatch, tmp_path):
    batch_setup(monkeypatch, tmp_path)
    calls = []
    def run():
        calls.append('run-' + str(len(calls)))
        return evidence(tmp_path, calls[-1])[0]
    monkeypatch.setattr(runner, 'complete_run', run)
    runner.run_complete_commands(repetitions=3, batch=True)
    folder = next((tmp_path / 'results/batches').iterdir())
    status = json.loads((folder / 'batch-status.json').read_text())
    summary = json.loads((folder / 'repetition-summary.json').read_text())
    assert len(calls) == 3
    assert status['status'] == 'complete' and len(status['runs']) == 3
    assert summary['groups'][0]['included_run_count'] == 3
    assert summary['groups'][0]['pooled_messages']['valid_completion_count'] == 3


def test_batch_stops_on_second_failure_and_keeps_first_result(monkeypatch, tmp_path):
    batch_setup(monkeypatch, tmp_path)
    calls = []
    def run():
        calls.append(1)
        if len(calls) == 2:
            raise RuntimeError('invalid second run')
        return evidence(tmp_path, 'run-one')[0]
    monkeypatch.setattr(runner, 'complete_run', run)
    with pytest.raises(RuntimeError, match='invalid second run'):
        runner.run_complete_commands(repetitions=5, batch=True)
    folder = next((tmp_path / 'results/batches').iterdir())
    status = json.loads((folder / 'batch-status.json').read_text())
    assert len(calls) == 2 and len(status['runs']) == 1
    assert status['status'] == 'failed'
    assert (tmp_path / 'results/run-one/manifest.json').exists()


def test_rate_sweep_runs_every_requested_rate(monkeypatch, tmp_path):
    batch_setup(monkeypatch, tmp_path)
    rates = []
    monkeypatch.setattr(runner, 'stop', lambda: None)
    monkeypatch.setattr(runner, 'pods', lambda role: ['c'])
    monkeypatch.setattr(runner, 'shared_read', lambda path: b'config')
    def remote(role, pod, code, raw):
        rate = '100' if "='100';" in code else '200'
        rates.append(rate)
        return rate.encode()
    monkeypatch.setattr(runner, 'remote', remote)
    monkeypatch.setattr(runner, 'shared_write', lambda *args: None)
    monkeypatch.setattr(runner, 'complete_run', lambda: evidence(tmp_path, 'run-' + str(len(rates)))[0])
    runner.run_complete_commands(repetitions=2, rates=[100, 200], batch=True)
    assert rates == ['100', '100', '200', '200']


def test_interrupt_stops_owned_run_and_preserves_evidence(monkeypatch, tmp_path):
    directory, control = evidence(tmp_path, 'new')
    current = [dict(run_id='previous')]
    calls = []
    monkeypatch.setattr(runner, 'read_control', lambda: current[0])
    def start():
        current[0] = control
        return control
    def interrupt(value):
        raise KeyboardInterrupt()
    monkeypatch.setattr(runner, 'start', start)
    monkeypatch.setattr(runner, 'wait_for_end', interrupt)
    monkeypatch.setattr(runner, 'stop', lambda: calls.append('stop'))
    monkeypatch.setattr(runner, 'collect', lambda: calls.append('collect') or directory)
    monkeypatch.setattr(runner, 'analyze_collected', lambda *args: calls.append('analyze'))
    with pytest.raises(KeyboardInterrupt):
        runner.complete_run()
    assert calls == ['stop', 'collect', 'analyze']
    assert json.loads((directory / 'runner-status.json').read_text())['status'] == 'interrupted_or_failed'


def test_existing_prometheus_url_does_not_launch_a_forward(monkeypatch):
    monkeypatch.setenv('PROM_URL', 'http://existing:9090')
    monkeypatch.setattr(runner, 'prometheus_ready', lambda url: True)
    def forbidden(*args, **kwargs):
        raise AssertionError('Must not start another connection')
    monkeypatch.setattr(runner.subprocess, 'Popen', forbidden)
    with runner.prometheus_connection():
        assert runner.os.environ['PROM_URL'] == 'http://existing:9090'
    assert runner.os.environ['PROM_URL'] == 'http://existing:9090'


def test_unreachable_prometheus_prevents_any_new_run(monkeypatch, tmp_path):
    batch_setup(monkeypatch, tmp_path)
    monkeypatch.setattr(runner, 'prometheus_ready', lambda url: False)
    def forbidden():
        raise AssertionError('Must not start a workload without the monitoring connection')
    monkeypatch.setattr(runner, 'complete_run', forbidden)
    with pytest.raises(RuntimeError, match='no next run'):
        runner.run_complete_commands(repetitions=3, batch=True)


def test_owned_forward_is_closed_after_failure(monkeypatch):
    monkeypatch.delenv('PROM_URL', raising=False)
    monkeypatch.setattr(runner, 'prometheus_ready', lambda url: True)
    calls = []
    class Forward:
        def __init__(self, command, stdout, **kwargs):
            assert 'svc/prometheus-svc' in command
            stdout.write(b'Forwarding from 127.0.0.1:43210 -> 9090\n')
            stdout.flush()
            self.stopped = False
        def poll(self): return 0 if self.stopped else None
        def terminate(self): self.stopped = True; calls.append('terminate')
        def wait(self, **kwargs): calls.append('wait')
    monkeypatch.setattr(runner.subprocess, 'Popen', Forward)
    with pytest.raises(RuntimeError, match='run failed'):
        with runner.prometheus_connection():
            assert runner.os.environ['PROM_URL'] == 'http://127.0.0.1:43210'
            raise RuntimeError('run failed')
    assert calls == ['terminate', 'wait']
    assert 'PROM_URL' not in runner.os.environ


def test_script_help_and_invalid_counts_never_call_kubectl(tmp_path):
    for script in ['save-run.sh', 'run_pipeline.sh']:
        result = subprocess.run(['bash', str(ROOT / 'my-shell' / script), '--help'], capture_output=True, text=True)
        assert result.returncode == 0 and 'Complete runs' in result.stdout
    result = subprocess.run(['bash', str(ROOT / 'my-shell/run_pipeline.sh'), '--repetitions', '0'], capture_output=True, text=True)
    assert result.returncode == 2 and 'positive repetition' in result.stderr
