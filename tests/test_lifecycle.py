import importlib.util
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time

import pytest

ROOT = Path(__file__).resolve().parents[1]


def test_actual_sigterm_flushes_final_evidence(tmp_path):
    # Exercise a real subprocess and signal; no Kafka or cluster is contacted.
    code = '''from pipeline_runtime import Runtime
import time
r=Runtime('producer')
for i in range(50): r.outcome('example', index=i)
print('READY',flush=True)
while not r.stop_event.is_set(): time.sleep(.01)
r.finish(lambda:b'test_counter_total 50\\n')
'''
    env = dict(os.environ, PYTHONPATH=str(ROOT / 'src/common'), RUN_ID='signal-test',
               PRODUCER_EVIDENCE_DIR=str(tmp_path), FINAL_SCRAPE_SECONDS='0')
    proc = subprocess.Popen([sys.executable, '-c', code], env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    try:
        assert proc.stdout.readline().strip() == 'READY'
        proc.send_signal(signal.SIGTERM)
        out, err = proc.communicate(timeout=10)
        assert proc.returncode == 0, err
        final = list(tmp_path.rglob('final.json'))
        assert len(final) == 1
        assert not json.loads(final[0].read_text())['failure']
        assert final[0].with_name('final.prom').read_text() == 'test_counter_total 50\n'
        assert len(final[0].with_name('events.jsonl').read_text().splitlines()) == 52
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait()


def test_second_process_cannot_take_same_role_lock(tmp_path):
    env = dict(os.environ, PYTHONPATH=str(ROOT / 'src/common'), RUN_ID='lock-test',
               PRODUCER_EVIDENCE_DIR=str(tmp_path), FINAL_SCRAPE_SECONDS='0')
    code = "from pipeline_runtime import Runtime; import time; r=Runtime('producer'); print('READY',flush=True); time.sleep(30)"
    proc = subprocess.Popen([sys.executable, '-c', code], env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    try:
        assert proc.stdout.readline().strip() == 'READY'
        second = subprocess.run([sys.executable, '-c', "from pipeline_runtime import Runtime; Runtime('producer')"],
                                env=env, capture_output=True, text=True, timeout=10)
        assert second.returncode != 0 and 'BlockingIOError' in second.stderr
    finally:
        proc.kill()
        proc.wait()


def test_launcher_uses_updated_shared_file_on_restart(tmp_path):
    import shutil
    shutil.copy(ROOT / 'src/common/launch.py', tmp_path / 'launch.py')
    (tmp_path / 'producer.py').write_text('import os; print(os.environ["TARGET_RATE"])')
    config = tmp_path / 'pipeline.yaml'
    env = dict(os.environ, PIPELINE_CONFIG=str(config), PIPELINE_STANDALONE='true')
    for rate in (100, 200):
        config.write_text(f'data:\n  TARGET_RATE: "{rate}"\n')
        result = subprocess.run([sys.executable, str(tmp_path / 'launch.py'), 'producer'], env=env,
                                capture_output=True, text=True, check=True)
        assert result.stdout.strip().endswith(str(rate))


@pytest.mark.parametrize('initial_state,exit_on_term,forced', [('Z', False, False), ('S', True, False), ('S', False, True)])
def test_stop_distinguishes_exited_children_from_live_processes(tmp_path, monkeypatch, initial_state, exit_on_term, forced):
    import contextlib
    import io
    sys.path.insert(0, str(ROOT / 'my-shell'))
    import run_experiment as runner
    fake = tmp_path / '123'
    fake.mkdir()
    (fake / 'cmdline').write_bytes(b'python3\x00producer.py\x00')
    (fake / 'status').write_text('State:\t' + initial_state + '\n')
    signals = []
    def signal_process(pid, sig):
        assert pid == 123
        signals.append(sig)
        if exit_on_term:
            (fake / 'status').write_text('State:\tZ\n')
    monkeypatch.setattr(os, 'kill', signal_process)
    monkeypatch.setattr(runner, 'pods', lambda role: ['producer-sts-0'])
    def execute(role, pod, code, **kwargs):
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            exec(code.replace("Path('/proc')", 'Path(' + repr(str(tmp_path)) + ')'), {})
        return output.getvalue().encode()
    monkeypatch.setattr(runner, 'remote', execute)
    assert runner.force_stop_role('producer', grace=.01) == (['producer-sts-0'] if forced else [])
    assert (signal.SIGKILL in signals) == forced
    if initial_state == 'Z':
        assert not signals
