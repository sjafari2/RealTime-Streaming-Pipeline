"""Restore this campaign's unchanged settings after its final evidence is saved.

This is a dated recovery helper, not a normal experiment command. It refuses to
touch a different managed run or controller configuration. Inspect its record if
any restoration step fails; do not loosen the ownership checks to force a retry.
"""
import hashlib
import json
import sys
import time
from pathlib import Path

A = Path(__file__).resolve().parent
R = Path('/Users/soheila/Desktop/Thesis-26-27/code')
sys.path.insert(0, str(R / 'my-shell'))
import run_experiment as api
import managed_hpa

path = A / 'campaign-restoration-status.json'
assert not path.exists(), 'Inspect the previous restoration record before retrying'
for name in ('sequence-status-v4.json', 'sequence-status-isolated.json', 'sequence-status-low-input.json'):
    if (A / name).exists():
        assert json.loads((A / name).read_text())['status'] != 'running', name

record = dict(status='checking', started_epoch=time.time(), steps={})


def save():
    temporary = path.with_suffix('.json.tmp')
    temporary.write_text(json.dumps(record, indent=2) + '\n')
    temporary.replace(path)


def step(name, operation):
    try:
        detail = operation()
        record['steps'][name] = dict(status='complete', detail=detail)
    except Exception as exc:
        record['steps'][name] = dict(status='pending', error=str(exc))
    save()


with api.command_lock():
    assert api.kubectl('config', 'current-context').decode().strip() == 'nautilus'
    assert api.NS == 'kafkastreamingdata'
    control = api.read_control()
    assert control and control['run_id'] in {
        Path(json.loads(p.read_text())['result_directory']).name
        for p in A.glob('*/launch.json')
        if json.loads(p.read_text()).get('result_directory')
    }, 'Current shared run is not a completed campaign launch'
    directory = R / 'results' / control['run_id']
    assert json.loads((directory / 'runner-status.json').read_text())['status'] == 'complete'
    evidence = json.loads((directory / 'evidence-verification.json').read_text())
    assert evidence['run_id'] == control['run_id'] and evidence['all_files_match'] and evidence['file_count'] > 0, 'Retain reconciled evidence first'
    assert time.time() > control['drain_end_epoch'] + 15
    record['last_run_id'] = control['run_id']
    raw = api.shared_read(api.CONFIG)
    record['before_config_sha256'] = hashlib.sha256(raw).hexdigest()
    record['expected_config_sha256'] = control['config_sha256']
    save()
    # stop() publishes stopped control before terminating applications, so new
    # baseline pods cannot restart an expired experiment through the supervisor.
    api.stop()
    stopped = api.read_control()
    assert stopped['run_id'] == control['run_id'] and stopped['state'] == 'stopped'
    record['steps']['applications_stopped'] = dict(status='complete')
    save()

    def restore_config():
        current = api.shared_read(api.CONFIG)
        assert hashlib.sha256(current).hexdigest() == control['config_sha256'], 'Shared config changed; preserve the newer edit'
        original = (A / 'shared-config-before.yaml').read_bytes()
        api.shared_write('/config/backups/campaign-20260912-final-before-restore.yaml', current)
        api.shared_write(api.CONFIG, original)
        assert api.shared_read(api.CONFIG) == original
        return dict(restored_sha256=hashlib.sha256(original).hexdigest())

    step('shared_configuration', restore_config)
    step('six_consumer_baseline', lambda: api.set_consumer_baseline(6, 180))

    def restore_hpa():
        recovery = A / 'campaign-hpa-restoration.json'
        managed_hpa.restore(api, recovery, json.loads(recovery.read_text()))
        return str(recovery)

    # Attempt the independently owned HPA restoration even if config or replica
    # restoration needs attention; its helper checks UID and the complete spec.
    step('hpa', restore_hpa)
    record['final_control'] = api.read_control()
    record['status'] = 'restored' if all(s['status'] == 'complete' for s in record['steps'].values()) else 'restoration_pending'
    record['finished_epoch'] = time.time()
    save()
print(json.dumps({k: v for k, v in record.items() if k != 'final_control'}, indent=2))
if record['status'] != 'restored':
    raise SystemExit(1)
