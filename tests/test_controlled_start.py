"""Exercise the real start barrier without a cluster or a production clock."""
import copy
import hashlib
import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'my-shell'))
import run_experiment as r
from placement_control import PlacementMismatch
from test_placement_control import example


@pytest.mark.parametrize('mode', ['prepare', 'run', 'wrong_owner', 'budget'])
def test_start_releases_only_a_verified_experiment(monkeypatch, mode):
    ref, cfg, rows, items = example()
    cfg['RUN_ID'] = 'run'
    raw = json.dumps(cfg).encode()
    for row in rows.values():
        row['config_sha256'] = hashlib.sha256(raw).hexdigest()
    if mode == 'wrong_owner':
        a, b = rows['consumer-sts-0'], rows['consumer-sts-1']
        a['assignments'], b['assignments'] = b['assignments'], a['assignments']
    plan = dict(action='none', initial_consumers=2, target_consumers=None,
                after_evaluation_start_seconds=60, placement_reference=ref, prepare_only=mode == 'prepare')
    if mode == 'budget':
        plan['preparation_budget_seconds'] = 1e-12
    published, journal = [], []
    monkeypatch.setattr(r, 'read_config', lambda: (raw, cfg))
    monkeypatch.setattr(r, 'validate_replica_control', lambda *a: [])
    monkeypatch.setattr(r, 'stop', lambda: None)
    monkeypatch.setattr(r, 'set_consumer_baseline', lambda *a: None)
    monkeypatch.setattr(r, 'preflight', lambda: None)
    monkeypatch.setattr(r, 'pods', lambda role: [p['pod'] for p in ref['pods'] if p['role'] == role])
    monkeypatch.setattr(r, 'kubectl', lambda *a, **k: json.dumps(dict(items=items)).encode())
    monkeypatch.setattr(r, 'shared_write', lambda *a: None)
    monkeypatch.setattr(r, 'publish', lambda c: published.append(copy.deepcopy(c)))
    monkeypatch.setattr(r, 'estimate_capacity', lambda *a: {})
    monkeypatch.setattr(r, 'resource_snapshot', lambda *a: dict(valid=True, pods=[]))
    monkeypatch.setattr(r, 'start_app', lambda *a: None)
    monkeypatch.setattr(r, 'status', lambda role, name: rows[name])
    monkeypatch.setattr(r, 'validate_readiness', lambda *a: True)
    monkeypatch.setattr(r, 'clock_probes', lambda *a: [])
    monkeypatch.setattr(r, 'monitoring_readiness', lambda *a: {})
    monkeypatch.setattr(r, 'journal', lambda *a: journal.append(copy.deepcopy(a[-1])))
    monkeypatch.setattr(r.time, 'sleep', lambda *a: None)
    if mode in ('wrong_owner', 'budget'):
        with pytest.raises(PlacementMismatch, match='ownership differs|budget exhausted'):
            r.start(plan)
        assert [c['state'] for c in published] == ['preparing', 'failed']
        assert all('start_epoch' not in c for c in published)
        assert journal[-1]['valid'] is (mode == 'budget')
    else:
        control = r.start(plan)
        assert [c['state'] for c in published] == ['preparing', 'stopped' if mode == 'prepare' else 'running']
        assert journal[-1]['valid'] is True
        assert control['initial_placement_processes']
        assert ('start_epoch' in control) == (mode == 'run')


def test_shared_stop_during_gate_prevents_scale(monkeypatch):
    control = dict(run_id='run', state='running', producer_end_epoch=10**12,
                   intervention=dict(action='scale', at_epoch=0, initial_consumers=3, target_consumers=6))
    states = iter([control, dict(run_id='run', state='stopped')])
    monkeypatch.setattr(r, 'read_control', lambda: next(states))
    monkeypatch.setattr(r, 'verify_placement', lambda *a: None)
    monkeypatch.setattr(r, 'kubectl', lambda *a, **k: pytest.fail('must not scale'))
    with pytest.raises(RuntimeError, match='changed during placement'):
        r.apply_intervention(control)
