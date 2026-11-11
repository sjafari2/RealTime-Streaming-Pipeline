"""Keep scientific admission failures separate from retryable interruptions."""
import importlib.util
import json
import os
from pathlib import Path
import sys

ROOT = Path(os.environ.get('PIPELINE_REPOSITORY', Path(__file__).resolve().parents[1]))
SCRIPT = Path(os.environ.get('MATCHED_SERIES_LAUNCH_SCRIPT', ROOT / 'experiments/matched-skew600-20261104/launch.py'))
sys.path.insert(0, str(ROOT / 'experiments/matched-skew600-20261104'))
spec = importlib.util.spec_from_file_location('matched_launch', SCRIPT)
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)


def test_only_restored_transient_attempts_are_retried():
    failed = dict(status='failed', restoration='verified', error='TLS handshake timeout')
    assert m.retry_allowed(failed, failed)
    assert m.retry_allowed(failed, None)
    assert not m.retry_allowed(dict(failed, restoration='pending'), failed)
    assert not m.retry_allowed(dict(failed, status='stopped_for_review'), failed)
    assert not m.retry_allowed(dict(failed, error='Stress not demonstrated'), None)
    assert not m.retry_allowed(dict(failed, requires_user_review=True), failed)


def test_completion_requires_every_planned_trial_and_successful_restoration():
    series = dict(status='complete', restoration='verified')
    order = [[f'hot{h}__{a}', 3, 83] for a in ['keep3','redistribute3','scale_redistribute6','kafka_scale6'] for h in [12,4]]
    runs = [dict(arm=a,run_number=n,seed=s,validation={'status':'passed'}) for a,n,s in order]
    campaign = dict(series,cost_protocol={'order':order},runs=runs)
    assert m.completed(series,campaign)
    assert not m.completed(series,dict(campaign,runs=runs[:-1]))
    assert not m.completed(series,dict(campaign,restoration='pending'))


def test_reviewed_resume_preserves_attempts_and_does_not_bypass_scientific_stop(tmp_path):
    import pytest
    series_path = tmp_path / 'series-status.json'
    campaign_path = tmp_path / 'campaign-status.json'
    state = dict(status='failed', restoration='verified', error='KeyboardInterrupt')
    series_path.write_text(json.dumps(state))
    campaign_path.write_text(json.dumps(dict(state, runs=[{'run_id': 'retained'}])))
    status = dict(status='requires_review', attempts=[{'pid': 123, 'log': 'original.log'}],
                  series_status=str(series_path), campaign_status=str(campaign_path), completed_trials=2)
    argv = m.reviewed_resume(['python', 'run.py'], status, 'Preloaded action code before production')
    assert argv[-4:] == ['--resume-series', str(tmp_path), '--resume-campaign', str(tmp_path)]
    assert status['attempts'] == [{'pid': 123, 'log': 'original.log'}]
    assert status['completed_trials'] == 1
    assert len(status['reviewed_resumptions']) == 1
    series_path.write_text(json.dumps(dict(state, status='stopped_for_review')))
    with pytest.raises(ValueError, match='scientific'):
        m.reviewed_resume(['python', 'run.py'], status, 'Cannot override scientific pressure')
