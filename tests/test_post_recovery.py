import importlib.util
import json
import os
from pathlib import Path

import pytest

ROOT = Path(os.environ.get('PIPELINE_REPOSITORY', Path(__file__).resolve().parents[1]))
SCRIPT = Path(os.environ.get('POST_RECOVERY_SCRIPT', ROOT/'experiments/four-condition-20260929/analyze_post_recovery.py'))
spec = importlib.util.spec_from_file_location('post_recovery', SCRIPT)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


def fixture_run(path):
    path.mkdir()
    manifest = dict(run_id='r', evaluation_start_epoch=100, producer_end_epoch=110, drain_end_epoch=120,
                    config=dict(TOPIC_TITLE='r', TOPIC_COUNT='1', NUM_PARTITIONS='1', SLO_THRESHOLD_MS='99'),
                    producer_pods=['p'], consumer_pods=['c'])
    (path/'manifest.json').write_text(json.dumps(manifest))
    # Include records just outside each boundary and one completing after cutoff.
    produced = [104.9, 105, 106, 109.9, 110, 105.5]
    finished = [106, 105.2, 106.1, 120.1, 111, None]
    for role, pod in [('producer', 'p'), ('consumer', 'c')]:
        folder = path/role/pod/'process'
        folder.mkdir(parents=True)
        (folder/'final.json').write_text(json.dumps(dict(run_id='r', role=role, pod=pod, incarnation='process')))
        events = []
        for i, birth in enumerate(produced):
            common = dict(message_id=str(i), producer_timestamp=birth, topic='r_0', partition=0, offset=i)
            if role == 'producer':
                events.append(dict(common, event='acknowledged', timestamp=birth+.001))
            elif finished[i] is not None:
                events.append(dict(common, event='completed', completion_timestamp=finished[i], output_sha256='same'))
        if role == 'consumer':
            events.append(dict(events[1], completion_timestamp=105.7))
        (folder/'events.jsonl').write_text('\n'.join(map(json.dumps, events))+'\n')
    return path


def test_birth_window_earliest_completion_cutoff_and_original_files(tmp_path):
    root = fixture_run(tmp_path/'run')
    before = {p: p.read_bytes() for p in root.rglob('*') if p.is_file()}
    result = module.evaluate_cohort(root, 105, 110)
    assert result['validity_failures'] == []
    assert result['admitted_evaluation_cohort'] == 4
    assert result['completed_by_drain'] == 2
    assert result['incomplete_by_drain'] == 2
    assert result['incomplete_fraction'] == .5
    assert result['duplicate_completion_attempts'] == 1
    assert result['admitted_cohort_completion_p99_seconds'] == pytest.approx(.2)
    assert result['admitted_cohort_completion_mean_seconds'] == pytest.approx(.15)
    assert result['deadline_misses'] == 4
    assert {p: p.read_bytes() for p in before} == before


@pytest.mark.parametrize('start,end', [(99, 110), (110, 111), (110, 110), (105, 111)])
def test_cohort_must_be_inside_evaluation(tmp_path, start, end):
    with pytest.raises(ValueError, match='inside'):
        module.evaluate_cohort(fixture_run(tmp_path/'run'), start, end)


def test_missing_or_later_recovery_rejected():
    row = dict(intervention_cost=dict(recovery_from_decision=[dict(threshold_offsets=100, hold_seconds=30, confirmed_epoch=481)]))
    with pytest.raises(ValueError, match='before confirmed'):
        module.verified_recovery(row, 480)
    row['intervention_cost']['recovery_from_decision'][0]['confirmed_epoch'] = None
    with pytest.raises(ValueError, match='No confirmed'):
        module.verified_recovery(row, 480)


def test_multiple_deadlines_keep_the_original_cutoff_and_primary(tmp_path):
    root=fixture_run(tmp_path/'run')
    result=module.evaluate_cohort(root,105,110,deadline_thresholds_ms=[500,1000])
    thresholds={x['threshold_ms']:x for x in result['completion_deadline_outcomes']}
    assert thresholds[99]['primary'] and thresholds[99]['deadline_misses']==4
    assert thresholds[500]['deadline_misses']==thresholds[1000]['deadline_misses']==2
    assert thresholds[500]['unfinished_deadline_misses']==2
    assert thresholds[1000]['admitted_evaluation_cohort']==4
    assert thresholds[1000]['deadline_miss_fraction']==.5
