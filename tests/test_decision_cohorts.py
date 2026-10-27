import importlib.util
import json
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('decision_cohorts', ROOT/'experiments/backlog-comparison/analyze_cohorts.py')
cohorts = importlib.util.module_from_spec(spec)
spec.loader.exec_module(cohorts)


def test_pending_and_new_messages_retain_unfinished_work(tmp_path):
    manifest = dict(run_id='r', start_epoch=0, evaluation_start_epoch=10,
                    producer_end_epoch=100, drain_end_epoch=110, config={'SLO_THRESHOLD_MS':'1000'})
    (tmp_path/'manifest.json').write_text(json.dumps(manifest))
    (tmp_path/'runner-status.json').write_text(json.dumps({'status':'complete','run_id':'r'}))
    records = []
    for identity, produced, ack in [('pending',60,61), ('pending_unfinished',65,66),
            ('new',71,72), ('new_unfinished',99,99), ('prior',20,21), ('late_ack',69,71)]:
        records.append(dict(event='acknowledged', message_id=identity, producer_timestamp=produced, timestamp=ack))
    for identity, finished in [('pending',101), ('pending',100), ('new',74), ('prior',22), ('late_ack',75)]:
        records.append(dict(event='completed', message_id=identity, completion_timestamp=finished))
    (tmp_path/'events.jsonl').write_text('\n'.join(map(json.dumps, records)))
    result = cohorts.analyze(tmp_path)['cohorts']
    old, new = result['pending_at_decision'], result['produced_after_decision']
    assert old['messages'] == 2 and old['unfinished'] == 1
    assert old['completion_p99_seconds'] == 40
    assert old['all_pending_completed_seconds'] is None
    assert old['completion_fraction_after_decision'][-1]['fraction'] == .5
    assert new['messages'] == 2 and new['unfinished'] == 1 and new['completion_p99_seconds'] == 3
    assert result['completed_before_decision']['messages'] == 1
    assert result['earlier_produced_acknowledged_later']['messages'] == 1
    assert sum(x['messages'] for x in result.values()) == 6


def test_not_yet_expired_deadline_is_unknown_instead_of_a_miss():
    result = cohorts.summarize([(60,100),(65,None)],70,100,110,50)
    assert result['deadline_censored'] == 1
    assert result['deadline_misses'] == 0 and result['deadline_miss_fraction'] is None


def test_negative_latency_is_not_counted_as_a_fast_completion():
    with pytest.raises(ValueError):
        cohorts.summarize([(60,59)],70,100,110,1)


def test_empty_pending_cohort_has_no_clearance_delay():
    result = cohorts.summarize([],70,100,110,1,pending=True)
    assert result['pending_clearance_confirmed'] and result['all_pending_completed_seconds'] == 0
