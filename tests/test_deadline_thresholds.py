"""Deadline sensitivity must retain identity, cutoff and censoring rules."""
import json
import os
from pathlib import Path
import sys

import pytest

ROOT=Path(os.environ.get('PIPELINE_REPOSITORY',Path(__file__).resolve().parents[1]))
sys.path.insert(0,str(ROOT/'python-scripts'))
from evaluate_run import evaluate


def evidence(root,produced=101,drain=120,values=(.5,.75,1,1.25,None),primary=1000):
    root.mkdir()
    manifest=dict(run_id='r',evaluation_start_epoch=100,producer_end_epoch=110,
        drain_end_epoch=drain,config=dict(SLO_THRESHOLD_MS=str(primary),TOPIC_TITLE='r',
        TOPIC_COUNT='1',NUM_PARTITIONS='1'),producer_pods=['p'],consumer_pods=['c'])
    (root/'manifest.json').write_text(json.dumps(manifest))
    for role,pod in [('producer','p'),('consumer','c')]:
        d=root/role;d.mkdir()
        (d/'final.json').write_text(json.dumps(dict(run_id='r',role=role,pod=pod,incarnation=pod)))
    arrivals=[dict(event='acknowledged',message_id=str(i),producer_timestamp=produced) for i in range(len(values))]
    completions=[dict(event='completed',message_id=str(i),producer_timestamp=produced,
        completion_timestamp=produced+delay,output_sha256='same') for i,delay in enumerate(values) if delay is not None]
    for role,events in [('producer',arrivals),('consumer',completions)]:
        (root/role/'events.jsonl').write_text('\n'.join(map(json.dumps,events))+'\n' if events else '')
    return root


def thresholds(root):
    result=evaluate(root,deadline_thresholds_ms=[500,1000])
    return result,{r['threshold_ms']:r for r in result['completion_deadline_outcomes']}


def test_strict_deadline_boundary_and_overdue_unfinished(tmp_path):
    result,rows=thresholds(evidence(tmp_path/'r'))
    assert rows[500]['late_completions']==3
    assert rows[1000]['late_completions']==1
    assert rows[500]['unfinished_deadline_misses']==rows[1000]['unfinished_deadline_misses']==1
    assert rows[500]['deadline_miss_fraction']==.8
    assert rows[1000]['deadline_miss_fraction']==result['deadline_miss_fraction']==.4
    assert rows[1000]['primary'] and not rows[500]['primary']


def test_duplicate_and_warmup_do_not_change_cohort_denominator(tmp_path):
    root=evidence(tmp_path/'r')
    with (root/'consumer/events.jsonl').open('a') as f:
        for event in [dict(event='completed',message_id='0',producer_timestamp=101,completion_timestamp=103,output_sha256='same'),
                      dict(event='completed',message_id='warmup',producer_timestamp=99,completion_timestamp=101.5,output_sha256='same')]:
            f.write(json.dumps(event)+'\n')
    result,rows=thresholds(root)
    assert result['duplicate_completion_attempts']==1
    assert result['valid_completion_attempts_in_evaluation']==6
    assert rows[500]['admitted_evaluation_cohort']==rows[1000]['admitted_evaluation_cohort']==5
    assert rows[500]['deadline_miss_fraction']==.8 and rows[1000]['deadline_miss_fraction']==.4
    assert rows[1000]['observed_completion_deadline_miss_fraction']==.5


def test_censoring_is_specific_to_each_threshold(tmp_path):
    _,rows=thresholds(evidence(tmp_path/'r',produced=109.75,drain=110.5,values=(None,)))
    assert rows[500]['deadline_miss_fraction']==1 and rows[500]['deadline_censored']==0
    assert rows[1000]['deadline_miss_fraction'] is None and rows[1000]['deadline_censored']==1


def test_completion_after_cutoff_stays_unfinished(tmp_path):
    _,rows=thresholds(evidence(tmp_path/'r',drain=102,values=(2,)))
    assert all(r['completed_by_drain']==0 and r['unfinished_deadline_misses']==1 for r in rows.values())


def test_bad_clock_suppresses_both_cohort_rates(tmp_path):
    result,rows=thresholds(evidence(tmp_path/'r',values=(-.1,)))
    assert result['invalid_clock_observations']==1
    assert all(r['deadline_miss_fraction'] is None for r in rows.values())


def test_reporting_preserves_historical_primary_and_raw_evidence(tmp_path):
    root=evidence(tmp_path/'r',values=(.05,.125,None),primary=99)
    before={p:p.read_bytes() for p in root.rglob('*') if p.is_file()}
    result,rows=thresholds(root)
    assert rows[99]['primary'] and rows[99]['deadline_miss_fraction']==result['deadline_miss_fraction']==2/3
    assert not rows[1000]['primary']
    assert before=={p:p.read_bytes() for p in before}


def test_empty_cohort_is_unavailable(tmp_path):
    _,rows=thresholds(evidence(tmp_path/'r',values=()))
    assert all(r['deadline_miss_fraction'] is None for r in rows.values())


@pytest.mark.parametrize('value',[0,-1,float('nan'),float('inf')])
def test_invalid_deadlines_are_rejected(tmp_path,value):
    with pytest.raises(ValueError,match='positive finite'):
        evaluate(evidence(tmp_path/'r'),deadline_thresholds_ms=[value])
