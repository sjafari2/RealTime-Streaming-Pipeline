import importlib.util
import json
import os
from pathlib import Path
import pytest
ROOT=Path(os.environ.get('PIPELINE_REPOSITORY',Path(__file__).resolve().parents[1]))
SCRIPT=Path(os.environ.get('LATE_COHORT_SCRIPT',ROOT/'experiments/four-hot-partitions-20260930/analyze_late_cohort.py'))
spec=importlib.util.spec_from_file_location('late_cohort',SCRIPT);m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)


@pytest.mark.parametrize('confirmed,expected',[(None,False),(481,False),(480,True),(400,True)])
def test_late_cohort_is_not_automatically_after_recovery(confirmed,expected):
    row=dict(intervention_cost=dict(recovery_from_decision=[dict(threshold_offsets=100,hold_seconds=30,confirmed_epoch=confirmed)]))
    assert m.recovery_before_window(row,480) is expected


def test_historical_comparison_cannot_be_called_prespecified(tmp_path):
    p=tmp_path/'comparison.json';p.write_text(json.dumps(dict(cost_protocol={'hot_partition_count':12})))
    with pytest.raises(ValueError,match='prespecified'):m.analyze(p)


def test_report_keeps_unfinished_and_censored_outcomes_visible():
    row=dict(arm='redistribute3',run_number=1,whole_run_p99_seconds=50,recovery_confirmed_before_window=False,
        late_cohort=dict(admitted_cohort_completion_p99_seconds=None,incomplete_fraction=1,
            completion_deadline_outcomes=[dict(threshold_ms=500,deadline_miss_fraction=1),dict(threshold_ms=1000,deadline_miss_fraction=None)]))
    report=m.report(dict(scope='Selected before outcomes',runs=[row]))
    assert '| 50.000 | Unavailable | 100.000 | 100.000 | Unavailable | No |' in report


def test_hotspot_timing_distinguishes_missing_zero_and_detected():
    rows=[dict(timestamp=t,valid=valid,persistent_hot=hot) for t,valid,hot in
          [(0,True,['p']), (2,False,None), (4,True,None), (6,True,[]), (8,True,['p']), (10,True,['p'])]]
    result=m.hotspot_timing(dict(snapshots=rows),2,10)
    assert result['total_snapshots']==4
    assert result['defined_snapshots']==2
    assert result['nonempty_snapshots']==1
    assert result['first_detection_after_start_seconds']==6
    assert result['last_detection_after_start_seconds']==6
    absent=m.hotspot_timing(dict(snapshots=rows),2,6)
    assert absent['defined_snapshots']==0 and absent['maximum_count'] is None
