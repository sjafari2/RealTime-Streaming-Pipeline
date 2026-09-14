import sys
from pathlib import Path
import pytest
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'python-scripts'))
from audit_measurements import summarize_samples
from analyze_lag import signals


def test_resource_integral_and_stale_gap():
    values=[[0,'100'],[2,'200'],[4,'100'],[6,'999']]
    result=summarize_samples(values,{0:0,2:2,4:4,6:-20},0,6)
    assert result['covered_seconds']==4
    assert result['time_weighted_mean']==150
    assert result['observed_integral']/100==6
    assert result['peak_sampled']==200
    assert result['covered_fraction']==pytest.approx(2/3)


def test_missing_and_long_gaps_are_not_zero():
    assert summarize_samples([[0,'1'],[10,'2']],{0:0,10:10},0,10)['time_weighted_mean'] is None
    assert summarize_samples([[0,'1'],[2,'2']],{},0,2)['covered_seconds']==0


def test_processing_growth_separate_from_fetch_lag_and_owner_reset():
    rows=[dict(timestamp=t,valid=True,lags={'p':10},positions={'p':t},highs={'p':t+10},
               owners={'p':owner},processing_backlog=backlog)
          for t,backlog,owner in [(0,30,'a'),(2,50,'a'),(4,40,'a'),(6,60,'b')]]
    output=signals(rows,window_samples=2)['snapshots']
    assert output[1]['growth_offsets_per_second']==0
    assert output[1]['processing_backlog_growth_offsets_per_second']==10
    assert output[2]['processing_backlog_growth_offsets_per_second']==-5
    assert output[2]['window_processing_backlog_growth_offsets_per_second']==2.5
    assert output[3]['processing_backlog_growth_offsets_per_second'] is None


def test_missing_resources_fail_audit_but_do_not_remove_events(tmp_path):
    import json
    from audit_measurements import analyze
    (tmp_path/'manifest.json').write_text(json.dumps({'run_id':'x'}))
    (tmp_path/'prometheus.json').write_text(json.dumps({'status':'success','data':{'result':[]}}))
    (tmp_path/'final.json').write_text(json.dumps({'role':'consumer','incarnation':'c','pod':'consumer-0'}))
    for name in ('outcome-summary.json','lag-summary.json','execution-summary.json'):
        (tmp_path/name).write_text('{}')
    result=analyze(tmp_path)
    assert len(result['issues'])==2
    assert all(p['status']=='unavailable' for p in result['process_resources'])
