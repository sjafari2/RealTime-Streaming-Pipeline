import pytest
from analyze_stability import window

def point(t,b,owner='a',valid=True):
    return dict(timestamp=t,processing_backlog=b,owners={'0':owner},valid=valid)

def test_linear_backlog_growth_and_flat_backlog():
    result=window([point(t,2*t) for t in range(11)],0,10)
    assert result['covered_interval_growth_offsets_per_second']==2
    assert result['coverage_fraction']==1
    assert result['time_weighted_mean_backlog']==10
    assert window([point(t,8) for t in range(11)],0,10)['covered_interval_growth_offsets_per_second']==0

def test_gaps_and_ownership_changes_are_not_bridged():
    result=window([point(0,0),point(1,1),point(2,99,valid=False),point(3,100),point(4,101),point(5,500,'b'),point(6,501,'b'),point(20,800,'b')],0,20)
    assert result['covered_seconds']==3
    assert result['covered_interval_growth_offsets_per_second']==1
    assert len(result['segments'])==3

def test_missing_data_is_unavailable_not_zero():
    result=window([point(0,0,valid=False)],0,10)
    assert result['covered_interval_growth_offsets_per_second'] is None
    assert result['time_weighted_mean_backlog'] is None
    assert result['coverage_fraction']==0
