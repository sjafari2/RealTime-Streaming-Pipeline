"""Check percentage direction, baseline selection and undefined references."""
import importlib.util
from pathlib import Path
import pytest

script=Path(__file__).resolve().parents[1]/'experiments/four-hot600-20261021/compare_rates.py'
spec=importlib.util.spec_from_file_location('rate_comparison',script)
mod=importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)

@pytest.mark.parametrize('reference,value,higher,expected',[(100,150,True,50),(100,150,False,-50),(100,25,False,75),(100,100,False,0)])
def test_improvement_sign(reference,value,higher,expected):
    assert mod.percent_benefit(reference,value,higher)==expected

@pytest.mark.parametrize('reference,value',[(0,0),(0,10),(None,10),(10,None)])
def test_undefined_reference(reference,value):
    assert mod.percent_benefit(reference,value) is None

@pytest.mark.parametrize('reference,value',[(-1,2),(1,float('nan')),(1,float('inf'))])
def test_invalid_measurement(reference,value):
    with pytest.raises(ValueError):mod.percent_benefit(reference,value)

def row(rate,arm,n,value):
    return dict(rate=rate,arm=arm,run_number=n,run_id=f'{rate}-{arm}-{n}',useful_throughput=value,p99_seconds=value,slo_violation_percent=value,diagnostics={'mean_total_backlog':value},resources={'requested_cpu_core_minutes':value,'requested_memory_gib_minutes':value})

def test_baseline_selection_keeps_runs_and_configurations_separate():
    rows=[row(600,a,n,100*n if a=='keep3' else 25*n) for a in ['keep3','redistribute3','scale_redistribute6','kafka_scale6'] for n in [1,2]]
    rows += [row(700,a,n,50*n) for a in ['redistribute3','scale_redistribute6'] for n in [1,2]]
    groups=mod.percentage_comparisons(rows)
    assert len(groups['within_600_versus_keep3'])==6
    assert len(groups['configuration_600_relative_to_700'])==4
    for item in groups['within_600_versus_keep3']:
        assert item['reference_run_id']==f"600-keep3-{item['run_number']}"
        assert item['changes']['p99_reduction_percent']==75
        assert item['changes']['throughput_increase_percent']==-75
    for item in groups['configuration_600_relative_to_700']:
        assert item['reference_run_id']==f"700-{item['arm']}-{item['run_number']}"
        assert item['changes']['p99_reduction_percent']==50
