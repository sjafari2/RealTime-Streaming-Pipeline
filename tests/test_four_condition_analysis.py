import importlib.util
from pathlib import Path
import numpy as np
ROOT=Path(__file__).resolve().parents[1]
spec=importlib.util.spec_from_file_location('four_cost',ROOT/'experiments/four-condition-20260929/summarize.py')
m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)


def test_parallel_processing_prevents_a_false_global_pause():
    result=m.longest_processing_gap(np.array([0.,1.,3.]),np.array([2.,4.,5.]),0,5)
    assert result['seconds']==0


def test_processing_gap_uses_union_and_clips_observation_boundaries():
    result=m.longest_processing_gap(np.array([-1.,.2,4.,7.]),np.array([1.,2.,5.,8.]),0,6)
    assert result==dict(start_epoch=2.,end_epoch=4.,seconds=2.)


def test_native_settlement_restarts_on_missing_data_or_ownership_change():
    owners={str(i):'consumer-sts-'+str(i%6)+'/process' for i in range(60)}
    rows=[dict(timestamp=t,valid=True,owners=dict(owners)) for t in range(0,30,2)]
    rows[2]['valid']=False
    rows[5]['owners']['0']='consumer-sts-1/process'
    result=m.stable_six(rows,0)
    assert result['first_epoch']==12 and result['confirmed_epoch']==22


def test_five_owners_do_not_satisfy_native_scale_settlement():
    owners={str(i):'consumer-sts-'+str(i%5)+'/process' for i in range(60)}
    assert m.stable_six([dict(timestamp=t,valid=True,owners=owners) for t in range(20)],0) is None
