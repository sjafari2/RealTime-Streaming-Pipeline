"""Check workload comparability and keep native scaling free of a target map."""
import importlib.util
import json
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
spec=importlib.util.spec_from_file_location('four_condition',ROOT/'experiments/four-condition-20260929/run.py')
module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)


def reference():
    # Pure configuration tests do not need cluster pod metadata.
    rows=[dict(topic_index=0,partition=p,pod='consumer-sts-'+str(p//20)) for p in range(60)]
    return dict(schema_version=1,assignment=rows,pods=[])


def test_target_maps_balance_hot_and_cold_work_separately():
    hot=list(range(40,52))
    for count in (3,6):
        rows=module.target_map(hot,count)
        assert len(rows)==60 and len({x['partition'] for x in rows})==60
        for i in range(count):
            parts={x['partition'] for x in rows if x['owner']=='consumer-sts-'+str(i)}
            assert len(parts)==60//count
            assert len(parts.intersection(hot))==12//count


def test_four_arms_use_identical_work_and_phase_boundaries(monkeypatch):
    monkeypatch.setattr(module.r,'validate_intervention',lambda *args:None)
    hot=list(range(40,52));prepared=[module.prepared(a,1,81,reference(),hot) for a in ('keep3','redistribute3','scale_redistribute6','kafka_scale6')]
    keys=['TARGET_RATE','NUM_PARTITIONS','EXP_DURATION_SEC','WARMUP_SECONDS','DRAIN_SECONDS','SKEW_PARTITION','SKEW_FRACTION','APP_CPU_ITERATIONS','WORKLOAD_SEED']
    assert len({tuple(c['data'][k] for k in keys) for c,p in prepared})==1
    for c,p in prepared:
        assert float(c['data']['TARGET_RATE'])*3==700
        assert int(c['data']['EXP_DURATION_SEC'])-int(c['data']['WARMUP_SECONDS'])==600
        assert int(c['data']['EXP_DURATION_SEC'])+int(c['data']['DRAIN_SECONDS'])==780
        assert p['after_evaluation_start_seconds']==60
    native,plan=prepared[-1]
    assert native['data']['CONSUMER_ASSIGNMENT_MODE']=='cooperative'
    assert native['data']['CONSUMER_STATIC_MEMBERSHIP']=='true'
    assert plan['action']=='scale' and plan['target_consumers']==6
    assert 'target_assignment' not in plan and 'EXPLICIT_ASSIGNMENT_JSON' not in native['data']


def test_each_arm_runs_twice_in_reversed_order():
    first,second=module.ORDER[:4],module.ORDER[4:]
    assert [x[0] for x in first]==list(reversed([x[0] for x in second]))
    assert {(n,s) for _,n,s in first}=={(1,81)}
    assert {(n,s) for _,n,s in second}=={(2,82)}


def test_native_technical_validation_is_not_a_performance_trial():
    cfg,plan=module.prepared('kafka_scale6',0,81,gate=True)
    assert float(cfg['data']['TARGET_RATE'])*3==300
    assert int(cfg['data']['EXP_DURATION_SEC'])==120
    assert plan['action']=='scale'


def test_outer_controller_plan_passes_the_managed_runner_validation():
    config,_=module.prepared('kafka_scale6',0,81)
    module.r.validate_intervention(module.HPA_PLAN,config['data'])


def resume_state():
    protocol=json.loads((ROOT/'experiments/four-condition-20260929/protocol.json').read_text())
    state=dict(status='failed',restoration='verified',cost_protocol=protocol,monitoring_gate={'status':'passed'},
        runs=[dict(arm='keep3',run_number=1,seed=81,validation={'status':'passed'})],
        technical=[{'validation':{'status':'passed'}}],reference={'assignment':'frozen'},hot_partitions=list(range(12)))
    return state,protocol


def test_resume_starts_after_the_already_verified_baseline():
    state,protocol=resume_state()
    assert module.validate_resume_state(state,protocol)==1
    assert module.ORDER[len(state['runs'])][0]=='redistribute3'


def test_resume_rejects_duplicates_or_changed_protocol():
    import copy,pytest
    state,protocol=resume_state();state['runs'].append(copy.deepcopy(state['runs'][0]))
    with pytest.raises(ValueError,match='unrepeated prefix'):module.validate_resume_state(state,protocol)
    state,protocol=resume_state();changed=copy.deepcopy(protocol);changed['aggregate_input_rate']=800
    with pytest.raises(ValueError,match='protocol'):module.validate_resume_state(state,changed)


def test_resume_requires_restoration_and_passed_evidence():
    import pytest
    state,protocol=resume_state();state['restoration']='pending'
    with pytest.raises(ValueError,match='restored'):module.validate_resume_state(state,protocol)
    state,protocol=resume_state();state['runs'][0]['validation']['status']='failed'
    with pytest.raises(ValueError,match='validation'):module.validate_resume_state(state,protocol)
