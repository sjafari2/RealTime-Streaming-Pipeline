"""Check workload comparability and keep native scaling free of a target map."""
import importlib.util
import json
import os
from pathlib import Path

ROOT=Path(os.environ.get('PIPELINE_REPOSITORY',Path(__file__).resolve().parents[1]))
spec=importlib.util.spec_from_file_location('four_condition',Path(os.environ.get('CAPACITY_RUN_SCRIPT',ROOT/'experiments/four-condition-20260929/run.py')))
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


def test_capacity_comparison_preserves_work_and_reverses_two_conditions(monkeypatch):
    # The fixture has no live pod identities; validate the intervention itself
    # and retain the placement reference for the equality check below.
    validate=module.r.validate_intervention
    monkeypatch.setattr(module.r,'validate_intervention',
                        lambda plan,config:validate({k:v for k,v in plan.items() if k!='placement_reference'},config))
    protocol=module.campaign_protocol(700,True,4)
    assert protocol['performance_trials']==4
    assert protocol['aggregate_input_rate']==700
    assert protocol['order']==[['redistribute3',1,81],['scale_redistribute6',1,81],
                               ['scale_redistribute6',2,82],['redistribute3',2,82]]
    assert set(protocol['conditions'])=={'redistribute3','scale_redistribute6'}
    hot=list(range(40,44))
    configs=[module.prepared(arm,1,81,reference(),hot,aggregate_rate=700,hot_count=4)
             for arm in protocol['conditions']]
    for cfg,plan in configs:
        assert float(cfg['data']['TARGET_RATE'])*3==700
        assert len(cfg['data']['SKEW_PARTITION'].split(','))==4
        assert int(cfg['data']['EXP_DURATION_SEC'])==660
        assert cfg['data']['APP_CPU_ITERATIONS']=='2000'
        assert cfg['data']['SKEW_FRACTION']=='0.8'
        assert plan['after_evaluation_start_seconds']==60
        assert plan['placement_reference']==reference()
    assert configs[0][1]['target_consumers'] is None
    assert configs[1][1]['target_consumers']==6


def test_historical_default_protocol_is_unchanged():
    expected=json.loads((ROOT/'experiments/four-condition-20260929/protocol.json').read_text())
    assert module.campaign_protocol()==expected


def test_new_deadlines_are_prespecified_and_workload_stays_identical():
    protocol=module.campaign_protocol(700,True,4,1000)
    assert protocol['completion_deadline_ms']==1000
    assert protocol['completion_deadline_reporting_ms']==[500,1000]
    assert protocol['performance_trials']==4
    for arm in protocol['conditions']:
        cfg,_=module.prepared(arm,1,81,aggregate_rate=700,hot_count=4,deadline_ms=1000)
        assert cfg['data']['SLO_THRESHOLD_MS']=='1000'
        assert cfg['data']['APP_CPU_ITERATIONS']=='2000'
        assert float(cfg['data']['TARGET_RATE'])*3==700


def test_resume_checks_the_selected_condition_order():
    state,_=resume_state()
    protocol=module.campaign_protocol(700,True,4)
    state.update(cost_protocol=protocol,hot_partitions=list(range(4)),runs=[dict(arm='redistribute3',run_number=1,seed=81,validation={'status':'passed'})])
    assert module.validate_resume_state(state,protocol)==1
    state['runs'][0]['arm']='keep3'
    import pytest
    with pytest.raises(ValueError,match='unrepeated prefix'):
        module.validate_resume_state(state,protocol)


def test_four_hot_partitions_and_one_second_deadline():
    for arm,count in [('redistribute3',3),('scale_redistribute6',6)]:
        config,plan=module.prepared(arm,1,81,hot_count=4,deadline_ms=1000)
        hot=set(map(int,config['data']['SKEW_PARTITION'].split(',')))
        owners=[f'consumer-sts-{i}' for i in range(count)]
        counts=[sum(x['partition'] in hot and x['owner']==owner for x in plan['target_assignment']) for owner in owners]
        assert counts==([2,1,1] if count==3 else [1,1,1,1,0,0])
        assert config['data']['SLO_THRESHOLD_MS']=='1000'
        assert float(config['data']['TARGET_RATE'])*3==700
    protocol=module.campaign_protocol(700,True,4,1000)
    assert protocol['completion_deadline_ms']==1000
    assert protocol['lag_analysis_parameters']['growth_window_samples']==5


def test_rates_above_user_limit_rejected():
    import pytest
    with pytest.raises(ValueError,match='Unreviewed'):
        module.prepared('redistribute3',1,81,aggregate_rate=900)


def test_published_four_hot_protocol_matches_generated_protocol():
    expected=json.loads((ROOT/'experiments/four-hot-partitions-20260930/protocol.json').read_text())
    assert module.campaign_protocol(700,True,4,1000)==expected
