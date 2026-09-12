import copy
import json
from types import SimpleNamespace

import pytest
from managed_hpa import paused_for_experiment,restore
from test_replica_control import hpa
import run_experiment as runner


def setup(tmp_path,paused=False):
    state=hpa(paused);state['spec']['minReplicas']=6 if not paused else 1
    state['spec']['metrics']=[dict(type='Resource',resource=dict(name='cpu'))]
    calls=[]
    def kubectl(*args):
        if args[:2]==('get','hpa'):
            return json.dumps(dict(items=[state]) if args[2]=='-o' else state).encode()
        if args[:2]==('patch','hpa'):
            patch=json.loads(args[args.index('-p')+1]);calls.append(patch)
            assert patch[0]['value']==state['metadata']['uid'] and patch[1]['value']==state['spec']
            state['spec']=copy.deepcopy(patch[2]['value']);return json.dumps(state).encode()
        raise AssertionError(args)
    config=dict(CONSUMER_POD_COUNT='3',PRODUCER_POD_COUNT='3')
    api=SimpleNamespace(ROOT=tmp_path,NS='test',read_config=lambda:(b'',config),
                        validate_config=lambda c:None,validate_intervention=lambda p,c:None,
                        validate_replica_control=runner.validate_replica_control,kubectl=kubectl,
                        stop=lambda:calls.append('stop'))
    return api,state,calls


@pytest.mark.parametrize('interrupted',[False,True])
def test_restore_original_hpa_after_success_or_interruption(tmp_path,interrupted):
    api,state,calls=setup(tmp_path);original=copy.deepcopy(state['spec'])
    def run():
        with paused_for_experiment(api,dict(initial_consumers=3,target_consumers=6)) as path:
            assert state['spec']['minReplicas']==3 and state['spec']['maxReplicas']==12
            assert state['spec']['metrics']==original['metrics']
            assert state['spec']['behavior']['scaleUp']['selectPolicy']=='Disabled'
            if interrupted:raise KeyboardInterrupt()
    if interrupted:
        with pytest.raises(KeyboardInterrupt):run()
    else:run()
    assert state['spec']==original and calls[0]=='stop' and len(calls)==3
    record=json.loads(next((tmp_path/'results/controller-settings').glob('*.json')).read_text())
    assert record['status']=='restored'


def test_prepaused_controller_is_not_owned_or_restored(tmp_path):
    api,state,calls=setup(tmp_path,True);original=copy.deepcopy(state)
    with paused_for_experiment(api):pass
    assert state==original and calls==[]


def test_concurrent_hpa_change_is_preserved_and_reported(tmp_path):
    api,state,calls=setup(tmp_path)
    with pytest.raises(RuntimeError,match='restoration requires inspection'):
        with paused_for_experiment(api):state['spec']['maxReplicas']=11
    assert state['spec']['maxReplicas']==11 and len(calls)==2
    record=json.loads(next((tmp_path/'results/controller-settings').glob('*.json')).read_text())
    assert record['status']=='restoration_pending'


def test_maximum_resource_bound_is_not_expanded(tmp_path):
    api,state,calls=setup(tmp_path)
    with pytest.raises(ValueError,match='existing HPA maximum'):
        with paused_for_experiment(api,dict(target_consumers=13)):pass
    assert calls==[]


def test_uncertain_pause_response_is_recovered_by_inspecting_saved_state(tmp_path):
    api,state,calls=setup(tmp_path);original=copy.deepcopy(state['spec']);real=api.kubectl
    def uncertain(*args):
        result=real(*args)
        if args[0]=='patch' and len(calls)==2:raise RuntimeError('Connection failed after API accepted pause')
        return result
    api.kubectl=uncertain
    with pytest.raises(RuntimeError,match='Connection failed'):
        with paused_for_experiment(api):pass
    assert state['spec']==original


@pytest.mark.parametrize('repetitions,batch',[(1,False),(3,True)])
def test_complete_entrypoints_hold_pause_across_all_runs_and_restore(monkeypatch,tmp_path,repetitions,batch):
    from test_run_commands import batch_setup,evidence
    batch_setup(monkeypatch,tmp_path)
    api,state,calls=setup(tmp_path);original=copy.deepcopy(state['spec'])
    monkeypatch.setattr(runner,'paused_for_experiment',lambda module,plan:paused_for_experiment(api,plan))
    observed=[]
    def run():
        assert state['spec']['behavior']['scaleUp']['selectPolicy']=='Disabled'
        observed.append(True)
        return evidence(tmp_path,'hpa-run-'+str(len(observed)))[0]
    monkeypatch.setattr(runner,'complete_run',run)
    runner.run_complete_commands(repetitions=repetitions,batch=batch)
    assert len(observed)==repetitions and state['spec']==original
    assert len(calls)==3  # One stop, one pause, one restoration for the invocation.
