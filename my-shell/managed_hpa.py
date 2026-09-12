"""Temporarily isolate fixed/scheduled experiments from competing HPA decisions."""
from contextlib import contextmanager
import copy
import json
from pathlib import Path
import time
import uuid


def save(path, record):
    temporary=path.with_suffix('.json.tmp')
    temporary.write_text(json.dumps(record,indent=2)+'\n')
    temporary.replace(path)


def restore(api, path, record):
    errors=[]
    for item in reversed(record['controllers']):
        if not item.get('attempted'):continue
        name=item['name']
        try:
            current=json.loads(api.kubectl('get','hpa',name,'-o','json'))
            if current['metadata']['uid']!=item['uid']:
                raise RuntimeError('Controller was replaced; its UID changed')
            if current['spec']==item['original_spec']:
                item['status']='restored';continue
            if current['spec']!=item['paused_spec']:
                raise RuntimeError('Controller settings changed after this invocation paused them')
            patch=[dict(op='test',path='/metadata/uid',value=item['uid']),
                   dict(op='test',path='/spec',value=item['paused_spec']),
                   dict(op='replace',path='/spec',value=item['original_spec'])]
            result=json.loads(api.kubectl('patch','hpa',name,'--type=json','-p',json.dumps(patch),'-o','json'))
            if result['metadata']['uid']!=item['uid'] or result['spec']!=item['original_spec']:
                raise RuntimeError('Restoration response did not match the saved original settings')
            item['status']='restored'
        except Exception as exc:
            item.update(status='restoration_pending',error=str(exc));errors.append(name+': '+str(exc))
        finally:
            save(path,record)
    record.update(status='restoration_pending' if errors else 'restored',finished_epoch=time.time())
    save(path,record)
    if errors:
        raise RuntimeError('HPA restoration requires inspection of '+str(path)+': '+'; '.join(errors))


@contextmanager
def paused_for_experiment(api, plan=None):
    config=api.read_config()[1]
    api.validate_config(config);api.validate_intervention(plan,config)
    hpas=json.loads(api.kubectl('get','hpa','-o','json'))['items']
    requested={'consumer-sts':[int((plan or {}).get('initial_consumers') or config['CONSUMER_POD_COUNT'])],
               'producer-sts':[int(config['PRODUCER_POD_COUNT'])]}
    if (plan or {}).get('target_consumers'):requested['consumer-sts'].append(plan['target_consumers'])
    proposed=copy.deepcopy(hpas);changes=[]
    for item in proposed:
        spec=item['spec'];target=spec['scaleTargetRef']
        if target.get('kind')!='StatefulSet' or target.get('name') not in requested:continue
        behavior=spec.get('behavior',{})
        # A controller already paused by someone else stays under their control.
        if all(behavior.get(direction,{}).get('selectPolicy')=='Disabled' for direction in ('scaleUp','scaleDown')):continue
        original=copy.deepcopy(spec)
        counts=requested[target['name']]
        if max(counts)>spec['maxReplicas']:
            raise ValueError('Requested replicas exceed the existing HPA maximum for '+item['metadata']['name'])
        spec['minReplicas']=min(spec.get('minReplicas',1),min(counts))
        for direction in ('scaleUp','scaleDown'):
            spec.setdefault('behavior',{}).setdefault(direction,{})['selectPolicy']='Disabled'
        changes.append(dict(name=item['metadata']['name'],uid=item['metadata']['uid'],
                            original_spec=original,paused_spec=copy.deepcopy(spec),status='prepared',attempted=False))
    api.validate_replica_control(config,plan,proposed)
    if not changes:
        yield None
        return
    # stop() uses the existing managed drain. Freeze the controller only after
    # previous applications stop, so their finishing measurements are preserved.
    api.stop()
    directory=api.ROOT/'results/controller-settings';directory.mkdir(parents=True,exist_ok=True)
    path=directory/('hpa-'+time.strftime('%Y%m%d-%H%M%S')+'-'+uuid.uuid4().hex[:8]+'.json')
    record=dict(status='pausing',namespace=api.NS,created_epoch=time.time(),controllers=changes)
    save(path,record)
    print('[HPA] Saved original controller settings:',path,flush=True)
    try:
        for item in changes:
            item['attempted']=True;save(path,record)
            patch=[dict(op='test',path='/metadata/uid',value=item['uid']),
                   dict(op='test',path='/spec',value=item['original_spec']),
                   dict(op='replace',path='/spec',value=item['paused_spec'])]
            result=json.loads(api.kubectl('patch','hpa',item['name'],'--type=json','-p',json.dumps(patch),'-o','json'))
            if result['metadata']['uid']!=item['uid'] or result['spec']!=item['paused_spec']:
                raise RuntimeError('Pause response did not match the prepared controller settings')
            item['status']='paused';save(path,record)
        record['status']='paused';save(path,record)
        yield path
    finally:
        restore(api,path,record)
        print('[HPA] Restored this invocation\'s original settings.',flush=True)


if __name__=='__main__':
    import argparse
    import run_experiment as api
    parser=argparse.ArgumentParser(description='Restore only unchanged HPA settings from a saved managed invocation.')
    parser.add_argument('record',type=Path);args=parser.parse_args()
    record=json.loads(args.record.read_text())
    if record['namespace']!=api.NS:raise ValueError('Saved controller namespace differs from NAMESPACE')
    with api.command_lock():restore(api,args.record,record)
