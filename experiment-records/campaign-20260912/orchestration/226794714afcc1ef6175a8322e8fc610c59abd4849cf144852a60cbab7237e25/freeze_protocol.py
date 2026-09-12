"""Freeze the next comparisons only after the declared calibration checks pass."""
import hashlib
from datetime import datetime
import json
from pathlib import Path
import statistics
import sys
import time
import yaml

A=Path(__file__).resolve().parent
R=Path('/Users/soheila/Desktop/Thesis-26-27/code')
E=R/'experiments/preliminary-campaign-20260912'
D=R/'results/run-20260912-054927'
assert not (E/'comparison-protocol.json').exists()
status=json.loads((D/'runner-status.json').read_text())
o=json.loads((D/'outcome-summary.json').read_text())
lag=json.loads((D/'lag-summary.json').read_text())
m=json.loads((D/'manifest.json').read_text())
assert status['status']=='complete' and not o['validity_failures']
assert o['admitted_messages_per_second']/1500 >= .98 and lag['covered_fraction']>=.9
rows=[json.loads(x) for x in (A/'cpu-calibration-coherent/live-monitor.jsonl').read_text().splitlines()]
growth=[]
for left,right in [(0,60),(60,120)]:
    first=[r['processing_backlog'] for r in rows if r['valid'] and m['evaluation_start_epoch']+left<=r['timestamp']<m['evaluation_start_epoch']+left+10]
    last=[r['processing_backlog'] for r in rows if r['valid'] and m['evaluation_start_epoch']+right-10<=r['timestamp']<m['evaluation_start_epoch']+right]
    assert len(first)>=2 and len(last)>=2
    growth.append(statistics.median(last)-statistics.median(first))
assert min(growth)>1500
sys.path.insert(0,str(R/'python-scripts'))
from evidence_io import event_paths,open_events
acknowledged=0
for path in event_paths(D/'producer'):
    with open_events(path) as stream:
        acknowledged+=sum(json.loads(line)['event']=='acknowledged' for line in stream)
assert acknowledged/(1500*180)>=.98
base=yaml.safe_load((E/'balanced-cpu-calibration.yaml').read_text())
plans={}
for action in ('none','scale'):
    plan=dict(action=action,after_evaluation_start_seconds=60,initial_consumers=3,
              recovery_threshold_offsets=1500,recovery_hold_seconds=20)
    if action=='scale':plan['target_consumers']=6
    name='scheduled-'+action+'.json';(E/name).write_text(json.dumps(plan,indent=2)+'\n');plans[action]=name
trials=[]
for family,pair,seed,actions in [('sustained','L1',21,['scale','none']),('short','S1',31,['none','scale']),
                                 ('sustained','L2',22,['none','scale']),('short','S2',32,['scale','none'])]:
    duration=600 if family=='sustained' else 180
    config=json.loads(json.dumps(base))
    config['data'].update(EXP_ID='cpu-'+family+'-s'+str(seed),EXP_DURATION_SEC=str(duration),WORKLOAD_SEED=str(seed))
    name=family+'-seed-'+str(seed)+'.yaml'
    (E/name).write_text('# Frozen matched workload; the scheduled plan supplies the action.\n'+yaml.safe_dump(config,sort_keys=False))
    for action in actions:
        trials.append(dict(label=family+'-s'+str(seed)+'-'+action,family=family,pair=pair,seed=seed,
                           action=action,production_seconds=duration,config=name,plan=plans[action]))
intent=json.loads((A/'campaign-intent.json').read_text())
deadline=datetime.fromisoformat(intent['working_deadline_utc'].replace('Z','+00:00')).timestamp()
protocol=dict(frozen_epoch=time.time(),calibration_run_id=D.name,calibration_qualified=True,
              calibration_evidence=dict(admitted_evaluation=o['admitted_evaluation_cohort'],
                   admitted_whole_production=acknowledged,lag_coverage=lag['covered_fraction'],
                   median_boundary_growth_offsets=growth,unfinished=o['incomplete_by_drain']),
              deadline_epoch=deadline,total_target_messages_per_second=1500,initial_consumers=3,target_consumers=6,
              application_sha256_iterations=2000,production_seconds_by_family=dict(sustained=600,short=180),
              warmup_seconds=60,drain_seconds=120,action_seconds_from_production_start=120,
              recovery_threshold_offsets=1500,recovery_hold_seconds=20,
              transition_monitoring_guard_grace_seconds=60,trials=trials,
              notes=[
                  'Preliminary finite production episodes, not the full 25-minute/five-repetition Table 1 protocol.',
                  'The sustained episode permits eight minutes after the scheduled request; the short episode permits one minute. Input stops at the declared boundary in both cases, without a background low-rate phase.',
                  'The first 60 seconds are excluded from the admission cohort but deliberately develop the same backlog before evaluation; no stationary warm-up claim is made.',
                  'Two pairs per duration with reversed treatment order across pairs. Workload parameters and seed match within each pair; actual admissions and assignments remain measured.',
                  'The existing HPA remains paused. Scheduled 3-to-6 scaling is an action test, not a new selector or tuned HPA baseline.',
                  'The 60-second guard grace permits expected monitoring interruption after a scale request; missing observations still reduce analysis coverage and never count as recovery.',
                  'Retain unfavorable valid results. Stop for evidence/control problems and report technical exclusions explicitly.',
                  'Resource comparisons use common evaluation-through-drain horizons; calibration/setup is reported separately.',
                  'Recovery not confirmed during production is censored; do not substitute drain recovery for in-production recovery.',
                  'No confidence interval, universal superiority, novelty or legal eligibility claim follows from these short repeated pilots.'])
for row in trials:
    row['config_sha256']=hashlib.sha256((E/row['config']).read_bytes()).hexdigest()
    row['plan_sha256']=hashlib.sha256((E/row['plan']).read_bytes()).hexdigest()
(E/'comparison-protocol.json').write_text(json.dumps(protocol,indent=2)+'\n')
print('Frozen',len(trials),'trials after calibration',D.name)
