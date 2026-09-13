"""Describe recorded hot-partition ownership and task time; not a capacity benchmark."""
from pathlib import Path
import collections,hashlib,json
R=Path('/Users/soheila/Desktop/Thesis-26-27/code');A=Path(__file__).resolve().parent
ids=[('pair1','none','run-20260912-234504'),('pair1','scale','run-20260912-233413'),('pair2','none','run-20260913-033550'),('pair2','scale','run-20260913-034633')]
rows=[]
for pair,action,rid in ids:
 d=R/'results'/rid;m=json.loads((d/'manifest.json').read_text());t=m['evaluation_start_epoch'];x=json.loads((d/'execution-summary.json').read_text());nodes={p['pod']:p['node'] for p in x['observed_consumer_pods']};pods=[];hashes={}
 for f in sorted(d.glob('consumer/*/*/events.jsonl')):
  hashes[str(f.relative_to(d))]=hashlib.sha256(f.read_bytes()).hexdigest();pod=f.parts[-3];count=0;seconds=0;events=[]
  with f.open() as stream:
   for l in stream:
    e=json.loads(l)
    if e['event'] in ('assign','revoke','lost') and any(p[1]==0 for p in e.get('partitions',[])):
     events.append(dict(event=e['event'],evaluation_second=e['timestamp']-t))
    if e['event']=='completed' and e['partition']==0:
     count+=1;seconds+=e['processing_seconds']
  if events or count:pods.append(dict(pod=pod,node=nodes.get(pod),ownership_events=events,partition0_completion_attempts=count,mean_application_task_ms=1000*seconds/count if count else None))
 rows.append(dict(pair=pair,action=action,run_id=rid,input_event_sha256=hashes,pods=pods))
(A/'hot-partition-diagnostic.json').write_text(json.dumps(dict(runs=rows,notes=['Counts and task-time means include warm-up, evaluation and drain records, not only the evaluation admission cohort.','Application-task wall duration is not full consumer service time; overhead and contention are not isolated.','Cross-pod event times have unmeasured clock uncertainty.','This is descriptive evidence of effective processing differences, not a controlled hardware causal estimate.']),indent=2)+'\n')
for row in rows:print(row['pair'],row['action'],[(p['pod'],p['node'],round(p['mean_application_task_ms'],3)) for p in row['pods'] if p['mean_application_task_ms']])
