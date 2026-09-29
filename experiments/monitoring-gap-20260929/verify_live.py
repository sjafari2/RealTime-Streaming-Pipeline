"""Check raw offset evidence and post-handoff lag availability in a technical run."""
import json,sys,hashlib
from pathlib import Path
root=Path(sys.argv[1]);m=json.loads((root/'manifest.json').read_text())
events=[json.loads(s) for s in (root/'explicit-handoff-events.jsonl').open()]
active=next(e['timestamp'] for e in events if e['event']=='active_verified')
resume=next(e['timestamp'] for e in events if e['event']=='resume_requested')
release=next(e['timestamp'] for e in events if e['event']=='released_verified')
released=next(e['command']['offsets'] for e in events if e['event']=='acquire_requested')
lag=json.loads((root/'lag-summary.json').read_text())
fail=[];fallback=[];first_return={};native={};invalid=[]
for p in root.glob('consumer/*/*/events.jsonl'):
 pod=p.parent.parent.name
 for line in p.open():
  e=json.loads(line)
  if e['timestamp']<release:continue
  if e['event']=='completed':first_return.setdefault((pod,e['partition']),e['processing_start_timestamp'])
  if e['event']=='lag_position_resolved':
   e['pod']=pod
   if e['source']=='assignment_start':
    fallback.append(e)
    if e['raw_position']!=-1001 or e['position']!=released[str(e['partition'])]:fail.append('Fallback does not match verified handoff offset')
   else:native[(pod,e['partition'])]=e['timestamp']
  if e['event']=='lag_invalid':invalid.append(dict(e,pod=pod))
for e in fallback:
 if first_return.get((e['pod'],e['partition']),float('inf'))<e['timestamp']:
  fail.append('Assignment fallback used after a returned record started processing')
samples=[s for s in lag['snapshots'] if s['timestamp']>=active and s['timestamp']<m['producer_end_epoch']]
valid=[s for s in samples if s['valid']]
first_valid=min((s['timestamp'] for s in valid),default=float('inf'))
late_invalid=[s for s in samples if not s['valid'] and s['timestamp']>=active+10]
if not fallback:fail.append('Unresolved position mechanism was not demonstrated after handoff')
if first_valid-active>10:fail.append('Total lag did not recover within ten seconds after active verification')
if late_invalid:fail.append('Invalid total lag remains after the post-handoff allowance')
validation=json.loads((root/'handoff-validation.json').read_text())
if validation['status']!='passed':fail.append('Message/offset validation failed')
result=dict(run_id=m['run_id'],status='failed' if fail else 'passed',failures=fail,
 evaluation_lag_coverage=lag['covered_fraction'],fallback_partitions_after_handoff=len({(e['pod'],e['partition']) for e in fallback}),
 first_valid_seconds_after_active=first_valid-active,first_valid_seconds_after_resume=first_valid-resume,
 invalid_samples_after_active_plus_10=len(late_invalid),raw_invalid_events=invalid,
 longest_client_initialization_seconds=max((native.get((e['pod'],e['partition']),e['timestamp'])-e['timestamp'] for e in fallback),default=None),
 fallback_examples=fallback[:3],message_validation=validation,
 hashes={n:hashlib.sha256((root/n).read_bytes()).hexdigest() for n in ['manifest.json','prometheus.json','lag-summary.json','handoff-validation.json']})
out=Path(sys.argv[2]) if len(sys.argv)>2 else root/'monitoring-gap-verification.json';out.write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result,indent=2))
if fail:raise SystemExit(1)
