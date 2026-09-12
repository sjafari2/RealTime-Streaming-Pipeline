"""Apply the predeclared dedicated-partition calibration checks to saved evidence."""
import argparse,hashlib,json,statistics,time
from pathlib import Path
R=Path('/Users/soheila/Desktop/Thesis-26-27/code');A=Path(__file__).resolve().parent
p=argparse.ArgumentParser();p.add_argument('run_id');args=p.parse_args();D=R/'results'/args.run_id
read=lambda name:json.loads((D/name).read_text())
m,o,lag,e,status,verification=[read(n) for n in ['manifest.json','outcome-summary.json','lag-summary.json','execution-summary.json','runner-status.json','evidence-verification.json']]
c=m['config'];start=m['evaluation_start_epoch'];end=m['producer_end_epoch'];drain=m['drain_end_epoch']
assert int(c['NUM_PARTITIONS'])==1 and int(c['CONSUMER_POD_COUNT'])==1 and int(c['TOPIC_COUNT'])==1
assert int(c['APP_CPU_ITERATIONS'])==2000 and float(c['TARGET_RATE'])*int(c['PRODUCER_POD_COUNT'])==1200
assert end-start==120 and drain-end==120
points=[]
for left,right in [(0,10),(55,65),(110,120)]:
 values=[x['processing_backlog'] for x in lag['snapshots'] if x['valid'] and left<=x['timestamp']-start<=right]
 points.append(dict(relative_window_seconds=[left,right],valid_snapshots=len(values),median_backlog=statistics.median(values) if len(values)>=3 else None))
med=[x['median_backlog'] for x in points]
growth=[med[i+1]-med[i] if med[i] is not None and med[i+1] is not None else None for i in range(2)]
changes=[x for x in e['ownership_events'] if m['start_epoch']<=x['timestamp']<=drain]
checks=dict(complete_evidence=status['status']=='complete' and not o['validity_failures'],sha256_matches_original=verification['all_files_match'],target_admission=o['admitted_messages_per_second']>=1176,stable_ownership=not changes,lag_coverage=lag['covered_fraction']>=.9,three_observations_per_boundary=all(x['valid_snapshots']>=3 for x in points),growing_in_both_halves=all(x is not None and x>1000 for x in growth),processing_below_arrival=o['useful_throughput_per_second']<.9*o['admitted_messages_per_second'])
result=dict(run_id=args.run_id,assessed_epoch=time.time(),qualified=all(checks.values()),checks=checks,admitted_per_second=o['admitted_messages_per_second'],unique_completions_per_evaluation_second=o['useful_throughput_per_second'],admitted_cohort=o['admitted_evaluation_cohort'],unfinished_by_drain=o['incomplete_by_drain'],lag_coverage=lag['covered_fraction'],boundary_windows=points,median_boundary_growth_offsets=growth,ownership_changes=changes,initial_resources=m['initial_consumer_resources'],source_sha256={n:hashlib.sha256((D/n).read_bytes()).hexdigest() for n in ['manifest.json','outcome-summary.json','lag-summary.json','execution-summary.json','runner-status.json','evidence-verification.json']},limitations=['One calibration identifies overload for the recorded consumer/node and interval, not a universal capacity constant.','The later 60-partition trials have sparse cold traffic and potentially different hot owners and node placements; retain those conditions in the interpretation.'])
(A/'single-partition-qualification.json').write_text(json.dumps(result,indent=2,allow_nan=False)+'\n')
print(json.dumps(result,indent=2),flush=True)
