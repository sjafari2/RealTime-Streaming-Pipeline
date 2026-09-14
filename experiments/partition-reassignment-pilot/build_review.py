"""Build review-only maps and synthetic fixtures. Never contact a cluster."""
import json
from pathlib import Path
from planner import plan

HERE=Path(__file__).resolve().parent
HOT=[5,10,25,33,35,39,43,45,51,55,58,59]
COLD=[p for p in range(60) if p not in HOT]
CONSUMERS=['consumer-sts-0','consumer-sts-1','consumer-sts-2']


def assignment(hot_counts):
    result=[];hi=ci=0
    for owner,count in zip(CONSUMERS,hot_counts):
        selected=HOT[hi:hi+count]+COLD[ci:ci+20-count]
        result.extend(dict(partition=p,owner=owner) for p in selected)
        hi+=count;ci+=20-count
    assert hi==12 and ci==48 and {r['partition'] for r in result}==set(range(60))
    return sorted(result,key=lambda r:r['partition'])


def build():
    cases={}
    for name,counts in [('imbalanced',[8,4,0]),('balanced',[4,4,4])]:
        rows=assignment(counts)
        sample=dict(schema_version=1,work_model='equal_cost_records',run_id='illustration-not-a-run',assignment_epoch=0,
                    observed_at=100,maximum_age_seconds=10,capacity_observed_at=95,capacity_maximum_age_seconds=30,
                    capacity_records_per_second=dict.fromkeys(CONSUMERS,400),expected_partitions=list(range(60)),
                    partitions=[dict(r,arrival_records_per_second=60 if r['partition'] in HOT else 3.75,
                                     processing_backlog=100 if name=='imbalanced' and r['owner']==CONSUMERS[0] else 0,
                                     observed_at=100,valid=True) for r in rows])
        candidate=plan(sample)
        cases[name]=dict(initial_assignment=rows,hot_partition_counts=counts,
                         illustrative_snapshot=sample,illustrative_planner_output=candidate)
    review=dict(schema_version=1,status='review_only_not_executable',
                warning='Capacities and planner outputs below are illustrative, not Nautilus measurements. No live assignment adapter exists.',
                workload=dict(partitions=60,hot_partitions=HOT,hot_fraction=.8,producers=3,
                              candidate_rate_per_producer=300,payload_bytes=100,sha256_iterations=2000,
                              added_sleep_ms=0,production_seconds=600,warmup_seconds=60,drain_seconds=120,
                              decision_after_evaluation_start_seconds=120,workload_seeds=[71,72]),
                consumer_count=3,cases=cases,
                trial_order=[dict(case=case,seed=seed,actions=['keep','redistribution_policy'] if seed==71 else ['redistribution_policy','keep'])
                             for case in cases for seed in (71,72)],
                performance_trials=8,
                release_gates=['User confirms exact trial configuration',
                               'A reviewed live ownership adapter passes failure and nonempty-topic handoff tests',
                               'Separate calibration establishes source overload and destination spare capacity',
                               'Freeze workload, capacity estimates, source revision and full starting maps',
                               'Matched empty preparations verify exact ownership and unchanged original processes'])
    (HERE/'review-plan.json').write_text(json.dumps(review,indent=2)+'\n')
    print(json.dumps({name:c['illustrative_planner_output'] for name,c in cases.items()},indent=2))


if __name__=='__main__':build()
