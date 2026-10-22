"""Screen saved assignments using measured input and calibration service rates.

This is an offline diagnostic, not a controller or a guarantee of queue stability.
Counter resets, missing observations and unmeasured consumers remain explicit.
"""
import argparse
from collections import defaultdict, Counter
import csv
import hashlib
import json
import math
from pathlib import Path
import statistics

from build_analysis import GROUPS, LABELS, read


SETTINGS = {'APP_CPU_ITERATIONS':'0', 'APP_DELAY_MS':'0',
    'PAYLOAD_SIZE_BYTES':'100', 'POLL_MAX_MSG':'25', 'POLL_TIMEOUT':'0.1',
    'LAG_QUERY_INTERVAL':'2', 'LAG_QUERY_TIMEOUT':'3', 'LAG_QUERY_BUDGET_SEC':'0.2',
    'COMMIT_INTERVAL_SECONDS':'2', 'FETCH_MIN_BYTES':'1', 'FETCH_MAX_WAIT_MS':'5'}
NAMES = {'producer_partition_messages_total', 'producer_delivery_ok_total',
    'producer_cpu_percent_scrape_timestamp_seconds',
    'consumer_messages_consumed_total', 'consumer_processing_seconds_sum',
    'consumer_processing_seconds_count', 'consumer_processing_backlog',
    'consumer_partition_owned', 'consumer_rebalances_total',
    'consumer_cpu_percent_scrape_timestamp_seconds'}


def base(labels):
    return {k:v for k,v in labels.items() if k not in ('__name__','topic','partition','event_type')}


def condition(manifest, resource):
    return dict(node=resource['node'], cpu_request_cores=resource['cpu_request_cores'],
        memory_request_gib=resource['memory_request_gib'],
        task_and_loop_settings={k:float(manifest['config'].get(k,v)) for k,v in SETTINGS.items()})


class CounterData:
    def __init__(self, export, manifest):
        self.start=manifest['evaluation_start_epoch']; self.data=defaultdict(list)
        if export.get('status')!='success':raise ValueError('Unsuccessful monitoring export')
        for item in export['data']['result']:
            labels=item['metric'];name=labels.get('__name__')
            if name not in NAMES or labels.get('run_id')!=manifest['run_id']:continue
            values={}
            for t,v in item['values']:
                tick=round(float(t)-self.start)
                if abs(float(t)-self.start-tick)>.0015:raise ValueError('Unexpected export grid')
                if tick in values:raise ValueError('Duplicate timestamp')
                values[tick]=float(v)
            self.data[name].append((labels,values))

    def series(self,name,pod,ticks,identity=None,partition=None):
        found=[]
        for labels,values in self.data[name]:
            if labels.get('pod')!=pod:continue
            if identity is not None and base(labels)!=identity:continue
            if partition is not None and (labels.get('topic'),labels.get('partition'))!=partition:continue
            if all(t in values and math.isfinite(values[t]) and values[t]>=0 for t in ticks):
                found.append((labels,values))
        if len(found)!=1:raise ValueError('Missing or ambiguous '+name)
        return found[0]

    def counter(self,name,pod,left,right,identity=None,partition=None,allow_interior_gaps=False):
        requested=list(range(left,right+1,2))
        labels,values=self.series(name,pod,[left,right] if allow_interior_gaps else requested,identity,partition)
        ticks=[t for t in requested if t in values and math.isfinite(values[t]) and values[t]>=0]
        if any(values[b]<values[a] for a,b in zip(ticks,ticks[1:])):
            raise ValueError('Counter reset: '+name)
        role='producer' if name.startswith('producer') else 'consumer'
        _,fresh=self.series(role+'_cpu_percent_scrape_timestamp_seconds',pod,ticks,base(labels))
        if any(not 0 <= self.start+t-fresh[t] <= 10 for t in ticks):
            raise ValueError('Stale counter endpoint')
        if any(fresh[b]<fresh[a] for a,b in zip(ticks,ticks[1:])):
            raise ValueError('Scrape time moved backwards')
        elapsed=fresh[right]-fresh[left]
        if elapsed<=0:raise ValueError('No distinct scrapes in interval')
        return dict(delta=values[right]-values[left],seconds=elapsed,
                    rate=(values[right]-values[left])/elapsed,identity=base(labels),
                    scrape_start=fresh[left],scrape_end=fresh[right],
                    missing_interior_query_samples=[t for t in requested if t not in ticks])


def partition_arrivals(data, manifest, left=28,right=58):
    config=manifest['config'];rates=defaultdict(float);observations=[]
    for pod in manifest['producer_pods']:
        # A cumulative counter can bridge an absent interior query sample without
        # filling it, provided both fresh endpoints belong to the same process.
        total=data.counter('producer_delivery_ok_total',pod,left,right,allow_interior_gaps=True)
        deltas=[]
        for topic_index in range(int(config['TOPIC_COUNT'])):
            topic=config['TOPIC_TITLE']+'_'+str(topic_index)
            for partition in range(int(config['NUM_PARTITIONS'])):
                row=data.counter('producer_partition_messages_total',pod,left,right,
                                 total['identity'],(topic,str(partition)),allow_interior_gaps=True)
                if row['seconds']!=total['seconds']:raise ValueError('Partition counter interval differs')
                rates[f'{topic_index}/{partition}']+=row['rate'];deltas.append(row['delta'])
        if abs(sum(deltas)-total['delta'])>1:
            raise ValueError('Partition counters disagree with acknowledged total')
        observations.append(dict(pod=pod,**{k:v for k,v in total.items() if k!='identity'}))
    return dict(partitions=dict(rates),aggregate_rate=sum(rates.values()),producer_intervals=observations)


def stable_lag(snapshots,start,left,right):
    selected=[]
    for tick in range(left,right+1,2):
        found=[r for r in snapshots if abs(r['timestamp']-start-tick)<=.0015]
        if len(found)!=1 or not found[0]['valid']:raise ValueError('Missing or invalid backlog')
        selected.append(found[0])
    for a,b in zip(selected,selected[1:]):
        if a['owners']!=b['owners']:raise ValueError('Ownership changed')
        if any(b[f][p]<a[f][p] for f in ('positions','highs') for p in a[f]):
            raise ValueError('Offset reset')
    return selected


def service_window(data,lag,manifest,pod,left,right):
    # Include the ten-second freshness allowance before the first counter endpoint.
    snapshots=stable_lag(lag['snapshots'],data.start,left-10,right)
    done=data.counter('consumer_messages_consumed_total',pod,left,right)
    count=data.counter('consumer_processing_seconds_count',pod,left,right,done['identity'])
    task=data.counter('consumer_processing_seconds_sum',pod,left,right,done['identity'])
    if count['delta']<=0 or task['delta']<=0:raise ValueError('No completed processing observations')
    if abs(count['delta']-done['delta'])>1:raise ValueError('Processing and completion counts differ')
    for labels,values in data.data['consumer_rebalances_total']:
        if base(labels)==done['identity']:
            ticks=range(left-10,right+1,2)
            if not any(t in values for t in ticks):
                continue  # A revoke counter may be created only at shutdown.
            if any(t not in values for t in ticks) or len({values[t] for t in ticks})!=1:
                raise ValueError('Ownership callback during interval')
    backlog=[]
    expected_owner=pod+'/'+done['identity']['incarnation']
    for row in snapshots:
        tick=round(row['timestamp']-data.start);total=0
        owned_count=0
        for key,owner in row['owners'].items():
            if owner!=expected_owner:continue
            owned_count+=1
            topic,partition=key.rsplit('/',1)
            _,v=data.series('consumer_processing_backlog',pod,[tick],done['identity'],(topic,partition))
            _,owned=data.series('consumer_partition_owned',pod,[tick],done['identity'],(topic,partition))
            if owned[tick]!=1:raise ValueError('Partition is not currently owned')
            total+=v[tick]
        if not owned_count:raise ValueError('No partitions owned by the measured process')
        backlog.append(total)
    short=[data.counter('consumer_messages_consumed_total',pod,t,t+10,done['identity'])['rate']
           for t in range(left,right,10)]
    cv=statistics.pstdev(short)/statistics.mean(short) if statistics.mean(short) else math.inf
    busy_cutoff=max(100,2*done['rate'])
    # Backlog is sampled, so this identifies intervals consistent with saturation,
    # not proof that the process never waited between monitoring observations.
    return dict(left_seconds=left,right_seconds=right,
        completions=done['delta'],observed_seconds=done['seconds'],
        processing_count=count['delta'],processing_seconds=task['delta'],
        effective_rate=done['rate'],mean_processing_seconds=task['delta']/count['delta'],
        task_only_rate=count['delta']/task['delta'],task_fraction=task['delta']/done['seconds'],
        minimum_processing_backlog=min(backlog),busy_backlog_threshold=busy_cutoff,
        ten_second_rate_cv=cv,stable_rate=cv<=.10,
        backlogged=min(backlog)>=busy_cutoff,
        eligible=cv<=.10 and min(backlog)>=busy_cutoff,
        scrape_start_epoch=done['scrape_start'],scrape_end_epoch=done['scrape_end'])


def calibration_sources(repository):
    rel='experiment-records/stability-fixed3-20260921/comparison-data.json'
    rows=json.loads((repository/rel).read_text())
    for row in rows:row['source_record']=rel;row['block']='historical'
    for filename in ('early-stop-summary.json','retest-summary.json','balanced600-summary.json'):
        rel='experiment-records/resource-calibration-20261021/'+filename
        record=json.loads((repository/rel).read_text())
        rows.append(dict(run_id=record['run_id'],rate=600 if filename.startswith('balanced') else 700,
            block='revised',evidence_hashes=record['source_sha256'],source_record=rel))
    return rows


def summarize_windows(windows):
    busy=[w for w in windows if w['eligible']]
    stable=[w for w in windows if w['stable_rate']]
    return dict(eligible_windows=len(busy),stable_windows=len(stable),
        effective_rate=(sum(w['completions'] for w in busy)/sum(w['observed_seconds'] for w in busy)) if busy else None,
        minimum_busy_rate=min((w['effective_rate'] for w in busy),default=None),
        maximum_busy_rate=max((w['effective_rate'] for w in busy),default=None),
        task_only_rate=(sum(w['processing_count'] for w in busy)/sum(w['processing_seconds'] for w in busy)) if busy else None,
        task_rate_all_stable=(sum(w['processing_count'] for w in stable)/sum(w['processing_seconds'] for w in stable)) if stable else None,
        sustained_rate_all_stable=(sum(w['completions'] for w in stable)/sum(w['observed_seconds'] for w in stable)) if stable else None,
        highest_demonstrated_rate=max((w['effective_rate'] for w in stable),default=None))


def assignment_check(arrivals,owners,references,conditions):
    offered=defaultdict(float)
    for part,rate in arrivals.items():
        if part not in owners:raise ValueError('Candidate omits a partition')
        offered[owners[part]]+=rate
    if set(owners)!=set(arrivals):raise ValueError('Candidate contains unexpected partitions')
    rows=[]
    for pod,rate in sorted(offered.items()):
        matches=[r for r in references if pod in conditions and r['condition']==conditions[pod]]
        windows=[w for r in matches for w in r['windows']]
        capacity=summarize_windows(windows)
        sustained=[summarize_windows(r['windows'])['sustained_rate_all_stable'] for r in matches]
        capacity['highest_sustained_calibration_rate']=max((v for v in sustained if v is not None),default=None)
        mu=capacity['effective_rate'];minimum=capacity['minimum_busy_rate']
        status=('below_all_observed_busy_rates' if rate<minimum else
                'above_observed_mean_capacity' if rate>=mu else
                'within_observed_capacity_range') if mu is not None else 'capacity_unavailable'
        rows.append(dict(pod=pod,arrival_rate=rate,reference_status=status,
            load_ratio=rate/mu if mu else None,mean_reference_headroom=mu-rate if mu else None,
            below_demonstrated_throughput=(rate<capacity['highest_sustained_calibration_rate'])
                if capacity['highest_sustained_calibration_rate'] is not None else None,
            conditionally_supported=(rate<minimum if minimum is not None else
                rate<capacity['highest_sustained_calibration_rate'] if capacity['highest_sustained_calibration_rate'] is not None else False),
            matched_calibrations=sorted({r['run_id'] for r in matches}),**capacity))
    return dict(consumers=rows,complete_capacity_coverage=all(r['effective_rate'] is not None for r in rows),
        all_below_observed_minimum=all(r['reference_status']=='below_all_observed_busy_rates' for r in rows),
        reference_screen=('supported_conditionally' if all(r['conditionally_supported'] for r in rows) else
            'load_exceeds_measured_reference' if any(r['reference_status']=='above_observed_mean_capacity' for r in rows)
            else 'incomplete_or_borderline_reference'),
        interpretation='Conditional steady-input screen using calibration references; excludes handover cost, backlog clearance and application deadlines. Unmeasured capacity is not zero.')


def observed_conditions(manifest, path, hashes, key):
    """Join later consumers only when their observed node and requests are stable."""
    conditions={r['pod']:condition(manifest,r) for r in manifest['initial_consumer_resources']}
    if not path.exists():return conditions
    blob=path.read_bytes();hashes[key]=hashlib.sha256(blob).hexdigest()
    observed=defaultdict(set)
    for line in blob.decode().splitlines():
        row=json.loads(line)
        if not row.get('valid') or not manifest['evaluation_start_epoch'] <= row['timestamp'] <= manifest['producer_end_epoch']:
            continue
        for resource in row.get('pods',[]):
            if not resource.get('ready') or resource.get('deleting_at') or not resource.get('node'):
                continue
            observed[resource['pod']].add(json.dumps(condition(manifest,resource),sort_keys=True))
    for pod,values in observed.items():
        if len(values)!=1:raise ValueError('Placement or resources changed during evaluation: '+pod)
        current=json.loads(next(iter(values)))
        if pod in conditions and current!=conditions[pod]:raise ValueError('Initial resources disagree: '+pod)
        conditions[pod]=current
    return conditions


def write_report(result, output):
    def fmt(v):return 'Unmeasured' if v is None else f'{v:,.2f}'
    groups=defaultdict(list)
    for row in result['calibration_consumers']:
        groups[json.dumps(row['condition'],sort_keys=True)].append(row)
    lines=['# Partition input and consumer-capacity check','',
        'This extends the signal analysis using the same twenty intervention trials, twelve historical balanced trials and three later deployment-calibration attempts. It adds no experiments. Early-stopped calibration attempts retain their actual boundaries. The windows below are subdivisions of those runs, not independent trials.','',
        '## Calculation','',
        'Partition input is estimated from changes in `producer_partition_messages_total`, summed over all producers. The counter is updated after the delivery callback, so this is an acknowledgment-rate proxy for arrival, not an exact broker-arrival timestamp. The pre-action window ends at nominal evaluation second 58 and begins at second 28. Each producer uses its actual scrape elapsed time. A missing interior query can be bridged by the cumulative counter when both fresh endpoints belong to the same process; missing endpoints or resets are rejected.','',
        'For sequential processing, task-only capacity is the processing-count increment divided by the processing-time-sum increment, equivalently `1 / mean processing seconds`. A **5 ms illustrative mean gives 200 messages/s for that task**. Effective service rate instead divides completed attempts by elapsed wall time while work remains available. It includes polling, result bookkeeping, lag collection, commits and other runtime overhead outside the application timer.','',
        result['method']['selection'],'',
        'Reference estimates match node, CPU/memory requests and the recorded task/loop settings. They do not establish identical software, assignment/commit mode, co-location or current contention. In particular, historical calibration used cooperative assignment while targeted actions use the explicit assignment path. All transfer to intervention trials is therefore conditional.','',
        '## Calibration evidence','',
        '| Node | CPU / GiB requested | Busy windows | Task-only rate in busy windows | Effective rate | Busy-window range | Highest sustained rate in one calibration |',
        '|---|---:|---:|---:|---:|---:|---:|']
    for key,rows in groups.items():
        c=rows[0]['condition'];s=summarize_windows([w for r in rows for w in r['windows']])
        sustained=[r['summary']['sustained_rate_all_stable'] for r in rows]
        high=max((v for v in sustained if v is not None),default=None)
        span=f"{fmt(s['minimum_busy_rate'])}–{fmt(s['maximum_busy_rate'])}" if s['eligible_windows'] else 'Unmeasured'
        lines.append(f"| {c['node']} | {c['cpu_request_cores']:g} / {c['memory_request_gib']:g} | {s['eligible_windows']} | {fmt(s['task_only_rate'])} | {fmt(s['effective_rate'])} | {span} | {fmt(high)} |")
    lines += ['', 'Rates are messages/s. The effective estimate pools completed counts over observed seconds in eligible windows; task-only capacity pools processing counts over processing seconds. The range is observed variation, not a confidence interval. A sustained rate in an underloaded interval demonstrates work completed; it is **not the consumer’s maximum capacity**. It is the highest per-calibration pooled rate across valid stable windows, not a single peak scrape.','',
        '## Reassignment within three consumers','',
        '| Workload | Run | Initial input to C2 | Candidate C0 input | Candidate C1 input | Candidate C2 input | Reference screen |',
        '|---|---:|---:|---:|---:|---:|---|']
    for trial in result['trials']:
        if trial['action']!='redistribute3':continue
        initial={r['pod']:r for r in trial['checks']['initial']['consumers']}
        candidate={r['pod']:r for r in trial['checks']['candidate']['consumers']}
        vals=[fmt(initial['consumer-sts-2']['arrival_rate'])]+[fmt(candidate['consumer-sts-'+str(i)]['arrival_rate']) for i in range(3)]
        status='Conditional support' if trial['checks']['candidate']['reference_screen']=='supported_conditionally' else 'Incomplete calibration'
        lines.append('| '+ ' | '.join([trial['group'],str(trial['run_number'])]+vals+[status])+' |')
    lines += ['',
        'The historical hot owner has a measured mean service rate of about 292 messages/s. Its initial input of about 580–603 messages/s exceeds that reference, whereas redistribution reduces its input to about 182–233 messages/s. The other two historical consumers handled about 499–500 messages/s each in calibration without meeting the backlogged-window criterion; the proposed loads are below that demonstrated rate. This is consistent with the observed recovery after redistribution, without identifying every consumer’s capacity ceiling.','',
        'For the revised 600-message/s block, the matching calibration supports about 214 messages/s on the node hosting C1, based on only one eligible minute at the matching allocation. C2 previously sustained about 200 messages/s while underloaded, above its proposed 156–164 messages/s. C0 would receive 278–282 messages/s, but the matching calibration demonstrated only about 232 messages/s and did not saturate it. **That gap means capacity is unmeasured at the required load; it does not show that the assignment is infeasible.** The successful experiment outcomes are retained unchanged.','',
        'The CSV includes initial and final-map checks for every trial. Recorded resource histories identify added consumers, but not every node and allocation has a matching backlogged calibration. Scaling therefore cannot be certified by this screen. The targeted candidate maps were specified before their actions and match the observed final maps. Native Kafka final maps are retrospective, not choices known before scaling.','',
        '## Interpretation and next measurement','',
        'For a candidate assignment A, sum the measured rates of its partitions: lambda_c(A) = sum(lambda_p). A steady-input screen requires lambda_c(A) below effective capacity for every consumer. An added safety margin is a policy choice that still requires calibration. Under heterogeneous application work, use partition-specific service demand rather than assuming equal message costs.','',
        'Positive spare service is also needed to drain accumulated work. Passing an average-rate screen does not establish a recovery time, tail-latency target, deadline guarantee or acceptable intervention cost. These must be evaluated separately, including variability and handover time.','',
        'Next, measure the uncalibrated nodes under sustained available work with the same assignment/commit path, source and resources intended for intervention. Retain separate validation workloads. This is more informative than treating every sampled window as an independent training example or fitting a deep model to the present few conditions.','',
        '[Full evidence and method](capacity-analysis.json) · [Per-consumer assignment checks](assignment-capacity.csv)','']
    (output/'CAPACITY_CHECK.md').write_text('\n'.join(lines))


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repository',type=Path,default=Path(__file__).resolve().parents[2])
    parser.add_argument('--calibration-root',type=Path)
    parser.add_argument('--inputs',type=Path)
    parser.add_argument('--raw600',type=Path)
    parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args();repo=args.repository;calroot=args.calibration_root or repo/'results'
    inputs=args.inputs or repo;raw600=args.raw600 or repo;hashes={};references=[]
    for source in calibration_sources(repo):
        run=source['run_id'];root=calroot/run;expected=source['evidence_hashes']
        evidence={name:read(root/name,hashes,'results/'+run+'/'+name,expected.get(name)) for name in
                  ('manifest.json','prometheus.json','lag-summary.json','outcome-summary.json')}
        manifest=evidence['manifest.json'];lag=evidence['lag-summary.json'];outcome=evidence['outcome-summary.json']
        if outcome['validity_failures']:raise ValueError('Invalid calibration outcomes: '+run)
        data=CounterData(evidence['prometheus.json'],manifest)
        end=int(manifest['producer_end_epoch']-manifest['evaluation_start_epoch'])
        for resource in manifest['initial_consumer_resources']:
            windows=[];rejected=Counter()
            for left in range(60,end-59,60):
                try:windows.append(service_window(data,lag,manifest,resource['pod'],left,left+60))
                except ValueError as exc:rejected[str(exc)]+=1
            references.append(dict(run_id=run,pod=resource['pod'],input_rate=source['rate'],block=source['block'],
                source_record=source['source_record'],condition=condition(manifest,resource),
                assignment_mode=manifest['config'].get('CONSUMER_ASSIGNMENT_MODE','cooperative'),
                windows=windows,rejected_windows=dict(rejected),summary=summarize_windows(windows)))
        print('Calibration',run,flush=True)
    trials=[]
    for group,title,rel,count in GROUPS:
        comparison=read(inputs/rel,hashes,rel)
        for trial in comparison['runs']:
            root=(raw600 if group=='4hot-600' else inputs)/trial['raw_evidence']
            evidence={name:read(root/name,hashes,trial['raw_evidence']+'/'+name,trial['evidence_hashes'][name])
                      for name in ('manifest.json','prometheus.json')}
            manifest=evidence['manifest.json'];data=CounterData(evidence['prometheus.json'],manifest)
            rates=partition_arrivals(data,manifest)
            if int(manifest['config']['TOPIC_COUNT'])!=1:raise ValueError('Saved candidate maps require one topic')
            conditions=observed_conditions(manifest,root/'resource-history.jsonl',hashes,
                                           trial['raw_evidence']+'/resource-history.jsonl')
            if trial['arm'] in ('redistribute3','scale_redistribute6'):
                planned={str(r['partition']):r['owner'] for r in manifest['intervention']['target_assignment']}
                if planned!=trial['final_observed_ownership']:raise ValueError('Targeted map differs from the declared candidate')
            checks={}
            for key,field in [('initial','initial_ownership'),('candidate','final_observed_ownership')]:
                owners={'0/'+p:pod for p,pod in trial[field].items()}
                checks[key]=assignment_check(rates['partitions'],owners,references,conditions)
                checks[key]['assignment']=owners
            checks['candidate']['map_source']=('predeclared targeted map verified against final ownership'
                if trial['arm'] in ('redistribute3','scale_redistribute6') else
                'initial map retained without intervention' if trial['arm']=='keep3' else 'retrospective native Kafka final map')
            trials.append(dict(run_id=trial['run_id'],group=group,action=trial['arm'],run_number=trial['run_number'],
                assignment_mode=manifest['config'].get('CONSUMER_ASSIGNMENT_MODE','cooperative'),
                arrivals=rates,checks=checks,observed_conditions=conditions,
                transfer_cautions=['Capacity references match node, requests and listed task/loop settings, not identical runtime code, assignment/commit mode or contemporaneous machine contention.',
                    'Resource histories identify added consumers where available; unmatched node/allocation conditions retain unmeasured capacity.',
                    'The candidate is the observed final map. Native Kafka maps are retrospective and were not known before scaling.']))
            print('Assignment',group,trial['run_id'],flush=True)
    args.output.mkdir(parents=True,exist_ok=True)
    result=dict(schema_version=1,source_sha256=hashes,
        analysis_source_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        calibration_consumers=references,trials=trials,
        method={'arrivals':'Partition acknowledgment-counter differences summed over producers, using nominal seconds 28--58 and actual endpoint scrape times per producer. Not exact broker-arrival timestamps.',
            'task_capacity':'Processing count difference / processing-time-sum difference = 1 / mean local processing seconds. Excludes polling, output/evidence bookkeeping, lag collection and commits.',
            'effective_capacity':'Completed-attempt counter difference / actual scrape elapsed seconds in selected calibration intervals; intervals are pooled by total completions / total observed time.',
            'selection':'Nonoverlapping 60-second evaluation intervals after second 60, before production stops. Fresh counters, fixed ownership/incarnation, no resets, valid contiguous backlog; 10-second throughput CV <= 0.10; minimum owner processing backlog >= max(100, 2 * observed msg/s). Backlog checks include the preceding 10 seconds. These sampled conditions are consistent with sustained work availability, not proof of continuous busyness.',
            'reference_range':'Minimum and maximum eligible-window rates describe observed variability, not confidence bounds. Underloaded intervals provide demonstrated throughput only, not a capacity ceiling.',
            'screen':'Sum of measured partition rates for each candidate consumer compared with its reference capacity. Positive headroom is required for accumulated work to drain. A margin, deadline model and transfer cost require further validation.',
            'scope':'Offline retrospective diagnosis of existing trials. No new live run, no fitted predictor, and no reclassification of original performance outcomes.'})
    (args.output/'capacity-analysis.json').write_text(json.dumps(result,indent=2,allow_nan=False)+'\n')
    rows=[]
    for trial in trials:
        for stage,check in trial['checks'].items():
            for consumer in check['consumers']:
                rows.append(dict(group=trial['group'],run_id=trial['run_id'],action=trial['action'],
                    run_number=trial['run_number'],assignment=stage,
                    **{k:v for k,v in consumer.items() if not isinstance(v,list)}))
    with (args.output/'assignment-capacity.csv').open('w',newline='') as f:
        writer=csv.DictWriter(f,fieldnames=list(rows[0]),lineterminator='\n');writer.writeheader();writer.writerows(rows)
    write_report(result,args.output)


if __name__=='__main__':main()
