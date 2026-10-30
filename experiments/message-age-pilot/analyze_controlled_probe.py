"""Independently reconcile the controlled-age assignment diagnostics."""
import argparse
from bisect import bisect_right
from collections import Counter
import hashlib
import json
import math
import os
from pathlib import Path
import tarfile

from capacity_age_ablation import forecast, VARIANTS
from controlled_age_probe import busy_capacity
from kafka_age_probe import percentile


def unpack(directory):
    execution=json.loads((directory/'execution.json').read_text())
    archive=directory/'raw-evidence.tar.gz'
    assert hashlib.sha256(archive.read_bytes()).hexdigest()==execution['archive_sha256']
    extracted=directory/'extracted';extracted.mkdir(exist_ok=True)
    with tarfile.open(archive) as tf:
        for entry in tf.getmembers():
            dest=(extracted/entry.name).resolve()
            assert os.path.commonpath([str(dest),str(extracted.resolve())])==str(extracted.resolve())
            assert entry.isfile() or entry.isdir()
        tf.extractall(extracted)
    root=extracted/execution['token']
    for name,digest in execution['sources'].items():
        assert hashlib.sha256((root/name).read_bytes()).hexdigest()==digest
    return execution,root


def analyze(directory):
    execution,root=unpack(directory)
    summaries=json.loads((root/'summary.json').read_text())
    protocol=json.loads((root/'protocol.json').read_text())
    calibration=next(s for s in summaries if s['calibration'])
    cal=root/calibration['id']
    release=json.loads((cal/'release.json').read_text())['release_epoch']
    counts=json.loads((cal/'producer-before.json').read_text())['acknowledged_counts']
    manifest=json.loads((cal/'manifest.json').read_text())
    assert counts==[8000]*60
    capacity=[];calibration_ids=set()
    for c in range(6):
        data=json.loads((cal/f'consumer-{c}.json').read_text())
        assert data['error'] is None
        last_offsets={};previous_end=None
        for record in data['records']:
            ident,p,offset,created,start,end,service,cohort=record
            assert ident not in calibration_ids and manifest['owners'][p]==c
            assert ident==calibration['id']+'-'+str((offset-1)*60+p)
            assert offset==last_offsets.get(p,0)+1 and offset<=counts[p]
            assert cohort=='calibration' and created<release and service>=0 and end>=start
            assert previous_end is None or start>=previous_end
            last_offsets[p]=offset;previous_end=end;calibration_ids.add(ident)
        expected=sum(n for p,n in enumerate(counts) if manifest['owners'][p]==c)
        result=busy_capacity(data['records'],data['resources'],release,expected)
        assert result==calibration['capacity'][c]
        capacity.append(result)
    assert all(c['eligible'] for c in capacity)
    clocks={c['pod']:c for c in execution['clock_bounds']}
    producer_clock=clocks['producer-workers-0']
    individual=[c['effective_rate'] for c in capacity]
    pooled=[sum(individual[:3])/3]*6
    rows=[];age_profiles=[];queue_profiles=[]
    for saved in summaries:
        if saved['calibration']:continue
        episode=root/saved['id']
        manifest=json.loads((episode/'manifest.json').read_text())
        producer=json.loads((episode/'producer.json').read_text())
        before=json.loads((episode/'producer-before.json').read_text())
        predictions=json.loads((episode/'predictions.json').read_text())
        release=producer['release_epoch'];cutoff=release+100
        assert not producer['errors'] and predictions['saved_epoch']<release
        assert before['maximum_schedule_error']<=.1
        assert len(producer['ack'])==60600
        ack={r[0]:r for r in producer['ack']}
        assert len(ack)==len(producer['ack'])
        initial={r[0]:r for r in before['ack']}
        assert len(initial)==6600 and all(r[4]<release for r in initial.values())
        assert initial.keys()=={r[0] for r in producer['ack'] if r[5]!='continuing'}
        profile=Counter(r[1] for r in initial.values())
        queue_profiles.append([profile[p] for p in range(60)])
        for p in range(60):
            offsets=sorted(r[2] for r in ack.values() if r[1]==p)
            assert offsets==list(range(1,len(offsets)+1))
        completed={};observed={};certain_late=0;possible_late=0
        workers=[]
        for c in range(6):
            data=json.loads((episode/f'consumer-{c}.json').read_text())
            assert data['error'] is None
            previous_end=None;last_offsets={}
            for r in data['records']:
                ident,p,offset,created,start,end,service,cohort=r
                assert ident in ack and ident not in completed
                assert ack[ident][1:4]==[p,offset,created] and ack[ident][5]==cohort
                assert manifest['owners'][p]==c and offset==last_offsets.get(p,0)+1
                assert previous_end is None or start>=previous_end
                assert service>=0 and end>=start
                assert start>=release-.1
                previous_end=end;last_offsets[p]=offset;completed[ident]=r
                if ident in initial and end<=cutoff:
                    observed[ident]=r
                    latency=end-created;cc=clocks[f'consumer-sts-{c}']
                    lower=latency-(cc['maximum_offset_seconds']-producer_clock['minimum_offset_seconds'])
                    upper=latency-(cc['minimum_offset_seconds']-producer_clock['maximum_offset_seconds'])
                    certain_late+=lower>20;possible_late+=upper>20
            workers.append(data)
        unfinished=len(initial)-len(observed)
        certain_late+=unfinished;possible_late+=unfinished
        latency=[r[5]-r[3] for r in observed.values()]
        late=sum(v>20 for v in latency)+unfinished
        allowed=len(initial)-math.ceil(.99*len(initial))
        clock_class=('pass' if possible_late<=allowed else 'fail' if certain_late>allowed else 'ambiguous')
        assert unfinished==saved['initial_unfinished'] and late==saved['deadline_misses']
        assert percentile(latency)==saved['completion_p99_seconds']
        selected=next(c for c in predictions['candidates'] if c['response']==saved['response'])
        for name,separate,age in VARIANTS:
            computed=forecast(list(initial.values()),manifest['owners'],individual if separate else pooled,release,age)
            assert computed==selected['predictions'][name]
        assert all(a<c['conservative_rate'] for a,c in zip(selected['consumer_arrival_rates'],capacity))
        age_normalized=[];deviations=[]
        for r in sorted(initial.values(),key=lambda r:int(r[0].rsplit('-',1)[1])):
            serial=int(r[0].rsplit('-',1)[1]);actual=release-r[3]
            expected=(8+saved['hold_seconds']-serial/3600 if r[5]=='preload'
                      else 6-(serial-3600)/600)
            deviations.append(actual-expected)
            age_normalized.append(actual-(saved['hold_seconds'] if r[5]=='preload' else 0))
        assert max(abs(v) for v in deviations)<=.1
        age_profiles.append(age_normalized)
        # These are message-accounting curves, not committed-offset lag. Known
        # created records include input still in delivery at a sampled instant.
        created_times=sorted(r[3] for r in ack.values())
        completion_times=sorted(r[5] for r in completed.values())
        initial_end_times=sorted(r[5] for r in observed.values())
        timeline=[]
        for second in range(0,101,2):
            t=release+second
            pending=bisect_right(created_times,t)-bisect_right(completion_times,t)
            assert pending>=0
            timeline.append(dict(seconds=second,pending_messages=pending,
                initial_pending=len(initial)-bisect_right(initial_end_times,t)))
        final_samples=[r['pending_messages'] for r in timeline if 60<r['seconds']<=90]
        final_completions=bisect_right(completion_times,release+90)-bisect_right(completion_times,release+60)
        first_low=None
        for index in range(len(timeline)-14):
            window=timeline[index:index+15]
            if window[-1]['seconds']<=90 and all(r['pending_messages']<=100 for r in window):
                first_low=window[0]['seconds'];break
        service_order=[]
        for c,data in enumerate(workers):
            initial_records=[r for r in data['records'] if r[0] in observed]
            if not initial_records:continue
            last=max(r[5] for r in initial_records)
            preceding=[r for r in data['records'] if r[5]<=last]
            newer=[r for r in preceding if r[7]=='continuing']
            service_order.append(dict(consumer=c,
                last_initial_completion_after_release=last-release,
                continuing_completions_before_last_initial=len(newer),
                processing_seconds_for_those_continuing_messages=sum(r[6] for r in newer),
                observed_task_rate_until_last_initial=len(preceding)/sum(r[6] for r in preceding),
                calibrated_effective_rate=individual[c]))
        rows.append(dict(**saved,deadline_miss_percent=100*late/6600,
            clock_robust_classification=clock_class,
            clock_bounded_miss_counts=[certain_late,possible_late],
            predictions=selected['predictions'],consumer_arrival_rates=selected['consumer_arrival_rates'],
            maximum_creation_schedule_error=before['maximum_schedule_error'],
            maximum_age_schedule_deviation=max(abs(v) for v in deviations),
            continuing_maximum_schedule_error=producer['maximum_schedule_error'],
            final_30s_completion_throughput=final_completions/30,
            final_15_sample_mean_pending=sum(final_samples)/len(final_samples),
            pending_at_input_end=timeline[45]['pending_messages'],
            pending_at_observation_end=timeline[-1]['pending_messages'],
            first_low_pending_15_sample_window_start=first_low,
            timeline=timeline,service_order_diagnostic=service_order))
    assert len(set(tuple(p) for p in queue_profiles))==1
    age_spread=max(max(a)-min(a) for a in zip(*age_profiles))
    original={p['pod']:(p['uid'],p['node']) for p in execution['before'] if int(p['pod'].rsplit('-',1)[1])<3}
    after={p['pod']:(p['uid'],p['node']) for p in execution['after']}
    assert all(after[p]==value for p,value in original.items())
    prepared={p['pod']:(p['uid'],p['node']) for p in execution['prepared']}
    assert all(after[p]==value for p,value in prepared.items())
    return dict(kind='controlled-age final-assignment diagnostic',execution_status=execution['status'],
        token=execution['token'],archive_sha256=execution['archive_sha256'],remote=execution['remote'],
        protocol=protocol,capacity=capacity,rows=rows,
        validation=dict(completed_episodes=len(rows),initial_partition_counts=queue_profiles[0],
            matched_counts=True,maximum_age_profile_difference_after_normalizing_hold_seconds=age_spread,
            original_three_pods_unchanged=True,all_six_prepared_pods_unchanged=True,
            restored_replicas=execution['restored_replicas'],
            distinct_ids=True,offset_continuity=True,predictions_before_release=True,
            clock_robust_episodes=sum(r['clock_robust_classification']!='ambiguous' for r in rows)),
        false_successes={name:sum(r['predictions'][name]['meets_deadline_objective'] and not r['deadline_success'] for r in rows)
                         for name,_,_ in VARIANTS},
        limitations=[
            'Final assignments are installed before release; the diagnostic does not include an actual scaling or reassignment transition.',
            'All six pods remain allocated; active consumer counts do not represent measured resource-cost differences.',
            'Capacity is estimated from one specified busy 60-second interval per consumer on shared nodes.',
            'Continuing input lasts 90 seconds; bounded pending work here is not a guarantee of indefinite stability.',
            'Predictions assume oldest-first service across partitions, which actual Kafka consume batches do not guarantee.',
            'The 20-second waiting-cohort objective is diagnostic and does not replace the proposal SLO.',
            'This is a candidate-assignment test, not an evaluation of a complete published or proposed controller.'])


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('directory',type=Path)
    parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args();result=analyze(args.directory)
    args.output.write_text(json.dumps(result,indent=2,allow_nan=False)+'\n')
    print(json.dumps(result['validation'],indent=2))
    for r in result['rows']:
        print(r['id'],round(r['completion_p99_seconds'],3),r['deadline_misses'],
              r['clock_robust_classification'],r['pending_at_input_end'])
