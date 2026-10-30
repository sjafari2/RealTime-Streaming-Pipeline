"""Compare sustainable three- and six-consumer assignments at controlled ages.

This diagnostic installs final assignments before release. It measures neither
the transition between assignments nor pod-startup cost. Creation timestamps
are actual producer timestamps; no message timestamp is altered to simulate age.
"""
import argparse
import json
import math
import time
from pathlib import Path

from kafka_age_probe import put, read, wait_for, topic_ready, percentile
from capacity_age_ablation import forecast, assigned_rates, VARIANTS


def busy_capacity(records, resources, release, initial_count):
    """Estimate effective rate in a specified busy 60-second measurement interval."""
    a, middle, b = release+10, release+40, release+70
    counts = [sum(left < r[5] <= right for r in records)
              for left, right in [(a, middle), (middle, b)]]
    rates = [n/30 for n in counts]
    inside = [r for r in records if a <= r[4] <= b]
    gaps = [r[4]-p[5] for p, r in zip(inside, inside[1:])]
    relevant = [r for r in resources if a-2 <= r[0] <= b+2]
    reasons = []
    if not records or records[0][4] > a or records[-1][5] < b:
        reasons.append('processing does not span the measurement interval')
    if sum(r[5] <= b for r in records) >= initial_count:
        reasons.append('preloaded work depleted before interval end')
    if not relevant or relevant[-1][4] != relevant[0][4]:
        reasons.append('an empty poll occurred within the busy interval')
    if not gaps or max(gaps) > .25:
        reasons.append('a processing gap exceeds 250 ms')
    if min(rates) <= 0 or max(rates)/min(rates) > 1.2:
        reasons.append('half-interval rates differ by more than 20 percent')
    return dict(interval_seconds=[10,70], completions=sum(counts),
        effective_rate=sum(counts)/60, half_interval_rates=rates,
        conservative_rate=.85*min(rates),
        eligible=not reasons, reasons=reasons,
        maximum_interrecord_gap=max(gaps) if gaps else None,
        task_rate=len(inside)/sum(r[6] for r in inside) if inside else None)


def coordinator(root, bootstrap):
    from confluent_kafka import Producer
    from confluent_kafka.admin import AdminClient, NewTopic
    if read('/config/run-control.json')['state'] != 'stopped':
        raise RuntimeError('Managed run must remain stopped')
    wait_for(lambda: all((root/f'worker-{i}.json').exists() for i in range(6)))
    producer = Producer({'bootstrap.servers':bootstrap,'acks':'all',
        'enable.idempotence':True,'linger.ms':5,'compression.type':'lz4',
        'queue.buffering.max.messages':100000})
    admin = AdminClient({'bootstrap.servers':bootstrap})
    arms = {'redistribute3':[p%3 for p in range(60)],
            'targeted6':[p%6 for p in range(60)]}
    order = [(h,n,1) for h in (0,8) for n in arms]
    order += [(h,n,2) for h,n,_ in reversed(order)]
    protocol = dict(primary_deadline_seconds=20, required_fraction=.99,
        total_input_rate=600, partitions=60, high_input_partitions=list(range(12)),
        high_input_fraction=.8, initial_count=6600, common_trace_seconds=5,
        continuing_input_seconds=90, observation_seconds=100,
        added_hold_seconds=[0,8], planned_preload_age_range_seconds=[[7,8],[15,16]],
        maximum_creation_schedule_error_seconds=.1, calibration_window_seconds=[10,70],
        capacity_guard='Input below 85% of the smaller calibrated 30-second rate for every consumer',
        order=[dict(hold=h,response=n,run=r) for h,n,r in order],
        assignments=arms, allocated_pods=6,
        scope='Final assignments installed in advance; no observed transition or resource saving')
    put(root/'protocol.json',protocol)
    summaries=[]; sequence=0; capacity=None

    def episode(label, owners, hold=0, calibration=False):
        nonlocal sequence
        sequence+=1; eid=f'{sequence:02d}-{label}'; dest=root/eid; dest.mkdir()
        topic=root.name+'-'+eid
        admin.create_topics([NewTopic(topic,num_partitions=60,replication_factor=1,
            config={'retention.ms':'86400000'})])[topic].result(30)
        topic_ready(producer,topic)
        # Prime every destination before recording creation times. Workers start
        # at offset 1, beyond these explicit connection-preparation markers.
        prime_errors=[]
        for p in range(60):
            producer.produce(topic,partition=p,value=b'connection-primer',
                on_delivery=lambda err,msg: prime_errors.append(str(err)) if err else None)
        if producer.flush(30) or prime_errors:
            raise RuntimeError('Connection preparation failed')
        manifest=dict(id=eid,topic=topic,group=topic+'-group',bootstrap=bootstrap,
            owners=owners,iterations=2000,start_offset=1,calibration=calibration,
            observation_seconds=80 if calibration else 100,hold_seconds=hold,
            preparation_timeout_seconds=600)
        put(dest/'manifest.json',manifest); put(root/'episode.json',manifest)
        wait_for(lambda:all((dest/f'ready-{i}.json').exists() for i in range(6)),90)
        ack=[]; counts=[0]*60; errors=[]; serial=0; schedule_errors=[]
        payload=b'x'*100
        def send(part,cohort,due=None):
            nonlocal serial
            if due is not None:
                while time.time()<due:
                    time.sleep(min(.001,max(0,due-time.time())))
            ident=f'{eid}-{serial}'; serial+=1; created=time.time()
            if due is not None: schedule_errors.append(created-due)
            def delivered(err,msg):
                if err: errors.append(str(err))
                else:
                    counts[part]+=1
                    if not calibration:
                        ack.append([ident,part,msg.offset(),created,time.time(),cohort])
            while True:
                try:
                    producer.produce(topic,partition=part,value=payload,
                        headers={'id':ident,'created':repr(created),'cohort':cohort},on_delivery=delivered)
                    break
                except BufferError:
                    producer.poll(.01)
            producer.poll(0)
        def flush():
            if producer.flush(30) or errors: raise RuntimeError('Delivery did not finish successfully')
        def trace(begin,seconds,cohort):
            for k in range(600*seconds):
                if k%600==0 and read(root/'stop.json'): raise RuntimeError('Stopped')
                block,pos=divmod(k,5)
                part=(4*block+pos)%12 if pos<4 else 12+block%48
                send(part,cohort,begin+k/600)
            flush()
        if calibration:
            # Aggregate producer acknowledgments keep a large preload from
            # exhausting the producer's memory limit. Consumer records are saved.
            for k in range(8000):
                if k%100==0 and read(root/'stop.json'): raise RuntimeError('Stopped')
                for p in range(60): send(p,'calibration')
            flush(); release=time.time()+1
            put(dest/'producer-before.json',dict(acknowledged_counts=counts.copy(),errors=errors))
        else:
            start=time.time()+1
            for k in range(3600): send(k%12,'preload',start+k/3600)
            flush()
            common_start=start+2+hold
            if time.time()>common_start-.1:
                raise RuntimeError('Preload delivery missed its preparation boundary')
            trace(common_start,5,'common')
            release=start+8+hold
            before=ack.copy()
            put(dest/'producer-before.json',dict(ack=before,errors=errors,counts=counts.copy(),
                preparation_start=start,common_start=common_start,
                maximum_schedule_error=max(schedule_errors)))
            if max(schedule_errors)>.1 or time.time()>release-.2:
                raise RuntimeError('Creation or acknowledgment timing exceeded the frozen preparation tolerance')
            measured_rates=[sum(r[1]==p and r[5]=='common' for r in before)/5 for p in range(60)]
            individual=[c['effective_rate'] for c in capacity]
            conservative=[c['conservative_rate'] for c in capacity]
            pooled=[sum(individual[:3])/3]*6
            candidates=[]
            for name,mapping in arms.items():
                inputs=assigned_rates(measured_rates,mapping,6)
                if any(a>=mu for a,mu in zip(inputs,conservative)):
                    raise RuntimeError('A candidate fails the per-consumer capacity guard')
                predictions={tag:forecast(before,mapping,individual if separate else pooled,
                    release,age) for tag,separate,age in VARIANTS}
                candidates.append(dict(response=name,consumer_arrival_rates=inputs,
                    predictions=predictions))
            put(dest/'predictions.json',dict(saved_epoch=time.time(),candidates=candidates,
                observed_response=label.split('-hold')[0],
                assumption='Oldest-first across assigned partitions; no modeled activation delay'))
        put(dest/'release.json',dict(release_epoch=release))
        if not calibration: trace(release,90,'continuing')
        put(dest/'producer.json',dict(ack=ack,acknowledged_counts=counts,
            errors=errors,release_epoch=release,maximum_schedule_error=max(schedule_errors,default=0)))
        wait_for(lambda:all((dest/f'consumer-{i}.json').exists() for i in range(6)),manifest['observation_seconds']+45)
        data=[read(dest/f'consumer-{i}.json') for i in range(6)]
        if any(d['error'] for d in data): raise RuntimeError('Consumer processing error')
        if calibration:
            estimates=[busy_capacity(d['records'],d['resources'],release,
                sum(counts[p] for p,c in enumerate(owners) if c==i)) for i,d in enumerate(data)]
            put(dest/'capacity.json',estimates)
            summary=dict(id=eid,calibration=True,capacity=estimates)
        else:
            initial={r[0]:r for r in before}; cutoff=release+100
            records=[r for d in data for r in d['records'] if r[5]<=cutoff]
            if len({r[0] for r in records})!=len(records): raise RuntimeError('Duplicate completion')
            completed={r[0]:r for r in records if r[0] in initial}
            late=sum(r[5]-r[3]>20 for r in completed.values())+len(initial)-len(completed)
            summary=dict(id=eid,calibration=False,hold_seconds=hold,
                response=label.split('-hold')[0],run=int(label.rsplit('run',1)[1]),
                initial_count=len(initial),initial_unfinished=len(initial)-len(completed),
                age_p99_seconds=percentile([release-r[3] for r in before]),
                completion_p99_seconds=percentile([r[5]-r[3] for r in completed.values()]),
                deadline_misses=late,deadline_success=late<=len(initial)-math.ceil(.99*len(initial)))
        put(dest/'summary.json',summary); summaries.append(summary); put(root/'summary.json',summaries)
        print(json.dumps(summary),flush=True)
        return summary
    try:
        result=episode('calibration',arms['targeted6'],calibration=True)
        capacity=result['capacity']
        if not all(c['eligible'] for c in capacity):
            raise RuntimeError('Busy calibration did not pass for every candidate consumer')
        for name,mapping in arms.items():
            inputs=assigned_rates([40]*12+[2.5]*48,mapping,6)
            if any(a>=c['conservative_rate'] for a,c in zip(inputs,capacity)):
                raise RuntimeError('Candidate '+name+' fails capacity admission')
        put(root/'capacity-admission.json',dict(passed=True,capacity=capacity,assignments=arms))
        for hold,name,run in order:
            episode(f'{name}-hold{hold}-run{run}',arms[name],hold)
    finally:
        put(root/'stop.json',dict(time=time.time()))


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('role',choices=['coordinator'])
    parser.add_argument('--root',type=Path,required=True)
    parser.add_argument('--bootstrap',default='pip-kafka:9092')
    args=parser.parse_args();coordinator(args.root,args.bootstrap)
