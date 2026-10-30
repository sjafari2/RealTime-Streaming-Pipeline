"""Isolated Kafka queue-age probe; not the managed performance campaign.

Workers use explicit assignments, the existing SHA-256 task and batches of 25.
All six pods are prepared before measurement. Consequently this probe measures
queue-age sensitivity under candidate assignments, not Kubernetes scale-out or
handover overhead, and does not estimate resource savings from fewer pods.
"""
import argparse
import hashlib
import json
import math
import os
import random
import time
from pathlib import Path


def put(path, value):
    path=Path(path); tmp=path.with_suffix(path.suffix+'.tmp')
    tmp.write_text(json.dumps(value, allow_nan=False)); tmp.replace(path)


def read(path):
    try: return json.loads(Path(path).read_text())
    except FileNotFoundError: return None


def wait_for(predicate, timeout=90):
    until=time.monotonic()+timeout
    while time.monotonic()<until:
        value=predicate()
        if value: return value
        time.sleep(.05)
    raise TimeoutError('Probe condition did not become ready')


def topic_ready(client, topic):
    """Creation can finish before every broker's metadata cache is current."""
    def available():
        metadata=client.list_topics(topic=topic,timeout=5)
        value=metadata.topics.get(topic)
        return (value and not value.error and len(value.partitions)==60
                and all(not p.error and p.leader>=0 for p in value.partitions.values()))
    wait_for(available,60)


def percentile(values, p=.99):
    return sorted(values)[math.ceil(p*len(values))-1] if values else None


def worker(root, index):
    from confluent_kafka import Consumer, TopicPartition
    import psutil
    started=time.monotonic(); seen=set(); proc=psutil.Process()
    put(root/f'worker-{index}.json', {'ready':True,'wall_time':time.time(),'pid':os.getpid()})
    while time.monotonic()-started < 2400:
        manifest=read(root/'episode.json')
        if read(root/'stop.json'): return
        if not manifest or manifest['id'] in seen:
            time.sleep(.1); continue
        episode=root/manifest['id']; episode.mkdir(exist_ok=True)
        seen.add(manifest['id'])
        owners=manifest['owners']; parts=[p for p,c in enumerate(owners) if c==index]
        consumer=None; records=[]; snapshots=[]; error=None
        if parts:
            consumer=Consumer({'bootstrap.servers':manifest['bootstrap'],
                'group.id':manifest['group'], 'client.id':f'age-probe-{index}',
                'enable.auto.commit':False, 'enable.auto.offset.store':False,
                'auto.offset.reset':'earliest','fetch.wait.max.ms':5,'fetch.min.bytes':1,
                'max.poll.interval.ms':120000, 'queued.min.messages':1000})
            topic_ready(consumer,manifest['topic'])
            consumer.assign([TopicPartition(manifest['topic'],p,manifest.get('start_offset',0)) for p in parts])
            # Preparation occurs while the fresh topic is empty. Initialize the
            # consumer connection before the coordinator releases the preload.
            for _ in range(3):
                early=consumer.consume(num_messages=25,timeout=.1)
                if early: raise RuntimeError('Fresh preparation topic was not empty')
        put(episode/f'ready-{index}.json', {'partitions':parts,'wall_time':time.time()})
        try:
            gate=wait_for(lambda:read(episode/'release.json') or read(root/'stop.json'),manifest.get('preparation_timeout_seconds',180))
            if 'release_epoch' not in gate: raise RuntimeError('Probe stopped before release')
            while time.time()<gate['release_epoch']:
                if read(root/'stop.json'): raise RuntimeError('Probe stopped before scheduled release')
                time.sleep(min(.01,max(0,gate['release_epoch']-time.time())))
            end=gate['release_epoch']+manifest['observation_seconds']
            last_snapshot=0; done=0; idle=0; last_offset={}
            while time.time()<end:
                if read(root/'stop.json'): break
                if not parts:
                    time.sleep(.05); continue
                messages=consumer.consume(num_messages=25,timeout=.1)
                if not messages: idle+=1
                for msg in messages:
                    if msg.error(): raise RuntimeError(str(msg.error()))
                    if time.time()>=end: break
                    p=msg.partition(); off=msg.offset()
                    if p in last_offset and off != last_offset[p]+1:
                        raise ValueError('Non-contiguous partition processing')
                    last_offset[p]=off
                    header={k:v.decode() for k,v in msg.headers()}
                    began=time.time(); mono=time.monotonic(); value=msg.value()
                    for _ in range(manifest['iterations']): value=hashlib.sha256(value).digest()
                    service=time.monotonic()-mono; finished=time.time()
                    records.append([header['id'],p,off,float(header['created']),began,finished,
                                    service,header['cohort']])
                    done+=1
                if time.monotonic()-last_snapshot>=1:
                    snapshots.append([time.time(),done,proc.cpu_times().user+proc.cpu_times().system,
                                      proc.memory_info().rss,idle])
                    last_snapshot=time.monotonic()
        except Exception as exc:
            error=repr(exc)
        finally:
            if consumer: consumer.close()
            put(episode/f'consumer-{index}.json',dict(records=records,resources=snapshots,error=error,
                                                     pod=os.getenv('POD_NAME'),parts=parts))
        if error: raise RuntimeError(error)
    raise TimeoutError('Worker lifetime exhausted')


def coordinator(root, bootstrap):
    from confluent_kafka import Producer
    from confluent_kafka.admin import AdminClient,NewTopic
    from transition_audit import decide,spread_assignment
    # Refuse to overlap a managed pipeline run.
    control=read('/config/run-control.json')
    if not control or control.get('state')!='stopped':
        raise RuntimeError('Managed pipeline must be stopped')
    root.mkdir(exist_ok=True,parents=True)
    wait_for(lambda:all((root/f'worker-{i}.json').exists() for i in range(6)))
    admin=AdminClient({'bootstrap.servers':bootstrap})
    # Reuse the initialized producer so idempotence setup does not change the
    # age of one response's preload more than another's.
    producer=Producer({'bootstrap.servers':bootstrap,'acks':'all','enable.idempotence':True,
                       'linger.ms':5,'compression.type':'lz4','queue.buffering.max.messages':100000})
    all_summary=[]; counter=0
    experiment_token=root.name.replace('_','-').lower()
    def episode(label,owners,preload,hold,production,observe,calibration=False):
        nonlocal counter
        if read(root/'stop.json'): raise RuntimeError('Probe stopped')
        counter+=1; eid=f'{counter:02d}-{label}'; dest=root/eid; dest.mkdir()
        topic=f'{experiment_token}-{eid}'
        admin.create_topics([NewTopic(topic,num_partitions=60,replication_factor=1,
                          config={'retention.ms':'86400000'})])[topic].result(30)
        topic_ready(producer,topic)
        ack=[]; errors=[]; serial=0; payload=b'x'*100
        def send(p,cohort):
            nonlocal serial
            ident=f'{eid}-{serial}'; serial+=1; created=time.time()
            def delivery(err,msg):
                if err: errors.append(str(err))
                else: ack.append([ident,p,msg.offset(),created,time.time(),cohort])
            producer.produce(topic,partition=p,value=payload,
                             headers={'id':ident,'created':repr(created),'cohort':cohort},on_delivery=delivery)
            producer.poll(0)
        manifest=dict(id=eid,topic=topic,group=topic+'-group',bootstrap=bootstrap,
                      owners=owners,observation_seconds=observe,iterations=2000,
                      initial_backlog_per_partition=preload,hold_seconds=hold,
                      continuing_input_seconds=production,calibration=calibration)
        put(dest/'manifest.json',manifest); put(root/'episode.json',manifest)
        wait_for(lambda:all((dest/f'ready-{i}.json').exists() for i in range(6)),90)
        for k in range(max(preload)):
            for p,n in enumerate(preload):
                if k<n: send(p,'preload')
        if producer.flush(30) or errors: raise RuntimeError('Preload delivery incomplete')
        preload_end=time.time()
        time.sleep(hold)
        # During this identical final five-second preparation trace, application
        # processing remains gated. Backlog counts and recent input are matched.
        common_seconds=0 if calibration else 5
        def produce_trace(seconds,cohort):
            begin=time.monotonic()
            # Exactly 80% of a deterministic 600/s trace goes to twelve partitions.
            for k in range(int(600*seconds)):
                if k%600==0 and read(root/'stop.json'): raise RuntimeError('Probe stopped')
                due=begin+k/600
                while time.monotonic()<due:
                    time.sleep(min(.001,max(0,due-time.monotonic())))
                block,pos=divmod(k,5)
                p=(4*block+pos)%12 if pos<4 else 12+block%48
                send(p,cohort)
            if producer.flush(30) or errors: raise RuntimeError('Trace delivery incomplete')
        trace_begin=time.time()
        produce_trace(common_seconds,'common')
        trace_end=time.time()
        release=time.time(); put(dest/'producer-before.json',dict(ack=ack.copy(),errors=errors,
                                                                  preload_end=preload_end))
        if not calibration:
            # Freeze predictions before releasing processing. The ablation uses
            # the same capacity, assignment and service assumptions; only age differs.
            common_counts=[0]*60
            for record in ack:
                if record[5]=='common': common_counts[record[1]]+=1
            measured_rates=[n/(trace_end-trace_begin) for n in common_counts]
            measured_rule=decide(measured_rates,backlog,current,3,mu,20,0)
            put(dest/'decision-inputs.json',dict(measured_rates=measured_rates,
                common_trace_seconds=trace_end-trace_begin,backlog=backlog,
                measured_rule=measured_rule,reference_rule=decision))
            if measured_rule['owners']!=decision['owners'] or measured_rule['consumers']!=decision['consumers']:
                raise ValueError('Measured input changes the frozen baseline assignment; preparation is invalid')
            predicted=[]
            for candidate_name,candidate_owners in [('baseline',decision['owners']),
                                                     ('targeted6',spread_assignment(6))]:
                end_times={}; age_latencies=[]; blind_latencies=[]
                for record in sorted(ack,key=lambda row:(row[3],row[0])):
                    owner=candidate_owners[record[1]]
                    end_times[owner]=end_times.get(owner,0)+1/mu
                    blind_latencies.append(end_times[owner])
                    age_latencies.append(release-record[3]+end_times[owner])
                predicted.append(dict(response=candidate_name,
                    active_consumers=len(set(candidate_owners)),
                    p99_age_blind=percentile(blind_latencies),
                    p99_age_aware=percentile(age_latencies)))
            put(dest/'predictions.json',dict(at_epoch=release,candidates=predicted,
                observed_response=label.split('-hold')[0],
                service_assumption='Oldest queued message first across assigned partitions',
                capacity_assumption='Same pooled mean task rate for both predictors',
                activation_assumption='Prestarted workers and zero modeled transition delay'))
        put(dest/'release.json', {'release_epoch':release})
        produce_trace(production,'continuing')
        put(dest/'producer.json',dict(ack=ack,errors=errors,release_epoch=release))
        wait_for(lambda:all((dest/f'consumer-{i}.json').exists() for i in range(6)),observe+45)
        messages=[]; worker_summaries=[]
        for i in range(6):
            value=read(dest/f'consumer-{i}.json')
            if value['error']: raise RuntimeError(value['error'])
            messages.extend(value['records'])
            worker_summaries.append({'consumer':i,'count':len(value['records']),
                'task_rate':len(value['records'])/sum(r[6] for r in value['records']) if value['records'] else None})
        ids=[r[0] for r in messages]
        if len(ids)!=len(set(ids)): raise RuntimeError('Duplicate completions')
        acknowledgments={r[0]:r for r in ack}
        if any(r[0] not in acknowledgments for r in messages): raise RuntimeError('Unknown completion')
        initial=[r for r in ack if r[5]!='continuing']; initial_ids={r[0] for r in initial}
        initial_done=[r for r in messages if r[0] in initial_ids and r[5]<=release+observe]
        completed_ids={r[0] for r in initial_done}
        ages=[release-r[3] for r in initial]
        latency=[r[5]-r[3] for r in initial_done]
        if any(x<-.05 for x in latency): raise RuntimeError('Negative latency exceeds clock tolerance')
        summary=dict(id=eid,active_consumers=len(set(owners)),hold_seconds=hold,
          allocated_pods=6,planned_consumers=(6 if calibration or label.startswith('targeted6') else decision['consumers']),
          calibration=calibration,initial_count=len(initial),initial_completed=len(initial_done),
          initial_unfinished=len(initial)-len(initial_done),age_min=min(ages),age_max=max(ages),
          age_p99=percentile(ages),initial_completion_p99=percentile(latency),
          deadline_misses={str(d):sum(x>d for x in latency)+sum(release+observe-r[3]>d
                          for r in initial if r[0] not in completed_ids) for d in (10,15,20)},
          consumers=worker_summaries,release_epoch=release,observation_seconds=observe,
          producer_errors=errors,distinct_completion_records=len(messages))
        put(dest/'summary.json',summary); all_summary.append(summary)
        put(root/'summary.json',all_summary)
        print(json.dumps(summary),flush=True)
        return summary
    try:
        # Task-rate diagnostic uses busy work but is not a full capacity calibration.
        calibration=episode('calibration',[p%6 for p in range(60)],
                            [1200]*60,0,0,60,True)
        mu=sum(c['task_rate'] for c in calibration['consumers'][:3])/3
        if any(c['count']<1000 for c in calibration['consumers']):
            raise ValueError('Insufficient calibration processing')
        rates=[40.]*12+[2.5]*48
        # 300 preload records/hot partition plus the common 5-second trace.
        # Save the exact deterministic common-trace partition counts instead.
        backlog=[300]*12+[0]*48
        for k in range(3000):
            b,pos=divmod(k,5); p=(4*b+pos)%12 if pos<4 else 12+b%48
            backlog[p]+=1
        current=[0]*12+[p%3 for p in range(12,60)]
        decision=decide(rates,backlog,current,3,mu,20,0)
        if decision['consumers']>6: raise ValueError('Baseline requires more than six prepared consumers')
        # This pilot isolates age at fixed candidate assignments; observed pod
        # startup and ownership-transfer costs need a separate managed campaign.
        put(root/'decision.json',dict(rule=decision,pooled_task_capacity=mu,
            rates=rates,backlog=backlog,assumed_pause=0,deadline=20,
            warning='Paper reconstruction; task capacity is not end-to-end pipeline capacity.'))
        arms=[('baseline',decision['owners']),('targeted6',spread_assignment(6))]
        order=[(age,name,owners,1) for age in (0,8) for name,owners in arms]
        order += [(age,name,owners,2) for age,name,owners,_ in reversed(order)]
        put(root/'protocol.json',dict(order=[{'hold_seconds':a,'response':n,'run':r} for a,n,_,r in order],
            measurement='Initial acknowledged cohort at gate release; no fake creation timestamps.',
            primary_deadline_seconds=20,secondary_deadlines_seconds=[10,15],
            scope='Isolated assignment probe; all six pods prestarted. Not a scale-out or cost benchmark.'))
        for age,name,owners,run in order:
            episode(f'{name}-hold{age}-run{run}',owners,[300]*12+[0]*48,age,20,30)
    finally:
        put(root/'stop.json',{'time':time.time()})


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('role',choices=['worker','coordinator']);p.add_argument('--root',type=Path,required=True)
    p.add_argument('--index',type=int);p.add_argument('--bootstrap',default='pip-kafka:9092')
    a=p.parse_args()
    if a.role=='worker': worker(a.root,a.index)
    else: coordinator(a.root,a.bootstrap)
