"""Follow waiting cohorts and partition service in the saved Kafka trials."""
import argparse
import bisect
from collections import Counter, defaultdict, namedtuple
import csv
import gzip
import hashlib
import json
import math
from pathlib import Path
import statistics

from analyze_scheduling import describe, digest, percentile

Ack = namedtuple('Ack', 'producer_timestamp acknowledged topic partition offset')
Done = namedtuple('Done', 'message_id completion_timestamp producer_timestamp pod incarnation '
                 'assignment_epoch topic partition offset processing_start_timestamp processing_seconds output_sha256')


def cohort_stats(ids, ack, done, anchor, activation, end, cutoff, deadline):
    """The denominator is the entire cohort, including unfinished messages."""
    completed = sorted(done[key].completion_timestamp for key in ids
                       if key in done and done[key].completion_timestamp <= cutoff)
    latencies = [done[key].completion_timestamp - ack[key].producer_timestamp for key in ids
                 if key in done and done[key].completion_timestamp <= cutoff]
    def reached(fraction):
        rank = math.ceil(fraction * len(ids))
        return completed[rank - 1] - anchor if rank and len(completed) >= rank else None
    def pending(at):
        return sum(ack[key].acknowledged <= at and
                   (key not in done or done[key].completion_timestamp > at) for key in ids)
    known_late, censored = 0, 0
    for key in ids:
        finished = done[key].completion_timestamp if key in done else math.inf
        if finished <= cutoff:
            known_late += finished - ack[key].producer_timestamp > deadline
        elif ack[key].producer_timestamp + deadline <= cutoff:
            known_late += 1
        else:
            censored += 1
    return dict(messages=len(ids), completed_by_cutoff=len(completed), unfinished=len(ids)-len(completed),
        unfinished_fraction=(len(ids)-len(completed))/len(ids) if ids else None,
        completed_by_evaluation_end=bisect.bisect_right(completed, end),
        pending_at_activation=pending(activation), pending_at_evaluation_end=pending(end),
        median_age_at_anchor_seconds=statistics.median(anchor-ack[key].producer_timestamp for key in ids) if ids else None,
        mean_age_at_anchor_seconds=statistics.mean(anchor-ack[key].producer_timestamp for key in ids) if ids else None,
        deadline_already_expired_at_anchor=sum(ack[key].producer_timestamp+deadline <= anchor for key in ids),
        completion_p99_seconds=percentile(latencies, .99),
        completion_mean_seconds=statistics.mean(latencies) if latencies else None,
        deadline_misses_known_by_cutoff=known_late, deadline_censored=censored,
        cohort_50_percent_completed_seconds=reached(.5), cohort_95_percent_completed_seconds=reached(.95),
        all_completed_seconds=reached(1),
        clearance_curve=[dict(seconds=offset, completed=bisect.bisect_right(completed, anchor+offset))
                         for offset in range(0, int(cutoff-anchor)+1, 5)] if ids else [])


def pending_ids(ack, done, anchor):
    return {key for key, value in ack.items() if value.acknowledged <= anchor and
            (key not in done or done[key].completion_timestamp > anchor)}


def decision_groups(ack, done, decision):
    groups = {name: set() for name in ('pending_at_decision', 'produced_after_decision',
              'completed_before_decision', 'earlier_produced_acknowledged_later')}
    for key, value in ack.items():
        if value.producer_timestamp >= decision:
            name = 'produced_after_decision'
        elif value.acknowledged > decision:
            name = 'earlier_produced_acknowledged_later'
        elif key in done and done[key].completion_timestamp <= decision:
            name = 'completed_before_decision'
        else:
            name = 'pending_at_decision'
        groups[name].add(key)
    return groups


def ownership_coverage(snapshots, owners, start, end):
    """Invalid ownership observations stay missing rather than becoming zeros."""
    selected = [s for s in snapshots if start <= s['timestamp'] < end]
    valid = [s for s in selected if s['valid']]
    matches = [s for s in valid if
               {int(k.rsplit('/',1)[1]):v.split('/')[0] for k,v in s['owners'].items()} == owners]
    return dict(expected_samples=round((end-start)/2), recorded_samples=len(selected),
                valid_samples=len(valid), matching_activation_owner_samples=len(matches))


def checked_activation(directory, manifest, row, windows, snapshots, source_paths):
    evaluation = manifest['evaluation_start_epoch']
    recorded = [w for w in windows if w['run_id'] == row['run_id'] and w['interval'] == 'Active–end']
    if len(recorded) != 1:
        raise ValueError('Expected one saved activation boundary')
    boundary = recorded[0]['start_epoch']
    transfer = None
    if row['arm'] == 'keep3':
        if not math.isclose(boundary, evaluation+60, rel_tol=0, abs_tol=1e-6):
            raise ValueError('Invalid no-action reference')
        eligible = [s for s in snapshots if s['valid'] and s['timestamp'] <= boundary+.001]
        owners = {int(k.rsplit('/', 1)[1]): v.split('/')[0] for k,v in eligible[-1]['owners'].items()}
        basis = 'Scheduled second 60, with no action'
    elif row['arm'] in ('redistribute3', 'scale_redistribute6'):
        path = directory/'explicit-handoff-events.jsonl'
        source_paths.add(path)
        events = [json.loads(line) for line in path.read_text().splitlines()]
        active = [event for event in events if event['event'] == 'active_verified']
        releases = [event for event in events if event['event'] == 'release_requested']
        if len(active) != 1 or len(releases) != 1 or active[0]['timestamp'] != boundary:
            raise ValueError('Explicit activation does not match the recorded interval')
        owners = {}
        for pod, status in active[0]['statuses'].items():
            if status['run_id'] != row['run_id'] or any(status.get(k) for k in ('failure','evidence_dropped','evidence_error')):
                raise ValueError('Invalid active status')
            if status.get('explicit_assignment',{}).get('stage') != 'active':
                raise ValueError('An owner has not reached the active assignment stage')
            for _, partition in status['assignments']:
                if partition in owners:
                    raise ValueError('Duplicate owner in activation receipt')
                owners[partition] = pod
        target = {p['partition']:p['owner'] for p in manifest['intervention']['target_assignment']}
        if owners != target:
            raise ValueError('Target ownership differs from activation receipt')
        transfer = releases[0]['timestamp']
        basis = 'Explicit active_verified receipt; release_requested is a coordinator boundary, not a measured global processing pause'
    else:
        # Reconstruct the published confirmation rule from the saved samples.
        journal = directory/'intervention-events.jsonl'
        source_paths.add(journal)
        decisions = [e['timestamp'] for e in map(json.loads, journal.read_text().splitlines()) if e['event']=='decision']
        if len(decisions) != 1:
            raise ValueError('Missing native scaling decision')
        first = previous = confirmation = None
        for snapshot in snapshots:
            if snapshot['timestamp'] < decisions[0]:
                continue
            observed = snapshot.get('owners', {})
            if not snapshot['valid'] or len(observed) != 60 or len({v.split('/')[0] for v in observed.values()}) != 6:
                first = previous = None
                continue
            if previous is None or observed != previous['owners'] or not 0 < snapshot['timestamp']-previous['timestamp'] <= 3:
                first = snapshot['timestamp']
            previous = snapshot
            if snapshot['timestamp']-first >= 10:
                confirmation = snapshot
                break
        if confirmation is None or confirmation['timestamp'] != boundary:
            raise ValueError('Native activation does not match the saved ten-second ownership check')
        owners = {int(k.rsplit('/',1)[1]):v.split('/')[0] for k,v in confirmation['owners'].items()}
        basis = 'Six-owner map confirmed unchanged for ten seconds'
    if len(owners) != 60 or not evaluation+60 <= boundary < manifest['producer_end_epoch']:
        raise ValueError('Invalid activation ownership or time')
    return boundary, owners, transfer, basis


def load_evidence(directory, manifest, source_paths):
    compressed_path = directory/'compressed-evidence.json'
    source_paths.add(compressed_path)
    compressed = json.loads(compressed_path.read_text())
    expected = {row['compressed_path']:row for row in compressed['files']}
    paths = sorted(directory.rglob('events.jsonl.gz'))
    if {str(p.relative_to(directory)) for p in paths} != set(expected):
        raise ValueError('Missing or extra compressed evidence')
    ack, done, physical, metadata = {}, {}, defaultdict(list), {}
    duplicates = 0
    for path in paths:
        receipt = expected[str(path.relative_to(directory))]
        if digest(path) != receipt['compressed_sha256']:
            raise ValueError('Compressed evidence hash mismatch: '+str(path))
        source_paths.add(path)
        final_path = path.parent/'final.json'
        source_paths.add(final_path)
        final = json.loads(final_path.read_text())
        if final['run_id'] != manifest['run_id'] or any(final.get(k) for k in ('failure','evidence_dropped','evidence_error')):
            raise ValueError('Incomplete process evidence: '+str(path))
        pod, incarnation = final['pod'], final['incarnation']
        checksum = hashlib.sha256()
        started = False
        with gzip.open(path, 'rb') as stream:
            for line in stream:
                checksum.update(line)
                event = json.loads(line)
                if event['event'] == 'started':
                    if not event.get('outcomes_enabled'):
                        raise ValueError('Message outcomes were not logged')
                    started = True
                    if final['role'] == 'consumer':
                        metadata[(pod,incarnation)] = dict(source_sha256=event['source_sha256']['consumer.py'],
                                                        packages=event['packages'])
                elif event['event'] == 'acknowledged':
                    value = Ack(event['producer_timestamp'],event['timestamp'],event['topic'],event['partition'],event['offset'])
                    old = ack.get(event['message_id'])
                    if old is not None and (old.producer_timestamp,old.topic,old.partition,old.offset) != (value.producer_timestamp,value.topic,value.partition,value.offset):
                        raise ValueError('Conflicting acknowledgment identity')
                    if old is None or value.acknowledged < old.acknowledged:
                        ack[event['message_id']] = value
                elif event['event'] == 'completed':
                    value = Done(event['message_id'],event['completion_timestamp'],event['producer_timestamp'],
                        pod,incarnation,event['assignment_epoch'],event['topic'],event['partition'],event['offset'],
                        event['processing_start_timestamp'],event['processing_seconds'],event['output_sha256'])
                    if not all(math.isfinite(x) for x in [value.completion_timestamp,value.producer_timestamp,
                            value.processing_start_timestamp,value.processing_seconds]) or value.processing_seconds < 0:
                        raise ValueError('Invalid processing timing')
                    if value.completion_timestamp < value.processing_start_timestamp or value.completion_timestamp < value.producer_timestamp:
                        raise ValueError('Negative processing duration or completion latency')
                    old = done.get(value.message_id)
                    if old is not None:
                        duplicates += 1
                        if (old.producer_timestamp,old.topic,old.partition,old.offset,old.output_sha256) != (value.producer_timestamp,value.topic,value.partition,value.offset,value.output_sha256):
                            raise ValueError('Conflicting completion identity')
                    if old is None or value.completion_timestamp < old.completion_timestamp:
                        done[value.message_id] = value
                    physical[(pod,incarnation)].append(value)
        if not started or checksum.hexdigest() != receipt['original_sha256']:
            raise ValueError('Decompressed evidence differs from the original file')
    if not set(done).issubset(ack):
        raise ValueError('Completion without an acknowledgment')
    for key, value in done.items():
        a = ack[key]
        if (a.producer_timestamp,a.topic,a.partition,a.offset) != (value.producer_timestamp,value.topic,value.partition,value.offset):
            raise ValueError('Producer and consumer identity differs')
    for role in ('producer','consumer'):
        seen = {path.parts[-3] for path in paths if path.parts[-4] == role}
        if not set(manifest[role+'_pods']).issubset(seen):
            raise ValueError('Missing initial '+role+' process')
    ack = {key:value for key,value in ack.items() if manifest['start_epoch'] <= value.producer_timestamp < manifest['producer_end_epoch']}
    if any(not math.isfinite(a.acknowledged) or a.acknowledged > manifest['drain_end_epoch'] for a in ack.values()):
        raise ValueError('Invalid or post-cutoff acknowledgment')
    return ack, done, physical, metadata, duplicates


def check_saved_cohorts(groups, stats, path):
    saved = json.loads(path.read_text())['cohorts']
    for name, ids in groups.items():
        old, new = saved[name], stats[name]
        for a,b in [('messages','messages'),('completed_by_cutoff','completed_by_cutoff'),('unfinished','unfinished'),
                    ('completed_by_evaluation_end','completed_by_evaluation_end'),('deadline_misses','deadline_misses_known_by_cutoff')]:
            if old[a] != new[b]:
                raise ValueError('Existing cohort count differs: '+name+'/'+a)
        for key in ('completion_mean_seconds','completion_p99_seconds'):
            if old[key] is None and new[key] is None:
                continue
            if not math.isclose(old[key],new[key],rel_tol=1e-10,abs_tol=1e-6):
                raise ValueError('Existing cohort latency differs: '+name+'/'+key)


def analyze(root, row, windows):
    directory = root/'results'/row['run_id']
    used = {directory/name for name in ('manifest.json','runner-status.json','lag-summary.json','decision-cohort-summary.json')}
    for path in used:
        expected = row['evidence_hashes'].get(path.name)
        if expected and digest(path) != expected:
            raise ValueError('Versioned evidence hash differs: '+path.name)
    manifest = json.loads((directory/'manifest.json').read_text())
    status = json.loads((directory/'runner-status.json').read_text())
    if status['status'] != 'complete' or status['run_id'] != row['run_id']:
        raise ValueError('Run is not complete')
    evaluation, end, cutoff = (manifest[k] for k in ('evaluation_start_epoch','producer_end_epoch','drain_end_epoch'))
    decision, deadline = evaluation+60, float(manifest['config']['SLO_THRESHOLD_MS'])/1000
    snapshots = json.loads((directory/'lag-summary.json').read_text())['snapshots']
    activation, owners, transfer, basis = checked_activation(directory,manifest,row,windows,snapshots,used)
    ack, done, physical, metadata, duplicates = load_evidence(directory,manifest,used)
    if not set(owners.values()).issubset({pod for pod,_ in metadata}):
        raise ValueError('A confirmed owner has no process evidence')
    groups = decision_groups(ack,done,decision)
    stats = {name:cohort_stats(ids,ack,done,decision,activation,end,cutoff,deadline) for name,ids in groups.items()}
    check_saved_cohorts(groups,stats,directory/'decision-cohort-summary.json')
    # Only waiting cohorts have an age at their anchor; future arrivals do not.
    for name in groups:
        if name != 'pending_at_decision':
            stats[name]['median_age_at_anchor_seconds'] = None
            stats[name]['mean_age_at_anchor_seconds'] = None
    waiting_at_activation = pending_ids(ack,done,activation)
    activation_stats = cohort_stats(waiting_at_activation,ack,done,activation,activation,end,cutoff,deadline)
    evaluation_ids = {key for key,a in ack.items() if evaluation <= a.producer_timestamp < end}
    evaluation_done = [key for key in evaluation_ids if key in done and done[key].completion_timestamp <= cutoff]
    if len(evaluation_ids) != row['admitted'] or len(evaluation_done) != row['completed']:
        raise ValueError('Original evaluation cohort no longer reconciles')
    if not math.isclose(percentile([done[k].completion_timestamp-ack[k].producer_timestamp for k in evaluation_done],.99),row['p99_seconds'],abs_tol=1e-6):
        raise ValueError('Whole-run p99 differs')
    end_first = min(activation+30,end)
    coverage = ownership_coverage(snapshots,owners,activation,end_first)
    cohort_for = {key:name for name,ids in groups.items() for key in ids}
    partition_rows, consumer_rows = [], []
    distinct_first = [value for key,value in done.items() if key in ack and activation <= value.completion_timestamp < end_first]
    distinct_by_partition = defaultdict(list)
    for value in distinct_first:
        distinct_by_partition[value.partition].append(value)
    all_first = [value for rows in physical.values() for value in rows
                 if activation <= value.processing_start_timestamp < end_first]
    physical_by_partition = defaultdict(list)
    for value in all_first:
        physical_by_partition[value.partition].append(value)
    activation_by_partition = Counter(ack[key].partition for key in waiting_at_activation)
    decision_by_partition = Counter(ack[key].partition for key in groups['pending_at_decision'])
    hot = {int(p) for p in manifest['config']['SKEW_PARTITION'].split(',')}
    for partition,owner in sorted(owners.items()):
        completions = distinct_by_partition[partition]
        starts = physical_by_partition[partition]
        partition_rows.append(dict(partition=partition,owner_at_activation=owner,high_input=partition in hot,
            pending_at_decision=decision_by_partition[partition],pending_at_activation=activation_by_partition[partition],
            processing_starts=len(starts), observed_start_rate=len(starts)/(end_first-activation),
            distinct_completions=len(completions),throughput=len(completions)/(end_first-activation),
            decision_cohort_completions=sum(c.message_id in groups['pending_at_decision'] for c in completions),
            activation_cohort_completions=sum(c.message_id in waiting_at_activation for c in completions),
            produced_after_activation_completions=sum(c.producer_timestamp >= activation for c in completions),
            observed_processing_pods=sorted({c.pod for c in starts})))
    for (pod,incarnation), rows in physical.items():
        starts = [value for value in rows if activation <= value.processing_start_timestamp < end_first]
        if not starts and pod not in set(owners.values()):
            continue
        summary = describe([value._asdict() for value in starts],end_first-activation)
        completions = [value for value in distinct_first if value.pod==pod and value.incarnation==incarnation]
        counts = Counter(cohort_for[value.message_id] for value in completions)
        summary.update(pod=pod,incarnation=incarnation,source=metadata[(pod,incarnation)],
            assigned_partitions=[p for p,c in sorted(owners.items()) if c==pod],
            distinct_completions=len(completions),throughput=len(completions)/(end_first-activation),
            decision_groups=dict(counts),activation_cohort_completions=sum(v.message_id in waiting_at_activation for v in completions),
            produced_after_activation_completions=sum(v.producer_timestamp >= activation for v in completions))
        consumer_rows.append(summary)
    if any(c['nonincreasing_offsets_within_epoch'] or c['overlapping_or_reversed_processing_times']
           for c in consumer_rows):
        raise ValueError('Processing order failed its within-process check')
    interval_rows = []
    for name,left,right in [('Before',evaluation+30,decision),('Decision–activation',decision,activation),
                           ('First 30 s active',activation,end_first),('Active–end',activation,end),
                           ('Eval. end',end-30,end),('Drain',end,cutoff)]:
        if right <= left:
            continue
        selected = [c for key,c in done.items() if key in ack and left <= c.completion_timestamp < right]
        counts = Counter(cohort_for[c.message_id] for c in selected)
        pending_now = pending_ids(ack,done,right)
        interval_rows.append(dict(name=name,start_after_evaluation=left-evaluation,end_after_evaluation=right-evaluation,
            seconds=right-left,distinct_completions=len(selected),throughput=len(selected)/(right-left),
            completion_p99_seconds=percentile([c.completion_timestamp-c.producer_timestamp for c in selected],.99),
            decision_groups=dict(counts),activation_cohort_completions=sum(c.message_id in waiting_at_activation for c in selected),
            produced_after_activation_completions=sum(c.producer_timestamp >= activation for c in selected),
            pending_all_at_end=len(pending_now),pending_decision_cohort_at_end=len(pending_now & groups['pending_at_decision']),
            pending_activation_cohort_at_end=len(pending_now & waiting_at_activation)))
    # This is a temporal coexistence diagnostic, not proof of a local scheduling choice.
    old = groups['pending_at_decision']
    last_old = max((done[k].completion_timestamp for k in old if k in done),default=decision)
    if any(k not in done or done[k].completion_timestamp > cutoff for k in old):
        last_old = cutoff
    coexist = sum(c.producer_timestamp >= decision and activation <= c.completion_timestamp < min(last_old,cutoff)
                  for key,c in done.items() if key in ack)
    known_current_sha = digest(root/'src/consumer/consumer.py')
    result = {key:row[key] for key in ('run_id','backlog_condition','arm','run_number')}
    result.update(decision_after_evaluation=60,activation_after_evaluation=activation-evaluation,
        activation_after_decision=activation-decision,activation_basis=basis,
        release_requested_after_decision=transfer-decision if transfer is not None else None,
        decision_cohort_completed_before_release=(last_old <= transfer) if transfer is not None else None,
        owner_map={str(k):v for k,v in sorted(owners.items())},cohorts=stats,pending_at_activation=activation_stats,
        first_30_seconds=dict(start_after_evaluation=activation-evaluation,end_after_evaluation=end_first-evaluation,
                              ownership_observations=coverage,consumers=consumer_rows,partitions=partition_rows),
        intervals=interval_rows,post_decision_completions_while_decision_cohort_remains_after_activation=coexist,
        original_recovery_after_decision=row['recovery_common_decision'],original_p99_seconds=row['p99_seconds'],
        original_unfinished_percent=row['unfinished_percent'],physical_repeated_completions=duplicates,
        checks=dict(original_cohorts_reconciled=True,original_evaluation_cohort_reconciled=True,
                    original_p99_reconciled=True,compressed_and_original_event_hashes_verified=True,
                    all_consumer_sources_match=all(m['source_sha256']==known_current_sha for m in metadata.values())))
    hashes=[]
    for path in sorted(used):
        value=digest(path)
        expected=row['evidence_hashes'].get(path.name)
        if expected and value != expected:
            raise ValueError('Versioned evidence hash differs: '+str(path))
        hashes.append(dict(path=str(path.relative_to(root)),sha256=value))
    return result,hashes


def write_outputs(output, rows, hashes, planned):
    payload = dict(scope='Post-mitigation analysis of existing real Nautilus trials; no new experiment or scheduler change.',
        status='complete' if len(rows)==planned else 'partial',analyzed_trials=len(rows),selected_trials=planned,
        definitions=dict(decision='Common scheduled decision at evaluation second 60.',
            activation='Recorded assignment confirmation, which can follow actual processing resumption. Keep 3 uses second 60 as its reference.',
            decision_cohort='Created before and acknowledged by the decision, but not completed then, including warm-up messages.',
            activation_cohort='Acknowledged by activation and not completed then, including messages produced during preparation and handover.',
            cohort_latency='Creation to earliest completion by drain cutoff. Unfinished messages remain in the cohort denominator.',
            service='First 30 seconds after confirmed activation; physical processing starts retain replays, throughput counts earliest distinct completions.',
            limitation='Consumer buffers and consume-call boundaries were not logged. Coexistence of old and new work does not prove both were locally available. Timestamp comparisons retain clock uncertainty.',
            comparison='Activation-aligned windows begin at different absolute times and queue ages. Report common-decision times alongside them. Observed service is not a calibrated capacity.'),runs=rows)
    (output/'results.json').write_text(json.dumps(payload,indent=2,allow_nan=False)+'\n')
    (output/'source-manifest.json').write_text(json.dumps(hashes,indent=2)+'\n')
    summaries = []
    partitions, consumers = [], []
    for row in rows:
        base = {k:row[k] for k in ('run_id','backlog_condition','arm','run_number')}
        d,a = row['cohorts']['pending_at_decision'],row['pending_at_activation']
        summaries.append(dict(base,decision_pending=d['messages'],decision_cohort_clearance_s=d['all_completed_seconds'],
            activation_after_decision_s=row['activation_after_decision'],activation_pending=a['messages'],
            activation_cohort_clearance_s=a['all_completed_seconds'],
            recovery_after_decision_s=row['original_recovery_after_decision']['confirmation_after_anchor_seconds'],
            first_active_throughput=next(x['throughput'] for x in row['intervals'] if x['name']=='First 30 s active'),
            decision_cohort_completed_before_release=row['decision_cohort_completed_before_release']))
        for p in row['first_30_seconds']['partitions']:
            partitions.append(dict(base,**{k:v for k,v in p.items() if k!='observed_processing_pods'}))
        for c in row['first_30_seconds']['consumers']:
            consumers.append(dict(base,**{k:c[k] for k in ['pod','incarnation','selected_processing_starts','observed_start_rate',
                'distinct_completions','throughput','streak_mean','streak_max','same_partition_adjacent_fraction',
                'creation_time_backtracks_over_100ms','nonincreasing_offsets_within_epoch']},
                assigned_partitions=' '.join(map(str,c['assigned_partitions']))))
    for name,data in [('summary.csv',summaries),('partition-service.csv',partitions),('consumer-service.csv',consumers)]:
        with (output/name).open('w',newline='') as stream:
            writer=csv.DictWriter(stream,fieldnames=list(data[0]),lineterminator='\n')
            writer.writeheader();writer.writerows(data)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repo',type=Path,required=True)
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--run-id',help='Analyze one saved trial for verification')
    args=parser.parse_args()
    directory=args.repo/'experiment-records/backlog-comparison-20261027'
    progress_path,windows_path=directory/'progress.json',directory/'reporting-intervals.json'
    runs=json.loads(progress_path.read_text())['runs'];windows=json.loads(windows_path.read_text())
    if args.run_id:
        runs=[r for r in runs if r['run_id']==args.run_id]
        if not runs: raise ValueError('Unknown run ID')
    args.output.mkdir(parents=True,exist_ok=True)
    results=[];hashes=[dict(path=str(p.relative_to(args.repo)),sha256=digest(p)) for p in [progress_path,windows_path]]
    for index,row in enumerate(runs,1):
        result,evidence=analyze(args.repo,row,windows)
        results.append(result);hashes.extend(evidence)
        write_outputs(args.output,results,hashes,len(runs))
        print(str(index)+'/'+str(len(runs))+' '+row['run_id']+' '+row['arm']+' reconciled',flush=True)


if __name__=='__main__':
    main()
