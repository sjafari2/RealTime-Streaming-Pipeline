"""Read saved evidence only; compare failed offsets with later commit acknowledgments."""
import argparse
import collections
import hashlib
import json
from pathlib import Path


def audit(run):
    manifest = json.loads((run / 'manifest.json').read_text())
    start = manifest['evaluation_start_epoch']
    commits, transfers, inputs = [], [], {}
    completed, acknowledged = {}, {}
    counts = collections.Counter()
    final_events = []
    for path in sorted(run.glob('*/*/*/events.jsonl')):
        inputs[str(path.relative_to(run))] = hashlib.sha256(path.read_bytes()).hexdigest()
        pod = path.parts[-3]
        with path.open() as stream:
            for line in stream:
                event = json.loads(line)
                kind = event['event']
                if kind in ('completed', 'acknowledged'):
                    target = completed if kind == 'completed' else acknowledged
                    key = (event['topic'], event['partition'])
                    # A committed offset denotes the next record to consume.
                    target[key] = max(target.get(key, 0), event['offset'] + 1)
                    counts[kind] += 1
                elif kind == 'commit_result':
                    commits.append(dict(event, pod=pod))
                elif kind in ('assign_started', 'assign', 'revoke_started', 'revoke', 'lost'):
                    transfers.append(dict(event, pod=pod))
                elif kind == 'finished' and path.parts[-4] == 'consumer':
                    final_events.append(dict(event, pod=pod))
    commits.sort(key=lambda e: e['timestamp'])
    successful = [e for e in commits if e['success']]
    failed = [e for e in commits if not e['success']]
    by_partition = collections.defaultdict(list)
    for event in successful:
        for offset in event['offsets']:
            if not offset.get('error') and offset['offset'] >= 0:
                by_partition[(offset['topic'], offset['partition'])].append(
                    dict(timestamp=event['timestamp'], pod=event['pod'], offset=offset['offset']))
    failures = []
    for event in failed:
        recovered = []
        for offset in event['offsets']:
            candidates = by_partition[(offset['topic'], offset['partition'])]
            later = next((c for c in candidates if c['timestamp'] > event['timestamp']
                          and c['offset'] >= offset['offset']), None)
            recovered.append(dict(partition=offset['partition'], failed_offset=offset['offset'],
                                  later_success=later,
                                  delay_seconds=later['timestamp'] - event['timestamp'] if later else None))
        failures.append(dict(pod=event['pod'], evaluation_second=event['timestamp'] - start,
                             context=event['context'], errors=sorted(set(event.get('errors', []))),
                             group_transition=event.get('group_transition'), offsets=recovered))
    partition_ends = []
    for key in sorted(set(acknowledged) | set(completed)):
        successes = by_partition[key]
        last = successes[-1] if successes else None
        partition_ends.append(dict(topic=key[0], partition=key[1],
            acknowledged_next_offset=acknowledged.get(key), completed_next_offset=completed.get(key),
            last_success=last,
            final_commit_covers_acknowledged=bool(last and last['offset'] >= acknowledged.get(key, 0)),
            final_commit_covers_completed=bool(last and last['offset'] >= completed.get(key, 0))))

    lag_path = run / 'lag-summary.json'
    lag = json.loads(lag_path.read_text())
    inputs[lag_path.name] = hashlib.sha256(lag_path.read_bytes()).hexdigest()
    inputs['manifest.json'] = hashlib.sha256((run / 'manifest.json').read_bytes()).hexdigest()
    snapshots = lag['snapshots']
    intervals = []
    covered = 0
    # Reproduce the existing coverage rule; do not bridge invalid/changed-owner samples.
    for previous, current in zip(snapshots, snapshots[1:]):
        reasons = []
        dt = current['timestamp'] - previous['timestamp']
        if not previous['valid'] or not current['valid']:
            reasons.append('invalid endpoint')
        else:
            if current['owners'] != previous['owners']:
                reasons.append('ownership changed')
            if any(current['positions'][p] < previous['positions'][p] or
                   current['highs'][p] < previous['highs'][p] for p in current['lags']):
                reasons.append('offset decreased')
        if not 0 < dt <= lag['parameters']['max_gap']:
            reasons.append('sampling gap')
        if reasons:
            intervals.append(dict(start=previous['timestamp'] - start,
                                  end=current['timestamp'] - start, reasons=reasons))
        else:
            covered += dt
    for a, b in [(start, snapshots[0]['timestamp']),
                 (snapshots[-1]['timestamp'], manifest['producer_end_epoch'])]:
        if b > a:
            intervals.append(dict(start=a-start, end=b-start, reasons=['unsampled boundary']))
    groups = []
    for interval in sorted(intervals, key=lambda x: x['start']):
        if groups and abs(groups[-1]['end']-interval['start']) < 1e-6:
            groups[-1]['end'] = interval['end']
            groups[-1]['reasons'] = sorted(set(groups[-1]['reasons'] + interval['reasons']))
        else:
            groups.append(dict(interval))
    assert abs(covered-lag['covered_seconds']) < 1e-6
    return dict(run_id=manifest['run_id'], evaluation_start_epoch=start,
        input_sha256=inputs, event_counts=dict(counts), successful_commit_events=len(successful),
        failed_commit_events=len(failed), failures=failures,
        final_partition_offsets=partition_ends,
        consumer_finish_events=final_events,
        transfers=[dict(pod=e['pod'], event=e['event'], evaluation_second=e['timestamp']-start,
                        partitions=[p[1] for p in e.get('partitions', [])]) for e in
                   sorted(transfers, key=lambda e: e['timestamp'])
                   if start <= e['timestamp'] <= manifest['producer_end_epoch']],
        lag_covered_seconds=covered, lag_covered_fraction=lag['covered_fraction'],
        uncovered_intervals=groups,
        invalid_snapshots=[dict(evaluation_second=s['timestamp']-start,
                               reasons=s['invalid_reasons']) for s in snapshots if not s['valid']],
        limits=['Saved client commit acknowledgments, not a new broker query.',
                'Final offset coverage is not by itself proof of exactly-once processing.',
                'Cross-pod event alignment has unmeasured clock uncertainty.',
                'Empty assign callbacks do not themselves show a partition transfer.'])


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('run', type=Path)
    parser.add_argument('output', type=Path)
    args = parser.parse_args()
    result = audit(args.run)
    args.output.write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps({k: result[k] for k in ('event_counts', 'successful_commit_events',
        'failed_commit_events', 'lag_covered_seconds', 'uncovered_intervals')}, indent=2))
    for event in result['failures']:
        delays = [o['delay_seconds'] for o in event['offsets'] if o['later_success']]
        print(event['pod'], round(event['evaluation_second'], 3),
              'recovered', len(delays), '/', len(event['offsets']),
              'max delay', max(delays, default=None))
    print('Final commits cover acknowledged:', sum(p['final_commit_covers_acknowledged']
          for p in result['final_partition_offsets']), '/', len(result['final_partition_offsets']))
