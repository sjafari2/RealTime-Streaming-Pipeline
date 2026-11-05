"""Audit busy processing intervals without treating trial data as prior calibration."""
import argparse
import bisect
from collections import Counter, defaultdict
import gzip
import hashlib
import json
from pathlib import Path
import sys


def read(path):
    return json.loads(Path(path).read_text())


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def save(path, value):
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    Path(path).write_text(json.dumps(value, indent=2, allow_nan=False) + '\n')


def aggregate(records):
    eligible = [(record, w) for record in records for w in record['windows'] if w['eligible']]
    windows = [w for _, w in eligible]
    seconds = sum(w['observed_seconds'] for w in windows)
    return dict(
        eligible_windows=len(windows),
        contributing_runs=sorted({record['run_id'] for record, _ in eligible}),
        observed_seconds=seconds,
        completed_attempts=sum(w['completions'] for w in windows),
        effective_rate=sum(w['completions'] for w in windows) / seconds if seconds else None,
        minimum_window_rate=min((w['effective_rate'] for w in windows), default=None),
        maximum_window_rate=max((w['effective_rate'] for w in windows), default=None),
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repository', type=Path, required=True)
    parser.add_argument('--campaign-root', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    sys.path.insert(0, str(args.repository / 'experiments/signal-outcome-20261021'))
    import build_capacity as capacity

    comparison = read(args.campaign_root / 'paired-analysis/comparison.json')
    balanced_path = args.repository / 'experiment-records/matched-skew600-20261104/balanced-calibration/summary.json'
    balanced = read(balanced_path)
    sources = [dict(run_id=balanced['run_id'], kind='balanced_reference', arm='balanced',
                    condition='balanced', evidence_hashes=balanced['evidence_sha256'])]
    sources += [dict(run_id=r['run_id'], kind='mitigation_trial', arm=r['arm'],
                     condition=r['backlog_condition'], evidence_hashes=r['evidence_hashes'])
                for r in comparison['runs']]
    assert len(sources) == 17 and len({s['run_id'] for s in sources}) == 17
    records = []
    source_hashes = {}
    for source in sources:
        directory = args.campaign_root / source['run_id']
        names = ['manifest.json', 'prometheus.json', 'lag-summary.json',
                 'outcome-summary.json', 'matched-placement-checks.jsonl', 'resource-history.jsonl']
        hashes = {name: digest(directory / name) for name in names}
        for name, expected in source['evidence_hashes'].items():
            if name in hashes:
                assert hashes[name] == expected, (source['run_id'], name, 'changed evidence')
        source_hashes[source['run_id']] = hashes
        manifest, lag = read(directory / 'manifest.json'), read(directory / 'lag-summary.json')
        assert not read(directory / 'outcome-summary.json')['validity_failures']
        placement = [json.loads(line) for line in (directory / 'matched-placement-checks.jsonl').read_text().splitlines()]
        assert placement and not any(p['issues'] for p in placement)
        identities = defaultdict(set)
        for check in placement:
            for client in check['clients']:
                if client['role'] == 'consumer':
                    identities[client['pod']].add(json.dumps({k:client[k] for k in
                        ('node','uid','container_id','image_id','restart_count','resources')}, sort_keys=True))
        assert all(len(ids) == 1 for ids in identities.values()), 'Consumer identity changed'
        conditions = capacity.observed_conditions(manifest, directory / 'resource-history.jsonl', {}, 'resources')
        data = capacity.CounterData(read(directory / 'prometheus.json'), manifest)
        end = int(manifest['producer_end_epoch'] - manifest['evaluation_start_epoch'])
        run_records = []
        for pod in sorted(conditions):
            if not pod.startswith('consumer-'):
                continue
            windows, rejected = [], []
            for left in range(60, end - 59, 60):
                try:
                    window = capacity.service_window(data, lag, manifest, pod, left, left + 60)
                    reasons = []
                    if not window['backlogged']:
                        reasons.append('insufficient sustained backlog')
                    if not window['stable_rate']:
                        reasons.append('unstable completion rate')
                    window['ineligibility_reasons'] = reasons
                    windows.append(window)
                except ValueError as exc:
                    rejected.append(dict(left_seconds=left, right_seconds=left+60, reason=str(exc)))
            record = dict(run_id=source['run_id'], source_kind=source['kind'], arm=source['arm'],
                          workload=source['condition'], pod=pod, condition=conditions[pod],
                          runtime_identity=json.loads(next(iter(identities[pod]))),
                          windows=windows, rejected_intervals=rejected)
            record['summary'] = aggregate([record])
            records.append(record)
            run_records.append(record)
            print(source['run_id'], pod, 'eligible', record['summary']['eligible_windows'],
                  'rate', record['summary']['effective_rate'],
                  'rejected', dict(Counter(w['reason'] for w in rejected)), flush=True)
        save(args.output / 'per-run' / (source['run_id'] + '.json'), run_records)

    summaries = []
    for pod in sorted({r['pod'] for r in records}):
        selected = [r for r in records if r['pod'] == pod]
        assert len({json.dumps(r['condition'],sort_keys=True) for r in selected}) == 1, 'Unmatched deployment conditions'
        assert len({json.dumps({k:r['runtime_identity'][k] for k in ('node','image_id','resources')},sort_keys=True)
                    for r in selected}) == 1, 'Unmatched image or resource limits'
        reasons = Counter()
        for r in selected:
            reasons.update(w['reason'] for w in r['rejected_intervals'])
            for w in r['windows']:
                reasons.update(w['ineligibility_reasons'])
        summaries.append(dict(pod=pod, condition=selected[0]['condition'],
            retrospective=aggregate([r for r in selected if r['source_kind']=='mitigation_trial']),
            separate_balanced_reference=aggregate([r for r in selected if r['source_kind']=='balanced_reference']),
            by_response={arm:aggregate([r for r in selected if r['arm']==arm]) for arm in sorted({r['arm'] for r in selected})},
            rejected_or_ineligible_reasons=dict(reasons)))
    # Reconcile the counter estimates with independent per-message completion records.
    crosschecks=[]
    for record in records:
        windows=[w for w in record['windows'] if w['eligible']]
        if not windows:
            continue
        paths=list((args.campaign_root/record['run_id']/'consumer'/record['pod']).rglob('events.jsonl.gz'))
        assert len(paths)==1
        times=[]
        with gzip.open(paths[0],'rt') as stream:
            for line in stream:
                event=json.loads(line)
                if event.get('event')=='completed':
                    times.append(event['completion_timestamp'])
        times.sort()
        event_hash=digest(paths[0])
        for w in windows:
            count=bisect.bisect_right(times,w['scrape_end_epoch'])-bisect.bisect_right(times,w['scrape_start_epoch'])
            crosschecks.append(dict(run_id=record['run_id'],pod=record['pod'],left_seconds=w['left_seconds'],
                counter_completions=w['completions'],recorded_completions=count,
                difference=count-w['completions'],relative_difference=abs(count-w['completions'])/w['completions'],
                event_file_sha256=event_hash))
    save(args.output/'completion-crosscheck.json',crosschecks)
    result = dict(schema_version=1, consumers=summaries, records=records, source_sha256=source_hashes,
        analysis_sha256=digest(__file__), eligibility_implementation_sha256=digest(capacity.__file__),
        comparison_sha256=digest(args.campaign_root/'paired-analysis/comparison.json'),
        completion_crosscheck=dict(intervals=len(crosschecks),
            maximum_relative_count_difference=max((c['relative_difference'] for c in crosschecks),default=None),
            note='Timestamp-bounded completion records independently check the monitoring counters. Scrape timing and counter updates are not atomic with the event log, so small boundary differences are retained.'),
        method={
            'intervals':'Nonoverlapping 60-second intervals starting at evaluation second 60 and ending by production stop. Warm-up and drain are excluded.',
            'eligibility':'Existing service_window rule, unchanged: complete valid lag/ownership observations including the preceding 10 seconds, fixed ownership and process incarnation, no counter resets or ownership callbacks, counters at most 10 seconds old, six 10-second completion rates with population CV <= 0.10, and owner processing backlog always >= max(100 offsets, two seconds of observed throughput).',
            'estimate':'Completed processing attempts divided by actual scrape elapsed time, pooled over eligible intervals for each consumer. Repeated attempts count because they consume service capacity.',
            'interpretation':'Retrospective effective service estimates from the comparison trials are diagnostic. They are not independently calibrated, available before action, or evidence of prospective prediction accuracy. Sampled backlog does not prove continuous busyness between observations.',
            'range':'Observed minimum and maximum eligible 60-second rates, not confidence limits. Multiple intervals within a run are not independent experiments.',
            'missing':'No qualifying interval means capacity remains unestimated. Underloaded throughput is not substituted.',
            'transfer':'Consumers are matched by node, image, resource settings and recorded processing configuration. Assignment shape, execution path and shared-node contention can still affect effective service.'})
    save(args.output / 'capacity-audit.json', result)
    lines = ['# Effective consumer capacity from saved matched trials', '',
             'This audit adds no live experiments. It separately examines the balanced reference and the 16 validated comparison trials with the unchanged capacity-screening criteria.', '',
             '| Consumer | Retrospective rate (messages/s) | Eligible-window range | Eligible 60-second intervals | Contributing trials | Separate calibration |',
             '|---|---:|---:|---:|---:|---|']
    for s in summaries:
        d=s['retrospective']; mu=d['effective_rate']
        rate='Not estimated' if mu is None else f'{mu:.2f}'
        span='—' if mu is None else f"{d['minimum_window_rate']:.2f}–{d['maximum_window_rate']:.2f}"
        cal=s['separate_balanced_reference']['effective_rate']
        lines.append(f"| {s['pod']} | {rate} | {span} | {d['eligible_windows']} | {len(d['contributing_runs'])} | {'No eligible interval' if cal is None else f'{cal:.2f} messages/s'} |")
    crosscheck_note=(f"Completion-log cross-check: {len(crosschecks)} eligible intervals, with a maximum count difference of {100*result['completion_crosscheck']['maximum_relative_count_difference']:.3f}% between timestamp-bounded completion events and counter increments."
                    if crosschecks else 'No eligible interval was available for a completion-log cross-check.')
    lines += ['', result['method']['estimate'], '', result['method']['eligibility'], '',
              result['method']['interpretation'], '', result['method']['range'], '', result['method']['missing'], '',
              crosscheck_note, '',
              'Individual intervals, rejected intervals, deployment conditions and source hashes are retained in capacity-audit.json.']
    (args.output / 'CAPACITY_REPORT.md').write_text('\n'.join(lines)+'\n')
    print('COMPLETE', args.output, flush=True)


if __name__ == '__main__':
    main()
