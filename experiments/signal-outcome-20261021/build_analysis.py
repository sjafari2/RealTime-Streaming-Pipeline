"""Connect pre-action observations to outcomes without pooling campaigns.

The unit of analysis is a performance trial, not an individual scrape. Original
message outcomes are reused; raw monitoring and manifests are hash-verified.
"""
import argparse
import csv
import hashlib
import json
import math
from pathlib import Path
import statistics
import sys

GROUPS = (
    ('12hot-700', 'Twelve high-input partitions, 700 msg/s',
     'experiment-records/four-condition-20260929/comparison.json', 8),
    ('4hot-700', 'Four high-input partitions, 700 msg/s',
     'experiment-records/four-hot-partitions-20260930/comparison.json', 4),
    ('4hot-600', 'Four high-input partitions, 600 msg/s',
     'results/four-hot600-20261021-analysis/comparison.json', 8),
)
LABELS = {'keep3': 'Keep 3', 'redistribute3': 'Redistribute 3',
          'scale_redistribute6': 'Targeted scale 6', 'kafka_scale6': 'Kafka scale 6'}


def digest(data):
    return hashlib.sha256(data).hexdigest()


def read(path, hashes, label, expected=None):
    data = path.read_bytes()
    actual = digest(data)
    if expected and actual != expected:
        raise ValueError('Evidence hash mismatch: ' + label)
    hashes[label] = actual
    return json.loads(data)


def contiguous(a, b):
    if not (a.get('valid') and b.get('valid') and
            0 < b['timestamp'] - a['timestamp'] <= 3 and a['owners'] == b['owners']):
        return False
    return all(b[field][p] >= a[field][p]
               for field in ('positions', 'highs') for p in a[field])


def select_grid(snapshots, start, seconds):
    """Match the nominal grid, allowing only export rounding (one millisecond)."""
    selected = []
    for second in seconds:
        matches = [r for r in snapshots if abs(r['timestamp'] - start - second) <= .001]
        if len(matches) != 1 or not matches[0].get('valid'):
            return None
        selected.append(matches[0])
    if not all(contiguous(a, b) for a, b in zip(selected, selected[1:])):
        return None
    return selected


def mean_field(rows, field):
    values = [row.get(field) for row in rows or []]
    return (statistics.mean(values) if values and
            all(v is not None and math.isfinite(v) for v in values) else None)


def signal_features(snapshots, start, stop=60):
    grid = select_grid(snapshots, start, range(stop - 30, stop, 2))
    result = {'valid': grid is not None, 'sample_count': len(grid) if grid else 0,
              'mean_backlog_offsets': mean_field(grid, 'total_lag'),
              'mean_processing_backlog_offsets': mean_field(grid, 'processing_backlog'),
              'mean_skew': mean_field(grid, 'skew'),
              'mean_max_partition_lag': mean_field(grid, 'max_partition_lag')}
    # Growth has 15 intervals (16 observations), unlike a 15-observation mean.
    growth = select_grid(snapshots, start, range(stop - 32, stop, 2))
    for field, name in [('total_lag', 'backlog_growth_30s_offsets_per_second'),
                        ('processing_backlog', 'processing_growth_30s_offsets_per_second')]:
        result[name] = ((growth[-1][field] - growth[0][field]) /
                        (growth[-1]['timestamp'] - growth[0]['timestamp'])) if growth else None
    result['persistent_hot_count'] = (len(grid[-1]['persistent_hot'])
        if grid and grid[-1].get('persistent_hot') is not None else None)
    result['mean_persistent_hot_count'] = (statistics.mean(len(r['persistent_hot']) for r in grid)
        if grid and all(r.get('persistent_hot') is not None for r in grid) else None)
    shares = []
    for row in grid or []:
        owned = {}
        for partition, owner in row['owners'].items():
            owned[owner] = owned.get(owner, 0) + row['lags'][partition]
        if row['total_lag'] > 0:
            shares.append(max(owned.values()) / row['total_lag'])
    result['mean_largest_owner_backlog_share'] = statistics.mean(shares) if len(shares) == 15 else None
    result['total_backlog_last_sample'] = grid[-1]['total_lag'] if grid else None
    result['observed_sample_seconds'] = [r['timestamp'] - start for r in grid or []]
    return result


def resource_features(series, pods, start):
    """Fresh, complete process observations on the same pre-action sample grid."""
    result = {}
    for suffix, divisor in [('cpu_percent', 100), ('memory_bytes', 2**20)]:
        field = 'consumer_' + suffix
        values = {}
        for pod in pods:
            measurements = [s for s in series if s['metric'].get('__name__') == field
                            and s['metric'].get('pod') == pod]
            stamps = [s for s in series if s['metric'].get('__name__') == field + '_scrape_timestamp_seconds'
                      and s['metric'].get('pod') == pod]
            samples = []
            identities = set()
            for second in range(30, 60, 2):
                found = []
                for item in measurements:
                    label = {k:v for k,v in item['metric'].items() if k != '__name__'}
                    matching_stamps = [s for s in stamps if
                        {k:v for k,v in s['metric'].items() if k != '__name__'} == label]
                    if len(matching_stamps) != 1:
                        continue
                    stamp_map = {float(t): float(v) for t,v in matching_stamps[0]['values']}
                    for t, value in item['values']:
                        t, value = float(t), float(value)
                        if abs(t-start-second) <= .001 and math.isfinite(value) and value >= 0:
                            stamp = stamp_map.get(t)
                            if stamp is not None and math.isfinite(stamp) and 0 <= t-stamp <= 10:
                                found.append((value/divisor, tuple(sorted(label.items()))))
                samples.append(found[0][0] if len(found) == 1 else None)
                if len(found) == 1:
                    identities.add(found[0][1])
            if len(identities) != 1:
                samples = [None] * 15
            values[pod] = samples
        complete = [i for i in range(15) if all(values[p][i] is not None for p in pods)]
        result[suffix] = {'expected_samples': 15, 'complete_samples': len(complete),
            'total_mean': statistics.mean(sum(values[p][i] for p in pods) for i in complete)
                          if len(complete) == 15 else None,
            'per_pod_mean': {p:statistics.mean(v) if all(x is not None for x in v) else None
                            for p,v in values.items()},
            'per_pod_valid_samples': {p:sum(x is not None for x in v) for p,v in values.items()}}
    return result


def outcome_cost(row):
    request = row['requested_resources_evaluation_and_drain']
    full = math.isclose(request['covered_fraction'], 1, abs_tol=1e-6)
    return {'requested_cpu_core_minutes': request['consumer_container_requested_cpu_seconds_observed']/60 if full else None,
            'requested_memory_gib_minutes': request['consumer_container_requested_gib_seconds_observed']/60 if full else None,
            'request_coverage': request['covered_fraction']}


def load_analysis(repository, inputs, raw600):
    sys.path.insert(0, str(repository/'python-scripts'))
    from analyze_execution import recovery
    hashes = {}
    audit_path = 'experiment-records/result-metric-audit-20260930/metrics.json'
    audit = read(inputs/audit_path, hashes, audit_path)
    audits = {r['run_id']:r for rows in audit['groups'].values() for r in rows}
    window_paths = ['experiment-records/four-condition-20260929/action-window-performance.json',
                    'experiment-records/four-hot-rate-comparison-20261021/historical-completion-windows.json',
                    'experiment-records/four-hot600-20261021/completion-windows.json']
    windows = {}
    window_hashes = {}
    for rel in window_paths:
        data = read(inputs/rel, hashes, rel)
        windows.update({r['run_id']:r for r in data['runs']})
        window_hashes.update({r['run_id']:data['comparison_sha256'] for r in data['runs']})
    rows = []
    for group, title, rel, count in GROUPS:
        comparison = read(inputs/rel, hashes, rel, audit['source_sha256'].get(rel))
        if len(comparison['runs']) != count:
            raise ValueError('Unexpected trial count: '+group)
        rate = comparison['cost_protocol']['aggregate_input_rate']
        hot = comparison['hot_partitions']
        for trial in comparison['runs']:
            run_id = trial['run_id']
            if window_hashes.get(run_id) != hashes[rel]:
                raise ValueError('Completion windows refer to another comparison: '+run_id)
            raw = (raw600 if group == '4hot-600' else inputs)/trial['raw_evidence']
            evidence = {name:read(raw/name, hashes, trial['raw_evidence']+'/'+name,
                                 trial['evidence_hashes'][name])
                        for name in ('manifest.json', 'lag-summary.json', 'prometheus.json')}
            manifest, lag = evidence['manifest.json'], evidence['lag-summary.json']
            start, end = manifest['evaluation_start_epoch'], manifest['producer_end_epoch']
            if not math.isclose(end-start, 600) or manifest['warmup_seconds'] != 60 or manifest['drain_seconds'] != 120:
                raise ValueError('Incompatible phase durations: '+run_id)
            if any(lag['parameters'].get(key) != value for key,value in
                   {'window_samples':15,'hot_k':1.0,'minimum_lag':10.0,'persistence':.8,'max_gap':3.0}.items()):
                raise ValueError('Incompatible persistence window: '+run_id)
            before = signal_features(lag['snapshots'], start)
            before['resources'] = resource_features(evidence['prometheus.json']['data']['result'],
                                                    manifest['consumer_pods'], start)
            before['performance'] = windows[run_id]['windows']['before']
            hot_counts = {p:0 for p in manifest['consumer_pods']}
            for partition in hot:
                hot_counts[trial['initial_ownership'][str(partition)]] += 1
            before['high_input_partitions_per_owner'] = hot_counts
            before['largest_owner_high_input_share'] = max(hot_counts.values())/len(hot)
            before['calibrated_spare_capacity_messages_per_second'] = None
            # The manifest's "capacity_estimate" is a time budget calculated from
            # target input, not an independently measured service capacity.
            before['capacity_note'] = 'No per-owner measured spare-capacity estimate joined to this trial; CPU usage and target-derived processing budget are not capacity measurements.'
            cost = trial['intervention_cost']
            anchor = cost.get('decision_epoch', start+60)
            rec = recovery(lag['snapshots'], anchor, end, 100, 30, 3)
            if trial['arm'] != 'keep3':
                original = next(r for r in cost['recovery_from_decision'] if
                                r['threshold_offsets'] == 100 and r['hold_seconds'] == 30)
                if rec.get('confirmed_epoch') != original['confirmed_epoch']:
                    raise ValueError('Recovery replay differs: '+run_id)
            old = audits.get(run_id)
            deadline = (old['deadline_percent']['1000'] if old else
                        100*next(r for r in trial['completion_deadline_outcomes']
                                 if r['threshold_ms'] == 1000)['deadline_miss_fraction'])
            outcomes = dict(p99_seconds=trial['p99_seconds'], mean_completion_seconds=trial['mean_completion_seconds'],
                unfinished_percent=trial['unfinished_percent'], unfinished_messages=trial['unfinished'],
                admitted_messages=trial['admitted'], completed_messages=trial['completed'],
                useful_throughput_messages_per_second=trial['useful_throughput'],
                achieved_cohort_input_messages_per_second=trial['admitted_rate'],
                deadline_miss_percent_1s=deadline,
                deadline_definition='Full acknowledged evaluation cohort; late completions plus overdue unfinished messages. One-second threshold retrospectively applied to 12hot-700.',
                recovery_confirmed=rec.get('confirmed_epoch') is not None,
                recovery_minutes=(rec['seconds_to_confirmation']/60
                                  if rec.get('confirmed_epoch') is not None else None),
                recovery_observation_minutes=(end-anchor)/60,
                recovery_status='confirmed' if rec.get('confirmed_epoch') is not None else 'unconfirmed_by_evaluation_end',
                recovery_anchor='actual decision' if trial['arm'] != 'keep3' else 'evaluation second 60',
                decision_to_activation_seconds=(cost['transition_end_epoch']-anchor if trial['arm'] != 'keep3' else None),
                end_signals=signal_features(lag['snapshots'],start,600),
                completion_windows={k:v for k,v in windows[run_id]['windows'].items() if k != 'before'},
                resources=outcome_cost(trial),
                backlog_recovery_and_full_completion=rec.get('confirmed_epoch') is not None and trial['unfinished'] == 0,
                application_sufficiency=None)
            if old:
                outcomes['mean_evaluation_backlog_offsets'] = old['statistics']['total_lag']['sample_mean']
            else:
                outcomes['mean_evaluation_backlog_offsets'] = trial['mean_lag']
            rows.append(dict(group=group,group_title=title,run_id=run_id,run_number=trial['run_number'],
                workload_seed=trial['seed'],action=trial['arm'],target_rate=rate,high_input_partitions=len(hot),
                grouping_key=group+'/run'+str(trial['run_number']),
                campaign_revision=comparison['campaign_revision'],
                campaign_resume_events=comparison.get('resume_events', []),
                configuration_sha256=manifest['config_sha256'],
                initial_resources=manifest['initial_consumer_resources'],
                before=before,outcomes=outcomes))
            print('Analyzed',group,run_id,flush=True)
    if len({r['run_id'] for r in rows}) != 20:
        raise ValueError('Expected 20 unique performance trials')
    return dict(schema_version=1,source_sha256=hashes,runs=rows,
        definitions={
            'unit':'One performance trial; scheduled intervention at evaluation second 60.',
            'pre_action_means':'15 valid, contiguous observations at nominal seconds 30,32,...,58; equal weight. Missing or discontinuous grids remain unavailable.',
            'growth':'Last pre-action trailing 30-second growth, (Q58-Q28)/(t58-t28), using 16 valid contiguous snapshots. B growth is also retained. This differs from a mean of the historical 10-second plotted growth series.',
            'persistence':'At second 58: hot now and hot in at least 80% of the last 15 observations; lag > 10 and lag > partition mean + one population standard deviation.',
            'resources':'CPU in process cores; RSS in MiB. Requires fresh per-incarnation observations for all original consumers at all 15 sample points. No imputation.',
            'full_evaluation_means':'Equal-weight means of valid raw evaluation observations, reused from the reviewed summaries; not a time-weighted mean or a mean of rolling means.',
            'recovery':'Total processing backlog <= 100 offsets for 30 continuous valid seconds during input. Gaps/ownership changes restart the hold. Keep 3 uses second 60 as reference. Unconfirmed is censored, not zero or necessarily persistent failure.',
            'outcomes':'Completion before commit acknowledgment; cohort p99 and unfinished followed through fixed drain. Completion-window statistics use different populations and exclude drain.',
            'useful_throughput':'Distinct messages completed during the 600-second evaluation divided by 600 seconds; includes warm-up messages finishing in that interval. It is not completed evaluation-cohort messages divided by the observation period.',
            'cost':'Requested consumer resources integrated over evaluation plus drain, retained only with full request-history coverage. Not actual usage or whole-cluster cost.',
            'interpretation':'Descriptive associations; no fitted predictive model, causal feature attribution, pooled rate comparison or application-sufficiency label.'})


def flatten(row):
    b,o=row['before'],row['outcomes']
    result={k:row[k] for k in ('group','run_id','run_number','workload_seed','action','target_rate','high_input_partitions','grouping_key')}
    result.update({'pre_'+k:v for k,v in b.items() if not isinstance(v,(dict,list))})
    result.update(pre_consumer_cpu_cores=b['resources']['cpu_percent']['total_mean'],
        pre_consumer_rss_mib=b['resources']['memory_bytes']['total_mean'],
        pre_acknowledged_input_msg_s=b['performance']['acknowledged_input_per_second'],
        pre_completion_throughput_msg_s=b['performance']['completion_throughput_per_second'],
        pre_completion_window_p99_seconds=b['performance']['completion_window_p99_seconds'],
        pre_pending_evaluation_messages=b['performance']['acknowledged_evaluation_messages_pending_at_end'])
    result.update({'outcome_'+k:v for k,v in o.items() if not isinstance(v,(dict,list))})
    result.update({'outcome_'+k:v for k,v in o['resources'].items()})
    return result


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repository',type=Path,default=Path(__file__).resolve().parents[2])
    parser.add_argument('--inputs',type=Path)
    parser.add_argument('--raw600',type=Path)
    parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args()
    data=load_analysis(args.repository,args.inputs or args.repository,args.raw600 or args.repository)
    data['analysis_source_sha256']=digest(Path(__file__).read_bytes())
    args.output.mkdir(parents=True,exist_ok=True)
    (args.output/'analysis.json').write_text(json.dumps(data,indent=2,allow_nan=False)+'\n')
    rows=[flatten(row) for row in data['runs']]
    with (args.output/'trial-data.csv').open('w',newline='') as stream:
        writer=csv.DictWriter(stream,fieldnames=list(rows[0]),lineterminator='\n')
        writer.writeheader();writer.writerows(rows)


if __name__=='__main__':
    main()
