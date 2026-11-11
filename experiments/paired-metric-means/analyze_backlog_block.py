"""Analyze the separate eight-trial backlog block without pooling older deployments."""
import argparse
import importlib.util
import json
from pathlib import Path
import sys

from analyze_repetitions import (
    REPO, ARMS, digest, read, save, reviewed_attempts, series_history,
    verify_evidence,
)
from repetition_statistics import validate_design


def validate_block(series, campaign):
    if any(s.get('status') != 'complete' or s.get('restoration') != 'verified'
           for s in (series, campaign)):
        raise ValueError('The backlog block must be completed and restored')
    protocol = campaign['cost_protocol']['matched_workloads']
    expected = [(c + '__' + a, 1, 91) for a in ARMS for c in ('smaller', 'larger')]
    observed = [(r['arm'], r['run_number'], r['seed']) for r in campaign['runs']]
    if observed != expected or [tuple(r) for r in protocol['order']] != expected:
        raise ValueError('Expected one validated repetition in each of eight conditions')
    if (protocol.get('comparison_mode') != 'starting-backlog'
            or protocol.get('warmup_aggregate_rates') != {'smaller': 300, 'larger': 900}
            or protocol.get('aggregate_rate') != 600
            or protocol.get('hot_partition_counts') != [12]
            or protocol.get('comparison_phases_seconds') != [60, 600, 120]):
        raise ValueError('Unexpected backlog preparation or continuing input')
    if any(r.get('validation', {}).get('status') != 'passed' for r in campaign['runs']):
        raise ValueError('An unvalidated trial cannot enter the comparison')
    if len({r['run_id'] for r in campaign['runs']}) != 8:
        raise ValueError('A run was reused across comparison conditions')
    if series.get('starting_backlog_contrast', {}).get('status') != 'passed':
        raise ValueError('Measured starting-backlog contrast was not verified')
    if series.get('starting_backlog_preparation', {}).get('status') != 'passed':
        raise ValueError('Preparation traces were not verified')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--series', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    series = read(args.series)
    campaign_path = Path(series['campaign_directory']) / 'campaign-status.json'
    campaign = read(campaign_path)
    validate_block(series, campaign)
    sys.path.insert(0, str(REPO / 'experiments/matched-skew600-20261104'))
    from backlog_evidence import verify_preparation
    verify_preparation(series['starting_backlog_preparation'])
    script = REPO / 'experiments/paired-metric-means/analyze_matched.py'
    spec = importlib.util.spec_from_file_location('backlog_metric_methods', script)
    methods = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(methods)
    resource_files = sorted(campaign_path.parent.rglob('resource-observations.jsonl'))
    resources = sorted([json.loads(line) for p in resource_files
                        for line in p.read_text().splitlines()], key=lambda r: r['timestamp'])
    rows = []
    for trial in campaign['runs']:
        condition, arm = trial['arm'].split('__')
        directory = Path(trial['directory'])
        result, _, _ = methods.summary.load(dict(trial, arm=arm), resources,
            campaign['reference'], campaign['hot_partitions'][:12], 15, 600, 1000, [500, 1000])
        manifest = read(directory / 'manifest.json')
        schedule = json.loads(manifest['config']['PRODUCER_RATE_SCHEDULE_JSON'])
        expected = [{'at_seconds': 0, 'rate': 100 if condition == 'smaller' else 300},
                    {'at_seconds': 60, 'rate': 200}]
        if schedule != expected:
            raise ValueError('Saved producer preparation differs from its condition')
        lag = read(directory / 'lag-summary.json')
        result.update(backlog_condition=condition,
            activation=methods.windows.post_action_boundary(directory, manifest, result),
            recovery_common_decision=methods.recovery(lag['snapshots'],
                manifest['evaluation_start_epoch'] + 60, manifest['producer_end_epoch'], 100),
            matched_placement_sha256=digest(directory / 'matched-placement-checks.jsonl'),
            arrival_and_capacity={'before': {'status': 'not yet derived',
                'reason': 'Balanced reference does not establish saturated consumer capacity.'}})
        verify_evidence(result)
        rows.append(result)
        save(args.output / 'verified-trials' / (trial['run_id'] + '.json'), result)
        print('Verified', condition, arm, trial['run_id'], flush=True)
    validate_design(rows, ('smaller', 'larger'), ARMS, (1,))
    for role in ('producer', 'consumer'):
        versions = {json.dumps({k: v for k, v in entry.items() if k not in ('pod', 'role')}, sort_keys=True)
                    for r in rows for entry in r['intervention_cost']['runtime_software']
                    if entry['role'] == role}
        if len(versions) != 1:
            raise ValueError('Runtime software differs within the backlog block')
    save(args.output / 'comparison.json', dict(status='complete', runs=rows,
        cost_protocol=campaign['cost_protocol'], campaign_sha256=digest(campaign_path),
        analysis_script_sha256=digest(__file__), analysis_module_sha256=digest(script),
        resource_observer_sha256={str(p): digest(p) for p in resource_files},
        series_history=series_history(args.series),
        reviewed_technical_attempts=reviewed_attempts(campaign),
        limitation='One run per cell on the current fixed deployment. No sample standard deviation '
                   'is estimated and no October trials are pooled. Backlog and age vary together.'))
    print('Complete', args.output / 'comparison.json', flush=True)


if __name__ == '__main__':
    main()
