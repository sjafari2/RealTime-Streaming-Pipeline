"""Combine completed traffic repetitions only after checking their compatibility."""
import argparse
import collections
import hashlib
import importlib.util
import json
import os
from pathlib import Path

from repetition_statistics import validate_design

REPO = Path(os.environ.get('PIPELINE_REPOSITORY', '/Users/soheila/Desktop/Thesis-26-27/code'))
ARMS = ('keep3', 'redistribute3', 'scale_redistribute6', 'kafka_scale6')
CONDITIONS = ('hot12', 'hot4')
MATCHED_FIELDS = (
    'aggregate_rate', 'partitions', 'hot_fraction', 'initial_hot_owner',
    'hot_partition_counts', 'comparison_phases_seconds',
    'balanced_reference_phases_seconds', 'stress', 'balanced_admission', 'placement',
)
# These identify the run or name interchangeable synthetic partitions. Their
# semantic roles are checked separately by the original per-trial analysis.
RUN_IDENTIFIERS = {
    'EXP_ID', 'RUN_ID', 'TOPIC_TITLE', 'CONSUMER_GROUP_ID', 'PRODUCER_CLIENT_ID',
    'CONSUMER_CLIENT_ID', 'WORKLOAD_SEED', 'EXPLICIT_ASSIGNMENT_JSON', 'SKEW_PARTITION',
}


def read(path):
    return json.loads(Path(path).read_text())


def digest(path):
    h = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            h.update(block)
    return h.hexdigest()


def save(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix('.tmp')
    temporary.write_text(json.dumps(value, indent=2, allow_nan=False) + '\n')
    temporary.replace(path)


def validate_campaign(campaign, repetitions):
    if campaign.get('status') != 'complete' or campaign.get('restoration') != 'verified':
        raise ValueError('Only completed, restored campaigns can be combined')
    runs = campaign.get('runs', [])
    expected = {(c + '__' + a, n, 80 + n) for c in CONDITIONS for a in ARMS for n in repetitions}
    actual = [(r['arm'], r['run_number'], r['seed']) for r in runs]
    if len(actual) != len(expected) or set(actual) != expected:
        raise ValueError('Campaign does not contain the expected unique repetitions')
    if actual != [tuple(row) for row in campaign['cost_protocol']['order']]:
        raise ValueError('Completed runs differ from the frozen trial order')
    if any(r.get('validation', {}).get('status') != 'passed' for r in runs):
        raise ValueError('A trial lacks passed validation')
    if len({r['run_id'] for r in runs}) != len(runs):
        raise ValueError('Duplicate run identity')


def validate_protocols(previous, current):
    old = previous['cost_protocol']['matched_workloads']
    new = current['cost_protocol']['matched_workloads']
    mismatches = [key for key in MATCHED_FIELDS if key not in old or key not in new or old[key] != new[key]]
    if mismatches:
        raise ValueError('Incompatible comparison settings: ' + ', '.join(mismatches))


def verify_evidence(row):
    directory = REPO / row['raw_evidence']
    expected = dict(row['evidence_hashes'])
    expected.update(row['intervention_cost']['event_file_sha256'])
    expected['matched-placement-checks.jsonl'] = row['matched_placement_sha256']
    for name, sha in expected.items():
        if digest(directory / name) != sha:
            raise ValueError('Changed evidence: ' + row['run_id'] + '/' + name)
    checks = [json.loads(line) for line in (directory / 'matched-placement-checks.jsonl').read_text().splitlines()]
    if not checks or any(check['issues'] for check in checks):
        raise ValueError('Matched placement was not verified')


def validate_configurations(rows):
    for condition in CONDITIONS:
        for arm in ARMS:
            group = [r for r in rows if r['backlog_condition'] == condition and r['arm'] == arm]
            configurations = [{k: v for k, v in r['config'].items() if k not in RUN_IDENTIFIERS} for r in group]
            if any(config != configurations[0] for config in configurations[1:]):
                raise ValueError('Runtime configuration differs for ' + condition + '/' + arm)
            ownership_sizes = [dict(collections.Counter(r['initial_ownership'].values())) for r in group]
            if any(counts != ownership_sizes[0] for counts in ownership_sizes[1:]):
                raise ValueError('Initial partition counts differ for ' + condition + '/' + arm)
    for role in ('producer', 'consumer'):
        versions = {json.dumps({k: v for k, v in entry.items() if k not in ('pod', 'role')}, sort_keys=True)
                    for row in rows for entry in row['intervention_cost']['runtime_software'] if entry['role'] == role}
        if len(versions) != 1:
            raise ValueError('Runtime software differs across repetitions: ' + role)


def reviewed_attempts(campaign):
    """Keep reviewed technical exclusions explicit and verify retained evidence."""
    accepted = {r['run_id'] for r in campaign['runs']}
    attempts = campaign.get('technical_attempts', [])
    for attempt in attempts:
        trial = attempt['trial']
        if trial['run_id'] in accepted:
            raise ValueError('A reviewed technical attempt is also an accepted repetition')
        if not attempt.get('classification') or not attempt.get('retention'):
            raise ValueError('A technical exclusion lacks its review or retention record')
        directory = Path(trial['directory'])
        for name, expected in attempt.get('timing_review', {}).get('source_sha256', {}).items():
            if digest(directory / name) != expected:
                raise ValueError('Technical-attempt evidence changed: ' + trial['run_id'] + '/' + name)
        retention = attempt['retention']
        if isinstance(retention, dict) and retention.get('verified_backup'):
            hashes = read(Path(retention['verified_backup']) / 'backup-sha256.json')
            for name, expected in hashes.items():
                if digest(directory / name) != expected:
                    raise ValueError('Technical-attempt evidence changed: ' + trial['run_id'] + '/' + name)
    return attempts


def series_history(path):
    """Record the runner revision of each resume, not only the first campaign."""
    history, seen = [], set()
    while path:
        path = Path(path).resolve()
        if path in seen:
            raise ValueError('Cycle in series resume history')
        seen.add(path)
        series = read(path)
        history.append(dict(path=str(path), sha256=digest(path),
                            code_commit=series.get('code_commit'),
                            status=series.get('status'), error=series.get('error')))
        path = Path(series['resumed_from']) / 'series-status.json' if series.get('resumed_from') else None
    return history


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--previous-comparison', type=Path, required=True)
    parser.add_argument('--previous-campaign', type=Path, required=True)
    parser.add_argument('--new-series', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    old_campaign = read(args.previous_campaign / 'campaign-status.json')
    series = read(args.new_series)
    if series.get('status') != 'complete' or series.get('restoration') != 'verified':
        raise ValueError('The new series has not completed and restored')
    new_path = Path(series['campaign_directory'])
    new_campaign = read(new_path / 'campaign-status.json')
    validate_campaign(old_campaign, (1, 2))
    validate_campaign(new_campaign, (3,))
    validate_protocols(old_campaign, new_campaign)
    old = read(args.previous_comparison)
    if old.get('status') != 'complete' or old['campaign_sha256'] != digest(args.previous_campaign / 'campaign-status.json'):
        raise ValueError('Prior comparison is not tied to the verified original campaign')
    validate_design(old['runs'], CONDITIONS, ARMS, (1, 2))
    if {r['run_id'] for r in old['runs']} != {r['run_id'] for r in old_campaign['runs']}:
        raise ValueError('Original comparison identities differ from its campaign')
    for name, expected in old.get('resource_observer_sha256', {}).items():
        if digest(args.previous_campaign / name) != expected:
            raise ValueError('Original resource observer evidence changed: ' + name)
    rows = list(old['runs'])
    for row in rows:
        verify_evidence(row)
        print('Verified previous', row['run_id'], flush=True)
    script = REPO / 'experiments/paired-metric-means/analyze_matched.py'
    spec = importlib.util.spec_from_file_location('original_matched_analysis', script)
    analysis = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(analysis)
    resources_files = sorted(new_path.rglob('resource-observations.jsonl'))
    resources = sorted([json.loads(line) for p in resources_files for line in p.read_text().splitlines()], key=lambda r: r['timestamp'])
    cache = args.output / 'verified-trials'
    for trial in new_campaign['runs']:
        cached = cache / (trial['run_id'] + '.json')
        fingerprint = hashlib.sha256(json.dumps(trial, sort_keys=True).encode()).hexdigest()
        if cached.exists():
            saved = read(cached)
            if saved['trial_sha256'] != fingerprint or saved['analysis_module_sha256'] != digest(script):
                raise ValueError('Cached analysis provenance differs')
            row = saved['row']
        else:
            row = analysis.analyze_trial(trial, new_campaign, resources)
            save(cached, dict(trial_sha256=fingerprint, analysis_module_sha256=digest(script), row=row))
        verify_evidence(row)
        rows.append(row)
        print('Verified new', row['run_id'], flush=True)
    validate_design(rows, CONDITIONS, ARMS, (1, 2, 3))
    validate_configurations(rows)
    save(args.output / 'comparison.json', dict(
        status='complete', runs=rows,
        campaign_revisions=[old_campaign['code_commit'], new_campaign['code_commit']],
        original_comparison_sha256=digest(args.previous_comparison),
        campaign_sha256={str(p): digest(p) for p in [args.previous_campaign / 'campaign-status.json', new_path / 'campaign-status.json']},
        analysis_script_sha256=digest(__file__), analysis_module_sha256=digest(script),
        resource_observer_sha256={str(p): digest(p) for p in resources_files},
        series_history=series_history(args.new_series),
        reviewed_technical_attempts=reviewed_attempts(new_campaign),
        campaign_resume_events=new_campaign.get('resume_events', []),
        comparability='Matched protocol, fixed placements, runtime configurations and software verified. Shared-node activity may differ between dates.',
        excluded_attempts='Failed, incomplete, and reviewed technical attempts remain in the original evidence and are not counted as matched repetitions. Reviewed exclusions and resumed runner revisions are listed explicitly.',
    ))
    print('Complete', args.output / 'comparison.json', flush=True)


if __name__ == '__main__':
    main()
