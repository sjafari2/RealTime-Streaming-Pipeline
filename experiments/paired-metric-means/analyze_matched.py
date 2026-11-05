"""Reconcile each completed matched trial, then plot the complete two-run pairs."""
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import time

ROOT = Path(os.environ.get('PIPELINE_REPOSITORY', Path(__file__).resolve().parents[2]))
sys.path.insert(0, str(ROOT / 'python-scripts'))
from intervention_cost import recovery


def read(path):
    return json.loads(Path(path).read_text())


def save(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + '.tmp')
    temporary.write_text(json.dumps(value, indent=2, allow_nan=False) + '\n')
    temporary.replace(path)


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


summary = load('matched_summary', ROOT / 'experiments/four-condition-20260929/summarize.py')
windows = load('matched_windows', ROOT / 'experiments/four-condition-20260929/analyze_action_windows.py')


def completed_prefix(campaign):
    expected = [tuple(r) for r in campaign['cost_protocol']['order']]
    observed = [(r['arm'], r['run_number'], r['seed']) for r in campaign['runs']]
    if observed != expected[:len(observed)]:
        raise ValueError('Completed trials are not the planned prefix')
    if len(expected) != 16 or len(set(expected)) != 16:
        raise ValueError('Expected the authorized sixteen-trial design')
    if any(r['validation']['status'] != 'passed' for r in campaign['runs']):
        raise ValueError('An unvalidated attempt was included in completed trials')
    return observed == expected


def analyze_trial(trial, campaign, resources):
    condition, arm = trial['arm'].split('__')
    if condition not in ('hot12', 'hot4'):
        raise ValueError('Unexpected workload condition')
    count = int(condition[3:])
    hot = campaign['hot_partitions'][:count]
    directory = Path(trial['directory'])
    placement = directory / 'matched-placement-checks.jsonl'
    checks = [json.loads(line) for line in placement.read_text().splitlines()]
    if not checks or any(check['issues'] for check in checks):
        raise ValueError('Missing or failed matched placement evidence')
    result, _, _ = summary.load(dict(trial, arm=arm), resources,
        campaign['reference'], hot, 15, 600, 1000, [500, 1000])
    manifest = read(directory / 'manifest.json')
    lag = read(directory / 'lag-summary.json')
    start, end = manifest['evaluation_start_epoch'], manifest['producer_end_epoch']
    result.update(backlog_condition=condition,
        activation=windows.post_action_boundary(directory, manifest, result),
        recovery_common_decision=recovery(lag['snapshots'], start + 60, end, 100),
        matched_placement_sha256=digest(placement),
        arrival_and_capacity={'before': {'status': 'not yet derived',
            'reason': 'The balanced reference establishes sustainability, not saturated consumer capacity.'}})
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('campaign', type=Path)
    parser.add_argument('output', type=Path)
    parser.add_argument('--wait', action='store_true')
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    cache = args.output / 'verified-trials'
    cache.mkdir(exist_ok=True)
    rows = {}
    status_file = args.output / 'analysis-status.json'
    started = time.time()
    save(status_file, dict(status='waiting_for_trials', started_epoch=started))
    while True:
        campaign = read(args.campaign / 'campaign-status.json')
        complete = completed_prefix(campaign)
        resource_files = sorted(args.campaign.rglob('resource-observations.jsonl'))
        resources = []
        for path in resource_files:
            raw = path.read_text()
            resources.extend(json.loads(line) for line in raw[:raw.rfind('\n')+1].splitlines())
        resources.sort(key=lambda r: r['timestamp'])
        for trial in campaign['runs']:
            if trial['run_id'] in rows:
                continue
            cached = cache / (trial['run_id'] + '.json')
            if cached.exists():
                result = read(cached)
                for name, expected in result['evidence_hashes'].items():
                    if digest(Path(trial['directory']) / name) != expected:
                        raise ValueError('Changed evidence for ' + trial['run_id'])
            else:
                result = analyze_trial(trial, campaign, resources)
                save(cached, result)
            rows[trial['run_id']] = result
            print('Verified', trial['arm'], trial['run_number'], trial['run_id'], flush=True)
            save(status_file, dict(status='analyzing_completed_trials',
                verified_trials=len(rows), planned_trials=16, started_epoch=started))
        if complete and campaign.get('status') == 'complete' and campaign.get('restoration') == 'verified':
            break
        if not args.wait:
            save(status_file, dict(status='partial', verified_trials=len(rows), planned_trials=16))
            return
        # Infrastructure retries may replace the series while retaining this campaign.
        # No plots or pair means are published until every planned run is verified.
        save(status_file, dict(status='waiting_for_trials', verified_trials=len(rows),
            planned_trials=16, campaign_status=campaign['status'], started_epoch=started))
        time.sleep(30)
    results = [rows[trial['run_id']] for trial in campaign['runs']]
    for role in ('producer', 'consumer'):
        software = {json.dumps({k:v for k,v in entry.items() if k not in ('pod','role')},sort_keys=True)
            for r in results for entry in r['intervention_cost']['runtime_software'] if entry['role']==role}
        if len(software) != 1:
            raise ValueError('Runtime software differs within the comparison: ' + role)
    comparison = args.output / 'comparison.json'
    save(comparison, dict(status='complete', runs=results, cost_protocol=campaign['cost_protocol'],
        campaign_revision=campaign['code_commit'], campaign_sha256=digest(args.campaign/'campaign-status.json'),
        analysis_script_sha256=digest(__file__),
        resource_observer_sha256={str(p.relative_to(args.campaign)):digest(p) for p in resource_files}))
    subprocess.run([sys.executable, str(Path(__file__).with_name('plot_metrics.py')),
        '--dataset','matched-skew','--comparison',str(comparison),
        '--output',str(args.output/'figures')],check=True)
    save(status_file, dict(status='complete', verified_trials=len(results),
        started_epoch=started, finished_epoch=time.time()))


if __name__ == '__main__':
    try:
        main()
    except Exception as exc:
        if len(sys.argv) > 2:
            save(Path(sys.argv[2])/'analysis-status.json',
                dict(status='failed',error=str(exc),finished_epoch=time.time()))
        raise
