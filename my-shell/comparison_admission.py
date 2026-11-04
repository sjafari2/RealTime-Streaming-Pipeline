"""Admission checks for comparable deployments and deliberately stressed workloads."""
from collections import Counter
import hashlib
import json
import math
from pathlib import Path

from placement_control import validate_reference

PROCESSING_KEYS = (
    'APP_CPU_ITERATIONS', 'APP_DELAY_MS', 'APP_DELAY_MODE', 'APP_DELAY_MAX_MS',
    'PAYLOAD_SIZE_BYTES', 'POLL_MAX_MSG', 'POLL_TIMEOUT', 'FETCH_MIN_BYTES',
    'FETCH_MAX_BYTES', 'FETCH_MAX_WAIT_MS', 'MAX_PARTITION_FETCH_BYTES',
    'COMMIT_INTERVAL_SECONDS',
)


class AdmissionStopped(RuntimeError):
    """Keep the evidence and require review before another comparison is launched."""


def sha256(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def validate_stress_settings(settings):
    required = {'window_seconds', 'consecutive_windows', 'minimum_growth_offsets_per_second',
                'minimum_mean_backlog_offsets', 'minimum_coverage', 'maximum_gap_seconds'}
    if set(settings) != required:
        raise ValueError('Stress settings must specify exactly: ' + ', '.join(sorted(required)))
    for key, value in settings.items():
        if isinstance(value, bool) or not isinstance(value, (float, int)) or not math.isfinite(value) or value <= 0:
            raise ValueError('Stress threshold must be finite and positive: ' + key)
    if type(settings['consecutive_windows']) is not int or settings['consecutive_windows'] < 2:
        raise ValueError('At least two consecutive stress windows are required')
    if not .9 <= settings['minimum_coverage'] <= 1:
        raise ValueError('Stress coverage must be between 0.9 and 1')
    if settings['maximum_gap_seconds'] >= settings['window_seconds']:
        raise ValueError('Maximum monitoring gap must be shorter than a stress window')
    return settings


def load_plan(path, root):
    """Freeze a prior run's deployment rather than silently accepting today's nodes."""
    path, root = Path(path), Path(root)
    plan = json.loads(path.read_text())
    if plan.get('schema_version') != 1 or plan.get('purpose') != 'stressed_skew_comparison':
        raise ValueError('Admission plan must declare a stressed_skew_comparison')
    reference = Path(plan['reference_run'])
    if not reference.is_absolute():
        reference = root / reference
    manifest_path = reference / 'manifest.json'
    manifest = json.loads(manifest_path.read_text())
    placement = manifest['placement_reference']
    validate_reference(placement, manifest['config'])
    hot = set(map(int, manifest['config']['SKEW_PARTITION'].split(',')))
    owners = {row['pod'] for row in placement['assignment'] if row['partition'] in hot}
    if manifest['config'].get('TRAFFIC_MODE') != 'skew' or len(owners) != 1 or not hot:
        raise ValueError('Reference must record skew initially concentrated on one consumer')
    if manifest.get('intervention', {}).get('action') != 'none':
        raise ValueError('Use a no-mitigation reference run')
    owner = next(iter(owners))
    owner_node = next(p['node'] for p in placement['pods'] if p['pod'] == owner)
    if not isinstance(plan.get('hot_owner'), str) or not plan['hot_owner'].startswith('consumer-sts-'):
        raise ValueError('Declare the intended current hot-partition owner explicitly')
    if 'APP_CPU_ITERATIONS' not in manifest['config'] or 'PAYLOAD_SIZE_BYTES' not in manifest['config']:
        raise ValueError('Reference processing settings are incomplete')
    return dict(schema_version=1, purpose=plan['purpose'], plan_sha256=sha256(path),
                reference_run_id=manifest['run_id'], reference_manifest_sha256=sha256(manifest_path),
                reference_hot_owner=owner, reference_hot_node=owner_node, hot_owner=plan['hot_owner'],
                reference_pods=placement['pods'],
                processing_settings={key: manifest['config'].get(key) for key in PROCESSING_KEYS},
                stress=validate_stress_settings(plan['stress']))


def deployment_check(admission, current_pods, config):
    """Match node/resource placement by role, allowing recreated pod identities."""
    def signature(pod):
        return json.dumps({key: pod.get(key) for key in ('role', 'node', 'image_id', 'resources')},
                          sort_keys=True, separators=(',', ':'))
    expected = Counter(signature(p) for p in admission['reference_pods'])
    observed = Counter(signature(p) for p in current_pods)
    failures = []
    if expected != observed:
        failures.append('Producer/consumer node placement, container images, or resources differ from the declared reference')
    owner = next((p for p in current_pods if p['pod'] == admission['hot_owner'] and p['role'] == 'consumer'), None)
    if owner is None or owner['node'] != admission['reference_hot_node']:
        failures.append('The selected hot-partition owner is not on the reference bottleneck node')
    differences = {key: dict(reference=value, proposed=config.get(key))
                   for key, value in admission['processing_settings'].items() if config.get(key) != value}
    if differences:
        failures.append('Processing settings differ from the declared reference')
    return dict(status='passed' if not failures else 'stopped_for_review', failures=failures,
                reference_run_id=admission['reference_run_id'], hot_owner=admission['hot_owner'],
                expected_hot_node=admission['reference_hot_node'], observed_hot_node=owner['node'] if owner else None,
                missing_deployment_entries=[json.loads(s) for s in (expected-observed).elements()],
                unexpected_deployment_entries=[json.loads(s) for s in (observed-expected).elements()],
                processing_differences=differences,
                interpretation='Matching placement does not establish unchanged capacity on shared nodes. A live Keep 3 stress check is also required.')


def stress_check(manifest, snapshots, settings):
    """Require growth in each final evaluation window, excluding warm-up and drain."""
    validate_stress_settings(settings)
    if manifest.get('intervention', {}).get('action') != 'none':
        raise ValueError('Only a no-mitigation run can qualify the starting stress condition')
    start, end = manifest['evaluation_start_epoch'], manifest['producer_end_epoch']
    width, count = settings['window_seconds'], settings['consecutive_windows']
    if end-start < width*count:
        raise ValueError('Evaluation is shorter than the declared stress observation period')
    eligible, invalid = [], []
    owners = None
    previous_time = None
    first_window = end-width*count
    for row in snapshots:
        t = row.get('timestamp')
        if not isinstance(t, (int, float)) or not math.isfinite(t):
            invalid.append('Nonfinite or missing timestamp')
            continue
        if t < first_window-.01 or t > end+.01:
            continue
        lag = row.get('total_lag')
        if (not row.get('valid') or not isinstance(lag, (int, float)) or
                not math.isfinite(lag) or lag < 0 or not row.get('owners')):
            invalid.append('Missing or invalid lag/ownership observation')
            continue
        if previous_time is not None and t <= previous_time:
            invalid.append('Duplicate or decreasing monitoring time')
        previous_time = t
        if owners is not None and row['owners'] != owners:
            invalid.append('Partition ownership changed in the stress observation period')
        owners = row['owners']
        eligible.append((t, lag))
    windows = []
    for index in range(count):
        left, right = first_window+index*width, first_window+(index+1)*width
        points = [(t,b) for t,b in eligible if left-.01 <= t <= right+.01]
        gaps = [b[0]-a[0] for a,b in zip(points,points[1:])]
        coverage = min(1., sum(dt for dt in gaps if 0 < dt <= settings['maximum_gap_seconds'])/width)
        complete = (len(points) >= 2 and coverage >= settings['minimum_coverage'] and
                    all(0 < dt <= settings['maximum_gap_seconds'] for dt in gaps))
        growth = (points[-1][1]-points[0][1])/(points[-1][0]-points[0][0]) if complete else None
        mean = sum(b for _,b in points)/len(points) if complete else None
        passed = complete and growth >= settings['minimum_growth_offsets_per_second'] and mean >= settings['minimum_mean_backlog_offsets']
        windows.append(dict(start_after_evaluation_seconds=left-start, end_after_evaluation_seconds=right-start,
                            valid_observations=len(points), coverage_fraction=coverage,
                            growth_offsets_per_second=growth, mean_backlog_offsets=mean, passed=passed))
    insufficient = bool(invalid) or any(w['growth_offsets_per_second'] is None for w in windows)
    passed = not insufficient and all(w['passed'] for w in windows)
    reason = 'monitoring_insufficient' if insufficient else ('stress_demonstrated' if passed else 'stress_not_demonstrated')
    return dict(status='passed' if passed else 'stopped_for_review', reason=reason,
                run_id=manifest['run_id'], settings=settings, windows=windows, invalid_observations=invalid,
                message=('Keep 3 demonstrated the prespecified stress condition.' if passed else
                         'STOP: Keep 3 did not establish the required stress condition. Do not start further mitigation trials. Report the measurements for review without automatically changing rate, placement, or processing settings.'))


def check_saved_control(directory, admission):
    directory = Path(directory)
    manifest_path, lag_path = directory/'manifest.json', directory/'lag-summary.json'
    outcome_path = directory/'outcome-summary.json'
    status = json.loads((directory/'runner-status.json').read_text())
    outcome = json.loads(outcome_path.read_text())
    if status.get('status') != 'complete' or outcome.get('validity_failures'):
        raise AdmissionStopped('Keep 3 evidence has not passed run validation')
    result = stress_check(json.loads(manifest_path.read_text()),
                          json.loads(lag_path.read_text())['snapshots'], admission['stress'])
    result['evidence_sha256'] = {p.name: sha256(p) for p in (manifest_path,lag_path,outcome_path)}
    return result


def require_passed(report):
    if report['status'] != 'passed':
        raise AdmissionStopped(report.get('message') or 'STOP: ' + '. '.join(report['failures']))
