"""Process lifetimes, sampled resource requests and observable intervention timing."""
from decimal import Decimal
import json
import math
from pathlib import Path
from evidence_io import event_paths, open_events
import re


def quantity(value):
    """Read Kubernetes quantities as cores or bytes, before unit conversion."""
    match = re.fullmatch(r'([+]?(?:\d+(?:\.\d*)?|\.\d+))([eE][+-]?\d+|[KMGTPE]i|[numkKMGTPE]?)', str(value))
    if not match:
        raise ValueError('Unsupported resource quantity: ' + str(value))
    number, suffix = match.groups()
    decimal = {'': 0, 'n': -9, 'u': -6, 'm': -3, 'k': 3, 'K': 3, 'M': 6, 'G': 9, 'T': 12, 'P': 15, 'E': 18}
    factor = Decimal(2) ** (10 * ('KMGTPE'.index(suffix[0]) + 1)) if suffix.endswith('i') else Decimal(10) ** (
        int(suffix[1:]) if len(suffix) > 1 and suffix[0] in 'eE' else decimal[suffix])
    result = float(Decimal(number) * factor)
    if not math.isfinite(result):
        raise ValueError('Non-finite resource quantity')
    return result


def pod_observation(item):
    metadata, spec, status = (item.get(k, {}) for k in ('metadata', 'spec', 'status'))
    container = next((c for c in spec.get('containers', []) if c['name'] == 'consumer-container'), None)
    if container is None:
        raise ValueError('Consumer pod has no consumer-container: ' + metadata.get('name', '?'))
    requests = container.get('resources', {}).get('requests', {})
    # Record the declared application-container requests, not actual CPU/RSS usage or whole-pod cost.
    unsupported = bool(spec.get('resources')) or bool(status.get('resize'))
    row = dict(uid=metadata['uid'], pod=metadata['name'], node=spec.get('nodeName'),
               created_at=metadata.get('creationTimestamp'), deleting_at=metadata.get('deletionTimestamp'),
               phase=status.get('phase'), cpu_request_cores=None if unsupported else quantity(requests.get('cpu', '0')),
               memory_request_gib=None if unsupported else quantity(requests.get('memory', '0')) / 2**30,
               unsupported_resource_accounting=unsupported)
    ready = next((c for c in status.get('conditions', []) if c.get('type') == 'Ready'), {})
    row.update(ready=ready.get('status') == 'True', ready_transition=ready.get('lastTransitionTime'))
    return row


def resource_integral(samples, start, end, max_gap):
    covered = cpu = memory = pod_seconds = 0.0
    missing_resources = 0
    for left, right in zip(samples, samples[1:]):
        dt = right['timestamp'] - left['timestamp']
        span = min(end, right['timestamp']) - max(start, left['timestamp'])
        if not left.get('valid') or not right.get('valid') or not 0 < dt <= max_gap or span <= 0:
            continue
        covered += span
        for pod in left['pods']:
            # Include scheduled pods, including readiness and termination; unscheduled pods have no allocated node.
            if not pod.get('node') or pod.get('phase') in ('Succeeded', 'Failed'):
                continue
            pod_seconds += span
            if pod['cpu_request_cores'] is None or pod['memory_request_gib'] is None:
                missing_resources += 1
                continue
            cpu += pod['cpu_request_cores'] * span
            memory += pod['memory_request_gib'] * span
    return dict(covered_seconds=covered, observation_seconds=max(0, end-start),
                covered_fraction=covered/(end-start) if end > start else None,
                scheduled_consumer_pod_seconds_observed=pod_seconds if covered else None,
                consumer_container_requested_cpu_seconds_observed=cpu if covered and not missing_resources else None,
                consumer_container_requested_gib_seconds_observed=memory if covered and not missing_resources else None,
                intervals_with_unsupported_resources=missing_resources)


def recovery(snapshots, anchor, end, threshold, hold, max_gap):
    beginning = previous = None
    valid_count = 0
    for row in snapshots:
        now = row['timestamp']
        if not anchor <= now <= end:
            continue
        if not row.get('valid') or not isinstance(row.get('processing_backlog'), (float, int)):
            beginning = previous = None
            continue
        valid_count += 1
        contiguous = previous is not None and 0 < now-previous['timestamp'] <= max_gap and row.get('owners') == previous.get('owners')
        if row['processing_backlog'] > threshold:
            beginning = None
        elif beginning is None or not contiguous:
            beginning = now
        if beginning is not None and now-beginning >= hold:
            return dict(status='recovered', threshold_offsets=threshold, hold_seconds=hold,
                        qualifying_interval_start_epoch=beginning, confirmed_epoch=now,
                        seconds_to_confirmation=now-anchor, censored=False)
        previous = row
    return dict(status='not_observed_by_evaluation_end' if valid_count else 'unavailable',
                threshold_offsets=threshold, hold_seconds=hold, seconds_to_confirmation=None,
                censored=True, followup_seconds=max(0, end-anchor))


def analyze(directory, lag=None):
    directory = Path(directory)
    manifest = json.loads((directory / 'manifest.json').read_text())
    lifetimes, ownership, callbacks, resumes = [], [], [], []
    for path in event_paths(directory):
        final_path = path.with_name('final.json')
        final = json.loads(final_path.read_text()) if final_path.exists() else {}
        started = finished = elapsed = None
        callback_starts, waiting_for_completion = {}, {}
        with open_events(path) as stream:
            for line in stream:
                event = json.loads(line)
                kind, timestamp = event.get('event'), event.get('timestamp')
                if kind == 'started': started = timestamp
                elif kind == 'finished':
                    finished = timestamp
                    elapsed = event.get('lifetime_seconds')
                elif kind in ('assign_started', 'assign', 'revoke_started', 'revoke', 'lost'):
                    ownership.append(dict(event, pod=final.get('pod'), incarnation=final.get('incarnation')))
                    if kind.endswith('_started'):
                        callback_starts[kind[:-8]] = timestamp
                    elif kind in ('assign', 'revoke'):
                        before = callback_starts.pop(kind, None)
                        callbacks.append(dict(pod=final.get('pod'), incarnation=final.get('incarnation'), event=kind,
                            started_epoch=before, finished_epoch=timestamp,
                            blocked_seconds=timestamp-before if before is not None and timestamp >= before else None))
                    for partition in event.get('partitions', []):
                        key = tuple(partition)
                        if kind == 'assign':
                            row = dict(pod=final.get('pod'), incarnation=final.get('incarnation'), topic=key[0], partition=key[1],
                                       assigned_epoch=timestamp, first_completion_epoch=None, assign_to_first_completion_seconds=None)
                            resumes.append(row)
                            waiting_for_completion[key] = row
                        elif kind in ('revoke', 'lost'):
                            waiting_for_completion.pop(key, None)
                elif kind == 'completed':
                    key = (event.get('topic'), event.get('partition'))
                    row = waiting_for_completion.pop(key, None)
                    if row is not None:
                        finished_at = event['completion_timestamp']
                        row.update(first_completion_epoch=finished_at, assign_to_first_completion_seconds=
                                   finished_at-row['assigned_epoch'] if finished_at >= row['assigned_epoch'] else None)
        timestamps_present = all(isinstance(t, (float, int)) and math.isfinite(t) for t in (started, finished))
        valid = timestamps_present and finished >= started
        monotonic = timestamps_present and isinstance(elapsed, (float, int)) and math.isfinite(elapsed) and elapsed >= 0
        lifetimes.append(dict(role=final.get('role'), pod=final.get('pod'), incarnation=final.get('incarnation'),
                              started_epoch=started, finished_epoch=finished,
                              process_seconds=elapsed if monotonic else (finished-started if valid and elapsed is None else None),
                              duration_source='monotonic' if monotonic else 'wall_clock_fallback',
                              evidence=str(path.relative_to(directory))))
    consumers = [r for r in lifetimes if r['role'] == 'consumer']
    complete = bool(consumers) and all(r['process_seconds'] is not None for r in consumers)
    result = dict(run_id=manifest['run_id'], process_lifetimes=lifetimes,
                  consumer_process_seconds=sum(r['process_seconds'] for r in consumers) if complete else None,
                  consumer_lifetime_coverage_complete=complete,
                  ownership_events=sorted(ownership, key=lambda r: r.get('timestamp', 0)),
                  callback_blocking_intervals=callbacks, assignment_first_completions=resumes,
                  resource_requests=None, recovery=None)
    resource_path = directory / 'resource-history.jsonl'
    if resource_path.exists():
        samples = [json.loads(line) for line in resource_path.read_text().splitlines()]
        if samples:
            result['resource_requests'] = resource_integral(samples, manifest.get('preparing_epoch', samples[0]['timestamp']),
                samples[-1]['timestamp'], float(manifest['config'].get('RESOURCE_MAX_GAP_SECONDS', 15)))
            result['observed_consumer_pods'] = list({p['uid']: p for s in samples if s.get('valid') for p in s['pods']}.values())
    action_path = directory / 'intervention-events.jsonl'
    result['intervention_events'] = [json.loads(line) for line in action_path.read_text().splitlines()] if action_path.exists() else []
    plan = manifest.get('intervention', {})
    if plan.get('at_epoch') is not None and plan.get('recovery_threshold_offsets') is not None:
        result['recovery'] = recovery(lag['snapshots'] if lag else [], plan['at_epoch'], manifest['producer_end_epoch'],
                                      plan['recovery_threshold_offsets'], plan['recovery_hold_seconds'],
                                      float(manifest['config'].get('LAG_ANALYSIS_MAX_GAP_SECONDS', 3)))
    result['notes'] = [
        'Consumer process-seconds sum every recorded started-to-finished lifetime, including preparation, warm-up and drain; new runtime evidence uses a local monotonic timer.',
        'Final scrape hold after the finished event is outside process-seconds; scheduled-pod accounting includes observed hold time.',
        'Resource request integrals use left-held samples on covered intervals. Transition times are uncertain by up to a sample gap.',
        'CPU/GiB request-time covers only consumer-container declarations on scheduled pods, not broker/producer/sidecar/whole-cluster cost or actual usage.',
        'Missing samples and unsupported pod-level resources/resize are not silently treated as zero. Report coverage.',
        'Recovery requires processing backlog at or below the declared threshold for the full hold interval with valid contiguous same-owner samples.',
        'Recovery is confirmed at the end of the hold interval; missing/unobserved recovery is censored, not zero.',
        'Ownership callbacks are observable boundaries, not proof of broker-coordinator timing or a precise processing pause.',
        'Callback blocking intervals bracket the sequential consumer callback; assignment-to-first-completion also includes idle time and work.'
    ]
    return result


if __name__ == '__main__':
    import argparse
    from analyze_lag import analyze as lag_analysis
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('run_directory', type=Path)
    args = parser.parse_args()
    lag = lag_analysis(args.run_directory) if (args.run_directory / 'prometheus.json').exists() else None
    result = analyze(args.run_directory, lag)
    output = args.run_directory / 'execution-summary.json'
    output.write_text(json.dumps(result, indent=2, allow_nan=False) + '\n')
    print('Execution summary:', output)
