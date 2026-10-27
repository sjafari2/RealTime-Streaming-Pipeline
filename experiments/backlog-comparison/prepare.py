#!/usr/bin/env python3
"""Validate the deployment and two backlog preparations before comparisons."""
import argparse
import copy
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import time

import yaml

ROOT = Path(__file__).resolve().parents[2]
sys.path[:0] = [str(ROOT / 'my-shell'), str(ROOT / 'python-scripts'),
                str(ROOT / 'experiments/signal-outcome-20261021')]
import run_experiment as r
from placement_control import pod_identity
from static_startup import audit_preparation
from validate_handoff import validate
from compress_evidence import compress
from compress_monitoring_csv import compress_csv
from build_capacity import CounterData, service_window, summarize_windows
from verify_workload import compare_traces

spec = importlib.util.spec_from_file_location('four_condition_setup', ROOT / 'experiments/four-condition-20260929/run.py')
base = importlib.util.module_from_spec(spec)
spec.loader.exec_module(base)


def normalized(raw):
    document = yaml.safe_load(raw)
    for key in ('RUN_ID', 'TOPIC_TITLE'):
        document['data'].pop(key, None)
    return document


def capacity_windows(directory):
    manifest = json.loads((directory / 'manifest.json').read_text())
    lag = json.loads((directory / 'lag-summary.json').read_text())
    counters = CounterData(json.loads((directory / 'prometheus.json').read_text()), manifest)
    result = {}
    for pod in manifest['consumer_pods']:
        windows, errors = [], []
        for left, right in ((30, 90), (90, 150)):
            try:
                windows.append(service_window(counters, lag, manifest, pod, left, right))
            except ValueError as exc:
                errors.append(str(exc))
        result[pod] = dict(windows=windows, errors=errors, summary=summarize_windows(windows))
    return result


def verify_pilot(directory, rate):
    from evidence_io import event_paths, open_events
    manifest = json.loads((directory / 'manifest.json').read_text())
    lag = json.loads((directory / 'lag-summary.json').read_text())
    changes, admissions = {}, {}
    for path in event_paths(directory / 'producer'):
        rows = []
        counts = {'warmup': 0, 'evaluation': 0}
        with open_events(path) as stream:
            for line in stream:
                row = json.loads(line)
                if row['event'] == 'rate_changed':
                    rows.append(row)
                if row['event'] == 'acknowledged':
                    produced = row['producer_timestamp']
                    if manifest['start_epoch'] <= produced < manifest['evaluation_start_epoch']:
                        counts['warmup'] += 1
                    elif manifest['evaluation_start_epoch'] <= produced < manifest['producer_end_epoch']:
                        counts['evaluation'] += 1
        changes[path.parent.parent.name] = rows
        admissions[path.parent.parent.name] = counts
    if set(changes) != set(manifest['producer_pods']):
        raise ValueError('Missing producer rate-change evidence')
    for rows in changes.values():
        if ([x['stage'] for x in rows] != [0, 1]
                or [x['target_rate'] for x in rows] != [rate / 3, 200]
                or any(abs(x['scheduling_delay_seconds']) > 2 or x['discarded_pacing_debt'] > 2 for x in rows)):
            raise ValueError('Producer did not follow the common input schedule within tolerance')
    for counts in admissions.values():
        expected = {'warmup': rate / 3 * 60,
                    'evaluation': 200 * (manifest['producer_end_epoch'] - manifest['evaluation_start_epoch'])}
        if any(abs(counts[phase] / count - 1) > .03 for phase, count in expected.items()):
            raise ValueError('Acknowledged input differs from the scheduled rate by more than three percent')
    end = manifest['evaluation_start_epoch'] + 60
    # Prometheus exports timestamps rounded to milliseconds.
    before = [x for x in lag['snapshots'] if x['valid'] and end - 2 <= x['timestamp'] <= end + .001]
    if not before or lag['covered_fraction'] < .95:
        raise ValueError('Insufficient monitoring for the backlog preparation')
    point = before[-1]
    return dict(run_id=manifest['run_id'], directory=str(directory),
                pre_action_processing_backlog=point['processing_backlog'],
                pre_action_total_lag=point['total_lag'],
                sample_epoch=point['timestamp'], lag_coverage=lag['covered_fraction'],
                rate_changes=changes, acknowledged_counts=admissions)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--calibration-reference', required=True)
    parser.add_argument('--reuse-capacity', type=Path,
                        help='Recheck only the pilots using an already validated, unchanged consumer deployment')
    parser.add_argument('--execute', action='store_true')
    args = parser.parse_args()
    if not args.execute:
        print('Readiness of six consumers; empty-topic reference; busy capacity calibration; 300/900 warm-up pilots returning to 600 total.')
        return
    if subprocess.check_output(['git', 'status', '--porcelain', '--untracked-files=no'], cwd=ROOT).strip():
        raise RuntimeError('Commit the reviewed source before preparation')
    work = ROOT / 'results' / ('backlog-preparation-' + time.strftime('%Y%m%d-%H%M%S'))
    work.mkdir()
    state = dict(status='preparing', runs=[], calibration_reference=args.calibration_reference,
                 code_revision=subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT).decode().strip())
    def save():
        temp = work / 'status.tmp'
        temp.write_text(json.dumps(state, indent=2) + '\n')
        temp.replace(work / 'status.json')
    save()
    print('[PREPARATION]', work, flush=True)
    with r.command_lock():
        control = r.read_control()
        if control and control.get('state') in ('preparing', 'running'):
            raise RuntimeError('Another run is active')
        original = r.shared_read(r.CONFIG)
        (work / 'original-config.yaml').write_bytes(original)
        expected = None
        def run(config, plan, label):
            nonlocal expected
            r.stop()
            if expected is not None and normalized(r.shared_read(r.CONFIG)) != normalized(expected):
                raise RuntimeError('External configuration change')
            expected = yaml.safe_dump(config, sort_keys=False).encode()
            r.shared_write(r.CONFIG, expected)
            r.preflight()
            state.update(current=label)
            save()
            with r.prometheus_connection():
                directory = r.complete_run(plan)
            checked = audit_preparation(directory) if plan.get('prepare_only') else validate(directory, require_all=False)
            if not plan.get('prepare_only'):
                compress(directory)
                compress_csv(directory)
            state['runs'].append(dict(label=label, directory=str(directory), validation=checked))
            save()
            return directory
        try:
            with r.paused_for_experiment(r, base.HPA_PLAN):
                r.stop()
                if args.reuse_capacity:
                    previous = json.loads((args.reuse_capacity / 'status.json').read_text())
                    if (previous['status'] != 'complete' or previous.get('restoration') != 'verified'
                            or previous['calibration_reference'] != args.calibration_reference):
                        raise ValueError('Capacity preparation is incomplete or uses a different balanced reference')
                    reference = json.loads((args.reuse_capacity / 'placement-reference.json').read_text())
                    pods = json.loads(r.kubectl('get', 'pods', '-l', r.POD_SELECTOR, '-o', 'json'))['items']
                    if {p['pod']: p for p in reference['pods']} != {p['metadata']['name']: pod_identity(p) for p in pods}:
                        raise ValueError('Pod identity, placement or resources changed since capacity calibration')
                    capacity_run = Path(next(item['directory'] for item in previous['runs'] if item['label'] == 'busy-capacity'))
                    state.update(reused_preparation=str(args.reuse_capacity.resolve()),
                                 six_consumer_readiness=previous['six_consumer_readiness'])
                    state['runs'].extend(item for item in previous['runs'] if item['label'] in ('empty-reference', 'busy-capacity'))
                else:
                    # This is a scheduling check, before performance measurements.
                    r.set_consumer_baseline(6, 240)
                    state['six_consumer_readiness'] = 'passed'
                    save()
                    r.set_consumer_baseline(3, 240)
                    pods = json.loads(r.kubectl('get', 'pods', '-l', r.POD_SELECTOR, '-o', 'json'))['items']
                    config, plan = base.prepared('kafka_scale6', 0, 91, aggregate_rate=600, deadline_ms=1000)
                    plan.update(action='none', target_consumers=None, prepare_only=True,
                                capture_placement_pods=[pod_identity(p) for p in pods])
                    reference_run = run(config, plan, 'empty-reference')
                    reference = json.loads((reference_run / 'manifest.json').read_text())['placement_reference']
                    config, plan = base.prepared('keep3', 0, 90, reference, list(range(12)), aggregate_rate=600, deadline_ms=1000)
                    config['data'].update(EXP_ID='backlog-capacity-calibration', TRAFFIC_MODE='balanced',
                                          TARGET_RATE='800', EXP_DURATION_SEC='240', WARMUP_SECONDS='60')
                    capacity_run = run(config, plan, 'busy-capacity')
                (work / 'placement-reference.json').write_text(json.dumps(reference, indent=2) + '\n')
                capacities = capacity_windows(capacity_run)
                state['capacities'] = capacities
                save()
                eligible = {pod: value['summary']['effective_rate'] for pod, value in capacities.items()
                            if value['summary']['eligible_windows'] > 0}
                if len(eligible) != 3:
                    raise RuntimeError('Not all three consumers have eligible busy capacity measurements; inspect calibration')
                hot_owner = min(eligible, key=eligible.get)
                hot = sorted(row['partition'] for row in reference['assignment'] if row['pod'] == hot_owner)[:12]
                if len(hot) != 12:
                    raise RuntimeError('Hot owner lacks twelve partitions')
                pilots = {}
                for level, rate in [('smaller', 300), ('larger', 900)]:
                    config, plan = base.prepared('keep3', 0, 91, reference, hot, aggregate_rate=600, deadline_ms=1000)
                    config['data'].update(EXP_ID='backlog-' + level + '-pilot', EXP_DURATION_SEC='180',
                        PRODUCER_RATE_SCHEDULE_JSON=json.dumps([
                            {'at_seconds': 0, 'rate': rate / 3}, {'at_seconds': 60, 'rate': 200}]))
                    capacity_config = json.loads((capacity_run / 'manifest.json').read_text())['config']
                    for key in ('APP_CPU_ITERATIONS', 'APP_DELAY_MS', 'NUM_PARTITIONS'):
                        if config['data'].get(key) != capacity_config.get(key):
                            raise ValueError('Processing settings differ from the reused calibration: ' + key)
                    pilots[level] = verify_pilot(run(config, plan, level + '-pilot'), rate)
                    state['pilots'] = pilots
                    save()
                low, high = [pilots[k]['pre_action_processing_backlog'] for k in ('smaller', 'larger')]
                if high < 1.5 * low or high - low < 5000:
                    raise RuntimeError('Pilot backlogs were not sufficiently separated; revise preparation before comparisons')
                trace_check = compare_traces(pilots['smaller']['directory'], pilots['larger']['directory'])
                state['evaluation_trace_check'] = trace_check
                specification = dict(status='validated_before_performance_trials', aggregate_input_rate=600,
                    hot_partition_count=12, initial_hot_owner=hot_owner,
                    warmup_aggregate_rates={'smaller': 300, 'larger': 900},
                    calibration_reference=args.calibration_reference,
                    technical_evidence=state['runs'], capacity_reference=str(capacity_run),
                    pilot_backlogs=pilots, capacity_estimates=capacities, evaluation_trace_check=trace_check,
                    code_revision=state['code_revision'],
                    hot_owner_selection='Lowest eligible measured capacity among the original three, selected before performance comparisons.')
                (work / 'validated-specification.json').write_text(json.dumps(specification, indent=2) + '\n')
                state['status'] = 'complete'
        except BaseException as exc:
            state.update(status='failed', error=str(exc) or type(exc).__name__)
            raise
        finally:
            r.stop()
            r.set_consumer_baseline(3, 240)
            if expected is not None and normalized(r.shared_read(r.CONFIG)) != normalized(expected):
                state['restoration'] = 'external_change'
                save()
                raise RuntimeError('Configuration changed externally; restoration withheld')
            r.shared_write(r.CONFIG, original)
            if r.shared_read(r.CONFIG) != original:
                raise RuntimeError('Configuration restoration failed')
            state['restoration'] = 'verified'
            save()


if __name__ == '__main__':
    main()
