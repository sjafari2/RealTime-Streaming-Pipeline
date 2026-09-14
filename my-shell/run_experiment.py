#!/usr/bin/env python3
"""Coordinate our shared-file workflow from the machine running kubectl."""
import argparse
from contextlib import contextmanager
import fcntl
import hashlib
import json
import math
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import re
import uuid
import urllib.parse
import urllib.request
from managed_hpa import paused_for_experiment
from placement_control import (PlacementMismatch, capture_reference, check_placement,
                               pod_identity, reference_hash, validate_pod_reference, validate_reference)

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src/common'))
NS = os.getenv('NAMESPACE', 'kafkastreamingdata')
CONTROL = '/config/run-control.json'
CONFIG = '/config/pipeline-configmap.yaml'
ROLES = {'producer': ('producer-container', '/app/producer-data', 8001),
         'consumer': ('consumer-container', '/app/consumer-merge-data', 8002)}


def kubectl(*args, input=None, timeout=60, request_timeout='30s'):
    return subprocess.run(['kubectl', '-n', NS, '--request-timeout=' + request_timeout, *args],
                          input=input, capture_output=True, check=True, timeout=timeout).stdout


def pods(role):
    result = json.loads(kubectl('get', 'pods', '-l', 'app=' + role + '-sts', '-o', 'json'))
    return sorted(item['metadata']['name'] for item in result['items'] if not item['metadata'].get('deletionTimestamp'))


def remote(role, pod, code, payload=None, timeout=60):
    args = ['exec'] + (['-i'] if payload is not None else [])
    return kubectl(*args, pod, '-c', ROLES[role][0], '--', 'python3', '-c', code, input=payload, timeout=timeout)


def shared_read(path):
    return remote('consumer', pods('consumer')[0], f'from pathlib import Path; import sys; sys.stdout.buffer.write(Path({path!r}).read_bytes())')


def shared_write(path, value):
    code = f'''import os,sys,tempfile
from pathlib import Path
p=Path({path!r}); p.parent.mkdir(parents=True,exist_ok=True)
f=tempfile.NamedTemporaryFile(dir=p.parent,delete=False)
f.write(sys.stdin.buffer.read()); f.flush(); os.fsync(f.fileno()); f.close()
os.chmod(f.name,0o644); os.replace(f.name,p)
'''
    remote('consumer', pods('consumer')[0], code, value)


def read_control():
    try:
        return json.loads(shared_read(CONTROL))
    except subprocess.CalledProcessError as exc:
        if b'FileNotFoundError' in exc.stderr:
            return None
        raise


def publish(control):
    shared_write(CONTROL, json.dumps(control).encode())


def status(role, pod):
    port = ROLES[role][2]
    code = f'''import urllib.request,sys
from pathlib import Path
try:
    sys.stdout.buffer.write(urllib.request.urlopen('http://localhost:{port}/status',timeout=2).read())
except Exception:
    p=Path('/tmp/pipeline-{role}-final.json')
    print(p.read_text() if p.exists() else '{{}}')
'''
    try:
        return json.loads(remote(role, pod, code, timeout=10))
    except (subprocess.SubprocessError, ValueError):
        return {}


def force_stop_role(role, grace=60):
    # Match Python's script argument, not arbitrary text in a shell command.
    code = f'''import os,signal,time,json
from pathlib import Path
def alive(pid):
    try:
        status = (Path('/proc') / str(pid) / 'status').read_text()
        state = next(line.split()[1] for line in status.splitlines() if line.startswith('State:'))
        return state not in ('Z', 'X')  # Exited children can remain until PID 1 reaps them.
    except (OSError, StopIteration, IndexError):
        return False
pids=[]
for p in Path('/proc').iterdir():
    if not p.name.isdigit(): continue
    try:
        argv=(p/'cmdline').read_bytes().split(b'\\0')
        if len(argv)>1 and Path(os.fsdecode(argv[1])).name=={(role + '.py')!r}:

            if alive(int(p.name)): pids.append(int(p.name))
    except (OSError,ValueError): pass
for pid in pids:
    try: os.kill(pid,signal.SIGTERM)
    except ProcessLookupError: pass
end=time.monotonic()+{grace}
while pids and time.monotonic()<end:
    pids=[pid for pid in pids if alive(pid)]
    if pids: time.sleep(.25)
for pid in pids:
    try: os.kill(pid,signal.SIGKILL)
    except ProcessLookupError: pass
print(json.dumps({{'forced_pids':pids}}))
'''
    forced = []
    for pod in pods(role):
        result = json.loads(remote(role, pod, code, timeout=grace + 15))
        if result['forced_pids']:
            forced.append(pod)
    return forced


def stop():
    control = read_control()
    if control and control['state'] in ('preparing', 'running'):
        if control['state'] == 'running':
            # End production together; leave the configured drain available to consumers.
            now = time.time()
            control['intervention_cancelled_epoch'] = now
            control['producer_end_epoch'] = min(now, control['producer_end_epoch'])
            control['drain_end_epoch'] = min(control['drain_end_epoch'], now + control['drain_seconds'])
            control['expires_epoch'] = control['drain_end_epoch'] + 120
            publish(control)
            wait_for_end(control)
        control['state'] = 'stopped'
        publish(control)  # Prevent a supervisor from starting another application.
    print('[STOP] Stopping producer applications first.')
    forced = force_stop_role('producer')
    if not control:
        # For an older/manual run we cannot infer a shared end time.
        delay = float(os.getenv('LEGACY_DRAIN_SECONDS', '10'))
        print(f'[STOP] Allowing {delay:g} seconds to drain the previous manual run.')
        time.sleep(delay)
    forced += force_stop_role('consumer')
    if forced:
        raise RuntimeError('Forced termination was needed; preserve and mark this run invalid: ' + ', '.join(forced))


def validate_readiness(rows, expected, config_hash):
    seen = []
    for row in rows:
        if row.get('phase') != 'ready' or row.get('config_sha256') != config_hash or row.get('failure'):
            return False
        seen.extend(tuple(p) for p in row.get('assignments', []))
    return len(seen) == len(set(seen)) and set(seen) == set(expected)


def start_app(role, pod):
    if role == 'consumer':
        supervised = remote(role, pod, "from pathlib import Path; print(b'supervise.py' in Path('/proc/1/cmdline').read_bytes())").decode().strip()
        if supervised == 'True':
            print('[START]', pod, 'will be started by its supervisor')
            return
    path = ROLES[role][1]
    # A supervisor may already be starting it. Runtime's per-pod lock prevents duplicates.
    kubectl('exec', pod, '-c', ROLES[role][0], '--', 'bash', '-c',
            f'mkdir -p {path}/logs; nohup bash {path}/run.sh </dev/null >>{path}/logs/run_{pod}.log 2>&1 &')


def clock_probes(role, names):
    samples = []
    for pod in names:
        candidates = []
        for _ in range(3):
            before = time.time()
            remote_time = float(remote(role, pod, 'import time; print(time.time())'))
            after = time.time()
            candidates.append(dict(pod=pod, role=role, roundtrip_seconds=after-before,
                                   offset_low_seconds=remote_time-after,
                                   offset_high_seconds=remote_time-before))
        samples.append(min(candidates, key=lambda row: row['roundtrip_seconds']))
    return samples


def estimate_capacity(config, producer_count, consumer_count):
    # This is a balanced-load budget, not measured latency or a guarantee under skew.
    total_rate = float(config['TARGET_RATE']) * producer_count
    per_consumer = total_rate / consumer_count if consumer_count else None
    return dict(producer_count=producer_count, consumer_count=consumer_count,
                target_messages_per_second=total_rate,
                balanced_messages_per_second_per_consumer=per_consumer,
                balanced_processing_budget_ms=1000 / per_consumer if per_consumer and per_consumer > 0 else None,
                app_delay_ms=float(config.get('APP_DELAY_MS', 0)),
                note='Initial replica counts; assumes balanced sequential processing. All overhead uses this budget. Recalculate after scaling.')


def read_config():
    raw = shared_read(CONFIG)
    config = json.loads(remote('consumer', pods('consumer')[0],
                              'import yaml,json,sys; print(json.dumps(yaml.safe_load(sys.stdin.buffer.read())["data"]))', raw))
    return raw, config


def validate_config(config):
    duration = float(config.get('EXP_DURATION_SEC', 300))
    drain = float(config.get('DRAIN_SECONDS', 60))
    warmup = float(config.get('WARMUP_SECONDS', 0))
    readiness_timeout = float(config.get('READINESS_TIMEOUT_SECONDS', 180))
    if duration <= 0 or drain < max(float(config.get('SLO_THRESHOLD_MS', 99)) / 1000, float(config.get('PRODUCER_FLUSH_SECONDS', 30))):
        raise ValueError('DRAIN_SECONDS must cover the deadline and producer flush; EXP_DURATION_SEC must be positive')
    if warmup < 0 or warmup >= duration:
        raise ValueError('WARMUP_SECONDS must be nonnegative and shorter than EXP_DURATION_SEC')
    retention_ms = int(config.get('RETENTION_MS', 0))
    required_retention_ms = (duration + drain + readiness_timeout + 120) * 1000
    if retention_ms < required_retention_ms:
        raise ValueError(
            f'RETENTION_MS is {retention_ms} ms; needs at least {required_retention_ms:g} ms '
            f'({duration:g}s production + {drain:g}s drain + {readiness_timeout:g}s readiness + 120s margin). '
            'Update /config/pipeline-configmap.yaml between runs. The local template uses 86400000 ms (24 hours).')
    if str(config.get('ACKS', '0')) == '0':
        raise ValueError('Set ACKS=1 or all in the shared YAML before this measurement run')
    return duration, drain, warmup, readiness_timeout


def validate_intervention(plan, config):
    if plan is None:
        return
    duration, _, warmup, _ = validate_config(config)
    if plan['action'] not in ('none', 'scale', 'redistribute'):
        raise ValueError('Unknown scheduled pilot action')
    explicit = config.get('CONSUMER_ASSIGNMENT_MODE', 'cooperative') == 'explicit'
    if explicit:
        from explicit_assignment import ownership_map
        names = ['consumer-sts-' + str(i) for i in range(int(config['CONSUMER_POD_COUNT']))]
        if int(config['TOPIC_COUNT']) != 1 or str(config.get('CONSUMER_STATIC_MEMBERSHIP', 'false')).lower() != 'false':
            raise ValueError('Explicit pilot requires one topic and no static group membership')
        ownership_map(json.loads(config['EXPLICIT_ASSIGNMENT_JSON']), int(config['NUM_PARTITIONS']), names)
        if plan['action'] == 'scale':
            raise ValueError('Explicit pilot requires fixed membership')
    if plan['action'] == 'redistribute':
        if not explicit or plan.get('target_consumers') is not None:
            raise ValueError('Redistribution requires fixed explicit membership')
        ownership_map(plan['target_assignment'], int(config['NUM_PARTITIONS']), names)
    capture_pods = plan.get('capture_placement_pods')
    if capture_pods is not None:
        if not plan.get('prepare_only') or plan.get('placement_reference') is not None:
            raise ValueError('Reference capture is preparation-only and cannot replace a frozen reference')
        validate_pod_reference(capture_pods, config)
    controlled = plan.get('placement_reference') is not None or capture_pods is not None
    if plan.get('placement_reference') is not None:
        validate_reference(plan['placement_reference'], config)
    if controlled:
        if plan.get('initial_consumers') != int(config['CONSUMER_POD_COUNT']):
            raise ValueError('Controlled placement requires matching explicit initial consumers')
    if plan.get('prepare_only') and not controlled:
        raise ValueError('Preparation-only validation requires a placement reference')
    budget = plan.get('preparation_budget_seconds')
    if budget is not None and (not controlled or not math.isfinite(budget) or budget <= 0):
        raise ValueError('A positive preparation budget requires a placement reference')
    after = plan['after_evaluation_start_seconds']
    if not math.isfinite(after) or not 0 <= after < duration-warmup:
        raise ValueError('Intervention time must fall inside the evaluation interval')
    initial = plan.get('initial_consumers')
    target = plan.get('target_consumers')
    if initial is not None and initial < 1:
        raise ValueError('Initial consumer count must be positive')
    if plan['action'] == 'scale' and (initial is None or target is None or target <= initial):
        raise ValueError('Scale-up requires --initial-consumers and a larger --target-consumers')
    if plan['action'] == 'none' and target is not None:
        raise ValueError('No-action does not take --target-consumers')
    threshold, hold = plan.get('recovery_threshold_offsets'), plan.get('recovery_hold_seconds')
    if (threshold is None) != (hold is None) or (threshold is not None and
            (not all(math.isfinite(v) for v in (threshold, hold)) or threshold < 0 or hold <= 0)):
        raise ValueError('Recovery needs both a nonnegative --recovery-threshold and positive --recovery-hold')


def set_consumer_baseline(count, timeout):
    current = json.loads(kubectl('get', 'statefulset', 'consumer-sts', '-o', 'json'))['spec']['replicas']
    if current != count:
        kubectl('scale', 'statefulset', 'consumer-sts', '--current-replicas=' + str(current), '--replicas=' + str(count))
    limit = time.monotonic() + timeout
    while time.monotonic() < limit:
        items = json.loads(kubectl('get', 'pods', '-l', 'app=consumer-sts', '-o', 'json'))['items']
        ready = [p for p in items if not p['metadata'].get('deletionTimestamp') and
                 any(c.get('type') == 'Ready' and c.get('status') == 'True' for c in p.get('status', {}).get('conditions', []))]
        if len(items) == len(ready) == count:
            return
        time.sleep(2)
    raise RuntimeError('Consumer baseline did not become ready before the timeout; no workload was released')


def journal(control, filename, row):
    directory = ROOT / 'results' / control['run_id']
    directory.mkdir(parents=True, exist_ok=True)
    with (directory / filename).open('a') as stream:
        stream.write(json.dumps(row, allow_nan=False) + '\n')
        stream.flush()
        os.fsync(stream.fileno())


def resource_snapshot(control):
    # One API read every few seconds, on the coordinator rather than the processing path.
    sys.path.insert(0, str(ROOT / 'python-scripts'))
    from analyze_execution import pod_observation
    started = time.time()
    try:
        result = json.loads(kubectl('get', 'pods', '-l', 'app=consumer-sts', '-o', 'json', timeout=10))
        row = dict(valid=True, timestamp=time.time(), request_started_epoch=started,
                   pods=[pod_observation(p) for p in result['items']])
    except Exception as exc:
        row = dict(valid=False, timestamp=time.time(), request_started_epoch=started, error=str(exc), pods=[])
        print('[RESOURCES] Snapshot unavailable:', exc, flush=True)
    journal(control, 'resource-history.jsonl', row)
    return row


def verify_placement(control, stage):
    reference = control.get('placement_reference')
    capture_pods = control.get('capture_placement_pods')
    if reference is None and capture_pods is None:
        return
    started = time.monotonic()
    record = dict(stage=stage, request_started_epoch=time.time(), valid=False,
                  reference_sha256=reference_hash(reference) if reference is not None else None,
                  reference_capture=reference is None)
    try:
        if capture_pods is not None and (stage != 'before_production' or
                not control.get('preparation_only') or 'start_epoch' in control):
            raise PlacementMismatch('Reference capture cannot release production or run before an action')
        if reference is not None and record['reference_sha256'] != control['placement_reference_sha256']:
            raise PlacementMismatch('Frozen placement reference hash changed')
        expected_pods = reference['pods'] if reference is not None else capture_pods
        rows = {p['pod']: status(p['role'], p['pod']) for p in expected_pods}
        record['application_status'] = rows
        items = json.loads(kubectl('get', 'pods', '-l', 'app in (producer-sts,consumer-sts)',
                                  '-o', 'json', timeout=10))['items']
        record['observed_pods'] = [pod_identity(item) for item in items]
        if reference is None:
            reference = capture_reference(capture_pods, control['config'], control['run_id'],
                                          control['config_sha256'], rows, items)
            record['reference_sha256'] = reference_hash(reference)
        processes = check_placement(reference, control['config'], control['run_id'],
                                    control['config_sha256'], rows, items,
                                    expected_processes=control.get('initial_placement_processes'),
                                    preparing=stage == 'before_production')
        elapsed = time.monotonic() - started
        if elapsed > 30:
            raise PlacementMismatch('Placement observation exceeded the 30-second freshness budget')
        record.update(valid=True, processes=processes, observation_seconds=elapsed)
        if record['reference_capture']:
            control['placement_reference'] = reference
            control['placement_reference_sha256'] = record['reference_sha256']
            control['reference_captured_epoch'] = time.time()
        if stage == 'before_production':
            control['initial_placement_processes'] = processes
    except Exception as exc:
        record['error'] = str(exc)
        raise
    finally:
        record['timestamp'] = time.time()
        journal(control, 'placement-checks.jsonl', record)


def apply_intervention(control):
    plan = control['intervention']
    current = read_control()
    if not current or current['run_id'] != control['run_id'] or current.get('state') != 'running':
        raise RuntimeError('Shared run changed before the intervention; refusing to act')
    verify_placement(control, 'before_intervention')
    current = read_control()
    if not current or current['run_id'] != control['run_id'] or current.get('state') != 'running':
        raise RuntimeError('Shared run changed during placement verification; refusing to act')
    now = time.time()
    if now >= control['producer_end_epoch']:
        raise RuntimeError('Intervention time was missed; refusing to act after evaluation ended')
    journal(control, 'intervention-events.jsonl', dict(event='decision', timestamp=now,
        scheduled_epoch=plan['at_epoch'], scheduling_delay_seconds=now-plan['at_epoch'], action=plan['action']))
    if plan['action'] == 'scale':
        kubectl('scale', 'statefulset', 'consumer-sts', '--current-replicas=' + str(plan['initial_consumers']),
                '--replicas=' + str(plan['target_consumers']))
    if plan['action'] == 'redistribute':
        from explicit_control import transfer
        transfer(sys.modules[__name__], control, plan['target_assignment'])
    journal(control, 'intervention-events.jsonl', dict(event='request_completed', timestamp=time.time(), action=plan['action']))


def validate_replica_control(config, plan=None, hpas=None):
    # A separate HPA can undo a scheduled replica count between readiness checks.
    if hpas is None:
        hpas = json.loads(kubectl('get', 'hpa', '-o', 'json'))['items']
    initial = ((plan or {}).get('initial_consumers') or int(config.get('CONSUMER_POD_COUNT', 0)))
    target = (plan or {}).get('target_consumers')
    requested = {'consumer-sts': [n for n in (initial, target) if n],
                 'producer-sts': [int(config['PRODUCER_POD_COUNT'])] if 'PRODUCER_POD_COUNT' in config else []}
    records = []
    for item in hpas:
        spec = item['spec']
        reference = spec['scaleTargetRef']
        if reference.get('kind') != 'StatefulSet' or reference.get('name') not in requested:
            continue
        name = item['metadata']['name']
        behavior = spec.get('behavior', {})
        paused = all(behavior.get(direction, {}).get('selectPolicy') == 'Disabled'
                     for direction in ('scaleUp', 'scaleDown'))
        if not paused:
            raise RuntimeError('HPA ' + name + ' can change experiment replicas. Pause it for fixed/scheduled trials, preserve its settings, then rerun preflight.')
        counts = requested[reference['name']]
        if any(n < spec.get('minReplicas', 1) or n > spec['maxReplicas'] for n in counts):
            raise RuntimeError('HPA ' + name + ' bounds conflict with the requested replica counts, even with scaling policies disabled.')
        records.append(dict(name=name, uid=item['metadata'].get('uid'), spec=spec))
    return records


def validate_initial_replicas(config, producers, consumers, plan=None):
    expected_producers = int(config.get('PRODUCER_POD_COUNT', len(producers)))
    expected_consumers = ((plan or {}).get('initial_consumers') or int(config.get('CONSUMER_POD_COUNT', len(consumers))))
    if len(producers) != expected_producers or len(consumers) != expected_consumers:
        raise RuntimeError(f'Initial replica mismatch: expected {expected_producers} producers and {expected_consumers} consumers; found {len(producers)} and {len(consumers)}. No workload was released.')


def start(plan=None):
    preparation_started = time.monotonic()
    # Validate a requested pilot before stopping existing applications or changing replica counts.
    validate_intervention(plan, read_config()[1])
    validate_replica_control(read_config()[1], plan)
    stop()
    initial = (plan or {}).get('initial_consumers') or read_config()[1].get('CONSUMER_POD_COUNT')
    if initial is not None:
        set_consumer_baseline(int(initial), float(read_config()[1].get('READINESS_TIMEOUT_SECONDS', 180)))
        preflight()  # Include newly created baseline replicas in source/dependency checks.
    consumer_pods, producer_pods = pods('consumer'), pods('producer')
    validate_initial_replicas(read_config()[1], producer_pods, consumer_pods, plan)
    if not consumer_pods or not producer_pods:
        raise RuntimeError('Need at least one consumer and producer pod')
    validate_config(read_config()[1])
    if plan and plan['action'] == 'scale':
        for pod in consumer_pods:
            supervised = remote('consumer', pod, "from pathlib import Path; print(b'supervise.py' in Path('/proc/1/cmdline').read_bytes())").decode().strip()
            if supervised != 'True':
                raise RuntimeError('Apply the documented consumer supervisor setup before a scale-up pilot')
    # Topic creation updates RUN_ID and TOPIC_TITLE in our one shared YAML.
    kubectl('exec', consumer_pods[0], '-c', 'consumer-container', '--', 'bash',
            '/app/consumer-merge-data/create_topics.sh', timeout=120)
    raw, config = read_config()
    duration, drain, warmup, readiness_timeout = validate_config(config)
    run_id = str(config['RUN_ID'])
    frozen = f'/config/runs/{run_id}/pipeline-configmap.yaml'
    shared_write(frozen, raw)
    control = dict(run_id=run_id, state='preparing', config_path=frozen,
                   lag_freshness_clock='monotonic_scrape_v2',
                   preparing_epoch=time.time(),
                   config_sha256=hashlib.sha256(raw).hexdigest(), drain_seconds=drain,
                   duration_seconds=duration, warmup_seconds=warmup,
                   producer_pods=producer_pods, consumer_pods=consumer_pods,
                   expected_partitions=int(config['NUM_PARTITIONS']) * int(config['TOPIC_COUNT']),
                   expires_epoch=time.time() + readiness_timeout + 30, config=config)
    if (plan or {}).get('placement_reference') is not None:
        control['placement_reference'] = plan['placement_reference']
        control['placement_reference_sha256'] = reference_hash(plan['placement_reference'])
    if (plan or {}).get('capture_placement_pods') is not None:
        control['capture_placement_pods'] = plan['capture_placement_pods']
    if (plan or {}).get('prepare_only'):
        control['preparation_only'] = True
    control['capacity_estimate'] = estimate_capacity(config, len(producer_pods), len(consumer_pods))
    print('[CAPACITY]', json.dumps(control['capacity_estimate']))
    publish(control)
    try:
        initial_resources = resource_snapshot(control)
        control['initial_consumer_resources'] = [
            {k: p.get(k) for k in ('pod', 'node', 'cpu_request_cores', 'memory_request_gib')}
            for p in sorted(initial_resources['pods'], key=lambda p: p['pod'])] if initial_resources['valid'] else None
        last_resource_sample = time.monotonic()
        for role, names in [('consumer', consumer_pods), ('producer', producer_pods)]:
            for pod in names:
                start_app(role, pod)
        expected = [(f"{config['TOPIC_TITLE']}_{t}", p) for t in range(int(config['TOPIC_COUNT'])) for p in range(int(config['NUM_PARTITIONS']))]
        deadline = time.monotonic() + readiness_timeout
        stable = 0
        previous_assignment = None
        while time.monotonic() < deadline:
            if time.monotonic() - last_resource_sample >= 5:
                resource_snapshot(control)
                last_resource_sample = time.monotonic()
            consumers = [status('consumer', p) for p in consumer_pods]
            producers = [status('producer', p) for p in producer_pods]
            for pod, row in zip(consumer_pods + producer_pods, consumers + producers):
                if row.get('run_id') == run_id and (row.get('failure') or row.get('phase') == 'failed'):
                    raise RuntimeError(pod + ': readiness failed: ' + str(row.get('failure')))
            ready = validate_readiness(consumers, expected, control['config_sha256'])
            ready = ready and all(r.get('run_id') == run_id and r.get('phase') == 'ready' and
                                  r.get('config_sha256') == control['config_sha256'] for r in producers)
            assignment = json.dumps([(r.get('incarnation'), r.get('assignment_epoch'), r.get('assignments')) for r in consumers], sort_keys=True)
            stable = stable + 1 if ready and assignment == previous_assignment else (1 if ready else 0)
            previous_assignment = assignment
            if stable >= 3:
                break
            time.sleep(1)
        else:
            raise RuntimeError('Readiness timed out. Check assignments, shared mounts and logs; no workload was released.')
        control['clock_probes'] = clock_probes('producer', producer_pods) + clock_probes('consumer', consumer_pods)
        validate_initial_replicas(config, pods('producer'), pods('consumer'), plan)
        if pods('consumer') != consumer_pods or pods('producer') != producer_pods:
            raise RuntimeError('Initial pod membership changed during readiness. No workload was released.')
        control['replica_control'] = validate_replica_control(config, plan)
        control['initial_assignment'] = sorted([dict(pod=pod, topic=topic, partition=partition)
            for pod, row in zip(consumer_pods, consumers) for topic, partition in row.get('assignments', [])],
            key=lambda row: (row['topic'], row['partition']))
        control['monitoring_readiness'] = monitoring_readiness(control)
        verify_placement(control, 'before_production')
        control['preparation_elapsed_seconds'] = time.monotonic() - preparation_started
        budget = (plan or {}).get('preparation_budget_seconds')
        if budget is not None:
            control['preparation_budget_seconds'] = budget
            if control['preparation_elapsed_seconds'] >= budget:
                raise PlacementMismatch('Preparation budget exhausted; no workload released')
        if control.get('placement_reference') is not None:
            # Use the freshly verified map in the manifest, with this run's topic prefix.
            control['initial_assignment'] = [dict(pod=r['pod'],
                topic=f"{config['TOPIC_TITLE']}_{r['topic_index']}", partition=r['partition'])
                for r in control['placement_reference']['assignment']]
        if control.get('preparation_only'):
            control['preparation_verified_epoch'] = time.time()
            control['state'] = 'stopped'  # Producers never receive a running state.
            publish(control)
            shared_write(f'/config/runs/{run_id}/manifest.json', json.dumps(control, indent=2).encode())
            print('[PREPARED]', run_id, 'placement verified; no workload released', flush=True)
            return control
        control['state'] = 'running'
        control['start_epoch'] = time.time() + 5
        control['evaluation_start_epoch'] = control['start_epoch'] + warmup
        control['producer_end_epoch'] = control['start_epoch'] + duration
        control['drain_end_epoch'] = control['producer_end_epoch'] + drain
        control['expires_epoch'] = control['drain_end_epoch'] + 120
        if plan:
            control['intervention'] = dict(plan, at_epoch=control['evaluation_start_epoch'] + plan['after_evaluation_start_seconds'])
        publish(control)
        shared_write(f'/config/runs/{run_id}/manifest.json', json.dumps(control, indent=2).encode())
        print('[START]', run_id, 'all initial consumers assigned; shared start', control['start_epoch'])
        return control
    except BaseException:
        control['state'] = 'failed'
        publish(control)
        raise


def wait_for_end(control):
    finish = control['drain_end_epoch'] + float(control['config'].get('FINAL_SCRAPE_SECONDS', 15)) + 10
    action = None if control.get('intervention_cancelled_epoch') else control.get('intervention')
    action_done = False
    ready_recorded = set()
    next_progress = 0
    while time.time() < finish:
        if action and not action_done and time.time() >= action['at_epoch']:
            # Never repeat an action after a wait is resumed during stop/cleanup.
            events = ROOT / 'results' / control['run_id'] / 'intervention-events.jsonl'
            if not events.exists():
                apply_intervention(control)
            action_done = True
        snapshot = resource_snapshot(control)
        if action_done and snapshot['valid']:
            for pod in snapshot['pods']:
                if pod['ready'] and pod['uid'] not in ready_recorded:
                    journal(control, 'intervention-events.jsonl', dict(event='pod_ready_observed', timestamp=snapshot['timestamp'],
                        pod=pod['pod'], uid=pod['uid'], kubernetes_ready_transition=pod['ready_transition']))
                    ready_recorded.add(pod['uid'])
        if time.time() >= next_progress:
            print('[WAIT]', max(0, round(finish - time.time())), 'seconds including drain and final scrapes', flush=True)
            next_progress = time.time() + 30
        delay = min(5, max(0, finish-time.time()))
        if action and not action_done:
            delay = min(delay, max(0, action['at_epoch']-time.time()))
        time.sleep(delay)
    # A slow final record or filesystem flush can outlive the nominal bound.
    # Check actual process completion before copying files from the shared volumes.
    timeout = time.monotonic() + 120
    next_resource = 0
    while time.monotonic() < timeout:
        if time.monotonic() >= next_resource:
            resource_snapshot(control)
            next_resource = time.monotonic() + 5
        rows = [status(role, pod) for role in ROLES for pod in pods(role)]
        if rows and all(row.get('run_id') == control['run_id'] and row.get('phase') in ('finished', 'failed') for row in rows):
            resource_snapshot(control)
            return
        print('[WAIT] Waiting for final process snapshots.', flush=True)
        time.sleep(2)
    raise RuntimeError('Applications did not finish cleanup; inspect status before collecting the run')


def collect():
    control = read_control()
    if not control:
        raise RuntimeError('No managed run to collect')
    if control['state'] == 'running' and time.time() < control['drain_end_epoch']:
        raise RuntimeError('Stop/drain the run before collecting final evidence')
    directory = ROOT / 'results' / control['run_id']
    directory.mkdir(parents=True, exist_ok=True)
    (directory / 'manifest.json').write_text(json.dumps(control, indent=2))
    (directory / 'pipeline-configmap.yaml').write_bytes(shared_read(control['config_path']))
    # Evidence is on each role's shared PVC, so one copy includes terminated/scaled pods too.
    for role in ROLES:
        root = str(control['config'].get(role.upper() + '_EVIDENCE_DIR', ROLES[role][1] + '/evidence'))
        pod = pods(role)[0]
        source = root + '/' + control['run_id']
        import tempfile
        import shutil
        with tempfile.TemporaryDirectory(dir=directory) as temporary:
            staging = Path(temporary) / role
            staging.mkdir()
            # Copying every pod in one stream can fail even with cp retries.
            # Copy each saved pod directory separately, including pods that no
            # longer exist after scaling. Keep earlier local evidence until every
            # directory has arrived successfully.
            names = json.loads(remote(role, pod,
                'from pathlib import Path; import json; '
                f'print(json.dumps(sorted(p.name for p in Path({source!r}).iterdir())))'))
            if not isinstance(names, list) or any(not isinstance(name, str) or not name or
                    name in ('.', '..') or Path(name).name != name for name in names):
                raise RuntimeError('Invalid evidence directory listing for ' + role)
            for name in names:
                destination = staging / name
                for attempt in range(1, 4):
                    print('[COLLECT] Copying', role, name, 'from', pod, 'attempt', attempt, flush=True)
                    try:
                        kubectl('cp', pod + ':' + source + '/' + name, str(destination), '-c', ROLES[role][0],
                                '--retries=3', timeout=900, request_timeout='0')
                        break
                    except subprocess.CalledProcessError:
                        # kubectl's own resume option does not handle every
                        # unexpected-EOF error from the API error stream.
                        if attempt == 3:
                            raise
                        if destination.is_dir():
                            shutil.rmtree(destination)
                        elif destination.exists():
                            destination.unlink()
            if (directory / role).exists():
                shutil.rmtree(directory / role)
            staging.rename(directory / role)
    print('[COLLECT]', directory)
    return directory


def add_lag_sample_timestamps(selector, run_id):
    ages = 'consumer_lag_observation_age_seconds{run_id=' + json.dumps(run_id) + '}'
    # Default set matching ignores the metric name and would discard the added
    # series because the raw ages have the same labels. This reserved new name
    # must be a separate member of the union, with all original labels retained.
    return selector + ' or on(__name__) label_replace(timestamp(' + ages + '), "__name__", "consumer_lag_scrape_timestamp_seconds", "", "")'


def monitoring_readiness(control, timeout=30):
    if not os.getenv('PROM_URL'):
        return dict(status='not_checked', reason='Manual start without a Prometheus connection')
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]/'python-scripts'))
    from lag_freshness import observation_validity
    config=control['config']
    expected={(f"{config['TOPIC_TITLE']}_{t}", str(p)) for t in range(int(config['TOPIC_COUNT']))
              for p in range(int(config['NUM_PARTITIONS']))}
    selector='{__name__=~"consumer_lag.*|consumer_partition_owned|consumer_processing_backlog|consumer_position_offset|consumer_high_offset",run_id='+json.dumps(control['run_id'])+'}'
    query=add_lag_sample_timestamps(selector, control['run_id'])
    deadline=time.monotonic()+timeout
    error='No complete observation'
    while time.monotonic()<deadline:
        timestamp=time.time()-1
        try:
            params=urllib.parse.urlencode(dict(query=query,time=timestamp))
            with urllib.request.urlopen(os.environ['PROM_URL'].rstrip('/')+'/api/v1/query?'+params, timeout=5) as response:
                data=json.load(response)
            if data.get('status')!='success': raise ValueError('Prometheus query failed')
            entries={}
            for series in data['data']['result']:
                m=series['metric']
                if 'partition' not in m: continue
                key=(m.get('topic'),m['partition'],m.get('pod'),m.get('incarnation'))
                values=entries.setdefault(key,{})
                if m['__name__'] in values: raise ValueError('Duplicate partition metric')
                values[m['__name__']]=float(series['value'][1])
            selected={};duplicate=False
            for (topic,partition,pod,incarnation),values in entries.items():
                if values.get('consumer_partition_owned')!=1: continue
                duplicate=duplicate or (topic,partition) in selected
                selected[(topic,partition)]=values
            observed=uninitialized=0
            freshness=float(config.get('LAG_FRESHNESS_SECONDS',10))
            for values in selected.values():
                if observation_validity(values,timestamp,freshness)[0]:
                    observed+=1
                    continue
                # On a newly created empty topic, Kafka may not yet return a
                # position. Verify the exporter schema and scrape freshness;
                # never invent a zero lag or call the missing position valid.
                sample_age=timestamp-values.get('consumer_lag_scrape_timestamp_seconds',math.nan)
                unknown=(values.get('consumer_lag_valid')==0 and
                         all(math.isnan(values.get(name,math.inf)) for name in
                             ('consumer_lag','consumer_processing_backlog','consumer_lag_observation_age_seconds')) and
                         'consumer_position_offset' not in values and 'consumer_high_offset' not in values)
                if unknown and math.isfinite(sample_age) and 0<=sample_age<=freshness:
                    uninitialized+=1
            valid=not duplicate and set(selected)==expected and observed+uninitialized==len(expected)
            if valid:
                return dict(status='exporter_schema_ready',query=query,query_timestamp=timestamp,
                            owned_partitions=len(selected),valid_partition_observations=observed,
                            unresolved_empty_topic_positions=uninitialized,freshness_clock='monotonic_scrape_v2',
                            note='The full offset freshness rule applies during production; unresolved positions remain invalid, never zero.')
            error='Missing, duplicated, invalid or stale partition observations'
        except Exception as exc:
            error=str(exc)
        time.sleep(1)
    raise RuntimeError('Prometheus measurement readiness failed before production: '+error)


def export_metrics(directory, control):
    # Export raw series with all labels; dashboard layout never determines research data.
    start = control.get('start_epoch')
    if not start:
        raise ValueError('This run never passed readiness')
    end = min(time.time(), control['drain_end_epoch'] + float(control['config'].get('FINAL_SCRAPE_SECONDS', 15)))
    selector = '{__name__=~"consumer_.*|producer_.*",run_id=' + json.dumps(control['run_id']) + '}'
    # query_range timestamps are evaluation times, not the original scrape times.
    # Preserve the latter as a named series alongside the exporter-local ages.
    selector = add_lag_sample_timestamps(selector, control['run_id'])
    for role in ('producer', 'consumer'):
        for suffix in ('cpu_percent', 'memory_bytes'):
            name = role + '_' + suffix
            resource_selector = name + '{run_id=' + json.dumps(control['run_id']) + '}'
            selector += ' or on(__name__) label_replace(timestamp(' + resource_selector + '), "__name__", "' + name + '_scrape_timestamp_seconds", "", "")'
    (directory / 'prometheus-query.json').write_text(json.dumps(dict(query=selector, start=start, end=end, step=2), indent=2))
    params = urllib.parse.urlencode(dict(query=selector, start=start, end=end, step=2))
    url = os.getenv('PROM_URL', 'http://localhost:9090').rstrip('/') + '/api/v1/query_range?' + params
    with urllib.request.urlopen(url, timeout=120) as response:
        result = json.load(response)
    if result.get('status') != 'success' or not result.get('data', {}).get('result'):
        raise RuntimeError('Prometheus export failed or returned no series')
    (directory / 'prometheus.json').write_text(json.dumps(result))
    # Keep CSV convenient for plotting, retaining the complete label set as JSON.
    import csv
    with (directory / 'prometheus.csv').open('w', newline='') as stream:
        writer = csv.writer(stream)
        writer.writerow(['timestamp_epoch', 'labels_json', 'value'])
        for series in result['data']['result']:
            labels = json.dumps(series['metric'], sort_keys=True)
            writer.writerows((t, labels, v) for t, v in series['values'])
    actual = {s['metric'].get('incarnation') for s in result['data']['result']}
    expected = {json.loads(p.read_text())['incarnation'] for p in directory.rglob('final.json')}
    missing = sorted(expected - actual)
    (directory / 'export-status.json').write_text(json.dumps({'status': 'incomplete' if missing else 'complete',
                                                             'missing_incarnations': missing,
                                                             'series_count': len(result['data']['result'])}))
    if missing:
        raise RuntimeError('Prometheus did not observe every process incarnation: ' + ', '.join(missing))
    print('[EXPORT]', directory / 'prometheus.csv')


def backup_code():
    directory = ROOT / 'backups' / time.strftime('nautilus-%Y%m%d-%H%M%S')
    directory.mkdir(parents=True)
    for role in ROLES:
        # Never overwrite edited local source with a backup from the cluster.
        target = directory / role
        target.mkdir()
        for name in (role + '.py', 'run.sh', 'pipeline_runtime.py', 'launch.py'):
            try:
                kubectl('cp', pods(role)[0] + ':' + ROLES[role][1] + '/' + name, str(target / name), '-c', ROLES[role][0])
            except subprocess.CalledProcessError as exc:
                print('[BACKUP]', name, exc.stderr.decode(), file=sys.stderr)
    (directory / 'pipeline-configmap.yaml').write_bytes(shared_read(CONFIG))
    print('[BACKUP]', directory)


def analysis_tools():
    # Use the same analyzers as manual analysis, so there is only one set of formulas.
    sys.path.insert(0, str(ROOT / 'python-scripts'))
    from evaluate_run import evaluate
    from analyze_lag import analyze
    from summarize_runs import summarize
    return evaluate, analyze, summarize


def preflight():
    analysis_tools()  # Check local analysis imports before changing a run.
    context = kubectl('config', 'current-context').decode().strip()
    print('[CONTEXT]', context, 'namespace:', NS, flush=True)
    for role in ROLES:
        names = pods(role)
        if not names:
            raise RuntimeError('Need at least one ' + role + ' pod')
        sources = [ROOT / 'src' / role / (role + '.py'), ROOT / 'src' / role / 'run.sh',
                   ROOT / 'src/common/launch.py', ROOT / 'src/common/pipeline_runtime.py']
        if role == 'consumer':
            sources += [ROOT / 'src/common/explicit_assignment.py', ROOT / 'src/consumer/supervise.py', ROOT / 'src/consumer/create_topics.sh']
        expected = {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in sources}
        for pod in names:
            code = f'''import confluent_kafka,prometheus_client,psutil,yaml,hashlib,json
from pathlib import Path
root=Path({ROLES[role][1]!r})
print(json.dumps({{name:hashlib.sha256((root/name).read_bytes()).hexdigest() if (root/name).is_file() else None for name in {list(expected)!r}}}))
'''
            actual = json.loads(remote(role, pod, code))
            if actual != expected:
                raise RuntimeError(pod + ': shared source differs from the local code. Between runs, use '
                                   'save-run.sh stop and sync-code.sh; complete the one-time setup first.')
        print('[CHECK]', len(names), role, 'pods: imports and shared source match', flush=True)
    config = read_config()[1]
    validate_config(config)
    validate_replica_control(config)
    print('[CONFIG] Rate per producer:', config['TARGET_RATE'], 'production seconds:',
          config.get('EXP_DURATION_SEC', 300), 'drain seconds:', config.get('DRAIN_SECONDS', 60), flush=True)


def prometheus_ready(url):
    try:
        query = urllib.parse.urlencode({'query': 'vector(1)'})
        with urllib.request.urlopen(url.rstrip('/') + '/api/v1/query?' + query, timeout=3) as response:
            result = json.load(response)
        return result.get('status') == 'success' and bool(result.get('data', {}).get('result'))
    except (OSError, ValueError):
        return False


@contextmanager
def prometheus_connection():
    configured = os.getenv('PROM_URL')
    if configured:
        if not prometheus_ready(configured):
            raise RuntimeError('PROM_URL is not reachable as a Prometheus API; no new experiment was started.')
        print('[PROMETHEUS] Using the configured PROM_URL', flush=True)
        yield
        return
    # Ask kubectl for an available local port; leave any existing forwards alone.
    service = os.getenv('PROM_SERVICE', 'prometheus-svc')
    port = os.getenv('PROM_SERVICE_PORT', '9090')
    namespace = os.getenv('PROM_NAMESPACE', NS)
    with tempfile.TemporaryFile(mode='w+b') as log:
        process = subprocess.Popen(['kubectl', '-n', namespace, 'port-forward', '--address=127.0.0.1',
                                    'svc/' + service, ':' + port], stdout=log, stderr=log,
                                   start_new_session=True)
        try:
            deadline = time.monotonic() + 30
            output = ''
            while time.monotonic() < deadline:
                output = os.pread(log.fileno(), 65536, 0).decode(errors='replace')
                match = re.search(r'Forwarding from 127\.0\.0\.1:(\d+)\s+->', output)
                if process.poll() is not None:
                    break
                if match:
                    url = 'http://127.0.0.1:' + match.group(1)
                    if prometheus_ready(url):
                        os.environ['PROM_URL'] = url
                        print('[PROMETHEUS] Temporary connection to', service, 'at', url, flush=True)
                        yield
                        return
                time.sleep(.25)
            raise RuntimeError('Could not connect to Prometheus before starting the experiment. '
                               'Check PROM_NAMESPACE / PROM_SERVICE / PROM_SERVICE_PORT or set PROM_URL.\n' + output)
        finally:
            os.environ.pop('PROM_URL', None)
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()


@contextmanager
def command_lock():
    directory = ROOT / 'results'
    directory.mkdir(exist_ok=True)
    with (directory / '.experiment.lock').open('a') as stream:
        try:
            fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise RuntimeError('Another complete-run command is already using this local code folder.')
        try:
            yield
        finally:
            fcntl.flock(stream, fcntl.LOCK_UN)


def analyze_collected(directory, control):
    evaluate, analyze, _ = analysis_tools()
    issues = []
    lag = None
    try:
        export_metrics(directory, control)
    except Exception as exc:
        issues.append('Prometheus export: ' + str(exc))
    # Persistent identity evidence remains useful even when Prometheus export fails.
    try:
        outcomes = evaluate(directory)
        (directory / 'outcome-summary.json').write_text(json.dumps(outcomes, indent=2) + '\n')
        issues.extend(outcomes['validity_failures'])
    except Exception as exc:
        issues.append('Outcome analysis: ' + str(exc))
    if (directory / 'prometheus.json').exists():
        try:
            lag = analyze(directory)
            (directory / 'lag-summary.json').write_text(json.dumps(lag, indent=2, allow_nan=False) + '\n')
            print('[LAG] Covered evaluation fraction:', lag['covered_fraction'])
        except Exception as exc:
            issues.append('Lag analysis: ' + str(exc))
    try:
        from analyze_execution import analyze as execution_analysis
        execution = execution_analysis(directory, lag)
        (directory / 'execution-summary.json').write_text(json.dumps(execution, indent=2, allow_nan=False) + '\n')
    except Exception as exc:
        issues.append('Execution analysis: ' + str(exc))
    try:
        from audit_measurements import analyze as measurement_analysis
        measurement = measurement_analysis(directory)
        (directory / 'measurement-audit.json').write_text(json.dumps(measurement, indent=2, allow_nan=False) + '\n')
        issues.extend(measurement['issues'])
    except Exception as exc:
        issues.append('Measurement audit: ' + str(exc))
    result = dict(run_id=control['run_id'], status='failed' if issues else 'complete', issues=issues)
    (directory / 'runner-status.json').write_text(json.dumps(result, indent=2) + '\n')
    print('[RESULTS]', directory.resolve(), flush=True)
    if issues:
        raise RuntimeError('Run files were saved in ' + str(directory.resolve()) + ', but checks failed: ' + '; '.join(issues))
    return directory


def complete_run(plan=None):
    previous = read_control()
    previous_id = previous.get('run_id') if previous else None
    control = None
    try:
        control = start(plan) if plan else start()
        if control.get('preparation_only'):
            stop()
            directory = collect()
            result = dict(run_id=control['run_id'], status='preparation_verified',
                          workload_released=False, issues=[])
            (directory / 'runner-status.json').write_text(json.dumps(result, indent=2) + '\n')
            print('[PREPARATION RESULTS]', directory.resolve(), flush=True)
            return directory
        wait_for_end(control)
    except BaseException:
        # On Ctrl+C or startup/wait failure, stop only the run this invocation created.
        try:
            current = read_control()
            owned = current and (current['run_id'] == control['run_id'] if control else current['run_id'] != previous_id)
            if owned:
                print('[STOP] Ending this run and preserving available evidence; allow time for drain.', flush=True)
                stop()
                current = read_control()
                directory = collect()
                if 'evaluation_start_epoch' in current:
                    analyze_collected(directory, current)
                (directory / 'runner-status.json').write_text(json.dumps(dict(
                    run_id=current['run_id'], status='interrupted_or_failed',
                    issues=['The command did not complete its scheduled run.']), indent=2) + '\n')
        except Exception as cleanup_error:
            print('[CLEANUP]', cleanup_error, file=sys.stderr)
        raise
    current = read_control()
    if not current or current['run_id'] != control['run_id']:
        raise RuntimeError('Shared run changed while waiting; refusing to collect another run.')
    return analyze_collected(collect(), current)


def run_complete_commands(repetitions=1, rates=None, batch=False, plan=None):
    if repetitions < 1 or (rates is not None and (not rates or min(rates) <= 0)):
        raise ValueError('Use positive repetition counts and rates')
    if (plan or {}).get('prepare_only') and (batch or repetitions != 1 or rates is not None):
        raise ValueError('Preparation-only validation is one preparation, not an experiment batch')
    with command_lock(), paused_for_experiment(sys.modules[__name__], plan):
        preflight()
        if plan:
            validate_intervention(plan, read_config()[1])
        with prometheus_connection():
            directories = []
            batch_dir = None
            record = dict(status='running', requested_repetitions=repetitions, rates=rates, intervention=plan, runs=[])
            if batch:
                batch_dir = ROOT / 'results/batches' / ('batch-' + time.strftime('%Y%m%d-%H%M%S') + '-' + uuid.uuid4().hex[:8])
                batch_dir.mkdir(parents=True)
                print('[BATCH]', batch_dir.resolve(), flush=True)
            try:
                for rate in rates if rates is not None else [None]:
                    for repetition in range(repetitions):
                        if directories:
                            preflight()
                        if not prometheus_ready(os.environ['PROM_URL']):
                            raise RuntimeError('Prometheus connection was lost; no next run was started.')
                        if rate is not None:
                            stop()
                            # Only an explicit --rates option changes the shared target rate.
                            code = "import yaml,sys; d=yaml.safe_load(sys.stdin.buffer.read()); d['data']['TARGET_RATE']=" + repr(str(rate)) + "; print(yaml.safe_dump(d,sort_keys=False))"
                            updated = remote('consumer', pods('consumer')[0], code, shared_read(CONFIG))
                            shared_write(CONFIG, updated)
                        print('[RUN]', repetition + 1, 'of', repetitions, 'rate:', rate if rate is not None else 'shared configuration', flush=True)
                        if batch_dir:
                            record.update(current_repetition=repetition + 1, current_rate=rate)
                            (batch_dir / 'batch-status.json').write_text(json.dumps(record, indent=2) + '\n')
                        directory = complete_run(plan) if plan else complete_run()
                        directories.append(directory)
                        record['runs'].append(str(directory.resolve()))
                if batch_dir:
                    summary = analysis_tools()[2](directories)
                    (batch_dir / 'repetition-summary.json').write_text(json.dumps(summary, indent=2, allow_nan=False) + '\n')
                    if any(group['excluded_runs'] for group in summary['groups']):
                        raise RuntimeError('Batch summary detected invalid evidence; inspect repetition-summary.json')
                    print('[SUMMARY]', batch_dir / 'repetition-summary.json', flush=True)
                record['status'] = 'complete'
            except BaseException as exc:
                record.update(status='interrupted' if isinstance(exc, KeyboardInterrupt) else 'failed', error=str(exc) or type(exc).__name__)
                raise
            finally:
                if batch_dir:
                    (batch_dir / 'batch-status.json').write_text(json.dumps(record, indent=2) + '\n')


def main():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""Actions:
  run      Run once, wait through drain, export and analyze (default).
  repeat   Run --repetitions times; optionally sweep --rates; summarize the batch.
  start    Prepare and start only; return before the experiment finishes.
  status   Print per-pod run status, configuration hashes and assignments.
  stop     End production and allow the bounded consumer drain and cleanup.
  collect  Copy current-run evidence to results/RUN_ID without Prometheus.
  export   Collect evidence and export run-scoped Prometheus JSON/CSV.
  backup   Download current pod source/config into backups/.
  sweep    Alias for repeat with --rates (kept for existing commands).
  interactive  Use the previous interactive troubleshooting prompts.

Examples:
  bash my-shell/save-run.sh
  bash my-shell/run_pipeline.sh --repetitions 5
  bash my-shell/run_pipeline.sh --rates 1000 2000 --repetitions 5
  bash my-shell/save-run.sh start
  bash my-shell/save-run.sh status
  bash my-shell/save-run.sh stop
  bash my-shell/save-run.sh export
  python3 python-scripts/evaluate_run.py results/RUN_ID

Environment: NAMESPACE (default kafkastreamingdata),
             PROM_URL (optional existing Prometheus API URL).
Complete runs otherwise open their own temporary port-forward using
PROM_NAMESPACE (default NAMESPACE), PROM_SERVICE (prometheus-svc),
PROM_SERVICE_PORT (9090). Manual export still defaults to localhost:9090.
Source sync: bash my-shell/sync-code.sh (after stop).
Detailed help: docs/runtime-and-data-flow.md
Complete-run commands create fresh topics and preserve old topics/results.
One-time code/configuration/monitoring setup must already be complete.
""")
    parser.add_argument('action', choices=['run', 'repeat', 'interactive', 'start', 'stop', 'collect', 'export', 'status', 'backup', 'sweep'], nargs='?', default='run')
    parser.add_argument('--rates', type=int, nargs='+')
    parser.add_argument('--repetitions', type=int, default=1)
    parser.add_argument('--intervention', choices=['none', 'scale', 'redistribute'], help='Optional scheduled pilot action; not an automatic selector')
    parser.add_argument('--target-assignment', type=Path, help='Complete JSON ownership list for the experimental explicit pilot')
    parser.add_argument('--intervention-after', type=float, help='Seconds after evaluation starts; also use for the no-action baseline')
    parser.add_argument('--initial-consumers', type=int, help='Explicitly restore this replica baseline before EACH pilot run')
    parser.add_argument('--target-consumers', type=int, help='Consumer count after the scale-up action')
    parser.add_argument('--recovery-threshold', type=float, help='Calibrated total processing-backlog threshold in offsets')
    parser.add_argument('--recovery-hold', type=float, help='Seconds backlog must remain below the threshold')
    parser.add_argument('--placement-reference', type=Path,
                        help='Frozen JSON assignment/pod reference, checked before production and intervention')
    parser.add_argument('--prepare-only', action='store_true',
                        help='Check one referenced preparation and collect it without releasing a workload')
    parser.add_argument('--preparation-budget', type=float,
                        help='Maximum seconds to prepare a controlled run before releasing production')
    args = parser.parse_args()
    if (args.intervention == 'redistribute') != (args.target_assignment is not None):
        parser.error('Redistribute requires --target-assignment; other actions must omit it')
    plan_values = (args.intervention_after, args.initial_consumers, args.target_consumers, args.recovery_threshold, args.recovery_hold)
    plan = None
    if args.intervention:
        if args.action not in ('run', 'repeat', 'sweep') or args.intervention_after is None:
            parser.error('Scheduled pilots require a complete run/repeat and --intervention-after')
        plan = dict(action=args.intervention, after_evaluation_start_seconds=args.intervention_after,
                    initial_consumers=args.initial_consumers, target_consumers=args.target_consumers,
                    recovery_threshold_offsets=args.recovery_threshold, recovery_hold_seconds=args.recovery_hold)
        if args.target_assignment is not None:
            plan['target_assignment'] = json.loads(args.target_assignment.read_text())
    elif any(value is not None for value in plan_values):
        parser.error('Pilot settings require --intervention')
    if args.placement_reference is not None or args.prepare_only or args.preparation_budget is not None:
        if plan is None or (args.prepare_only and args.action != 'run'):
            parser.error('Placement checks need a scheduled action; --prepare-only is for a single run command')
        if args.placement_reference is None:
            parser.error('--prepare-only requires --placement-reference')
        reference = json.loads(args.placement_reference.read_text())
        validate_reference(reference)
        plan.update(placement_reference=reference, prepare_only=args.prepare_only,
                    preparation_budget_seconds=args.preparation_budget)
    if args.repetitions < 1 or (args.rates is not None and min(args.rates) <= 0):
        parser.error('Use positive repetition counts and rates')
    if args.action not in ('repeat', 'sweep') and (args.rates is not None or args.repetitions != 1):
        parser.error('Use run_pipeline.sh for --rates or --repetitions')
    if args.action == 'run':
        run_complete_commands(plan=plan)
    elif args.action in ('repeat', 'sweep'):
        if args.action == 'sweep' and not args.rates:
            parser.error('sweep requires --rates; use repeat for the current shared rate')
        run_complete_commands(args.repetitions, args.rates, batch=True, plan=plan)
    elif args.action == 'interactive':
        if input('Back up the code currently on Nautilus? [y/N] ').lower() == 'y':
            backup_code()
        stop()
        if read_control() and input('Collect the previous run before starting another? [Y/n] ').lower() != 'n':
            collect()
        if input('Start a new run using the shared configuration? [Y/n] ').lower() != 'n':
            control = start()
            if input('Wait, collect evidence and export Prometheus after the run? [Y/n] ').lower() != 'n':
                wait_for_end(control)
                export_metrics(collect(), read_control())
    elif args.action == 'start':
        start()
    elif args.action == 'stop':
        stop()
    elif args.action == 'collect':
        collect()
    elif args.action == 'export':
        control = read_control()
        if not control:
            raise ValueError('No run manifest')
        if time.time() < control.get('drain_end_epoch', float('inf')):
            raise ValueError('Wait for the drain period or stop the run first')
        export_metrics(collect(), control)
    elif args.action == 'status':
        print(json.dumps({r: {p: status(r, p) for p in pods(r)} for r in ROLES}, indent=2))
    else:
        backup_code()


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('[INTERRUPTED] Inspect the saved status before reusing this run.', file=sys.stderr)
        raise SystemExit(130)
    except Exception as exc:
        print('[ERROR]', exc, file=sys.stderr)
        if isinstance(exc, subprocess.CalledProcessError) and exc.stderr:
            print(exc.stderr.decode(errors='replace') if isinstance(exc.stderr, bytes) else exc.stderr, file=sys.stderr)
        raise SystemExit(1)
