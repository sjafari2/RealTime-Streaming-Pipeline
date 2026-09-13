"""Validate fresh static-member starts before admitting a controlled comparison."""
import copy
import json
import time

from placement_control import PlacementMismatch, pod_identity, reference_hash


def stages(include_comparison=False):
    sequence = [('capture_three', 3, True, True, 'none'),
                ('restart_three', 3, True, False, 'none'),
                ('prepare_six', 6, True, True, 'none'),
                ('return_to_three', 3, True, False, 'none')]
    if include_comparison:
        sequence += [('scale_trial', 3, False, False, 'scale'),
                     ('keep_trial', 3, False, False, 'none')]
    return sequence


def wait_for_empty_group(runner, config, observe, timeout=60):
    """Do not start while an old static member still holds group membership."""
    group = config['CONSUMER_GROUP_ID']
    code = f'BOOTSTRAP={config["BOOTSTRAP_SERVERS"]!r}\nGROUP={group!r}\n' + '''import json
from confluent_kafka import KafkaException, KafkaError
from confluent_kafka.admin import AdminClient
client = AdminClient({'bootstrap.servers': BOOTSTRAP})
try:
 result = client.describe_consumer_groups([GROUP], request_timeout=5)[GROUP].result(timeout=7)
 print(json.dumps({'group': GROUP, 'state': result.state.name, 'members': len(result.members)}))
except KafkaException as exc:
 if exc.args[0].code() != KafkaError.GROUP_ID_NOT_FOUND: raise
 print(json.dumps({'group': GROUP, 'state': 'DEAD', 'members': 0}))
'''
    pod = runner.pods('consumer')[0]
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        remaining = deadline - time.monotonic()
        if remaining < 10:
            break
        row = json.loads(runner.remote('consumer', pod, code, timeout=10))
        observe(dict(row, timestamp=time.time()))
        if row.get('members') == 0 and row.get('state') in ('EMPTY', 'DEAD'):
            return row
        time.sleep(min(2, max(0, deadline - time.monotonic())))
    raise PlacementMismatch('Previous consumer-group membership did not clear within the preparation budget')


def check_original_pods(original, observed):
    current = {p['pod']: p for p in observed}
    for expected in original:
        if current.get(expected['pod']) != expected:
            raise PlacementMismatch(expected['pod'] + ': original pod identity or placement changed')


def audit_preparation(directory):
    """A passed readiness check must also leave complete evidence of zero traffic."""
    manifest = json.loads((directory / 'manifest.json').read_text())
    issues, finals = [], []
    if 'start_epoch' in manifest or not manifest.get('preparation_only'):
        issues.append('Preparation has a production start or is not preparation-only')
    for path in sorted(directory.glob('*/*/*/events.jsonl')):
        final = json.loads(path.with_name('final.json').read_text())
        finals.append(final)
        if (final.get('run_id') != manifest['run_id'] or final.get('phase') != 'finished' or
                final.get('config_sha256') != manifest['config_sha256'] or final.get('failure') or
                final.get('evidence_error') or final.get('evidence_dropped', 0)):
            issues.append('Invalid final evidence: ' + str(path))
        summaries = []
        with path.open() as stream:
            for line in stream:
                event = json.loads(line)
                if event['event'] in ('acknowledged', 'completed', 'delivery_failed', 'enqueue_failed',
                                      'enqueue_cancelled', 'processing_failed'):
                    issues.append('Message activity in preparation: ' + event['event'])
                if event['event'] == 'delivery_summary':
                    summaries.append(event)
        if final['role'] == 'producer' and (len(summaries) != 1 or
                any(summaries[0].get(key) != 0 for key in ('enqueued', 'resolved', 'unresolved'))):
            issues.append('Missing zero-delivery summary: ' + final['pod'])
    expected = sorted(manifest['producer_pods'] + manifest['consumer_pods'])
    if sorted(row['pod'] for row in finals) != expected:
        issues.append('Missing or repeated process evidence')
    result = dict(run_id=manifest['run_id'], valid=not issues, issues=issues, final_records=len(finals))
    (directory / 'preparation-evidence-check.json').write_text(json.dumps(result, indent=2) + '\n')
    if issues:
        raise RuntimeError('Preparation evidence failed: ' + '; '.join(issues))
    return result


def execute_stages(runner, audit, block, config, original_pods, save, write,
                   update_configuration, capacity, include_comparison=False):
    reference = None
    total_limit = 1200
    for name, count, preparation_only, capture, action in stages(include_comparison):
        remaining = total_limit - block['preparation_seconds_used']
        if remaining <= 0:
            raise RuntimeError('Static-startup cumulative preparation budget exhausted')
        started = time.monotonic()
        attempt = dict(stage=name, action=action, initial_consumers=count,
                       preparation_only=preparation_only, capture_reference=capture,
                       started_epoch=time.time(), status='preparing')
        block['attempts'].append(attempt)
        save()
        before_runner = 0
        try:
            runner.stop()
            configuration = copy.deepcopy(config)
            configuration['data']['CONSUMER_POD_COUNT'] = str(count)
            update_configuration(configuration)
            capacity()
            observations = []
            def observe(row):
                observations.append(row)
                write(audit / (name + '-group-clearance.json'), observations)
            wait_for_empty_group(runner, configuration['data'], observe,
                                timeout=min(60, remaining - (time.monotonic() - started)))
            runner.set_consumer_baseline(count, min(180, max(0, remaining - (time.monotonic() - started))))
            observed = [pod_identity(item) for item in json.loads(runner.kubectl(
                'get', 'pods', '-l', 'app in (producer-sts,consumer-sts)', '-o', 'json'))['items']]
            check_original_pods(original_pods, observed)
            plan = dict(action=action, initial_consumers=count,
                        target_consumers=6 if action == 'scale' else None,
                        after_evaluation_start_seconds=60, recovery_threshold_offsets=1500,
                        recovery_hold_seconds=20, prepare_only=preparation_only)
            if capture:
                plan['capture_placement_pods'] = observed
            else:
                if reference is None:
                    raise RuntimeError('The three-consumer reference was not frozen')
                plan['placement_reference'] = reference
            before_runner = time.monotonic() - started
            plan['preparation_budget_seconds'] = remaining - before_runner
            if plan['preparation_budget_seconds'] <= 0:
                raise RuntimeError('Static-startup preparation budget exhausted before start')
            print('[STATIC STARTUP]', name, 'preparation only:', preparation_only, flush=True)
            directory = runner.complete_run(plan)
            manifest = json.loads((directory / 'manifest.json').read_text())
            if preparation_only:
                audit_preparation(directory)
            elapsed = (time.monotonic() - started if preparation_only
                       else before_runner + manifest['preparation_elapsed_seconds'])
            block['preparation_seconds_used'] += elapsed
            attempt.update(status='preparation_verified' if preparation_only else 'complete',
                           run_id=manifest['run_id'], directory=str(directory),
                           preparation_seconds=elapsed, finished_epoch=time.time())
            if preparation_only and 'start_epoch' in manifest:
                raise RuntimeError('Preparation unexpectedly released production')
            if name == 'capture_three':
                reference = copy.deepcopy(manifest['placement_reference'])
                block['reference_sha256'] = reference_hash(reference)
                write(audit / 'placement-reference.json', reference)
            elif name == 'prepare_six':
                write(audit / 'six-consumer-preparation-reference.json', manifest['placement_reference'])
            elif manifest.get('placement_reference_sha256') != block['reference_sha256']:
                raise RuntimeError('A later stage used a different frozen reference')
            if not preparation_only:
                block['runs'].append(dict(action=action, run_id=manifest['run_id'], directory=str(directory)))
            if name == 'return_to_three':
                block['preparation_verified'] = True
            if block['preparation_seconds_used'] > total_limit:
                raise RuntimeError('Static-startup cumulative preparation budget exhausted')
            save()
        except BaseException as exc:
            attempt.update(status='failed', error=str(exc) or type(exc).__name__)
            try:
                current = runner.read_control() or {}
            except Exception as control_error:
                current = {}
                attempt['control_read_error'] = str(control_error)
            if current.get('preparing_epoch', 0) >= attempt['started_epoch']:
                attempt['run_id'] = current.get('run_id')
            if 'preparation_seconds' not in attempt:
                elapsed = (before_runner + current['preparation_elapsed_seconds']
                           if current.get('preparing_epoch', 0) >= attempt['started_epoch'] and
                           'start_epoch' in current else time.monotonic() - started)
                block['preparation_seconds_used'] += elapsed
                attempt['preparation_seconds'] = elapsed
            save()
            raise
    block['status'] = 'complete' if include_comparison else 'preparation_verified'
    save()
