#!/usr/bin/env python3
"""Run the predeclared seed-71 matched pair, retaining every preparation attempt."""
import argparse
import copy
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import sys
import time

import yaml

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'my-shell'))
import run_experiment as r
from placement_control import PlacementMismatch, pod_identity, reference_hash, validate_reference


def write(path, value):
    temporary = path.with_suffix(path.suffix + '.tmp')
    temporary.write_text(json.dumps(value, indent=2, allow_nan=False) + '\n')
    temporary.replace(path)


def stable_config(raw):
    value = yaml.safe_load(raw)
    value = copy.deepcopy(value)
    for key in ('RUN_ID', 'TOPIC_TITLE'):
        value['data'].pop(key, None)
    return value


def capacity():
    free = {role: int(r.remote(role, r.pods(role)[0],
        f'import shutil;print(shutil.disk_usage({r.ROLES[role][1]!r}).free)').decode()) for role in r.ROLES}
    free['local'] = shutil.disk_usage(ROOT).free
    if free['producer'] < 2 * 2**30 or free['consumer'] < 5 * 2**30 or free['local'] < 10 * 2**30:
        raise RuntimeError('Storage headroom is below the declared block floor')
    return free


def execute(audit):
    audit.mkdir(parents=True, exist_ok=False)
    design = json.loads((Path(__file__).parent / 'review-plan.json').read_text())
    cfg = yaml.safe_load((ROOT / design['base_configuration']['path']).read_text())
    cfg['data'].update(design['priority_repeat']['configuration_overrides'])
    cfg['data']['EXP_ID'] = 'controlled-concentrated-s71'
    raw = yaml.safe_dump(cfg, sort_keys=False).encode()
    (audit / 'experiment-config.yaml').write_bytes(raw)
    block = dict(status='preparing', created_epoch=time.time(), seed=71, order=['scale', 'none'],
                 attempts=[], runs=[], preparation_seconds_used=0, preparation_verified=False)
    def save(): write(audit / 'block-status.json', block)
    save()
    with r.command_lock():
        if r.kubectl('config', 'current-context').decode().strip() != 'nautilus':
            raise RuntimeError('This block is restricted to the nautilus context')
        dirty = subprocess.check_output(['git', 'status', '--porcelain'], cwd=ROOT).decode()
        if dirty: raise RuntimeError('Commit the tested code before the block: ' + dirty)
        block['code_commit'] = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT).decode().strip()
        original = r.shared_read(r.CONFIG)
        (audit / 'original-shared-config.yaml').write_bytes(original)
        control = r.read_control()
        write(audit / 'original-control.json', control)
        if control and control.get('state') in ('preparing', 'running'):
            raise RuntimeError('Another managed run is active; inspect it before this block')
        sts = json.loads(r.kubectl('get', 'statefulset', 'producer-sts', 'consumer-sts', '-o', 'json'))
        write(audit / 'original-statefulsets.json', sts)
        original_counts = {p['metadata']['name']: p['spec']['replicas'] for p in sts['items']}
        if original_counts['producer-sts'] != 3:
            raise RuntimeError('This block expects the existing three producers')
        hpas = json.loads(r.kubectl('get', 'hpa', '-o', 'json'))
        write(audit / 'original-hpas.json', hpas)
        items = json.loads(r.kubectl('get', 'pods', '-l', 'app in (producer-sts,consumer-sts)', '-o', 'json'))['items']
        write(audit / 'original-pods.json', items)
        reference = dict(schema_version=1, captured_epoch=time.time(),
            assignment=design['reference']['initial_assignment'],
            pods=[pod_identity(p) for p in items if int(p['metadata']['name'].rsplit('-', 1)[1]) < 3])
        validate_reference(reference, cfg['data'])
        write(audit / 'placement-reference.json', reference)
        block['reference_sha256'] = reference_hash(reference)
        block['initial_free_bytes'] = capacity()
        save()
        plan_base = dict(action='scale', initial_consumers=3, target_consumers=6,
            after_evaluation_start_seconds=60, recovery_threshold_offsets=1500,
            recovery_hold_seconds=20, placement_reference=reference)
        changed_config = False
        try:
            r.stop()
            changed_config = True
            r.shared_write(r.CONFIG, raw)
            with r.paused_for_experiment(r, plan_base):
                try:
                    with r.prometheus_connection():
                        for action in block['order']:
                            completed = False
                            for number in range(1, 7):
                                remaining = 900 - block['preparation_seconds_used']
                                if remaining <= 0: break
                                capacity()
                                if stable_config(r.shared_read(r.CONFIG)) != stable_config(raw):
                                    raise RuntimeError('Shared configuration changed outside this block')
                                plan = dict(plan_base, action=action,
                                    target_consumers=6 if action == 'scale' else None,
                                    prepare_only=not block['preparation_verified'], preparation_budget_seconds=remaining)
                                attempt = dict(action=action, attempt=number, preparation_only=plan['prepare_only'],
                                    started_epoch=time.time(), budget_seconds=remaining, status='preparing')
                                block['attempts'].append(attempt); save()
                                started = time.monotonic()
                                print('[CONTROLLED ATTEMPT]', action, number, 'prepare only:', plan['prepare_only'], flush=True)
                                try:
                                    directory = r.complete_run(plan)
                                except PlacementMismatch as exc:
                                    current = r.read_control() or {}
                                    attempt.update(status='rejected', error=str(exc), run_id=current.get('run_id'))
                                    block['preparation_seconds_used'] += time.monotonic() - started
                                    save()
                                    if current.get('start_epoch'):
                                        raise RuntimeError('Controlled condition failed after production; preserve this trial and stop the block') from exc
                                    # Only a different initial assignment may be retried. A changed
                                    # pod, source/configuration, missing status or timeout ends the block.
                                    if str(exc) != 'Complete partition ownership differs from the block reference': raise
                                    print('[PREPARATION REJECTED]', current.get('run_id'), str(exc), flush=True)
                                    continue
                                except BaseException as exc:
                                    attempt.update(status='failed', error=str(exc) or type(exc).__name__)
                                    save()
                                    raise
                                current = json.loads((directory / 'manifest.json').read_text())
                                elapsed = (time.monotonic() - started if plan['prepare_only']
                                           else current['preparation_elapsed_seconds'])
                                block['preparation_seconds_used'] += elapsed
                                attempt.update(status='preparation_verified' if plan['prepare_only'] else 'complete',
                                    run_id=current['run_id'], directory=str(directory), preparation_seconds=elapsed,
                                    finished_epoch=time.time())
                                if plan['prepare_only']:
                                    block['preparation_verified'] = True
                                    save()
                                    continue
                                block['runs'].append(dict(action=action, run_id=current['run_id'], directory=str(directory)))
                                save()
                                completed = True
                                break
                            if not completed:
                                raise RuntimeError('Predeclared preparation limit exhausted for ' + action)
                        block['status'] = 'complete'; save()
                finally:
                    current = r.read_control() or {}
                    if current.get('state') in ('preparing', 'running') and current.get('config', {}).get('EXP_ID') != cfg['data']['EXP_ID']:
                        raise RuntimeError('Another run took control; automatic restoration requires inspection')
                    r.stop()
                    r.set_consumer_baseline(original_counts['consumer-sts'], 180)
        except BaseException as exc:
            block.update(status='failed', error=str(exc) or type(exc).__name__)
            save()
            raise
        finally:
            try:
                if changed_config:
                    observed = r.shared_read(r.CONFIG)
                    if observed != original:
                        if stable_config(observed) != stable_config(raw):
                            raise RuntimeError('Shared configuration changed; refusing to overwrite another edit')
                        r.shared_write(r.CONFIG, original)
                    if r.shared_read(r.CONFIG) != original:
                        raise RuntimeError('Shared configuration restoration did not verify')
                restored_hpas = json.loads(r.kubectl('get', 'hpa', '-o', 'json'))
                write(audit / 'restored-hpas.json', restored_hpas)
                before = {p['metadata']['name']: (p['metadata']['uid'], p['spec']) for p in hpas['items']}
                after = {p['metadata']['name']: (p['metadata']['uid'], p['spec']) for p in restored_hpas['items']}
                if before != after: raise RuntimeError('Original HPA identity/settings were not restored')
                final_sts = json.loads(r.kubectl('get', 'statefulset', 'producer-sts', 'consumer-sts', '-o', 'json'))
                write(audit / 'restored-statefulsets.json', final_sts)
                if {p['metadata']['name']: p['spec']['replicas'] for p in final_sts['items']} != original_counts:
                    raise RuntimeError('Original replica counts were not restored')
                write(audit / 'final-control.json', r.read_control())
                write(audit / 'final-pods.json', json.loads(r.kubectl('get', 'pods', '-l',
                    'app in (producer-sts,consumer-sts)', '-o', 'json'))['items'])
                block['restoration'] = 'verified'
            except Exception as exc:
                block['restoration'] = 'pending'
                block['restoration_error'] = str(exc)
                raise
            finally:
                block['finished_epoch'] = time.time(); save()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--audit-dir', type=Path, required=True, help='New directory for block evidence and restoration records')
    args = parser.parse_args()
    execute(args.audit_dir.resolve())
