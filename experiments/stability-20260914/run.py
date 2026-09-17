#!/usr/bin/env python3
"""Execute the six balanced stability trials after a recorded calibration review."""
import argparse
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import time
import yaml

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'my-shell'))
import run_experiment as r


def plan():
    return [('lower', 1), ('pressure', 1), ('pressure', 2), ('lower', 2)]


def lower(audit, number):
    audit.mkdir(parents=True, exist_ok=False)
    with r.command_lock():
        original = r.shared_read(r.CONFIG)
        (audit / 'original-config.yaml').write_bytes(original)
        current = r.read_control()
        if current and current.get('state') in ('preparing', 'running'):
            raise RuntimeError('Another managed run is active')
        cfg = yaml.safe_load((Path(__file__).parent / f'lower-keep3-run{number}.yaml').read_text())
        cfg['data']['CONSUMER_GROUP_ID'] += '-' + str(time.time_ns())
        raw = yaml.safe_dump(cfg, sort_keys=False).encode()
        (audit / 'experiment-config.yaml').write_bytes(raw)
        intervention = json.loads((Path(__file__).parent / f'lower-keep3-run{number}-plan.json').read_text())
        def normalize(value):
            result = yaml.safe_load(value)
            for key in ('RUN_ID', 'TOPIC_TITLE'): result['data'].pop(key, None)
            return result
        try:
            r.stop()
            r.shared_write(r.CONFIG, raw)
            r.preflight()
            with r.paused_for_experiment(r, intervention), r.prometheus_connection():
                directory = r.complete_run(intervention)
                return [dict(action='none', directory=str(directory))]
        finally:
            r.stop()
            if normalize(r.shared_read(r.CONFIG)) != normalize(raw):
                raise RuntimeError('Shared configuration changed outside this trial; inspect before restoration')
            r.shared_write(r.CONFIG, original)
            if r.shared_read(r.CONFIG) != original:
                raise RuntimeError('Configuration restoration failed')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--audit-root', type=Path, required=True)
    parser.add_argument('--calibration-review', type=Path)
    parser.add_argument('--execute', action='store_true')
    args = parser.parse_args()
    if not args.execute:
        print(json.dumps(dict(trials=6, total_minutes_per_trial=23,
            warmup_seconds=60, evaluation_seconds=1200, drain_seconds=120,
            scale_after_evaluation_seconds=300, blocks=plan()), indent=2))
        return
    if not args.calibration_review:
        parser.error('--execute requires --calibration-review')
    review = json.loads(args.calibration_review.read_text())
    if review.get('status') != 'approved_for_stability' or len(review.get('runs', [])) != 2:
        raise RuntimeError('Both calibration runs and their review are required')
    for entry in review['runs']:
        directory = Path(entry['directory'])
        status = json.loads((directory / 'runner-status.json').read_text())
        if status['status'] != 'complete': raise RuntimeError('Calibration run did not pass its evidence checks')
    if subprocess.check_output(['git','status','--porcelain','--untracked-files=no'],cwd=ROOT).strip():
        raise RuntimeError('Commit the reviewed implementation before running')
    audit = args.audit_root.resolve()
    audit.mkdir(parents=True, exist_ok=False)
    (audit / 'calibration-review.json').write_text(json.dumps(review, indent=2)+'\n')
    state = dict(status='running', started_epoch=time.time(), planned_trials=6, runs=[],
                 code_commit=subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT).decode().strip())
    def save():
        tmp = audit / 'campaign-status.tmp'
        tmp.write_text(json.dumps(state, indent=2)+'\n')
        tmp.replace(audit / 'campaign-status.json')
    save()
    spec = importlib.util.spec_from_file_location('stability_block', ROOT / 'experiments/controlled-followup-20260912/execute_block.py')
    block = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(block)
    try:
        for condition, number in plan():
            state.update(current_condition=condition, current_run_number=number);save()
            target = audit / f'{condition}-run{number}'
            if condition == 'lower':
                runs = lower(target, number)
            else:
                block.execute(target, static_startup=True, include_comparison=True,
                              workload='stability-pressure', trial_order='keep-first' if number == 1 else 'scale-first')
                record = json.loads((target / 'block-status.json').read_text())
                if record['status'] != 'complete' or record.get('restoration') != 'verified':
                    raise RuntimeError('Controlled block did not complete and restore')
                runs = record['runs']
            for entry in runs:
                subprocess.run([sys.executable, str(ROOT/'python-scripts/analyze_stability.py'), entry['directory']], check=True)
                state['runs'].append(dict(entry, condition=condition, run_number=number));save()
        state['status'] = 'complete'
    except BaseException as exc:
        state.update(status='failed', error=str(exc) or type(exc).__name__)
        raise
    finally:
        state['finished_epoch'] = time.time();save()


if __name__ == '__main__':
    main()
