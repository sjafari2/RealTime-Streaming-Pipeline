#!/usr/bin/env python3
"""Run two balanced rates twice with three consumers and no mitigation."""
import argparse
import json
from pathlib import Path
import subprocess
import sys
import time
import yaml

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'my-shell'))
import run_experiment as r


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--execute', action='store_true')
    args = parser.parse_args()
    design = dict(aggregate_rates=[600, 1500], repetitions=2, consumers=3,
                  warmup_seconds=60, evaluation_seconds=1200, drain_seconds=120,
                  mitigation='none', workload='balanced', workload_seed=71)
    if not args.execute:
        print(json.dumps(design, indent=2))
        return
    if subprocess.check_output(['git', 'status', '--porcelain', '--untracked-files=no'], cwd=ROOT).strip():
        raise RuntimeError('Commit reviewed changes before execution')
    audit = ROOT / 'results' / ('stability-fixed3-' + time.strftime('%Y%m%d-%H%M%S'))
    audit.mkdir(parents=True, exist_ok=False)
    state = dict(status='preparing', design=design, runs=[], started_epoch=time.time(),
                 code_commit=subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT).decode().strip())
    def save():
        temporary = audit / 'campaign-status.tmp'
        temporary.write_text(json.dumps(state, indent=2) + '\n')
        temporary.replace(audit / 'campaign-status.json')
    save()
    print('[CAMPAIGN]', audit, flush=True)
    with r.command_lock():
        control = r.read_control()
        if control and control.get('state') in ('preparing', 'running'):
            raise RuntimeError('Another managed experiment is active')
        original = r.shared_read(r.CONFIG)
        (audit / 'original-config.yaml').write_bytes(original)
        template = yaml.safe_load((ROOT / 'experiments/stability-20260914/lower-keep3-run1.yaml').read_text())
        intervention = json.loads((ROOT / 'experiments/stability-20260914/lower-keep3-run1-plan.json').read_text())
        expected = None
        def normalized(raw):
            cfg = yaml.safe_load(raw)
            for key in ('RUN_ID', 'TOPIC_TITLE'):
                cfg['data'].pop(key, None)
            return cfg
        try:
            # Validate HPA control against this campaign, not the previous run's timing.
            r.stop()
            expected = yaml.safe_dump(template, sort_keys=False).encode()
            r.shared_write(r.CONFIG, expected)
            with r.paused_for_experiment(r, intervention), r.prometheus_connection():
                for total_rate in design['aggregate_rates']:
                    for number in (1, 2):
                        r.stop()
                        name = f'stability-fixed3-rate{total_rate}-run{number}'
                        template['data'].update(TARGET_RATE=str(total_rate // 3), EXP_ID=name,
                                               CONSUMER_GROUP_ID=name + '-' + str(time.time_ns()))
                        expected = yaml.safe_dump(template, sort_keys=False).encode()
                        (audit / (name + '.yaml')).write_bytes(expected)
                        r.shared_write(r.CONFIG, expected)
                        r.preflight()
                        state.update(status='running', current_rate=total_rate, current_run_number=number)
                        save()
                        directory = r.complete_run(intervention)
                        subprocess.run([sys.executable, str(ROOT / 'python-scripts/analyze_stability.py'), str(directory)], check=True)
                        state['runs'].append(dict(aggregate_rate=total_rate, run_number=number, directory=str(directory)))
                        save()
            state['status'] = 'complete'
        except BaseException as exc:
            state.update(status='failed', error=str(exc) or type(exc).__name__)
            raise
        finally:
            try:
                r.stop()
                if expected is not None and normalized(r.shared_read(r.CONFIG)) != normalized(expected):
                    raise RuntimeError('Configuration changed externally; automatic restoration stopped')
                r.shared_write(r.CONFIG, original)
                if r.shared_read(r.CONFIG) != original:
                    raise RuntimeError('Restored configuration differs from backup')
                state['restoration'] = 'verified'
            except BaseException as exc:
                state.update(status='failed', restoration_error=str(exc))
                raise
            finally:
                state['finished_epoch'] = time.time()
                save()


if __name__ == '__main__':
    main()
