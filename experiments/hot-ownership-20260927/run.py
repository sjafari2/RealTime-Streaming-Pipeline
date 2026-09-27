#!/usr/bin/env python3
"""Run selected fixed ownership layouts at 700 aggregate messages/s, once each."""
import argparse
import copy
import json
from pathlib import Path
import subprocess
import sys
import time

import yaml

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'my-shell'))
import run_experiment as r

HERE = Path(__file__).resolve().parent
PLAN = dict(action='none', initial_consumers=3, target_consumers=None,
            after_evaluation_start_seconds=120)


def normalized(raw):
    config = yaml.safe_load(raw)
    for name in ('RUN_ID', 'TOPIC_TITLE'):
        config['data'].pop(name, None)
    return config


def configs(layouts=('distributed', 'concentrated')):
    result = []
    for layout in layouts:
        config = yaml.safe_load((HERE / (layout + '.yaml')).read_text())
        r.validate_config(config['data'])
        r.validate_intervention(PLAN, config['data'])
        result.append((layout, config))
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--execute', action='store_true')
    parser.add_argument('--layouts', nargs='+', choices=('distributed', 'concentrated', 'concentrated-c2', 'concentrated-c1'),
                        default=['distributed', 'concentrated'],
                        help='Run only these reviewed layouts, once each, in this order')
    args = parser.parse_args()
    if len(set(args.layouts)) != len(args.layouts):
        parser.error('List each layout once; this runner does not repeat trials')
    reviewed = configs(args.layouts)
    design = json.loads((HERE / 'design.json').read_text())
    design['runs'] = [next(row for row in design['runs'] if row['layout'] == layout)
                      for layout in args.layouts]
    if not args.execute:
        print(json.dumps(design, indent=2))
        return
    if subprocess.check_output(['git', 'status', '--porcelain', '--untracked-files=no'], cwd=ROOT).strip():
        raise RuntimeError('Commit reviewed changes before execution')
    context = subprocess.check_output(['kubectl', 'config', 'current-context']).decode().strip()
    if context != 'nautilus' or r.NS != 'kafkastreamingdata':
        raise RuntimeError('This campaign requires the reviewed Nautilus context and namespace')
    audit = ROOT / 'results' / ('hot-ownership-' + time.strftime('%Y%m%d-%H%M%S'))
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
        expected = None
        try:
            r.stop()
            expected = yaml.safe_dump(reviewed[0][1], sort_keys=False).encode()
            r.shared_write(r.CONFIG, expected)
            with r.paused_for_experiment(r, PLAN), r.prometheus_connection():
                for layout, reviewed_config in reviewed:
                    r.stop()
                    if normalized(r.shared_read(r.CONFIG)) != normalized(expected):
                        raise RuntimeError('Shared settings changed outside this campaign')
                    config = copy.deepcopy(reviewed_config)
                    config['data']['CONSUMER_GROUP_ID'] += '-' + str(time.time_ns())
                    expected = yaml.safe_dump(config, sort_keys=False).encode()
                    (audit / (layout + '.yaml')).write_bytes(expected)
                    r.shared_write(r.CONFIG, expected)
                    r.preflight()
                    state.update(status='running', current_layout=layout)
                    save()
                    directory = r.complete_run(PLAN)
                    manifest = json.loads((directory / 'manifest.json').read_text())
                    if not manifest.get('explicit_start_verification', {}).get('verified'):
                        raise RuntimeError('Missing exact initial-ownership verification')
                    state['runs'].append(dict(layout=layout, directory=str(directory), run_id=manifest['run_id']))
                    save()
            state['status'] = 'complete'
        except BaseException as exc:
            state.update(status='failed', error=str(exc) or type(exc).__name__)
            raise
        finally:
            try:
                r.stop()
                if expected is not None and normalized(r.shared_read(r.CONFIG)) != normalized(expected):
                    raise RuntimeError('Shared settings changed externally; automatic restoration stopped')
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
