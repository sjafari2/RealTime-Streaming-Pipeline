#!/usr/bin/env python3
"""Run the authorized balanced comparisons, stopping on any failed block."""
import argparse
import json
from pathlib import Path
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[2]
BLOCK = ROOT / 'experiments/controlled-followup-20260912/execute_block.py'
PLAN = [(workload, repetition, order)
        for workload in ('balanced-low', 'balanced-short', 'balanced-sustained')
        for repetition, order in ((1, 'scale-first'), (2, 'keep-first'))]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--audit-root', type=Path, required=True)
    parser.add_argument('--execute', action='store_true', help='Run the six matched blocks; otherwise show the plan')
    args = parser.parse_args()
    audit = args.audit_root.resolve()
    commands = [[sys.executable, str(BLOCK), '--audit-dir', str(audit / (workload + '-pair-' + str(rep))),
                 '--static-startup', '--include-comparison', '--workload', workload, '--trial-order', order]
                for workload, rep, order in PLAN]
    if not args.execute:
        print(json.dumps(commands, indent=2))
        return
    audit.mkdir(parents=True, exist_ok=False)
    state = dict(status='running', started_epoch=time.time(), planned_trials=12, blocks=[])
    def save():
        tmp=audit/'campaign-status.tmp'
        tmp.write_text(json.dumps(state, indent=2)+'\n')
        tmp.replace(audit/'campaign-status.json')
    save()
    try:
        for item, command in zip(PLAN, commands):
            workload, rep, order=item
            entry=dict(workload=workload, pair=rep, order=order, status='running', started_epoch=time.time())
            state['blocks'].append(entry);save()
            with (audit/(workload+'-pair-'+str(rep)+'.log')).open('x') as log:
                subprocess.run(command, cwd=ROOT, stdout=log, stderr=subprocess.STDOUT, check=True)
            record=json.loads((Path(command[3])/'block-status.json').read_text())
            if record['status']!='complete' or record.get('restoration')!='verified' or len(record['runs'])!=2:
                raise RuntimeError('Block did not complete both trials and verify restoration')
            entry.update(status='complete', runs=record['runs'], finished_epoch=time.time());save()
        state['status']='complete'
    except BaseException as exc:
        state.update(status='failed', error=str(exc) or type(exc).__name__)
        raise
    finally:
        state['finished_epoch']=time.time();save()


if __name__=='__main__':
    main()
