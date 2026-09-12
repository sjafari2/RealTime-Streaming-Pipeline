"""Execute only the frozen campaign rows, retaining evidence before the next trial."""
import json
import argparse
import hashlib
import os
from pathlib import Path
import signal
import subprocess
import sys
import time

A = Path(__file__).resolve().parent
W = A.parents[1]
R = Path('/Users/soheila/Desktop/Thesis-26-27/code')
E = R/'experiments/preliminary-campaign-20260912'
sys.path.insert(0, str(R/'my-shell'))
import run_experiment as coordinator
parser=argparse.ArgumentParser();parser.add_argument('--protocol',default='comparison-protocol.json');parser.add_argument('--ledger',default='sequence-status.json');args=parser.parse_args()
protocol_path=E/args.protocol
protocol = json.loads(protocol_path.read_text())
assert protocol['calibration_qualified'] is True
assert len(protocol['trials']) <= 8
state_path = A/args.ledger
assert not state_path.exists(), 'Inspect any existing sequence before resuming it'
state = dict(started_epoch=time.time(), status='running', protocol=str(protocol_path), trials=[])
env = dict(os.environ, PYTHONDONTWRITEBYTECODE='1', MPLCONFIGDIR='/tmp/pipeline-matplotlib')


def save():
    temporary = state_path.with_suffix('.json.tmp')
    temporary.write_text(json.dumps(state, indent=2)+'\n')
    temporary.replace(state_path)


def execute(arguments, log):
    with log.open('a') as stream:
        subprocess.run(arguments, cwd=W, env=env, stdout=stream, stderr=subprocess.STDOUT, check=True)


save()
try:
    for trial in protocol['trials']:
        for kind in ('config', 'plan'):
            assert hashlib.sha256((E/trial[kind]).read_bytes()).hexdigest()==trial[kind+'_sha256'], 'Frozen '+kind+' changed'
        # Leave at least one hour for final analysis and restoration, plus a
        # conservative allowance for this trial's readiness and collection.
        if time.time()+trial['production_seconds']+120+900 >= protocol['deadline_epoch']-3600:
            state['status']='stopped_at_budget_boundary'
            break
        label=trial['label']
        item=dict(trial, status='starting', started_epoch=time.time())
        state['trials'].append(item); save()
        log=A/(label+'-sequence.log')
        command=[sys.executable,'-u','review/preliminary-campaign-20260912/launch_trial.py',
                 '--label',label,'--config',str(E/trial['config']),
                 '--initial-consumers','3','--plan',str(E/trial['plan'])]
        guard_command=[sys.executable,'-u','review/preliminary-campaign-20260912/guard_trial.py',
                       '--label',label,'--monitoring-transition-seconds',str(protocol['transition_monitoring_guard_grace_seconds'])]
        print('Starting',label,flush=True)
        with (A/(label+'-runner.log')).open('w') as runner_log, (A/(label+'-guard.log')).open('w') as guard_log:
            runner=subprocess.Popen(command,cwd=W,env=env,stdout=runner_log,stderr=subprocess.STDOUT)
            guard=subprocess.Popen(guard_command,cwd=W,env=env,stdout=guard_log,stderr=subprocess.STDOUT)
            interrupted=False
            guard_failed=False
            while runner.poll() is None:
                if guard.poll() not in (None,0) and not interrupted:
                    guard_failed=True;interrupted=True
                    current=coordinator.read_control()
                    live=current and not current.get('intervention_cancelled_epoch') and (
                        current.get('state')=='preparing' or
                        current.get('state')=='running' and time.time()<current['drain_end_epoch'])
                    if live:
                        # Never send a second interrupt into existing cleanup.
                        runner.send_signal(signal.SIGINT)
                        print('Guard failed; requested managed stop for',label,flush=True)
                time.sleep(2)
            try:guard.wait(timeout=15)
            except subprocess.TimeoutExpired:
                guard.terminate();guard.wait(timeout=10)
            if runner.returncode:
                raise RuntimeError(label+': runner failed; inspect its retained log/evidence before another trial')
        launch=json.loads((A/label/'launch.json').read_text())
        D=Path(launch['result_directory']);run=D.name
        item.update(run_id=run,status='preserving_evidence');save()
        for script in ('verify_evidence.py','export_plot_data.py'):
            execute([sys.executable,'-u',str(A/script),str(D)],log)
        execute([sys.executable,str(A/'draw_run.py'),str(D/'plots')],log)
        execute([sys.executable,str(R/'python-scripts/compress_evidence.py'),str(D)],log)
        qualification=(f"Frozen {trial['family']} pilot, pair {trial['pair']}, scheduled action {trial['action']}. "
                       'HPA paused; initial replicas checked. This is a preliminary action comparison, not a full adaptive controller evaluation. '
                       'Interpret latency with unfinished outcomes, achieved admission, coverage, deployment/ownership events and common-horizon resource requests.')
        execute([sys.executable,str(A/'package_record.py'),run,'--label',label,'--qualification',qualification],log)
        record='experiment-records/'+run
        execute(['git','-C',str(R),'add','--',record],log)
        execute(['git','-C',str(R),'diff','--cached','--check'],log)
        execute(['git','-C',str(R),'commit','--quiet','-m','Save '+label+' evidence and figures'],log)
        summary=json.loads((D/'outcome-summary.json').read_text())
        lag=json.loads((D/'lag-summary.json').read_text())
        item.update(status='complete',finished_epoch=time.time(),
                    admitted=summary['admitted_evaluation_cohort'],unfinished=summary['incomplete_by_drain'],
                    p99_seconds=summary['admitted_cohort_completion_p99_seconds'],lag_coverage=lag['covered_fraction'])
        save();print('Completed',json.dumps(item),flush=True)
        # Keep an unfavorable valid result. Stop only for data/control problems
        # requiring inspection, rather than automatically selecting a replacement.
        fraction=summary['admitted_messages_per_second']/protocol['total_target_messages_per_second']
        if guard_failed or summary['validity_failures'] or fraction<.98:
            state['status']='paused_for_evidence_review';save();break
    else:
        state['status']='complete'
except BaseException as exc:
    state.update(status='failed',error=str(exc) or type(exc).__name__)
    raise
finally:
    state['finished_epoch']=time.time();save()
    print('Sequence status:',state['status'],flush=True)
