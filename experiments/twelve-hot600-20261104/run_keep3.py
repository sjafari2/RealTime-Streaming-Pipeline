"""Run one declared Consumer 0 stress control and stop for review of its outcome."""
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time

import yaml

ROOT = Path(os.environ.get('PIPELINE_REPOSITORY', Path(__file__).resolve().parents[2]))


def load_support():
    spec = importlib.util.spec_from_file_location('twelve_hot_support', ROOT/'experiments/twelve-hot600-20261104/run.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def hot_partitions(reference):
    selected = sorted(row['partition'] for row in reference['assignment']
                      if row['pod'] == 'consumer-sts-0' and row['topic_index'] == 0)[:12]
    if len(selected) != 12:
        raise ValueError('Consumer 0 must initially own at least twelve partitions')
    return selected


def validate_plan(plan):
    if plan.get('purpose') != 'single_keep3_new_deployment' or plan.get('schema_version') != 1:
        raise ValueError('Declare this standalone control as a new deployment, not a matched comparison')
    if plan.get('hot_owner') != 'consumer-sts-0':
        raise ValueError('All initial high-traffic partitions must belong to Consumer 0')
    if plan.get('aggregate_rate') != 600 or plan.get('phases_seconds') != [60,600,120]:
        raise ValueError('This control uses 600 messages/s and 60/600/120-second phases')
    if plan.get('maximum_performance_trials') != 1 or plan.get('continue_to_mitigation') is not False:
        raise ValueError('This command runs one control only and always stops for review')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--deployment-plan', type=Path, required=True)
    parser.add_argument('--evidence-root', type=Path, required=True)
    parser.add_argument('--backup-root', type=Path, required=True)
    parser.add_argument('--execute', action='store_true')
    args = parser.parse_args()
    document = json.loads(args.deployment_plan.read_text())
    validate_plan(document)
    support = load_support()
    base = support.load_base()
    from comparison_admission import deployment_check, require_passed, check_saved_control, validate_stress_settings
    from placement_control import pod_identity, validate_pod_reference
    validate_pod_reference(document['reference_pods'])
    validate_stress_settings(document['stress'])
    support.install(base, document['calibration_reference'])
    config, plan = base.prepared('keep3',1,81,aggregate_rate=600,hot_count=12,deadline_ms=1000)
    require_passed(deployment_check(document,document['reference_pods'],config['data']))
    if not args.execute:
        print(json.dumps(document,indent=2))
        return
    if subprocess.check_output(['git','status','--porcelain','--untracked-files=no'],cwd=ROOT).strip():
        raise RuntimeError('Commit reviewed source before execution')
    if subprocess.check_output(['kubectl','config','current-context']).decode().strip() != 'nautilus' or base.r.NS != 'kafkastreamingdata':
        raise RuntimeError('Unexpected cluster or namespace')
    evidence = args.evidence_root.resolve()
    evidence.mkdir(parents=True,exist_ok=True)
    audit = evidence/('keep3-control-'+time.strftime('%Y%m%d-%H%M%S'))
    audit.mkdir()
    state = dict(status='preparing',started_epoch=time.time(),protocol=document,
                 plan_sha256=hashlib.sha256(args.deployment_plan.read_bytes()).hexdigest(),
                 code_commit=subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT).decode().strip())

    def save(**values):
        state.update(values)
        target=audit/'status.tmp'
        target.write_text(json.dumps(state,indent=2)+'\n')
        target.replace(audit/'status.json')

    save()
    print('[CONTROL]',audit,flush=True)
    support.use_local_evidence(base,evidence)
    r=base.r
    if shutil.which('caffeinate'):
        subprocess.Popen(['caffeinate','-is','-w',str(os.getpid())])
    with r.command_lock():
        current=r.read_control()
        if current and current.get('state') in ('preparing','running'):
            raise RuntimeError('Another managed run is active')
        original=r.shared_read(r.CONFIG)
        (audit/'original-config.yaml').write_bytes(original)
        pods=[pod_identity(p) for p in json.loads(r.kubectl('get','pods','-l',r.POD_SELECTOR,'-o','json'))['items']]
        if sorted(pods,key=lambda p:p['pod']) != sorted(document['reference_pods'],key=lambda p:p['pod']):
            raise RuntimeError('Pod identity or placement changed after the declared deployment snapshot')
        admission=deployment_check(document,pods,config['data'])
        save(deployment_admission=admission)
        require_passed(admission)
        expected=None

        def configure(configuration,label):
            nonlocal expected
            if expected is not None and base.normalized(r.shared_read(r.CONFIG)) != base.normalized(expected):
                raise RuntimeError('Shared configuration changed externally')
            expected=yaml.safe_dump(configuration,sort_keys=False).encode()
            (audit/(label+'.yaml')).write_bytes(expected)
            r.shared_write(r.CONFIG,expected)

        try:
            r.stop()
            capture_config,capture_plan=base.prepared('kafka_scale6',0,81,aggregate_rate=600,hot_count=12,deadline_ms=1000)
            capture_plan.update(action='none',target_consumers=None,prepare_only=True,capture_placement_pods=pods)
            configure(capture_config,'preparation')
            with r.paused_for_experiment(r,plan),r.prometheus_connection():
                r.preflight()
                preparation=r.complete_run(capture_plan)
                checked=base.audit_preparation(preparation)
                if not checked['valid'] or checked['issues']:
                    raise RuntimeError('Empty-topic preparation did not pass')
                reference=json.loads((preparation/'manifest.json').read_text())['placement_reference']
                hot=hot_partitions(reference)
                support.preserve(preparation,args.backup_root)
                config,plan=base.prepared('keep3',1,81,reference,hot,aggregate_rate=600,hot_count=12,deadline_ms=1000)
                config['data']['EXP_ID']='keep3-consumer0-600-stress'
                configure(config,'keep3')
                save(status='starting',hot_partitions=hot,preparation_directory=str(preparation.resolve()))
                r.preflight()
                directory=r.complete_run(plan)
                checked=base.validate(directory,require_all=False)
                report=check_saved_control(directory,document)
                (audit/'stress-admission.json').write_text(json.dumps(report,indent=2)+'\n')
                if report['status']!='passed':
                    (audit/'attention-required.json').write_text(json.dumps(report,indent=2)+'\n')
                save(status='complete_waiting_for_review',run_directory=str(directory.resolve()),
                     validation=checked,stress_admission=report)
                print('[STRESS CHECK]',json.dumps(report),flush=True)
        except BaseException as exc:
            save(status='failed',error=str(exc) or type(exc).__name__)
            raise
        finally:
            r.stop()
            if expected is not None and base.normalized(r.shared_read(r.CONFIG)) != base.normalized(expected):
                save(restoration='pending_external_change')
                raise RuntimeError('External configuration change prevents restoration')
            r.shared_write(r.CONFIG,original)
            if r.shared_read(r.CONFIG)!=original:
                raise RuntimeError('Configuration restoration failed verification')
            save(restoration='verified',finished_epoch=time.time())
            if state.get('run_directory'):
                from compress_evidence import compress
                from compress_monitoring_csv import compress_csv
                completed=Path(state['run_directory'])
                compress(completed)
                compress_csv(completed)
                support.preserve(completed,args.backup_root)
            support.preserve(audit,args.backup_root)
            print('[STOPPED] Single Keep 3 control ended. No mitigation trial will start automatically.',flush=True)
    return audit


if __name__=='__main__':
    main()
