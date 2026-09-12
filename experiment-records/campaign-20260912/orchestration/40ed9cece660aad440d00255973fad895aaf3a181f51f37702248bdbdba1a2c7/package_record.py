"""Save a small Git record while retaining full evidence in results and on Nautilus."""
import argparse
import hashlib
import json
from pathlib import Path
import shutil
import time

R = Path('/Users/soheila/Desktop/Thesis-26-27/code')
A = Path(__file__).resolve().parent
p = argparse.ArgumentParser()
p.add_argument('run_id')
p.add_argument('--label', required=True)
p.add_argument('--qualification', required=True)
args = p.parse_args()
D = R / 'results' / args.run_id
O = R / 'experiment-records' / args.run_id
O.mkdir(exist_ok=True)
status = json.loads((D / 'runner-status.json').read_text())
manifest = json.loads((D / 'manifest.json').read_text())
config = manifest['config']
for name in ('manifest.json', 'pipeline-configmap.yaml', 'runner-status.json',
             'runner-status-before-export-recovery.json', 'outcome-summary.json',
             'execution-summary.json', 'lag-summary.json', 'export-status.json',
             'evidence-verification.json', 'compressed-evidence.json',
             'intervention-events.jsonl', 'resource-history.jsonl'):
    if (D / name).is_file():
        shutil.copy2(D / name, O / name)
if (D / 'plots').exists():
    shutil.copytree(D / 'plots', O / 'plots', dirs_exist_ok=True)
    shutil.copy2(A / 'draw_run.py', D / 'plots/draw_run.py')
    shutil.copy2(A / 'draw_run.py', O / 'plots/draw_run.py')
for name in ('launch.json', 'control.json', 'live-monitor.jsonl', 'container-resources.jsonl', 'guard-stop.json'):
    if (A / args.label / name).is_file():
        shutil.copy2(A / args.label / name, O / name)
finals = [json.loads(f.read_text()) for f in D.rglob('final.json')]
(O / 'process-final-statuses.json').write_text(json.dumps(finals, indent=2) + '\n')
lines = [f'# {args.run_id}', '', args.qualification, '',
         f'Managed status: **{status["status"]}**. Full data: `results/{args.run_id}/` in the active code folder; original event evidence remains on the Nautilus PVCs.', '',
         f'The frozen configuration uses {config["PRODUCER_POD_COUNT"]} producers, {config["CONSUMER_POD_COUNT"]} initial consumers, {config["NUM_PARTITIONS"]} partitions, {config["TRAFFIC_MODE"]} input, seed {config["WORKLOAD_SEED"]}, and {config["APP_CPU_ITERATIONS"]} SHA-256 iterations per message. The target is {float(config["TARGET_RATE"])*int(config["PRODUCER_POD_COUNT"]):,.0f} messages/s total. Production lasts {config["EXP_DURATION_SEC"]} seconds, with {config["WARMUP_SECONDS"]} seconds of warm-up and a {config["DRAIN_SECONDS"]}-second bounded drain.', '']
if status['status'] == 'complete':
    s = json.loads((D / 'outcome-summary.json').read_text())
    lag = json.loads((D / 'lag-summary.json').read_text())
    e = json.loads((D / 'execution-summary.json').read_text())
    assert not s['validity_failures']
    rows = [
        ('Distinct evaluation admissions', s['admitted_evaluation_cohort']),
        ('Completed by drain', s['completed_by_drain']),
        ('Unfinished by drain', s['incomplete_by_drain']),
        ('Duplicate cohort completion attempts', s['duplicate_completion_attempts']),
        ('Admissions / evaluation second', s['admitted_messages_per_second']),
        ('Unique completions / evaluation second', s['useful_throughput_per_second']),
        ('Recorded completion mean (ms)', 1000*s['admitted_cohort_completion_mean_seconds']),
        ('Recorded completion p99 (ms)', 1000*s['admitted_cohort_completion_p99_seconds']),
        ('Provisional 99 ms cohort deadline-miss fraction', s['deadline_miss_fraction']),
        ('Lag coverage fraction', lag['covered_fraction']),
        ('Covered mean returned-position lag (offsets)', lag['time_weighted_mean_lag']),
        ('Peak sampled returned-position lag (offsets)', lag['peak_sampled_lag']),
        ('Recorded consumer process-seconds', e['consumer_process_seconds']),
        ('Resource request-time coverage fraction', (e.get('resource_requests') or {}).get('covered_fraction'))]
    lines += ['| Measure | Value |', '| --- | ---: |']
    def formatted(value):
        if isinstance(value, float):
            return f'{value:,.4f}'.rstrip('0').rstrip('.')
        return f'{value:,}' if isinstance(value, int) else str(value)
    lines += [f'| {name} | {formatted(value)} |' for name, value in rows]
    lines += ['', 'Cohort latency follows each distinct acknowledged evaluation message to its earliest valid processing completion by drain, before commit acknowledgment. Unfinished messages are reported separately. Monitoring rates and histogram quantiles use rolling 30-second windows and different populations. Clock probes do not establish a tight independent cross-node accuracy bound; the 99 ms threshold is provisional.', '',
              'Resource-request integrals cover only observed intervals and consumer-container requests. Process lifetimes include preparation and drain. These quantities are not actual whole-cluster CPU use or a fair cost comparison when observation spans or coverage differ.', '',
              'Local gzip conversion preserves every event byte and verifies decompressed SHA-256 values. These small Git records are not the raw-data backup. Saved query JSON, CSV samples, PNG/SVG figures and plotting code allow the figures to be reproduced offline.']
else:
    lines += ['This attempt is excluded from performance comparisons. Retained failures are:'] + ['- ' + x for x in status.get('issues', [])]
(O / 'validation-report.md').write_text('\n'.join(lines) + '\n')
rows = []
for path in sorted(O.rglob('*')):
    if path.is_file() and path.name != 'record-files.json':
        rows.append(dict(path=str(path.relative_to(O)), bytes=path.stat().st_size,
                         sha256=hashlib.sha256(path.read_bytes()).hexdigest()))
(O / 'record-files.json').write_text(json.dumps(dict(run_id=args.run_id, packaged_epoch=time.time(), files=rows), indent=2) + '\n')
print('Packaged', args.run_id, len(rows), 'files,', sum(x['bytes'] for x in rows), 'bytes')
