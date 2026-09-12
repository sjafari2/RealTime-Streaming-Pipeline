"""Stage dated orchestration sources by content hash for the campaign record."""
import hashlib
import json
import time
from pathlib import Path

A = Path(__file__).resolve().parent
R = Path('/Users/soheila/Desktop/Thesis-26-27/code')
E = R / 'experiments/preliminary-campaign-20260912'
out = A / 'orchestration-archive'
out.mkdir(exist_ok=True)
by_hash = {}
for source in A.rglob('*.py'):
    by_hash.setdefault(hashlib.sha256(source.read_bytes()).hexdigest(), []).append(source)
previous = json.loads((out / 'source-index.json').read_text()) if (out / 'source-index.json').exists() else {}
entries = previous.get('files', {})
for path, entry in entries.items():
    assert hashlib.sha256((out / path).read_bytes()).hexdigest() == entry['sha256']
    entry.setdefault('first_archived_epoch', previous['generated_epoch'])


def include(name, digest, reference):
    candidates = by_hash.get(digest, [])
    assert candidates, (name, digest, 'Exact referenced source is missing')
    source = sorted(candidates, key=lambda p: (len(p.parts), str(p)))[0]
    target = out / digest / name
    target.parent.mkdir(exist_ok=True)
    target.write_bytes(source.read_bytes())
    assert hashlib.sha256(target.read_bytes()).hexdigest() == digest
    entry = entries.setdefault(str(target.relative_to(out)), dict(sha256=digest, bytes=target.stat().st_size, recovered_from=str(source.relative_to(A)), first_archived_epoch=time.time(), references=[]))
    if reference not in entry['references']:
        entry['references'].append(reference)


current = [
    'launch_trial.py', 'guard_trial.py', 'run_sequence.py', 'verify_evidence.py',
    'export_plot_data.py', 'draw_run.py', 'package_record.py', 'refresh_comparison.py',
    'write_comparison_report.py', 'publish_comparison.py', 'build_campaign_report.py',
    'run_single_calibration.py', 'qualify_single_partition.py', 'restore_campaign.py',
    'calibrate_cpu_task.py', 'launch_pressure.py', 'guard_pressure.py', 'archive_orchestration.py',
    'cleanup_monitoring_rbac.py', 'final_evidence_inventory.py',
]
for name in current:
    include(name, hashlib.sha256((A / name).read_bytes()).hexdigest(), 'Current dated campaign helper at archive time')
for source in sorted(E.glob('*protocol*.json')):
    for name, digest in json.loads(source.read_text()).get('orchestration_sha256', {}).items():
        include(name, digest, str(source.relative_to(R)))
for entry in json.loads((A / 'comparison-orchestration-source.json').read_text())['files']:
    include(Path(entry['path']).name, entry['sha256'], 'Original comparison-orchestration-source.json')
plan = json.loads((E / 'single-partition-capacity-plan.json').read_text())
for field, name in [('qualification_helper_sha256', 'qualify_single_partition.py'), ('driver_sha256', 'run_single_calibration.py'), ('guard_sha256', 'guard_trial.py'), ('launcher_sha256', 'launch_trial.py')]:
    include(name, plan[field], 'single-partition-capacity-plan.json / ' + field)
for family in ('sustained', 'short', 'isolated', 'low-input'):
    manifest = R / 'experiment-records/campaign-20260912/comparisons' / family / 'comparison-files.json'
    if manifest.exists():
        for path, digest in json.loads(manifest.read_text())['scripts'].items():
            if Path(path).name == 'write_comparison_report.py':
                include('write_comparison_report.py', digest, str(manifest.relative_to(R)))

(out / 'source-index.json').write_text(json.dumps(dict(generated_epoch=time.time(), files=entries), indent=2) + '\n')
(out / 'README.md').write_text('''# Dated campaign orchestration archive

These scripts record how the September 2026 preliminary campaign was coordinated,
guarded, collected and reported. They are outside active application and import
paths. `source-index.json` maps every preserved file to its SHA-256 and the protocol
or report that references it. Earlier versions match the recorded hashes exactly;
they are retained to explain technical exclusions and corrections.

This is an audit archive, not a second normal runtime. Scripts intentionally retain
the original workspace paths, dated deadlines, unique-label checks and recovery
state assumptions. Do not run them against an unrelated or current experiment.
Use the maintained `my-shell/save-run.sh` or `my-shell/run_pipeline.sh` commands and
the runtime guide for ordinary work. The campaign protocols define the exact
workloads, seeds, trial order and guard bounds used here.

Producer, consumer, coordinator, analysis and plotting modules are maintained in
the repository and preserved in Git history. Run records retain the actual source
and environment signatures; a later analysis commit is not represented as the
application revision that ran. Full message evidence remains in `results/RUN_ID/`
and on the Nautilus PVCs. This small source archive is not a full raw-data backup.

Private research-progress correspondence and proposal editing scripts are not
part of this archive.
''')
print(out, 'verified source objects:', len(entries))
