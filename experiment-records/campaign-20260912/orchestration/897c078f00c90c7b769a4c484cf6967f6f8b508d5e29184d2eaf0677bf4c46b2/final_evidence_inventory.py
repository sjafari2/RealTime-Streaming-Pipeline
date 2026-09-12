"""Reconcile the saved validation records before closing this campaign."""
import hashlib
import json
import time
from pathlib import Path

A = Path(__file__).resolve().parent
R = Path('/Users/soheila/Desktop/Thesis-26-27/code')
read = lambda p: json.loads(p.read_text())
ledgers = [read(A / n) for n in ('sequence-status-v4.json', 'sequence-status-isolated.json', 'sequence-status-low-input.json')]
assert all(l['status'] == 'complete' for l in ledgers)
trials = [r for l in ledgers for r in l['trials']]
assert len(trials) == 16 and all(r['status'] == 'complete' for r in trials)
rows = []
for trial in trials:
    d = R / 'results' / trial['run_id']
    evidence = read(d / 'evidence-verification.json')
    outcome = read(d / 'outcome-summary.json')
    assert read(d / 'runner-status.json')['status'] == 'complete'
    assert evidence['all_files_match'] and evidence['file_count'] > 0
    assert outcome['admitted_evaluation_cohort'] == trial['admitted']
    assert outcome['incomplete_by_drain'] == trial['unfinished']
    compressed = read(d / 'compressed-evidence.json')
    # The compression step already verified round-trip hashes. Check that its
    # listed representation is still present and has no duplicate plain file.
    for entry in compressed['files']:
        f = d / entry['compressed_path']
        assert f.exists() and f.stat().st_size == entry['compressed_bytes']
        assert not (d / entry['original_path']).exists()
    rows.append(dict(run_id=trial['run_id'], family=trial['family'], pair=trial['pair'], action=trial['action'], admitted=trial['admitted'], unfinished=trial['unfinished'], verified_evidence_files=evidence['file_count'], compressed_event_files=len(compressed['files'])))
for family in ('sustained', 'short', 'isolated', 'low-input'):
    d = A / 'comparisons' / family
    assert read(d / 'index.json')['status'] == 'complete_predeclared_family'
    for pair in read(d / 'comparison-summary.json')['pairs']:
        assert not pair['compatibility_failures'] and all(r['evidence_valid'] for r in pair['runs'].values())
record = dict(verified_epoch=time.time(), status='all_controlled_evidence_reconciled', controlled_trials=len(rows), evaluation_admissions=sum(r['admitted'] for r in rows), unfinished=sum(r['unfinished'] for r in rows), verified_evidence_files=sum(r['verified_evidence_files'] for r in rows), runs=rows, scope='Inventory of prior exact PVC hash checks and lossless compression records; numerical results remain separated by run and family.')
(A / 'final-evidence-inventory.json').write_text(json.dumps(record, indent=2) + '\n')

ids = {r['run_id'] for r in trials} | {'run-20260911-214924', 'run-20260911-234758', 'run-20260912-031020', 'run-20260912-043547', 'run-20260912-054927', 'run-20260912-083417'}
sources = []
for run_id in sorted(ids):
    files = {}
    for name in ('manifest.json', 'outcome-summary.json', 'lag-summary.json', 'execution-summary.json', 'runner-status.json', 'evidence-verification.json'):
        p = R / 'results' / run_id / name
        if p.exists():
            files[str(p)] = hashlib.sha256(p.read_bytes()).hexdigest()
    sources.append(dict(run_id=run_id, files=files))
(A / 'proposal-evidence-sources.json').write_text(json.dumps(dict(generated_epoch=time.time(), status='complete_source_index_for_retained_proposal_results', runs=sources), indent=2) + '\n')
print(json.dumps({k: v for k, v in record.items() if k != 'runs'}, indent=2))
