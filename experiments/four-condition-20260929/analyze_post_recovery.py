"""Compare a common late production cohort without replacing whole-run results."""
import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import subprocess
import sys
import tempfile

ROOT = Path(os.environ.get('PIPELINE_REPOSITORY', Path(__file__).resolve().parents[2]))
sys.path.insert(0, str(ROOT / 'python-scripts'))
from evaluate_run import evaluate
from evidence_io import event_paths

ARMS = ('redistribute3', 'scale_redistribute6')
LABELS = {'redistribute3': 'Redistribute within 3',
          'scale_redistribute6': 'Scale to 6 + targeted redistribution'}
KEYS = ('admitted_evaluation_cohort', 'completed_by_drain', 'incomplete_by_drain',
        'incomplete_fraction', 'deadline_misses', 'deadline_censored', 'deadline_miss_fraction',
        'duplicate_completion_attempts', 'invalid_clock_observations',
        'admitted_cohort_valid_completion_count', 'admitted_cohort_completion_mean_seconds',
        'admitted_cohort_completion_p50_seconds', 'admitted_cohort_completion_p95_seconds',
        'admitted_cohort_completion_p99_seconds', 'admitted_messages_per_second',
        'useful_throughput_per_second', 'validity_failures')


def sha256(path):
    digest = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(chunk)
    return digest.hexdigest()


def evaluate_cohort(directory, start, end, deadline_thresholds_ms=None):
    """Reuse the existing identity reconciliation and nearest-rank calculation."""
    directory = Path(directory).resolve()
    manifest = json.loads((directory / 'manifest.json').read_text())
    if not (manifest['evaluation_start_epoch'] <= start < end <= manifest['producer_end_epoch']):
        raise ValueError('Cohort window must be inside the original evaluation period')
    # Only this temporary view changes the birth-time filter. The original
    # manifest, evidence, cutoff and saved whole-run summaries remain untouched.
    with tempfile.TemporaryDirectory() as temporary:
        view = Path(temporary)
        selected = dict(manifest, evaluation_start_epoch=start, producer_end_epoch=end)
        (view / 'manifest.json').write_text(json.dumps(selected))
        for role in ('producer', 'consumer'):
            source = directory / role
            if source.exists():
                for path in source.rglob('*'):
                    if path.is_file():
                        target = view / path.relative_to(directory)
                        target.parent.mkdir(parents=True, exist_ok=True)
                        target.symlink_to(path)
        return evaluate(view, deadline_thresholds_ms=deadline_thresholds_ms)


def verified_recovery(row, window_start):
    matches = [r for r in row['intervention_cost']['recovery_from_decision']
               if r['threshold_offsets'] == 100 and r['hold_seconds'] == 30]
    if len(matches) != 1 or matches[0]['confirmed_epoch'] is None:
        raise ValueError('No confirmed recovery under the specified criterion')
    if matches[0]['confirmed_epoch'] > window_start:
        raise ValueError('The common cohort window starts before confirmed recovery')
    return matches[0]['confirmed_epoch']


def analyze(comparison_path, repository, window_seconds=120):
    comparison_path, repository = Path(comparison_path), Path(repository)
    comparison = json.loads(comparison_path.read_text())
    rows = [r for r in comparison['runs'] if r['arm'] in ARMS]
    if sorted((r['arm'], r['run_number']) for r in rows) != sorted((a, n) for a in ARMS for n in (1, 2)):
        raise ValueError('Expected two runs for each requested treatment')
    if not math.isfinite(window_seconds) or window_seconds <= 0:
        raise ValueError('Window length must be positive and finite')
    results = []
    for row in rows:
        directory = repository / row['raw_evidence']
        manifest = json.loads((directory / 'manifest.json').read_text())
        start, end = manifest['producer_end_epoch'] - window_seconds, manifest['producer_end_epoch']
        confirmed = verified_recovery(row, start)
        print('Checking evidence and cohort:', row['run_id'], flush=True)
        evidence = {str(p.relative_to(directory)): sha256(p) for p in event_paths(directory)}
        if evidence != row['intervention_cost']['event_file_sha256']:
            raise ValueError('Event evidence differs from the published comparison: ' + row['run_id'])
        for name, expected in row['evidence_hashes'].items():
            if sha256(directory / name) != expected:
                raise ValueError('Evidence hash changed: ' + row['run_id'] + '/' + name)
        # A complete replay verifies the shared evaluator against the published
        # admission, completion and latency statistics before selecting the window.
        full = evaluate(directory)
        if full['validity_failures']:
            raise ValueError(full['validity_failures'])
        for key, expected in [('admitted_evaluation_cohort', row['admitted']),
                              ('completed_by_drain', row['completed']),
                              ('incomplete_by_drain', row['unfinished']),
                              ('admitted_cohort_completion_p99_seconds', row['p99_seconds']),
                              ('admitted_cohort_completion_mean_seconds', row['mean_completion_seconds'])]:
            if not math.isclose(full[key], expected, rel_tol=1e-12, abs_tol=1e-12):
                raise ValueError('Whole-run replay mismatch: ' + key)
        late = evaluate_cohort(directory, start, end)
        if late['validity_failures']:
            raise ValueError(late['validity_failures'])
        original_hashes_unchanged = all(sha256(directory / n) == h for n, h in row['evidence_hashes'].items())
        if not original_hashes_unchanged:
            raise ValueError('Original summaries changed during analysis')
        result = dict(arm=row['arm'], run_number=row['run_number'], run_id=row['run_id'],
                      cohort_start_epoch=start, cohort_end_epoch=end,
                      cohort_start_after_evaluation_seconds=start-manifest['evaluation_start_epoch'],
                      cohort_end_after_evaluation_seconds=end-manifest['evaluation_start_epoch'],
                      observation_cutoff_epoch=manifest['drain_end_epoch'],
                      recovery_confirmed_after_evaluation_seconds=confirmed-manifest['evaluation_start_epoch'],
                      whole_run_p99_seconds=full['admitted_cohort_completion_p99_seconds'],
                      late_cohort={k: late[k] for k in KEYS},
                      late_cohort_diagnostics=late['diagnostic_cohort_latencies'],
                      last_two_minutes_backlog=row['growth_windows']['last_two_minutes'],
                      deadline_threshold_seconds=float(manifest['config'].get('SLO_THRESHOLD_MS', 99))/1000,
                      event_file_sha256=evidence, original_evidence_sha256=row['evidence_hashes'],
                      validation=dict(whole_run_replay_matches=True, recovery_before_window=True,
                                      original_hashes_unchanged=original_hashes_unchanged,
                                      partition_validity_failures=late['partition_metrics']['validity_failures'],
                                      correctness=late['partition_metrics']['correctness']))
        results.append(result)
        print('  late cohort:', late['admitted_evaluation_cohort'], 'p99 seconds:',
              late['admitted_cohort_completion_p99_seconds'], flush=True)
    return dict(schema_version=1, comparison_sha256=sha256(comparison_path),
                analysis_script_sha256=sha256(__file__),
                analysis_source_revision=subprocess.check_output(['git', '-C', str(repository), 'rev-parse', 'HEAD'], text=True).strip(),
                window_seconds=window_seconds,
                method='Distinct acknowledged messages with producer time in [evaluation + 480 s, evaluation + 600 s); earliest valid completion by original drain cutoff; nearest-rank percentiles.',
                scope='Post-hoc common-window diagnostic, selected after observing recovery. It supplements, and does not replace, whole-run outcomes.',
                clock_note='Unadjusted cross-machine producer/completion timestamps, consistent with the original analysis. Existing clock-probe bounds do not establish millisecond synchronization; small differences require caution.',
                runs=results)


def report(data):
    lines = ['# Latency for messages produced after recovery', '',
             'This supplementary analysis compares redistribution within three consumers with scaling to six plus targeted redistribution. It uses the final two evaluation minutes (480–600 seconds), after confirmed recovery in all four trials. No new trials were run.', '',
             'The cohort contains distinct producer-acknowledged messages produced in that half-open window. Completion means the end of application processing before commit acknowledgment. The original drain cutoff is retained, so late completions and unfinished messages remain accountable. Percentiles use nearest rank over completed cohort messages; they are not averages of rolling or consumer percentiles.', '',
             '| Condition | Run | Messages | Mean (ms) | p50 (ms) | p95 (ms) | p99 (ms) | Unfinished (%) |',
             '|---|---:|---:|---:|---:|---:|---:|---:|']
    for r in data['runs']:
        c = r['late_cohort']
        metrics = ' | '.join(f"{c['admitted_cohort_completion_'+k+'_seconds']*1000:.2f}" for k in ('mean','p50','p95','p99'))
        lines.append(f"| {LABELS[r['arm']]} | {r['run_number']} | {c['admitted_evaluation_cohort']:,} | {metrics} | {100*c['incomplete_fraction']:.3f} |")
    lines += ['', '| Condition | Run | Whole-run p99 (s) | Recovery confirmed (s from evaluation start) | Late-cohort deadline misses (%) |',
              '|---|---:|---:|---:|---:|']
    for r in data['runs']:
        lines.append(f"| {LABELS[r['arm']]} | {r['run_number']} | {r['whole_run_p99_seconds']:.2f} | {r['recovery_confirmed_after_evaluation_seconds']:.1f} | {100*r['late_cohort']['deadline_miss_fraction']:.2f} |")
    threshold = data['runs'][0]['deadline_threshold_seconds']*1000
    lines += ['', f'The configured completion deadline is {threshold:g} ms. Unfinished work is counted separately from latency; deadline misses also retain the existing outcome definition.', '',
              '## Interpretation and limits', '',
              'The whole-run p99 includes waiting before and during the intervention. This late production cohort isolates records arriving after the recovery criterion was met; it cannot erase earlier intervention costs. Both methods are supplied with the same target input, so once backlog is low, higher available capacity does not by itself require higher sustained throughput.', '',
              'This window was selected after observing the results and is an exploratory diagnostic, not a prespecified primary endpoint. The comparison has two runs per method, shared-machine variability, and additional replicas whose placement was not frozen. The results do not isolate replica count from assignment, coordination, or machine effects, and finite low backlog is not proof of permanent stability.', '',
              data['clock_note'], '',
              '## Validation and reproduction', '',
              'Every event-file hash matched the existing comparison. Full-cohort replay reproduced the saved admitted/completed/unfinished counts, mean and p99. All selected runs had confirmed recovery before 480 seconds. The existing reconciler checked partition identities, offsets, duplicate completions and final process evidence. Original evidence and summary hashes remained unchanged.', '',
              'Run from the repository root:', '', '```bash',
              'python3 experiments/four-condition-20260929/analyze_post_recovery.py', '```', '',
              '[Machine-readable results and hashes](post-recovery-comparison.json) · [Whole-run comparison](README.md)', '']
    return '\n'.join(lines)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, default=ROOT/'experiment-records/four-condition-20260929')
    args = parser.parse_args()
    data = analyze(ROOT/'experiment-records/four-condition-20260929/comparison.json', ROOT)
    args.output.mkdir(parents=True, exist_ok=True)
    (args.output/'post-recovery-comparison.json').write_text(json.dumps(data, indent=2)+'\n')
    (args.output/'POST_RECOVERY.md').write_text(report(data))


if __name__ == '__main__':
    main()
