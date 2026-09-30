"""Supplement the prespecified four-hot-partition comparison with its late cohort."""
import argparse
import importlib.util
import json
import os
from pathlib import Path
import subprocess

ROOT=Path(os.environ.get('PIPELINE_REPOSITORY',Path(__file__).resolve().parents[2]))
SCRIPT=Path(os.environ.get('POST_RECOVERY_SCRIPT',ROOT/'experiments/four-condition-20260929/analyze_post_recovery.py'))
spec=importlib.util.spec_from_file_location('cohort_reconciliation',SCRIPT)
cohort=importlib.util.module_from_spec(spec);spec.loader.exec_module(cohort)
ARMS=('redistribute3','scale_redistribute6')
LABELS={'redistribute3':'Redistribute within 3','scale_redistribute6':'Scale to 6 + targeted redistribution'}


def recovery_before_window(row,start):
    matches=[r for r in row['intervention_cost']['recovery_from_decision']
             if r['threshold_offsets']==100 and r['hold_seconds']==30]
    if len(matches)!=1:raise ValueError('Expected the prespecified recovery criterion')
    confirmed=matches[0]['confirmed_epoch']
    return confirmed is not None and confirmed<=start


def hotspot_timing(lag,start,end):
    """Count observed detections; missing windows must not become zeros."""
    selected=[r for r in lag['snapshots'] if start<=r['timestamp']<end]
    defined=[r for r in selected if r.get('valid') and r.get('persistent_hot') is not None]
    detected=[r for r in defined if r['persistent_hot']]
    return dict(total_snapshots=len(selected),defined_snapshots=len(defined),
        nonempty_snapshots=len(detected),
        first_detection_after_start_seconds=detected[0]['timestamp']-start if detected else None,
        last_detection_after_start_seconds=detected[-1]['timestamp']-start if detected else None,
        maximum_count=max((len(r['persistent_hot']) for r in defined),default=None))


def analyze(comparison_path,repository=ROOT):
    comparison_path=Path(comparison_path);comparison=json.loads(comparison_path.read_text())
    protocol=comparison['cost_protocol']
    if (protocol.get('hot_partition_count')!=4 or protocol.get('completion_deadline_ms')!=1000 or
            protocol.get('completion_deadline_reporting_ms')!=[500,1000] or
            'Final-two-minute production-cohort latency with unfinished work' not in protocol.get('secondary_outcomes',[])):
        raise ValueError('This report requires the prespecified four-hot-partition protocol')
    rows=comparison['runs']
    if sorted((r['arm'],r['run_number']) for r in rows)!=sorted((a,n) for a in ARMS for n in (1,2)):
        raise ValueError('Both planned runs of each condition are required')
    results=[]
    for row in sorted(rows,key=lambda r:(ARMS.index(r['arm']),r['run_number'])):
        directory=Path(repository)/row['raw_evidence']
        manifest=json.loads((directory/'manifest.json').read_text())
        end=manifest['producer_end_epoch'];start=end-120
        if not 599.99<=end-manifest['evaluation_start_epoch']<=600.01:
            raise ValueError('The report expects ten-minute evaluation periods')
        for name,expected in row['evidence_hashes'].items():
            if cohort.sha256(directory/name)!=expected:raise ValueError('Summary hash changed: '+name)
        actual={str(p.relative_to(directory)):cohort.sha256(p) for p in cohort.event_paths(directory)}
        if actual!=row['intervention_cost']['event_file_sha256']:raise ValueError('Event hash changed')
        late=cohort.evaluate_cohort(directory,start,end,deadline_thresholds_ms=[500,1000])
        if late['validity_failures']:raise ValueError(late['validity_failures'])
        if any(cohort.sha256(directory/name)!=expected for name,expected in row['evidence_hashes'].items()):
            raise ValueError('Original summaries changed during analysis')
        result=dict(arm=row['arm'],run_number=row['run_number'],run_id=row['run_id'],
            cohort_start_epoch=start,cohort_end_epoch=end,observation_cutoff_epoch=manifest['drain_end_epoch'],
            recovery_confirmed_before_window=recovery_before_window(row,start),
            whole_run_p99_seconds=row['p99_seconds'],whole_run_unfinished_percent=row['unfinished_percent'],
            late_cohort={k:late[k] for k in ['admitted_evaluation_cohort','completed_by_drain','incomplete_by_drain',
                'incomplete_fraction','admitted_cohort_completion_p99_seconds','completion_deadline_outcomes']},
            persistent_hot_observations=dict(
                whole_evaluation=hotspot_timing(json.loads((directory/'lag-summary.json').read_text()),manifest['evaluation_start_epoch'],end),
                late_window=hotspot_timing(json.loads((directory/'lag-summary.json').read_text()),start,end)),
            source_summary_sha256=row['evidence_hashes'],event_sha256=actual)
        results.append(result)
        print(row['run_id'],'late-cohort p99',late['admitted_cohort_completion_p99_seconds'],flush=True)
    return dict(schema_version=1,comparison_sha256=cohort.sha256(comparison_path),
        analysis_script_sha256=cohort.sha256(__file__),
        source_revision=subprocess.check_output(['git','rev-parse','HEAD'],cwd=repository,text=True).strip(),
        scope='Prespecified supplementary production cohort, evaluation seconds 480–600, followed through the original drain cutoff. Whole-run results remain primary. Label as after recovery only when recovery was confirmed before 480 seconds.',
        clock_note='Latency uses unadjusted producer and consumer timestamps, consistent with the primary analysis. Clock synchronization does not establish a measured bound of zero error; small cross-machine latency differences require caution.',
        runs=results)


def number(value,scale=1):return 'Unavailable' if value is None else f'{value*scale:.3f}'


def report(data):
    lines=['# Prespecified late production cohort','',data['scope'],'',
        'Each latency describes distinct acknowledged messages produced inside the selected window, using the earliest valid completion before the original cutoff. The same cohort is used for both deadlines. Overdue unfinished records count as misses; censored deadlines and invalid clocks prevent a complete cohort miss fraction. Completion precedes commit acknowledgment.','',
        '| Condition | Run | Whole-run p99 (s) | Late-cohort p99 (s) | Late unfinished (%) | Above 0.5 s (%) | Above 1 s (%) | Recovered before window? |',
        '|---|---:|---:|---:|---:|---:|---:|---|']
    for r in data['runs']:
        late=r['late_cohort'];deadlines={x['threshold_ms']:x for x in late['completion_deadline_outcomes']}
        values=[number(r['whole_run_p99_seconds']),number(late['admitted_cohort_completion_p99_seconds']),
                number(late['incomplete_fraction'],100),number(deadlines[500]['deadline_miss_fraction'],100),
                number(deadlines[1000]['deadline_miss_fraction'],100)]
        lines.append('| '+LABELS[r['arm']]+' | '+str(r['run_number'])+' | '+' | '.join(values)+' | '+('Yes' if r['recovery_confirmed_before_window'] else 'No')+' |')
    lines+=['','The late cohort does not include messages produced earlier that were waiting during the intervention. Recovery means processing backlog at most 100 offsets for 30 consecutive valid seconds with stable ownership and nondecreasing offsets while input continues. It is a different outcome from p99 or a latency deadline. Two repetitions and shared-machine variability limit generalization.','',data.get('clock_note',''),'',
            '[Whole-run outcomes and monitoring](README.md) · [Late-cohort counts and evidence hashes](late-cohort-comparison.json)','']
    if all('persistent_hot_observations' in r for r in data['runs']):
        lines+=['## When persistent hotspots were observed','',
            'A zero count in the final two minutes describes only that period. It does not mean persistent hotspots were absent earlier. A partition qualifies when its lag is above 10 offsets and above the partition mean plus one population standard deviation, it qualifies in at least 12 of 15 contiguous observations, and it qualifies now. These exploratory diagnostic thresholds do not by themselves establish a severe or growing backlog.','',
            '| Condition | Run | First detection (evaluation s) | Last detection (evaluation s) | Whole evaluation: nonempty / defined snapshots | Minutes 8–10: nonempty / defined snapshots |',
            '|---|---:|---:|---:|---:|---:|']
        for r in data['runs']:
            whole=r['persistent_hot_observations']['whole_evaluation'];late=r['persistent_hot_observations']['late_window']
            lines.append(f"| {LABELS[r['arm']]} | {r['run_number']} | {number(whole['first_detection_after_start_seconds'])} | {number(whole['last_detection_after_start_seconds'])} | {whole['nonempty_snapshots']} / {whole['defined_snapshots']} | {late['nonempty_snapshots']} / {late['defined_snapshots']} |")
        lines+=['','First and last detections are observed endpoints, not a claim of continuous detection between them. Ownership changes, missing observations and offset resets restart the window. Undefined observations are excluded from the defined count and are never reported as zero; complete counts are retained in the JSON. Low absolute lag can still satisfy this relative rule, so read hotspot counts together with backlog magnitude, growth and latency.','']
    return '\n'.join(lines)


def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('comparison',type=Path)
    parser.add_argument('--output',type=Path)
    args=parser.parse_args();out=args.output or args.comparison.parent
    result=analyze(args.comparison);out.mkdir(parents=True,exist_ok=True)
    (out/'late-cohort-comparison.json').write_text(json.dumps(result,indent=2,allow_nan=False)+'\n')
    (out/'LATE_COHORT.md').write_text(report(result))


if __name__=='__main__':main()
