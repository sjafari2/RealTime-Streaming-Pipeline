"""Build comparable four-partition tables without pooling deployment blocks."""
import argparse
import hashlib
import json
import math
from pathlib import Path
import statistics

ROOT = Path(__file__).resolve().parents[2]
LABELS = {'keep3': 'Keep 3', 'redistribute3': 'Redistribute 3',
          'scale_redistribute6': 'Targeted scale 6', 'kafka_scale6': 'Kafka scale 6'}
COLORS = {700: 'CFE2F3', 600: 'FFE1AD'}


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def read(path):
    return json.loads(path.read_text())


def actual_resources(row):
    result = {}
    for metric, name, divisor in [('consumer_cpu_percent', 'actual_cpu_core_minutes', 6000),
                                  ('consumer_memory_bytes', 'actual_rss_gib_minutes', 2**30*60)]:
        windows = [x['windows']['evaluation_and_drain'] for x in row['process_resources'] if x['metric'] == metric]
        if not windows:
            raise ValueError('Missing process resource observations')
        result[name] = sum(x['observed_integral'] for x in windows)/divisor
        result[name + '_process_coverage'] = [x['covered_fraction'] for x in windows]
    requests = row['requested_resources_evaluation_and_drain']
    full = math.isclose(requests['covered_fraction'], 1, abs_tol=1e-6)
    result.update(requested_cpu_core_minutes=requests['consumer_container_requested_cpu_seconds_observed']/60 if full else None,
                  requested_memory_gib_minutes=requests['consumer_container_requested_gib_seconds_observed']/60 if full else None,
                  requested_coverage=requests['covered_fraction'])
    return result


def intervention(row):
    cost = row['intervention_cost']
    if cost['action'] == 'none':
        return dict(decision_to_active_seconds=None, global_gap_seconds=None, extra_pending_during_pause=None,
                    net_pending_during_transfer=None, recovery_minutes=None, recovery_status='not_applicable')
    recovery = next(x for x in cost['recovery_from_decision'] if x['threshold_offsets'] == 100 and x['hold_seconds'] == 30)
    seconds = recovery['confirmation_after_anchor_seconds']
    return dict(decision_to_active_seconds=cost['transition_end_epoch']-cost['decision_epoch'],
                global_gap_seconds=cost['longest_global_no_processing_interval']['seconds'],
                extra_pending_during_pause=cost.get('additional_unfinished_during_pause'),
                net_pending_during_transfer=cost['net_additional_unfinished'],
                recovery_minutes=seconds/60 if seconds is not None else None,
                recovery_status='observed' if seconds is not None else 'not_observed_during_evaluation')


def fmt(x, digits=2):
    return '---' if x is None else f'{x:,.{digits}f}'


def tex_table(rows, headers, cells, caption, label, font='small', spacing=3):
    lines = [r'\begin{table}[!htbp]', r'\centering' + chr(92) + font,
             r'\setlength{\tabcolsep}{' + str(spacing) + 'pt}', r'\renewcommand{\arraystretch}{1.12}',
             r'\begin{tabular}{@{}cl' + 'c'*(len(headers)-2) + r'@{}}',
             r'\toprule', r'\rowcolor[HTML]{DDE3EA}', ' & '.join(headers) + r'\\', r'\midrule']
    for row in rows:
        lines.extend([r'\rowcolor[HTML]{' + COLORS[row['rate']] + '}',
                      ' & '.join([str(row['rate']), LABELS[row['arm']], str(row['run_number'])] + cells(row)) + r'\\'])
    lines += [r'\bottomrule', r'\end{tabular}', r'\caption{' + caption + '}', r'\label{' + label + '}', r'\end{table}']
    return '\n'.join(lines) + '\n'


def tables(rows):
    grouped = sorted(rows, key=lambda r: ({'keep3':0,'redistribute3':1,'scale_redistribute6':2,'kafka_scale6':3}[r['arm']], r['rate'], r['run_number']))
    common = [r'\shortstack{Rate\\(msg/s)}', 'Response', 'Run']
    outcome = tex_table(grouped, common + [r'\shortstack{Min\\(s)}', r'\shortstack{Mean\\(s)}', r'\shortstack{Max\\(s)}', r'\shortstack{p99\\(s)}', r'\shortstack{Unfinished\\(\%)}', r'\shortstack{SLO violations\\1 s (\%)}', r'\shortstack{Throughput\\(msg/s)}'],
        lambda r: [fmt(r['minimum_completion_seconds'],3),fmt(r['mean_completion_seconds'],3),fmt(r['maximum_completion_seconds']),fmt(r['p99_seconds']),fmt(r['unfinished_percent']),fmt(r['slo_violation_percent']),fmt(r['useful_throughput'])],
        r'Four high-input partitions: completion outcomes. Blue rows: 700 messages/s; amber rows: 600 messages/s. Latency uses distinct evaluation messages completed by the drain cutoff, before commit acknowledgment. Unfinished work and one-second SLO violations use the full acknowledged evaluation cohort. Throughput counts distinct evaluation-time completions, including warm-up work. Rates belong to different deployment configurations, not additional repetitions of one configuration.',
        'tab:four-partition-outcomes',font='footnotesize',spacing=2.5)
    outcome = outcome.replace(r'\label{tab:four-partition-outcomes}',r'\label{tab:four-partition-outcomes}\label{tab:four-partition-latency-details}')
    def resource_cells(r):
        x=r['resources']; cov=x['actual_cpu_core_minutes_process_coverage']
        return [fmt(x['actual_cpu_core_minutes']),fmt(x['actual_rss_gib_minutes']),fmt(x['requested_cpu_core_minutes']),fmt(x['requested_memory_gib_minutes']), f'{100*min(cov):.1f}--{100*max(cov):.1f}']
    resources=tex_table(grouped,common+[r'\shortstack{Actual CPU\\(core-min)}',r'\shortstack{Actual RSS\\(GiB-min)}',r'\shortstack{Requested CPU\\(core-min)}',r'\shortstack{Requested memory\\(GiB-min)}',r'\shortstack{Process coverage\\range (\%)}'],resource_cells,
        r'Consumer resources over evaluation plus drain, with the same rate colors. Actual CPU and RSS integrate covered process observations and sum across consumers; they are distinct from requests and whole-cluster cost. Process coverage is the minimum--maximum coverage across consumers over the full twelve-minute interval, including time before additional consumers started. CPU and RSS coverage match within each trial. Request-history coverage is 100\% for all twelve trials.',
        'tab:four-partition-resources',font='footnotesize',spacing=2)
    lag=tex_table(rows,common+[r'\shortstack{Mean total\\backlog}',r'\shortstack{Max total\\backlog}',r'\shortstack{Max partition\\lag}',r'\shortstack{Mean\\skew}',r'\shortstack{Lag coverage\\(\%)}'],
        lambda r:[fmt(r['diagnostics']['mean_total_backlog'],0),fmt(r['diagnostics']['max_total_backlog'],0),fmt(r['diagnostics']['max_partition_lag'],0),fmt(r['diagnostics']['mean_skew']),fmt(r['diagnostics']['lag_coverage_percent'],1)],
        r'Backlog and skew during evaluation, with the same rate colors. Backlog and partition lag are in offsets. Means are equal-weight arithmetic averages of valid individual evaluation samples; skew averages the individual maximum-to-mean partition-lag ratios. Maxima are sampled peaks. Lag coverage measures valid continuous time; missing observations are excluded.',
        'tab:four-partition-diagnostics')
    def cost_cells(r):
        x=r['intervention'];rec=fmt(x['recovery_minutes'])
        if x['recovery_status']=='not_observed_during_evaluation':rec=r'Not observed'
        return [fmt(x['decision_to_active_seconds']),fmt(x['global_gap_seconds'],3),fmt(x['extra_pending_during_pause'],0),fmt(x['net_pending_during_transfer'],0),rec]
    cost=tex_table(rows,common+[r'\shortstack{Decision to\\active (s)}',r'\shortstack{Global gap\\(s)}',r'\shortstack{Extra pending\\during pause}',r'\shortstack{Net pending\\during transfer}',r'\shortstack{Recovery\\(min)}'],cost_cells,
        r'Intervention cost and recovery, with the same rate colors. Recovery is time from the decision until backlog stays at or below 100 offsets for 30 valid consecutive seconds during input. \emph{Not observed} means the criterion was not reached during evaluation. A dash denotes no action or no coordinated global pause. Global gap is the longest interval without any consumer processing during the transition. Native activation requires a stable six-consumer map for ten seconds; targeted activation uses the explicit handoff acknowledgment. Negative net pending means fewer messages pending at transfer end. These boundaries differ by mechanism; activation is not recovery.',
        'tab:four-partition-intervention-cost',font='footnotesize',spacing=3)
    def window_cells(r):
        w=r['completion_windows'];out=[]
        for key in ['before','post_action_to_evaluation_end','evaluation_end']:
            out.extend([fmt(w[key]['completion_throughput_per_second'],1),fmt(w[key]['completion_window_p99_seconds'],3)])
        return out+[fmt(w['evaluation_end']['acknowledged_evaluation_messages_pending_at_end'],0)]
    windows=tex_table(rows,common+[r'\shortstack{Before\\msg/s}',r'\shortstack{Before\\p99 (s)}',r'\shortstack{Active--end\\msg/s}',r'\shortstack{Active--end\\p99 (s)}',r'\shortstack{Eval. end\\msg/s}',r'\shortstack{Eval. end\\p99 (s)}',r'\shortstack{Pending\\at 600 s}'],window_cells,
        r'Completion-window performance, with the same rate colors. Before is evaluation seconds 30--60; evaluation end is 570--600. Active--end starts at recorded activation and ends at second 600; Keep 3 starts at second 60. Throughput includes warm-up completions; p99 includes only evaluation messages completed within the stated window. Pending counts are acknowledged evaluation messages not yet completed at second 600. No window includes drain. A small window p99 may exclude slow messages still queued and must be read alongside pending work and whole-cohort outcomes.',
        'tab:four-partition-rate-windows',font='footnotesize',spacing=2)
    return [outcome,resources,lag,cost,windows]



def percent_benefit(reference, value, higher_is_better=False):
    """Return relative improvement; a zero reference has no defined percentage."""
    if reference is None or value is None:
        return None
    if not math.isfinite(reference) or not math.isfinite(value) or min(reference,value)<0:
        raise ValueError('Percentage comparisons require finite, nonnegative measurements')
    if reference == 0:
        return None
    return 100*(value-reference)/reference if higher_is_better else 100*(reference-value)/reference


def percentage_comparisons(rows):
    lookup={(r['rate'],r['arm'],r['run_number']):r for r in rows}
    def compare(reference,value):
        measures={
            'throughput_increase_percent':(reference['useful_throughput'],value['useful_throughput'],True),
            'mean_total_backlog_reduction_percent':(reference['diagnostics']['mean_total_backlog'],value['diagnostics']['mean_total_backlog'],False),
            'p99_reduction_percent':(reference['p99_seconds'],value['p99_seconds'],False),
            'slo_violation_rate_reduction_percent':(reference['slo_violation_percent'],value['slo_violation_percent'],False),
            'requested_cpu_reduction_percent':(reference['resources']['requested_cpu_core_minutes'],value['resources']['requested_cpu_core_minutes'],False),
            'requested_memory_reduction_percent':(reference['resources']['requested_memory_gib_minutes'],value['resources']['requested_memory_gib_minutes'],False)}
        return dict(rate=value['rate'],arm=value['arm'],run_number=value['run_number'],reference_run_id=reference['run_id'],compared_run_id=value['run_id'],
                    reference_values={k:v[0] for k,v in measures.items()},compared_values={k:v[1] for k,v in measures.items()},
                    changes={k:percent_benefit(*v) for k,v in measures.items()})
    within=[compare(lookup[600,'keep3',n],lookup[600,arm,n]) for arm in ['redistribute3','scale_redistribute6','kafka_scale6'] for n in [1,2]]
    across=[compare(lookup[700,arm,n],lookup[600,arm,n]) for arm in ['redistribute3','scale_redistribute6'] for n in [1,2]]
    return dict(within_600_versus_keep3=within,configuration_600_relative_to_700=across)


def percentage_tables(changes):
    headers=[r'\shortstack{Rate\\(msg/s)}','Response','Run',r'\shortstack{Throughput\\increase}',r'\shortstack{Mean backlog\\reduction}',r'\shortstack{p99\\reduction}',r'\shortstack{SLO rate\\reduction}',r'\shortstack{CPU request\\reduction}',r'\shortstack{Memory request\\reduction}']
    cells=lambda r:[fmt(v,1) for v in r['changes'].values()]
    formula=r'All entries are percentages calculated from unrounded measurements, separately for each run. Throughput increase is $100(T-T_0)/T_0$; reductions are $100(X_0-X)/X_0$. Positive values indicate improvement in the named metric; negative values indicate deterioration or higher requests. Backlog is the arithmetic mean of valid total-backlog samples during evaluation, not a sum over samples. SLO rate uses the one-second deadline and full acknowledged evaluation cohort; its reduction is relative percent, not percentage points. Cost uses requested consumer CPU and memory integrated over evaluation plus drain.'
    first=tex_table(changes['within_600_versus_keep3'],headers,cells,
        r'Relative changes at 600 messages/s versus Keep 3 with the same run number. '+formula+r' All interventions completed the evaluation cohort; Keep 3 left 26.92\% and 37.57\% unfinished, so completed-message p99 must be interpreted with these outcomes. Two runs do not establish statistical significance.',
        'tab:four-partition-relative-improvement',font='footnotesize',spacing=2)
    second=tex_table(changes['configuration_600_relative_to_700'],headers,cells,
        r'Configuration differences: 600 relative to 700 messages/s for the same response and run number, with the same percentage formulas and sign convention as Table~\ref{tab:four-partition-relative-improvement}. The 700 value is the reference. These are not isolated effects of lowering the input rate: client resources, placement and seed initialization also changed, and offered work is lower. There is no four-partition Keep 3 or native Kafka scaling baseline at 700 messages/s.',
        'tab:four-partition-rate-differences',font='footnotesize',spacing=2)
    return first+'\n'+second


def percentage_markdown(changes):
    out=['','## Relative changes','',
         'Throughput increase = 100 × (response − reference) / reference. Each reduction = 100 × (reference − response) / reference. Positive is favorable for the named metric; negative is unfavorable. A zero or unavailable reference is not assigned a percentage. Values are calculated before rounding, without averaging run percentiles.','',
         'Backlog means the arithmetic mean of valid evaluation total-backlog samples. SLO is the full-cohort one-second violation rate; relative reduction differs from percentage-point decrease. Cost is requested consumer CPU and memory over evaluation plus drain, not actual use or a monetary bill.']
    for key,title in [('within_600_versus_keep3','600 messages/s: each intervention relative to Keep 3 with the same run number'),('configuration_600_relative_to_700','600 configuration relative to 700 for the same response and run number')]:
        out+=['','### '+title,'','| Response | Run | Throughput increase (%) | Mean backlog reduction (%) | p99 reduction (%) | SLO-rate reduction (%) | CPU-request reduction (%) | Memory-request reduction (%) |','|---|---:|---:|---:|---:|---:|---:|---:|']
        for r in changes[key]:out.append('| '+' | '.join([LABELS[r['arm']],str(r['run_number'])]+[fmt(v,1) for v in r['changes'].values()])+' |')
    out+=['','The cross-configuration differences do not isolate the effect of input rate. Client resources, node placement and seed initialization changed, and 600 messages/s offers less work. There is no historical Keep 3 or native Kafka scaling baseline for these four-partition 700 trials. Two runs do not establish statistical significance.','']
    return '\n'.join(out)

def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--repository',type=Path,default=ROOT)
    p.add_argument('--current-raw-root',type=Path)
    p.add_argument('--historical-windows',type=Path,required=True)
    p.add_argument('--current-comparison',type=Path,required=True)
    p.add_argument('--output',type=Path,required=True)
    a=p.parse_args();root=a.repository;raw=a.current_raw_root or root
    historical_path=root/'experiment-records/four-hot-partitions-20260930/comparison.json'
    audit_path=root/'experiment-records/result-metric-audit-20260930/metrics.json'
    window_path=root/'experiment-records/four-hot600-20261021/completion-windows.json'
    outcome_path=root/'experiment-records/four-hot600-20261021/results-summary.json'
    h=read(historical_path);c=read(a.current_comparison);audit={x['run_id']:x for x in read(audit_path)['groups']['four-partition']}
    hw=read(a.historical_windows);cw=read(window_path)
    if hw['comparison_sha256']!=sha(historical_path) or cw['comparison_sha256']!=sha(a.current_comparison):
        raise ValueError('Completion windows refer to a different comparison')
    windows={x['run_id']:x for d in [hw,cw] for x in d['runs']}
    new_outcomes={x['run_id']:x for x in read(outcome_path)['runs']}
    rows=[];sources={str(path):sha(path) for path in [historical_path,a.current_comparison,audit_path,a.historical_windows,window_path,outcome_path]}
    for rate,data in [(700,h),(600,c)]:
        assert len(data['hot_partitions'])==4
        for old in data['runs']:
            w=windows[old['run_id']]
            assert w['whole_run_replay_matches'] and old['admitted']==rate*600
            assert old['completed']+old['unfinished']==old['admitted']
            if rate==700:
                prior=audit[old['run_id']];s=prior['statistics']
                extrema=[w['whole_run_completion_min_seconds'],w['whole_run_completion_max_seconds']]
                diagnostics=dict(mean_total_backlog=s['total_lag']['sample_mean'],max_total_backlog=s['total_lag']['sampled_peak'],max_partition_lag=s['max_partition_lag']['sampled_peak'],mean_skew=s['skew']['sample_mean'],lag_coverage_percent=100*s['total_lag']['coverage_fraction'])
            else:
                o=new_outcomes[old['run_id']];extrema=[o['minimum_completion_seconds'],o['maximum_completion_seconds']]
                directory=raw/old['raw_evidence'];lag_path=directory/'lag-summary.json';manifest_path=directory/'manifest.json'
                for path in [lag_path,manifest_path]:
                    assert sha(path)==old['evidence_hashes'][path.name],path
                    sources[f'raw/{old["run_id"]}/{path.name}']=sha(path)
                lag=read(lag_path);manifest=read(manifest_path)
                points=[x for x in lag['snapshots'] if x.get('valid') and manifest['evaluation_start_epoch']<=x['timestamp']<=manifest['producer_end_epoch']]
                mean=statistics.mean(x['total_lag'] for x in points)
                assert math.isclose(mean,old['mean_lag'],abs_tol=1e-9)
                diagnostics=dict(mean_total_backlog=mean,max_total_backlog=max(x['total_lag'] for x in points),max_partition_lag=max(x['max_partition_lag'] for x in points),mean_skew=statistics.mean(x['skew'] for x in points if x.get('skew') is not None),lag_coverage_percent=100*old['lag_coverage'])
            resources=actual_resources(old)
            if rate==700:
                assert math.isclose(resources['actual_cpu_core_minutes'],prior['resources']['actual_cpu_core_seconds']/60,abs_tol=1e-10)
                assert math.isclose(resources['actual_rss_gib_minutes'],prior['resources']['actual_rss_gib_minutes'],abs_tol=1e-10)
            assert resources['actual_cpu_core_minutes_process_coverage']==resources['actual_rss_gib_minutes_process_coverage']
            slo=next(x for x in old['completion_deadline_outcomes'] if x['threshold_ms']==1000)
            row={k:old[k] for k in ['run_id','arm','run_number','admitted','completed','unfinished','unfinished_percent','p99_seconds','mean_completion_seconds','useful_throughput']}
            row.update(rate=rate,minimum_completion_seconds=extrema[0],maximum_completion_seconds=extrema[1],slo_violation_percent=100*slo['deadline_miss_fraction'],resources=resources,diagnostics=diagnostics,intervention=intervention(old),completion_windows=w['windows'])
            assert row['minimum_completion_seconds']<=row['mean_completion_seconds']<=row['maximum_completion_seconds'] and row['p99_seconds']<=row['maximum_completion_seconds']
            rows.append(row)
    assert len(rows)==12 and len({x['run_id'] for x in rows})==12
    order = {'keep3': 0, 'redistribute3': 1, 'scale_redistribute6': 2, 'kafka_scale6': 3}
    rows.sort(key=lambda row: (order[row['arm']], row['rate'], row['run_number']))
    a.output.mkdir(parents=True,exist_ok=True)
    # Keep source references portable when publishing the compact record.
    portable={str(Path(k).relative_to(root)) if k.startswith(str(root)+'/') else ('historical-completion-windows.json' if k==str(a.historical_windows) else k):v for k,v in sources.items()}
    changes=percentage_comparisons(rows)
    record=dict(percentage_changes=changes,definition='Twelve distinct trials shown together, not pooled. Historical 700 and revised 600 messages/s are different deployment configurations.',source_sha256=portable,runs=rows)
    (a.output/'comparison.json').write_text(json.dumps(record,indent=2,allow_nan=False)+'\n')
    table_text=tables(rows)
    (a.output/'tables.tex').write_text('\n'.join(table_text[:2])+r'\clearpage'+'\n'+'\n'.join(table_text[2:4])+r'\clearpage'+'\n'+table_text[4]+r'\clearpage'+'\n'+percentage_tables(changes))
    lines=['# Four high-input partitions at two aggregate rates','',record['definition'],'','| Rate (msg/s) | Response | Run | Min (s) | Mean (s) | Max (s) | p99 (s) | Unfinished (%) | SLO violations (%) |','|---:|---|---:|---:|---:|---:|---:|---:|---:|']
    for row in rows:lines.append('| '+' | '.join([str(row['rate']),LABELS[row['arm']],str(row['run_number'])]+[fmt(row[k],3 if k=='minimum_completion_seconds' else 2) for k in ['minimum_completion_seconds','mean_completion_seconds','maximum_completion_seconds','p99_seconds','unfinished_percent','slo_violation_percent']])+' |')
    lines+=['','The [combined tables](tables.tex) include all twelve trials in outcomes, observed/requested resources, backlog/skew, intervention cost and completion-window performance. Blue denotes 700 messages/s and amber denotes 600 messages/s. The rate is also printed on every row. Missing interventions are not invented for the historical block.','',
            'The newer deployment used different client resource settings, producer-consumer placement and producer seed initialization. Compare responses within each deployment; differences across rates cannot be attributed to rate alone. The separate 600 messages/s proposal subsection repeats eight of these trials for detail, so the total remains 32 reported trials.','',
            'Latency follows distinct acknowledged evaluation messages through the drain cutoff; completion precedes commit acknowledgment. Window p99 uses only evaluation messages completed within each window. Throughput includes warm-up work completed during evaluation. Means of backlog and skew are arithmetic means of valid individual samples, matching the existing four-partition table. Resource integrals cover evaluation plus drain. Missing observations are not zero.','',
            'Historical message records were replayed to obtain the previously omitted minimum/maximum latency and completion-window values, with original counts, whole-run p99 and throughput verified. The [historical completion-window record](historical-completion-windows.json) and [combined numerical record](comparison.json) retain measurement definitions and source hashes.','']
    (a.output/'README.md').write_text('\n'.join(lines)+percentage_markdown(changes)+'\n## Reproduce the tables\n\nThe historical replay reads the separately retained raw message evidence and verifies its recorded hashes. The comparison builder reads the existing campaign analysis and checks the newer manifest and lag-summary hashes. The full newer analysis remains in local results storage; the committed records preserve the values used here and their hashes.\n\n```bash\npython3 experiments/four-condition-20260929/analyze_action_windows.py \\\n  experiment-records/four-hot-partitions-20260930/comparison.json \\\n  --output results/four-hot700-window-replay\npython3 experiments/four-hot600-20261021/compare_rates.py \\\n  --historical-windows results/four-hot700-window-replay/action-window-performance.json \\\n  --current-comparison results/four-hot600-20261021-analysis/comparison.json \\\n  --output results/four-hot-rate-comparison\n```\n\nUse `--current-raw-root` if the newer raw run directories are stored outside the repository. The published historical window file omits only the long rolling series; it retains interval metrics, replay checks, measurement definitions and source hashes.\n')
    print('Built comparable tables for',len(rows),'distinct trials')


if __name__=='__main__':
    main()
