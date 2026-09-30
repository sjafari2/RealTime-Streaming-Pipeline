"""Build reviewable results text, metric coverage, and a standalone PDF."""
import argparse
import hashlib
import json
import re
from pathlib import Path
import shutil
import sys

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

LABELS = {'keep3': 'Keep 3', 'redistribute3': 'Redistribute 3', 'scale_redistribute6': 'Targeted scale 6',
          'scale6': 'Targeted scale 6', 'kafka_scale6': 'Kafka scale 6'}
BLOCKS = [('twelve-partition','Twelve high-input partitions: ten-minute evaluation'),
          ('four-partition','Four high-input partitions: ten-minute evaluation'),
          ('short-targeted-scaling','Five-minute evaluation: targeted scaling'),
          ('short-redistribution','Five-minute evaluation: redistribution')]
SOURCES = {'twelve-partition':'four-condition-20260929','four-partition':'four-hot-partitions-20260930'}
PLOT_CAPTIONS = {
 'lag-comparison': 'Total consumer-position lag during evaluation. The scheduled action is at minute one. Gaps are retained.',
 'p99-unfinished': 'Completion p99 and unfinished evaluation messages at the fixed drain cutoff. These two outcomes must be read together.',
 'cpu': 'Observed consumer-process CPU during evaluation, in cores; this is actual process use, not requested CPU.',
 'memory': 'Observed consumer-process resident memory during evaluation. RSS is distinct from requested memory and transferred data.',
 'throughput': 'Acknowledgment and completion rates during evaluation. Completion-time membership includes warm-up messages completed during evaluation.',
 'growth': 'Processing-backlog growth over ten seconds: five intervals and six observations on the two-second grid. Gaps and owner changes reset the window.',
 'outstanding': 'Distinct acknowledged but unfinished messages during evaluation, including warm-up messages. This identity-based count remains separate from offset-span lag.',
 'ownership': 'Observed partition ownership. High-input partition counts describe the configured traffic distribution, not the persistent-hot detector.'}
COVERAGE = [
 ('Partition lag and total lag', 'Main lag plots, diagnostic figure and lag-summary tables; older five-minute counterparts in the appendix.', 'Offsets, not guaranteed record counts.'),
 ('Processing backlog and growth', 'Main backlog-growth plots and acknowledged-unfinished plots; growth is also retained for consumer-position lag.', 'Completion frontier and returned-record position have different definitions.'),
 ('Maximum and mean partition lag', 'Bottom row of each diagnostic figure and sampled-peak tables.', 'Maximum and mean are taken across partitions at each valid observation.'),
 ('Lag skew and window-mean skew', 'Runtime-state row shows window-mean skew; the summary table and separate diagnostic plot retain raw skew.', 'High relative skew can coexist with small absolute lag.'),
 ('Hot set, persistence score and persistent-hot set', 'Top diagnostic row, detector settings and interpretation of early versus late observations.', 'Exploratory diagnostic, not a calibrated controller trigger.'),
 ('Window-mean backlog and runtime state', 'Runtime-state figure and timestamped CSV/JSON, including persistent-hot partition IDs.', 'Reconstructed from retained observations; no adaptive controller was evaluated.'),
 ('Completion mean, p50, p95 and p99', 'Outcome and latency-detail tables for each block.', 'Each quantile uses completed evaluation-born messages, not averaged rolling quantiles.'),
 ('Processing-start latency and processing duration', 'Latency-detail tables and separation of queued delay from synthetic work.', 'Processing duration excludes evidence writing and commit bookkeeping.'),
 ('Unfinished work and completion deadlines', 'Unfinished percentage and 1-second cohort deadline misses in main tables; 0.5-second sensitivity in appendix.', 'Overdue unfinished messages count as misses; they have no fabricated completion latency.'),
 ('Observed-completion violation rate', 'Separate latency-detail column at 1 second.', 'Completion-attempt population during evaluation differs from the admitted cohort.'),
 ('Input, useful throughput and attempt throughput', 'Main throughput plots and outcome tables; admitted rate and replay checks in text.', 'Attempts and distinct completions agree only when no duplicate completions occur.'),
 ('Actual CPU and resident memory', 'Main process traces and observed resource integrals.', 'Consumer processes only; no full-cluster energy or monetary-cost claim.'),
 ('Requested CPU and memory cost', 'Main resource tables, integrated over evaluation plus drain; detailed resource-cost figures retained with records.', 'Partial request coverage is unavailable for full-run comparison, not zero.'),
 ('Interruption, transition backlog and recovery', 'Main intervention-cost tables, with operational endpoints identified.', 'Recovery is first threshold confirmation while input continues; not permanent stability.'),
 ('Correctness and measurement quality', 'Identity reconciliation, handover validation, clock limitations and explicit coverage discussion.', 'Synthetic records do not prove exactly-once business effects at a durable external sink.'),
 ('Controller overhead, splitting and state-transfer cost', 'Explicitly identified as unevaluated future measurements.', 'No adaptive controller, hot-key splitting or stateful transfer was evaluated in these trials.')]


def fmt(x, digits=2): return 'N/A' if x is None else f'{x:,.{digits}f}'


def tex_escape(s):
    return s.replace('&', r'\&').replace('%', r'\%').replace('_', r'\_')


def table(headers, rows, caption, label, spec=None):
    spec = spec or ('l'+'r'*(len(headers)-1))
    text = '\n'.join([r'\begin{table}[!htbp]', r'\centering\small',r'\setlength{\tabcolsep}{3pt}',
         r'\renewcommand{\arraystretch}{1.12}',r'\begin{tabular}{@{}'+spec+r'@{}}',r'\toprule',
         ' & '.join(headers)+r'\\',r'\midrule'])+'\n'
    text += '\n'.join(' & '.join(str(x) for x in row)+r'\\' for row in rows)
    return text+'\n'+ '\n'.join([r'\bottomrule',r'\end{tabular}',r'\caption{'+caption+'}',r'\label{'+label+'}',r'\end{table}'])+'\n'


def fig(name, caption, label):
    return '\n'.join([r'\begin{figure}[p]',r'\centering',
        r'\includegraphics[width=\linewidth,height=.79\textheight,keepaspectratio]{figures/'+name+'.pdf}',
        r'\caption{'+caption+'}',r'\label{'+label+'}',r'\end{figure}',r'\clearpage'])+'\n'


def compact_four_partition_layout(source):
    """Keep the small intervention tables beside their related figures."""
    tables = re.findall(r'\\begin\{table\}.*?\\end\{table\}\n', source, re.S)
    figures = re.findall(r'\\begin\{figure\}.*?\\end\{figure\}\n\\clearpage\n', source, re.S)
    if len(tables) != 7 or len(figures) != 3:
        raise ValueError('Expected seven tables and three four-partition figures')
    costs, owners, activation = tables[4:]
    costs = re.sub(r'\\caption\{.*?\}\n', lambda _: r'\caption{Intervention costs. Decision-to-active ends at verified ownership; transfer spans release request to verified ownership. Global gap is the longest interval without any consumer processing. Extra pending work uses that pause; net pending change uses the transfer interval. Recovery is measured from the decision to its confirmation.}'+'\n', costs)
    activation = re.sub(r'\\caption\{.*?\}\n', lambda _: r'\caption{Seconds from replica request to the first valid completion on each added consumer. These intervals include startup and handover; they are not pure rebalance delays or whole-action completion times.}'+'\n', activation)
    # The reference event is identical in both rows and remains in the caption.
    activation = activation.replace('lrrrrr@{}', 'lrrrr@{}').replace(' & Reference event', '').replace(' & Replica request', '')
    def beside_table(text):
        body = re.sub(r'\\begin\{table\}\[.*?\]\n|\\end\{table\}\n', '', text)
        return r'\begin{minipage}[t]{.48\linewidth}\vspace{0pt}'+'\n'+body+r'\end{minipage}'+'\n'
    ownership_details = r'\begin{table}[!htbp]'+'\n'+beside_table(owners)+r'\hfill'+'\n'+beside_table(activation)+r'\end{table}'+'\n'
    # Float-page stretch otherwise creates large gaps between small tables.
    begin = r'''\clearpage
\begingroup
\makeatletter
\setlength{\@fptop}{0pt}
\setlength{\@fpsep}{12pt}
\setlength{\@fpbot}{0pt plus 1fil}
\makeatother
'''
    outcome = figures[0].replace('[p]', '[!htbp]').replace('.79\\textheight', '.58\\textheight')
    performance = figures[1].replace('[p]', '[!htbp]').replace('.79\\textheight', '.60\\textheight')
    prefix = source[:source.index(tables[0])] + ''.join(tables[:4])
    return prefix + begin + costs + outcome + ownership_details + performance + figures[2] + '\\endgroup\n'


COMMON = r"""These comparisons report each run separately. Messages generated during evaluation form a distinct broker-acknowledged cohort followed through the original drain cutoff. Completion is the end of application processing before offset-commit acknowledgment. Warm-up messages are excluded from cohort latency and deadline outcomes but can remain queued and can contribute to completion throughput during evaluation. Every ten-minute evaluation is preceded by one minute of warm-up and followed by two minutes of drain: thirteen minutes in total. The earlier five-minute evaluations last eight minutes in total and are reported in the appendix.

The main deadline is one second; the half-second calculation is retained as sensitivity analysis in the appendix. Both were specified before the four-partition trials. Their application to older trials is retrospective; the original 99-ms outcomes remain in the records. These are experimental completion deadlines, not a validated application SLA, and no acceptable violation percentage has yet been defined.

Monitoring summaries and time-series figures cover evaluation, while requested-resource and observed-process integrals cover evaluation plus drain. Actual CPU use and resident memory are distinct from Kubernetes resource requests. Invalid measurements, ownership changes and offset discontinuities are excluded from continuous intervals; no missing period is filled with zero. Individual runs are retained because two repetitions do not establish a general effect or a confidence interval.

The cohort replay retained the original admission, completion and unfinished counts and p99 values. New handover validations passed, and no duplicate completion attempts were found in the ten-minute blocks. These checks support record-level accounting in the synthetic application; they do not establish exactly-once business effects at an external durable sink. Latency uses producer and consumer wall-clock timestamps, while application-processing duration uses a local monotonic timer. Clock synchronization does not imply zero timing uncertainty.
"""
TWELVE = r"""The twelve-partition comparison used 700 aggregate messages/s over 60 partitions, with 80\% of traffic directed to twelve partitions initially owned by Consumer 2. Eight trials compared retaining the same three-consumer assignment, redistribution within three, scaling to six with targeted redistribution, and scaling to six with Kafka's normal cooperative rebalance. Each condition was run twice, reversing condition order in the second repetition. Starting ownership and the original consumers' pod identities, nodes and resource declarations were checked before traffic.

Every trial admitted 420,000 evaluation messages. Keeping the initial assignment left 34.64--34.83\% unfinished at cutoff; all interventions completed the cohort. Redistribution within three produced p99 latency of 80.10--84.16 seconds, compared with 100.81--107.24 seconds for targeted scaling and 71.41--72.46 seconds for normal Kafka scaling. The one-second cohort deadline-miss rates reveal a different aspect of the latency distribution: 30.50--31.84\% for redistribution, 26.26--27.33\% for targeted scaling and 21.05--25.83\% for normal scaling, versus 83.32--83.36\% without intervention. Thus a higher p99 does not imply more misses at every deadline.

The resource measurements show the trade-off. Redistribution used approximately 1,035--1,038 observed consumer CPU core-seconds over evaluation plus drain; targeted scaling used 1,310--1,327 and normal scaling 1,452--1,459. Requested CPU was 72 core-minutes for redistribution and approximately 137 for either six-consumer response. Actual RSS integrals were 2.53--2.65, 3.95--4.15 and 3.66--3.69 GiB-minutes respectively. These values concern consumer processes and resource declarations, not total infrastructure cost. The first unchanged baseline lacks almost all local request history and cannot support a full-window request-cost comparison; its message outcomes and recovered historical process measurements remain usable within their stated coverage.

Targeted scaling confirmed recovery sooner than redistribution within three (169.0--184.9 versus 305.2--328.9 seconds after the action decision), despite its higher whole-cohort p99. Recovery requires processing backlog at most 100 offsets for thirty consecutive valid seconds while input continues. Normal scaling's recovery varied from 143.0 to 259.0 seconds. Explicit ownership changes use a global release/acquire/resume barrier; their 14.40--14.54-second and 19.05--19.50-second processing pauses are distinguished from complete action duration. Normal cooperative scaling had much shorter global gaps, which does not imply every partition processed continuously.

Persistent hotspots were present before intervention. In the redistribution and targeted-scaling runs, none remained at any defined snapshot in the final two evaluation minutes. The diagnostic figure also shows that relative skew may rise during successful backlog clearance: the maximum and mean absolute partition lags are then small. Persistence and skew are useful descriptions of concentration, but neither alone establishes overload.
"""
FOUR = r"""Four further trials retained the same aggregate rate of 700 messages/s, which had shown bounded lag under balanced input during calibration, but directed 80\% of input to four of the sixty partitions. All four initially belonged to Consumer 2. Two trials redistributed within three consumers and two scaled to six with targeted redistribution. With three consumers, the target hot-partition counts were 2--1--1; with six they were 1--1--1--1--0--0. Matching initial ownership and original consumer placement was verified, and the second repetition reversed condition order. These assignments concern high-input partitions; they do not measure service capacity or implement a capacity-optimal assignment.

All four trials admitted and completed 420,000 evaluation messages by the fixed cutoff. Redistribution within three had lower p99 in both runs (84.50 and 79.28 seconds) than targeted scaling (110.19 and 112.94 seconds), and fewer one-second deadline misses (26.63\% and 25.82\%, versus 35.23\% and 34.72\%). Its mean valid total lag was approximately 6,994--7,296 offsets, compared with 12,683--12,740 for targeted scaling. The sampled peaks were 50,157--50,879 and 66,598--70,217 offsets respectively. Both useful-throughput values and the full time traces are retained so that average completion rate does not hide the transition.

Redistribution used 72 requested CPU core-minutes and 36 requested memory GiB-minutes, whereas targeted scaling used approximately 136.84 core-minutes and 68.42 GiB-minutes over evaluation plus drain. Observed consumer CPU use was approximately 1,007--1,019 versus 1,301--1,302 core-seconds; observed RSS integrals were 2.72--2.75 versus 4.01--4.09 GiB-minutes. Additional requested capacity therefore had both a measured utilization footprint and an allocation cost in these trials.

Redistribution confirmed recovery 173.45--180.85 seconds after the decision, compared with 266.80--290.54 seconds for targeted scaling. Decision-to-active intervals were 21.92--27.26 versus 78.52--81.26 seconds, while processing pauses were 12.31--15.08 versus 19.88--21.73 seconds. Net additional unfinished work during release-to-active handover and additional work accumulated during the processing pause are reported separately because their endpoints differ. These observations support the sufficiency of redistribution within three for the tested workload; they do not establish that scaling cannot help under other conditions.

The prespecified final-two-minute production cohort completed in every trial. Its p99 was 0.187 and 0.349 seconds for redistribution, versus 2.714 and 0.481 seconds for targeted scaling. The first targeted-scaling run experienced a later backlog spike after satisfying the recovery criterion. First recovery confirmation therefore does not prove continuously low backlog thereafter. The earlier twelve-partition late window was selected retrospectively and is not interchangeable with this prespecified analysis.

Unlike the twelve-partition block, small persistent-hot sets sometimes remained in the late four-partition observations. For example, redistribution Run 1 classified a partition with 11 offsets of lag as persistently hot when total lag was only 42. A relative hotspot can therefore persist in a pipeline with low absolute backlog. This motivates interpreting persistence jointly with absolute lag, growth and completion outcomes.
"""
DIAGNOSTIC = r"""The combined diagnostic figures show persistent-hot counts, lag skew and maximum/mean partition lag for each run. A partition is hot when its lag is greater than 10 offsets and greater than the current mean plus one population standard deviation. It is persistently hot when it is hot now and was hot in at least twelve of fifteen contiguous valid observations. Fifteen points on the two-second grid span twenty-eight seconds between endpoints. Invalid observations, gaps greater than three seconds, ownership changes and decreasing offsets restart the history. Undefined detector values remain missing rather than being shown as zero. These are exploratory diagnostic settings; the experiments use scheduled actions and do not validate adaptive trigger thresholds.

The runtime-state panels reconstruct $\mathcal{X}(t)=(\overline{B}_W(t),\overline{G}_W(t),\overline{S}_W(t),H_{\mathrm{persistent}}(t))$ from the recorded observations. Backlog and skew average fifteen samples, spanning 28 seconds; growth averages fifteen two-second intervals, spanning 30 seconds and requiring sixteen samples. These are the sample and interval conventions in the Metrics formulas. The fourth component is a set: the figure shows its size, while the supporting CSV and JSON retain the exact partition identifiers at each timestamp. A missing set is different from an observed empty set. These states are reconstructed offline; no adaptive controller using them was evaluated. The performance figure separately uses a ten-second processing-backlog growth window (five intervals and six observations). Shortening that plot window does not change the runtime-state or persistence window.

The measured processing backlog uses the contiguous completion frontier, whereas consumer-position lag uses the position after records have been returned to the application. Their equality at the retained valid snapshots in these two ten-minute blocks does not make the definitions interchangeable, particularly for future batched or concurrent processing. The acknowledged-unfinished curves use message identities and include warm-up work, so they are not the evaluation-cohort unfinished percentage.
"""
SHORT = r"""These two earlier blocks each used a five-minute evaluation, one-minute warm-up and two-minute drain, with 210,000 admitted evaluation messages per trial. Each block contains its own unchanged-assignment baseline and its intervention, each run twice. They remain separate comparisons rather than being pooled into the later ten-minute experiments. Their diagnostics, process resources and retrospective completion-deadline sensitivity are included here for completeness.

The historical monitoring gaps are retained. Some predate the correction that initializes a returned-record position from a verified assignment offset while the client position remains unresolved. The correction was not applied retrospectively to invent missing values. Missing observations also restart the fifteen-observation persistence history, so the diagnostic gap may last longer than the ownership transfer itself. A shorter growth window cannot recover measurements that were never available. No comparison of these blocks with the later ten-minute blocks isolates duration alone: dates, selected partition identities and the monitoring implementation also changed.
"""


def main():
    ap=argparse.ArgumentParser();ap.add_argument('--root',type=Path,required=True);ap.add_argument('--output',type=Path,required=True)
    args=ap.parse_args();out=args.output;root=args.root
    data=json.loads((out/'metrics.json').read_text());groups=data['groups']
    assert len(groups)==4 and sum(map(len,groups.values()))==20
    for block in ['twelve-partition','four-partition']:
        assert all(r['position_vs_processing_backlog_max_observed_difference']==0 for r in groups[block])
    main_tex=r'\subsection{Measurement coverage and interpretation}\label{sec:result-metric-coverage}'+'\n'+COMMON+'\n'+DIAGNOSTIC
    coverage_table=table(['Metric family','Evidence in results','Interpretation'],
        [[tex_escape(x) for x in row] for row in COVERAGE],
        'Coverage of the defined measurement families. Planned measurements are distinguished from completed evidence; no result is inferred merely because a metric is defined.',
        'tab:result-metric-coverage',r'>{\raggedright\arraybackslash}p{.22\linewidth}>{\raggedright\arraybackslash}p{.42\linewidth}>{\raggedright\arraybackslash}p{.29\linewidth}')
    # This coverage inventory is longer than one page; it must not be a float.
    coverage_table=coverage_table.replace(r'\begin{table}[!htbp]',r'\begingroup').replace(r'\end{table}',r'\endgroup')
    coverage_table=coverage_table.replace(r'\begin{tabular}',r'\begin{longtable}').replace(r'\end{tabular}',r'\end{longtable}')
    caption=coverage_table[coverage_table.index(r'\caption{'):coverage_table.index(r'\endgroup')]
    coverage_table=coverage_table[:coverage_table.index(r'\caption{')]+r'\endgroup'+'\n'
    caption=caption.strip()+r'\\'+'\n'
    at=coverage_table.index(r'\toprule')
    coverage_table=coverage_table[:at]+caption+coverage_table[at:]
    header=r'Metric family & Evidence in results & Interpretation\\'
    coverage_table=coverage_table.replace(r'\midrule',r'\midrule\endfirsthead'+'\n'+r'\toprule'+'\n'+header+r'\midrule\endhead',1)
    main_tex+=coverage_table
    (out/'Measurement_Coverage_Results.tex').write_text(main_tex)
    for block,prose in [('twelve-partition',TWELVE),('four-partition',FOUR)]:
        rows=groups[block]
        s=r'\subsection{'+dict(BLOCKS)[block]+r'}\label{sec:'+block+r'-updated}'+'\n'+prose+'\n'
        start_means=[r['diagnostic_cohort_latencies']['start_seconds']['mean'] for r in rows]
        s+=('Processing-start mean latency ranged from '+fmt(min(start_means),3)+' to '+fmt(max(start_means),3)+
            ' seconds across this block; it remains separate from completion latency. Every trial achieved 700 acknowledged evaluation messages/s. '+
            'No duplicate completion attempts were detected, so completed-attempt and distinct-completion throughput coincide in these trials. '+
            'Mean application work is reported below; it excludes evidence-writing and offset-commit overhead.\n')
        coverages=[c for r in rows for c in r['resources']['actual_cpu_core_seconds_process_coverage']]
        s+=('Observed per-process CPU coverage over evaluation plus drain ranged from '+fmt(100*min(coverages),2)+
            '\\% to '+fmt(100*max(coverages),2)+'\\%. Coverage uses the complete observation window, including time before additional consumers started; '
            'a lower value for a newly started consumer is not automatically a monitoring failure. The integral includes only observed valid process intervals.\n')
        assert all(r['duplicate_completion_attempts']==0 for r in rows)
        s+=(out/(block+'-tables.tex')).read_text()
        latency=[];cost=[]
        for r in rows:
            latency.append([LABELS[r['arm']],r['run_number'],fmt(r['completion_mean_seconds'],3),fmt(r['completion_p50_seconds'],3),
                            fmt(r['completion_p95_seconds'],3),fmt(r['diagnostic_cohort_latencies']['processing_seconds']['mean']*1000,3),
                            fmt(r['observed_completion_deadline_percent']['1000'])])
            c=r['intervention_cost']; rec=next((x for x in c.get('recovery_from_decision',[]) if x['threshold_offsets']==100),{})
            cost.append([LABELS[r['arm']],r['run_number'],fmt(c.get('transition_end_epoch',0)-c.get('decision_epoch',0)) if c.get('decision_epoch') else '---',
                         fmt(c.get('longest_global_no_processing_interval',{}).get('seconds'),3),
                         fmt(c.get('additional_unfinished_during_pause'),0),fmt(c.get('net_additional_unfinished'),0),fmt(rec.get('confirmation_after_anchor_seconds'),1)])
        s+=table(['Response','Run',r'Mean (s)',r'p50 (s)',r'p95 (s)',r'\shortstack{Mean work\\(ms)}',r'\shortstack{Observed misses\\$>1$ s (\%)}'],latency,
            'Supplementary latency diagnostics. Mean, p50 and p95 use completed evaluation-born messages. Mean work is the local monotonic application-processing duration. '
            'Observed violations use completion attempts occurring during evaluation and have a different denominator from the primary admitted-cohort deadline rate.',
            'tab:'+block+'-latency-details')
        s+=table(['Response','Run',r'\shortstack{Decision to\\active (s)}',r'\shortstack{Global gap\\(s)}',r'\shortstack{Extra pending\\during pause}',r'\shortstack{Net pending\\during transfer}',r'\shortstack{Recovery\\(s)}'],cost,
            'Intervention costs. For targeted actions, transfer is release request to verified active ownership; for native scaling it is decision to stable-six-owner confirmation. '
            'The latter also defines native decision-to-active. Recovery is confirmation after the actual decision. Global gap measures the longest interval without any consumer processing, not a per-partition downtime. '
            'Negative net pending change means processing reduced outstanding work during the measured transition. Dashes denote unavailable or inapplicable values.',
            'tab:'+block+'-intervention-cost')
        ownership=[[LABELS[r['arm']],r['run_number']]+[str(r['final_high_input_partition_counts'].get('consumer-sts-'+str(i),'---')) for i in range(6)] for r in rows]
        s+=table(['Response','Run','C0','C1','C2','C3','C4','C5'],ownership,
            'Final observed numbers of configured high-input partitions per consumer. These are traffic targets, not persistent-hot detector counts. All high-input partitions initially belonged to Consumer 2. A dash means no observed partition ownership.',
            'tab:'+block+'-ownership')
        delay=[]
        for r in rows:
            if 'scale' not in r['arm']:continue
            directory=root/'results'/r['run_id']
            events=[json.loads(x) for x in (directory/'intervention-events.jsonl').read_text().splitlines()]
            request=next((e['timestamp'] for e in events if e['event']=='replicas_requested'),None)
            origin='Replica request' if request is not None else 'Action decision'
            if request is None:request=next(e['timestamp'] for e in events if e['event']=='decision')
            execution=json.loads((directory/'execution-summary.json').read_text())
            values=[]
            for i in range(3,6):
                observed=[x['first_completion_epoch'] for x in execution['assignment_first_completions'] if x['pod']=='consumer-sts-'+str(i) and x.get('first_completion_epoch') is not None and x['first_completion_epoch']>=request]
                values.append(fmt(min(observed)-request) if observed else '---')
            delay.append([LABELS[r['arm']],r['run_number'],origin]+values)
        s+=table(['Response','Run','Reference event',r'C3 (s)',r'C4 (s)',r'C5 (s)'],delay,
            'Activation delay to the first valid completion on each added consumer. Targeted scaling uses the recorded replica-request event; native scaling uses the action decision because an equivalent replica-request timestamp was not separately recorded. These are not whole-action completion times or pure rebalance delays.',
            'tab:'+block+'-activation-delay')
        s+=r'\clearpage'+'\n'
        s+=fig(block+'-compact-outcomes','Compact outcome comparison: total consumer-position lag and exact acknowledged-but-unfinished work during evaluation, followed by whole-cohort p99 and unfinished percentage at the drain cutoff. Warm-up work is included in the time-series counts but excluded from the outcome cohort.','fig:'+block+'-compact-outcomes')
        s+=fig(block+'-compact-performance','Actual consumer CPU and resident memory, useful completion throughput, and ten-second processing-backlog growth. Resource traces sum fresh measurements for all started consumer processes; an unavailable component leaves a gap. Requested resources and their integrals remain separate in the resource table.','fig:'+block+'-compact-performance')
        s+=fig(block+'-runtime-state','Reconstructed runtime state and partition-lag diagnostics. The first four rows show window-mean total lag, window growth, window-mean skew and persistent-hot count. The final row shows maximum and mean partition lag. Exact persistent-hot identifiers remain in the supporting data; the count alone does not identify a reassignment target. No scheduled action was selected by this reconstructed state.','fig:'+block+'-runtime-state')
        source=root/'experiment-records'/SOURCES[block]
        for metric,caption in PLOT_CAPTIONS.items():
            src=source/('four-condition-'+metric+'.pdf');name=block+'-'+metric
            if metric!='ownership':shutil.copy2(src,out/'figures'/(name+'.pdf'))
        if block == 'four-partition':
            s = compact_four_partition_layout(s)
        (out/('Twelve_Partition_Results_Updated.tex' if block=='twelve-partition' else 'Four_Partition_Results_New.tex')).write_text(s)
    short_tex=r'\subsection{Five-minute evaluation experiments: additional metrics}'+'\n'+SHORT
    for block,_ in BLOCKS[2:]:
        short_tex+=r'\subsubsection{'+dict(BLOCKS)[block]+'}\n'+(out/(block+'-tables.tex')).read_text()
        short_tex+=fig(block+'-diagnostics','Earlier five-minute evaluation with the original monitoring gaps retained. '
                       'Persistent-hot counts, skew and maximum/mean partition lag are interpreted together.','fig:'+block+'-diagnostics')
    (out/'Five_Minute_Metrics_Appendix.tex').write_text(short_tex)
    sensitivity=r'\subsection{Completion-deadline sensitivity}\label{app:deadline-sensitivity}'+'\n'+r'The half-second deadline is retained here to assess sensitivity. The main tables use one second. An overdue unfinished record is a miss at either applicable deadline; no unfinished latency is invented.'+'\n'
    # Split this table by experiment block to avoid an overlong float.
    for block,title in BLOCKS:
        values=[[LABELS[r['arm']],r['run_number'],fmt(r['deadline_percent']['500'],3),fmt(r['deadline_percent']['1000'],3)] for r in groups[block]]
        sensitivity+=table(['Response','Run',r'Misses $>0.5$ s (\%)',r'Misses $>1$ s (\%)'],values,
           tex_escape(title)+'. '+('Both thresholds were prespecified.' if block=='four-partition' else 'Retrospective calculation; original 99-ms results remain preserved.'),
           'tab:'+block+'-deadline-sensitivity')
    (out/'Deadline_Sensitivity_Appendix.tex').write_text(sensitivity)
    (out/'metric-coverage.md').write_text('# Measurement coverage\n\n'+
        '| Metric family | Evidence | Interpretation |\n|---|---|---|\n'+ '\n'.join('| '+' | '.join(row)+' |' for row in COVERAGE)+'\n')
    instructions='''# Results metric audit and proposal-ready material

This record covers twenty existing performance trials: eight twelve-partition ten-minute trials, four four-partition ten-minute trials, and two earlier four-trial five-minute blocks. No new experiment was run. The repository total remains sixty completed performance trials.

Main results combine the ten-minute and new four-partition plots into three figures per workload: outcomes, performance/resources, and runtime state with partition-lag diagnostics. This replaces ten separate figure pages per workload while retaining the metrics. Ownership and resource cost use compact tables. Detailed individual plots remain available alongside the combined figures. Five-minute-evaluation blocks and half-second deadline sensitivity belong in the appendix. Earlier calibration and historical background are not silently removed.

The one-second deadline is the main outcome. Half-second sensitivity is secondary. Both were prespecified for the four-partition campaign; applying them to earlier blocks is retrospective. The original 99-ms data are preserved.

## Integration

- Read the current online Measurement and Results sources before applying any replacement. These files are a reviewable insertion/replacement package, not a claim that the online proposal has been updated.
- Insert `Measurement_Coverage_Results.tex` at the beginning of the relevant results discussion, reconciling terminology with the current Measurement section.
- Replace the existing twelve-partition comparison with `Twelve_Partition_Results_Updated.tex`, avoiding a second duplicate report.
- Add `Four_Partition_Results_New.tex` immediately afterward. Keep the two workloads separate.
- Keep `Five_Minute_Metrics_Appendix.tex` and `Deadline_Sensitivity_Appendix.tex` in the appendix. Retain the existing five-minute process plots alongside the added diagnostics.
- Copy the PDFs from `figures/` into the project's figure directory. Remove old appendix includes for the ten-minute figures so each appears once in main results.
- Compile, inspect changed pages and confirm all labels, figures and references resolve before claiming online completion.

## Evidence and limits

`metrics.json` records run identifiers, source hashes, exact statistics, gap-preserving plot data, deadline populations and coverage. `runtime-states.csv` and `runtime-states.json` retain the four runtime components at each observation, including the actual persistent-hot partition set. B and S use fifteen observations (28 seconds); G uses fifteen intervals (30 seconds). These reconstructed states are not evidence of an online controller. The separate processing-growth plot uses ten seconds. `compact-plot-data.json` retains the compact resource and outcome traces and event-file hashes. Time-series summaries cover evaluation only; process and requested-resource integrals cover evaluation plus drain. Requested costs for the first twelve-partition unchanged baseline are unavailable over the full interval because most request history is missing. Actual process integrals sum observed intervals, do not extrapolate gaps, and exclude brokers, producers and sidecars. Scaled consumers' whole-window coverage includes time before startup.

`build_report.py` verifies source summary hashes and reconciles retrospective deadline replay with original cohort counts and p99. `build_compact_figures.py` reconstructs runtime states and creates compact comparison panels. `build_documents.py` creates the accompanying LaTeX, compiled separately for PDF review. Large raw logs and local replay caches remain outside this record.
'''
    (out/'README.md').write_text(instructions)
    (out/'Results_Metric_Update.tex').write_text(r'''\documentclass[11pt]{article}
\usepackage[a4paper,margin=1in]{geometry}
\usepackage{graphicx,booktabs,longtable,array,amsmath,hyperref}
\hypersetup{hidelinks}
\title{Experimental results: expanded measurement coverage}
\author{}
\date{30 September 2026}
\begin{document}
\maketitle
\noindent This results package covers twenty existing trials. The online proposal must be reconciled with its current source before publication.
\section{Experimental results}
\input{Measurement_Coverage_Results.tex}
\clearpage
\input{Twelve_Partition_Results_Updated.tex}
\clearpage
\input{Four_Partition_Results_New.tex}
\clearpage
\appendix
\section{Earlier five-minute experiments and sensitivity}
\input{Five_Minute_Metrics_Appendix.tex}
\clearpage
\input{Deadline_Sensitivity_Appendix.tex}
\end{document}
''')
    print('Created proposal-ready text and figure references; compile Results_Metric_Update.tex for the review PDF')


if __name__=='__main__':main()
