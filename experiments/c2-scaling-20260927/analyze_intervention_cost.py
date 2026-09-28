"""Intervention costs from preserved events and valid monitoring.

The report records whether the criteria were specified before the experiment
or applied afterward to an earlier block.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import sys
os.environ.setdefault('MPLCONFIGDIR', '/private/tmp/intervention-cost-mpl')

ROOT = Path(os.environ.get('PIPELINE_REPOSITORY', Path(__file__).resolve().parents[2]))
sys.path.insert(0, str(ROOT / 'python-scripts'))
from evidence_io import event_paths, open_events


def outstanding(ack, done, timestamp):
    # Completion may precede the producer receiving its acknowledgment callback.
    if hasattr(ack, 'shape'):
        import numpy as np
        return int(np.count_nonzero((ack <= timestamp) & (done > timestamp)))
    return sum(a <= timestamp < d for a, d in zip(ack, done))


def recovery(snapshots, anchor, end, threshold, hold=30):
    candidate = previous = None
    for row in snapshots:
        t = row['timestamp']
        if not anchor <= t <= end:
            continue
        good = row.get('valid') and row.get('processing_backlog') is not None
        good = good and row['processing_backlog'] <= threshold
        if not good:
            candidate = previous = None
            continue
        if previous is None or not 0 < t-previous['timestamp'] <= 3 or row['owners'] != previous['owners'] or any(
                row[field][p] < value for field in ('highs', 'positions') for p, value in previous[field].items()):
            candidate = t
        previous = row
        if t-candidate >= hold:
            return dict(threshold_offsets=threshold, hold_seconds=hold,
                        first_low_epoch=candidate, confirmed_epoch=t,
                        onset_after_anchor_seconds=candidate-anchor,
                        confirmation_after_anchor_seconds=t-anchor)
    return dict(threshold_offsets=threshold, hold_seconds=hold, first_low_epoch=None,
                confirmed_epoch=None, onset_after_anchor_seconds=None,
                confirmation_after_anchor_seconds=None,
                unconfirmed_low_start_epoch=candidate,
                observed_low_duration_seconds=previous['timestamp']-candidate if previous is not None and candidate is not None else 0)


def analyze(directory):
    import numpy as np
    manifest = json.loads((directory/'manifest.json').read_text())
    audit = json.loads((directory/'handoff-validation.json').read_text())
    assert audit['status'] == 'passed' and audit['handoff_checked']
    transitions = {e['event']: e['timestamp'] for e in map(json.loads, (directory/'explicit-handoff-events.jsonl').read_text().splitlines())}
    arrivals, completions, starts, pods = {}, {}, {}, {}
    sources = {}
    for path in event_paths(directory):
        sources[str(path.relative_to(directory))] = hashlib.sha256(path.read_bytes()).hexdigest()
        final = json.loads(path.with_name('final.json').read_text())
        with open_events(path) as stream:
            for line in stream:
                e = json.loads(line)
                if e['event'] == 'acknowledged':
                    assert e['message_id'] not in arrivals
                    arrivals[e['message_id']] = e['timestamp']
                elif e['event'] == 'completed':
                    assert e['message_id'] not in completions
                    completions[e['message_id']] = e['completion_timestamp']
                    starts[e['message_id']] = e['processing_start_timestamp']
                elif e['event'] in ('explicit_released', 'explicit_resumed'):
                    row = pods.setdefault(final['pod'], {})
                    assert e['event'] not in row
                    row[e['event']] = e['timestamp']
    assert set(completions) == set(starts) and set(completions) <= set(arrivals)
    keys = list(arrivals)
    ack = np.array([arrivals[k] for k in keys])
    done = np.array([completions.get(k,float('inf')) for k in keys])
    begin = np.array([starts.get(k,float('inf')) for k in keys])
    release, resume, active = (transitions[k] for k in ('release_requested', 'resume_requested', 'active_verified'))
    last_done = float(done[done < resume].max())
    first_start = float(begin[begin >= resume].min())
    first_done = float(done[done >= resume].min())
    assert first_start > last_done
    assert not np.any((begin < first_start) & (done > last_done))
    assert release-5 < last_done < resume < first_start < active+5
    lag = json.loads((directory/'lag-summary.json').read_text())
    # The report identifies whether these criteria were set before this block.
    recoveries = [recovery(lag['snapshots'], resume, manifest['producer_end_epoch'], threshold)
                  for threshold in (50, 100, 200)]
    schedule = manifest['evaluation_start_epoch']+float(manifest['intervention']['after_evaluation_start_seconds'])
    points = {name: dict(epoch=t, acknowledged_unfinished=outstanding(ack, done, t))
              for name,t in [('scheduled_action', schedule), ('release_requested', release),
                             ('last_pre_handoff_completion', last_done), ('first_post_handoff_processing', first_start),
                             ('active_verified', active)]}
    result = dict(run_id=manifest['run_id'],
        observed_no_processing_seconds=first_start-last_done,
        observed_no_completion_seconds=first_done-last_done,
        coordination_seconds=active-release,
        scheduled_to_active_seconds=active-schedule,
        points=points,
        acknowledged_unfinished_increase_during_coordination=points['active_verified']['acknowledged_unfinished']-points['release_requested']['acknowledged_unfinished'],
        acknowledged_unfinished_increase_during_processing_gap=points['first_post_handoff_processing']['acknowledged_unfinished']-points['last_pre_handoff_completion']['acknowledged_unfinished'],
        per_consumer_released_to_resumed_seconds={pod: row['explicit_resumed']-row['explicit_released'] for pod,row in pods.items()},
        recovery_anchor='resume_requested', recovery_anchor_epoch=resume,
        recovery_threshold_sensitivity=recoveries,
        raw_evidence='results/'+manifest['run_id'], event_file_sha256=sources,
        summary_file_sha256={name:hashlib.sha256((directory/name).read_bytes()).hexdigest() for name in ('manifest.json','explicit-handoff-events.jsonl','lag-summary.json','handoff-validation.json')},
        clock_probes=manifest.get('clock_probes'), scale_clock_probes=manifest.get('scale_clock_probes'))
    x = np.arange(manifest['evaluation_start_epoch'], manifest['producer_end_epoch']+0.1, 1)
    y = [outstanding(ack,done,t) for t in x]
    return result, (x-manifest['evaluation_start_epoch'])/60, y, transitions, manifest


def main():
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    import numpy as np
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('record',type=Path)
    parser.add_argument('--treatment-arm',default='scale6')
    parser.add_argument('--treatment-label',default='Scale to 6 + redistribution')
    parser.add_argument('--figure-prefix',default='c2-scaling')
    args=parser.parse_args();record=args.record
    comparison=json.loads((record/'comparison.json').read_text())
    scale=[r for r in comparison['runs'] if r['arm']==args.treatment_arm]
    results=[];curves=[]
    for row in scale:
        result,x,y,marks,manifest=analyze(ROOT/row['raw_evidence'])
        result['run_number']=row['run_number'];results.append(result);curves.append((x,y,marks,manifest))
    definitions=dict(
        processing_interruption='Gap between the last pre-resume completion and first post-resume processing start across all consumers; verified that no logged application processing interval intersects the gap. It is not pod downtime.',
        transition_backlog='Distinct messages with producer acknowledgment callback timestamp <= t and completion timestamp > t, including warm-up. Net change between release_requested and active_verified. This reconstructs acknowledged-but-unfinished work, not exact broker offset lag; callback delay and inter-host clock uncertainty affect boundary counts.',
        recovery='First valid total processing-backlog observation <= 100 offsets that remains <= 100 for at least 30 seconds of consecutive observations, no gaps >3 seconds or owner/offset resets. Measured from resume_requested, during continuing production only. 50/200 thresholds supplied as post-hoc sensitivity, not pre-registered criteria.',
        interpretation='Observed '+args.treatment_label+' overhead, not a causal estimate of excess backlog. Cross-host timestamps have the saved clock-probe uncertainty; report durations approximately.')
    if comparison.get('cost_protocol'):
        definitions['recovery']=definitions['recovery'].replace('post-hoc sensitivity, not pre-registered criteria', 'sensitivity specified before this block')
    payload=dict(analysis_type='prospectively_specified_descriptive' if comparison.get('cost_protocol') else 'post_hoc_descriptive',definitions=definitions,runs=results)
    (record/'intervention-cost.json').write_text(json.dumps(payload,indent=2,allow_nan=False)+'\n')
    plt.rcParams.update({'font.family':'DejaVu Sans','font.size':10,'axes.spines.top':False,'axes.spines.right':False})
    fig,axes=plt.subplots(1,2,figsize=(11,4.6),sharey=True)
    for i,(ax,(x,y,marks,m)) in enumerate(zip(axes,curves)):
        ax.plot(x,y,color='#176b93',lw=1.5)
        ax.axvspan((marks['release_requested']-m['evaluation_start_epoch'])/60,(marks['active_verified']-m['evaluation_start_epoch'])/60,color='#64748b',alpha=.18)
        ax.axvline(1,color='#555555',ls=':',lw=1)
        ax.set(title='Run '+str(i+1),xlabel='Evaluation time (minutes)',ylabel='Acknowledged but unfinished messages',xlim=(0,5),ylim=(0,None))
        ax.grid(axis='y',alpha=.2)
    fig.suptitle('Outstanding work — '+args.treatment_label,fontweight='bold')
    fig.text(.08,.025,'Reconstructed from message identities and timestamps, including warm-up. This is not broker offset lag.\nShading: release requested to active verified. Dotted line: scheduled intervention.',fontsize=9)
    fig.tight_layout(rect=(0,.12,1,.94))
    for ext in ('png','pdf'):fig.savefig(record/('intervention-cost-backlog.'+ext),dpi=180)
    fig,axes=plt.subplots(1,2,figsize=(11,4.8))
    centers=np.arange(2);width=.34
    for arm,shift,color,label in [('keep3',-width/2,'#1f77b4','Keep 3'),(args.treatment_arm,width/2,'#ff7f0e',args.treatment_label)]:
        rows=sorted([s for s in comparison['runs'] if s['arm']==arm],key=lambda s:s['run_number'])
        for ax,key in zip(axes,('p99_seconds','unfinished_percent')):
            vals=[s[key] for s in rows];bars=ax.bar(centers+shift,vals,width,color=color,label=label)
            for bar,value in zip(bars,vals):ax.annotate(f'{value:.2f}',(bar.get_x()+bar.get_width()/2,value),xytext=(0,4),textcoords='offset points',ha='center',fontsize=10)
    for ax in axes:ax.set_xticks(centers);ax.set_xticklabels(['Run 1','Run 2']);ax.grid(axis='y',alpha=.2);ax.set_axisbelow(True)
    axes[0].set(ylabel='Completion p99 (seconds)',ylim=(0,max(1,max(s['p99_seconds'] for s in comparison['runs']))*1.2),title='Completed evaluation messages')
    axes[1].set(ylabel='Unfinished evaluation messages (%)',ylim=(0,max(1,max(s['unfinished_percent'] for s in comparison['runs']))*1.25),title='At the fixed drain cutoff')
    handles,labels=axes[0].get_legend_handles_labels();fig.legend(handles,labels,loc='upper center',bbox_to_anchor=(.5,.90),ncol=2,frameon=False)
    fig.suptitle('Completion latency and unfinished work — 700 messages/s',fontweight='bold')
    fig.text(.08,.025,'Each trial: 1 min warm-up + 5 min evaluation + 2 min drain; 210,000 evaluation messages.\nP99 is a whole-run completed-cohort percentile, not an average of rolling p99 values.',fontsize=9)
    fig.tight_layout(rect=(0,.13,1,.82))
    for ext in ('png','pdf'):fig.savefig(record/(args.figure_prefix+'-p99-unfinished.'+ext),dpi=180)
    lines=['# Observed intervention costs','',
           'Treatment: '+args.treatment_label+'. Cost criteria are '+('specified in advance for this block' if comparison.get('cost_protocol') else 'post-hoc for this block')+'. Cross-host timestamps have clock-probe uncertainty, so durations are approximate.','',
           '| Metric | Run 1 | Run 2 |','|---|---:|---:|']
    for label,key in [('No application-processing interval observed (s)','observed_no_processing_seconds'),('No completion interval (s)','observed_no_completion_seconds'),('Handoff coordination (s)','coordination_seconds'),('Net additional acknowledged-but-unfinished messages during coordination','acknowledged_unfinished_increase_during_coordination'),('Additional acknowledged-but-unfinished messages during processing gap','acknowledged_unfinished_increase_during_processing_gap')]:
        vals=[r[key] for r in results];lines.append(f'| {label} | {vals[0]:,.1f} | {vals[1]:,.1f} |')
    for i,r in enumerate(results):
        rec=r['recovery_threshold_sensitivity'][1]
        if rec['first_low_epoch'] is None:
            lines+=['',f'Run {i+1}: the primary recovery criterion was not confirmed before production ended. A short low-backlog interval alone is insufficient; see the unconfirmed-candidate fields in the JSON.']
            continue
        lines+=['',f"Run {i+1}: recovery to at most 100 processing-backlog offsets starts {rec['onset_after_anchor_seconds']:.1f} seconds after the resume request and is confirmed after {rec['confirmation_after_anchor_seconds']:.1f} seconds, requiring 30 seconds of continuous valid low-backlog observations."]
    lines+=['']+[f'**{key.replace("_"," ").capitalize()}:** {value}\n' for key,value in definitions.items()]
    lines+=['The reconstructed message curve fills the monitoring gap using separate event evidence; it does not replace or interpolate missing lag observations. Recovery uses actual valid processing-backlog monitoring, not that reconstructed curve. Warm-up work is included in outstanding work. Counts are observed changes, not the number of messages that would have accumulated under a hypothetical no-action counterfactual.','',
            '[Machine-readable results and sensitivity](intervention-cost.json)','',
            '![Whole-run p99 and unfinished work]('+args.figure_prefix+'-p99-unfinished.png)','',
            '![Reconstructed outstanding messages](intervention-cost-backlog.png)']
    (record/'INTERVENTION_COST.md').write_text('\n'.join(lines).rstrip()+'\n')
    print(json.dumps([{k:v for k,v in r.items() if k not in ('event_file_sha256','clock_probes','scale_clock_probes')} for r in results],indent=2))


if __name__=='__main__':main()
