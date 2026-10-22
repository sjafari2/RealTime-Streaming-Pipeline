"""Render the trial-level signal analysis as figures and a compact report."""
import argparse
import json
import os
from pathlib import Path
import statistics
import tempfile

os.environ.setdefault('MPLCONFIGDIR', str(Path(tempfile.gettempdir())/'signal-outcome-mpl'))

from build_analysis import GROUPS, LABELS

COLORS = {'keep3':'#2765a6', 'redistribute3':'#d97b1d',
          'scale_redistribute6':'#168361', 'kafka_scale6':'#9653a6'}
SHORT = {'keep3':'Keep 3', 'redistribute3':'Redistribute 3',
         'scale_redistribute6':'Targeted 6', 'kafka_scale6':'Kafka 6'}
TITLES = {'12hot-700':'12 high-input partitions | 700 msg/s',
          '4hot-700':'4 high-input partitions | 700 msg/s',
          '4hot-600':'4 high-input partitions | 600 msg/s'}


def fmt(value, digits=2):
    return 'Unavailable' if value is None else f'{value:,.{digits}f}'


def save(fig, path):
    for ext in ('png','pdf'):
        fig.savefig(path.with_suffix('.'+ext),dpi=190)
    plt.close(fig)


def legend(fig):
    from matplotlib.lines import Line2D
    handles=[Line2D([],[],marker='o',linestyle='',color=c,label=LABELS[a]) for a,c in COLORS.items()]
    fig.legend(handles=handles,loc='upper center',bbox_to_anchor=(.5,.945),ncol=4,frameon=False,fontsize=9)


def figures(data, output):
    global plt
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    plt.rcParams.update({'font.size':9,'axes.titlesize':10,'pdf.fonttype':42,'ps.fonttype':42})
    rows=data['runs']
    fig,axes=plt.subplots(2,3,figsize=(12,7.2))
    for col,(group,_,_,_) in enumerate(GROUPS):
        subset=[r for r in rows if r['group']==group]
        for r in subset:
            b,o=r['before'],r['outcomes'];color=COLORS[r['action']]
            marker='o' if r['run_number']==1 else '^'
            x=b['processing_growth_30s_offsets_per_second']
            axes[0,col].scatter(x,o['p99_seconds'],c=color,marker=marker,s=65,edgecolor='white',linewidth=.5,zorder=3)
            offset=(-14,-12) if group=='12hot-700' and r['action']=='redistribute3' and r['run_number']==1 else (4,4)
            axes[0,col].annotate(str(r['run_number']),(x,o['p99_seconds']),xytext=offset,textcoords='offset points',fontsize=8,color=color)
            y=o['recovery_minutes'] if o['recovery_confirmed'] else o['recovery_observation_minutes']
            axes[1,col].scatter(x,y,c=color,marker=marker if o['recovery_confirmed'] else 'x',s=65,zorder=3)
            if group=='4hot-600' and r['action']=='keep3' and r['run_number']==2:offset=(-28,-13)
            axes[1,col].annotate(str(r['run_number']) if o['recovery_confirmed'] else f"{r['run_number']} NC",(x,y),
                                 xytext=offset,textcoords='offset points',fontsize=8,color=color)
        axes[0,col].set_title(TITLES[group]);axes[0,col].set_ylim(0,420)
        axes[1,col].set_ylim(0,10);axes[1,col].set_xlabel('Pre-action Q growth (offsets/s)')
        for ax in axes[:,col]:ax.grid(alpha=.22);ax.margins(x=.2)
    axes[0,0].set_ylabel('Whole-cohort completion p99 (s)')
    axes[1,0].set_ylabel('Recovery confirmation from decision (min)')
    fig.suptitle('Similar starting growth, different intervention outcomes',fontsize=14,y=.99)
    legend(fig)
    fig.text(.5,.025,'One point per trial; labels identify Run 1 or 2. Growth: trailing 30 s ending at evaluation second 58.\n'
             'NC: recovery unconfirmed; crosses show the observation cutoff, not a measured recovery time. Keep 3 uses second 60.\n'
             'Cohort p99 excludes unfinished messages; their percentages are retained in the outcome table. Panels are separate campaigns.',ha='center',fontsize=8)
    fig.subplots_adjust(top=.84,bottom=.19,left=.08,right=.985,hspace=.28,wspace=.27)
    save(fig,output/'signals-and-outcomes')

    fig,axes=plt.subplots(1,3,figsize=(12,4.4))
    for ax,(group,_,_,_) in zip(axes,GROUPS):
        for r in rows:
            if r['group']!=group:continue
            o=r['outcomes'];x=o['resources']['requested_cpu_core_minutes'];color=COLORS[r['action']]
            if x is None:continue
            ax.scatter(x,o['p99_seconds'],c=color,marker='o' if r['run_number']==1 else '^',s=70,zorder=3)
            ax.annotate(str(r['run_number']),(x,o['p99_seconds']),xytext=(5,5 if r['run_number']==1 else -11),textcoords='offset points',fontsize=8,color=color)
        ax.set_title(TITLES[group]);ax.set_xlim(62,152);ax.set_ylim(0,420)
        ax.set_xlabel('Requested consumer CPU (core-min)');ax.grid(alpha=.22)
    axes[0].set_ylabel('Whole-cohort completion p99 (s)')
    fig.suptitle('Resource requests and latency describe different advantages',fontsize=14,y=.99);legend(fig)
    fig.text(.5,.025,'Requests cover evaluation plus drain. Keep 3 unfinished: 34.64/34.83% (12 hot, 700), 26.92/37.57% (4 hot, 600).\n'
             'All intervention trials finished the evaluation cohort. Keep 3 Run 1 at 700 is omitted here because request coverage is incomplete.\n'
             'No Keep 3 or native Kafka scaling trial exists in the four-partition 700 block. No cross-campaign percentage comparison is made.',ha='center',fontsize=8)
    fig.subplots_adjust(top=.77,bottom=.25,left=.07,right=.985,wspace=.27)
    save(fig,output/'cost-and-latency')

    fig,axes=plt.subplots(1,3,figsize=(12,4.3))
    for ax,(group,_,_,_) in zip(axes,GROUPS):
        subset=[r for r in rows if r['group']==group]
        for i,r in enumerate(subset):
            for c,pod in enumerate(('consumer-sts-0','consumer-sts-1','consumer-sts-2')):
                y=r['before']['resources']['cpu_percent']['per_pod_mean'][pod]
                if y is not None:ax.scatter(c+(i-(len(subset)-1)/2)*.035,y,color=COLORS[r['action']],marker='o' if r['run_number']==1 else '^',s=35)
        ax.set_title(TITLES[group]);ax.set_xticks([0,1,2],['Consumer 0','Consumer 1','Consumer 2'])
        ax.set_ylim(0,1.15);ax.grid(axis='y',alpha=.22)
    axes[0].set_ylabel('Pre-action mean process CPU (cores)')
    fig.suptitle('The initial processing load was concentrated on Consumer 2',fontsize=14,y=.99);legend(fig)
    fig.text(.5,.025,'Arithmetic means at evaluation seconds 30, 32, ..., 58. Small horizontal offsets separate trials visually.\n'
             'All selected high-input partitions initially belonged to Consumer 2. Low CPU on other consumers suggests uneven use,\n'
             'but CPU observations do not measure how much additional work those consumers can sustainably process.',ha='center',fontsize=8)
    fig.subplots_adjust(top=.77,bottom=.24,left=.07,right=.985,wspace=.27)
    save(fig,output/'pre-action-cpu')


def table_rows(data, kind):
    result=[]
    for r in data['runs']:
        b,o=r['before'],r['outcomes'];prefix=[r['group'],SHORT[r['action']],str(r['run_number'])]
        if kind=='signals':
            result.append(prefix+[fmt(b['mean_backlog_offsets'],0),fmt(b['processing_growth_30s_offsets_per_second'],1),
                fmt(b['mean_skew'],2),fmt(b['persistent_hot_count'],0),
                fmt(b['resources']['cpu_percent']['total_mean'],3),fmt(b['resources']['memory_bytes']['total_mean'],1)])
        else:
            result.append(prefix+[fmt(o['p99_seconds'],2),fmt(o['unfinished_percent'],2),fmt(o['deadline_miss_percent_1s'],2),
                fmt(o['recovery_minutes'],2) if o['recovery_confirmed'] else 'Unconfirmed',
                fmt(o['useful_throughput_messages_per_second'],1),fmt(o['resources']['requested_cpu_core_minutes'],1)])
    return result


def overview(data):
    texts=[]
    for group,_,_,_ in GROUPS:
        rows=[r for r in data['runs'] if r['group']==group]
        def span(name):
            x=[r['before'][name] for r in rows];return f'{min(x):,.1f}-{max(x):,.1f}'
        texts.append(f"{TITLES[group]}: pre-action mean backlog {span('mean_backlog_offsets')} offsets; "
                     f"30-second processing-backlog growth {span('processing_growth_30s_offsets_per_second')} offsets/s; "
                     f"mean skew {span('mean_skew')}. All high-input partitions belonged to Consumer 2.")
    return texts


FINDINGS = [
    'The twenty trials cover three separate experiment blocks. Eight twelve-partition trials at 700 messages/s provide the primary comparison; four four-partition trials at 700 and eight at 600 provide additional evidence. All actions were scheduled; these records do not demonstrate a learned or adaptive controller.',
    'Redistribution confirmed the specified backlog-recovery criterion and completed every evaluation message in all six observed trials. This establishes observed sufficiency for those two criteria in the tested conditions, not for an unspecified application SLO. Its absence of observed failures prevents learning a redistribution success/failure boundary from these trials alone.',
    'The twelve-partition block shows a trade-off: redistribution used 72 requested CPU core-minutes and had p99 of 80.10/84.16 s; Kafka scaling used about 137 core-minutes and had p99 of 71.41/72.46 s. Targeted scaling recovered sooner than redistribution in both repetitions, despite higher whole-cohort p99. Recovery and latency rank the responses differently.',
    'The four-partition 700 block favors redistribution over targeted scaling on recorded p99, recovery time and requested resources in both repetitions. Keep 3 and native scaling were not tested in that block. The 600 block includes all four actions; Kafka scaling had the lowest p99 in Run 1 and redistribution in Run 2. Kafka scaling Run 2 completed during drain without confirming low-backlog recovery during input.',
    'Starting ownership concentration is constant in all twenty trials, and persistent-hot count is constant within each block. Skew values mostly distinguish the four- and twelve-partition designs. A pooled correlation could therefore reflect the experimental design or changed deployment rather than a transferable decision rule.',
    'The present records support descriptive comparisons and hypotheses. They do not support a defensible general-purpose classifier or neural controller. No predictive model, feature-importance ranking, significance test or pooled correlation is fitted in this analysis.'
]


NEXT = [
    'First vary initial ownership at fixed workload and deployment: concentrated on one consumer, partly distributed, and already distributed. Compare the available actions in matched blocks with reversed or randomized action order. This directly tests whether ownership improves prediction beyond backlog and skew.',
    'Then vary offered work relative to measured consumer capacity while keeping the ownership design controlled. Measure per-consumer service capacity under sustained available work; do not infer capacity from an underloaded completion rate or CPU percentage. Select load levels on both sides of the observed recovery boundary using calibration.',
    'Retain unfavorable outcomes and unconfirmed recoveries. Keep trial/campaign/seed groups together in future validation; neighboring scrapes and windows are not independent examples. Reserve separate workload schedules for final testing and record deployment changes.',
    'Specify acceptable deadline-miss, unfinished-work and recovery requirements before assigning an application-sufficiency label. Start with a simple interpretable baseline only when the experiments include diverse conditions and both sufficient and insufficient redistribution outcomes.'
]


def markdown(data, output):
    lines=['# Signals before action and experimental outcomes','',*sum(([p,''] for p in FINDINGS),[]),'## Starting conditions','']
    lines += sum(([p,''] for p in overview(data)),[])
    for title,kind,headers in [
        ('Pre-action measurements','signals',['Block','Response','Run','Mean B (offsets)','Q growth (offsets/s)','Mean skew','Persistent hot','CPU (cores)','RSS (MiB)']),
        ('Outcomes','outcomes',['Block','Response','Run','p99 (s)','Unfinished (%)','1 s misses (%)','Recovery (min)','Throughput (msg/s)','CPU requests (core-min)'])]:
        lines += ['## '+title,'','| '+' | '.join(headers)+' |','| '+' | '.join(['---']*len(headers))+' |']
        lines += ['| '+' | '.join(row)+' |' for row in table_rows(data,kind)];lines.append('')
    lines += ['## Definitions and interpretation','']
    lines += [f'- **{key}:** {value}' for key,value in data['definitions'].items()]
    lines += ['','The CSV is one row per trial; blank fields are unavailable, not zero. JSON retains per-owner resources, coverage, pre-action and later completion windows, source hashes, and grouping metadata. Pending/unfinished messages must accompany completed-message percentiles. Small latency differences remain subject to original clock uncertainty.','',
              '## Next experiments','']+sum(([p,''] for p in NEXT),[])
    for name in ('signals-and-outcomes','cost-and-latency','pre-action-cpu'):
        lines += [f'![{name.replace("-"," ")}](figures/{name}.png)','']
    lines += ['## Reproduce','', 'From the repository root, with the retained raw evidence available:', '', '```bash',
              'python3 experiments/signal-outcome-20261021/build_analysis.py --output /path/to/analysis',
              'python3 experiments/signal-outcome-20261021/build_report.py /path/to/analysis/analysis.json --output /path/to/analysis',
              '```','', 'The analysis requires no cluster access. The report builder uses Matplotlib and ReportLab. The input hashes in `analysis.json` identify the exact evidence and summaries used.']
    (output/'README.md').write_text('\n'.join(lines)+'\n')


def pdf(data, output):
    from reportlab.lib import colors
    from reportlab.lib.pagesizes import A4, landscape
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, Image, PageBreak
    width,height=landscape(A4);margin=36;usable=width-2*margin
    styles=getSampleStyleSheet();styles.add(ParagraphStyle(name='BodySmall',fontName='Helvetica',fontSize=10,leading=14,spaceAfter=8))
    styles.add(ParagraphStyle(name='NoteSmall',fontName='Helvetica',fontSize=8.5,leading=11,spaceAfter=6))
    story=[]
    def p(text,style='BodySmall'):return Paragraph(text,styles[style])
    def heading(text):story.extend([p(text,'Heading1'),Spacer(1,6)])
    def image(name,height_limit):
        from PIL import Image as PILImage
        path=output/'figures'/(name+'.png');w,h=PILImage.open(path).size
        scale=min(usable/w,height_limit/h);return Image(str(path),width=w*scale,height=h*scale)
    heading('Signals before action and experimental outcomes')
    story.append(p('20 performance trials | Three experiment blocks | Offline analysis of saved evidence','Heading2'))
    for text in FINDINGS:story.append(p(text))
    story.append(p('The analysis preserves the original outcome definitions. No new performance trials were run.'))
    story.append(PageBreak());heading('Starting conditions: one row per trial')
    story.append(p('B is consumer-position lag; Q is processing backlog. Means use seconds 30, 32, ..., 58. Q growth spans seconds 28-58. CPU and RSS sum the original three consumer processes.'))
    def draw_table(headers,rows):
        data_rows=[[p(h,'NoteSmall') for h in headers]]+[[p(str(v),'NoteSmall') for v in row] for row in rows]
        widths=[72,92,28]+[(usable-192)/(len(headers)-3)]*(len(headers)-3)
        table=Table(data_rows,colWidths=widths,repeatRows=1)
        style=[('BACKGROUND',(0,0),(-1,0),colors.HexColor('#dce5ee')),('VALIGN',(0,0),(-1,-1),'MIDDLE'),('TOPPADDING',(0,0),(-1,-1),3),('BOTTOMPADDING',(0,0),(-1,-1),3),('LINEBELOW',(0,0),(-1,0),.6,colors.HexColor('#76899c'))]
        for i,row in enumerate(rows,1):
            shade={'12hot-700':'#edf4fc','4hot-700':'#e8f4ec','4hot-600':'#fff0d8'}[row[0]]
            style.append(('BACKGROUND',(0,i),(-1,i),colors.HexColor(shade)))
        table.setStyle(TableStyle(style));return table
    story.append(draw_table(['Block','Response','Run','Mean B\n(offsets)','Q growth\n(offsets/s)','Mean\nskew','Persistent\nhot','CPU\n(cores)','RSS\n(MiB)'],table_rows(data,'signals')))
    story.append(PageBreak());heading('Performance, recovery and resource cost')
    story.append(p('p99 follows evaluation-born messages completed by the drain cutoff. Unfinished and one-second deadline outcomes use the full admitted cohort. Recovery requires Q <= 100 offsets for 30 valid continuous seconds during input.'))
    story.append(draw_table(['Block','Response','Run','p99\n(s)','Unfinished\n(%)','1 s misses\n(%)','Recovery\n(min)','Throughput\n(msg/s)','CPU requests\n(core-min)'],table_rows(data,'outcomes')))
    story.append(p('Throughput counts distinct completions during evaluation, including warm-up messages completed then, divided by 600 seconds. Unconfirmed recoveries are censored; they are not zero. The unavailable first Keep 3 request total is due to incomplete history. Cost covers evaluation plus drain and is distinct from actual CPU use. Application sufficiency remains unspecified.','NoteSmall'))
    story.append(PageBreak());heading('Signals and outcomes within each campaign')
    story.append(image('signals-and-outcomes',440))
    story.append(PageBreak());heading('Resource trade-offs and the next comparisons')
    story.append(image('cost-and-latency',290))
    for text in NEXT[:2]:story.append(p(text))
    story.append(PageBreak());heading('Consumer-level measurements and limits on learning')
    story.append(image('pre-action-cpu',290))
    for text in NEXT[2:]:story.append(p(text))
    story.append(p('Reproducibility: analysis.json contains the definitions, source SHA-256 hashes, pre-action measurements and original outcomes. trial-data.csv provides one record per trial. All three blocks must remain identifiable in subsequent analysis.','NoteSmall'))
    def footer(canvas,doc):
        canvas.setFont('Helvetica',8);canvas.setFillColor(colors.HexColor('#617083'))
        canvas.drawString(margin,18,'Kafka stream-processing experiments | Signal-to-outcome analysis | 21 October 2026')
        canvas.drawRightString(width-margin,18,str(doc.page))
    SimpleDocTemplate(str(output/'signal-outcome-report.pdf'),pagesize=(width,height),
                      leftMargin=margin,rightMargin=margin,topMargin=28,bottomMargin=30,
                      title='Signals before action and experimental outcomes',author='Soheila Jafari Khouzani').build(story,onFirstPage=footer,onLaterPages=footer)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('analysis',type=Path);parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--stage',choices=('all','figures','report'),default='all')
    args=parser.parse_args();data=json.loads(args.analysis.read_text());(args.output/'figures').mkdir(parents=True,exist_ok=True)
    if args.stage in ('all','figures'):figures(data,args.output/'figures')
    if args.stage in ('all','report'):markdown(data,args.output);pdf(data,args.output)


if __name__=='__main__':main()
