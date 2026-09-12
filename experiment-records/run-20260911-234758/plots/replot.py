"""Redraw the saved figures offline: python replot.py (requires matplotlib)."""
from pathlib import Path
import csv
import json
import math
import platform
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

HERE = Path(__file__).resolve().parent
spec = json.loads((HERE / 'queries.json').read_text())
lag = json.loads((HERE / 'lag-summary.json').read_text())
RUN = spec['run_id']
plt.rcParams.update({'font.size': 10, 'axes.spines.top': False, 'axes.spines.right': False,
                     'axes.titleweight': 'bold', 'savefig.facecolor': 'white', 'svg.fonttype': 'none'})
COLORS = ['#166a96', '#dd7432', '#37834a', '#9564ad', '#b54b68', '#776936']

def read(name):
    return json.loads((HERE / (name + '.json')).read_text())['data']['result']

def line(ax, name, label=None, color=None):
    for index, series in enumerate(sorted(read(name), key=lambda row: row['metric'].get('pod', ''))):
        x = [(float(t)-spec['start']) for t,v in series['values']]
        y = [float(v) for t,v in series['values']]
        ax.plot(x, y, label=label or series['metric'].get('pod', name),
                color=color or COLORS[index % len(COLORS)], linewidth=1.5)

def timing(ax):
    ax.set_xlim(0, spec['end']-spec['start'])
    ax.axvspan(300, 360, color='#cbd5df', alpha=.25, linewidth=0)
    ax.axvline(300, color='#6b7280', linestyle='--', linewidth=.8)
    ax.axvline(360, color='#9ca3af', linestyle=':', linewidth=.8)
    ax.set_xlabel('Seconds from shared start')
    ax.grid(axis='y', alpha=.18)
    ax.set_axisbelow(True)

def throughput(ax):
    line(ax, 'admitted_rate', 'Acknowledged attempts', COLORS[0])
    line(ax, 'completion_rate', 'Completed attempts', COLORS[1])
    ax.axhline(9000, color='#777777', linestyle=':', linewidth=1, label='Target 9,000/s')
    ax.set_title('Throughput · rolling 30 s')
    ax.set_ylabel('Messages / second'); ax.set_ylim(bottom=0)
    ax.legend(fontsize=8, loc='lower left')

def latency(ax):
    for name,label,color in [('p50_ms','p50',COLORS[2]),('p95_ms','p95',COLORS[0]),('p99_ms','p99',COLORS[1])]:
        line(ax,name,label,color)
    ax.axhline(99,color='#666666',linestyle=':',linewidth=1,label='99 ms objective')
    ax.set_title('Completion latency · rolling 30 s histogram estimates')
    ax.set_ylabel('Milliseconds'); ax.set_ylim(bottom=0); ax.legend(fontsize=8)

def deadline(ax):
    line(ax,'deadline_percent','Recorded misses',COLORS[4])
    ax.set_title('Recorded deadline misses · rolling 30 s')
    ax.set_ylabel('% of valid completed attempts'); ax.set_ylim(bottom=0)

def lag_plot(ax):
    x = [s['timestamp']-spec['start'] for s in lag['snapshots']]
    y = [s['total_lag'] if s['valid'] else math.nan for s in lag['snapshots']]
    ax.plot(x,y,color=COLORS[0],linewidth=1.5,label='Valid full-pipeline lag')
    ax.set_title(f"Operational lag · {100*lag['covered_fraction']:.0f}% integration coverage")
    ax.set_ylabel('Offsets'); ax.set_ylim(bottom=0)
    ax.text(.99,.96,'Gaps = invalid observations\nEvaluation window only',ha='right',va='top',
            transform=ax.transAxes,fontsize=8,color='#555555')

def cpu(ax):
    line(ax,'consumer_cpu_percent')
    ax.set_title('Consumer process CPU · 100% = one core')
    ax.set_ylabel('Process CPU %'); ax.set_ylim(bottom=0)
    ax.legend(fontsize=7,ncol=2)

def memory(ax):
    line(ax,'consumer_memory_mib')
    ax.set_title('Consumer resident memory')
    ax.set_ylabel('MiB'); ax.set_ylim(bottom=0)
    ax.legend(fontsize=7,ncol=2)

PANELS = [('throughput',throughput),('rolling-latency',latency),('deadline-misses',deadline),
          ('valid-lag',lag_plot),('consumer-cpu',cpu),('consumer-memory',memory)]

def save(fig,name):
    fig.savefig(HERE/(name+'.png'),dpi=180,bbox_inches='tight')
    fig.savefig(HERE/(name+'.svg'),bbox_inches='tight')
    plt.close(fig)

fig, axes = plt.subplots(3,2,figsize=(14,11),layout='constrained')
for ax, (name,draw) in zip(axes.flat,PANELS):
    draw(ax);timing(ax)
fig.suptitle('Balanced monitoring validation | '+RUN+'\n3 producers · 6 consumers · 60 partitions · no mitigation',fontsize=16)
fig.supxlabel('Production 0–300 s | shaded drain 300–360 s | clock accuracy not yet calibrated',fontsize=10)
save(fig,'overview')
for name,draw in PANELS:
    fig,ax=plt.subplots(figsize=(9,4.8),layout='constrained')
    draw(ax);timing(ax)
    fig.suptitle(RUN+' | balanced · no mitigation',fontsize=11)
    fig.supxlabel('Shaded area: drain. Rolling latency is separate from the exact whole-run cohort.',fontsize=9)
    save(fig,name)

with (HERE/'valid-lag.csv').open('w',newline='') as stream:
    writer=csv.writer(stream);writer.writerow(['timestamp_epoch','seconds_from_start','valid','total_lag_offsets'])
    writer.writerows((s['timestamp'],s['timestamp']-spec['start'],s['valid'],s.get('total_lag') if s['valid'] else '')
                    for s in lag['snapshots'])

outcome_path = HERE.parent/'outcome-summary.json'
if outcome_path.exists():
    outcome=json.loads(outcome_path.read_text())
    assert outcome['run_id']==RUN
    rows=sorted(outcome['partition_metrics']['partitions'],key=lambda r:r['partition'])
    fig,ax=plt.subplots(figsize=(12,4.5),layout='constrained')
    ax.bar([r['partition'] for r in rows],[r['admitted_messages'] for r in rows],color=COLORS[0])
    ax.axhline(outcome['admitted_evaluation_cohort']/60,color=COLORS[1],linestyle='--',label='Partition mean')
    ax.set(xlabel='Partition',ylabel='Distinct admitted messages',title='Observed balanced traffic across 60 partitions')
    ax.legend();ax.grid(axis='y',alpha=.18);ax.set_axisbelow(True)
    fig.suptitle(RUN,fontsize=11);save(fig,'partition-message-counts')
    with (HERE/'partition-message-counts.csv').open('w',newline='') as stream:
        writer=csv.writer(stream);writer.writerow(['partition','admitted_messages','completed_by_drain','incomplete'])
        writer.writerows((r['partition'],r['admitted_messages'],r['completed_cohort_messages'],r['incomplete_messages']) for r in rows)

(HERE/'plot-environment.json').write_text(json.dumps(dict(python=platform.python_version(),matplotlib=matplotlib.__version__),indent=2)+'\n')
print('Saved PNG/SVG figures in',HERE)
