"""Redraw one run's archived plotting data without contacting Nautilus."""
import argparse,csv,json,math,platform
from pathlib import Path
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.colors import SymLogNorm
import numpy as np
p=argparse.ArgumentParser();p.add_argument('plot_directory',type=Path);a=p.parse_args();D=a.plot_directory
s=json.loads((D/'queries.json').read_text());c=s['manifest'];lag=json.loads((D/'lag-summary.json').read_text())
start=s['evaluation_start'];duration=s['producer_end']-start
colors=['#21618c','#cc641c','#3d7b52','#8b5c9c','#bd4c67','#817143']
plt.rcParams.update({'font.size':10,'axes.spines.top':False,'axes.spines.right':False,'svg.fonttype':'none','savefig.facecolor':'white'})

def read(name):return json.loads((D/(name+'.json')).read_text())['data']['result']
def curves(ax,name,label=None,color=None):
 for i,row in enumerate(sorted(read(name),key=lambda x:x['metric'].get('pod',''))):
  x=[float(t)-start for t,v in row['values']];y=[float(v) for t,v in row['values']]
  ax.plot(x,y,color=color or colors[i%len(colors)],label=label or row['metric'].get('pod',name),lw=1.4)

def timing(ax):
 ax.set_xlim(0,duration);ax.set_xlabel('Seconds from evaluation start');ax.grid(axis='y',alpha=.2);ax.set_axisbelow(True)
 intervention=c.get('intervention',{})
 if intervention.get('at_epoch') is not None:
  ax.axvline(intervention['at_epoch']-start,color='#444444',ls='--',lw=1)

def save(fig,name):
 fig.savefig(D/(name+'.png'),dpi=190,bbox_inches='tight');fig.savefig(D/(name+'.svg'),bbox_inches='tight');plt.close(fig)
 svg=D/(name+'.svg');svg.write_text('\n'.join(line.rstrip() for line in svg.read_text().splitlines())+'\n')

def backlog(ax):
 rows=lag['snapshots'];x=[r['timestamp']-start for r in rows];y=[r.get('processing_backlog',math.nan) if r['valid'] else math.nan for r in rows]
 ax.plot(x,y,color=colors[0],lw=1.5)
 ax.set(title=f"Processing backlog · {100*lag['covered_fraction']:.1f}% lag coverage\nGaps denote invalid observations",ylabel='Uncompleted offsets');ax.set_ylim(bottom=0)

def throughput(ax):
 curves(ax,'admitted_rate','Acknowledged attempts',colors[0]);curves(ax,'completion_rate','Completed attempts',colors[1])
 ax.axhline(float(c['config']['TARGET_RATE'])*len(c['producer_pods']),color='#777777',ls=':',label='Configured target',lw=1)
 ax.set(title='Throughput · rolling 30-second rates',ylabel='Messages / second');ax.set_ylim(bottom=0);ax.legend(fontsize=8,loc='lower left')

def cpu(ax):
 curves(ax,'consumer_cpu_percent');ax.set(title='Consumer process CPU · 100% is one core',ylabel='CPU %');ax.set_ylim(bottom=0);ax.legend(fontsize=7,ncol=3)

panels=[('processing-backlog',backlog),('throughput',throughput),('consumer-cpu',cpu)]
count=len(c['consumer_pods']);mode=c['config']['TRAFFIC_MODE'];action=c.get('intervention',{}).get('action','none')
title=f"{mode.capitalize()} · {count} initial consumers · target {float(c['config']['TARGET_RATE'])*len(c['producer_pods']):,.0f}/s"
fig,axes=plt.subplots(3,1,figsize=(8.5,9),layout='constrained')
for ax,(_,draw) in zip(axes,panels):draw(ax);timing(ax)
fig.suptitle(title+'\n'+s['run_id']+' · action: '+action,fontsize=12);save(fig,'overview')
for name,draw in panels:
 fig,ax=plt.subplots(figsize=(8.5,3.5),layout='constrained');draw(ax);timing(ax);fig.suptitle(title,fontsize=11);save(fig,name)
fig,ax=plt.subplots(figsize=(8.5,3.8),layout='constrained')
for i,q in enumerate((50,95,99)):curves(ax,f'p{q}_ms',f'p{q}',colors[i])
ax.set(title='Recorded completion latency · rolling histogram estimates',ylabel='Milliseconds');ax.set_ylim(bottom=0);ax.legend();timing(ax)
fig.supxlabel('Completed attempts only; this is separate from exact cohort percentiles and unfinished-message counts.',fontsize=8);save(fig,'rolling-completion-latency')
valid=[r for r in lag['snapshots'] if r['valid']]
if valid:
 keys=sorted(valid[0]['lags'],key=lambda k:(k.rsplit('/',1)[0],int(k.rsplit('/',1)[1])))
 rows=lag['snapshots'];x=np.array([r['timestamp']-start for r in rows]);matrix=np.array([[r['lags'][key] if r['valid'] else np.nan for r in rows] for key in keys])
 fig,ax=plt.subplots(figsize=(8.5,4.5),layout='constrained');vmax=max(10,float(np.nanmax(matrix)))
 img=ax.pcolormesh(x,np.arange(len(keys)),matrix,shading='nearest',cmap='YlGnBu',norm=SymLogNorm(linthresh=10,vmin=0,vmax=vmax))
 ax.set(title='Partition pattern · returned-position lag',ylabel='Partition index');timing(ax);fig.colorbar(img,ax=ax,label='Offsets (linear below 10, logarithmic above)');save(fig,'partition-lag')
with (D/'processing-backlog.csv').open('w',newline='') as stream:
 writer=csv.writer(stream,lineterminator="\n");writer.writerow(['timestamp_epoch','seconds_from_evaluation_start','valid','processing_backlog_offsets'])
 writer.writerows((r['timestamp'],r['timestamp']-start,r['valid'],r.get('processing_backlog') if r['valid'] else '') for r in lag['snapshots'])
(D/'plot-environment.json').write_text(json.dumps(dict(python=platform.python_version(),matplotlib=matplotlib.__version__,numpy=np.__version__),indent=2)+'\n')
print('Saved reproducible figures:',D)
