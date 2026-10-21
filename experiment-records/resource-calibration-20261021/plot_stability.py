"""Plot continuing-input backlog and its growth from valid saved observations."""
import argparse,json,os,sys
from pathlib import Path
os.environ.setdefault('MPLCONFIGDIR','/private/tmp/resource-calibration-matplotlib')
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np
p=argparse.ArgumentParser(description=__doc__)
p.add_argument('run',type=Path);p.add_argument('output',type=Path)
a=p.parse_args();load=lambda n:json.loads((a.run/n).read_text())
m=load('manifest.json');lag=load('lag-summary.json');start=m['evaluation_start_epoch'];end=m['producer_end_epoch']
xs=[];q=[];b=[];growth=[];skew=[];previous=None
for r in lag['snapshots']:
 if not start<=r['timestamp']<=end:continue
 valid=r['valid']
 broken=valid and previous is not None and (not 0<r['timestamp']-previous['timestamp']<=lag['parameters']['max_gap'] or r['owners']!=previous['owners'] or any(r[f][k]<v for f in ('highs','positions') for k,v in previous[f].items()))
 if broken:
  for values in (xs,q,b,growth,skew):values.append(np.nan)
 xs.append((r['timestamp']-start)/60)
 for values,key in ((q,'processing_backlog'),(b,'total_lag'),(growth,'window_processing_backlog_growth_offsets_per_second'),(skew,'window_mean_skew')):
  v=r.get(key);values.append(v if valid and v is not None else np.nan)
 previous=r if valid else None
plt.rcParams.update({'font.size':11,'axes.spines.top':False,'axes.spines.right':False})
f,axes=plt.subplots(3,1,figsize=(11,9),sharex=True,layout='constrained',gridspec_kw={'height_ratios':[1.5,1,1]})
axes[0].plot(xs,q,color='#216a89',lw=1.6,label='Processing backlog (not yet completed)')
axes[0].plot(xs,b,color='#b66a2c',lw=1,label='Fetch backlog (not yet returned to application)')
axes[0].set(ylabel='Total offsets',ylim=(0,None));axes[0].legend(frameon=False,fontsize=9)
axes[1].plot(xs,growth,color='#568244',lw=1)
axes[1].axhline(0,color='#444444',lw=.8,ls='--');axes[1].set(ylabel='Backlog growth\n(offsets / second)')
axes[2].plot(xs,skew,color='#78559c',lw=1.2)
axes[2].set(ylabel='Mean lag skew\n(max / partition mean)',xlabel='Minutes from evaluation start',ylim=(0,None),xlim=(0,(end-start)/60))
for ax in axes:
 ax.grid(axis='y',alpha=.2)
 for x in range(5,int((end-start)/60),5):ax.axvline(x,color='#dddddd',lw=.6)
f.suptitle(f'Balanced input at {float(m["config"]["TARGET_RATE"])*int(m["config"]["PRODUCER_POD_COUNT"]):g} messages/s · three fixed consumers\n{(end-start)/60:.2f}-minute evaluation; no scaling or reassignment',fontsize=15)
f.supxlabel('Warm-up and drain are outside the displayed interval. Gaps remain unavailable. Growth uses 15 consecutive intervals;\nmean skew uses 15 equally weighted valid observations (two-second sampling).',fontsize=9)
a.output.parent.mkdir(parents=True,exist_ok=True)
for ext in ('png','pdf'):f.savefig(a.output.with_suffix('.'+ext),dpi=180,facecolor='white')
print(a.output.with_suffix('.png'))
