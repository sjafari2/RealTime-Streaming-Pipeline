"""Plot validated evaluation lag without connecting missing or changed-owner samples."""
import argparse,json,os
from pathlib import Path
os.environ.setdefault('MPLCONFIGDIR','/private/tmp/stability-matplotlib')
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np

parser=argparse.ArgumentParser()
parser.add_argument('run_directory',type=Path)
parser.add_argument('output',type=Path)
args=parser.parse_args()
p=args.run_directory
lag=json.loads((p/'lag-summary.json').read_text())
m=json.loads((p/'manifest.json').read_text())
cfg=m['config']
rate=float(cfg['TARGET_RATE'])*int(cfg['PRODUCER_POD_COUNT'])
number=cfg['EXP_ID'].rsplit('run',1)[-1]
duration=lag['evaluation_seconds']/60
partitions=int(cfg['NUM_PARTITIONS'])*int(cfg['TOPIC_COUNT'])
x=[];y=[];previous=None
for row in lag['snapshots']:
    t=(row['timestamp']-m['evaluation_start_epoch'])/60
    good=row.get('valid') and row.get('total_lag') is not None
    if good and previous is not None:
        reset=any(row[field][k]<v for field in ('highs','positions') for k,v in previous[field].items())
        if row['timestamp']-previous['timestamp']>lag['parameters']['max_gap'] or row['owners']!=previous['owners'] or reset:
            x.append(t);y.append(np.nan)
    x.append(t);y.append(row['total_lag'] if good else np.nan)
    previous=row if good else None
plt.rcParams.update({'font.family':'DejaVu Sans','font.size':11,'axes.spines.top':False,'axes.spines.right':False})
fig,ax=plt.subplots(figsize=(11,5.4))
ax.plot(x,y,color='#176b93',linewidth=1.15,label=f'Total lag across {partitions} partitions')
ax.set(xlim=(0,duration),ylim=(0,max(150,lag['peak_sampled_lag']*1.18)),xlabel='Time since evaluation started (minutes)',ylabel='Total lag (offsets)')
ax.set_xticks(np.linspace(0,duration,11));ax.grid(axis='y',alpha=.2);ax.legend(loc='upper left',frameon=False)
fig.suptitle(f'Balanced input — {rate:,.0f} messages/s — Run {number}',x=.09,ha='left',fontsize=17,fontweight='bold',y=.97)
ax.set_title(f"{cfg['CONSUMER_POD_COUNT']} consumers • {partitions} partitions • no scaling",loc='left',fontsize=11,pad=16,color='#555555')
fig.text(.09,.075,f"Peak observed lag: {lag['peak_sampled_lag']:,.0f} offsets",fontsize=11)
fig.text(.09,.025,f"Evaluation only: {duration:g} minutes, following {float(cfg['WARMUP_SECONDS'])/60:g} minute warm-up; the {float(cfg['DRAIN_SECONDS'])/60:g}-minute drain is not shown.",fontsize=9,color='#555555')
fig.subplots_adjust(left=.09,right=.98,top=.82,bottom=.22)
args.output.parent.mkdir(parents=True,exist_ok=True)
fig.savefig(args.output.with_suffix('.png'),dpi=180,facecolor='white')
fig.savefig(args.output.with_suffix('.pdf'),facecolor='white')
print(args.output.with_suffix('.png'))
