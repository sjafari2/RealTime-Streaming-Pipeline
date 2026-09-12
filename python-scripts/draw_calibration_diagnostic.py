"""Plot a fixed-assignment calibration without treating balanced input as balanced progress."""
import argparse
from collections import defaultdict
import hashlib
import json
import math
from pathlib import Path
import statistics

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np

p=argparse.ArgumentParser(description=__doc__);p.add_argument('run_directory',type=Path);p.add_argument('--guard',type=Path,required=True);p.add_argument('--output',type=Path,required=True)
a=p.parse_args();D=a.run_directory;O=a.output;O.mkdir(parents=True,exist_ok=True)
load=lambda name:json.loads((D/name).read_text())
m=load('manifest.json');s=load('outcome-summary.json');e=load('execution-summary.json')
assert load('runner-status.json')['status']=='complete' and not s['validity_failures']
assert not [x for x in e['ownership_events'] if m['start_epoch']<=x['timestamp']<m['drain_end_epoch']], 'Initial-owner grouping requires fixed ownership through drain'
owner={(x['topic'],x['partition']):x['pod'] for x in m['initial_assignment']}
counts=defaultdict(lambda:dict(admitted=0,incomplete=0,partitions=0))
for row in s['partition_metrics']['partitions']:
    c=counts[owner[(row['topic'],row['partition'])]];c['admitted']+=row['admitted_messages'];c['incomplete']+=row['incomplete_messages'];c['partitions']+=1
rows=[json.loads(line) for line in a.guard.read_text().splitlines()]
start=m['start_epoch'];end=m['producer_end_epoch'];evaluation=end-m['evaluation_start_epoch']
rows=[r for r in rows if start<=r['query_timestamp']<=end]
keys=sorted(counts);colors=['#24648a','#b95728','#507344'];names=[k.replace('consumer-sts-','Consumer ') for k in keys]
plt.rcParams.update({'font.size':10,'axes.spines.top':False,'axes.spines.right':False,'svg.fonttype':'none','savefig.facecolor':'white'})
fig=plt.figure(figsize=(9,7),layout='constrained');grid=fig.add_gridspec(2,2,height_ratios=[1.3,1])
ax=fig.add_subplot(grid[0,:]);left=fig.add_subplot(grid[1,0]);right=fig.add_subplot(grid[1,1])
for k,color,name in zip(keys,colors,names):
    x=[];y=[];previous=None
    for r in rows:
        t=r['query_timestamp']
        if previous is not None and t-previous>7.5:x.append(math.nan);y.append(math.nan)
        x.append(t-start);y.append(r.get('per_consumer_backlog',{}).get(k,math.nan) if r['valid'] else math.nan);previous=t
    ax.plot(x,y,label=name,color=color,lw=1.7)
ax.axvspan(0,m['evaluation_start_epoch']-start,color='#dddddd',alpha=.5,label='Warm-up excluded from cohort')
ax.set(title='Balanced input, uneven backlog accumulation',ylabel='Completion-frontier offsets',xlabel='Seconds from production start',xlim=(0,end-start));ax.set_ylim(bottom=0);ax.legend(fontsize=8)
rate=[counts[k]['admitted']/evaluation for k in keys]
left.bar(names,rate,color=colors,width=.65);left.set(title='Acknowledged evaluation input',ylabel='Messages / second to assigned partitions');left.set_ylim(0,max(rate)*1.2)
for i,v in enumerate(rate):left.text(i,v+8,f'{v:.1f}',ha='center',fontsize=9)
share=[100*counts[k]['incomplete']/counts[k]['admitted'] for k in keys]
right.bar(names,share,color=colors,width=.65);right.set(title='Unfinished at bounded drain',ylabel='% of assigned evaluation admissions');right.set_ylim(0,max(share)*1.25)
for i,v in enumerate(share):right.text(i,v+.12,f'{v:.2f}%\n({counts[keys[i]]["incomplete"]:,})',ha='center',fontsize=9)
for axis in (ax,left,right):axis.grid(axis='y',alpha=.2);axis.set_axisbelow(True)
fig.suptitle(f"Qualified pressure calibration · {m['run_id']}\n{len(keys)} consumers · {int(m['config']['NUM_PARTITIONS'])} partitions · {float(m['config']['TARGET_RATE'])*int(m['config']['PRODUCER_POD_COUNT']):,.0f} messages/s · {int(m['config']['APP_CPU_ITERATIONS']):,} SHA-256 iterations/message",fontsize=12)
fig.supxlabel('Consumers retained their initial partitions throughout production and drain. The input bars use exact cohort evidence;\nthe backlog curves use saved 5-second guard observations. Shared-node differences were observed, not experimentally isolated.',fontsize=8)
for ext in ('png','svg','pdf'):fig.savefig(O/('balanced-input-uneven-progress.'+ext),dpi=190,bbox_inches='tight')
plt.close(fig)
png=O/'balanced-input-uneven-progress.svg';png.write_text('\n'.join(line.rstrip() for line in png.read_text().splitlines())+'\n')
values=[row['admitted_messages'] for row in s['partition_metrics']['partitions']]
record=dict(run_id=m['run_id'],consumer_cohorts=dict(counts),admission_rates=dict(zip(keys,rate)),unfinished_percent=dict(zip(keys,share)),partition_admission_cv=statistics.pstdev(values)/statistics.mean(values),source_sha256={str(p.resolve()):hashlib.sha256(p.read_bytes()).hexdigest() for p in [D/'manifest.json',D/'outcome-summary.json',D/'execution-summary.json',a.guard]},note='One preliminary calibration, without a mitigation action. Node effects and application effects are not isolated.')
(O/'calibration-diagnostic.json').write_text(json.dumps(record,indent=2)+'\n');print(json.dumps(record,indent=2))
