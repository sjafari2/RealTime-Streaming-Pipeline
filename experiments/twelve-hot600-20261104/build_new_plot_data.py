"""Reconstruct evaluation traces from validated twelve-partition trial evidence."""
import argparse
import hashlib
import importlib.util
import json
import math
import os
from pathlib import Path
import sys

ROOT=Path(os.environ.get('PIPELINE_REPOSITORY',Path(__file__).resolve().parents[2]))
sys.path.insert(0,str(ROOT/'python-scripts'))
sys.path.insert(0,str(ROOT/'experiments/result-metric-audit-20260930'))
from evidence_io import event_paths,open_events
from build_compact_figures import process_total


def digest(path):
    h=hashlib.sha256()
    with path.open('rb') as f:
        for b in iter(lambda:f.read(1024*1024),b''):h.update(b)
    return h.hexdigest()


def read(path):return json.loads(path.read_text())


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('comparison',type=Path)
    parser.add_argument('output',type=Path)
    parser.add_argument('--campaign',default='twelve600')
    args=parser.parse_args()
    source=ROOT/'experiments/four-condition-20260929/analyze_action_windows.py'
    spec=importlib.util.spec_from_file_location('window_analysis',source)
    windows=importlib.util.module_from_spec(spec);spec.loader.exec_module(windows)
    rows=[]
    cache=args.output.parent/(args.output.stem+'-parts')
    cache.mkdir(parents=True,exist_ok=True)
    for trial in read(args.comparison)['runs']:
        signature=hashlib.sha256(json.dumps(trial,sort_keys=True).encode()).hexdigest()
        cached=cache/(trial['run_id']+'.json')
        if cached.exists():
            saved=read(cached)
            assert saved['comparison_row_sha256']==signature
            rows.append(saved)
            print('Previously verified:',trial['run_id'],flush=True)
            continue
        directory=ROOT/trial['raw_evidence']
        hashes={n:digest(directory/n) for n in ('manifest.json','lag-summary.json','prometheus.json')}
        assert all(h==trial['evidence_hashes'][n] for n,h in hashes.items())
        manifest=read(directory/'manifest.json')
        start,end=manifest['evaluation_start_epoch'],manifest['producer_end_epoch']
        assert math.isclose(end-start,600)
        row=dict(run_id=trial['run_id'],campaign=args.campaign,condition=None,rate=600,
                 arm=trial['arm'],run=trial['run_number'],source_hashes=hashes,
                 whole_run_p99_seconds=trial['p99_seconds'],unfinished_percent=trial['unfinished_percent'])
        lag=read(directory/'lag-summary.json')
        assert lag['parameters']['window_samples']==15
        assert lag['parameters'].get('growth_window_samples',15)==15
        for label,field in [('processing_backlog','processing_backlog'),('total_backlog','total_lag'),
                            ('growth','window_growth_offsets_per_second'),
                            ('skew','window_mean_skew'),('persistent_hot','persistent_hot')]:
            row[label]=[]
            for sample in lag['snapshots']:
                if not start<=sample['timestamp']<=end:continue
                value=sample.get(field) if sample['valid'] else None
                if label=='persistent_hot' and value is not None:value=len(value)
                row[label].append([sample['timestamp']-start,value])
        ack,done,event_hashes=windows.load_messages(directory,manifest,trial['intervention_cost']['event_file_sha256'])
        latencies=[v[0]-ack[k][0] for k,v in done.items()
                   if start<=ack[k][0]<end and v[0]<=manifest['drain_end_epoch']]
        assert len(latencies)==trial['completed']
        assert math.isclose(windows.p99(latencies),trial['p99_seconds'],abs_tol=1e-9)
        assert sum(start<=v[0]<end for v in ack.values())==trial['admitted']
        series=windows.build_series(ack,done,start,end)
        calculated=[windows.window_metrics(series,start+t,start+t+30) for t in range(0,600,30)]
        row['throughput']=[[t+30,w['completion_throughput_per_second']] for t,w in zip(range(0,600,30),calculated)]
        row['window_p99']=[[t+30,w['completion_window_p99_seconds']] for t,w in zip(range(0,600,30),calculated)]
        assert math.isclose(sum(v*30 for _,v in row['throughput'])/600,trial['useful_throughput'],rel_tol=1e-10)
        row['event_sha256']=event_hashes
        del ack,done,latencies,series
        starts={}
        for path in event_paths(directory):
            final=read(path.parent/'final.json')
            if final['role']!='consumer':continue
            with open_events(path) as f:event=json.loads(next(f))
            assert event['event']=='started'
            assert final['pod'] not in starts
            starts[final['pod']]=event['timestamp']
        prom=read(directory/'prometheus.json')['data']['result']
        for key,metric,divisor in [('cpu','consumer_cpu_percent',100),('memory','consumer_memory_bytes',2**20)]:
            observed=process_total(prom,metric,divisor,starts,start,end)
            row[key]=[[t*60,v] for t,v in zip(observed['minutes'],observed['values'])]
        row['activation']=windows.post_action_boundary(directory,manifest,trial)
        row['comparison_row_sha256']=signature
        cached.write_text(json.dumps(row,allow_nan=False)+'\n')
        rows.append(row)
        print('Verified plot data:',trial['run_id'],flush=True)
    assert len(rows)==8 and len(set(r['run_id'] for r in rows))==8
    args.output.parent.mkdir(parents=True,exist_ok=True)
    args.output.write_text(json.dumps(dict(rows=rows,comparison_sha256=digest(args.comparison),
        definitions=dict(throughput='Distinct completions in each nonoverlapping 30-second interval, including warm-up messages completed during evaluation.',
            window_p99='Evaluation-message creation-to-completion latencies for messages completing in each interval. Drain excluded.',
            total_backlog='Log end offset minus consumer position, summed across all partitions.',
            missing='Missing monitoring observations remain unavailable.')),allow_nan=False)+'\n')


if __name__=='__main__':main()
