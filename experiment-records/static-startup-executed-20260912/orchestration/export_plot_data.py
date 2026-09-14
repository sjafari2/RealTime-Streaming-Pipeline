"""Archive run-scoped plotting queries while Prometheus retains the observations."""
import argparse,json,os,sys,urllib.parse,urllib.request
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
ROOT=Path('/Users/soheila/Desktop/Thesis-26-27/code')
sys.path.insert(0,str(ROOT/'my-shell'));sys.path.insert(0,str(ROOT/'python-scripts'))
import run_experiment as r
from analyze_lag import analyze
p=argparse.ArgumentParser();p.add_argument('run_directory',type=Path);p.add_argument('--output',type=Path);a=p.parse_args()
D=a.run_directory;O=a.output or D/'plots';O.mkdir(parents=True,exist_ok=True)
c=json.loads((D/'manifest.json').read_text());run=c['run_id'];selector='{run_id='+json.dumps(run)+'}'
queries=dict(admitted_rate=f'sum(rate(producer_delivery_ok_total{selector}[30s]))',completion_rate=f'sum(rate(consumer_messages_consumed_total{selector}[30s]))',consumer_cpu_percent='consumer_cpu_percent'+selector,producer_cpu_percent='producer_cpu_percent'+selector,consumer_memory_mib='consumer_memory_bytes'+selector+' / 1048576',consumer_partition_count='consumer_assigned_partitions'+selector)
for q in (50,95,99): queries[f'p{q}_ms']=f'1000 * histogram_quantile({q/100}, sum by (le) (rate(consumer_e2e_latency_seconds_bucket{selector}[30s])))'
queries['deadline_percent']=f'100 * sum(rate(consumer_slo_violations_total{selector}[30s])) / sum(rate(consumer_e2e_latency_seconds_count{selector}[30s]))'
spec=dict(run_id=run,start=c['start_epoch'],evaluation_start=c['evaluation_start_epoch'],producer_end=c['producer_end_epoch'],drain_end=c['drain_end_epoch'],end=c['drain_end_epoch'],step=2,window_seconds=30,queries=queries,manifest=c,note='Rolling attempt rates and histogram quantiles are monitoring estimates; cohort statistics are reconciled separately.')
with r.prometheus_connection():
 base=os.environ['PROM_URL']
 def get(item):
  name,query=item;params=urllib.parse.urlencode(dict(query=query,start=spec['start'],end=spec['end'],step=2))
  with urllib.request.urlopen(base+'/api/v1/query_range?'+params,timeout=90) as response: data=json.load(response)
  if data.get('status')!='success' or not data.get('data',{}).get('result'): raise RuntimeError('Missing plot series: '+name)
  (O/(name+'.json')).write_text(json.dumps(data)+'\n');return name
 with ThreadPoolExecutor(max_workers=2) as pool:
  for name in pool.map(get,queries.items()): print('Saved',name,flush=True)
(O/'queries.json').write_text(json.dumps(spec,indent=2)+'\n')
lag=analyze(D);(O/'lag-summary.json').write_text(json.dumps(lag,indent=2)+'\n')
print('Plot data saved:',O,flush=True)
