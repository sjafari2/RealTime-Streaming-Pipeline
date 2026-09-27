"""Record consumer resource requests independently of blocking handoff work."""
import argparse
import hashlib
import json
from pathlib import Path
import sys
import time
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'my-shell'));sys.path.insert(0,str(ROOT/'python-scripts'))
import run_experiment as r
from analyze_execution import pod_observation


def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('campaign',type=Path)
    args=parser.parse_args();path=args.campaign/'resource-observations.jsonl'
    if path.exists():raise RuntimeError('Refusing to overwrite existing observations')
    if r.kubectl('config','current-context').decode().strip()!='nautilus':raise RuntimeError('Unexpected context')
    (args.campaign/'resource-observer-manifest.json').write_text(json.dumps(dict(source_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),interval_seconds=2,started_epoch=time.time(),namespace=r.NS),indent=2)+'\n')
    print('[OBSERVE]',path,flush=True)
    with path.open('x') as out:
        while True:
            c=json.loads((args.campaign/'campaign-status.json').read_text())
            if c['status'] in ('complete','failed'):break
            began=time.time()
            try:
                items=json.loads(r.kubectl('get','pods','-l','app=consumer-sts','-o','json',timeout=10))['items']
                row=dict(valid=True,timestamp=time.time(),request_started_epoch=began,pods=[pod_observation(p) for p in items])
            except Exception as exc:
                row=dict(valid=False,timestamp=time.time(),request_started_epoch=began,pods=[],error=str(exc))
            out.write(json.dumps(row,allow_nan=False)+'\n');out.flush()
            time.sleep(max(0,2-(time.time()-began)))
    print('[OBSERVE] Campaign ended; observations saved.',flush=True)


if __name__=='__main__':main()
