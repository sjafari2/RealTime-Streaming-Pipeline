"""Bound relative wall-clock offsets using an established kubectl stream."""
import concurrent.futures,json,selectors,subprocess,time
from pathlib import Path

def probe(pod):
    container='producer-container' if pod.startswith('producer') else 'consumer-container'
    code='import sys,time; print("ready",flush=True)\nfor line in sys.stdin: print(time.time(),flush=True)'
    p=subprocess.Popen(['kubectl','--context','nautilus','-n','kafkastreamingdata','exec','-i',pod,'-c',container,'--','python3','-u','-c',code],
                       stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True,bufsize=1)
    def line():
        sel=selectors.DefaultSelector();sel.register(p.stdout,selectors.EVENT_READ)
        if not sel.select(20): raise TimeoutError(pod)
        sel.close();return p.stdout.readline().strip()
    rows=[]
    try:
        assert line()=='ready'
        for _ in range(8):
            before=time.time();p.stdin.write('stamp\n');p.stdin.flush();remote=float(line());after=time.time()
            rows.append(dict(lower=remote-after,upper=remote-before,round_trip=after-before))
        return dict(pod=pod,minimum_offset_seconds=max(r['lower'] for r in rows),
                    maximum_offset_seconds=min(r['upper'] for r in rows),samples=rows)
    finally:
        p.stdin.close()
        try:p.wait(timeout=5)
        except subprocess.TimeoutExpired:p.terminate()

if __name__=='__main__':
    pods=[f'consumer-sts-{i}' for i in range(6)]+['producer-workers-0']
    with concurrent.futures.ThreadPoolExecutor(max_workers=7) as pool: results=list(pool.map(probe,pods))
    out=Path(__file__).parent/'live/clock-probe.json';out.write_text(json.dumps(results,indent=2))
    print(json.dumps([{k:v for k,v in r.items() if k!='samples'} for r in results],indent=2))
