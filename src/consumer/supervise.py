"""Start a consumer for an active shared run, including in newly scaled pods."""
import json
import os
from pathlib import Path
import signal
import subprocess
import time

stopping = False
child = None

def stop(*_):
    global stopping
    stopping = True
    if child and child.poll() is None:
        child.terminate()

signal.signal(signal.SIGTERM, stop)
signal.signal(signal.SIGINT, stop)
last_run = None
while not stopping:
    try:
        control = json.loads(Path('/config/run-control.json').read_text())
        run_id = control['run_id']
        active = control['state'] in ('preparing', 'running') and time.time() < control.get('drain_end_epoch', control['expires_epoch'])
        if active and run_id != last_run:
            last_run = run_id
            # run.sh loads the frozen copy of our shared configuration.
            child = subprocess.Popen(['bash', str(Path(__file__).with_name('run.sh'))])
            while child.poll() is None and not stopping:
                time.sleep(1)
            if stopping:
                child.wait()
            else:
                print('[supervisor] Consumer exited with', child.returncode, 'for', run_id, flush=True)
    except FileNotFoundError:
        pass  # No experiment has been started yet.
    except Exception as exc:
        print('[supervisor]', exc, flush=True)
    time.sleep(1)
