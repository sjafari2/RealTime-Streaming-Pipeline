"""Retain probe exit status and logs independently of a kubectl connection."""
import argparse
import os
from pathlib import Path
import subprocess
import sys
import time
from kafka_age_probe import put


if __name__ == '__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root',type=Path,required=True)
    parser.add_argument('--name',required=True)
    parser.add_argument('command',nargs=argparse.REMAINDER)
    args=parser.parse_args()
    command=args.command[1:] if args.command[:1]==['--'] else args.command
    status=args.root/(args.name+'-process.json')
    state=dict(wrapper_pid=os.getpid(),started_epoch=time.time(),command=command,state='running')
    put(status,state)
    try:
        with (args.root/(args.name+'.log')).open('wb') as log:
            child=subprocess.Popen(command,stdout=log,stderr=subprocess.STDOUT)
            state['child_pid']=child.pid;put(status,state)
            state['exit_code']=child.wait()
        state['state']='completed' if state['exit_code']==0 else 'failed'
    except BaseException as exc:
        state.update(state='failed',error=repr(exc),exit_code=1)
    finally:
        state['finished_epoch']=time.time();put(status,state)
    sys.exit(state['exit_code'])
