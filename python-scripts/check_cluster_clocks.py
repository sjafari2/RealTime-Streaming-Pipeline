#!/usr/bin/env python3
"""Read kernel clock status in existing experiment pods; never adjust clocks."""
import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import json
from pathlib import Path
import subprocess
import time

# modes=0 asks adjtimex for status only. The oversized zeroed buffer lets libc
# return its full structure; we decode only the documented 64-bit ABI prefix.
# This needs no host mount, privileged debug pod, daemon installation or clock set.
REMOTE_QUERY = r'''
import ctypes, json, platform, shutil, subprocess, time
result = dict(timestamp_epoch=time.time(), architecture=platform.machine())
if platform.system() != 'Linux' or ctypes.sizeof(ctypes.c_long) != 8:
    result['error'] = 'This read-only decoder requires a 64-bit Linux long ABI.'
else:
    class Prefix(ctypes.Structure):
        _fields_ = [('modes',ctypes.c_uint), ('offset',ctypes.c_long), ('freq',ctypes.c_long),
                    ('maxerror',ctypes.c_long), ('esterror',ctypes.c_long), ('status',ctypes.c_int)]
    libc = ctypes.CDLL(None, use_errno=True)
    fn = libc.adjtimex
    fn.argtypes = [ctypes.c_void_p]
    fn.restype = ctypes.c_int
    buffer = ctypes.create_string_buffer(512)
    prefix = Prefix.from_buffer(buffer)
    assert prefix.modes == 0
    state = fn(ctypes.byref(buffer))
    if state < 0:
        result['error'] = 'adjtimex failed with errno ' + str(ctypes.get_errno())
    else:
        result['kernel_clock'] = dict(read_only_modes=prefix.modes, state=state,
            status_bits=prefix.status, unsynchronized=bool(prefix.status & 0x40),
            clock_error=bool(prefix.status & 0x1000),
            offset_seconds=prefix.offset/(1e9 if prefix.status & 0x2000 else 1e6),
            maximum_error_seconds=prefix.maxerror/1e6, estimated_error_seconds=prefix.esterror/1e6)
result['chronyc_available_in_container'] = bool(shutil.which('chronyc'))
if result['chronyc_available_in_container']:
    try:
        command = subprocess.run(['chronyc','-n','tracking'],capture_output=True,text=True,timeout=3)
        result['chronyc_tracking'] = dict(returncode=command.returncode,stdout=command.stdout,stderr=command.stderr)
    except subprocess.TimeoutExpired:
        result['chronyc_tracking'] = dict(error='Query timed out; no clock settings changed.')
print(json.dumps(result))
'''


def inspect(context, namespace):
    def kubectl(*args):
        return subprocess.check_output(['kubectl','--context',context,'-n',namespace,
            '--request-timeout=15s',*args],stderr=subprocess.PIPE,timeout=25)
    pods = json.loads(kubectl('get','pods','-o','json'))['items']
    placements = []
    selected = []
    for item in pods:
        name = item['metadata']['name']
        if name.startswith(('producer-sts-', 'consumer-sts-', 'pip-kafka-controller-')):
            placements.append(dict(pod=name,node=item['spec'].get('nodeName'),
                resources=[dict(container=c['name'],resources=c.get('resources',{})) for c in item['spec']['containers']]))
        for role in ('producer','consumer'):
            if name.startswith(role+'-sts-'):
                selected.append((role,item))
    if not selected:
        raise RuntimeError('No experiment producer/consumer pods found')

    def probe(entry):
        role,item=entry
        row=dict(role=role,pod=item['metadata']['name'],node=item['spec'].get('nodeName'))
        before=time.time()
        try:
            result=json.loads(kubectl('exec',row['pod'],'-c',role+'-container','--','python3','-c',REMOTE_QUERY))
            after=time.time()
            row.update(result,api_roundtrip_seconds=after-before,
                offset_from_client_interval_seconds=[result['timestamp_epoch']-after,result['timestamp_epoch']-before])
        except (subprocess.SubprocessError,ValueError) as exc:
            row['error']=str(exc)
        return row
    with ThreadPoolExecutor(max_workers=3) as pool:
        samples=list(pool.map(probe,selected))
    return dict(recorded_utc=datetime.now(timezone.utc).isoformat(),context=context,namespace=namespace,
        placements=placements,samples=samples,
        notes=['Point-in-time kernel synchronization diagnostics, not retrospective validation of earlier run clocks.',
               'Kernel offsets/error estimates are reported by the host discipline; they are not an independently verified cross-node error bound.',
               'Repeat at run boundaries and obtain clock-daemon/source evidence before a strict latency deadline claim.',
               'No Kubernetes resources or system clock parameters are changed.'],
        reference='https://man7.org/linux/man-pages/man2/adjtimex.2.html')


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--context',default='nautilus')
    parser.add_argument('--namespace',default='kafkastreamingdata')
    parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args()
    result=inspect(args.context,args.namespace)
    args.output.parent.mkdir(parents=True,exist_ok=True)
    args.output.write_text(json.dumps(result,indent=2,allow_nan=False)+'\n')
    for row in result['samples']:
        print(row['pod'],row.get('kernel_clock',row.get('error')))
    print('Saved',args.output)
