#!/usr/bin/env python3
"""Create a fresh recovery-cluster identity; never replace an existing identity."""
import base64
import json
import subprocess
import uuid

def identifier():
    return base64.urlsafe_b64encode(uuid.uuid4().bytes).decode().rstrip('=')

if __name__ == '__main__':
    secret = dict(apiVersion='v1', kind='Secret', type='Opaque',
        metadata=dict(name='pip-kafka-recovery-ucsd-kraft', namespace='kafkastreamingdata'),
        stringData={key: identifier() for key in ('cluster-id', 'controller-0-id', 'controller-1-id', 'controller-2-id')})
    subprocess.run(['kubectl', 'create', '-f', '-'], input=json.dumps(secret).encode(), check=True)
