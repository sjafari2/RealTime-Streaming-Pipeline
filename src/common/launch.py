"""Load the shared YAML when an application starts, then replace this process."""
import hashlib
import json
import os
from pathlib import Path
import sys
import yaml


def launch(role):
    path = Path(os.getenv('PIPELINE_CONFIG', '/config/pipeline-configmap.yaml'))
    control_path = Path('/config/run-control.json')
    managed = os.getenv('PIPELINE_STANDALONE', 'false').lower() != 'true' and control_path.exists()
    if managed:
        control = json.loads(control_path.read_text())
        if control['state'] not in ('preparing', 'running'):
            raise RuntimeError('No active managed run. Use save-run.sh, or PIPELINE_STANDALONE=true for troubleshooting.')
        path = Path(control['config_path'])
        os.environ['PIPELINE_RUN_CONTROL'] = str(control_path)
    raw = path.read_bytes()
    config = yaml.safe_load(raw)['data']
    for name, value in config.items():
        os.environ[name] = str(value).lower() if isinstance(value, bool) else str(value)
    os.environ['PIPELINE_CONFIG_SHA256'] = hashlib.sha256(raw).hexdigest()
    os.environ['PYTHONUNBUFFERED'] = '1'
    if not managed:
        os.environ.pop('PIPELINE_RUN_CONTROL', None)
        print('[INFO] Standalone troubleshooting run; timing is local to this process.', flush=True)
    os.execv(sys.executable, [sys.executable, str(Path(os.getenv('PIPELINE_APP_DIR', str(Path(__file__).parent))) / (role + '.py'))])


if __name__ == '__main__':
    launch(sys.argv[1])
