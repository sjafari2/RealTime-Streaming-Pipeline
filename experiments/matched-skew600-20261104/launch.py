"""Launch a matched series and retry interrupted infrastructure attempts safely."""
import argparse
import fcntl
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time

from supervise import TRANSIENT_ERRORS


def read(path):
    return json.loads(path.read_text())


def write(path, value):
    temporary = path.with_suffix('.tmp')
    temporary.write_text(json.dumps(value, indent=2) + '\n')
    temporary.replace(path)


def retry_allowed(series, campaign):
    states = [series] + ([campaign] if campaign else [])
    if any(s.get('status') != 'failed' or s.get('restoration') != 'verified'
           or s.get('requires_user_review') for s in states):
        return False
    error = ' '.join(s.get('error', '') for s in states)
    return any(text in error for text in TRANSIENT_ERRORS)


def completed(series, campaign):
    if not campaign:
        return False
    expected = campaign.get('cost_protocol', {}).get('order', [])
    actual = [(r['arm'], r['run_number'], r['seed']) for r in campaign.get('runs', [])]
    return (bool(expected) and actual == [tuple(row) for row in expected]
            and all(s.get('status') == 'complete' and s.get('restoration') == 'verified'
                    for s in [series, campaign])
            and all(r.get('validation', {}).get('status') == 'passed' for r in campaign['runs']))


def reviewed_resume(initial, status, reason):
    """Resume an inspected infrastructure stop without erasing attempt history."""
    if not reason.strip() or status.get('status') != 'requires_review':
        raise ValueError('A stopped supervisor and documented review are required')
    series_path = Path(status['series_status'])
    campaign_path = Path(status['campaign_status'])
    series, campaign = read(series_path), read(campaign_path)
    if any(s.get('status') != 'failed' or s.get('restoration') != 'verified'
           or s.get('requires_user_review') for s in (series, campaign)):
        raise ValueError('Reviewed resume cannot bypass restoration or scientific admission')
    status.setdefault('reviewed_resumptions', []).append(dict(
        timestamp=time.time(), reason=reason, previous_completed_trials=status.get('completed_trials'),
        retained_trials=len(campaign['runs']), series_status=str(series_path),
        campaign_status=str(campaign_path)))
    status.update(completed_trials=len(campaign['runs']))
    status.pop('reason', None)
    status.pop('finished_epoch', None)
    return initial + ['--resume-series', str(series_path.parent),
                      '--resume-campaign', str(campaign_path.parent)]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--launch-record', type=Path, required=True)
    parser.add_argument('--maximum-consecutive-failures', type=int, default=3)
    parser.add_argument('--retry-delay-seconds', type=int, default=120)
    parser.add_argument('--resume-reviewed-reason', help='Continue a stopped supervisor after a documented infrastructure review')
    args = parser.parse_args()
    if args.maximum_consecutive_failures < 1 or args.retry_delay_seconds < 0:
        parser.error('Use a positive retry limit and nonnegative delay')
    record = read(args.launch_record)
    initial = list(record['argv'])
    if '--resume-series' in initial or '--resume-campaign' in initial:
        parser.error('The launch record must describe a fresh series')
    root = Path(initial[initial.index('--evidence-root') + 1])
    root.mkdir(parents=True, exist_ok=True)
    lock = (root / 'supervisor.lock').open('a')
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    status_path = root / 'supervisor-status.json'
    if status_path.exists():
        if not args.resume_reviewed_reason:
            parser.error('Existing supervisor evidence will not be overwritten. Review it before resuming.')
        status = read(status_path)
        argv = reviewed_resume(initial, status, args.resume_reviewed_reason)
    else:
        if args.resume_reviewed_reason:
            parser.error('No existing supervisor to resume')
        status = dict(status='starting', started_epoch=time.time(), attempts=[], completed_trials=0)
        argv = initial
    if shutil.which('caffeinate'):
        awake = subprocess.Popen(['caffeinate', '-is', '-w', str(os.getpid())])
        status['awake_pid'] = awake.pid
    write(status_path, status)
    failures = 0
    while True:
        known = set(root.glob('matched-series-*/series-status.json'))
        before = status['completed_trials']
        number = len(status['attempts']) + 1
        log_path = root / f'execution-attempt-{number}.log'
        if log_path.exists():
            raise RuntimeError('Refusing to overwrite an existing attempt log: ' + str(log_path))
        attempt = dict(started_epoch=time.time(), argv=argv, log=str(log_path))
        status['attempts'].append(attempt)
        status.update(status='running')
        with log_path.open('w') as log:
            process = subprocess.Popen(argv, cwd=record['cwd'], stdout=log,
                                       stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL)
            attempt['pid'] = process.pid
            write(status_path, status)
            attempt['exit_code'] = process.wait()
        attempt['finished_epoch'] = time.time()
        created = set(root.glob('matched-series-*/series-status.json')) - known
        if len(created) != 1:
            status.update(status='requires_review', reason='No unique series status. Inspect the attempt log.')
            write(status_path, status)
            return 1
        series_path = created.pop()
        series = read(series_path)
        campaign_path = Path(series['campaign_directory']) / 'campaign-status.json' if series.get('campaign_directory') else None
        campaign = read(campaign_path) if campaign_path and campaign_path.exists() else None
        status.update(series_status=str(series_path),
                      campaign_status=str(campaign_path) if campaign_path else None,
                      completed_trials=len(campaign.get('runs', [])) if campaign else 0)
        if completed(series, campaign):
            status.update(status='complete', finished_epoch=time.time())
            write(status_path, status)
            return 0
        failures = failures + 1 if status['completed_trials'] == before else 1
        if not retry_allowed(series, campaign) or failures >= args.maximum_consecutive_failures:
            status.update(status='requires_review', reason=series.get('error', 'Inspect campaign evidence'),
                          consecutive_failures=failures, finished_epoch=time.time())
            write(status_path, status)
            return 1
        # Accepted trials stay in place. Only the interrupted condition is repeated.
        argv = initial + (['--resume-series', str(series_path.parent),
                          '--resume-campaign', str(campaign_path.parent)] if campaign else [])
        status.update(status='waiting_to_retry', next_attempt_epoch=time.time() + args.retry_delay_seconds,
                      consecutive_failures=failures)
        write(status_path, status)
        time.sleep(args.retry_delay_seconds)


if __name__ == '__main__':
    sys.exit(main())
