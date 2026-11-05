"""Resume interrupted matched trials without changing the experimental protocol."""
import argparse
import fcntl
import json
from pathlib import Path
import subprocess
import sys
import time

TRANSIENT_ERRORS = (
    'pod/container is not continuously ready',
    'New consumer enrollment timed out',
    'TLS handshake timeout', 'i/o timeout', 'context deadline exceeded',
    'ServiceUnavailable', 'connection reset by peer',
)


def read(path):
    return json.loads(path.read_text())


def write(path, value):
    temporary = path.with_suffix('.tmp')
    temporary.write_text(json.dumps(value, indent=2) + '\n')
    temporary.replace(path)


def retry_reason(series, campaign):
    """Only retry interrupted infrastructure attempts after verified restoration."""
    for state in (series, campaign):
        if state.get('status') != 'failed' or state.get('restoration') != 'verified':
            return None
        if state.get('requires_user_review'):
            return None
    error = series.get('error', '') + ' ' + campaign.get('error', '')
    return error if any(fragment in error for fragment in TRANSIENT_ERRORS) else None


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--launch-record', type=Path, required=True)
    parser.add_argument('--resume-series', type=Path, required=True)
    parser.add_argument('--maximum-consecutive-failures', type=int, default=3)
    parser.add_argument('--retry-delay-seconds', type=int, default=300)
    args = parser.parse_args()
    argv = list(read(args.launch_record)['argv'])
    root = Path(argv[argv.index('--evidence-root') + 1])
    campaign_path = Path(argv[argv.index('--resume-campaign') + 1]) / 'campaign-status.json'
    series_path = args.resume_series / 'series-status.json'
    argv[argv.index('--resume-series') + 1] = str(args.resume_series)
    status_path = root / 'supervisor-status.json'
    lock = (root / 'supervisor.lock').open('a')
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    status = dict(status='checking', started_epoch=time.time(), attempts=[],
                  maximum_consecutive_failures=args.maximum_consecutive_failures,
                  retry_delay_seconds=args.retry_delay_seconds)
    write(status_path, status)
    failures = 0
    while True:
        campaign, series = read(campaign_path), read(series_path)
        reason = retry_reason(series, campaign)
        if not reason:
            status.update(status='requires_review', reason='Resume eligibility or infrastructure-failure check failed')
            write(status_path, status)
            return 1
        completed_before = len(campaign['runs'])
        known = set(root.glob('matched-series-*/series-status.json'))
        attempt = dict(started_epoch=time.time(), retained_trials=completed_before,
                       prior_failure=reason, resumed_from=str(series_path.parent))
        log_path = root / ('execution-auto-' + time.strftime('%Y%m%d-%H%M%S') + '.log')
        attempt['log'] = str(log_path)
        status['attempts'].append(attempt)
        status.update(status='running', completed_trials=completed_before)
        write(status_path, status)
        with log_path.open('w') as log:
            process = subprocess.Popen(argv, stdout=log, stderr=subprocess.STDOUT)
            attempt['pid'] = process.pid
            write(status_path, status)
            attempt['exit_code'] = process.wait()
        attempt['finished_epoch'] = time.time()
        new = set(root.glob('matched-series-*/series-status.json')) - known
        if len(new) != 1:
            status.update(status='requires_review', reason='Cannot identify the resumed series unambiguously')
            write(status_path, status)
            return 1
        series_path = new.pop()
        series, campaign = read(series_path), read(campaign_path)
        status.update(completed_trials=len(campaign['runs']), series_status=str(series_path))
        if (series.get('status') == campaign.get('status') == 'complete'
                and series.get('restoration') == campaign.get('restoration') == 'verified'
                and len(campaign['runs']) == 16):
            status.update(status='complete', finished_epoch=time.time())
            write(status_path, status)
            return 0
        failures = failures + 1 if len(campaign['runs']) == completed_before else 1
        if not retry_reason(series, campaign) or failures >= args.maximum_consecutive_failures:
            status.update(status='requires_review', reason=series.get('error'),
                          consecutive_failures=failures)
            write(status_path, status)
            return 1
        argv[argv.index('--resume-series') + 1] = str(series_path.parent)
        status.update(status='waiting_to_retry', next_attempt_epoch=time.time() + args.retry_delay_seconds,
                      consecutive_failures=failures)
        write(status_path, status)
        time.sleep(args.retry_delay_seconds)


if __name__ == '__main__':
    sys.exit(main())
