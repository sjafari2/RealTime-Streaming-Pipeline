"""Separate work pending at the decision from messages produced afterward."""
import argparse
import bisect
import json
import math
from pathlib import Path
import sqlite3
import sys
import tempfile

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'python-scripts'))
from evidence_io import event_paths, open_events


def summarize(rows, anchor, end, cutoff, deadline, pending=False):
    completed = [finished-produced for produced, finished in rows if finished is not None]
    if any(not math.isfinite(value) or value < 0 for value in completed):
        raise ValueError('Invalid completion clock in a cohort')
    completed.sort()
    missing = sum(finished is None for _, finished in rows)
    censored = sum(finished is None and produced + deadline > cutoff for produced, finished in rows)
    late = sum((finished-produced > deadline) if finished is not None else produced+deadline <= cutoff
               for produced, finished in rows)
    count = len(rows)
    result = dict(messages=count, completed_by_cutoff=len(completed), unfinished=missing,
        unfinished_fraction=missing/count if count else None,
        completed_by_evaluation_end=sum(finished is not None and finished <= end for _, finished in rows),
        completion_mean_seconds=sum(completed)/len(completed) if completed else None,
        completion_p99_seconds=completed[math.ceil(.99*len(completed))-1] if completed else None,
        completion_min_seconds=min(completed, default=None), completion_max_seconds=max(completed, default=None),
        deadline_seconds=deadline, deadline_misses=late, deadline_censored=censored,
        deadline_miss_fraction=late/count if count and not censored else None)
    if pending:
        ages = sorted(anchor-produced for produced, _ in rows)
        finish_times = sorted(finished-anchor for _, finished in rows if finished is not None)
        result.update(mean_age_at_decision_seconds=sum(ages)/count if count else None,
            maximum_age_at_decision_seconds=max(ages, default=None),
            pending_clearance_confirmed=not missing,
            all_pending_completed_seconds=(max(finish_times) if count else 0) if not missing else None,
            completion_fraction_after_decision=[dict(seconds=t, fraction=bisect.bisect_right(finish_times, t)/count)
                for t in range(0, math.floor(cutoff-anchor)+1, 10)] if count else [])
    return result


def analyze(directory):
    directory = Path(directory)
    manifest = json.loads((directory/'manifest.json').read_text())
    status = json.loads((directory/'runner-status.json').read_text())
    if status['status'] != 'complete' or status['run_id'] != manifest['run_id']:
        raise ValueError('Only validated runs can be compared')
    anchor = manifest['evaluation_start_epoch'] + 60
    start, end, cutoff = manifest['start_epoch'], manifest['producer_end_epoch'], manifest['drain_end_epoch']
    deadline = float(manifest['config']['SLO_THRESHOLD_MS'])/1000
    with tempfile.TemporaryDirectory() as temporary:
        db = sqlite3.connect(str(Path(temporary)/'cohorts.sqlite'))
        db.executescript('PRAGMA journal_mode=OFF; PRAGMA synchronous=OFF; PRAGMA cache_size=-65536; '
            'CREATE TABLE admitted(id TEXT PRIMARY KEY, produced REAL, ack REAL); '
            'CREATE TABLE completed(id TEXT PRIMARY KEY, finished REAL);')
        for path in event_paths(directory):
            with open_events(path) as stream:
                for line in stream:
                    row = json.loads(line)
                    if row['event'] == 'acknowledged' and start <= row['producer_timestamp'] < end:
                        db.execute('INSERT INTO admitted VALUES (?,?,?)',
                                   (row['message_id'], row['producer_timestamp'], row['timestamp']))
                    elif row['event'] == 'completed' and row['completion_timestamp'] <= cutoff:
                        db.execute('INSERT INTO completed VALUES (?,?) ON CONFLICT(id) DO UPDATE '
                                   'SET finished=min(finished,excluded.finished)',
                                   (row['message_id'], row['completion_timestamp']))
        db.commit()
        groups = dict(pending_at_decision=[], produced_after_decision=[],
                      completed_before_decision=[], earlier_produced_acknowledged_later=[])
        for produced, acknowledged, finished in db.execute('SELECT a.produced,a.ack,c.finished '
                                                           'FROM admitted a LEFT JOIN completed c USING(id)'):
            if not math.isfinite(produced) or not math.isfinite(acknowledged) or acknowledged > cutoff:
                raise ValueError('Invalid or post-cutoff acknowledgment')
            if produced >= anchor:
                group = 'produced_after_decision'
            elif acknowledged > anchor:
                group = 'earlier_produced_acknowledged_later'
            elif finished is not None and finished <= anchor:
                group = 'completed_before_decision'
            else:
                group = 'pending_at_decision'
            groups[group].append((produced, finished))
        db.close()
    return dict(run_id=manifest['run_id'], anchor_epoch=anchor,
        anchor_definition='Common scheduled decision at evaluation second 60; actual intervention delays are reported separately.',
        pending_definition='Acknowledged by the decision time but not completed then, including warm-up records.',
        late_ack_definition='Produced before the decision but acknowledged afterward; retained separately from already admitted pending work.',
        completion_definition='Earliest valid application completion by the drain cutoff, before commit acknowledgment.',
        cohorts={name:summarize(rows, anchor, end, cutoff, deadline, name=='pending_at_decision')
                 for name, rows in groups.items()})


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('run_directory', type=Path)
    args = parser.parse_args()
    result = analyze(args.run_directory)
    output = args.run_directory/'decision-cohort-summary.json'
    output.write_text(json.dumps(result, indent=2, allow_nan=False)+'\n')
    print(output)
