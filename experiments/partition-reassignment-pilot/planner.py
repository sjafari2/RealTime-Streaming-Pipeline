"""Offline whole-partition candidate planning; this does not execute Kafka changes."""
import argparse
import copy
import json
import math
from pathlib import Path


def number(value, name, positive=False):
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        raise ValueError(name + ' must be finite')
    if value < 0 or (positive and value == 0):
        raise ValueError(name + ' is outside its allowed range')
    return value


def validate(snapshot):
    if snapshot.get('schema_version') != 1:
        raise ValueError('Unsupported snapshot schema')
    if snapshot.get('work_model') != 'equal_cost_records':
        raise ValueError('This pilot requires the same processing task for every record')
    if not snapshot.get('run_id') or type(snapshot.get('assignment_epoch')) is not int or snapshot['assignment_epoch'] < 0:
        raise ValueError('A run and assignment epoch are required')
    now = number(snapshot['observed_at'], 'observation time')
    age = number(snapshot['maximum_age_seconds'], 'maximum age', True)
    capacities = snapshot['capacity_records_per_second']
    if not capacities or any(not isinstance(c, str) or not c for c in capacities):
        raise ValueError('Consumer identities are required')
    for c, value in capacities.items():
        number(value, 'capacity for ' + c, True)
    expected = snapshot['expected_partitions']
    if not expected or len(expected) != len(set(expected)) or any(type(p) is not int or p < 0 for p in expected):
        raise ValueError('Expected partitions must be unique nonnegative integers')
    rows = snapshot['partitions']
    seen = set()
    for row in rows:
        p = row['partition']
        if type(p) is not int or p in seen or p not in expected:
            raise ValueError('Duplicate or unexpected partition')
        seen.add(p)
        if row['owner'] not in capacities or row.get('valid') is not True:
            raise ValueError('Missing valid ownership or measurement')
        timestamp = number(row['observed_at'], 'partition observation time')
        if not 0 <= now - timestamp <= age:
            raise ValueError('Stale or future partition observation')
        number(row['arrival_records_per_second'], 'arrival rate')
        number(row['processing_backlog'], 'processing backlog')
    if seen != set(expected):
        raise ValueError('Every partition must be observed exactly once')
    # Capacity needs its own calibration freshness, not a timestamp borrowed from lag.
    calibrated = number(snapshot['capacity_observed_at'], 'capacity observation time')
    limit = number(snapshot['capacity_maximum_age_seconds'], 'capacity maximum age', True)
    if not 0 <= now - calibrated <= limit:
        raise ValueError('Capacity calibration is stale or from the future')


def loads(rows, capacities):
    demand = dict.fromkeys(capacities, 0.0)
    for r in rows:
        demand[r['owner']] += r['arrival_records_per_second']
    return {c: demand[c] / capacities[c] for c in sorted(capacities)}


def plan(snapshot, target_utilization=.85, max_changed_partitions=8):
    """Greedy moves/swaps using explicit calibrated capacities; not an optimal policy.

    All proposed changes form one plan. Intermediate search states are not live
    actions. A partial plan that fails to reach the target is never released.
    """
    validate(snapshot)
    number(target_utilization, 'target utilization', True)
    if target_utilization >= 1 or type(max_changed_partitions) is not int or max_changed_partitions < 1:
        raise ValueError('Use a target below one and a positive partition-change budget')
    rows = sorted(copy.deepcopy(snapshot['partitions']), key=lambda r: r['partition'])
    caps = snapshot['capacity_records_per_second']
    before = loads(rows, caps)
    original = {r['partition']: r['owner'] for r in rows}
    result = dict(schema_version=1, run_id=snapshot['run_id'], assignment_epoch=snapshot['assignment_epoch'],
                  executable=False, before_utilization=before, after_utilization=before, moves=[],
                  status='no_action', reason='No measured consumer overload')
    if max(before.values()) <= 1:
        return result
    if sum(r['arrival_records_per_second'] for r in rows) > target_utilization * sum(caps.values()):
        result.update(status='infeasible', reason='Insufficient group capacity for the declared headroom')
        return result
    while max(loads(rows, caps).values()) > target_utilization:
        current = loads(rows, caps)
        options = []
        for i, row in enumerate(rows):
            if current[row['owner']] <= target_utilization:
                continue
            for dest in sorted(caps):
                if dest == row['owner']:
                    continue
                # Include a simple move and swaps with each destination partition.
                for j in [None] + [k for k, other in enumerate(rows) if other['owner'] == dest]:
                    trial = copy.deepcopy(rows)
                    source = row['owner']
                    trial[i]['owner'] = dest
                    if j is not None:
                        trial[j]['owner'] = source
                    after = loads(trial, caps)
                    changed = sum(r['owner'] != original[r['partition']] for r in trial)
                    # Never overload a previously healthy destination. Compare the
                    # full load vector so moving a tie in maximum load can help.
                    score = tuple(sorted(after.values(), reverse=True))
                    if changed > max_changed_partitions or after[dest] > target_utilization:
                        continue
                    if score >= tuple(sorted(current.values(), reverse=True)):
                        continue
                    options.append((score, changed, i, -1 if j is None else j, dest, trial))
        if not options:
            result.update(status='no_feasible_plan', reason='No greedy whole-partition plan reaches the target within the change budget')
            return result
        rows = min(options, key=lambda x: x[:-1])[-1]
    result.update(status='candidate', reason='Estimated load fits the declared headroom; live revalidation is required',
                  after_utilization=loads(rows, caps),
                  moves=[dict(partition=r['partition'], source=original[r['partition']], destination=r['owner'])
                         for r in rows if r['owner'] != original[r['partition']]])
    return result


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('snapshot', type=Path)
    parser.add_argument('--target-utilization', type=float, default=.85)
    args = parser.parse_args()
    print(json.dumps(plan(json.loads(args.snapshot.read_text()), args.target_utilization), indent=2, allow_nan=False))
