"""Separate capacity and message-age assumptions in the saved Kafka pilots.

This retrospective diagnostic keeps the observed assignment and oldest-first
service assumption fixed. It does not rerun the published decision rule or
interpret its lag constraint as a published per-message latency forecast.
"""
import argparse
import hashlib
import json
import math
from pathlib import Path


def forecast(records, owners, capacities, release, include_age, deadline=20):
    elapsed = [0.0] * len(capacities)
    latencies = []
    for record in sorted(records, key=lambda row: (row[3], row[0])):
        consumer = owners[record[1]]
        if capacities[consumer] <= 0:
            raise ValueError('Capacity must be positive')
        elapsed[consumer] += 1 / capacities[consumer]
        age = release - record[3] if include_age else 0
        if include_age and age < 0:
            raise ValueError('Initial messages must exist before release')
        latencies.append(age + elapsed[consumer])
    if not latencies:
        raise ValueError('The waiting cohort is empty')
    misses = sum(value > deadline for value in latencies)
    allowed = len(latencies) - math.ceil(.99 * len(latencies))
    return dict(p99_seconds=sorted(latencies)[math.ceil(.99*len(latencies))-1],
                deadline_misses=misses, meets_deadline_objective=misses <= allowed)


def assigned_rates(rates, owners, count):
    result = [0.0] * count
    for rate, consumer in zip(rates, owners):
        result[consumer] += rate
    return result


VARIANTS = [('A', False, False), ('B', True, False),
            ('C', False, True), ('D', True, True)]


def analyze_pilot(directory):
    execution = json.loads((directory/'execution.json').read_text())
    digest = hashlib.sha256((directory/'raw-evidence.tar.gz').read_bytes()).hexdigest()
    if digest != execution['archive_sha256']:
        raise ValueError('Raw archive hash does not match its execution record')
    root = directory/'extracted'/execution['token']
    summaries = json.loads((root/'summary.json').read_text())
    decision = json.loads((root/'decision.json').read_text())
    calibration = next(s for s in summaries if s['calibration'])
    individual = [c['task_rate'] for c in calibration['consumers']]
    pooled = [decision['pooled_task_capacity']] * len(individual)
    rows = []
    for summary in summaries:
        if summary['calibration']:
            continue
        episode = root/summary['id']
        owners = json.loads((episode/'manifest.json').read_text())['owners']
        records = json.loads((episode/'producer-before.json').read_text())['ack']
        release = json.loads((episode/'release.json').read_text())['release_epoch']
        rates = json.loads((episode/'decision-inputs.json').read_text())['measured_rates']
        inputs = assigned_rates(rates, owners, len(individual))
        allowed = len(records) - math.ceil(.99*len(records))
        observed_pass = summary['deadline_misses']['20'] <= allowed
        predictions = {}
        saved = json.loads((episode/'predictions.json').read_text())
        old = next(c for c in saved['candidates'] if c['response'] == saved['observed_response'])
        for label, separate, age in VARIANTS:
            capacities = individual if separate else pooled
            prediction = forecast(records, owners, capacities, release, age)
            prediction.update(
                false_success=prediction['meets_deadline_objective'] and not observed_pass,
                false_failure=not prediction['meets_deadline_objective'] and observed_pass,
                assigned_input_below_task_rate=all(a < c for a, c in zip(inputs, capacities)))
            predictions[label] = prediction
        # A and C must reproduce the forecasts actually frozen before release.
        assert abs(predictions['A']['p99_seconds']-old['p99_age_blind']) < 1e-8
        assert abs(predictions['C']['p99_seconds']-old['p99_age_aware']) < 1e-8
        rows.append(dict(id=summary['id'], observed_deadline_success=observed_pass,
            observed_deadline_misses=summary['deadline_misses']['20'],
            observed_completed_p99_seconds=summary['initial_completion_p99'],
            unfinished=summary['initial_unfinished'], initial_count=len(records),
            consumer_arrival_rates=inputs, predictions=predictions))
    return dict(token=execution['token'], archive_sha256=digest,
        individual_task_rates=individual, pooled_task_rate=pooled[0], rows=rows,
        error_counts={label: dict(
            false_successes=sum(r['predictions'][label]['false_success'] for r in rows),
            false_failures=sum(r['predictions'][label]['false_failure'] for r in rows))
            for label, _, _ in VARIANTS})


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('pilots', type=Path, nargs='+')
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    pilots = [analyze_pilot(p) for p in args.pilots]
    result = dict(kind='retrospective 2 by 2 prediction ablation', pilots=pilots,
        variants={label: dict(individual_capacity=separate, include_age=age)
                  for label, separate, age in VARIANTS},
        limitations=[
            'Task-processing rates are not full effective pipeline capacities.',
            'Oldest-first processing across partitions is held fixed in all four diagnostics; Kafka does not guarantee it.',
            'Only executed assignments have observed outcomes; predictions for unexecuted alternatives are not validated.',
            'The 16 episodes share deployments and diagnostics and are not 16 independent calibration replicates.',
            'The baseline assigns about 300 messages/s to a consumer measured below that task rate. Capacity alone challenges continuing-input recovery.',
            'A finite waiting cohort can meet a deadline even when continuing input overloads a consumer; these are different objectives.'])
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2, allow_nan=False)+'\n')
    print(json.dumps({p['token']: p['error_counts'] for p in pilots}, indent=2))


if __name__ == '__main__':
    main()
