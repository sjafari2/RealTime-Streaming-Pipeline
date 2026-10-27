#!/usr/bin/env python3
"""Compare existing responses under two measured starting-backlog conditions."""
import argparse
import importlib.util
import json
import math
import os
from pathlib import Path
import sys

ROOT = Path(os.environ.get('PIPELINE_REPOSITORY', Path(__file__).resolve().parents[2]))
spec = importlib.util.spec_from_file_location('four_condition_campaign', ROOT / 'experiments/four-condition-20260929/run.py')
base = importlib.util.module_from_spec(spec)
spec.loader.exec_module(base)
original_prepared = base.prepared
original_protocol = base.campaign_protocol
ARMS = ('keep3', 'redistribute3', 'scale_redistribute6', 'kafka_scale6')


def validate_specification(document):
    if document['aggregate_input_rate'] != 600 or document['hot_partition_count'] != 12:
        raise ValueError('This protocol uses 600 total messages/s and twelve high-input partitions')
    levels = document['warmup_aggregate_rates']
    if (set(levels) != {'smaller', 'larger'}
            or any(type(v) not in (int, float) or not math.isfinite(v) for v in levels.values())
            or not 0 < levels['smaller'] < levels['larger']):
        raise ValueError('Specify two ordered warm-up rates')
    if document['initial_hot_owner'] not in {'consumer-sts-0', 'consumer-sts-1', 'consumer-sts-2'}:
        raise ValueError('The hot owner must be one of the original consumers')
    if not document.get('calibration_reference') or not document.get('technical_evidence'):
        raise ValueError('Record the calibration and technical evidence before the campaign')


def trial_order():
    # Reverse both condition and response order in the second repetition.
    first = [(level + '__' + arm, 1, 91)
             for level in ('smaller', 'larger') for arm in ARMS]
    return first + [(arm, 2, 92) for arm, _, _ in reversed(first)]


def install(document):
    validate_specification(document)
    base.HOT_OWNER = document['initial_hot_owner']
    base.ORDER = trial_order()

    def prepared(arm, number, seed, reference=None, hot=None, gate=False,
                 aggregate_rate=600, hot_count=12, deadline_ms=1000):
        level, separator, response = arm.partition('__')
        response = response if separator else arm
        config, plan = original_prepared(response, number, seed, reference, hot, gate,
                                        aggregate_rate, hot_count, deadline_ms)
        if separator:
            rate = document['warmup_aggregate_rates'][level]
            config['data'].update(
                EXP_ID='backlog-' + level + '-' + response + '-run' + str(number),
                PRODUCER_RATE_SCHEDULE_JSON=json.dumps([
                    {'at_seconds': 0, 'rate': rate / 3},
                    {'at_seconds': 60, 'rate': aggregate_rate / 3}]))
            base.r.validate_config(config['data'])
        return config, plan

    def protocol(aggregate_rate=600, targeted_only=False, hot_count=12, deadline_ms=1000):
        result = original_protocol(aggregate_rate, False, hot_count, deadline_ms)
        result.update(document)
        result.update(performance_trials=16, order=[list(row) for row in trial_order()],
                      conditions={level + '__' + arm: level + ' starting backlog; ' + arm
                                  for level in ('smaller', 'larger') for arm in ARMS},
                      starting_conditions='Verified common initial ownership and original pod identities, nodes and resources. '
                          'Twelve partitions owned by ' + document['initial_hot_owner'] + ' receive 80 percent of input.',
                      study_purpose='Test how accumulated work changes the performance and cost of the four existing responses. '
                          'Warm-up history also changes message count and age; report these differences.',
                      calibration_reference=document['calibration_reference'],
                      lag_analysis_parameters=dict(window_samples=15, growth_window_samples=15,
                          export_step_seconds=2, hot_k=1., minimum_lag=10., persistence=.8, max_gap_seconds=3.))
        return result

    base.prepared, base.campaign_protocol = prepared, protocol


def main():
    parser = argparse.ArgumentParser(description=__doc__, add_help=False)
    parser.add_argument('--specification', type=Path, required=True)
    args, remaining = parser.parse_known_args()
    document = json.loads(args.specification.read_text())
    if '--execute' in remaining and document.get('status') != 'validated_before_performance_trials':
        raise ValueError('Pilot results must validate the specification before performance trials')
    install(document)
    # Raw logs remain on the cluster PVCs. Locally, retain a byte-verified
    # compressed copy so this campaign does not exhaust the analysis machine.
    from compress_evidence import compress
    complete = base.r.complete_run
    def complete_and_compress(plan=None):
        directory = complete(plan)
        if not (plan or {}).get('prepare_only'):
            compress(directory)
        return directory
    base.r.complete_run = complete_and_compress
    sys.argv = [sys.argv[0], '--aggregate-rate', '600', '--hot-partitions', '12', '--slo-ms', '1000'] + remaining
    base.main()


if __name__ == '__main__':
    main()
