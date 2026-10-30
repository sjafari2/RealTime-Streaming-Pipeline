"""Paired message-age experiment using the published-rule reconstruction.

This is a synthetic single-decision audit. It is not a Kafka measurement or
an execution of the authors' released controller. Message timestamps affect
latency accounting; neither simulated service discipline changes its policy
between the younger and older cases.
"""
import argparse
import hashlib
import json
from pathlib import Path

from transition_audit import decide, simulate, spread_assignment, totals


def fixture():
    return ([40.0] * 12 + [2.5] * 48,
            [300] * 12 + [0] * 48,
            [0] * 12 + [p % 3 for p in range(12, 60)])


def candidate_choice(rows, deadline):
    """Diagnostic oracle over observed outcomes, not a deployable predictor."""
    eligible = [r for r in rows if r['unfinished_by_cutoff'] == 0
                and r['p99_completed_by_cutoff_seconds'] <= deadline + 1e-8]
    if not eligible:
        return None
    return min(eligible, key=lambda r: (r['requested_consumer_seconds_to_cutoff'],
                                       r['p99_completed_by_cutoff_seconds']))['response']


def run(output):
    rates, backlog, current = fixture()
    rows, checks = [], []
    for pause in (0, 1, 3, 5):
        decision = decide(rates, backlog, current, 3, 300, 10, pause)
        assert decision['projected_constraints_satisfied']
        for discipline in ('oldest', 'round_robin'):
            for age in (0, 6, 9.5):
                arms = [('keep3', current, 3, 0),
                        ('reconstructed_rule', decision['owners'], decision['consumers'], pause),
                        ('targeted_scale6', spread_assignment(6), 6, pause)]
                group = []
                for name, owners, n, interruption in arms:
                    value = simulate(rates, backlog, owners, [300] * n,
                                     pause=interruption, cutoff=20, deadline=10,
                                     initial_age=age, discipline=discipline)
                    row = dict(age_seconds=age, pause_seconds=pause, discipline=discipline,
                               response=name, rule_decision=decision['action'],
                               consumers=n, **value)
                    rows.append(row); group.append(row)
                checks.append(dict(age_seconds=age, pause_seconds=pause,
                                   discipline=discipline,
                                   cheapest_observed_feasible_candidate=candidate_choice(group, 10)))
    # Age changes deadline exposure, not the amount of work processed.
    for pause in (0, 1, 3, 5):
        for discipline in ('oldest', 'round_robin'):
            for arm in ('keep3', 'reconstructed_rule', 'targeted_scale6'):
                pair = [r for r in rows if r['pause_seconds'] == pause
                        and r['discipline'] == discipline and r['response'] == arm]
                assert len({r['completed_by_cutoff'] for r in pair}) == 1
                assert len({r['unfinished_by_cutoff'] for r in pair}) == 1
                for r in pair[1:]:
                    assert abs(r['p99_completed_by_cutoff_seconds']
                               - pair[0]['p99_completed_by_cutoff_seconds'] - r['age_seconds']) < 1e-7
    result = dict(kind='synthetic single-decision message-age audit',
        rates=rates, backlog=backlog, initial_owners=current,
        capacity_per_consumer_msg_s=300, latency_objective_seconds=10,
        observation_seconds_after_decision=20, continuing_input_seconds=30,
        publication='https://doi.org/10.1007/s42979-025-03740-9',
        reconstruction_sha256=hashlib.sha256(Path(__file__).with_name('transition_audit.py').read_bytes()).hexdigest(),
        rows=rows, candidate_checks=checks,
        limits=['Published action branches with reconstructed packing, not the verified author release.',
                'The diagnostic choice uses known synthetic outcomes; it is not a learned or validated predictor.',
                'The initial waiting cohort is distinct from messages arriving after the decision.',
                'Homogeneous capacity, a global pause and synthetic service orders are assumptions.',
                'Both original-rule successes and cases where neither intervention meets the objective are retained.',
                'The paper acknowledges violations; these cases do not invalidate its empirical findings.',
                'Message age is already known in scheduling; no novelty claim follows from this audit.'])
    output.mkdir(parents=True, exist_ok=True)
    (output / 'offline-results.json').write_text(json.dumps(result, indent=2, allow_nan=False) + '\n')
    print(json.dumps({'cases':len(rows), 'paired_controls_passed':True,
                      'output':str(output / 'offline-results.json')}))
    return result


if __name__ == '__main__':
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--output',type=Path,required=True)
    run(p.parse_args().output)
