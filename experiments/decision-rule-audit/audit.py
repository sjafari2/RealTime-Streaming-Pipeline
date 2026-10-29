"""Deterministic decision-rule checks, not Kafka measurements.

CC-MWF decisions are read from an execution of the pinned upstream C# code.
Ezzeddine's two-stage action logic is reconstructed from the 2024 paper;
the same action branches were checked in the full 2025 extension on 2026-10-29.
The paper omits its Least-Loaded packing pseudocode, so all item permutations
are checked for the small examples; an exact packing oracle checks feasibility.
"""

import argparse
import itertools
import json
import math
from pathlib import Path

ALPHA = 5 / 6
PROFILES = (("Small", 500, 0.5), ("Medium", 1000, 1.0), ("Large", 2500, 2.5))


def loads(values, assignment, n):
    return [sum(v for v, owner in zip(values, assignment) if owner == c) for c in range(n)]


def feasible(rates, lags, assignment, capacities, factor=1, horizon=None):
    r = loads(rates, assignment, len(capacities))
    b = loads(lags, assignment, len(capacities))
    return all(r[c] + (b[c] / horizon if horizon else 0) <= factor * cap + 1e-9
               for c, cap in enumerate(capacities))


def recovery(rates, lags, assignment, capacities, pause=0):
    """Global blocking interval, followed by constant-rate work-conserving service.

    This is time to zero fluid backlog under continuing input, not message latency
    and not the proposal's <=100-offset, 30-second recovery criterion.
    """
    r = loads(rates, assignment, len(capacities))
    b = loads(lags, assignment, len(capacities))
    times = []
    for cap, rate, backlog in zip(capacities, r, b):
        waiting = backlog + rate * pause
        if waiting == 0 and rate < cap:
            times.append(0)
        elif cap <= rate:
            return math.inf
        else:
            times.append(pause + waiting / (cap - rate))
    return max(times, default=0)


def packing_oracle(rates, capacities, factor=ALPHA):
    return next((list(a) for a in itertools.product(range(len(capacities)), repeat=len(rates))
                 if feasible(rates, [0] * len(rates), a, capacities, factor)), None)


def cheapest_feasible_fleet(rates):
    candidates = []
    for n in range(1, len(rates) + 1):
        for indices in itertools.combinations_with_replacement(range(3), n):
            profiles = [PROFILES[i] for i in indices]
            assignment = packing_oracle(rates, [p[1] for p in profiles])
            if assignment is not None:
                candidates.append((sum(p[2] + 0.01 for p in profiles), profiles, assignment))
    return min(candidates, key=lambda x: x[0])


def pack_2d(rates, lags, mu, window, factor, order):
    """Least current-rate feasible bin; oversize partitions get a dedicated bin.

    This is a stated reconstruction choice, not author source. Counterexamples
    below produce the same decisions for every item order.
    """
    bins = []
    assignment = [None] * len(rates)
    for p in order:
        possible = [c for c, (r, b) in enumerate(bins)
                    if r + rates[p] < mu * factor and b + lags[p] < mu * window * factor]
        if possible:
            c = min(possible, key=lambda c: bins[c][0])
            bins[c][0] += rates[p]
            bins[c][1] += lags[p]
        else:
            c = len(bins)
            bins.append([rates[p], lags[p]])
        assignment[p] = c
    return assignment, len(bins)


def ezzeddine_decision(rates, lags, current, n, mu, window, pause, order):
    up, up_count = pack_2d(rates, lags, mu, window, 0.8, order)
    down, down_count = pack_2d(rates, lags, mu, window, 0.4, order)
    old_rates, old_lags = loads(rates, current, n), loads(lags, current, n)
    if up_count > n:
        trigger = "UP"
    elif down_count < n:
        trigger = "DOWN"
    elif any(r > mu * 0.8 or b > mu * window * 0.8 for r, b in zip(old_rates, old_lags)):
        trigger = "REASS"
    else:
        return "NONE", list(current), n
    projected = [b + r * pause for r, b in zip(rates, lags)]
    plan, count = pack_2d(rates, projected, mu, window, 0.4 if trigger == "DOWN" else 0.8, order)
    if trigger == "DOWN":
        return ("DOWN", plan, count) if count < n else ("NONE", list(current), n)
    return ("UP" if count > n else "REASS"), plan, count


def initial_cohort(rates, lags, assignment, capacities, pause=0, deadline=10):
    """All initial messages created at t=0; initial messages served before new ones.

    An explicit feasible FIFO schedule for this constructed example. This is
    not an assumption about Kafka's cross-partition polling order in real runs.
    """
    counts = loads(lags, assignment, len(capacities))
    completions = sorted(pause + k / cap for cap, count in zip(capacities, counts)
                         for k in range(1, int(count) + 1))
    late = sum(t > deadline + 1e-10 for t in completions)
    return {"count": len(completions), "p99_seconds": completions[math.ceil(.99 * len(completions)) - 1],
            "max_seconds": completions[-1], "deadline_misses": late,
            "deadline_miss_percent": late / len(completions) * 100}


def main(output, source_path):
    folder = Path(__file__).resolve().parent
    text = Path(source_path).read_text()
    source = json.loads(text[text.index('{'):])
    checks = []

    packing = source["packing"]
    assert packing["backlog_after_10_seconds"] == 1000
    assert packing_oracle([1100, 50, 50], [500, 1000], factor=1) is None
    score, fleet, assignment = cheapest_feasible_fleet([1100, 50, 50])
    assert [p[0] for p in fleet] == ["Large"]
    checks.append("Aggregate-capacity counterexample reproduced in original C#; exact feasible fleet found.")

    rates, lags, current, caps = [790, 790, 790, 250, 250], [10000, 10000, 10000, 0, 0], [0, 0, 0, 1, 1], [2500] * 3
    assert source["stickiness"]["backlog_after_10_seconds"] == 28700
    assert not feasible(rates, lags, current, caps, ALPHA, 10)
    candidates = [a for a in itertools.product(range(3), repeat=5)
                  if feasible(rates, lags, a, caps, ALPHA, 10)]
    repaired = min(candidates, key=lambda a: (sum(x != y for x, y in zip(a, current)),
                                            recovery(rates, lags, a, caps, 5), a))
    sticky_before = recovery(rates, lags, current, caps)
    sticky_after = recovery(rates, lags, repaired, caps, 5)
    assert sticky_after < sticky_before
    checks.append("Lag-aware feasibility repair exists in the same fleet, with three moves and five-second global pause.")

    # Ezzeddine's published logic reacts to the concentrated backlog in this case.
    sticky_ezz = []
    for order in itertools.permutations(range(5)):
        action, plan, n = ezzeddine_decision(rates, lags, current, 3, 2500, 10, 5, order)
        assert action == "REASS" and n == 3
        assert recovery(rates, lags, plan, caps, 5) < sticky_before
        sticky_ezz.append(recovery(rates, lags, plan, caps, 5))
    checks.append("Ezzeddine 2024 handles the concentrated-backlog state for all 120 item orders.")

    # An avoidable latency regression: its action adds pause time even when input
    # during that pause has been included in the packing constraint.
    er, eb, old = [100, 100], [18000, 3000], [0, 0]
    ezz_cases = []
    for order in itertools.permutations(range(2)):
        action, plan, n = ezzeddine_decision(er, eb, old, 2, 2500, 10, 5, order)
        assert action == "REASS" and n == 2
        old_metrics = initial_cohort(er, eb, old, [2500] * 2)
        new_metrics = initial_cohort(er, eb, plan, [2500] * 2, 5)
        assert old_metrics["deadline_misses"] == 0 and new_metrics["deadline_misses"] == 5500
        # Admission check for this state: reject a transition that turns a
        # feasible initial-cohort deadline into an infeasible one.
        accept = not (old_metrics["max_seconds"] <= 10 < new_metrics["max_seconds"])
        assert not accept
        ezz_cases.append({"order": list(order), "action": action, "assignment": plan,
                          "unchanged": old_metrics, "published_rule": new_metrics,
                          "admission_guard": "KEEP", "fixed": old_metrics})
    checks.append("Elapsed-pause counterexample survives both orders; prospective lag is included, not omitted.")

    # Sensitivity: for this state the harmful-action boundary is analytically
    # d > 10 - 18000/2500 = 2.8 s. At d=0 the move is helpful, so a blanket
    # prohibition on reassignment would be wrong.
    sensitivity = []
    for pause in [0, 1, 2, 2.8, 3, 5]:
        action, plan, n = ezzeddine_decision(er, eb, old, 2, 2500, 10, pause, (0, 1))
        metrics = initial_cohort(er, eb, plan, [2500] * n, pause)
        sensitivity.append({"pause_seconds": pause, "action": action, **metrics})
    assert sensitivity[0]["p99_seconds"] < old_metrics["p99_seconds"]
    assert sensitivity[3]["deadline_misses"] == 0 and sensitivity[4]["deadline_misses"] > 0
    checks.append("Pause sensitivity includes beneficial and harmful moves; a zero-pause negative control passes.")

    # No action is needed when the current assignment is comfortably feasible.
    healthy = ezzeddine_decision([800, 800], [100, 100], [0, 1], 2, 2500, 10, 5, (0, 1))
    assert healthy[0] == "NONE"
    # A partition above every available capacity is outside both repairs' scope.
    assert packing_oracle([2600, 50, 50], [2500] * 3, factor=1) is None
    checks.append("Healthy no-action and physically infeasible single-partition controls pass.")

    # Adding net-drain demand unconditionally would solve a different objective.
    r, b = 1900, 1000
    assert r < .8 * 2500 and b < .8 * 2500 * 10
    assert b / (2500 - r) > b / 2500
    checks.append("Snapshot waiting/latency and time to zero backlog are kept distinct.")

    result = {
        "scope": "Constructed deterministic cases; source execution plus mathematical oracle. No Nautilus trials.",
        "source_revision": source["upstream_revision"], "checks": checks,
        "cc_packing": {"original": packing, "repair_profiles": [p[0] for p in fleet],
                       "repair_assignment": assignment, "repair_cost_per_second": sum(p[2] for p in fleet),
                       "steady_backlog_growth_original_msg_s": 100, "steady_backlog_growth_repair_msg_s": 0},
        "cc_stickiness": {"original": source["stickiness"], "repair_assignment": list(repaired),
                          "moved_partitions": sum(x != y for x, y in zip(repaired, current)),
                          "no_action_fluid_recovery_seconds": sticky_before,
                          "repair_fluid_recovery_with_5s_pause_seconds": sticky_after,
                          "ezzeddine_recovery_seconds_range": [min(sticky_ezz), max(sticky_ezz)],
                          "same_fleet_cost_per_second": 7.5},
        "ezzeddine_elapsed_pause": {"rates": er, "initial_lags": eb, "capacity_each": 2500,
                                   "latency_target_seconds": 10, "item_orders": ezz_cases,
                                   "pause_sensitivity": sensitivity,
                                   "no_action_fluid_recovery_seconds": recovery(er, eb, old, [2500] * 2),
                                   "reassignment_fluid_recovery_seconds": recovery(er, eb, [0, 1], [2500] * 2, 5)},
        "limits": ["2024 and 2025 action branches verified; this original packing helper remains a reconstruction. See the 20261029 transition audit for the source-informed packing sensitivity.",
                   "Packing helper is a reconstruction; all item orders checked, not author implementation.",
                   "Repair uses a tiny exhaustive oracle, not a scalable controller.",
                   "Transition guard tested with known creation time, exact capacities and constant arrivals.",
                   "No claim of novelty, general superiority, production correctness, or reproduction of published tables."]
    }
    Path(output).write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps({"checks_passed": len(checks), "sticky_recovery": [sticky_before, sticky_after],
                      "ezz_old_vs_action_p99": [old_metrics["p99_seconds"], ezz_cases[0]["published_rule"]["p99_seconds"]],
                      "output": str(output)}, indent=2))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", default=str(Path(__file__).with_name("results.json")))
    parser.add_argument("--source-output", default=str(Path(__file__).with_name("upstream-execution.txt")))
    args = parser.parse_args()
    main(args.output, args.source_output)
