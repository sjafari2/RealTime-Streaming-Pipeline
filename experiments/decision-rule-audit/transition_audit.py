"""Offline checks of the 2025 two-stage rule under skew. No cluster access.

The action branches follow the paper. Its packing helper is not specified in
full; this reconstruction retries packing with a fixed number of initially
empty bins, as in the linked repository, and labels all assumptions explicitly.
The queue experiment is synthetic, not a replay of measured Kafka completions.
"""

import argparse
import csv
import json
import math
from pathlib import Path


def totals(values, owners, count):
    result = [0.0] * count
    for value, owner in zip(values, owners):
        result[owner] += value
    return result


def pack(rates, backlog, capacity, window, factor, order=None, priority="rate"):
    """Try increasing fleet sizes, using the least loaded feasible bin.

    Oversized items are capped only for packing, following linked code. The
    original values remain unchanged and feasibility is checked separately.
    Thus a successful packing does not certify that an oversized item is safe.
    """
    order = list(order) if order is not None else sorted(range(len(rates)), key=lambda p: (-rates[p], p))
    limits = (capacity * factor, capacity * window * factor)
    effective = [(min(r, limits[0]), min(q, limits[1])) for r, q in zip(rates, backlog)]
    for count in range(1, len(rates) + 1):
        bins = [[0.0, 0.0] for _ in range(count)]
        assignment = [-1] * len(rates)
        for p in order:
            r, q = effective[p]
            eligible = [c for c, (a, b) in enumerate(bins)
                        if a + r <= limits[0] + 1e-9 and b + q <= limits[1] + 1e-9]
            if not eligible:
                break
            c = min(eligible, key=lambda c: (bins[c][0 if priority == "rate" else 1], c))
            bins[c][0] += r
            bins[c][1] += q
            assignment[p] = c
        else:
            return assignment, count
    raise AssertionError("Dedicated bins must accommodate the capped inputs")


def decide(rates, backlog, current, count, capacity, window, pause, up=.9, down=.4,
           order=None, priority="rate"):
    """Reconstruction of Algorithm 1 and its procedures, pages 8-9 (2025)."""
    _, up_count = pack(rates, backlog, capacity, window, up, order, priority)
    _, down_count = pack(rates, backlog, capacity, window, down, order, priority)
    old_rates, old_backlog = totals(rates, current, count), totals(backlog, current, count)
    if up_count > count:
        trigger = "UP"
    elif down_count < count:
        trigger = "DOWN"
    elif any(r > capacity * up or q > capacity * window * up for r, q in zip(old_rates, old_backlog)):
        trigger = "REASS"
    else:
        return {"action": "NONE", "trigger": "NONE", "owners": list(current), "consumers": count,
                "packed_bins": count, "projected_constraints_satisfied": True}
    projected = [q + r * pause for r, q in zip(rates, backlog)]
    factor = down if trigger == "DOWN" else up
    owners, bins = pack(rates, projected, capacity, window, factor, order, priority)
    if trigger == "DOWN" and bins >= count:
        return {"action": "NONE", "trigger": trigger, "owners": list(current), "consumers": count,
                "packed_bins": bins, "projected_constraints_satisfied": None}
    action = "DOWN" if trigger == "DOWN" else ("UP" if bins > count else "REASS")
    # REASS does not remove existing replicas, even if the helper used fewer bins.
    fleet = bins if action in ("UP", "DOWN") else count
    valid = all(r <= capacity * factor + 1e-9 and q <= capacity * window * factor + 1e-9
                for r, q in zip(totals(rates, owners, fleet), totals(projected, owners, fleet)))
    return {"action": action, "trigger": trigger, "owners": owners, "consumers": fleet,
            "packed_bins": bins, "projected_constraints_satisfied": valid}


def quantile(values, fraction=.99):
    values = sorted(values)
    return values[math.ceil(fraction * len(values)) - 1] if values else None


def simulate(rates, backlog, owners, capacities, pause=0, cutoff=12, deadline=10,
             production=30, initial_age=0, partition_ready=None, discipline="oldest"):
    """Deterministic arrivals and sequential, work-conserving consumer service.

    Each partition is FIFO. Across partitions, the default takes the oldest
    available message; a partition-round-robin alternative checks sensitivity.
    The initial cohort is separate from continuing arrivals. No duplicate,
    commit, broker, network, scheduler or CPU-contention model is included.
    """
    from collections import deque
    ready = partition_ready if partition_ready is not None else [pause] * len(rates)
    events = [[] for _ in capacities]
    serial = 0
    for p, (rate, initial, owner) in enumerate(zip(rates, backlog, owners)):
        for _ in range(int(initial)):
            events[owner].append((max(0.0, ready[p]), serial, p, -initial_age, True))
            serial += 1
        for k in range(1, math.floor(rate * production + 1e-9) + 1):
            arrival = k / rate
            events[owner].append((max(arrival, ready[p]), serial, p, arrival, False))
            serial += 1
    completed = []
    for c, arrivals in enumerate(events):
        arrivals.sort()
        queues = {}
        i, now, last_partition = 0, 0.0, -1
        while i < len(arrivals) or queues:
            if not queues:
                now = max(now, arrivals[i][0])
            while i < len(arrivals) and arrivals[i][0] <= now + 1e-10:
                release, ident, p, created, initial = arrivals[i]
                queues.setdefault(p, deque()).append((created, ident, initial))
                i += 1
            if discipline == "oldest":
                p = min(queues, key=lambda p: queues[p][0][:2])
            else:
                candidates = sorted(queues)
                p = next((p for p in candidates if p > last_partition), candidates[0])
                last_partition = p
            created, ident, initial = queues[p].popleft()
            if not queues[p]:
                del queues[p]
            now += 1 / capacities[c]
            completed.append((now, created, initial, p))
    initial = [x for x in completed if x[2]]
    observed = [x for x in initial if x[0] <= cutoff + 1e-8]
    late_completed = sum(t - created > deadline + 1e-8 for t, created, _, _ in observed)
    unfinished_late = sum(t > cutoff + 1e-8 and cutoff - created >= deadline - 1e-8
                          for t, created, _, _ in initial)
    return {
        "initial_cohort": len(initial), "completed_by_cutoff": len(observed),
        "unfinished_by_cutoff": len(initial) - len(observed),
        "p99_completed_by_cutoff_seconds": quantile([t - created for t, created, _, _ in observed]),
        "deadline_misses_known_by_cutoff": late_completed + unfinished_late,
        "deadline_censored_at_cutoff": len(initial) - len(observed) - unfinished_late,
        "initial_cohort_p99_followed_to_completion_seconds": quantile([t - created for t, created, _, _ in initial]),
        "initial_cohort_last_completion_seconds": max((t for t, _, _, _ in initial), default=0),
        "all_completions_by_cutoff": sum(t <= cutoff + 1e-8 for t, _, _, _ in completed),
        "all_completions_by_production_end": sum(t <= production + 1e-8 for t, _, _, _ in completed),
        "admitted_by_cutoff": sum(backlog) + sum(math.floor(r * min(cutoff, production) + 1e-9) for r in rates),
        "requested_consumer_seconds_to_cutoff": len(capacities) * cutoff,
    }


def predict_initial(backlog, owners, capacities, pause, age=0, deadline=10, cutoff=12):
    """Exact only for the stipulated oldest-initial-cohort-first schedule."""
    counts = totals(backlog, owners, len(capacities))
    times = [pause + k / cap for count, cap in zip(counts, capacities) for k in range(1, int(count) + 1)]
    return {"unfinished_by_cutoff": sum(t > cutoff + 1e-9 for t in times),
            "deadline_misses": sum(t + age > deadline + 1e-9 for t in times),
            "p99_followed_to_completion_seconds": quantile([t + age for t in times])}


def fixture(backlog_total=2900, leading_backlog=2490, concentrated_arrivals=False):
    rates = [40.0] * 12 + [2.5] * 48
    owners = [p % 3 for p in range(60)]
    if concentrated_arrivals:
        owners[:12] = [0] * 12
    backlog = [0] * 60
    remaining = backlog_total
    for p in (0, 3, 6, 9):
        backlog[p] = min(leading_backlog, remaining)
        remaining -= backlog[p]
    if remaining:
        raise ValueError("Fixture backlog exceeds the four initial partitions")
    return rates, backlog, owners


def spread_assignment(count):
    hot_order = [0, 3, 6, 9, 1, 4, 7, 10, 2, 5, 8, 11]
    owners = [p % count for p in range(60)]
    for i, p in enumerate(hot_order):
        owners[p] = i % count
    return owners


def main(output):
    output.mkdir(parents=True, exist_ok=True)
    scenarios = []
    for label, total, leading in [("smaller-synthetic", 2900, 2490), ("larger-synthetic", 5800, 2400)]:
        rates, backlog, current = fixture(total, leading)
        for pause in [0, 1, 3, 5]:
            decision = decide(rates, backlog, current, 3, 300, 10, pause)
            baseline_pause = pause if decision["action"] != "NONE" else 0
            prediction = predict_initial(backlog, decision["owners"], [300] * decision["consumers"], baseline_pause)
            keep_prediction = predict_initial(backlog, current, [300] * 3, 0)
            current_stable = all(r < 300 for r in totals(rates, current, 3))
            reject = current_stable and prediction["deadline_misses"] > keep_prediction["deadline_misses"]
            alternatives = {
                "keep3": (current, 3, 0),
                "redistribute3": (spread_assignment(3), 3, pause),
                "scale_redistribute6": (spread_assignment(6), 6, pause),
                "paper_rule": (decision["owners"], decision["consumers"], baseline_pause),
            }
            results = {}
            for arm, (owners, fleet, downtime) in alternatives.items():
                results[arm] = simulate(rates, backlog, owners, [300] * fleet, downtime)
            assert results["paper_rule"]["unfinished_by_cutoff"] == prediction["unfinished_by_cutoff"]
            assert math.isclose(results["paper_rule"]["initial_cohort_p99_followed_to_completion_seconds"],
                                prediction["p99_followed_to_completion_seconds"], abs_tol=1e-7)
            scenarios.append({"condition": label, "pause_seconds": pause, "rates": rates,
                              "initial_backlog": backlog, "current_assignment": current,
                              "decision": decision, "prediction": prediction,
                              "admission_check": "KEEP" if reject else "ACCEPT", "outcomes": results})
    import random
    rng = random.Random(20261029)
    sensitivity = []
    rates, backlog, current = fixture()
    for up in (.8, .9):
        for priority in ("rate", "lag"):
            for pause in (0, 1, 3, 5):
                # Preserve descending arrival rate; vary only equal-rate ties.
                orders = [list(range(60)), list(reversed(range(12))) + list(reversed(range(12, 60)))]
                for _ in range(64):
                    hot, cold = list(range(12)), list(range(12, 60))
                    rng.shuffle(hot); rng.shuffle(cold); orders.append(hot + cold)
                outcomes = []
                for order in orders:
                    decision = decide(rates, backlog, current, 3, 300, 10, pause, up=up,
                                      order=order, priority=priority)
                    prediction = predict_initial(backlog, decision["owners"], [300]*decision["consumers"], pause)
                    outcomes.append({"action": decision["action"], "consumers": decision["consumers"],
                                     "feasible": decision["projected_constraints_satisfied"], **prediction})
                sensitivity.append({"up_factor": up, "packing_priority": priority, "pause": pause,
                                    "orders": len(orders), "feasible_orders": sum(x["feasible"] for x in outcomes),
                                    "actions": sorted(set(x["action"] for x in outcomes)),
                                    "fleet_range": [min(x["consumers"] for x in outcomes), max(x["consumers"] for x in outcomes)],
                                    "deadline_miss_range": [min(x["deadline_misses"] for x in outcomes), max(x["deadline_misses"] for x in outcomes)],
                                    "initial_cohort_p99_range": [min(x["p99_followed_to_completion_seconds"] for x in outcomes),
                                                                 max(x["p99_followed_to_completion_seconds"] for x in outcomes)]})
    queue_sensitivity = []
    for discipline in ("oldest", "round_robin"):
        for true_capacity in (240, 300, 360):
            for age in (0, 5):
                decision = decide(rates, backlog, current, 3, 300, 10, 5)
                outcomes = {}
                for arm, owners, n, pause in [("keep3", current, 3, 0),
                                              ("paper_rule", decision["owners"], decision["consumers"], 5)]:
                    outcomes[arm] = simulate(rates, backlog, owners, [true_capacity]*n, pause,
                                             initial_age=age, discipline=discipline)
                keep_prediction = predict_initial(backlog, current, [300]*3, 0, age=age)
                action_prediction = predict_initial(backlog, decision["owners"], [300]*decision["consumers"], 5, age=age)
                reject = action_prediction["deadline_misses"] > keep_prediction["deadline_misses"]
                queue_sensitivity.append({"discipline": discipline, "true_capacity": true_capacity,
                                          "capacity_assumed_by_rule_and_guard": 300, "initial_age": age,
                                          "admission_check": "KEEP" if reject else "ACCEPT",
                                          "guard_increases_unfinished": reject and outcomes["keep3"]["unfinished_by_cutoff"] > outcomes["paper_rule"]["unfinished_by_cutoff"],
                                          "guard_increases_deadline_misses": reject and outcomes["keep3"]["deadline_misses_known_by_cutoff"] > outcomes["paper_rule"]["deadline_misses_known_by_cutoff"],
                                          "outcomes": outcomes})
    result = {"kind": "synthetic offline experiment", "capacity_each_msg_s": 300,
              "latency_target_seconds": 10, "common_cutoff_seconds_after_decision": 12,
              "continuing_input_seconds": 30, "initial_message_age_seconds": 0,
              "scenarios": scenarios, "packing_sensitivity": sensitivity,
              "queue_sensitivity": queue_sensitivity,
              "limits": ["Published action branches with reconstructed packing, not execution of the paper's exact release.",
                         "Synthetic initial burst and periodic continuing arrivals; not a replay of the 16 Nautilus trials.",
                         "Homogeneous 300 msg/s capacity is illustrative; measured added-consumer capacities are unavailable.",
                         "All-active-parts pause model follows the paper's blocking assumption; activation duration is not treated as a measured pause.",
                         "Admission check assumes oldest-first service and known capacity, age and pause; sensitivity reports assumption failures.",
                         "One decision with fixed assignments; no full closed-loop controller or novelty claim."]}
    (output / "results.json").write_text(json.dumps(result, indent=2, allow_nan=False) + "\n")
    rows = []
    for case in scenarios:
        for arm, values in case["outcomes"].items():
            rows.append({"condition": case["condition"], "pause_seconds": case["pause_seconds"],
                         "arm": arm, "rule_action": case["decision"]["action"] if arm == "paper_rule" else "",
                         **values})
    with (output / "outcomes.csv").open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=rows[0]); writer.writeheader(); writer.writerows(rows)
    for case in scenarios:
        values = case["outcomes"]["paper_rule"]
        print(case["condition"], case["pause_seconds"], case["decision"]["action"],
              case["decision"]["consumers"], case["decision"]["projected_constraints_satisfied"],
              round(values["initial_cohort_p99_followed_to_completion_seconds"], 4),
              values["unfinished_by_cutoff"], case["admission_check"])


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    main(parser.parse_args().output)
