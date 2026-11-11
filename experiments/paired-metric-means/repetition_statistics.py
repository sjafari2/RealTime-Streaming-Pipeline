"""Summarize independent repetitions without treating missing outcomes as zero."""
import math
import statistics


def complete_summary(values):
    """Require every repetition for a mean, retaining incomplete individual values."""
    values = list(values)
    valid = [v for v in values if v is not None and math.isfinite(v)]
    complete = bool(values) and len(valid) == len(values)
    return dict(
        mean=statistics.mean(valid) if complete else None,
        sample_sd=statistics.stdev(valid) if complete and len(valid) >= 2 else None,
        available_runs=len(valid),
        total_runs=len(values),
        individual=values,
    )


def validate_design(rows, conditions, arms, repetitions):
    """Reject duplicated, missing, or unexpected workload/response repetitions."""
    expected = {(c, a, n) for c in conditions for a in arms for n in repetitions}
    observed = [(r['backlog_condition'], r['arm'], r['run_number']) for r in rows]
    if len(observed) != len(set(observed)) or set(observed) != expected:
        raise ValueError('Each workload and response must have exactly the requested repetitions')
    ids = [r['run_id'] for r in rows]
    if len(ids) != len(set(ids)):
        raise ValueError('A run cannot contribute more than once')
