"""Outstanding work and recovery from valid, continuous observations."""

def outstanding(ack, done, timestamp):
    # Completion may precede the producer receiving its acknowledgment callback.
    if hasattr(ack, 'shape'):
        import numpy as np
        return int(np.count_nonzero((ack <= timestamp) & (done > timestamp)))
    return sum(a <= timestamp < d for a, d in zip(ack, done))

def recovery(snapshots, anchor, end, threshold, hold=30):
    candidate = previous = None
    for row in snapshots:
        t = row['timestamp']
        if not anchor <= t <= end:
            continue
        good = row.get('valid') and row.get('processing_backlog') is not None
        good = good and row['processing_backlog'] <= threshold
        if not good:
            candidate = previous = None
            continue
        if previous is None or not 0 < t-previous['timestamp'] <= 3 or row['owners'] != previous['owners'] or any(
                row[field][p] < value for field in ('highs', 'positions') for p, value in previous[field].items()):
            candidate = t
        previous = row
        if t-candidate >= hold:
            return dict(threshold_offsets=threshold, hold_seconds=hold,
                        first_low_epoch=candidate, confirmed_epoch=t,
                        onset_after_anchor_seconds=candidate-anchor,
                        confirmation_after_anchor_seconds=t-anchor)
    return dict(threshold_offsets=threshold, hold_seconds=hold, first_low_epoch=None,
                confirmed_epoch=None, onset_after_anchor_seconds=None,
                confirmation_after_anchor_seconds=None,
                unconfirmed_low_start_epoch=candidate,
                observed_low_duration_seconds=previous['timestamp']-candidate if previous is not None and candidate is not None else 0)
