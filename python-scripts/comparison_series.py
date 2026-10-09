"""Plotting series shared by the scheduled intervention comparisons."""
import json
import math
import numpy as np
from evidence_io import event_paths, open_events


def lag_lines(manifest, lag):
    x, total, per_owner, growth = [], [], [[],[],[]], []
    previous = None
    for row in lag['snapshots']:
        t = (row['timestamp']-manifest['evaluation_start_epoch'])/60
        valid = row['valid'] and row.get('total_lag') is not None
        if valid and previous is not None:
            broken = (row['timestamp']-previous['timestamp'] > lag['parameters']['max_gap'] or
                      row['owners'] != previous['owners'] or any(row[field][k] < v
                      for field in ('highs','positions') for k,v in previous[field].items()))
            if broken:
                x.append(t); total.append(np.nan); growth.append(np.nan)
                for values in per_owner: values.append(np.nan)
        x.append(t); total.append(row['total_lag'] if valid else np.nan)
        value = row.get('window_processing_backlog_growth_offsets_per_second')
        growth.append(value if valid and value is not None else np.nan)
        for i, values in enumerate(per_owner):
            values.append(sum(v for k,v in row.get('lags',{}).items()
                              if row['owners'][k].split('/')[0] == f'consumer-sts-{i}') if valid else np.nan)
        previous = row if valid else None
    return x, total, per_owner, growth

def skew_lines(manifest, lag):
    """Show unavailable intervals as gaps, including missing snapshot timestamps."""
    x, raw, smooth = [], [], []
    previous = None
    for row in lag['snapshots']:
        t = (row['timestamp']-manifest['evaluation_start_epoch'])/60
        valid = row['valid'] and row.get('skew') is not None
        if valid and previous is not None:
            broken = (not 0 < row['timestamp']-previous['timestamp'] <= lag['parameters']['max_gap'] or
                      row['owners'] != previous['owners'] or any(row[field][k] < v
                      for field in ('highs','positions') for k,v in previous[field].items()))
            if broken:
                x.append(t); raw.append(np.nan); smooth.append(np.nan)
        x.append(t)
        raw.append(row['skew'] if valid else np.nan)
        value = row.get('window_mean_skew')
        smooth.append(value if valid and value is not None else np.nan)
        previous = row if valid else None
    return x, raw, smooth

def resource_line(series, name, pod, start, end, divisor):
    chosen = [s for s in series if s['metric'].get('__name__') == name and s['metric'].get('pod') == pod]
    stamps = [s for s in series if s['metric'].get('__name__') == name+'_scrape_timestamp_seconds' and s['metric'].get('pod') == pod]
    assert len(chosen) == len(stamps) == 1
    timestamps = {float(t):float(v) for t,v in stamps[0]['values']}
    x, y = [], []
    previous = None
    for t, value in chosen[0]['values']:
        t, value = float(t), float(value)
        if not start <= t <= end: continue
        if previous is not None and t-previous > 3:
            x.append((t-start)/60); y.append(np.nan)
        fresh = t in timestamps and 0 <= t-timestamps[t] <= 10
        x.append((t-start)/60); y.append(value/divisor if fresh and math.isfinite(value) and value >= 0 else np.nan)
        previous = t
    return x, y

def throughput_bins(directory, start, end, width=10):
    admitted, completed = {}, {}
    for path in event_paths(directory):
        with open_events(path) as stream:
            for line in stream:
                event = json.loads(line)
                if event['event'] == 'acknowledged':
                    target, timestamp = admitted, event['producer_timestamp']
                elif event['event'] == 'completed':
                    target, timestamp = completed, event['completion_timestamp']
                else:
                    continue
                if start <= timestamp < end:
                    identity = event['message_id']
                    target[identity] = min(target.get(identity, timestamp), timestamp)
    edges = np.arange(start, end + width, width)
    x = ((edges[:-1]+edges[1:])/2-start)/60
    return x, [np.histogram(list(values.values()), bins=edges)[0]/np.diff(edges)
               for values in (admitted, completed)]
