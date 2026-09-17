"""Summarize backlog trends during continuous input, without bridging monitoring gaps."""
import argparse
import json
from pathlib import Path


def window(snapshots, start, end, max_gap=3):
    selected = [p for p in snapshots if start <= p['timestamp'] <= end]
    segments, current = [], []
    for point in selected:
        valid = point.get('valid') and point.get('processing_backlog') is not None
        if not valid:
            if current: segments.append(current)
            current = []
            continue
        if current:
            previous = current[-1]
            dt = point['timestamp']-previous['timestamp']
            reset = any(point.get(name, {}).get(k, v) < v for name in ('highs', 'positions') for k,v in previous.get(name, {}).items())
            if not (0 < dt <= max_gap) or point.get('owners') != previous.get('owners') or reset:
                segments.append(current);current = []
        current.append(point)
    if current: segments.append(current)
    area = covered = change = 0
    records = []
    for segment in segments:
        if len(segment) < 2: continue
        duration = segment[-1]['timestamp']-segment[0]['timestamp']
        delta = segment[-1]['processing_backlog']-segment[0]['processing_backlog']
        covered += duration;change += delta
        for a,b in zip(segment,segment[1:]):
            area += (a['processing_backlog']+b['processing_backlog'])*.5*(b['timestamp']-a['timestamp'])
        records.append(dict(start=segment[0]['timestamp'],end=segment[-1]['timestamp'],
            growth_offsets_per_second=delta/duration,first=segment[0]['processing_backlog'],last=segment[-1]['processing_backlog']))
    return dict(start=start,end=end,covered_seconds=covered,coverage_fraction=covered/(end-start),
        covered_interval_growth_offsets_per_second=change/covered if covered else None,
        time_weighted_mean_backlog=area/covered if covered else None,
        peak_observed_backlog=max((p['processing_backlog'] for s in segments for p in s),default=None),
        segments=records)


def analyze(directory):
    manifest=json.loads((directory/'manifest.json').read_text())
    lag=json.loads((directory/'lag-summary.json').read_text())
    start,end=manifest['evaluation_start_epoch'],manifest['producer_end_epoch']
    windows={f'evaluation_minutes_{i//60}_{min(i+300,int(end-start))//60}':window(lag['snapshots'],start+i,min(start+i+300,end)) for i in range(0,int(end-start),300)}
    windows['final_ten_minutes']=window(lag['snapshots'],max(start,end-600),end)
    return dict(run_id=manifest['run_id'],windows=windows,
        interpretation='Continuous-production interval only. Growth summarizes covered, same-owner segments; it is not extrapolated over missing data. Inspect segment direction and coverage, input/completion throughput and resource traces. No automatic stable/unstable threshold or infinite-horizon guarantee.')


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('directory',type=Path)
    args=parser.parse_args()
    (args.directory/'stability-summary.json').write_text(json.dumps(analyze(args.directory),indent=2,allow_nan=False)+'\n')
