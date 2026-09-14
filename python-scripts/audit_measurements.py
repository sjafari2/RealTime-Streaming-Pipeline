"""Audit retained evidence and summarize observed process CPU/RSS without filling gaps."""
import argparse
import json
import math
from pathlib import Path


def summarize_samples(values, stamps, start, end, max_gap=3, freshness=10):
    points = sorted((float(t), float(v)) for t, v in values if start <= float(t) <= end)
    valid = {}
    for t, v in points:
        stamp = stamps.get(t, math.nan)
        if math.isfinite(v) and v >= 0 and math.isfinite(stamp) and 0 <= t-stamp <= freshness:
            valid[t] = v
    area = covered = 0.0
    for (t, v), (u, w) in zip(points, points[1:]):
        if t in valid and u in valid and 0 < u-t <= max_gap:
            area += (v+w)*.5*(u-t)
            covered += u-t
    return dict(covered_seconds=covered, requested_window_seconds=end-start,
                covered_fraction=covered/(end-start) if end>start else None,
                time_weighted_mean=area/covered if covered else None,
                observed_integral=area if covered else None,
                peak_sampled=max(valid.values(), default=None))


def analyze(directory):
    directory = Path(directory)
    manifest = json.loads((directory/'manifest.json').read_text())
    data = json.loads((directory/'prometheus.json').read_text())
    if data.get('status') != 'success':
        raise ValueError('Prometheus export unsuccessful')
    issues, records = [], []
    for name in ('outcome-summary.json', 'lag-summary.json', 'execution-summary.json'):
        if not (directory/name).is_file(): issues.append('Missing analysis: '+name)
    finals = [json.loads(p.read_text()) for p in directory.rglob('final.json')]
    if not finals: issues.append('No process final records')
    for final in finals:
        role, incarnation = final['role'], final['incarnation']
        for suffix in ('cpu_percent', 'memory_bytes'):
            name = role+'_'+suffix
            selected = [s for s in data['data']['result'] if s['metric'].get('run_id') == manifest['run_id']
                        and s['metric'].get('incarnation') == incarnation]
            raw = [s for s in selected if s['metric'].get('__name__') == name]
            stamp = [s for s in selected if s['metric'].get('__name__') == name+'_scrape_timestamp_seconds']
            row = dict(pod=final['pod'], incarnation=incarnation, metric=name)
            if len(raw)!=1 or len(stamp)!=1:
                issues.append('Missing or duplicate process measurement/timestamp: '+incarnation+'/'+name)
                row['status']='unavailable'
            else:
                stamps={float(t):float(v) for t,v in stamp[0]['values']}
                row['windows']={label:summarize_samples(raw[0]['values'], stamps, manifest['evaluation_start_epoch'], end)
                                for label,end in [('evaluation',manifest['producer_end_epoch']),
                                                  ('evaluation_and_drain',manifest['drain_end_epoch'])]}
                row['status']='observed_intervals_only'
                if not row['windows']['evaluation_and_drain']['covered_seconds']:
                    issues.append('No fresh resource interval: '+incarnation+'/'+name)
            records.append(row)
    return dict(run_id=manifest['run_id'], issues=issues, process_resources=records,
                scope='Process CPU percent and RSS only; not container, broker or node utilization.',
                interpretation='CPU mean /100 gives cores; CPU integral /100 gives observed CPU-seconds. Memory units are bytes.',
                coverage_note='Fractions use the entire declared window, including time before scaled processes start. Not a live-process completeness verdict.',
                freshness_note='Scrape timestamps detect absent scrapes, not a stalled exporter sampling loop. Gaps remain unavailable; no extrapolation.',
                limitations=['This audit does not establish that all proposal metrics or all samples are present.',
                             'Original events, manifests, final records and raw monitoring exports must be retained.'])


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('directory',type=Path)
    args=parser.parse_args()
    (args.directory/'measurement-audit.json').write_text(json.dumps(analyze(args.directory),indent=2,allow_nan=False)+'\n')
