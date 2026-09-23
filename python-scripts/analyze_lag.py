#!/usr/bin/env python3
"""Compute proposal lag signals from exported Prometheus series with explicit missing coverage."""
import argparse
import json
import math
from pathlib import Path
import statistics
from lag_freshness import observation_validity


def signals(snapshots, window_samples=15, hot_k=1.0, minimum_lag=10.0, persistence=.8, max_gap=3.0):
    if not all(math.isfinite(x) for x in (hot_k, minimum_lag, persistence, max_gap)):
        raise ValueError('Settings must be finite')
    if window_samples < 1 or hot_k < 0 or minimum_lag < 0 or not 0 < persistence <= 1 or max_gap <= 0:
        raise ValueError('Invalid window, hotspot or gap settings')
    output = []
    chain = []
    area = covered = 0.0
    for snapshot in snapshots:
        row = dict(snapshot)
        row.update(growth_offsets_per_second=None, window_growth_offsets_per_second=None,
                   processing_backlog_growth_offsets_per_second=None, window_processing_backlog_growth_offsets_per_second=None,
                   window_mean_backlog=None, window_mean_skew=None, persistence=None, persistent_hot=None)
        if not row['valid']:
            chain = []
            output.append(row)
            continue
        values = list(row['lags'].values())
        total = sum(values)
        mean = statistics.mean(values)
        sigma = statistics.pstdev(values)
        hot = [p for p,v in row['lags'].items() if v > minimum_lag and v > mean + hot_k*sigma]
        row.update(total_lag=total, mean_partition_lag=mean, max_partition_lag=max(values),
                   population_stddev=sigma, skew=max(values)/mean if mean else 0, hot=hot)
        if chain:
            previous = chain[-1]
            dt = row['timestamp']-previous['timestamp']
            monotonic_offsets = all(row['positions'][p] >= previous['positions'][p] and
                                    row['highs'][p] >= previous['highs'][p] for p in row['lags'])
            contiguous = 0 < dt <= max_gap and row['owners'] == previous['owners'] and monotonic_offsets
            if contiguous:
                row['growth_offsets_per_second'] = (total-previous['total_lag'])/dt
                if 'processing_backlog' in row and 'processing_backlog' in previous:
                    row['processing_backlog_growth_offsets_per_second'] = (row['processing_backlog']-previous['processing_backlog'])/dt
                area += .5*(total+previous['total_lag'])*dt
                covered += dt
            else:
                chain = []
        chain.append(row)
        if len(chain) >= window_samples:
            window = chain[-window_samples:]
            row['window_mean_backlog'] = statistics.mean(x['total_lag'] for x in window)
            row['window_mean_skew'] = statistics.mean(x['skew'] for x in window)
            row['persistence'] = {p:sum(p in x['hot'] for x in window)/window_samples for p in row['lags']}
            row['persistent_hot'] = [p for p in hot if row['persistence'][p] >= persistence]
        if len(chain) > window_samples:
            first = chain[-window_samples-1]
            row['window_growth_offsets_per_second'] = (total-first['total_lag'])/(row['timestamp']-first['timestamp'])
            if 'processing_backlog' in row and 'processing_backlog' in first:
                row['window_processing_backlog_growth_offsets_per_second'] = (row['processing_backlog']-first['processing_backlog'])/(row['timestamp']-first['timestamp'])
        chain = chain[-(window_samples+1):]
        output.append(row)
    valid = [x for x in output if x['valid']]
    return dict(snapshots=output, covered_seconds=covered, backlog_area_offset_seconds=area if covered else None,
                time_weighted_mean_lag=area/covered if covered else None,
                peak_sampled_lag=max((x['total_lag'] for x in valid), default=None))


def analyze(directory, **options):
    options = {**dict(window_samples=15, hot_k=1.0, minimum_lag=10.0, persistence=.8, max_gap=3.0), **options}
    directory = Path(directory)
    manifest = json.loads((directory/'manifest.json').read_text())
    data = json.loads((directory/'prometheus.json').read_text())
    if data.get('status') != 'success': raise ValueError('Prometheus export was not successful')
    start,end = manifest['evaluation_start_epoch'],manifest['producer_end_epoch']
    config = manifest['config']
    expected = {f"{config['TOPIC_TITLE']}_{t}/{p}" for t in range(int(config['TOPIC_COUNT'])) for p in range(int(config['NUM_PARTITIONS']))}
    freshness = float(config.get('LAG_FRESHNESS_SECONDS',10))
    # Do not interpolate absent series. query_range already supplies a common evaluation grid.
    table = {}
    names = {'consumer_lag','consumer_lag_valid','consumer_lag_observed_timestamp_seconds',
             'consumer_lag_observation_age_seconds','consumer_lag_scrape_timestamp_seconds',
             'consumer_partition_owned','consumer_position_offset','consumer_high_offset','consumer_processing_backlog'}
    for series in data['data']['result']:
        labels = series['metric']; name=labels.get('__name__')
        if name not in names or labels.get('run_id') != manifest['run_id']: continue
        key=(labels.get('topic','')+'/'+labels.get('partition',''),labels.get('pod',''),labels.get('incarnation',''))
        for timestamp,value in series.get('values',[]):
            if start <= timestamp <= end:
                bucket=table.setdefault(timestamp,{}).setdefault(key,{})
                if name in bucket: raise ValueError('Duplicate Prometheus series for one process/partition')
                bucket[name]=float(value)
    clock = manifest.get('lag_freshness_clock', 'monotonic_scrape_v2' if any(
        row['metric'].get('__name__') == 'consumer_lag_observation_age_seconds' for row in data['data']['result'])
        else 'legacy_wall_clock_v1')
    snapshots=[]
    for timestamp,entries in sorted(table.items()):
        selected={}; reasons=[]
        for (partition,pod,incarnation), metrics in entries.items():
            if metrics.get('consumer_partition_owned') != 1: continue
            if partition in selected: reasons.append('Duplicate owner for '+partition)
            selected[partition]=(pod,incarnation,metrics)
        if set(selected)!=expected: reasons.append('Missing or unexpected partition ownership')
        lags={};owners={};positions={};highs={};backlog={};ages=[]
        for partition,(pod,incarnation,m) in selected.items():
            lag=m.get('consumer_lag',math.nan); pos=m.get('consumer_position_offset',math.nan)
            high=m.get('consumer_high_offset',math.nan); back=m.get('consumer_processing_backlog',math.nan)
            good, age = observation_validity(m, timestamp, freshness, clock)
            if not good:
                reasons.append('Invalid/stale observation for '+partition)
            lags[partition]=lag; positions[partition]=pos; highs[partition]=high;backlog[partition]=back
            owners[partition]=pod+'/'+incarnation
            ages.append(age)
        row=dict(timestamp=timestamp,valid=not reasons,invalid_reasons=reasons)
        if not reasons:
            row.update(lags=lags,owners=owners,positions=positions,highs=highs,
                       processing_backlog=sum(backlog.values()),maximum_observation_age_seconds=max(ages),
                       observation_time_spread_seconds=max(ages)-min(ages))
        snapshots.append(row)
    result=signals(snapshots,**options)
    result.update(run_id=manifest['run_id'],evaluation_seconds=end-start,
                  covered_fraction=result['covered_seconds']/(end-start) if end>start else None,
                  final_lag=result['snapshots'][-1].get('total_lag') if snapshots and snapshots[-1]['timestamp']==end else None,
                  parameters=options, freshness_clock=clock,
                  notes=['Offline diagnostic signals; this does not implement a controller.',
                         'V2 age is exporter monotonic age plus elapsed Prometheus sample age; a conservative scrape-resolution estimate, not exact cross-pod timing.',
                         'Legacy exports keep their original wall-clock validity rule; they are not retrospectively repaired.',
                         'Lag is high offset minus returned-record position. Processing backlog is reported separately.',
                         'Trapezoidal integration covers only adjacent valid, same-owner observations within max_gap.',
                         'Windows reset after invalid samples, owner changes, gaps or decreasing observed offsets.',
                         'Window growth uses m+1 snapshots; window means and persistence use m snapshots.',
                         'Hotspot thresholds are exploratory inputs and require pilot calibration.',
                         'Final lag requires a valid sample at the exact evaluation boundary; no extrapolation.',
                         'Unsampled ownership changes or resets cannot be recovered from these exported gauges.'])
    return result


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('run_directory',type=Path)
    parser.add_argument('--window-samples',type=int,default=15)
    parser.add_argument('--hot-k',type=float,default=1.0)
    parser.add_argument('--minimum-lag',type=float,default=10.0)
    parser.add_argument('--persistence',type=float,default=.8)
    parser.add_argument('--max-gap',type=float,default=3.0)
    args=parser.parse_args()
    result=analyze(args.run_directory,window_samples=args.window_samples,hot_k=args.hot_k,
                   minimum_lag=args.minimum_lag,persistence=args.persistence,max_gap=args.max_gap)
    output=args.run_directory/'lag-summary.json'
    output.write_text(json.dumps(result,indent=2,allow_nan=False)+'\n')
    print('Lag summary:',output)
