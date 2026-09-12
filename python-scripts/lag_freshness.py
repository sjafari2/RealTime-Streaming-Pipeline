"""Shared freshness rule for saved analysis and the live experiment guard."""
import math


def observation_validity(metrics, query_time, freshness, clock='monotonic_scrape_v2'):
    if clock == 'monotonic_scrape_v2':
        local_age = metrics.get('consumer_lag_observation_age_seconds', math.nan)
        sample_age = query_time - metrics.get('consumer_lag_scrape_timestamp_seconds', math.nan)
        age = local_age + sample_age
        ages_valid = (math.isfinite(local_age) and math.isfinite(sample_age)
                      and min(local_age, sample_age) >= 0)
    elif clock == 'legacy_wall_clock_v1':
        age = query_time - metrics.get('consumer_lag_observed_timestamp_seconds', math.nan)
        ages_valid = math.isfinite(age) and age >= 0
    else:
        raise ValueError('Unknown lag freshness clock: ' + str(clock))
    lag, pos, high, back = (metrics.get(name, math.nan) for name in
                          ('consumer_lag', 'consumer_position_offset',
                           'consumer_high_offset', 'consumer_processing_backlog'))
    good = (metrics.get('consumer_lag_valid') == 1 and ages_valid and age <= freshness
            and all(math.isfinite(v) for v in (lag, pos, high, back))
            and min(lag, pos, high, back) >= 0 and high >= pos)
    return good, age
