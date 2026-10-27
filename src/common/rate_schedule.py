"""Fixed producer-rate changes, measured from the shared warm-up start."""
import bisect
import json
import math


class RateSchedule:
    def __init__(self, specification, duration):
        if not math.isfinite(duration) or duration <= 0:
            raise ValueError('The production duration must be finite and positive')
        rows = json.loads(specification)
        if not isinstance(rows, list) or not rows:
            raise ValueError('PRODUCER_RATE_SCHEDULE_JSON must be a nonempty list')
        self.points = []
        for row in rows:
            if not isinstance(row, dict) or set(row) != {'at_seconds', 'rate'}:
                raise ValueError('Each rate step needs at_seconds and rate')
            at, rate = row['at_seconds'], row['rate']
            if (type(at) not in (int, float) or type(rate) not in (int, float)
                    or not math.isfinite(at) or not math.isfinite(rate)
                    or at < 0 or at >= duration or rate <= 0):
                raise ValueError('Rate steps need finite positive rates and times inside production')
            if self.points and at <= self.points[-1][0]:
                raise ValueError('Rate-step times must increase strictly')
            self.points.append((float(at), float(rate)))
        if self.points[0][0] != 0:
            raise ValueError('The first rate step must start at zero')
        self.times = [point[0] for point in self.points]

    def at(self, elapsed):
        index = max(0, bisect.bisect_right(self.times, elapsed) - 1)
        return index, self.points[index][1]
