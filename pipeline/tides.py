"""
Harmonic tide model for Richmond, fitted to the EA gauge's own readings.

Tide tables are made the same way: the tide is a sum of cosine waves at known
astronomical frequencies (the moon's and sun's pull, plus the shallow-water overtides
that distort the tide in a river like the Thames). Fitting their sizes and timings to
the last month of Richmond readings lets us predict the tide at any time:
high/low water times for the app, and the spring/neap changes in low water that the
flag predictions need.

The fit is redone on recent data each run, so its mean level tracks the current river
flow; what it can't know is how river flow will change, which is the model's job.
"""

from datetime import timedelta

import numpy as np

# Constituent speeds in degrees per hour (standard values)
CONSTITUENTS = {
    "M2": 28.9841042, "S2": 30.0, "N2": 28.4397295,          # semidiurnal
    "K1": 15.0410686, "O1": 13.9430356, "Q1": 13.3986609,     # diurnal
    "M4": 57.9682084, "MS4": 58.9841042, "MN4": 57.4238337,   # shallow-water overtides
    "M3": 43.4761563, "MK3": 44.0251729, "2MK3": 42.9271398,
    "M6": 86.9523127, "2MS6": 87.9682084, "2MN6": 86.4079380, "MSN6": 87.4238337,
    "M8": 115.9364166, "3MS8": 116.9523127, "M10": 144.9205210, "M12": 173.9046252,
    "MSf": 1.0158958, "Mm": 0.5443747,                         # fortnightly / monthly (spring-neap mean level)
}
FIT_DAYS = 30


def _design(hours):
    columns = [np.ones_like(hours)]
    for speed in CONSTITUENTS.values():
        w = np.deg2rad(speed) * hours
        columns += [np.cos(w), np.sin(w)]
    return np.column_stack(columns)


class TideModel:
    """Least-squares harmonic fit to (timestamp, level) readings."""

    def __init__(self, times, levels):
        self.epoch = times[0]
        hours = self._hours(times)
        self.coef, *_ = np.linalg.lstsq(_design(hours), np.asarray(levels, dtype=float), rcond=None)
        residual = np.asarray(levels) - _design(hours) @ self.coef
        self.rmse = float(np.sqrt(np.mean(residual ** 2)))

    @classmethod
    def from_recent(cls, readings, end=None, days=FIT_DAYS):
        """Fit to the `days` before `end` from a ts-sorted [(ts, level)] list."""
        end = end or readings[-1][0]
        start = end - timedelta(days=days)
        window = [(t, v) for t, v in readings if start <= t <= end]
        if len(window) < 96 * 20:   # need ~3 weeks to separate the main constituents
            return None
        times, levels = zip(*window)
        return cls(list(times), list(levels))

    def _hours(self, times):
        return np.array([(t - self.epoch).total_seconds() / 3600 for t in times])

    def predict(self, times):
        return _design(self._hours(times)) @ self.coef

    def series(self, start, end, step_minutes=5):
        times = []
        t = start
        while t <= end:
            times.append(t)
            t += timedelta(minutes=step_minutes)
        return times, self.predict(times)

    def window_low(self, end, hours=12):
        """Predicted lowest level in the `hours` before `end` (a flag window)."""
        times, levels = self.series(end - timedelta(hours=hours), end, step_minutes=10)
        return float(np.min(levels))

    def turning_points(self, start, end):
        """Predicted high and low waters between start and end -> [(ts, 'high'|'low', level)]."""
        times, levels = self.series(start - timedelta(hours=1), end + timedelta(hours=1), step_minutes=5)
        events = []
        for i in range(1, len(levels) - 1):
            if levels[i] > levels[i - 1] and levels[i] >= levels[i + 1]:
                kind = "high"
            elif levels[i] < levels[i - 1] and levels[i] <= levels[i + 1]:
                kind = "low"
            else:
                continue
            # Ignore wiggles: a real turn must differ from the last one by 0.5 m+
            if events and abs(levels[i] - events[-1][2]) < 0.5:
                if (kind == "high") == (levels[i] > events[-1][2]):
                    events[-1] = (times[i], kind, float(levels[i]))
                continue
            events.append((times[i], kind, float(levels[i])))
        return [e for e in events if start <= e[0] <= end]
