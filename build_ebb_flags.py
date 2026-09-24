"""
Rebuild the PLA Ebb Tide Flag history from stored Richmond levels.

PLA rule: at 06:00 and 18:00 (UK time) take the lowest Richmond tide reading of the
preceding 12 hours (metres above chart datum):
    >= 2.6 RED | >= 1.7 YELLOW | >= 0 GREEN | < 0 BLACK

Levels come from richmond_pla_levels (PLA, chart datum) where available, otherwise
richmond_tidal_levels (EA, mAOD) converted to chart datum with an offset:
    level_cd = level_maod + offset
The offset is taken from RICHMOND_CD_OFFSET if set, otherwise calibrated from
timestamps where both tables have a reading.

The result (richmond_ebb_flags) is the target the model learns to predict, and what
predictions get compared against.

Usage: python build_ebb_flags.py [--full]
"""

import bisect
import os
import statistics
import sys
from datetime import datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo

import psycopg

from common import get_database_url


# --------------------------------------------------
# CONFIG
# --------------------------------------------------

PLA_TABLE = "richmond_pla_levels"
EA_TABLE = "richmond_tidal_levels"
FLAGS_TABLE = "richmond_ebb_flags"
UK = ZoneInfo("Europe/London")
ISSUE_HOURS = (6, 18)
WINDOW = timedelta(hours=12)
MIN_READINGS = 36          # of 48 possible 15-min readings in the 12h window
MIN_CALIBRATION_LOWS = 4      # low-water pairs needed to calibrate the offset


# --------------------------------------------------
# UTILS
# --------------------------------------------------

def flag_for(level_cd):
    if level_cd >= 2.6:
        return "RED"
    if level_cd >= 1.7:
        return "YELLOW"
    if level_cd >= 0:
        return "GREEN"
    return "BLACK"


def table_exists(cur, name):
    cur.execute("SELECT to_regclass(%s) IS NOT NULL;", (name,))
    return cur.fetchone()[0]


def window_min(series, start, end):
    """(ts, level, n) of the lowest reading with start < ts <= end in a ts-sorted
    [(ts, level), ...] list, or None if the window is empty."""
    lo = bisect.bisect_right(series, start, key=lambda r: r[0])
    hi = bisect.bisect_right(series, end, key=lambda r: r[0])
    if lo >= hi:
        return None
    ts, level = min(series[lo:hi], key=lambda r: r[1])
    return ts, level, hi - lo


def get_offset(cur, have_pla, have_ea):
    """Chart datum offset (level_cd = level_maod + offset).

    The PLA and EA gauges don't differ by a perfectly constant amount through the
    tide (e.g. +0.71 m at low water but +0.60 m at high water on 24 Sep 2026), so
    calibrate on what the flag uses: the lowest reading in each 12h flag window."""
    if os.getenv("RICHMOND_CD_OFFSET"):
        return float(os.getenv("RICHMOND_CD_OFFSET")), "env"
    if not (have_pla and have_ea):
        return None, None
    cur.execute(f"SELECT ts, observed FROM {PLA_TABLE} WHERE observed IS NOT NULL ORDER BY ts;")
    pla = cur.fetchall()
    if not pla:
        return None, None
    cur.execute(f"SELECT ts, water_level FROM {EA_TABLE} WHERE ts BETWEEN %s AND %s ORDER BY ts;",
                (pla[0][0], pla[-1][0]))
    ea = cur.fetchall()

    diffs = []
    for issued in issue_times(pla[0][0], pla[-1][0]):
        pla_low = window_min(pla, issued - WINDOW, issued)
        ea_low = window_min(ea, issued - WINDOW, issued)
        if pla_low and ea_low and pla_low[2] >= MIN_READINGS and ea_low[2] >= MIN_READINGS:
            diffs.append(pla_low[1] - ea_low[1])
    if len(diffs) < MIN_CALIBRATION_LOWS:
        return None, None
    return statistics.median(diffs), (f"calibrated from {len(diffs)} low waters, "
                                      f"range {min(diffs):+.3f} to {max(diffs):+.3f}")


def issue_times(start, end):
    """All 06:00/18:00 UK-local issue times in (start, end], as UTC datetimes."""
    day = start.astimezone(UK).date()
    while True:
        for hour in ISSUE_HOURS:
            t = datetime.combine(day, time(hour), tzinfo=UK).astimezone(timezone.utc)
            if t > end:
                return
            if t > start:
                yield t
        day += timedelta(days=1)


# --------------------------------------------------
# MAIN LOGIC
# --------------------------------------------------

def main():
    database_url = get_database_url()
    full = "--full" in sys.argv

    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {FLAGS_TABLE} (
                    issued_at TIMESTAMPTZ PRIMARY KEY,
                    min_level_cd DOUBLE PRECISION NOT NULL,
                    min_level_ts TIMESTAMPTZ NOT NULL,
                    flag TEXT NOT NULL,
                    n_readings INTEGER NOT NULL,
                    source TEXT NOT NULL,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                );
            """)
            conn.commit()

            have_pla = table_exists(cur, PLA_TABLE)
            have_ea = table_exists(cur, EA_TABLE)
            offset, offset_note = get_offset(cur, have_pla, have_ea)
            if offset is not None:
                print(f"Chart datum offset: {offset:+.3f} m ({offset_note})")
            elif have_ea:
                print("NOTE: no chart datum offset yet, so EA readings are not used. Set "
                      "RICHMOND_CD_OFFSET or ingest some PLA data alongside EA data to calibrate.")

            # Where to (re)start from
            cur.execute(f"SELECT MAX(issued_at) FROM {FLAGS_TABLE};")
            last_issued = cur.fetchone()[0]
            since = None if full or last_issued is None else last_issued - timedelta(days=2)

            # 15-min readings in chart datum; PLA preferred where both exist
            levels = {}
            if have_ea and offset is not None:
                cur.execute(f"""
                    SELECT ts, water_level + %s FROM {EA_TABLE}
                    WHERE %s::timestamptz IS NULL OR ts > %s::timestamptz - INTERVAL '12 hours';
                """, (offset, since, since))
                levels.update({ts: (lvl, "ea") for ts, lvl in cur.fetchall()})
            if have_pla:
                cur.execute(f"""
                    SELECT ts, observed FROM {PLA_TABLE}
                    WHERE observed IS NOT NULL
                      AND EXTRACT(MINUTE FROM ts)::int %% 15 = 0
                      AND (%s::timestamptz IS NULL OR ts > %s::timestamptz - INTERVAL '12 hours');
                """, (since, since))
                levels.update({ts: (lvl, "pla") for ts, lvl in cur.fetchall()})

            if not levels:
                print("No calibrated Richmond levels available yet; nothing to build.")
                return

            series = sorted(levels.items())
            start = series[0][0] if since is None else since
            end = datetime.now(timezone.utc)

            rows = []
            skipped = 0
            for issued in issue_times(start, end):
                low = window_min(series, issued - WINDOW, issued)
                if not low or low[2] < MIN_READINGS:
                    skipped += 1
                    continue
                min_ts, (min_level, _), n = low
                window = series[bisect.bisect_right(series, issued - WINDOW, key=lambda r: r[0]):
                                bisect.bisect_right(series, issued, key=lambda r: r[0])]
                sources = sorted({src for _, (_, src) in window})
                rows.append((issued, min_level, min_ts, flag_for(min_level), n, "+".join(sources)))

            cur.executemany(f"""
                INSERT INTO {FLAGS_TABLE} (issued_at, min_level_cd, min_level_ts, flag, n_readings, source)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (issued_at) DO UPDATE
                SET min_level_cd = EXCLUDED.min_level_cd, min_level_ts = EXCLUDED.min_level_ts,
                    flag = EXCLUDED.flag, n_readings = EXCLUDED.n_readings, source = EXCLUDED.source;
            """, rows)

    print(f"Upserted {len(rows)} flag issues into {FLAGS_TABLE} "
          f"({skipped} skipped for missing data).")


if __name__ == "__main__":
    main()
