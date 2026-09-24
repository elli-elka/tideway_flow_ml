"""
Backfill years of Richmond levels (and Kingston flow and the E8290 rain gauge) from
the Environment Agency's public daily archive: one CSV per day holding every reading
from every EA gauge. This is the same data FloodRadar shows, straight from the source.

Each archive file is tens of MB, so each run handles a limited number of missing
days (newest first, since recent winters matter most) and the next run carries on
where it left off. Days already stored are skipped, so it is safe to run repeatedly.
After backfilling, run build_ebb_flags.py --full to rebuild the flag history.

Usage: python backfill_ea_archive.py [start YYYY-MM-DD] [end YYYY-MM-DD] [max days]
       (defaults: 2017-01-01, yesterday, 45)
"""

import sys
from datetime import date, datetime, timedelta, timezone

import psycopg

from common import ensure_readings_table, fetch_ea_archive_day_multi, get_database_url, http_session


# --------------------------------------------------
# CONFIG
# --------------------------------------------------

# measure id -> (table, value column, station id). Kingston's flow measure has had
# two names over the years; both go into the same table.
SERIES = {
    "0009-level-tidal_level-i-15_min-mAOD": ("richmond_tidal_levels", "water_level", "0009"),
    "3400TH-flow-water-i-15_min-m3_s": ("kingston_flow_readings", "flow_m3s", "3400TH"),
    "3400TH-flow--i-15_min-m3_s": ("kingston_flow_readings", "flow_m3s", "3400TH"),
    "E8290-rainfall-tipping_bucket_raingauge-t-15_min-mm": ("isfield_rainfall_readings", "rainfall_mm", "E8290"),
}
# A day counts as done once Richmond has most of its 96 readings
DRIVER_TABLE = "richmond_tidal_levels"
# Days already tried (fetched, or confirmed absent), so none is downloaded twice
LOG_TABLE = "ea_archive_backfill_log"
MIN_READINGS_PER_DAY = 80
DEFAULT_START = date(2017, 1, 1)
DEFAULT_MAX_DAYS = 45


# --------------------------------------------------
# MAIN LOGIC
# --------------------------------------------------

def missing_days(cur, start, end):
    cur.execute(f"""
        SELECT (ts AT TIME ZONE 'UTC')::date AS day, COUNT(*)
        FROM {DRIVER_TABLE} WHERE ts >= %s AND ts < %s
        GROUP BY 1;""", (start, end + timedelta(days=1)))
    complete = {day for day, n in cur.fetchall() if n >= MIN_READINGS_PER_DAY}
    cur.execute(f"SELECT day FROM {LOG_TABLE} WHERE day BETWEEN %s AND %s;", (start, end))
    tried = {row[0] for row in cur.fetchall()}
    days = [end - timedelta(days=i) for i in range((end - start).days + 1)]

    # Once a previous run found the start of the archive (14+ missing files just
    # before the earliest day it could fetch), skip everything older
    cur.execute(f"SELECT MIN(day) FROM {LOG_TABLE} WHERE status = 'fetched';")
    earliest = cur.fetchone()[0]
    if earliest:
        cur.execute(f"SELECT COUNT(*) FROM {LOG_TABLE} WHERE status = 'no_file' AND day BETWEEN %s AND %s;",
                    (earliest - timedelta(days=14), earliest - timedelta(days=1)))
        if cur.fetchone()[0] >= 14:
            days = [d for d in days if d >= earliest]

    return [d for d in days if d not in complete and d not in tried]


def log_day(cur, day, status, readings=0):
    cur.execute(f"""
        INSERT INTO {LOG_TABLE} (day, status, readings) VALUES (%s, %s, %s)
        ON CONFLICT (day) DO UPDATE SET status = EXCLUDED.status, readings = EXCLUDED.readings,
            processed_at = NOW();""", (day, status, readings))


def main():
    args = sys.argv[1:] + [""] * 3
    start = date.fromisoformat(args[0]) if args[0] else DEFAULT_START
    end = date.fromisoformat(args[1]) if args[1] else datetime.now(timezone.utc).date() - timedelta(days=1)
    max_days = int(args[2]) if args[2] else DEFAULT_MAX_DAYS

    database_url = get_database_url()
    session = http_session()

    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            for table, column, _ in set(SERIES.values()):
                ensure_readings_table(cur, table, column)
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {LOG_TABLE} (
                    day DATE PRIMARY KEY,
                    status TEXT NOT NULL,
                    readings INTEGER NOT NULL DEFAULT 0,
                    processed_at TIMESTAMPTZ DEFAULT NOW()
                );""")
            conn.commit()

            todo = missing_days(cur, start, end)
            print(f"{len(todo)} day(s) missing Richmond data between {start} and {end}; "
                  f"doing up to {max_days} this run (newest first)")

            done, unavailable, inserted = 0, 0, 0
            consecutive_missing = 0
            for day in todo[:max_days]:
                readings = fetch_ea_archive_day_multi(session, day, list(SERIES))
                if readings is None:
                    unavailable += 1
                    consecutive_missing += 1
                    log_day(cur, day, "no_file")
                    conn.commit()
                    print(f"  {day}: no archive file")
                    # Working backwards, a long run of missing files means we've gone
                    # past the start of the archive
                    if consecutive_missing >= 14:
                        print("  14 days in a row with no archive file: reached the start of the archive.")
                        break
                    continue
                consecutive_missing = 0

                counts, day_total = [], 0
                for measure, rows in readings.items():
                    table, column, station = SERIES[measure]
                    values = [(ts, v, station) for ts, v in rows if v is not None]
                    if not values:
                        continue
                    cur.execute(f"SELECT COUNT(*) FROM {table};")
                    before = cur.fetchone()[0]
                    cur.executemany(f"""
                        INSERT INTO {table} (ts, {column}, station_id) VALUES (%s, %s, %s)
                        ON CONFLICT (ts) DO NOTHING;""", values)
                    cur.execute(f"SELECT COUNT(*) FROM {table};")
                    added = cur.fetchone()[0] - before
                    inserted += added
                    day_total += len(values)
                    counts.append(f"{table.split('_')[0]} +{added}")
                log_day(cur, day, "fetched", day_total)
                conn.commit()
                done += 1
                print(f"  {day}: {', '.join(counts) or 'none of our gauges in this file'}")

    remaining = max(len(todo) - done - unavailable, 0)
    print(f"Backfilled {done} day(s), {inserted} readings; {unavailable} day(s) not in the archive; "
          f"about {remaining} day(s) left to do.")


if __name__ == "__main__":
    main()
