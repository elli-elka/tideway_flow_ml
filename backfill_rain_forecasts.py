"""
Backfill catchment_rain_forecast with what past forecasts said, from Open-Meteo's
Previous Runs API (free, no key). Coverage starts around 2024.

For each past day it returns the rain forecast made 1..7 days beforehand, so the model
can be trained and tested on forecasts as they were at the time, instead of waiting a
year for ingest_catchment_rain.py's daily snapshots to build up. Rows from the live
snapshots are never overwritten.

Usage: python backfill_rain_forecasts.py [start YYYY-MM-DD, default 2024-01-01] [end]
"""

import sys
import time
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone

import psycopg

from common import get_database_url, http_session
from ingest_catchment_rain import (FORECAST_MODEL, FORECAST_TABLE, create_tables, get_json_list,
                                   location_params)


# --------------------------------------------------
# CONFIG
# --------------------------------------------------

PREVIOUS_RUNS_URL = "https://previous-runs-api.open-meteo.com/v1/forecast"
MAX_LEAD_DAYS = 7
CHUNK_DAYS = 31


# --------------------------------------------------
# MAIN LOGIC
# --------------------------------------------------

def daily_by_lead(hourly):
    """Hourly precipitation per lead -> {(day, lead): mm}. `precipitation` is the most
    recent run (lead 0); `precipitation_previous_dayN` is the run from N days earlier."""
    totals = defaultdict(float)
    seen = set()
    for lead in range(MAX_LEAD_DAYS + 1):
        key = "precipitation" if lead == 0 else f"precipitation_previous_day{lead}"
        for ts, value in zip(hourly.get("time", []), hourly.get(key) or []):
            if value is not None:
                totals[(ts[:10], lead)] += value
                seen.add((ts[:10], lead))
    return {k: totals[k] for k in seen}


def main():
    database_url = get_database_url()
    session = http_session()
    start = date.fromisoformat(sys.argv[1]) if len(sys.argv) > 1 and sys.argv[1] else date(2024, 1, 1)
    end = (date.fromisoformat(sys.argv[2]) if len(sys.argv) > 2 and sys.argv[2]
           else datetime.now(timezone.utc).date() - timedelta(days=1))

    names, params = location_params()
    variables = ["precipitation"] + [f"precipitation_previous_day{n}" for n in range(1, MAX_LEAD_DAYS + 1)]

    total = 0
    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            create_tables(cur)
            conn.commit()
            chunk_start = start
            while chunk_start <= end:
                chunk_end = min(chunk_start + timedelta(days=CHUNK_DAYS - 1), end)
                results = get_json_list(session, PREVIOUS_RUNS_URL, {
                    **params,
                    "hourly": ",".join(variables),
                    "start_date": chunk_start.isoformat(),
                    "end_date": chunk_end.isoformat(),
                    "models": FORECAST_MODEL,
                })
                rows = []
                for name, result in zip(names, results):
                    for (day, lead), precip in daily_by_lead(result.get("hourly") or {}).items():
                        target = date.fromisoformat(day)
                        rows.append((target - timedelta(days=lead), target, name,
                                     FORECAST_MODEL, lead, precip))
                cur.executemany(f"""
                    INSERT INTO {FORECAST_TABLE} (issued_date, target_day, point, model, lead_days, precip_mm)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (issued_date, target_day, point, model) DO NOTHING;
                """, rows)
                conn.commit()
                total += len(rows)
                print(f"{chunk_start} -> {chunk_end}: {len(rows)} rows")
                chunk_start = chunk_end + timedelta(days=1)
                time.sleep(1)

    print(f"Backfilled {total} forecast rows into {FORECAST_TABLE}.")


if __name__ == "__main__":
    main()
