"""
Daily rainfall across the Thames catchment upstream of Teddington, from Open-Meteo
(free, no API key, non-commercial use).

Two tables:
  catchment_rain_daily     observed/reanalysis rainfall per point per day (ERA5-based,
                           back to 1940; lags real time by ~5 days)
  catchment_rain_forecast  a snapshot of the 16-day forecast, saved every day it runs.
                           Keeping every snapshot lets you train/evaluate the model on
                           what the forecast said at the time, not what actually fell.

The same points are used for history and forecast, so the model sees consistent inputs.
"""

import os
import sys
import time
from datetime import date, datetime, timedelta, timezone

import psycopg

from common import TIMEOUT, get_database_url, http_session


# --------------------------------------------------
# CONFIG
# --------------------------------------------------

# One point per main sub-catchment feeding the Thames above Teddington weir
POINTS = {
    "cirencester_upper_thames": (51.72, -1.97),
    "banbury_cherwell":         (52.06, -1.34),
    "oxford":                   (51.75, -1.26),
    "aylesbury_thame":          (51.82, -0.81),
    "newbury_kennet":           (51.40, -1.32),
    "reading":                  (51.45, -0.97),
    "basingstoke_loddon":       (51.27, -1.09),
    "windsor":                  (51.48, -0.61),
    "guildford_wey":            (51.24, -0.57),
}

ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
FORECAST_URL = "https://api.open-meteo.com/v1/forecast"
HISTORY_TABLE = "catchment_rain_daily"
FORECAST_TABLE = "catchment_rain_forecast"
HISTORY_START = date.fromisoformat(os.getenv("RAIN_HISTORY_START", "2005-01-01"))
FORECAST_MODEL = os.getenv("RAIN_FORECAST_MODEL", "best_match")  # e.g. ukmo_seamless, ecmwf_ifs025
ARCHIVE_LAG_DAYS = 6  # reanalysis is published ~5 days behind


# --------------------------------------------------
# UTILS
# --------------------------------------------------

def location_params():
    names = list(POINTS)
    return names, {
        "latitude": ",".join(str(POINTS[n][0]) for n in names),
        "longitude": ",".join(str(POINTS[n][1]) for n in names),
        "timezone": "Europe/London",
    }


def get_json_list(session, url, params):
    response = session.get(url, params=params, timeout=TIMEOUT)
    response.raise_for_status()
    data = response.json()
    return data if isinstance(data, list) else [data]  # a single location returns an object


# --------------------------------------------------
# MAIN LOGIC
# --------------------------------------------------

def ingest_history(cur, session):
    cur.execute(f"SELECT MAX(day) FROM {HISTORY_TABLE};")
    last_day = cur.fetchone()[0]
    start = last_day + timedelta(days=1) if last_day else HISTORY_START
    end = datetime.now(timezone.utc).date() - timedelta(days=ARCHIVE_LAG_DAYS)
    if start > end:
        print("History already up to date.")
        return

    names, params = location_params()
    total = 0
    # One year per request keeps responses small and within the free-tier call weighting
    chunk_start = start
    while chunk_start <= end:
        chunk_end = min(date(chunk_start.year, 12, 31), end)
        print(f"History {chunk_start} -> {chunk_end}")
        results = get_json_list(session, ARCHIVE_URL, {
            **params,
            "start_date": chunk_start.isoformat(),
            "end_date": chunk_end.isoformat(),
            "daily": "precipitation_sum",
        })
        rows = []
        for name, result in zip(names, results):
            daily = result["daily"]
            for day, precip in zip(daily["time"], daily["precipitation_sum"]):
                if precip is not None:
                    rows.append((day, name, precip))
        cur.executemany(f"""
            INSERT INTO {HISTORY_TABLE} (day, point, precip_mm)
            VALUES (%s, %s, %s)
            ON CONFLICT (day, point) DO UPDATE SET precip_mm = EXCLUDED.precip_mm;
        """, rows)
        cur.connection.commit()  # keep progress if a later chunk fails
        total += len(rows)
        chunk_start = chunk_end + timedelta(days=1)
        time.sleep(1)
    print(f"Upserted {total} history rows into {HISTORY_TABLE}.")


def ingest_forecast(cur, session):
    names, params = location_params()
    issued = datetime.now(timezone.utc).date()
    results = get_json_list(session, FORECAST_URL, {
        **params,
        "daily": "precipitation_sum,precipitation_probability_max",
        "forecast_days": 16,
        "models": FORECAST_MODEL,
    })
    rows = []
    for name, result in zip(names, results):
        daily = result["daily"]
        probs = daily.get("precipitation_probability_max") or [None] * len(daily["time"])
        for day, precip, prob in zip(daily["time"], daily["precipitation_sum"], probs):
            if precip is None:
                continue
            lead = (date.fromisoformat(day) - issued).days
            rows.append((issued, day, name, FORECAST_MODEL, lead, precip, prob))
    if not rows:
        print("ERROR: forecast returned no data")
        sys.exit(1)
    cur.executemany(f"""
        INSERT INTO {FORECAST_TABLE} (issued_date, target_day, point, model, lead_days, precip_mm, precip_prob)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (issued_date, target_day, point, model) DO UPDATE
        SET precip_mm = EXCLUDED.precip_mm, precip_prob = EXCLUDED.precip_prob;
    """, rows)
    print(f"Saved {len(rows)} forecast rows (issued {issued}, model {FORECAST_MODEL}).")


def main():
    database_url = get_database_url()
    session = http_session()

    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {HISTORY_TABLE} (
                    day DATE NOT NULL,
                    point TEXT NOT NULL,
                    precip_mm DOUBLE PRECISION NOT NULL,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    PRIMARY KEY (day, point)
                );
                CREATE TABLE IF NOT EXISTS {FORECAST_TABLE} (
                    issued_date DATE NOT NULL,
                    target_day DATE NOT NULL,
                    point TEXT NOT NULL,
                    model TEXT NOT NULL,
                    lead_days INTEGER NOT NULL,
                    precip_mm DOUBLE PRECISION NOT NULL,
                    precip_prob DOUBLE PRECISION,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    PRIMARY KEY (issued_date, target_day, point, model)
                );
            """)
            conn.commit()
            ingest_forecast(cur, session)
            conn.commit()
            ingest_history(cur, session)


if __name__ == "__main__":
    main()
