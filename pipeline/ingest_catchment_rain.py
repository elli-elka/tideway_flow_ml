"""
Daily rainfall across the Thames catchment upstream of Teddington, from Open-Meteo
(free, no API key, non-commercial use).

Two tables:
  catchment_rain_daily     observed/reanalysis rainfall, reference evapotranspiration
                           (et0, how much water evaporates) and soil moisture (7-28cm
                           and 28-100cm) per point per day (ERA5-based,
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
ARCHIVE_LAG_DAYS = 6
# ERA5 soil layers (volumetric m3/m3): how wet the ground is decides whether rain runs off
SOIL_LAYERS = ("soil_moisture_7_to_28cm", "soil_moisture_28_to_100cm")  # reanalysis is published ~5 days behind


# --------------------------------------------------
# UTILS
# --------------------------------------------------

def daily_soil_moisture(hourly):
    """Daily mean of the hourly soil moisture layers -> {day: (shallow, deep)} in m3/m3."""
    sums = {}
    for i, ts in enumerate(hourly.get("time", [])):
        day = ts[:10]
        for j, layer in enumerate(SOIL_LAYERS):
            value = (hourly.get(layer) or [None] * (i + 1))[i]
            if value is not None:
                total, count = sums.setdefault((day, j), [0.0, 0])
                sums[(day, j)] = [total + value, count + 1]
    days = {d for d, _ in sums}
    return {d: tuple((sums[(d, j)][0] / sums[(d, j)][1]) if (d, j) in sums else None
                     for j in range(len(SOIL_LAYERS))) for d in days}


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
            "daily": "precipitation_sum,et0_fao_evapotranspiration",
            "hourly": ",".join(SOIL_LAYERS),
        })
        rows = []
        for name, result in zip(names, results):
            daily = result["daily"]
            et0s = daily.get("et0_fao_evapotranspiration") or [None] * len(daily["time"])
            soil = daily_soil_moisture(result.get("hourly") or {})
            for day, precip, et0 in zip(daily["time"], daily["precipitation_sum"], et0s):
                if precip is not None:
                    shallow, deep = soil.get(day, (None, None))
                    rows.append((day, name, precip, et0, shallow, deep))
        cur.executemany(f"""
            INSERT INTO {HISTORY_TABLE} (day, point, precip_mm, et0_mm, soil_moisture_shallow, soil_moisture_deep)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (day, point) DO UPDATE
            SET precip_mm = EXCLUDED.precip_mm, et0_mm = EXCLUDED.et0_mm,
                soil_moisture_shallow = EXCLUDED.soil_moisture_shallow,
                soil_moisture_deep = EXCLUDED.soil_moisture_deep;
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
        "daily": "precipitation_sum,precipitation_probability_max,et0_fao_evapotranspiration",
        "forecast_days": 16,
        "models": FORECAST_MODEL,
    })
    rows = []
    for name, result in zip(names, results):
        daily = result["daily"]
        probs = daily.get("precipitation_probability_max") or [None] * len(daily["time"])
        et0s = daily.get("et0_fao_evapotranspiration") or [None] * len(daily["time"])
        for day, precip, prob, et0 in zip(daily["time"], daily["precipitation_sum"], probs, et0s):
            if precip is None:
                continue
            lead = (date.fromisoformat(day) - issued).days
            rows.append((issued, day, name, FORECAST_MODEL, lead, precip, prob, et0))
    if not rows:
        print("ERROR: forecast returned no data")
        sys.exit(1)
    cur.executemany(f"""
        INSERT INTO {FORECAST_TABLE} (issued_date, target_day, point, model, lead_days, precip_mm, precip_prob, et0_mm)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (issued_date, target_day, point, model) DO UPDATE
        SET precip_mm = EXCLUDED.precip_mm, precip_prob = EXCLUDED.precip_prob, et0_mm = EXCLUDED.et0_mm;
    """, rows)
    print(f"Saved {len(rows)} forecast rows (issued {issued}, model {FORECAST_MODEL}).")


def create_tables(cur):
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {HISTORY_TABLE} (
            day DATE NOT NULL,
            point TEXT NOT NULL,
            precip_mm DOUBLE PRECISION NOT NULL,
            et0_mm DOUBLE PRECISION,
            soil_moisture_shallow DOUBLE PRECISION,
            soil_moisture_deep DOUBLE PRECISION,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            PRIMARY KEY (day, point)
        );
        ALTER TABLE {HISTORY_TABLE} ADD COLUMN IF NOT EXISTS soil_moisture_shallow DOUBLE PRECISION;
        ALTER TABLE {HISTORY_TABLE} ADD COLUMN IF NOT EXISTS soil_moisture_deep DOUBLE PRECISION;
        CREATE TABLE IF NOT EXISTS {FORECAST_TABLE} (
            issued_date DATE NOT NULL,
            target_day DATE NOT NULL,
            point TEXT NOT NULL,
            model TEXT NOT NULL,
            lead_days INTEGER NOT NULL,
            precip_mm DOUBLE PRECISION NOT NULL,
            precip_prob DOUBLE PRECISION,
            et0_mm DOUBLE PRECISION,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            PRIMARY KEY (issued_date, target_day, point, model)
        );
    """)


def main():
    database_url = get_database_url()
    session = http_session()

    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            create_tables(cur)
            conn.commit()
            ingest_forecast(cur, session)
            conn.commit()
            ingest_history(cur, session)


if __name__ == "__main__":
    main()
