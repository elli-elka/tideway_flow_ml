"""
One-off backfill of daily mean flow for the Thames at Kingston from the
National River Flow Archive (NRFA station 39001, record from 1883).

The NRFA series is quality-checked but lags real time by months, so use it for
model training history and kingston_flow_readings (EA, 15-min) for recent data.

Usage: python backfill_kingston_nrfa.py [start YYYY-MM-DD, default 2000-01-01]
"""

import sys
from datetime import date

import psycopg

from common import TIMEOUT, get_database_url, http_session


# --------------------------------------------------
# CONFIG
# --------------------------------------------------

NRFA_STATION = "39001"
API_URL = "https://nrfaapps.ceh.ac.uk/nrfa/ws/time-series"
TABLE_NAME = "kingston_daily_flow_nrfa"


# --------------------------------------------------
# UTILS
# --------------------------------------------------

def parse_data_stream(stream):
    """NRFA returns either a flat [date, value, date, value, ...] list or [[date, value], ...]."""
    if stream and isinstance(stream[0], list):
        pairs = stream
    else:
        pairs = zip(stream[0::2], stream[1::2])
    for day, value in pairs:
        if value is not None:
            yield date.fromisoformat(day[:10]), float(value)


# --------------------------------------------------
# MAIN LOGIC
# --------------------------------------------------

def main():
    database_url = get_database_url()
    start = date.fromisoformat(sys.argv[1]) if len(sys.argv) > 1 and sys.argv[1] else date(2000, 1, 1)

    print(f"Fetching NRFA gauged daily flow for station {NRFA_STATION}...")
    params = {"format": "json-object", "data-type": "gdf", "station": NRFA_STATION}
    response = http_session().get(API_URL, params=params, timeout=TIMEOUT)
    response.raise_for_status()
    stream = response.json().get("data-stream", [])

    rows = [(day, flow) for day, flow in parse_data_stream(stream) if day >= start]
    if not rows:
        print("ERROR: NRFA returned no data")
        sys.exit(1)
    print(f"Got {len(rows)} days ({rows[0][0]} -> {rows[-1][0]})")

    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
                    day DATE PRIMARY KEY,
                    flow_m3s DOUBLE PRECISION NOT NULL,
                    station_id TEXT NOT NULL DEFAULT '{NRFA_STATION}',
                    created_at TIMESTAMPTZ DEFAULT NOW()
                );
            """)
            cur.executemany(f"""
                INSERT INTO {TABLE_NAME} (day, flow_m3s)
                VALUES (%s, %s)
                ON CONFLICT (day) DO UPDATE SET flow_m3s = EXCLUDED.flow_m3s;
            """, rows)

    print(f"Upserted {len(rows)} days into {TABLE_NAME}.")


if __name__ == "__main__":
    main()
