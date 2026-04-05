"""
Ingest Rainfall readings for Isfield Gauge (E8290) from the Environment Agency API
and upsert into PostgreSQL.
"""

import os
import sys
import requests
import psycopg
from dotenv import load_dotenv
from datetime import datetime, timedelta, timezone

# --------------------------------------------------
# CONFIG
# --------------------------------------------------

# For Isfield (E8290), the measure ID for 15-min rainfall is usually:
# {stationId}-rainfall-tipping_bucket_re-15_min-mm
STATION_ID = "E8290"
MEASURE_ID = f"{STATION_ID}-rainfall-tipping_bucket_raingauge-t-15_min-mm"
TABLE_NAME = "isfield_rainfall_readings"
API_BASE = "https://environment.data.gov.uk/flood-monitoring/id/measures"

# --------------------------------------------------
# MAIN LOGIC
# --------------------------------------------------

def main():
    load_dotenv()
    database_url = os.getenv("DATABASE_URL")

    if not database_url:
        print("ERROR: DATABASE_URL not set")
        sys.exit(1)

    api_url = f"{API_BASE}/{MEASURE_ID}/readings"

    print(f"Connecting to database to check {TABLE_NAME}...")

    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            # Create table if not exists
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
                    ts TIMESTAMPTZ PRIMARY KEY,
                    rainfall_mm DOUBLE PRECISION NOT NULL,
                    station_id TEXT NOT NULL,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                );
            """)

            # Get latest timestamp already stored to avoid fetching huge historical sets
            cur.execute(f"SELECT MAX(ts) FROM {TABLE_NAME};")
            last_ts = cur.fetchone()[0]
        conn.commit()

    # --------------------------------------------------
    # Fetch data from API
    # --------------------------------------------------

    print(f"Fetching new rainfall data for station {STATION_ID}...")

    # EA API defaults to the last 24 hours if no 'since' is provided
    params = {"_limit": 10000}
    if last_ts:
        # If we have data, fetch only what's new
        fetch_since = last_ts.isoformat()
        print(f"Incremental update: Fetching since {last_ts}")
    else:
        # If DB is empty, default to last 5 days
        five_days_ago = datetime.now(timezone.utc) - timedelta(days=5)
        fetch_since = five_days_ago.isoformat()
        print(f"First run: Backfilling last 5 days (since {fetch_since})")

    try:
        response = requests.get(api_url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json().get("items", [])
    except requests.exceptions.RequestException as e:
        print(f"ERROR: Failed to fetch from API: {e}")
        sys.exit(1)

    if not data:
        print("No new readings found.")
        return

    print(f"Fetched {len(data)} potential readings.")

    # --------------------------------------------------
    # Insert new readings
    # --------------------------------------------------

    inserted = 0

    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            for reading in data:
                # API returns dateTime and value
                cur.execute(f"""
                    INSERT INTO {TABLE_NAME} (ts, rainfall_mm, station_id)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (ts) DO NOTHING;
                """, (
                    reading["dateTime"],
                    reading["value"],
                    STATION_ID
                ))
                inserted += cur.rowcount
        conn.commit()

    print(f"Successfully inserted {inserted} new readings into {TABLE_NAME}.")

# --------------------------------------------------

if __name__ == "__main__":
    main()