"""
Ingest new Richmond Lock tidal readings from the Environment Agency API
and upsert into PostgreSQL.

Designed for automation (cron / GitHub Actions / cloud runners).
"""

import os
import sys
import requests
import psycopg
from dotenv import load_dotenv


# --------------------------------------------------
# CONFIG
# --------------------------------------------------

MEASURE_ID = "0009-level-tidal_level-i-15_min-mAOD"
STATION_ID = "0009"
TABLE_NAME = "richmond_tidal_levels"
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

    print("Connecting to database...")

    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:

            # Create table if not exists
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
                    ts TIMESTAMPTZ PRIMARY KEY,
                    water_level DOUBLE PRECISION NOT NULL,
                    station_id TEXT NOT NULL,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                );
            """)

            # Get latest timestamp already stored
            cur.execute(f"SELECT MAX(ts) FROM {TABLE_NAME};")
            last_ts = cur.fetchone()[0]

    # --------------------------------------------------
    # Fetch data from API
    # --------------------------------------------------

    print("Fetching new data from Environment Agency...")

    params = {"_limit": 10000}

    if last_ts:
        params["since"] = last_ts.isoformat()
        print(f"Fetching readings since {last_ts}")

    response = requests.get(api_url, params=params, timeout=30)
    response.raise_for_status()

    data = response.json().get("items", [])

    if not data:
        print("No new readings found.")
        return

    print(f"Fetched {len(data)} readings")

    # --------------------------------------------------
    # Insert new readings
    # --------------------------------------------------

    inserted = 0

    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:

            for reading in data:
                cur.execute(f"""
                    INSERT INTO {TABLE_NAME} (ts, water_level, station_id)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (ts) DO NOTHING;
                """, (
                    reading["dateTime"],
                    reading["value"],
                    STATION_ID
                ))

                inserted += cur.rowcount  # counts successful inserts

        conn.commit()

    print(f"Inserted {inserted} new readings.")


# --------------------------------------------------

if __name__ == "__main__":
    main()