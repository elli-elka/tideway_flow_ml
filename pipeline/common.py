"""
Shared helpers for the ingestion scripts: database URL, a retrying HTTP session,
and Environment Agency (EA) flood-monitoring fetching with archive gap-filling.
"""

import csv
import os
import sys
from datetime import datetime, timedelta, timezone

import psycopg
import requests
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# --------------------------------------------------
# CONFIG
# --------------------------------------------------

EA_API_BASE = "https://environment.data.gov.uk/flood-monitoring"
USER_AGENT = "tideway-flow-ml/1.0 (+https://github.com/elli-elka/tideway_flow_ml)"

# (connect, read) seconds. The EA API regularly takes >30s to respond.
TIMEOUT = (15, 120)

# EA API page size (hard maximum is 10000)
EA_PAGE_LIMIT = 10000

# Cap on how many daily archive files one run will download (each is tens of MB)
MAX_ARCHIVE_DAYS = int(os.getenv("MAX_ARCHIVE_DAYS", "90"))


# --------------------------------------------------
# UTILS
# --------------------------------------------------

def get_database_url():
    load_dotenv(override=True)
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        print("ERROR: DATABASE_URL not set")
        sys.exit(1)
    return database_url


def http_session(headers=None):
    """requests.Session that retries timeouts, connection errors and 429/5xx with backoff."""
    retry = Retry(
        total=5,
        connect=5,
        read=5,
        status=5,
        backoff_factor=3,  # 0, 6, 12, 24, 48s
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
        respect_retry_after_header=True,
    )
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=retry))
    session.headers["User-Agent"] = USER_AGENT
    if headers:
        session.headers.update(headers)
    return session


def parse_value(value):
    """EA values are usually floats, but can be null, a list (duplicate readings)
    or a '|' separated string in the archive CSVs. Return a float or None."""
    if value is None or value == "":
        return None
    if isinstance(value, list):
        value = next((v for v in value if v is not None), None)
        return parse_value(value)
    if isinstance(value, str) and "|" in value:
        return parse_value(value.split("|")[0])
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def parse_ts(ts_str):
    return datetime.fromisoformat(ts_str.replace("Z", "+00:00"))


# --------------------------------------------------
# EA FETCHING
# --------------------------------------------------

def fetch_ea_api(session, measure_id, since):
    """Fetch readings for a measure from the live API since a timestamp.
    The live API only holds recent readings (a few weeks)."""
    url = f"{EA_API_BASE}/id/measures/{measure_id}/readings"
    readings = []
    offset = 0
    while True:
        params = {
            "since": since.isoformat(),
            "_sorted": "",
            "_limit": EA_PAGE_LIMIT,
            "_offset": offset,
        }
        response = session.get(url, params=params, timeout=TIMEOUT)
        response.raise_for_status()
        items = response.json().get("items", [])
        for item in items:
            readings.append((parse_ts(item["dateTime"]), parse_value(item.get("value"))))
        if len(items) < EA_PAGE_LIMIT:
            return readings
        offset += EA_PAGE_LIMIT


def fetch_ea_archive_day_multi(session, day, measure_ids):
    """One day of the EA daily archive CSV (every reading from every gauge), keeping
    only the given measures -> {measure_id: [(ts, value)]}. Returns None if the
    archive has no file for that day."""
    url = f"{EA_API_BASE}/archive/readings-{day.isoformat()}.csv"
    wanted = {f"/{m}": m for m in measure_ids}
    readings = {m: [] for m in measure_ids}
    with session.get(url, stream=True, timeout=TIMEOUT) as response:
        if response.status_code == 404:
            return None
        response.raise_for_status()
        response.encoding = "utf-8"
        for row in csv.DictReader(response.iter_lines(decode_unicode=True)):
            measure = row.get("measure", "")
            key = measure[measure.rfind("/"):]
            if key in wanted:
                readings[wanted[key]].append((parse_ts(row["dateTime"]), parse_value(row.get("value"))))
    return readings


def fetch_ea_archive_day(session, day, measure_id):
    """Fetch one day of readings for a measure from the EA daily archive CSV."""
    readings = fetch_ea_archive_day_multi(session, day, [measure_id])
    if readings is None:
        print(f"  WARNING: no archive file for {day}")
        return []
    return readings[measure_id]


def fetch_ea_readings(session, measure_id, since):
    """Fetch readings since `since`. If the live API no longer covers the start of
    that window (e.g. the pipeline was down for weeks), fill the gap from the daily
    archive so the database does not end up with holes."""
    readings = fetch_ea_api(session, measure_id, since)
    print(f"Live API returned {len(readings)} readings")

    earliest = min((ts for ts, _ in readings), default=datetime.now(timezone.utc))
    if earliest - since <= timedelta(days=1):
        return readings

    # Archive files are published the day after, so stop at yesterday
    first_day = since.date()
    last_day = min(earliest.date(), datetime.now(timezone.utc).date() - timedelta(days=1))
    days = [first_day + timedelta(days=i) for i in range((last_day - first_day).days + 1)]
    if len(days) > MAX_ARCHIVE_DAYS:
        print(f"Gap of {len(days)} days is larger than MAX_ARCHIVE_DAYS={MAX_ARCHIVE_DAYS}; "
              f"filling the first {MAX_ARCHIVE_DAYS}. Re-run to continue.")
        days = days[:MAX_ARCHIVE_DAYS]

    print(f"Gap detected ({since} -> {earliest}); filling {len(days)} day(s) from the EA archive")
    for day in days:
        day_readings = fetch_ea_archive_day(session, day, measure_id)
        print(f"  {day}: {len(day_readings)} readings")
        readings.extend(day_readings)
    return readings


def ensure_readings_table(cur, table_name, value_column):
    """The standard layout for a 15-minute EA readings table."""
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            ts TIMESTAMPTZ PRIMARY KEY,
            {value_column} DOUBLE PRECISION NOT NULL,
            station_id TEXT NOT NULL,
            created_at TIMESTAMPTZ DEFAULT NOW()
        );
    """)


def run_ea_ingest(table_name, value_column, measure_id, station_id, default_days=5):
    """Create the table if needed, fetch everything newer than the latest stored
    reading (gap-filling from the archive), and insert it."""
    database_url = get_database_url()

    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            ensure_readings_table(cur, table_name, value_column)
            cur.execute(f"SELECT MAX(ts) FROM {table_name};")
            last_ts = cur.fetchone()[0]

    # A date passed on the command line (YYYY-MM-DD) forces a backfill from that date
    if len(sys.argv) > 1 and sys.argv[1]:
        since = datetime.fromisoformat(sys.argv[1]).replace(tzinfo=timezone.utc)
        print(f"Manual backfill since {since}")
    elif last_ts:
        since = last_ts
        print(f"Incremental update since {since}")
    else:
        since = datetime.now(timezone.utc) - timedelta(days=default_days)
        print(f"Empty table: fetching last {default_days} days (since {since})")

    readings = fetch_ea_readings(http_session(), measure_id, since)
    rows = [(ts, value, station_id) for ts, value in readings if value is not None]
    skipped = len(readings) - len(rows)
    if skipped:
        print(f"Skipped {skipped} readings with no value")

    if not rows:
        print("No new readings found.")
        return

    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {table_name};")
            before = cur.fetchone()[0]
            cur.executemany(f"""
                INSERT INTO {table_name} (ts, {value_column}, station_id)
                VALUES (%s, %s, %s)
                ON CONFLICT (ts) DO NOTHING;
            """, rows)
            cur.execute(f"SELECT COUNT(*) FROM {table_name};")
            after = cur.fetchone()[0]

    print(f"Inserted {after - before} new readings into {table_name}.")
