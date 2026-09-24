"""
Groundwater levels in the chalk aquifers feeding the Thames above Teddington, from
the EA Hydrology API.

Much of the catchment is chalk, where winter river flow is fed by groundwater for
months. High groundwater means rain goes straight through to the river, so this is
the slow-moving signal behind a wet (or dry) season on the Tideway.

Usage: python ingest_groundwater.py [start YYYY-MM-DD for a full backfill]
"""

import os
import sys
from datetime import date, timedelta

import psycopg

from common import get_database_url, http_session
from hydrology import daily_mean, fetch_readings, find_stations, measure_ids, text


# --------------------------------------------------
# CONFIG
# --------------------------------------------------

# area key -> (lat, long): the nearest borehole with usable data is picked for each
AREAS = {
    "marlborough_downs": (51.45, -1.70),   # Kennet
    "berkshire_downs":   (51.55, -1.35),   # Lambourn / Pang
    "chilterns":         (51.65, -0.90),   # Thame / Wye / Colne
    "cotswolds":         (51.80, -1.90),   # Upper Thames (limestone)
}
SEARCH_RADIUS_KM = 20
TABLE_NAME = "groundwater_daily"
STATIONS_TABLE = "hydrology_stations"
HISTORY_START = date.fromisoformat(os.getenv("GROUNDWATER_HISTORY_START", "2005-01-01"))
# 15-minute logger records are large, so fetch less history for those
LOGGER_HISTORY_DAYS = 3 * 365


# --------------------------------------------------
# MAIN LOGIC
# --------------------------------------------------

def pick_groundwater_measure(station):
    """Prefer daily logged levels, then manual dips, then sub-daily logger data."""
    ids = [i for i in measure_ids(station) if "-gw-" in i or "-level-" in i]
    for pattern in ("-86400-", "dipped", "logged", ""):
        for i in ids:
            if pattern in i:
                return i
    return None


def main():
    database_url = get_database_url()
    session = http_session()
    forced_start = date.fromisoformat(sys.argv[1]) if len(sys.argv) > 1 and sys.argv[1] else None

    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
                    day DATE NOT NULL,
                    area TEXT NOT NULL,
                    level_maod DOUBLE PRECISION NOT NULL,
                    quality TEXT,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    PRIMARY KEY (day, area)
                );
                CREATE TABLE IF NOT EXISTS {STATIONS_TABLE} (
                    key TEXT PRIMARY KEY,
                    kind TEXT NOT NULL,
                    label TEXT,
                    river TEXT,
                    lat DOUBLE PRECISION,
                    long DOUBLE PRECISION,
                    measure TEXT NOT NULL,
                    updated_at TIMESTAMPTZ DEFAULT NOW()
                );
            """)
            conn.commit()

            resolved = 0
            for area, (lat, lon) in AREAS.items():
                stations = find_stations(session, "groundwaterLevel",
                                         lat=lat, long=lon, dist=SEARCH_RADIUS_KM)
                cur.execute(f"SELECT MAX(day) FROM {TABLE_NAME} WHERE area = %s;", (area,))
                last_day = cur.fetchone()[0]

                for station in stations:
                    measure = pick_groundwater_measure(station)
                    if not measure:
                        continue
                    if forced_start:
                        since = forced_start
                    elif last_day:
                        since = last_day - timedelta(days=30)
                    elif "-86400-" in measure or "dipped" in measure:
                        since = HISTORY_START
                    else:
                        since = date.today() - timedelta(days=LOGGER_HISTORY_DAYS)
                    rows = daily_mean(fetch_readings(session, measure, since))
                    if not rows:
                        continue  # try the next nearest borehole

                    resolved += 1
                    label = text(station.get("label"))
                    cur.execute(f"""
                        INSERT INTO {STATIONS_TABLE} (key, kind, label, river, lat, long, measure)
                        VALUES (%s, 'groundwater', %s, NULL, %s, %s, %s)
                        ON CONFLICT (key) DO UPDATE SET label = EXCLUDED.label,
                            lat = EXCLUDED.lat, long = EXCLUDED.long, measure = EXCLUDED.measure,
                            updated_at = NOW();
                    """, (f"gw_{area}", label, station.get("lat"), station.get("long"), measure))
                    cur.executemany(f"""
                        INSERT INTO {TABLE_NAME} (day, area, level_maod, quality)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (day, area) DO UPDATE
                        SET level_maod = EXCLUDED.level_maod, quality = EXCLUDED.quality;
                    """, [(day, area, value, q) for day, value, q in rows])
                    conn.commit()
                    print(f"{area}: {label}: {len(rows)} days ({rows[0][0]} -> {rows[-1][0]})")
                    break
                else:
                    print(f"WARNING: no groundwater borehole with data near {area} ({lat}, {lon})")

    if resolved == 0:
        print("ERROR: no boreholes resolved")
        sys.exit(1)


if __name__ == "__main__":
    main()
