"""
Daily mean river flow for the Thames and its main tributaries upstream of Teddington,
from the EA Hydrology API (quality-checked, records back decades).

Water passing Reading or Staines today reaches Teddington a day or two later, so these
gauges give the model advance notice of what Kingston flow (and so the ebb tide flag)
is about to do.

Usage: python ingest_upstream_flows.py [start YYYY-MM-DD for a full backfill]
"""

import os
import sys
from datetime import date, timedelta

import psycopg

from common import get_database_url, http_session
from hydrology import fetch_readings, find_stations, pick_measure, text


# --------------------------------------------------
# CONFIG
# --------------------------------------------------

# gauge key -> (station name to search for, river name it must be on)
GAUGES = {
    "kingston":           ("Kingston",    "Thames"),
    "staines":            ("Staines",     "Thames"),
    "reading":            ("Reading",     "Thames"),
    "days_weir":          ("Days Weir",   "Thames"),
    "kennet_theale":      ("Theale",      "Kennet"),
    "loddon_sheepbridge": ("Sheepbridge", "Loddon"),
    "wey_weybridge":      ("Weybridge",   "Wey"),
    "mole_esher":         ("Esher",       "Mole"),
    "colne_denham":       ("Denham",      "Colne"),
}
TABLE_NAME = "river_flow_daily"
STATIONS_TABLE = "hydrology_stations"
HISTORY_START = date.fromisoformat(os.getenv("FLOW_HISTORY_START", "2005-01-01"))


# --------------------------------------------------
# MAIN LOGIC
# --------------------------------------------------

def resolve_station(session, search, river):
    candidates = find_stations(session, "waterFlow", search=search)
    # Only accept a station on the expected river (or with no river recorded), so a
    # same-named gauge elsewhere is never picked by mistake
    on_river = [s for s in candidates if river.lower() in text(s.get("riverName")).lower()
                or not text(s.get("riverName"))]
    for station in on_river:
        measure = pick_measure(station, "flow")
        if measure:
            return station, measure
    return None, None


def main():
    database_url = get_database_url()
    session = http_session()
    forced_start = date.fromisoformat(sys.argv[1]) if len(sys.argv) > 1 and sys.argv[1] else None

    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
                    day DATE NOT NULL,
                    gauge TEXT NOT NULL,
                    flow_m3s DOUBLE PRECISION NOT NULL,
                    quality TEXT,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    PRIMARY KEY (day, gauge)
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
            for gauge, (search, river) in GAUGES.items():
                station, measure = resolve_station(session, search, river)
                if not measure:
                    print(f"WARNING: no flow gauge found for {gauge} ({search} on {river})")
                    continue
                resolved += 1
                label, river_name = text(station.get("label")), text(station.get("riverName"))
                cur.execute(f"""
                    INSERT INTO {STATIONS_TABLE} (key, kind, label, river, lat, long, measure)
                    VALUES (%s, 'flow', %s, %s, %s, %s, %s)
                    ON CONFLICT (key) DO UPDATE SET label = EXCLUDED.label, river = EXCLUDED.river,
                        lat = EXCLUDED.lat, long = EXCLUDED.long, measure = EXCLUDED.measure,
                        updated_at = NOW();
                """, (gauge, label, river_name, station.get("lat"), station.get("long"), measure))

                cur.execute(f"SELECT MAX(day) FROM {TABLE_NAME} WHERE gauge = %s;", (gauge,))
                last_day = cur.fetchone()[0]
                # Re-fetch the last 30 days: recent values get revised as they are quality-checked
                since = forced_start or (last_day - timedelta(days=30) if last_day else HISTORY_START)

                rows = fetch_readings(session, measure, since)
                cur.executemany(f"""
                    INSERT INTO {TABLE_NAME} (day, gauge, flow_m3s, quality)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (day, gauge) DO UPDATE
                    SET flow_m3s = EXCLUDED.flow_m3s, quality = EXCLUDED.quality;
                """, [(day, gauge, value, q) for day, value, q in rows])
                conn.commit()
                span = f"{rows[0][0]} -> {rows[-1][0]}" if rows else "no data"
                print(f"{gauge}: {label} on {river_name}: {len(rows)} days ({span})")

    if resolved == 0:
        print("ERROR: no gauges resolved")
        sys.exit(1)


if __name__ == "__main__":
    main()
