import sys
import psycopg
from datetime import datetime, timedelta

from common import TIMEOUT, get_database_url, http_session

# --------------------------------------------------
# CONFIG
# --------------------------------------------------
TABLE_NAME = "richmond_pla_levels"
API_URL = "https://pla.co.uk/pla-proxy/one-minute?url=tides/chart/14541"
BROWSER_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/128.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://pla.co.uk/",
}


# --------------------------------------------------
# UTILS
# --------------------------------------------------

def round_to_nearest_5_mins(ts_str):
    dt = datetime.fromisoformat(ts_str.replace('Z', '+00:00'))
    minute = dt.minute
    remainder = minute % 5
    if remainder < 3:
        dt -= timedelta(minutes=remainder)
    else:
        dt += timedelta(minutes=(5 - remainder))
    dt = dt.replace(second=0, microsecond=0)
    return dt.isoformat()

# --------------------------------------------------
# MAIN LOGIC
# --------------------------------------------------

def main():
    database_url = get_database_url()

    # 1. Database Setup
    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
                    ts TIMESTAMPTZ PRIMARY KEY,
                    predicted DOUBLE PRECISION,
                    observed DOUBLE PRECISION,
                    surge DOUBLE PRECISION,
                    tide_event TEXT,
                    tidal_flow TEXT,
                    station_id INTEGER DEFAULT 14541,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                );
            """)
        conn.commit()

    # 2. Fetch data
    # The PLA proxy returns 403 to requests without browser-like headers, and may
    # block cloud IP ranges (e.g. GitHub Actions) altogether.
    print("Fetching Richmond tide data...")
    session = http_session(headers=BROWSER_HEADERS)
    try:
        response = session.get(API_URL, timeout=TIMEOUT)
        response.raise_for_status()
        raw_data = response.json()
    except Exception as e:
        # Exit non-zero so the workflow fails and sends an email instead of
        # silently "succeeding" without any data
        print(f"ERROR: Fetch failed: {e}")
        sys.exit(1)

    heights = raw_data.get("heights", [])
    tpoints = raw_data.get("tpoints", [])
    print(f"Heights count: {len(heights)}, Tpoints count: {len(tpoints)}")

    if not heights:
        print("ERROR: No data found in response.")
        sys.exit(1)

    # 3. Prepare Lookups
    turns_lookup = {}
    for p in tpoints:
        raw_ts = p.get("tstamp")
        state = p.get("tidal_state")
        if raw_ts and state in [1, 2]:
            rounded_ts = round_to_nearest_5_mins(raw_ts)
            turns_lookup[rounded_ts] = "High" if state == 1 else "Low"

    # Sort heights chronologically
    heights.sort(key=lambda x: x['tstamp'])
    
    # Initialize the "Memory" based on the first two predicted points
    current_flow = "Ebb"
    if len(heights) > 1:
        if (heights[1].get('predicted') or 0) > (heights[0].get('predicted') or 0):
            current_flow = "Flood"

    # 4. Process and Insert
    # --- 4. PROCESS AND INSERT (REFINED) ---
    inserted_updated = 0
    # Threshold for a "real" change to prevent noise flip-flopping
    CHANGE_THRESHOLD = 0.05 # 5cm

    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            for i, row in enumerate(heights):
                ts = row.get("tstamp")
                pred = row.get("predicted")
                obs = row.get("observed")
                surge = row.get("surge")

                # Initialize event as None. The first `lookback` rows have no history,
                # so their flow is unknown (NULL) and existing values are kept on upsert.
                event = None
                flow = None
                
                # Use a wider lookback to determine flow (15-20 mins)
                lookback = 4 
                if i >= lookback:
                    past_obs = heights[i-lookback].get("observed")
                    past_pred = heights[i-lookback].get("predicted")

                    # 1. Determine Flow Direction with Hysteresis
                    if obs is not None and past_obs is not None:
                        # Require a 5cm movement to change direction
                        if obs > past_obs + CHANGE_THRESHOLD:
                            new_flow = "Flood"
                        elif obs < past_obs - CHANGE_THRESHOLD:
                            new_flow = "Ebb"
                        else:
                            new_flow = current_flow # No significant change, maintain memory
                    else:
                        # Fallback to Predicted logic if observed is missing
                        new_flow = "Flood" if (pred or 0) > (past_pred or 0) else "Ebb"

                    # 2. Event Detection (The Turn)
                    # Only trigger an event if the flow direction actually flipped
                    if current_flow == "Ebb" and new_flow == "Flood":
                        # Sanity Check: Is predicted level actually low? (Prevents noise-highs)
                        if pred is not None and pred < 2.5: 
                            event = "Low"
                    elif current_flow == "Flood" and new_flow == "Ebb":
                        # Sanity Check: Is predicted level actually high?
                        if pred is not None and pred > 3.5:
                            event = "High"

                    current_flow = new_flow
                    flow = current_flow

                # 3. UPSERT
                cur.execute(f"""
                    INSERT INTO {TABLE_NAME} (ts, predicted, observed, surge, tide_event, tidal_flow)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (ts) DO UPDATE
                    SET predicted = COALESCE(EXCLUDED.predicted, {TABLE_NAME}.predicted),
                        observed = COALESCE(EXCLUDED.observed, {TABLE_NAME}.observed),
                        surge = COALESCE(EXCLUDED.surge, {TABLE_NAME}.surge),
                        tidal_flow = COALESCE(EXCLUDED.tidal_flow, {TABLE_NAME}.tidal_flow),
                        tide_event = CASE WHEN EXCLUDED.tidal_flow IS NULL
                                          THEN {TABLE_NAME}.tide_event
                                          ELSE EXCLUDED.tide_event END;
                """, (ts, pred, obs, surge, event, flow))
                inserted_updated += 1
                
        conn.commit()

    print(f"Successfully processed {inserted_updated} readings for Richmond.")

if __name__ == "__main__":
    main()