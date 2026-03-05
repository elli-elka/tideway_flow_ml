import os
import sys
import requests
import psycopg
from datetime import datetime, timedelta
from dotenv import load_dotenv

# --------------------------------------------------
# CONFIG
# --------------------------------------------------
TABLE_NAME = "richmond_pla_levels"
API_URL = "https://pla.co.uk/pla-proxy/one-minute?url=tides/chart/14541"

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
    load_dotenv()
    database_url = os.getenv("DATABASE_URL")

    if not database_url:
        print("ERROR: DATABASE_URL not set")
        sys.exit(1)

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
    print("Fetching Richmond tide data...")
    try:
        response = requests.get(API_URL, timeout=30)
        response.raise_for_status()
        raw_data = response.json()
    except Exception as e:
        print(f"Fetch failed: {e}")
        return

    heights = raw_data.get("heights", [])
    tpoints = raw_data.get("tpoints", [])

    if not heights:
        print("No data found.")
        return

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

                # Initialize event as None
                event = None
                
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
                
                row["flow_memory"] = current_flow

                # 3. UPSERT
                cur.execute(f"""
                    INSERT INTO {TABLE_NAME} (ts, predicted, observed, surge, tide_event, tidal_flow)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (ts) DO UPDATE 
                    SET observed = EXCLUDED.observed,
                        surge = EXCLUDED.surge,
                        tidal_flow = EXCLUDED.tidal_flow,
                        tide_event = EXCLUDED.tide_event;
                """, (ts, pred, obs, surge, event, current_flow))
                inserted_updated += 1
                
        conn.commit()

    print(f"Successfully processed {inserted_updated} readings for Richmond.")

if __name__ == "__main__":
    main()