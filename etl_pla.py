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
    inserted_updated = 0
    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            for i, row in enumerate(heights):
                ts = row.get("ts") or row.get("tstamp")
                pred = row.get("predicted")
                obs = row.get("observed")
                surge = row.get("surge")

                # --- 1. SENSOR SPIKE FILTER ---
                if i > 0 and obs is not None and heights[i-1].get("observed") is not None:
                    diff = abs(obs - heights[i-1].get("observed"))
                    if diff > 0.4: # Richmond doesn't jump 40cm in 5 mins
                        obs = heights[i-1].get("observed") 
                        if pred is not None:
                            surge = round(obs - pred, 2)

                # --- 2. PERSISTENT FLOW LOGIC (DIRECTIONAL MEMORY) ---
                # We compare current water to 15 mins ago
                lookback = 3 
                if i >= lookback:
                    past_obs = heights[i-lookback].get("observed")
                    past_pred = heights[i-lookback].get("predicted")

                    # Primary: Observed data
                    if obs is not None and past_obs is not None:
                        # Only switch if there is a clear 1cm move
                        if obs > past_obs + 0.01:
                            current_flow = "Flood"
                        elif obs < past_obs - 0.01:
                            current_flow = "Ebb"
                        # Else: Keep current_flow as it was (Memory)
                    
                    # Secondary: Fallback to Predicted
                    elif pred is not None and past_pred is not None:
                        if pred > past_pred:
                            current_flow = "Flood"
                        elif pred < past_pred:
                            current_flow = "Ebb"

                event = turns_lookup.get(ts)

                # --- 3. DATABASE UPSERT ---
                cur.execute(f"""
                    INSERT INTO {TABLE_NAME} (ts, predicted, observed, surge, tide_event, tidal_flow)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (ts) DO UPDATE 
                    SET observed = EXCLUDED.observed,
                        surge = EXCLUDED.surge,
                        tide_event = COALESCE({TABLE_NAME}.tide_event, EXCLUDED.tide_event),
                        tidal_flow = EXCLUDED.tidal_flow;
                """, (ts, pred, obs, surge, event, current_flow))
                
                inserted_updated += 1
                
        conn.commit()

    print(f"Successfully processed {inserted_updated} readings for Richmond.")

if __name__ == "__main__":
    main()