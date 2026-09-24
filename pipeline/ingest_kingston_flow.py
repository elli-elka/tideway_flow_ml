"""
Ingest Thames at Kingston (Teddington) river flow readings (station 3400TH) from the
Environment Agency API and upsert into PostgreSQL.

This is the fluvial flow coming over Teddington weir, which is what pushes the
Richmond low-tide level up (and the ebb tide flag towards yellow/red).

Pass a date (YYYY-MM-DD) as the first argument to backfill from that date.
For decades of daily history use backfill_kingston_nrfa.py instead.
"""

from common import run_ea_ingest


# --------------------------------------------------
# CONFIG
# --------------------------------------------------

STATION_ID = "3400TH"
# Of the station's two 15-min flow measures, this is the one currently reporting
MEASURE_ID = f"{STATION_ID}-flow-water-i-15_min-m3_s"
TABLE_NAME = "kingston_flow_readings"


# --------------------------------------------------

if __name__ == "__main__":
    run_ea_ingest(TABLE_NAME, "flow_m3s", MEASURE_ID, STATION_ID)
