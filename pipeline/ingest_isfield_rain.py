"""
Ingest Rainfall readings for Isfield Gauge (E8290) from the Environment Agency API
and upsert into PostgreSQL.

Pass a date (YYYY-MM-DD) as the first argument to backfill from that date.
"""

from common import run_ea_ingest


# --------------------------------------------------
# CONFIG
# --------------------------------------------------

STATION_ID = "E8290"
MEASURE_ID = f"{STATION_ID}-rainfall-tipping_bucket_raingauge-t-15_min-mm"
TABLE_NAME = "isfield_rainfall_readings"


# --------------------------------------------------

if __name__ == "__main__":
    run_ea_ingest(TABLE_NAME, "rainfall_mm", MEASURE_ID, STATION_ID)
