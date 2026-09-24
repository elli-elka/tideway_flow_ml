"""
Ingest new Richmond Lock tidal readings from the Environment Agency API
and upsert into PostgreSQL.

Designed for automation (cron / GitHub Actions / cloud runners).
Pass a date (YYYY-MM-DD) as the first argument to backfill from that date.
"""

from common import run_ea_ingest


# --------------------------------------------------
# CONFIG
# --------------------------------------------------

MEASURE_ID = "0009-level-tidal_level-i-15_min-mAOD"
STATION_ID = "0009"
TABLE_NAME = "richmond_tidal_levels"


# --------------------------------------------------

if __name__ == "__main__":
    run_ea_ingest(TABLE_NAME, "water_level", MEASURE_ID, STATION_ID)
