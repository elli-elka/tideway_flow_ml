"""
Build feed.json for the Tideway Hub iPhone/iPad app.

The app never connects to the database: after each model run this writes one small
public JSON file (current flag, predictions, recent Richmond levels, Kingston flow,
upcoming tide times) which GitHub Actions publishes to GitHub Pages.

Usage: python publish_app_feed.py [output directory, default ./site]
"""

import json
import os
import sys
from datetime import datetime, timedelta, timezone

import psycopg

from build_ebb_flags import (EA_PLAUSIBLE, EA_TABLE, FLAGS_TABLE, PLA_TABLE, drop_spikes,
                             get_offset, table_exists)
from common import get_database_url
from predict_flags import PREDICTIONS_TABLE


# --------------------------------------------------
# CONFIG
# --------------------------------------------------

FEED_VERSION = 1
RICHMOND_HOURS = 36
KINGSTON_DAYS = 14
DISCLAIMER = ("Unofficial. Flags here are reconstructed from public gauge data and "
              "predictions are experimental. Always check the official PLA Ebb Tide "
              "Flag before boating.")
OFFICIAL_FLAG_URL = "https://pla.co.uk/ebb-tide-flag-warning"


# --------------------------------------------------
# UTILS
# --------------------------------------------------

def utc_now():
    """Current time; FEED_NOW (ISO timestamp) overrides it for testing on old data."""
    if os.getenv("FEED_NOW"):
        return datetime.fromisoformat(os.getenv("FEED_NOW")).astimezone(timezone.utc)
    return datetime.now(timezone.utc)


def iso(ts):
    return ts.astimezone(timezone.utc).isoformat(timespec="seconds") if ts else None


def rounded(value, places=3):
    return round(float(value), places) if value is not None else None


def query(cur, sql, params=()):
    cur.execute(sql, params)
    return cur.fetchall()


# --------------------------------------------------
# SECTIONS
# --------------------------------------------------

def flags_section(cur):
    rows = query(cur, f"""
        SELECT issued_at, flag, min_level_cd, min_level_ts, source
        FROM {FLAGS_TABLE} ORDER BY issued_at DESC LIMIT 14;
    """)
    recent = [{"issued_at": iso(t), "flag": f, "level_cd": rounded(lvl),
               "low_at": iso(low_ts), "source": src} for t, f, lvl, low_ts, src in rows]
    return (recent[0] if recent else None), list(reversed(recent))


def predictions_section(cur):
    if not table_exists(cur, PREDICTIONS_TABLE):
        return None
    rows = query(cur, f"""
        SELECT base_issue, target_issue, horizon, flag, pred_level_cd,
               p_black, p_green, p_yellow, p_red, method, model_version
        FROM {PREDICTIONS_TABLE}
        WHERE base_issue = (SELECT MAX(base_issue) FROM {PREDICTIONS_TABLE})
        ORDER BY target_issue;
    """)
    if not rows:
        return None
    return {
        "base_issue": iso(rows[0][0]),
        "model_version": rows[0][10],
        "issues": [{
            "issue_at": iso(target), "horizon": h, "flag": flag, "level_cd": rounded(lvl),
            "probabilities": {"BLACK": rounded(pb), "GREEN": rounded(pg),
                              "YELLOW": rounded(py), "RED": rounded(pr)},
            "method": method,
        } for _, target, h, flag, lvl, pb, pg, py, pr, method, _ in rows],
    }


def richmond_section(cur, offset):
    since = utc_now() - timedelta(hours=RICHMOND_HOURS)
    readings = []
    if table_exists(cur, PLA_TABLE):
        readings = [(ts, lvl) for ts, lvl in query(cur, f"""
            SELECT ts, observed FROM {PLA_TABLE}
            WHERE observed IS NOT NULL AND ts > %s ORDER BY ts;""", (since,))]
    source = "pla"
    if not readings and offset is not None and table_exists(cur, EA_TABLE):
        ea = drop_spikes(query(cur, f"SELECT ts, water_level FROM {EA_TABLE} WHERE ts > %s ORDER BY ts;",
                               (since,)), EA_PLAUSIBLE)
        readings = [(ts, lvl + offset) for ts, lvl in ea]
        source = "ea"
    if not readings:
        return None
    latest_ts, latest = readings[-1]
    earlier = [lvl for ts, lvl in readings if ts <= latest_ts - timedelta(minutes=15)]
    trend = None
    if earlier:
        trend = "flood" if latest > earlier[-1] else "ebb"
    return {
        "source": source,
        "latest_at": iso(latest_ts),
        "level_cd": rounded(latest),
        "stream": trend,
        "series": [{"t": iso(ts), "level_cd": rounded(lvl)} for ts, lvl in readings],
    }


def kingston_section(cur):
    if not table_exists(cur, "kingston_flow_readings"):
        return None
    since = utc_now() - timedelta(days=KINGSTON_DAYS)
    rows = query(cur, """
        SELECT date_trunc('hour', ts) AS hour, AVG(flow_m3s)
        FROM kingston_flow_readings WHERE ts > %s
        GROUP BY 1 ORDER BY 1;""", (since,))
    if not rows:
        return None
    series = rows[::3]  # every 3 hours is plenty for a sparkline
    if series[-1] != rows[-1]:
        series.append(rows[-1])
    day_ago = [f for t, f in rows if t <= rows[-1][0] - timedelta(hours=24)]
    return {
        "latest_at": iso(rows[-1][0]),
        "flow_m3s": rounded(rows[-1][1], 1),
        "change_24h": rounded(rows[-1][1] - day_ago[-1], 1) if day_ago else None,
        "series": [{"t": iso(t), "flow_m3s": rounded(f, 1)} for t, f in series],
    }


def tides_section(cur):
    """Upcoming high/low waters from PLA's astronomical prediction, if stored."""
    if not table_exists(cur, PLA_TABLE):
        return None
    now = utc_now()
    rows = query(cur, f"""
        SELECT ts, predicted FROM {PLA_TABLE}
        WHERE predicted IS NOT NULL AND ts BETWEEN %s AND %s ORDER BY ts;""",
                 (now - timedelta(hours=1), now + timedelta(hours=48)))
    events = []
    for i in range(1, len(rows) - 1):
        (_, prev), (ts, cur_level), (_, nxt) = rows[i - 1], rows[i], rows[i + 1]
        if cur_level >= prev and cur_level > nxt:
            events.append({"t": iso(ts), "type": "high", "predicted_cd": rounded(cur_level, 2)})
        elif cur_level <= prev and cur_level < nxt:
            events.append({"t": iso(ts), "type": "low", "predicted_cd": rounded(cur_level, 2)})
    return [e for e in events if e["t"] >= iso(now)] or None


# --------------------------------------------------
# MAIN LOGIC
# --------------------------------------------------

def main():
    out_dir = sys.argv[1] if len(sys.argv) > 1 else "site"
    database_url = get_database_url()

    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            have_pla, have_ea = table_exists(cur, PLA_TABLE), table_exists(cur, EA_TABLE)
            offset, _ = get_offset(cur, have_pla, have_ea)
            current, recent = flags_section(cur)
            feed = {
                "version": FEED_VERSION,
                "generated_at": iso(utc_now()),
                "disclaimer": DISCLAIMER,
                "official_flag_url": OFFICIAL_FLAG_URL,
                "current_flag": current,
                "recent_flags": recent,
                "predictions": predictions_section(cur),
                "richmond": richmond_section(cur, offset),
                "kingston_flow": kingston_section(cur),
                "tides": tides_section(cur),
            }

    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, "feed.json")
    with open(path, "w") as f:
        json.dump(feed, f, indent=1)
    flag = current["flag"] if current else "none"
    n_pred = len(feed["predictions"]["issues"]) if feed["predictions"] else 0
    print(f"Wrote {path}: current flag {flag}, {n_pred} predictions")


if __name__ == "__main__":
    main()
