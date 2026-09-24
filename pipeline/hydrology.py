"""
Helpers for the Environment Agency Hydrology API: quality-checked daily river flows
and groundwater levels with records going back decades.
https://environment.data.gov.uk/hydrology/doc/reference

Stations are looked up by name/location at runtime (rather than hard-coding the
API's internal station GUIDs) and the resolved station is printed and stored, so it
is easy to check the right gauge was picked.
"""

from collections import defaultdict
from datetime import date

from common import TIMEOUT

HYDROLOGY_BASE = "https://environment.data.gov.uk/hydrology"
PAGE_LIMIT = 100000


def get_items(session, path, params):
    response = session.get(f"{HYDROLOGY_BASE}/{path}", params=params, timeout=TIMEOUT)
    response.raise_for_status()
    return response.json().get("items", [])


def find_stations(session, observed_property, **filters):
    """Stations measuring `observed_property` (waterFlow, groundwaterLevel, ...)
    matching filters such as search=<label text> or lat/long/dist."""
    params = {"observedProperty": observed_property, "_limit": 50, **filters}
    return get_items(session, "id/stations.json", params)


def text(value):
    """Hydrology API fields are sometimes a list or a {'label': ...} object."""
    if isinstance(value, list):
        value = value[0] if value else ""
    if isinstance(value, dict):
        value = value.get("label") or value.get("@id") or ""
    return str(value or "")


def measure_ids(station):
    for m in station.get("measures", []) or []:
        yield m.get("@id", "") if isinstance(m, dict) else str(m)


def pick_measure(station, parameter, prefer=("-m-86400-", "-86400-")):
    """Choose the station's measure for `parameter` (flow/level), preferring daily means."""
    ids = [i for i in measure_ids(station) if f"-{parameter}-" in i]
    for pattern in prefer:
        for i in ids:
            if pattern in i:
                return i
    return ids[0] if ids else None


def fetch_readings(session, measure_url, since):
    """All readings for a measure on or after `since` -> [(date, value, quality)]."""
    path = measure_url.split("/hydrology/", 1)[-1] + "/readings.json"
    rows, offset = [], 0
    while True:
        items = get_items(session, path, {
            "mineq-date": since.isoformat(), "_limit": PAGE_LIMIT, "_offset": offset,
        })
        for item in items:
            value = item.get("value")
            day = item.get("date") or (item.get("dateTime") or "")[:10]
            if value is None or not day:
                continue
            rows.append((date.fromisoformat(day[:10]), float(value), text(item.get("quality"))))
        if len(items) < PAGE_LIMIT:
            return rows
        offset += PAGE_LIMIT


def daily_mean(rows):
    """Collapse sub-daily readings to one mean value per day."""
    by_day = defaultdict(list)
    quality = {}
    for day, value, q in rows:
        by_day[day].append(value)
        quality.setdefault(day, q)
    return [(day, sum(v) / len(v), quality[day]) for day, v in sorted(by_day.items())]
