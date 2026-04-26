"""TransitDocs historical scraper.

The public viewer at
  https://asm.transitdocs.com/train/{yyyy}/{m}/{d}/V/{train}
is backed by a JSON endpoint. The exact path changes from time to time, so we
probe a small set of known URL shapes and return the first one that parses.

If all probes fail we emit a single synthetic row with a None delay so the job
still records something useful (and tests can run offline).
"""
from __future__ import annotations

import logging
from datetime import date, datetime, timedelta, timezone
from typing import Iterable

import httpx

from via_common.config import settings
from via_common.schemas import StopObservation

log = logging.getLogger(__name__)


def _candidate_urls(train: str, d: date) -> list[str]:
    base = settings.TRANSITDOCS_BASE.rstrip("/")
    return [
        f"https://asm-backend.transitdocs.com/train/{d:%Y/%m/%d}/V/{train}?points=true",
        f"{base}/api/train/{d.year}/{d.month}/{d.day}/V/{train}",
        f"{base}/train/{d.year}/{d.month}/{d.day}/V/{train}.json",
        f"{base}/train/{d.year}/{d.month}/{d.day}/V/{train}",
    ]


def _parse_dt(value) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        try:
            return datetime.fromtimestamp(value, tz=timezone.utc).replace(tzinfo=None)
        except Exception:
            return None
    if isinstance(value, str):
        for fmt in ("%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%SZ",
                    "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
            try:
                dt = datetime.strptime(value, fmt)
                return dt.replace(tzinfo=None)
            except ValueError:
                continue
    return None


def _delay_from_var_seconds(var_seconds: float | None) -> float | None:
    if var_seconds is None:
        return None
    # TransitDocs backend variance is commonly "scheduled - actual" in seconds.
    # Negative => late, Positive => early. Our delay_minutes is non-negative lateness.
    try:
        return max(0.0, (-float(var_seconds)) / 60.0)
    except Exception:
        return None


def _extract_stops(payload: dict) -> list[dict]:
    for key in ("stops", "locations", "stations", "timeline"):
        if isinstance(payload.get(key), list):
            return payload[key]
    return []


def _variance_seconds(stop: dict, key: str) -> float | None:
    value = stop.get(key)
    if isinstance(value, dict):
        var = value.get("variance")
        if isinstance(var, (int, float)):
            return float(var)
    return None


def scrape(train: str, service_date: date) -> Iterable[StopObservation]:
    headers = {"User-Agent": settings.SCRAPE_USER_AGENT, "Accept": "application/json"}
    payload: dict | None = None
    last_error: Exception | None = None

    with httpx.Client(timeout=20.0, headers=headers, follow_redirects=True) as client:
        for url in _candidate_urls(train, service_date):
            try:
                r = client.get(url)
                if r.status_code != 200:
                    continue
                ct = r.headers.get("content-type", "")
                if "json" not in ct and not r.text.lstrip().startswith("{"):
                    continue
                payload = r.json()
                break
            except Exception as e:
                last_error = e
                continue

    if not payload:
        log.warning("transitdocs: no json payload for train=%s date=%s (%s)",
                    train, service_date, last_error)
        return []

    results: list[StopObservation] = []
    for i, stop in enumerate(_extract_stops(payload)):
        sched_arr = _parse_dt(
            stop.get("scheduled_arrival")
            or stop.get("scheduledArrival")
            or stop.get("sched_arrive")
        )
        sched_dep = _parse_dt(
            stop.get("scheduled_departure")
            or stop.get("scheduledDeparture")
            or stop.get("sched_depart")
        )

        actual_arr = _parse_dt(stop.get("actual_arrival") or stop.get("actualArrival"))
        actual_dep = _parse_dt(stop.get("actual_departure") or stop.get("actualDeparture"))

        # Newer TransitDocs backend payload uses epoch seconds for sched_* and variance seconds for delays.
        if sched_arr is None and stop.get("sched_arrive") is not None:
            sched_arr = _parse_dt(stop.get("sched_arrive"))
        if sched_dep is None and stop.get("sched_depart") is not None:
            sched_dep = _parse_dt(stop.get("sched_depart"))

        # TransitDocs current payload provides variance (seconds) under arrive/depart objects.
        if actual_arr is None and sched_arr is not None:
            arr_var = _variance_seconds(stop, "arrive")
            if arr_var is not None:
                actual_arr = sched_arr + timedelta(seconds=arr_var)
        if actual_dep is None and sched_dep is not None:
            dep_var = _variance_seconds(stop, "depart")
            if dep_var is not None:
                actual_dep = sched_dep + timedelta(seconds=dep_var)

        # Delay logic: prefer timestamp deltas (actual - scheduled) so we interpret "late" correctly
        # even if upstream variance fields use an unexpected sign convention.
        delay: float | None = None
        # Prefer backend variance (most reliable and timezone-free) when present.
        arr_var = _variance_seconds(stop, "arrive")
        dep_var = _variance_seconds(stop, "depart")
        delay = _delay_from_var_seconds(arr_var) or _delay_from_var_seconds(dep_var)
        if delay is None:
            # Fall back to timestamp delta (these can be timezone-shifted depending on payload shape).
            if sched_arr and actual_arr:
                delay = (actual_arr - sched_arr).total_seconds() / 60.0
            elif sched_dep and actual_dep:
                delay = (actual_dep - sched_dep).total_seconds() / 60.0
            elif (stop.get("delay") or stop.get("delay_minutes")) is not None:
                try:
                    delay = float(stop.get("delay") or stop.get("delay_minutes"))
                except Exception:
                    delay = None
            if delay is not None:
                delay = max(0.0, delay)

        results.append(StopObservation(
            train_number=str(train),
            service_date=service_date,
            stop_sequence=int(stop.get("sequence", i)),
            station_code=str(stop.get("code") or stop.get("station") or stop.get("name") or f"S{i}"),
            scheduled_arrival=sched_arr,
            actual_arrival=actual_arr,
            scheduled_departure=sched_dep,
            actual_departure=actual_dep,
            delay_minutes=float(delay) if delay is not None else None,
            source="transitdocs",
        ))
    return results
