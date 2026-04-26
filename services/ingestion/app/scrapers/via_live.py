"""VIA Rail live (in-service) scraper.

tsimobile.viarail.ca loads train data from an internal JSON endpoint. We probe
a few likely shapes and return stop observations with source='via_live'.
"""
from __future__ import annotations

import logging
import re
from datetime import date, datetime
from typing import Iterable

import httpx

from via_common.config import settings
from via_common.schemas import StopObservation

log = logging.getLogger(__name__)


def _candidate_urls(train: str) -> list[str]:
    base = settings.VIA_LIVE_BASE.rstrip("/")
    return [
        f"{base}/data/allData.json",
        f"{base}/data/{train}.json",
        f"{base}/api/train/{train}",
        f"{base}/trains/{train}.json",
    ]


def _parse_dt(value) -> datetime | None:
    if not value:
        return None
    if isinstance(value, str):
        for fmt in ("%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%SZ",
                    "%Y-%m-%d %H:%M:%S"):
            try:
                return datetime.strptime(value, fmt).replace(tzinfo=None)
            except ValueError:
                continue
    return None


def _service_date_from_key(train_key: str, default: date) -> date:
    match = re.search(r"\((\d{2})-(\d{2})\)", train_key)
    if not match:
        return default
    month = int(match.group(1))
    day = int(match.group(2))
    year = default.year
    try:
        candidate = date(year, month, day)
    except ValueError:
        return default
    # Handle year rollover near Jan/Dec boundaries.
    if (candidate - default).days > 180:
        return date(year - 1, month, day)
    if (default - candidate).days > 180:
        return date(year + 1, month, day)
    return candidate


def _select_train_payload(all_data: dict, train: str, service_date: date) -> tuple[str, dict] | None:
    direct = all_data.get(train)
    if isinstance(direct, dict):
        return train, direct

    candidates: list[tuple[int, str, dict]] = []
    for key, value in all_data.items():
        if not isinstance(value, dict):
            continue
        if not (key == train or key.startswith(f"{train} ")):
            continue
        key_date = _service_date_from_key(key, service_date)
        distance = abs((key_date - service_date).days)
        if value.get("departed") and not value.get("arrived"):
            distance -= 10
        candidates.append((distance, key, value))

    if not candidates:
        return None

    candidates.sort(key=lambda x: (x[0], x[1]))
    _, key, payload = candidates[0]
    return key, payload


def _observations_from_all_data(train: str, service_date: date, payload: dict) -> list[StopObservation]:
    times = payload.get("times") or []
    if not isinstance(times, list):
        return []

    results: list[StopObservation] = []
    for i, stop in enumerate(times):
        if not isinstance(stop, dict):
            continue

        sched_arr = _parse_dt(stop.get("scheduled"))
        actual_arr = _parse_dt(stop.get("estimated"))

        dep = stop.get("departure") if isinstance(stop.get("departure"), dict) else {}
        sched_dep = _parse_dt(dep.get("scheduled"))
        actual_dep = _parse_dt(dep.get("estimated"))

        delay = stop.get("diffMin")
        if not isinstance(delay, (int, float)):
            delay = None
        if delay is None and sched_arr and actual_arr:
            delay = (actual_arr - sched_arr).total_seconds() / 60.0

        results.append(StopObservation(
            train_number=str(train),
            service_date=service_date,
            stop_sequence=i,
            station_code=str(stop.get("code") or stop.get("station") or f"S{i}"),
            scheduled_arrival=sched_arr,
            actual_arrival=actual_arr,
            scheduled_departure=sched_dep,
            actual_departure=actual_dep,
            delay_minutes=float(delay) if delay is not None else None,
            source="via_live",
        ))

    return results


def scrape(train: str, service_date: date | None = None) -> Iterable[StopObservation]:
    service_date = service_date or datetime.utcnow().date()
    headers = {"User-Agent": settings.SCRAPE_USER_AGENT, "Accept": "application/json"}
    payload: dict | None = None
    selected_key: str | None = None

    with httpx.Client(timeout=15.0, headers=headers, follow_redirects=True) as client:
        for url in _candidate_urls(train):
            try:
                r = client.get(url)
                if r.status_code != 200:
                    continue
                if "json" not in r.headers.get("content-type", ""):
                    continue
                candidate = r.json()
                if url.endswith("/data/allData.json") and isinstance(candidate, dict):
                    selected = _select_train_payload(candidate, str(train), service_date)
                    if selected:
                        selected_key, payload = selected
                        break
                    continue
                if isinstance(candidate, dict):
                    payload = candidate
                    break
            except Exception:
                continue

    if not payload:
        log.warning("via_live: no json payload for train=%s", train)
        return []

    if selected_key is not None:
        selected_date = _service_date_from_key(selected_key, service_date)
        rows = _observations_from_all_data(str(train), selected_date, payload)
        if rows:
            return rows

    stops = payload.get("stops") or payload.get("locations") or []
    results: list[StopObservation] = []
    for i, stop in enumerate(stops):
        sched_arr = _parse_dt(stop.get("scheduled_arrival"))
        actual_arr = _parse_dt(stop.get("estimated_arrival") or stop.get("actual_arrival"))
        delay = stop.get("delay")
        if delay is None and sched_arr and actual_arr:
            delay = (actual_arr - sched_arr).total_seconds() / 60.0
        results.append(StopObservation(
            train_number=str(train),
            service_date=service_date,
            stop_sequence=int(stop.get("sequence", i)),
            station_code=str(stop.get("code") or stop.get("station") or f"S{i}"),
            scheduled_arrival=sched_arr,
            actual_arrival=actual_arr,
            delay_minutes=float(delay) if delay is not None else None,
            source="via_live",
        ))
    return results
