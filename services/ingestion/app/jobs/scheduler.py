from __future__ import annotations

import logging
from datetime import date, timedelta

from apscheduler.schedulers.asyncio import AsyncIOScheduler

from via_common.storage import storage
from app.scrapers import transitdocs, via_live

log = logging.getLogger(__name__)

TRACKED_TRAINS = ["67", "68", "55", "62", "63", "64", "65", "622", "651"]


def nightly_backfill():
    yesterday = date.today() - timedelta(days=1)
    for t in TRACKED_TRAINS:
        try:
            rows = list(transitdocs.scrape(t, yesterday))
            storage.upsert_stop_observations(rows)
            log.info("nightly backfill train=%s date=%s rows=%d", t, yesterday, len(rows))
        except Exception as e:  # noqa: BLE001
            log.exception("nightly backfill failed train=%s: %s", t, e)


def live_poll():
    for t in TRACKED_TRAINS:
        try:
            rows = list(via_live.scrape(t))
            storage.upsert_stop_observations(rows)
        except Exception as e:  # noqa: BLE001
            log.exception("live poll failed train=%s: %s", t, e)


def build_scheduler() -> AsyncIOScheduler:
    sched = AsyncIOScheduler(timezone="America/Toronto")
    return sched
