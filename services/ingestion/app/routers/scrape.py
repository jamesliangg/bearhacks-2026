from __future__ import annotations

import uuid
from datetime import date, datetime
from typing import List

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from via_common.storage import storage
from app.scrapers import transitdocs, via_live

router = APIRouter()


class BackfillRequest(BaseModel):
    trains: List[str]
    start_date: date
    end_date: date


def _run(kind: str, fn, **kwargs) -> dict:
    job_id = str(uuid.uuid4())
    started = datetime.utcnow()
    try:
        rows = list(fn(**kwargs))
        count = storage.upsert_stop_observations(rows)
        storage.record_job_run(job_id, kind, started, datetime.utcnow(),
                                "ok", count)
        return {"job_id": job_id, "kind": kind, "rows": count, "status": "ok"}
    except Exception as e:
        storage.record_job_run(job_id, kind, started, datetime.utcnow(),
                                "error", 0, str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/historical/{train}/{service_date}")
def scrape_historical(train: str, service_date: date):
    if storage.has_stop_observations(train, service_date, source="transitdocs"):
        return {"job_id": None, "kind": "historical", "rows": 0, "status": "skipped", "reason": "data_exists"}
    return _run("historical", transitdocs.scrape, train=train, service_date=service_date)


@router.post("/historical/backfill")
def backfill(req: BackfillRequest):
    results = []
    from dateutil.rrule import rrule, DAILY
    for t in req.trains:
        for dt in rrule(DAILY, dtstart=req.start_date, until=req.end_date):
            service_date = dt.date()
            if storage.has_stop_observations(t, service_date, source="transitdocs"):
                results.append(
                    {"job_id": None, "kind": "historical", "rows": 0, "status": "skipped", "reason": "data_exists",
                     "train": t, "service_date": service_date.isoformat()}
                )
                continue
            results.append(_run("historical", transitdocs.scrape, train=t, service_date=service_date))
    return {"jobs": results}


@router.post("/live/{train}")
def scrape_live(train: str):
    return _run("live", via_live.scrape, train=train)
