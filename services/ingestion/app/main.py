from __future__ import annotations

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.jobs.scheduler import build_scheduler
from app.routers import admin, jobs, scrape

logging.basicConfig(level=logging.INFO)


@asynccontextmanager
async def lifespan(app: FastAPI):
    sched = build_scheduler()
    sched.start()
    app.state.scheduler = sched
    try:
        yield
    finally:
        sched.shutdown(wait=False)


app = FastAPI(title="VIA Delay — Ingestion", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(scrape.router, prefix="/scrape", tags=["scrape"])
app.include_router(jobs.router, prefix="/jobs", tags=["jobs"])
app.include_router(admin.router, prefix="/admin", tags=["admin"])


@app.get("/healthz")
def healthz():
    return {"ok": True}
