from __future__ import annotations

import logging
from contextlib import asynccontextmanager

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.pipelines.train_model import train
from app.routers import train as train_router

logging.basicConfig(level=logging.INFO)


@asynccontextmanager
async def lifespan(app: FastAPI):
    sched = AsyncIOScheduler(timezone="America/Toronto")
    sched.add_job(train, "cron", hour=2, minute=45, id="nightly_train")
    sched.start()
    app.state.scheduler = sched
    try:
        yield
    finally:
        sched.shutdown(wait=False)


app = FastAPI(title="VIA Delay — Training", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])
app.include_router(train_router.router, tags=["training"])


@app.get("/healthz")
def healthz():
    return {"ok": True}
