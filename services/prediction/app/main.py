from __future__ import annotations

import logging

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.routers import predict

logging.basicConfig(level=logging.INFO)

app = FastAPI(title="VIA Delay — Prediction")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # restrict to extension host in production
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(predict.router, tags=["predict"])


@app.get("/healthz")
def healthz():
    return {"ok": True}
