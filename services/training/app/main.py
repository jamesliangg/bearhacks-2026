from __future__ import annotations

import logging
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.routers import train as train_router

logging.basicConfig(level=logging.INFO)

app = FastAPI(title="VIA Delay — Training")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])
app.include_router(train_router.router, tags=["training"])


@app.get("/healthz")
def healthz():
    return {"ok": True}
