"""End-to-end smoke test without hitting the network or Snowflake."""
from __future__ import annotations

import importlib.util
import os
import sys
from datetime import date, datetime, timedelta

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "services/common"))

os.environ.setdefault("LOCAL_SQLITE_PATH", os.path.join(ROOT, "data/via_delays.sqlite"))
os.environ.setdefault("MODEL_DIR", os.path.join(ROOT, "models"))

from via_common.schemas import StopObservation
from via_common.storage import storage


def _load(name: str, path: str):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


# Seed observations.
obs: list[StopObservation] = []
start = date.today() - timedelta(days=60)
for i in range(60):
    d = start + timedelta(days=i)
    base_delay = 7 + (3 if d.weekday() in (0, 4) else 0) + (4 if d.month in (12, 1, 2) else 0)
    for seq, code in enumerate(["TRTO", "GUEL", "KITC", "LNDN", "WDSR"]):
        obs.append(StopObservation(
            train_number="67",
            service_date=d,
            stop_sequence=seq,
            station_code=code,
            scheduled_arrival=datetime(d.year, d.month, d.day, 8 + seq, 0),
            actual_arrival=datetime(d.year, d.month, d.day, 8 + seq, 0) + timedelta(minutes=base_delay + seq),
            delay_minutes=float(base_delay + seq),
            source="transitdocs",
        ))
n = storage.upsert_stop_observations(obs)
print(f"seeded rows: {n}")

# Load the training pipeline with its own package root.
sys.path.insert(0, os.path.join(ROOT, "services/training"))
training_pkg = importlib.import_module("app.pipelines.train_model")
result = training_pkg.train()
print("train:", result)

# Remove the training 'app' from sys.modules so we can load prediction's 'app'.
for k in list(sys.modules):
    if k == "app" or k.startswith("app."):
        del sys.modules[k]
sys.path.remove(os.path.join(ROOT, "services/training"))
sys.path.insert(0, os.path.join(ROOT, "services/prediction"))

predict_main = importlib.import_module("app.main")
from fastapi.testclient import TestClient

client = TestClient(predict_main.app)
r = client.post("/predict", json={
    "items": [
        {"train_number": "67", "service_date": date.today().isoformat()},
        {"train_number": "67", "service_date": (date.today() + timedelta(days=1)).isoformat()},
    ],
})
print("status:", r.status_code)
print(r.json())
