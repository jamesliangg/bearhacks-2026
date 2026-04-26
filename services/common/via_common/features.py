"""Feature engineering shared between training and online inference."""
from __future__ import annotations

from datetime import date, datetime
from typing import Optional

import pandas as pd


FEATURE_COLUMNS = [
    "dow",
    "month",
    "hour",
    "is_weekend",
    "train_number_hash",
    "avg_delay_l30d",
    "avg_delay_l30d_dow",
]


def _train_hash(train_number: str) -> int:
    return abs(hash(train_number)) % 1000


def build_online_features(
    train_number: str,
    service_date: date,
    scheduled_departure: Optional[datetime],
    stats: dict,
) -> dict:
    dt = pd.Timestamp(service_date)
    hour = scheduled_departure.hour if scheduled_departure else 8
    return {
        "dow": int(dt.dayofweek),
        "month": int(dt.month),
        "hour": int(hour),
        "is_weekend": int(dt.dayofweek >= 5),
        "train_number_hash": _train_hash(train_number),
        "avg_delay_l30d": stats.get("avg_delay_l30d") or 0.0,
        "avg_delay_l30d_dow": stats.get("avg_delay_l30d_dow") or stats.get("avg_delay_l30d") or 0.0,
    }


def build_training_frame(obs: pd.DataFrame) -> pd.DataFrame:
    """Collapse stop observations into one row per (train, service_date) with
    final delay as target and engineered features."""
    if obs.empty:
        return pd.DataFrame(columns=FEATURE_COLUMNS + ["target_delay_min"])

    obs = obs.copy()
    obs["service_date"] = pd.to_datetime(obs["service_date"])

    runs = (
        obs.groupby(["train_number", "service_date"], as_index=False)
        .agg(target_delay_min=("delay_minutes", "max"))
        .dropna(subset=["target_delay_min"])
        .sort_values(["train_number", "service_date"])
    )

    runs["dow"] = runs["service_date"].dt.dayofweek
    runs["month"] = runs["service_date"].dt.month
    runs["hour"] = 8  # placeholder; scheduled departure hour not always available
    runs["is_weekend"] = (runs["dow"] >= 5).astype(int)
    runs["train_number_hash"] = runs["train_number"].map(_train_hash)

    # Rolling 30-day mean for this train, excluding current row.
    runs["avg_delay_l30d"] = (
        runs.groupby("train_number")["target_delay_min"]
        .transform(lambda s: s.shift(1).rolling(30, min_periods=1).mean())
        .fillna(0.0)
    )

    # Rolling per-DOW mean.
    runs["avg_delay_l30d_dow"] = (
        runs.groupby(["train_number", "dow"])["target_delay_min"]
        .transform(lambda s: s.shift(1).rolling(30, min_periods=1).mean())
        .fillna(runs["avg_delay_l30d"])
    )

    return runs[FEATURE_COLUMNS + ["target_delay_min"]]
