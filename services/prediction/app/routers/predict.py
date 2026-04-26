from __future__ import annotations

from typing import List

import numpy as np
from fastapi import APIRouter
from pydantic import BaseModel

from via_common.features import build_online_features
from via_common.schemas import (
    PredictionFactors,
    PredictionRequestItem,
    PredictionResult,
    severity_for,
)
from via_common.storage import storage

router = APIRouter()


class PredictRequest(BaseModel):
    items: List[PredictionRequestItem]


class PredictResponse(BaseModel):
    predictions: List[PredictionResult]

class RecentDelaysResponse(BaseModel):
    train_number: str
    service_date: str
    recent: list[dict]


def _factor_breakdown(feature_row: dict, p50: float) -> PredictionFactors:
    """Simple, explainable decomposition. These are not SHAP values — they are
    heuristic contributions inferred from feature deviations so the UI can
    display *why* a prediction is what it is.
    """
    weekday_effect = 0.0
    if feature_row["is_weekend"]:
        weekday_effect -= 1.5
    if feature_row["dow"] in (0, 4):  # Mon, Fri
        weekday_effect += 2.0

    weather_effect = 0.0
    if feature_row["month"] in (12, 1, 2):
        weather_effect += 3.0
    elif feature_row["month"] in (6, 7, 8):
        weather_effect += 1.0

    # Recent trend is entirely based on recent final delays for this train.
    recent_trend_effect = float(feature_row["avg_delay_l30d"])
    route_effect = p50 - (weekday_effect + weather_effect + recent_trend_effect)

    return PredictionFactors(
        weekday_effect=round(weekday_effect, 2),
        weather_effect=round(weather_effect, 2),
        recent_trend_effect=round(recent_trend_effect, 2),
        route_effect=round(route_effect, 2),
    )


@router.post("/predict", response_model=PredictResponse)
def predict(req: PredictRequest) -> PredictResponse:
    model_id = "weighted_stats"

    out: list[PredictionResult] = []
    for item in req.items:
        history = storage.final_delay_history(item.train_number, item.service_date, lookback_days=365)
        delays = history["final_delay"].to_numpy(dtype=float) if not history.empty else np.array([])
        if delays.size == 0:
            p50 = 0.0
            p90 = 5.0
            avg = 0.0
        else:
            # Recency weighting: last week > last month > rest of year.
            svc = item.service_date
            days_ago = np.array([(svc - d).days for d in history["service_date"]], dtype=float)
            w = np.where(days_ago <= 7, 3.0, np.where(days_ago <= 30, 1.5, 1.0))
            w = np.maximum(w, 0.0)
            w = w / w.sum()
            # Weighted average
            avg = float(np.sum(w * delays))
            # Approx weighted quantiles by sorting and cumulative weights
            idx = np.argsort(delays)
            d_sorted = delays[idx]
            cw = np.cumsum(w[idx])
            p50 = float(d_sorted[np.searchsorted(cw, 0.5, side="left")])
            p90 = float(d_sorted[np.searchsorted(cw, 0.9, side="left")])

        stats = {"avg_delay_l30d": avg}
        features = build_online_features(item.train_number, item.service_date, item.scheduled_departure, stats)

        out.append(PredictionResult(
            train_number=item.train_number,
            service_date=item.service_date,
            p50_delay_min=round(p50, 1),
            p90_delay_min=round(p90, 1),
            severity=severity_for(p50),
            factors=_factor_breakdown(features, p50),
            model_id=model_id,
        ))
    return PredictResponse(predictions=out)

@router.get("/recent/{train_number}/{service_date}", response_model=RecentDelaysResponse)
def recent(train_number: str, service_date: str) -> RecentDelaysResponse:
    from datetime import date as _date

    d = _date.fromisoformat(service_date)
    recent_rows = storage.recent_delays(train_number, d, limit=5)
    return RecentDelaysResponse(train_number=str(train_number), service_date=service_date, recent=recent_rows)


@router.post("/admin/reload")
def reload_model():
    b = loader.reload()
    return {"loaded": bool(b), "model_id": b.get("model_id") if b else None}
