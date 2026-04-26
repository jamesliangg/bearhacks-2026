from __future__ import annotations

from typing import List

import numpy as np
from fastapi import APIRouter
from pydantic import BaseModel

from app.model_loader import loader
from via_common.features import FEATURE_COLUMNS, build_online_features
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

    recent_trend_effect = float(feature_row["avg_delay_l30d"]) - 8.0
    route_effect = p50 - (weekday_effect + weather_effect + recent_trend_effect)

    return PredictionFactors(
        weekday_effect=round(weekday_effect, 2),
        weather_effect=round(weather_effect, 2),
        recent_trend_effect=round(recent_trend_effect, 2),
        route_effect=round(route_effect, 2),
    )


@router.post("/predict", response_model=PredictResponse)
def predict(req: PredictRequest) -> PredictResponse:
    bundle = loader.get()
    model = bundle["model"] if bundle else None
    model_id = bundle["model_id"] if bundle else "heuristic"

    out: list[PredictionResult] = []
    for item in req.items:
        stats = storage.recent_delay_stats(item.train_number, item.service_date)
        features = build_online_features(
            item.train_number, item.service_date, item.scheduled_departure, stats,
        )
        x = np.array([[features[c] for c in FEATURE_COLUMNS]])
        if model is not None:
            p50 = float(model.predict(x)[0])
        else:
            # Heuristic fallback when no model has been trained yet.
            p50 = float(features["avg_delay_l30d"] or 8.0)

        p50 = max(0.0, p50)
        p90 = p50 * 1.8 + 5.0  # simple upper envelope

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


@router.post("/admin/reload")
def reload_model():
    b = loader.reload()
    return {"loaded": bool(b), "model_id": b.get("model_id") if b else None}
