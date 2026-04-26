from __future__ import annotations

import os
import uuid
from datetime import datetime

import joblib
import numpy as np
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error

from via_common.config import settings
from via_common.features import FEATURE_COLUMNS, build_training_frame
from via_common.storage import storage


def _fallback_frame():
    """If we have no data yet (MVP cold start), synthesize a tiny dataset so
    the service is operable end-to-end. Values are intentionally modest."""
    import pandas as pd
    rng = np.random.default_rng(42)
    rows = []
    for t in ["67", "68", "55"]:
        for _ in range(60):
            dow = int(rng.integers(0, 7))
            month = int(rng.integers(1, 13))
            base = 6 + (2 if dow in (0, 4) else 0) + (3 if month in (12, 1, 2) else 0)
            rows.append({
                "dow": dow, "month": month, "hour": int(rng.integers(6, 20)),
                "is_weekend": int(dow >= 5),
                "train_number_hash": abs(hash(t)) % 1000,
                "avg_delay_l30d": float(rng.normal(base, 3)),
                "avg_delay_l30d_dow": float(rng.normal(base, 4)),
                "target_delay_min": float(max(0, rng.normal(base, 5))),
            })
    return pd.DataFrame(rows)


def _ensure_sf_stage() -> str:  # pragma: no cover - env specific
    stage = settings.SNOWFLAKE_MODEL_STAGE
    with storage._snowflake() as conn:
        with conn.cursor() as cur:
            cur.execute(f"CREATE STAGE IF NOT EXISTS {stage}")
    return stage


def _upload_bundle_to_stage(bundle: dict, model_id: str) -> str:  # pragma: no cover - env specific
    stage = _ensure_sf_stage()
    # Write to a temp location, then PUT to stage.
    tmp_dir = os.path.join("/tmp", "via_models")
    os.makedirs(tmp_dir, exist_ok=True)
    local_path = os.path.join(tmp_dir, f"{model_id}.joblib")
    joblib.dump(bundle, local_path)

    with storage._snowflake() as conn:
        with conn.cursor() as cur:
            cur.execute(f"PUT file://{local_path} @{stage} AUTO_COMPRESS=FALSE OVERWRITE=TRUE")
    return f"@{stage}/{model_id}.joblib"


def train(algo: str = "gbr") -> dict:
    obs = storage.load_training_frame()
    frame = build_training_frame(obs)
    if len(frame) < 20:
        frame = _fallback_frame()

    X = frame[FEATURE_COLUMNS].values
    y = frame["target_delay_min"].values

    split = max(1, int(len(X) * 0.8))
    X_train, X_test = X[:split], X[split:]
    y_train, y_test = y[:split], y[split:]

    model = GradientBoostingRegressor(
        n_estimators=200, max_depth=3, learning_rate=0.05, random_state=7,
    )
    model.fit(X_train, y_train)

    if len(X_test):
        preds = model.predict(X_test)
        mae = float(mean_absolute_error(y_test, preds))
        rmse = float(np.sqrt(mean_squared_error(y_test, preds)))
    else:
        mae, rmse = 0.0, 0.0

    model_id = f"m_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"
    bundle = {
        "model": model,
        "features": FEATURE_COLUMNS,
        "trained_at": datetime.utcnow().isoformat(),
        "model_id": model_id,
    }

    if settings.USE_SNOWFLAKE:
        artifact_uri = _upload_bundle_to_stage(bundle, model_id)
    else:
        os.makedirs(settings.MODEL_DIR, exist_ok=True)
        artifact_uri = os.path.join(settings.MODEL_DIR, f"{model_id}.joblib")
        joblib.dump(bundle, artifact_uri)
        active_path = os.path.join(settings.MODEL_DIR, settings.ACTIVE_MODEL_FILE)
        joblib.dump(joblib.load(artifact_uri), active_path)

    storage.register_model(model_id, algo, mae, rmse, FEATURE_COLUMNS, artifact_uri, activate=True)

    return {"model_id": model_id, "mae": mae, "rmse": rmse, "rows": int(len(frame)), "artifact_uri": artifact_uri}
