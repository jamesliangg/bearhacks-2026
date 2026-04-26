from __future__ import annotations

import json
import uuid
from datetime import datetime

import joblib
import numpy as np
import pandas as pd

# Snowflake Notebook (Python) cell.
# Assumes a Snowpark `session` variable is available in the notebook runtime.


def train_delay_model(session, algo: str = "ridge_numpy") -> str:
    obs = session.sql(
        """
        SELECT TRAIN_NUMBER, SERVICE_DATE, STOP_SEQUENCE, STATION_CODE,
               SCHEDULED_ARRIVAL, ACTUAL_ARRIVAL, DELAY_MINUTES
        FROM RAW.STOP_OBSERVATIONS
        """
    ).to_pandas()

    if obs.empty:
        raise ValueError("No STOP_OBSERVATIONS available for training")

    obs["SERVICE_DATE"] = pd.to_datetime(obs["SERVICE_DATE"])
    obs["SCHEDULED_ARRIVAL"] = pd.to_datetime(obs["SCHEDULED_ARRIVAL"], errors="coerce")

    per_day = (
        obs.groupby(["TRAIN_NUMBER", "SERVICE_DATE"], as_index=False)
        .agg(
            final_delay=("DELAY_MINUTES", "max"),
            hour=(
                "SCHEDULED_ARRIVAL",
                lambda s: int(pd.to_datetime(s, errors="coerce").dt.hour.dropna().iloc[0])
                if s.notna().any()
                else 12,
            ),
        )
        .sort_values(["TRAIN_NUMBER", "SERVICE_DATE"])
    )

    per_day["dow"] = per_day["SERVICE_DATE"].dt.dayofweek.astype(int)
    per_day["month"] = per_day["SERVICE_DATE"].dt.month.astype(int)
    per_day["is_weekend"] = (per_day["dow"] >= 5).astype(int)
    per_day["train_number_hash"] = per_day["TRAIN_NUMBER"].apply(lambda x: abs(hash(str(x))) % 1000).astype(int)

    per_day["avg_delay_l30d"] = (
        per_day.groupby("TRAIN_NUMBER")["final_delay"]
        .apply(lambda s: s.shift(1).rolling(30, min_periods=1).mean())
        .reset_index(level=0, drop=True)
    )

    def _dow_roll(g: pd.DataFrame) -> pd.Series:
        out = []
        for i in range(len(g)):
            cur_dow = int(g.iloc[i]["dow"])
            prev = g.iloc[max(0, i - 30):i]
            prev = prev[prev["dow"] == cur_dow]
            out.append(float(prev["final_delay"].mean()) if len(prev) else float("nan"))
        return pd.Series(out, index=g.index)

    per_day["avg_delay_l30d_dow"] = per_day.groupby("TRAIN_NUMBER", group_keys=False).apply(_dow_roll)
    per_day["avg_delay_l30d_dow"] = per_day["avg_delay_l30d_dow"].fillna(per_day["avg_delay_l30d"])

    frame = per_day.rename(columns={"final_delay": "target_delay_min"}).dropna(subset=["target_delay_min"])

    feature_cols = [
        "dow",
        "month",
        "hour",
        "is_weekend",
        "train_number_hash",
        "avg_delay_l30d",
        "avg_delay_l30d_dow",
    ]

    if len(frame) < 20:
        raise ValueError("Not enough training rows (need >= 20)")

    X = frame[feature_cols].astype(float).values
    y = frame["target_delay_min"].astype(float).values
    X = np.hstack([np.ones((len(X), 1)), X])

    split = max(1, int(len(X) * 0.8))
    X_train, X_test = X[:split], X[split:]
    y_train, y_test = y[:split], y[split:]

    lam = 1.0
    beta = np.linalg.solve(X_train.T @ X_train + lam * np.eye(X_train.shape[1]), X_train.T @ y_train)

    def _predict(m):
        return (m @ beta).astype(float)

    if len(X_test):
        preds = _predict(X_test)
        mae = float(np.mean(np.abs(y_test - preds)))
        rmse = float(np.sqrt(np.mean((y_test - preds) ** 2)))
    else:
        mae, rmse = 0.0, 0.0

    model_id = f"m_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"
    bundle = {
        "model": {"kind": "ridge_numpy", "beta": beta.tolist()},
        "features": feature_cols,
        "trained_at": datetime.utcnow().isoformat(),
        "model_id": model_id,
    }

    tmp_path = f"/tmp/{model_id}.joblib"
    joblib.dump(bundle, tmp_path)

    session.sql(f"PUT file://{tmp_path} @MART.MODEL_STAGE AUTO_COMPRESS=FALSE OVERWRITE=TRUE").collect()

    session.sql("UPDATE MART.MODEL_RUNS SET IS_ACTIVE = FALSE WHERE IS_ACTIVE = TRUE").collect()
    session.sql(
        """
        INSERT INTO MART.MODEL_RUNS (MODEL_ID, TRAINED_AT, ALGO, MAE, RMSE, FEATURES, ARTIFACT_URI, IS_ACTIVE)
        SELECT ?, TO_TIMESTAMP_NTZ(?), ?, ?, ?, PARSE_JSON(?), ?, TRUE
        """,
        params=[
            model_id,
            datetime.utcnow().isoformat(),
            algo,
            mae,
            rmse,
            json.dumps(feature_cols),
            f"@MART.MODEL_STAGE/{model_id}.joblib",
        ],
    ).collect()

    return model_id


# Example usage in the notebook:
# model_id = train_delay_model(session)
# print("trained model_id:", model_id)
