"""Unified storage layer.

Uses Snowflake when USE_SNOWFLAKE=true, otherwise a local SQLite file so the
MVP is runnable offline. The two backends expose the same methods used by the
ingestion/training/prediction services.
"""
from __future__ import annotations

import json
import logging
import os
import sqlite3
import math
from contextlib import contextmanager
from datetime import date, datetime
from typing import Iterable

import pandas as pd

from .config import settings
from .auth0_vault import Auth0VaultError, get_tenant_secret
from .schemas import StopObservation


logger = logging.getLogger(__name__)

SF_STOP_OBSERVATIONS_RESOURCE = f"{settings.SNOWFLAKE_SCHEMA_RAW}.STOP_OBSERVATIONS"
SF_MODEL_RUNS_RESOURCE = f"{settings.SNOWFLAKE_SCHEMA_MART}.MODEL_RUNS"
SF_TS_COLUMNS = [
    "SCHEDULED_ARRIVAL",
    "ACTUAL_ARRIVAL",
    "SCHEDULED_DEPARTURE",
    "ACTUAL_DEPARTURE",
    "SCRAPED_AT",
]


SQLITE_DDL = [
    """
    CREATE TABLE IF NOT EXISTS stop_observations (
        train_number TEXT NOT NULL,
        service_date TEXT NOT NULL,
        stop_sequence INTEGER NOT NULL,
        station_code TEXT,
        scheduled_arrival TEXT,
        actual_arrival TEXT,
        scheduled_departure TEXT,
        actual_departure TEXT,
        delay_minutes REAL,
        source TEXT NOT NULL,
        scraped_at TEXT NOT NULL,
        payload TEXT,
        PRIMARY KEY (train_number, service_date, stop_sequence, source)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS job_runs (
        job_id TEXT PRIMARY KEY,
        kind TEXT,
        started_at TEXT,
        finished_at TEXT,
        status TEXT,
        row_count INTEGER,
        error TEXT
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS model_runs (
        model_id TEXT PRIMARY KEY,
        trained_at TEXT,
        algo TEXT,
        mae REAL,
        rmse REAL,
        features TEXT,
        artifact_uri TEXT,
        is_active INTEGER DEFAULT 0
    );
    """,
]


class Storage:
    def __init__(self) -> None:
        self.use_snowflake = settings.USE_SNOWFLAKE
        if self.use_snowflake:
            self._init_snowflake()
        else:
            os.makedirs(os.path.dirname(settings.LOCAL_SQLITE_PATH) or ".", exist_ok=True)
            self._init_sqlite()

    # --- init ---
    def _init_sqlite(self) -> None:
        with self._sqlite() as conn:
            for stmt in SQLITE_DDL:
                conn.execute(stmt)

    def _init_snowflake(self) -> None:  # pragma: no cover - env specific
        import snowflake.connector  # noqa

        if not settings.SNOWFLAKE_TOKEN and settings.AUTH0_SNOWFLAKE_TOKEN_URL:
            try:
                settings.SNOWFLAKE_TOKEN = get_tenant_secret("SNOWFLAKE_TOKEN")
            except Auth0VaultError:
                logger.exception("Auth0 vault enabled but failed to fetch SNOWFLAKE_TOKEN")
                raise

        auth_mode = "pat" if settings.SNOWFLAKE_TOKEN else "password"
        logger.info(
            "Snowflake mode enabled. Verifying connection (account=%s, user=%s, auth=%s)",
            settings.SNOWFLAKE_ACCOUNT,
            settings.SNOWFLAKE_USER,
            auth_mode,
        )

        try:
            with self._snowflake() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT CURRENT_ACCOUNT(), CURRENT_USER(), CURRENT_ROLE(), "
                        "CURRENT_WAREHOUSE(), CURRENT_DATABASE(), CURRENT_SCHEMA()"
                    )
                    account, user, role, warehouse, database, schema = cur.fetchone()
            logger.info(
                "Snowflake connection verified (account=%s, user=%s, role=%s, warehouse=%s, database=%s, schema=%s)",
                account,
                user,
                role,
                warehouse,
                database,
                schema,
            )
        except Exception:
            logger.exception("Snowflake connection verification failed during startup")
            raise

        # Expect DDL to already be applied via infra/snowflake/*.sql
        return

    # --- connections ---
    @contextmanager
    def _sqlite(self):
        conn = sqlite3.connect(settings.LOCAL_SQLITE_PATH)
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    @contextmanager
    def _snowflake(self):  # pragma: no cover - env specific
        import snowflake.connector

        conn_kwargs = {
            "account": settings.SNOWFLAKE_ACCOUNT,
            "warehouse": settings.SNOWFLAKE_WAREHOUSE,
            "database": settings.SNOWFLAKE_DATABASE,
            "schema": settings.SNOWFLAKE_SCHEMA_RAW,
            "session_parameters": {
                "PYTHON_CONNECTOR_QUERY_RESULT_FORMAT": "JSON",
            },
        }

        if settings.SNOWFLAKE_TOKEN:
            # Snowflake PAT is used as a password in drivers/connectors.
            conn_kwargs["user"] = settings.SNOWFLAKE_USER
            conn_kwargs["password"] = settings.SNOWFLAKE_TOKEN
            conn_kwargs["role"] = settings.SNOWFLAKE_ROLE
        else:
            conn_kwargs["user"] = settings.SNOWFLAKE_USER
            conn_kwargs["password"] = settings.SNOWFLAKE_PASSWORD
            conn_kwargs["role"] = settings.SNOWFLAKE_ROLE

        conn = snowflake.connector.connect(**conn_kwargs)
        try:
            yield conn
        finally:
            conn.close()

    # --- writes ---
    def upsert_stop_observations(self, rows: Iterable[StopObservation]) -> int:
        rows = list(rows)
        if not rows:
            return 0
        if self.use_snowflake:
            return self._upsert_sf(rows)
        return self._upsert_sqlite(rows)

    def _upsert_sqlite(self, rows: list[StopObservation]) -> int:
        with self._sqlite() as conn:
            cur = conn.cursor()
            for r in rows:
                cur.execute(
                    """
                    INSERT OR REPLACE INTO stop_observations (
                      train_number, service_date, stop_sequence, station_code,
                      scheduled_arrival, actual_arrival, scheduled_departure,
                      actual_departure, delay_minutes, source, scraped_at, payload
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        r.train_number,
                        r.service_date.isoformat(),
                        r.stop_sequence,
                        r.station_code,
                        r.scheduled_arrival.isoformat() if r.scheduled_arrival else None,
                        r.actual_arrival.isoformat() if r.actual_arrival else None,
                        r.scheduled_departure.isoformat() if r.scheduled_departure else None,
                        r.actual_departure.isoformat() if r.actual_departure else None,
                        r.delay_minutes,
                        r.source,
                        r.scraped_at.isoformat(),
                        None,
                    ),
                )
        return len(rows)

    def _upsert_sf(self, rows: list[StopObservation]) -> int:  # pragma: no cover
        from snowflake.connector.pandas_tools import write_pandas

        df = pd.DataFrame([r.model_dump() for r in rows])
        # Snowflake table columns are defined as uppercase identifiers.
        df.columns = [c.upper() for c in df.columns]
        df["SERVICE_DATE"] = df["SERVICE_DATE"].astype(str)
        # Serialize timestamp columns as ISO strings to avoid nanosecond epoch
        # conversion issues with connector/Arrow paths.
        for col in SF_TS_COLUMNS:
            if col not in df.columns:
                continue
            parsed = pd.to_datetime(df[col], errors="coerce", utc=True)
            parsed = parsed.dt.tz_convert(None)
            df[col] = parsed.dt.strftime("%Y-%m-%d %H:%M:%S.%f")
            df.loc[parsed.isna(), col] = None
        logger.info(
            "Snowflake write start: resource=%s rows=%s",
            SF_STOP_OBSERVATIONS_RESOURCE,
            len(df),
        )
        with self._snowflake() as conn:
            success, nchunks, nrows, _ = write_pandas(
                conn,
                df,
                table_name="STOP_OBSERVATIONS",
                schema=settings.SNOWFLAKE_SCHEMA_RAW,
                overwrite=False,
            )
        logger.info(
            "Snowflake write done: resource=%s success=%s chunks=%s inserted_rows=%s",
            SF_STOP_OBSERVATIONS_RESOURCE,
            success,
            nchunks,
            nrows,
        )
        return len(rows)

    def record_job_run(self, job_id: str, kind: str, started_at: datetime,
                        finished_at: datetime, status: str, row_count: int,
                        error: str | None = None) -> None:
        if self.use_snowflake:
            return  # pragma: no cover
        with self._sqlite() as conn:
            conn.execute(
                """INSERT OR REPLACE INTO job_runs VALUES (?,?,?,?,?,?,?)""",
                (job_id, kind, started_at.isoformat(), finished_at.isoformat(),
                 status, row_count, error),
            )

    def list_job_runs(self, limit: int = 50):
        if self.use_snowflake:
            return []  # pragma: no cover
        with self._sqlite() as conn:
            rows = conn.execute(
                "SELECT job_id,kind,started_at,finished_at,status,row_count,error "
                "FROM job_runs ORDER BY started_at DESC LIMIT ?",
                (limit,),
            ).fetchall()
        return [
            {
                "job_id": r[0], "kind": r[1], "started_at": r[2],
                "finished_at": r[3], "status": r[4], "row_count": r[5],
                "error": r[6],
            }
            for r in rows
        ]

    def has_stop_observations(self, train_number: str, service_date: date, source: str = "transitdocs") -> bool:
        if self.use_snowflake:  # pragma: no cover - env specific
            query = (
                f"SELECT 1 FROM {settings.SNOWFLAKE_SCHEMA_RAW}.STOP_OBSERVATIONS "
                f"WHERE TRAIN_NUMBER=%s AND SERVICE_DATE=%s AND SOURCE=%s "
                f"LIMIT 1"
            )
            with self._snowflake() as conn:
                with conn.cursor() as cur:
                    cur.execute(query, (str(train_number), str(service_date), str(source)))
                    return cur.fetchone() is not None

        with self._sqlite() as conn:
            row = conn.execute(
                "SELECT 1 FROM stop_observations "
                "WHERE train_number=? AND service_date=? AND source=? "
                "LIMIT 1",
                (str(train_number), service_date.isoformat(), str(source)),
            ).fetchone()
        return row is not None

    # --- reads for training + online features ---
    def load_training_frame(self) -> pd.DataFrame:
        if self.use_snowflake:
            return self._load_training_sf()
        return self._load_training_sqlite()

    def _load_training_sqlite(self) -> pd.DataFrame:
        with self._sqlite() as conn:
            df = pd.read_sql_query(
                """
                SELECT train_number, service_date, stop_sequence, station_code,
                       scheduled_arrival, actual_arrival, delay_minutes
                FROM stop_observations
                ORDER BY train_number, service_date, stop_sequence
                """,
                conn,
            )
        return df

    def _load_training_sf(self) -> pd.DataFrame:  # pragma: no cover
        query = (
            f"SELECT "
            f"TRAIN_NUMBER AS train_number, "
            f"TO_VARCHAR(SERVICE_DATE) AS service_date, "
            f"STOP_SEQUENCE AS stop_sequence, "
            f"STATION_CODE AS station_code, "
            f"TO_VARCHAR(SCHEDULED_ARRIVAL) AS scheduled_arrival, "
            f"TO_VARCHAR(ACTUAL_ARRIVAL) AS actual_arrival, "
            f"DELAY_MINUTES AS delay_minutes "
            f"FROM {settings.SNOWFLAKE_SCHEMA_RAW}.STOP_OBSERVATIONS"
        )
        logger.info(
            "Snowflake read start: resource=%s",
            SF_STOP_OBSERVATIONS_RESOURCE,
        )
        with self._snowflake() as conn:
            df = pd.read_sql(query, conn)
        if not df.empty:
            # Be defensive: Snowflake/pandas can surface unexpected column casing depending
            # on connector + query shape.
            df.columns = [str(c).lower() for c in df.columns]
            if "scheduled_arrival" in df.columns:
                df["scheduled_arrival"] = pd.to_datetime(df["scheduled_arrival"], errors="coerce")
            if "actual_arrival" in df.columns:
                df["actual_arrival"] = pd.to_datetime(df["actual_arrival"], errors="coerce")
        logger.info(
            "Snowflake read done: resource=%s rows=%s",
            SF_STOP_OBSERVATIONS_RESOURCE,
            len(df),
        )
        return df

    def recent_delay_stats(self, train_number: str, up_to: date) -> dict:
        """Rolling stats used as online features for prediction."""
        if self.use_snowflake:  # pragma: no cover - env specific
            query = (
                f"SELECT TO_VARCHAR(SERVICE_DATE) AS service_date, MAX(DELAY_MINUTES) AS final_delay "
                f"FROM {settings.SNOWFLAKE_SCHEMA_RAW}.STOP_OBSERVATIONS "
                f"WHERE TRAIN_NUMBER = %s AND SERVICE_DATE < %s "
                f"GROUP BY SERVICE_DATE "
                f"ORDER BY SERVICE_DATE DESC "
                f"LIMIT 30"
            )
            with self._snowflake() as conn:
                df = pd.read_sql(query, conn, params=(str(train_number), str(up_to)))
            if df.empty:
                return {"avg_delay_l30d": None, "avg_delay_l30d_dow": None, "n": 0}
            df.columns = [str(c).lower() for c in df.columns]
            df["service_date"] = pd.to_datetime(df["service_date"], errors="coerce")
            dow = pd.Timestamp(up_to).dayofweek
            dow_mask = df["service_date"].dt.dayofweek == dow
            return {
                "avg_delay_l30d": float(df["final_delay"].mean()),
                "avg_delay_l30d_dow": float(df.loc[dow_mask, "final_delay"].mean()) if dow_mask.any() else None,
                "n": int(len(df)),
            }
        with self._sqlite() as conn:
            df = pd.read_sql_query(
                """
                SELECT service_date, MAX(delay_minutes) AS final_delay
                FROM stop_observations
                WHERE train_number = ? AND service_date < ?
                GROUP BY service_date
                ORDER BY service_date DESC
                LIMIT 30
                """,
                conn,
                params=(train_number, up_to.isoformat()),
            )
        if df.empty:
            return {"avg_delay_l30d": None, "avg_delay_l30d_dow": None, "n": 0}
        df["service_date"] = pd.to_datetime(df["service_date"])
        dow = pd.Timestamp(up_to).dayofweek
        dow_mask = df["service_date"].dt.dayofweek == dow
        return {
            "avg_delay_l30d": float(df["final_delay"].mean()),
            "avg_delay_l30d_dow": float(df.loc[dow_mask, "final_delay"].mean()) if dow_mask.any() else None,
            "n": int(len(df)),
        }

    def recent_delays(self, train_number: str, up_to: date, limit: int = 5) -> list[dict]:
        """Return most recent per-day final delays strictly before `up_to`."""
        if self.use_snowflake:  # pragma: no cover - env specific
            query = (
                f"SELECT TO_VARCHAR(SERVICE_DATE) AS service_date, MAX(DELAY_MINUTES) AS final_delay "
                f"FROM {settings.SNOWFLAKE_SCHEMA_RAW}.STOP_OBSERVATIONS "
                f"WHERE TRAIN_NUMBER=%s AND SERVICE_DATE < %s "
                f"GROUP BY SERVICE_DATE "
                f"ORDER BY SERVICE_DATE DESC "
                f"LIMIT {int(limit)}"
            )
            with self._snowflake() as conn:
                with conn.cursor() as cur:
                    cur.execute(query, (str(train_number), str(up_to)))
                    rows = cur.fetchall() or []
            return [{"service_date": r[0], "final_delay_min": float(r[1]) if r[1] is not None else None} for r in rows]

        with self._sqlite() as conn:
            rows = conn.execute(
                """
                SELECT service_date, MAX(delay_minutes) AS final_delay
                FROM stop_observations
                WHERE train_number = ? AND service_date < ?
                GROUP BY service_date
                ORDER BY service_date DESC
                LIMIT ?
                """,
                (str(train_number), up_to.isoformat(), int(limit)),
            ).fetchall()
        return [{"service_date": r[0], "final_delay_min": float(r[1]) if r[1] is not None else None} for r in rows]

    def final_delay_history(self, train_number: str, up_to: date, lookback_days: int = 365) -> pd.DataFrame:
        """Return per-day final delay history for a train for the lookback window strictly before up_to."""
        if self.use_snowflake:  # pragma: no cover - env specific
            query = (
                f"SELECT TO_VARCHAR(SERVICE_DATE) AS service_date, MAX(DELAY_MINUTES) AS final_delay "
                f"FROM {settings.SNOWFLAKE_SCHEMA_RAW}.STOP_OBSERVATIONS "
                f"WHERE TRAIN_NUMBER = %s "
                f"AND SERVICE_DATE < %s "
                f"AND SERVICE_DATE >= DATEADD(day, -{int(lookback_days)}, %s) "
                f"GROUP BY SERVICE_DATE "
                f"ORDER BY SERVICE_DATE DESC"
            )
            with self._snowflake() as conn:
                df = pd.read_sql(query, conn, params=(str(train_number), str(up_to), str(up_to)))
            if df.empty:
                return pd.DataFrame(columns=["service_date", "final_delay"])
            df.columns = [str(c).lower() for c in df.columns]
            df["service_date"] = pd.to_datetime(df["service_date"], errors="coerce").dt.date
            df["final_delay"] = pd.to_numeric(df["final_delay"], errors="coerce")
            return df.dropna(subset=["service_date", "final_delay"])

        with self._sqlite() as conn:
            df = pd.read_sql_query(
                """
                SELECT service_date, MAX(delay_minutes) AS final_delay
                FROM stop_observations
                WHERE train_number = ? AND service_date < ?
                GROUP BY service_date
                ORDER BY service_date DESC
                """,
                conn,
                params=(str(train_number), up_to.isoformat()),
            )
        if df.empty:
            return pd.DataFrame(columns=["service_date", "final_delay"])
        df["service_date"] = pd.to_datetime(df["service_date"], errors="coerce").dt.date
        df["final_delay"] = pd.to_numeric(df["final_delay"], errors="coerce")
        return df.dropna(subset=["service_date", "final_delay"])

    # --- model registry ---
    def register_model(self, model_id: str, algo: str, mae: float, rmse: float,
                        features: list[str], artifact_uri: str,
                        activate: bool = True) -> None:
        trained_at = datetime.utcnow().isoformat()
        if self.use_snowflake:  # pragma: no cover - env specific
            logger.info("Snowflake model register start: resource=%s model_id=%s", SF_MODEL_RUNS_RESOURCE, model_id)
            with self._snowflake() as conn:
                with conn.cursor() as cur:
                    if activate:
                        cur.execute(
                            f"UPDATE {settings.SNOWFLAKE_SCHEMA_MART}.MODEL_RUNS SET IS_ACTIVE = FALSE WHERE IS_ACTIVE = TRUE"
                        )
                    cur.execute(
                        f"""
                        INSERT INTO {settings.SNOWFLAKE_SCHEMA_MART}.MODEL_RUNS
                          (MODEL_ID, TRAINED_AT, ALGO, MAE, RMSE, FEATURES, ARTIFACT_URI, IS_ACTIVE)
                        SELECT %s, TO_TIMESTAMP_NTZ(%s), %s, %s, %s, PARSE_JSON(%s), %s, %s
                        """,
                        (
                            model_id,
                            trained_at,
                            algo,
                            mae,
                            rmse,
                            json.dumps(features),
                            artifact_uri,
                            bool(activate),
                        ),
                    )
            logger.info("Snowflake model register done: resource=%s model_id=%s", SF_MODEL_RUNS_RESOURCE, model_id)
            return

        with self._sqlite() as conn:
            if activate:
                conn.execute("UPDATE model_runs SET is_active = 0")
            conn.execute(
                """INSERT OR REPLACE INTO model_runs VALUES (?,?,?,?,?,?,?,?)""",
                (model_id, trained_at, algo, mae, rmse, json.dumps(features), artifact_uri, 1 if activate else 0),
            )

    def active_model(self) -> dict | None:
        if self.use_snowflake:  # pragma: no cover - env specific
            query = (
                f"SELECT MODEL_ID, ALGO, MAE, RMSE, FEATURES, ARTIFACT_URI "
                f"FROM {settings.SNOWFLAKE_SCHEMA_MART}.MODEL_RUNS "
                f"WHERE IS_ACTIVE = TRUE "
                f"ORDER BY TRAINED_AT DESC LIMIT 1"
            )
            logger.info("Snowflake latest active model query: %s", query)
            with self._snowflake() as conn:
                with conn.cursor() as cur:
                    cur.execute(query)
                    row = cur.fetchone()
            if not row:
                logger.warning("Snowflake active_model: no rows returned")
                return None
            return {
                "model_id": row[0],
                "algo": row[1],
                "mae": row[2],
                "rmse": row[3],
                "features": json.loads(row[4]) if isinstance(row[4], str) else row[4],
                "artifact_uri": row[5],
            }
        with self._sqlite() as conn:
            row = conn.execute(
                "SELECT model_id, algo, mae, rmse, features, artifact_uri "
                "FROM model_runs WHERE is_active = 1 ORDER BY trained_at DESC LIMIT 1"
            ).fetchone()
        if not row:
            return None
        return {
            "model_id": row[0], "algo": row[1], "mae": row[2], "rmse": row[3],
            "features": json.loads(row[4]), "artifact_uri": row[5],
        }

    def latest_model(self) -> dict | None:
        """Return the most recently trained model (regardless of IS_ACTIVE)."""
        if self.use_snowflake:  # pragma: no cover - env specific
            query = (
                f"SELECT MODEL_ID, ALGO, MAE, RMSE, FEATURES, ARTIFACT_URI "
                f"FROM {settings.SNOWFLAKE_SCHEMA_MART}.MODEL_RUNS "
                f"ORDER BY TRAINED_AT DESC LIMIT 1"
            )
            logger.info("Snowflake latest model query: %s", query)
            with self._snowflake() as conn:
                with conn.cursor() as cur:
                    cur.execute(query)
                    row = cur.fetchone()
            if not row:
                logger.warning("Snowflake latest_model: no rows returned")
                return None
            return {
                "model_id": row[0],
                "algo": row[1],
                "mae": row[2],
                "rmse": row[3],
                "features": json.loads(row[4]) if isinstance(row[4], str) else row[4],
                "artifact_uri": row[5],
            }
        with self._sqlite() as conn:
            row = conn.execute(
                "SELECT model_id, algo, mae, rmse, features, artifact_uri "
                "FROM model_runs ORDER BY trained_at DESC LIMIT 1"
            ).fetchone()
        if not row:
            return None
        return {
            "model_id": row[0], "algo": row[1], "mae": row[2], "rmse": row[3],
            "features": json.loads(row[4]), "artifact_uri": row[5],
        }

    def clear_stop_observations(self) -> int:
        """Remove all stop observations from the active storage backend."""
        if self.use_snowflake:
            logger.info("Snowflake clear start: resource=%s", SF_STOP_OBSERVATIONS_RESOURCE)
            with self._snowflake() as conn:
                with conn.cursor() as cur:
                    cur.execute(f"TRUNCATE TABLE {settings.SNOWFLAKE_SCHEMA_RAW}.STOP_OBSERVATIONS")
            logger.info("Snowflake clear done: resource=%s", SF_STOP_OBSERVATIONS_RESOURCE)
            return 0

        with self._sqlite() as conn:
            cur = conn.cursor()
            cur.execute("DELETE FROM stop_observations")
            deleted = cur.rowcount if cur.rowcount is not None else 0
        logger.info("SQLite clear done: table=stop_observations deleted_rows=%s", deleted)
        return int(deleted)

    def clear_model_registry(self) -> int:
        """Clear the local model registry and remove saved artifacts."""
        removed_files = 0

        if os.path.isdir(settings.MODEL_DIR):
            for name in os.listdir(settings.MODEL_DIR):
                if not name.endswith(".joblib"):
                    continue
                path = os.path.join(settings.MODEL_DIR, name)
                try:
                    os.remove(path)
                    removed_files += 1
                except FileNotFoundError:
                    continue

        if self.use_snowflake:
            logger.info("Snowflake model registry clear skipped (registry is local-only); removed_files=%s", removed_files)
            return removed_files

        with self._sqlite() as conn:
            conn.execute("DELETE FROM model_runs")
        logger.info("SQLite model registry cleared; removed_files=%s", removed_files)
        return removed_files

    def clear_all_training_state(self) -> dict[str, int]:
        """Convenience reset for raw data and model artifacts."""
        raw_deleted = self.clear_stop_observations()
        model_files_removed = self.clear_model_registry()
        return {
            "raw_deleted": raw_deleted,
            "model_files_removed": model_files_removed,
        }

    def dump_stop_observations(self) -> list[dict]:
        """Dump all stop observations from the active backend."""
        if self.use_snowflake:  # pragma: no cover - env specific
            # Avoid Snowflake connector trying to convert extreme/invalid timestamps into
            # Python datetimes (can raise OSError: value too large). We cast timestamp
            # fields to VARCHAR so we always get raw strings back.
            query = (
                f"SELECT "
                f"TRAIN_NUMBER, SERVICE_DATE, STOP_SEQUENCE, STATION_CODE, "
                f"TO_VARCHAR(SCHEDULED_ARRIVAL) AS SCHEDULED_ARRIVAL, "
                f"TO_VARCHAR(ACTUAL_ARRIVAL) AS ACTUAL_ARRIVAL, "
                f"TO_VARCHAR(SCHEDULED_DEPARTURE) AS SCHEDULED_DEPARTURE, "
                f"TO_VARCHAR(ACTUAL_DEPARTURE) AS ACTUAL_DEPARTURE, "
                f"DELAY_MINUTES, SOURCE, "
                f"TO_VARCHAR(SCRAPED_AT) AS SCRAPED_AT, "
                f"PAYLOAD "
                f"FROM {settings.SNOWFLAKE_SCHEMA_RAW}.STOP_OBSERVATIONS"
            )
            with self._snowflake() as conn:
                df = pd.read_sql(query, conn)
            # Normalize column casing for consistent JSON keys.
            df.columns = [str(c).lower() for c in df.columns]
            # Normalize NaNs to None for JSON serialization.
            return df.where(pd.notnull(df), None).to_dict(orient="records")

        with self._sqlite() as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute("SELECT * FROM stop_observations").fetchall()
        def _sanitize(v):
            if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
                return None
            return v

        cleaned: list[dict] = []
        for r in rows:
            d = dict(r)
            cleaned.append({k: _sanitize(v) for k, v in d.items()})
        return cleaned

    def delete_models(self) -> dict[str, int]:
        """Delete local model artifacts and clear registries (sqlite + snowflake if present)."""
        removed_files = self.clear_model_registry()
        snowflake_cleared = 0
        if self.use_snowflake:  # pragma: no cover - env specific
            # No Snowflake model registry is defined in this repo today; leave hook for future tables.
            snowflake_cleared = 0
        return {"model_files_removed": int(removed_files), "snowflake_cleared": int(snowflake_cleared)}


storage = Storage()
