from __future__ import annotations

import logging
import os
import threading
import time
from typing import Any

import joblib

from via_common.config import settings
from via_common.storage import storage

logger = logging.getLogger(__name__)


class ModelLoader:
    """Lazy-loads the active model artifact with a TTL refresh."""

    def __init__(self, ttl_seconds: int = 600) -> None:
        self._lock = threading.Lock()
        self._ttl = ttl_seconds
        self._bundle: dict[str, Any] | None = None
        self._loaded_at: float = 0.0
        self._loaded_mtime: float = 0.0
        self._loaded_model_id: str | None = None

    def _path(self) -> str:
        return os.path.join(settings.MODEL_DIR, settings.ACTIVE_MODEL_FILE)

    def _maybe_refresh_from_snowflake(self) -> None:
        if not settings.USE_SNOWFLAKE:
            logger.info("model_loader: snowflake disabled (USE_SNOWFLAKE=false); skipping remote refresh")
            return
        # Prefer "latest trained" so prediction automatically rolls forward even
        # if IS_ACTIVE wasn't updated (or multiple trainers ran concurrently).
        logger.info("model_loader: checking Snowflake for latest model (MODEL_RUNS)")
        meta = storage.latest_model()
        if meta:
            logger.info(
                "model_loader: latest_model() -> model_id=%s artifact_uri=%s",
                meta.get("model_id"),
                meta.get("artifact_uri"),
            )
        else:
            logger.warning("model_loader: latest_model() -> None; falling back to active_model()")
            meta = storage.active_model()
            logger.info(
                "model_loader: active_model() -> %s",
                {"model_id": meta.get("model_id"), "artifact_uri": meta.get("artifact_uri")} if meta else None,
            )
        if not meta:
            logger.warning("model_loader: no model metadata found in Snowflake; skipping remote refresh")
            return
        model_id = meta.get("model_id")
        artifact_uri = meta.get("artifact_uri")
        if not model_id or not artifact_uri:
            logger.warning(
                "model_loader: missing model_id or artifact_uri from Snowflake meta; model_id=%s artifact_uri=%s",
                model_id,
                artifact_uri,
            )
            return
        if self._loaded_model_id == model_id and os.path.exists(self._path()):
            logger.info("model_loader: model already loaded and local active file exists; model_id=%s", model_id)
            return

        os.makedirs(settings.MODEL_DIR, exist_ok=True)
        local_path = self._path()

        stage_ref = artifact_uri
        # Expect artifact_uri like "@MODEL_STAGE/<model_id>.joblib"
        logger.info("model_loader: downloading model via Snowflake GET: %s -> %s", stage_ref, settings.MODEL_DIR)
        with storage._snowflake() as conn:
            with conn.cursor() as cur:
                cur.execute(f"GET {stage_ref} file://{settings.MODEL_DIR} OVERWRITE=TRUE")
                try:
                    rows = cur.fetchall()
                    logger.info("model_loader: Snowflake GET returned %s rows", len(rows))
                    if rows:
                        logger.info("model_loader: Snowflake GET first row: %s", rows[0])
                except Exception:
                    logger.exception("model_loader: failed reading Snowflake GET results")

        # Always ensure the downloaded <model_id>.joblib is what becomes the active file.
        found_path: str | None = None
        for root, _, files in os.walk(settings.MODEL_DIR):
            for f in files:
                if f == f"{model_id}.joblib":
                    found_path = os.path.join(root, f)
                    break
            if found_path:
                break

        if not found_path:
            logger.warning(
                "model_loader: Snowflake GET completed but did not find expected artifact %s.joblib under %s",
                model_id,
                settings.MODEL_DIR,
            )
            return

        if os.path.abspath(found_path) != os.path.abspath(local_path):
            logger.info("model_loader: promoting downloaded artifact to active file: %s -> %s", found_path, local_path)
            os.replace(found_path, local_path)
        else:
            logger.info("model_loader: downloaded artifact already at active path: %s", local_path)
        logger.info("model_loader: remote refresh complete; active_path=%s exists=%s", local_path, os.path.exists(local_path))
        self._loaded_model_id = model_id

    def get(self) -> dict[str, Any] | None:
        now = time.time()
        with self._lock:
            logger.info(
                "model_loader: get() start (MODEL_DIR=%s ACTIVE_MODEL_FILE=%s ACTIVE_MODEL_ID=%s)",
                settings.MODEL_DIR,
                settings.ACTIVE_MODEL_FILE,
                settings.ACTIVE_MODEL_ID,
            )
            # Optional override for local/dev: if an explicit model id is provided,
            # try to load that artifact directly from the model dir.
            if settings.ACTIVE_MODEL_ID:
                path = os.path.join(settings.MODEL_DIR, f"{settings.ACTIVE_MODEL_ID}.joblib")
                if os.path.exists(path):
                    try:
                        logger.info("model_loader: loading override model from %s", path)
                        self._bundle = joblib.load(path)
                        self._loaded_at = now
                        self._loaded_mtime = os.path.getmtime(path)
                        self._loaded_model_id = self._bundle.get("model_id") if isinstance(self._bundle, dict) else None
                        logger.info("model_loader: override load complete; bundle.model_id=%s", self._loaded_model_id)
                        return self._bundle
                    except Exception:
                        logger.exception("model_loader: override load failed; continuing")
                        pass
                else:
                    logger.warning("model_loader: ACTIVE_MODEL_ID set but file missing: %s", path)

            self._maybe_refresh_from_snowflake()
            path = self._path()
            if not os.path.exists(path):
                logger.warning("model_loader: no local model artifact found at %s", path)
                self._bundle = None
                self._loaded_at = 0.0
                self._loaded_mtime = 0.0
                return None

            mtime = os.path.getmtime(path)
            if self._bundle and now - self._loaded_at < self._ttl and mtime == self._loaded_mtime:
                logger.info(
                    "model_loader: returning cached bundle (model_id=%s age=%.1fs ttl=%ss)",
                    self._loaded_model_id,
                    now - self._loaded_at,
                    self._ttl,
                )
                return self._bundle
            try:
                logger.info("model_loader: loading bundle from %s", path)
                self._bundle = joblib.load(path)
                self._loaded_at = now
                self._loaded_mtime = mtime
                self._loaded_model_id = self._bundle.get("model_id") if isinstance(self._bundle, dict) else None
                logger.info("model_loader: load complete; bundle.model_id=%s", self._loaded_model_id)
            except Exception:
                logger.exception("model_loader: bundle load failed; returning existing bundle (if any)")
                return self._bundle
            return self._bundle

    def reload(self) -> dict[str, Any] | None:
        self._loaded_at = 0.0
        self._loaded_model_id = None
        return self.get()


loader = ModelLoader()
