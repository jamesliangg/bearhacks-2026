from __future__ import annotations

import os
import threading
import time
from typing import Any

import joblib

from via_common.config import settings
from via_common.storage import storage


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
            return
        meta = storage.active_model()
        if not meta:
            return
        model_id = meta.get("model_id")
        artifact_uri = meta.get("artifact_uri")
        if not model_id or not artifact_uri:
            return
        if self._loaded_model_id == model_id and os.path.exists(self._path()):
            return

        os.makedirs(settings.MODEL_DIR, exist_ok=True)
        local_path = self._path()

        stage_ref = artifact_uri
        # Expect artifact_uri like "@MODEL_STAGE/<model_id>.joblib"
        with storage._snowflake() as conn:
            with conn.cursor() as cur:
                cur.execute(f"GET {stage_ref} file://{settings.MODEL_DIR} OVERWRITE=TRUE")

        # Snowflake GET writes into a subdir named after the stage by default; try to find the file.
        if not os.path.exists(local_path):
            for root, _, files in os.walk(settings.MODEL_DIR):
                for f in files:
                    if f == f"{model_id}.joblib":
                        os.replace(os.path.join(root, f), local_path)
                        break
        self._loaded_model_id = model_id

    def get(self) -> dict[str, Any] | None:
        now = time.time()
        with self._lock:
            self._maybe_refresh_from_snowflake()
            path = self._path()
            if not os.path.exists(path):
                self._bundle = None
                self._loaded_at = 0.0
                self._loaded_mtime = 0.0
                return None

            mtime = os.path.getmtime(path)
            if self._bundle and now - self._loaded_at < self._ttl and mtime == self._loaded_mtime:
                return self._bundle
            try:
                self._bundle = joblib.load(path)
                self._loaded_at = now
                self._loaded_mtime = mtime
                self._loaded_model_id = self._bundle.get("model_id") if isinstance(self._bundle, dict) else None
            except Exception:
                return self._bundle
            return self._bundle

    def reload(self) -> dict[str, Any] | None:
        self._loaded_at = 0.0
        self._loaded_model_id = None
        return self.get()


loader = ModelLoader()
