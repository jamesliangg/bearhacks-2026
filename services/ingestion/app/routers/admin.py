from __future__ import annotations

from fastapi import APIRouter

from via_common.storage import storage

router = APIRouter()


@router.get("/stop_observations")
def dump_stop_observations():
    data = storage.dump_stop_observations()
    return {"rows": data, "count": len(data)}


@router.delete("/stop_observations")
def delete_stop_observations():
    deleted = storage.clear_stop_observations()
    return {"deleted": deleted}


@router.delete("/models")
def delete_models():
    result = storage.delete_models()
    return result
