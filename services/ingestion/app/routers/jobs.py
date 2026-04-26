from fastapi import APIRouter

from via_common.storage import storage

router = APIRouter()


@router.get("")
def list_jobs(limit: int = 50):
    return {"jobs": storage.list_job_runs(limit=limit)}
