from fastapi import APIRouter
from pydantic import BaseModel

from app.pipelines.train_model import train
from via_common.config import settings
from via_common.storage import storage

router = APIRouter()


class TrainRequest(BaseModel):
    algo: str = "gbr"


class ClearTrainingStateResponse(BaseModel):
    raw_deleted: int
    model_files_removed: int


@router.post("/train")
def run_train(req: TrainRequest | None = None):
    algo = (req.algo if req else "gbr")
    return train(algo=algo)


@router.get("/models/active")
def active():
    return storage.active_model() or {}


@router.post("/admin/clear", response_model=ClearTrainingStateResponse)
def clear_training_state():
    result = storage.clear_all_training_state()
    return ClearTrainingStateResponse(**result)
