from __future__ import annotations

from datetime import date, datetime
from typing import Literal, Optional

from pydantic import BaseModel, Field


class StopObservation(BaseModel):
    train_number: str
    service_date: date
    stop_sequence: int
    station_code: str
    scheduled_arrival: Optional[datetime] = None
    actual_arrival: Optional[datetime] = None
    scheduled_departure: Optional[datetime] = None
    actual_departure: Optional[datetime] = None
    delay_minutes: Optional[float] = None
    source: Literal["transitdocs", "via_live"] = "transitdocs"
    scraped_at: datetime = Field(default_factory=datetime.utcnow)


class TrainRunSummary(BaseModel):
    train_number: str
    service_date: date
    origin_station: Optional[str] = None
    dest_station: Optional[str] = None
    final_delay_minutes: Optional[float] = None
    max_delay_minutes: Optional[float] = None
    stops: int = 0


class PredictionRequestItem(BaseModel):
    train_number: str
    service_date: date
    origin: Optional[str] = None
    destination: Optional[str] = None
    scheduled_departure: Optional[datetime] = None


class PredictionFactors(BaseModel):
    weekday_effect: float = 0.0
    weather_effect: float = 0.0
    recent_trend_effect: float = 0.0
    route_effect: float = 0.0


class PredictionResult(BaseModel):
    train_number: str
    service_date: date
    p50_delay_min: float
    p90_delay_min: float
    severity: Literal["on_time", "minor", "moderate", "significant", "severe"]
    factors: PredictionFactors
    model_id: str
    as_of: datetime = Field(default_factory=datetime.utcnow)


def severity_for(minutes: float) -> str:
    if minutes < 5:
        return "on_time"
    if minutes < 15:
        return "minor"
    if minutes < 30:
        return "moderate"
    if minutes < 60:
        return "significant"
    return "severe"
