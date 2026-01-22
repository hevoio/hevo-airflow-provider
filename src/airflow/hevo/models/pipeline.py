"""Pipeline-related Pydantic models for Hevo API responses."""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any, Optional

from pydantic import BaseModel, Field

from airflow.hevo.models.common import BaseResponse


class PipelineStatus(str, Enum):
    """Pipeline status values."""

    INITIALIZED = "INITIALIZED"
    PAUSED = "PAUSED"
    STOPPED = "STOPPED"
    FAILED = "FAILED"
    INCOMPLETE = "INCOMPLETE"


class SyncType(str, Enum):
    """
    Pipeline sync type values.

    Defines how and when a pipeline executes sync operations:
    - ON_DEMAND: Manual sync triggered via API or Airflow (no schedule)
    - SCHEDULED: Automatic sync based on configured schedule interval
    """

    ON_DEMAND = "ON_DEMAND"
    SCHEDULED = "SCHEDULED"


class PipelineSource(BaseResponse):
    """Pipeline source configuration."""

    source_id: Optional[str] = Field(None, description="Source connector ID")
    source_name: Optional[str] = Field(None, description="Source name")
    source_type: Optional[str] = Field(None, description="Type of source connector")
    connection_params: Optional[dict[str, Any]] = Field(None, description="Source connection parameters")


class PipelineDestination(BaseResponse):
    """Pipeline destination configuration."""

    destination_id: Optional[str] = Field(None, description="Destination ID")
    destination_name: Optional[str] = Field(None, description="Destination name")
    destination_type: Optional[str] = Field(None, description="Type of destination")


class PipelineConfig(BaseResponse):
    """Pipeline configuration details."""

    sync_type: Optional[SyncType] = Field(None, description="Sync type (ON_DEMAND or SCHEDULED)")
    schedule: Optional[dict[str, Any]] = Field(None, description="Schedule configuration for scheduled syncs")
    objects: Optional[list[dict[str, Any]]] = Field(None, description="List of objects being synced")


class Pipeline(BaseResponse):
    """
    Complete pipeline model representing a Hevo data pipeline.

    Returned by:
    - GET /api/v1/pipelines/{id}
    - GET /api/v1/pipelines (list endpoint)
    """

    id: int = Field(..., description="Unique pipeline identifier")
    name: str = Field(..., description="Pipeline name")
    status: PipelineStatus = Field(..., description="Current pipeline status")
    source: Optional[PipelineSource] = Field(None, description="Source configuration")
    destination: Optional[PipelineDestination] = Field(None, description="Destination configuration")
    config: Optional[PipelineConfig] = Field(None, description="Pipeline configuration")
    created_at: Optional[datetime] = Field(None, description="Pipeline creation timestamp")
    updated_at: Optional[datetime] = Field(None, description="Last update timestamp")
    last_synced_at: Optional[datetime] = Field(None, description="Last successful sync timestamp")


class PaginatedPipelinesResponse(BaseResponse):
    """Paginated response for listing pipelines."""

    data: list[Pipeline] = Field(..., description="List of pipelines")
    has_more: bool = Field(..., description="Whether more results are available")
    next_cursor: Optional[str] = Field(None, description="Cursor for the next page")
    count: Optional[int] = Field(None, description="Total count of pipelines")
