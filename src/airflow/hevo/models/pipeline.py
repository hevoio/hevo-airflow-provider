"""Pipeline-related Pydantic models for Hevo API responses."""

from __future__ import annotations

import logging
from enum import Enum
from typing import Any, Optional, Union

from pydantic import Field, field_validator

from airflow.hevo.models.common import BaseResponse

logger = logging.getLogger(__name__)


class PipelineStatus(str, Enum):
    """Pipeline status values."""

    INITIALIZING = "INITIALIZING"
    INITIALIZED = "INITIALIZED"
    INITIALIZE_FAILED = "INITIALIZE_FAILED"
    DISABLING = "DISABLING"
    DISABLED = "DISABLED"
    DISABLE_FAILED = "DISABLE_FAILED"
    ENABLING = "ENABLING"
    ENABLED = "ENABLED"
    ENABLE_FAILED = "ENABLE_FAILED"
    DELETING = "DELETING"
    DELETED = "DELETED"
    DELETE_FAILED = "DELETE_FAILED"
    RESTARTING = "RESTARTING"
    RESTART_FAILED = "RESTART_FAILED"


class SyncType(str, Enum):
    """
    Pipeline sync type values.

    Defines how and when a pipeline executes sync operations:
    - ON_DEMAND: Manual sync triggered via API or Airflow (no schedule)
    - SCHEDULED: Automatic sync based on configured schedule interval
    """

    ON_DEMAND = "ON_DEMAND"
    SCHEDULED = "SCHEDULED"


class PipelineAction(str, Enum):
    """
    Pipeline action types.

    Defines which API action to trigger:
    - SYNC_NOW: Trigger a regular sync (POST /pipelines/{id}/actions/sync-now)
    - RESYNC: Trigger a full historical resync (POST /pipelines/{id}/actions/resync)
    """

    SYNC_NOW = "SYNC_NOW"
    RESYNC = "RESYNC"


class ResyncMode(str, Enum):
    """
    Resync mode for pipeline resync operations.

    Defines how destination tables are handled during a resync:
    - EVOLVE_AND_MERGE: Triggers historical resync without dropping destination tables(default).
    - DROP_AND_LOAD: drops existing destination tables before loading.Ensures a clean slate by recreating tables from scratch
    """

    EVOLVE_AND_MERGE = "EVOLVE_AND_MERGE"
    DROP_AND_LOAD = "DROP_AND_LOAD"


class ReplicationType(str, Enum):
    """
    Pipeline replication type values.

    Defines what data is replicated:
    - HISTORICAL_AND_INCREMENTAL: Full historical load followed by incremental updates
    - INCREMENTAL_ONLY: Only incremental updates (no historical load)
    """

    HISTORICAL_AND_INCREMENTAL = "HISTORICAL_AND_INCREMENTAL"
    INCREMENTAL_ONLY = "INCREMENTAL_ONLY"


class LoadMode(str, Enum):
    """
    Pipeline load mode values.

    Determines how data is loaded into the destination:
    - APPEND: Adds new records without modifying existing ones
    - MERGE: Updates existing records based on primary keys and adds new ones
    """

    APPEND = "APPEND"
    MERGE = "MERGE"


class SchemaEvolution(str, Enum):
    """
    Schema evolution handling strategy.

    Controls how schema changes in the source are handled:
    - ALLOW_ALL: All schema changes are automatically propagated
    - BLOCK_ALL: All schema changes are blocked
    - ALLOW_COLUMN_LEVEL: Column-level changes are allowed
    """

    ALLOW_ALL = "ALLOW_ALL"
    BLOCK_ALL = "BLOCK_ALL"
    ALLOW_COLUMN_LEVEL = "ALLOW_COLUMN_LEVEL"


class Source(BaseResponse):
    """Pipeline source configuration."""

    name: str = Field(..., description="Source name")
    connector_id: str = Field(..., description="Source connector ID")
    source_type: str = Field(..., description="Type of source connector")
    config: Optional[dict[str, Any]] = Field(None, description="Source connection configuration")


class Destination(BaseResponse):
    """Pipeline destination configuration."""

    id: int = Field(..., description="Destination ID")
    name: str = Field(..., description="Destination name")
    connector_id: str = Field(..., description="Destination connector ID")
    destination_type: str = Field(..., description="Type of destination")
    status: Optional[str] = Field(None, description="Destination status")


class Schedule(BaseResponse):
    """Pipeline schedule configuration."""

    sync_type: Optional[SyncType] = Field(None, description="Sync type (ON_DEMAND or SCHEDULED)")
    frequency_minutes: Optional[int] = Field(None, description="Sync frequency in minutes (only for SCHEDULED)")


class FailureHandlingPolicy(BaseResponse):
    """Pipeline failure handling policy configuration."""

    object_failure_level: str = Field(..., description="Failure level threshold for objects")
    object_failure_threshold: int = Field(..., description="Number of object failures before taking action")


class LatencyAlert(BaseResponse):
    """Pipeline latency alert configuration."""

    enabled: bool = Field(..., description="Whether latency alerts are enabled")
    threshold_minutes: Optional[int] = Field(None, description="Latency threshold in minutes")


class SourceSchemaStatus(BaseResponse):
    """Source schema refresh status."""

    status: str = Field(..., description="Schema refresh status")
    refreshed_ts: Optional[int] = Field(None, description="Last schema refresh timestamp in milliseconds")


class Pipeline(BaseResponse):
    """
    Complete pipeline model representing a Hevo data pipeline.

    Returned by:
    - GET /api/v1/pipelines/{id}
    - GET /api/v1/pipelines (list endpoint)

    All timestamp fields (created_ts, updated_ts) are in milliseconds since epoch.
    """

    id: int = Field(..., description="Unique pipeline identifier")
    name: str = Field(..., description="Pipeline name")
    status: Union[PipelineStatus, str] = Field(..., description="Current pipeline status")
    source: Source = Field(..., description="Source configuration")
    destination: Destination = Field(..., description="Destination configuration")
    destination_prefix: str = Field(..., description="Prefix used for destination table names")
    replication_type: Union[ReplicationType, str] = Field(..., description="Type of data replicated")
    load_mode: Union[LoadMode, str] = Field(..., description="How data is loaded (APPEND or MERGE)")
    schema_evolution: Union[SchemaEvolution, str] = Field(..., description="Schema change handling strategy")
    schedule: Schedule = Field(..., description="Synchronization timing configuration")
    failure_handling_policy: FailureHandlingPolicy = Field(..., description="Failure handling settings")
    source_schema_status: SourceSchemaStatus = Field(..., description="Schema refresh details")
    created_by_email: str = Field(..., description="Email of user who created the pipeline")
    updated_by_email: str = Field(..., description="Email of user who last updated the pipeline")
    created_ts: int = Field(..., description="Pipeline creation timestamp in milliseconds since epoch")
    updated_ts: int = Field(..., description="Last update timestamp in milliseconds since epoch")

    # Optional fields that may not always be present
    latency_alert: Optional[LatencyAlert] = Field(None, description="Latency alert configuration")

    @field_validator("status", mode="before")
    @classmethod
    def validate_status(cls, value: Any) -> PipelineStatus:
        """
        Validate and normalize pipeline status values.

        :raises ValueError: If the API returns an unrecognized pipeline status.
        """
        if isinstance(value, PipelineStatus):
            return value

        # Try to match string value to known enum
        try:
            return PipelineStatus(str(value))
        except ValueError as e:
            error_msg = (
                f"Unknown pipeline status '{value}' received from Hevo API. "
                f"Known statuses: {', '.join([s.value for s in PipelineStatus])}. "
                f"Check for the latest version of the airflow-hevo provider or reach out to Hevo support."
            )
            logger.error(error_msg)
            raise ValueError(error_msg) from e

    @field_validator("replication_type", mode="before")
    @classmethod
    def validate_replication_type(cls, value: Any) -> ReplicationType:
        """
        Validate and normalize replication type values.

        :raises ValueError: If the API returns an unrecognized replication type.
        """
        if isinstance(value, ReplicationType):
            return value

        # Try to match string value to known enum
        try:
            return ReplicationType(str(value))
        except ValueError as e:
            error_msg = (
                f"Unknown replication type '{value}' received from Hevo API. "
                f"Known types: {', '.join([t.value for t in ReplicationType])}. "
                f"Check for the latest version of the airflow-hevo provider or reach out to Hevo support."
            )
            logger.error(error_msg)
            raise ValueError(error_msg) from e

    @field_validator("load_mode", mode="before")
    @classmethod
    def validate_load_mode(cls, value: Any) -> LoadMode:
        """
        Validate and normalize load mode values.

        :raises ValueError: If the API returns an unrecognized load mode.
        """
        if isinstance(value, LoadMode):
            return value

        # Try to match string value to known enum
        try:
            return LoadMode(str(value))
        except ValueError as e:
            error_msg = (
                f"Unknown load mode '{value}' received from Hevo API. "
                f"Known modes: {', '.join([m.value for m in LoadMode])}. "
                f"Check for the latest version of the airflow-hevo provider or reach out to Hevo support."
            )
            logger.error(error_msg)
            raise ValueError(error_msg) from e

    @field_validator("schema_evolution", mode="before")
    @classmethod
    def validate_schema_evolution(cls, value: Any) -> SchemaEvolution:
        """
        Validate and normalize schema evolution values.

        :raises ValueError: If the API returns an unrecognized schema evolution strategy.
        """
        if isinstance(value, SchemaEvolution):
            return value

        # Try to match string value to known enum
        try:
            return SchemaEvolution(str(value))
        except ValueError as e:
            error_msg = (
                f"Unknown schema evolution strategy '{value}' received from Hevo API. "
                f"Known strategies: {', '.join([s.value for s in SchemaEvolution])}. "
                f"Check for the latest version of the airflow-hevo provider or reach out to Hevo support."
            )
            logger.error(error_msg)
            raise ValueError(error_msg) from e


class PaginatedPipelinesResponse(BaseResponse):
    """
    Paginated response for listing pipelines.

    Returned by GET /api/v1/pipelines
    Uses cursor-based pagination.
    """

    data: list[Pipeline] = Field(..., description="List of pipelines")
    has_more: bool = Field(..., description="Whether more results are available")
    next_cursor: Optional[str] = Field(None, description="Cursor for the next page")
