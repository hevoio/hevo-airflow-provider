"""Job-related Pydantic models for Hevo API responses."""

from __future__ import annotations

import logging
from enum import Enum
from typing import Any, Optional, Union

from pydantic import Field, field_validator

from airflow.hevo.models.common import BaseResponse

logger = logging.getLogger(__name__)


class JobStatus(str, Enum):
    """
    Job status values.

    These represent the various states a sync job can be in.
    """

    IN_PROGRESS = "IN_PROGRESS"
    CANCELLING = "CANCELLING"
    COMPLETED = "COMPLETED"
    COMPLETED_WITH_FAILURES = "COMPLETED_WITH_FAILURES"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
    SKIPPED = "SKIPPED"
    DEFERRED = "DEFERRED"
    DEFERRED_WITH_FAILURES = "DEFERRED_WITH_FAILURES"


class JobType(str, Enum):
    """Job type values indicating the type of sync operation."""

    INCREMENTAL = "INCREMENTAL"
    HISTORICAL = "HISTORICAL"
    RESYNC_WITH_EVOLVE = "RESYNC_WITH_EVOLVE"
    RESYNC_WITH_DROP_AND_LOAD = "RESYNC_WITH_DROP_AND_LOAD"
    REFRESHER = "REFRESHER"


class JobCompletionStatus(str, Enum):
    """
    Normalized job completion status values.

    These are the canonical status values returned by get_job_completion_status()
    methods, abstracting the raw JobStatus enum into four simple states for
    easier consumption by operators, sensors, and triggers.

    Mapping from JobStatus:
    - COMPLETED: Job finished successfully without any failures
    - COMPLETED_WITH_FAILURES: Job finished but some records failed
      (only returned as success when accept_completed_with_failures=True)
    - FAILED: Job failed, was cancelled, skipped, or deferred with failure
      (FAILED, CANCELLED, SKIPPED, DEFERRED, DEFERRED_WITH_FAILURES, or
      COMPLETED_WITH_FAILURES when not accepting failures)
    - PENDING: Job is still running (IN_PROGRESS, CANCELLING)
    """

    COMPLETED = "completed"
    COMPLETED_WITH_FAILURES = "completed_with_failures"
    FAILED = "failed"
    PENDING = "pending"


class Job(BaseResponse):
    """
    Complete job model representing a pipeline sync job.

    Returned by:
    - GET /api/v1/pipelines/{id}/jobs/{job_id}
    - GET /api/v1/pipelines/{id}/jobs (list endpoint)

    All timestamp fields (created_ts, updated_ts) are in milliseconds since epoch.
    All duration and latency fields are in milliseconds.
    """

    job_id: str = Field(..., description="Unique job identifier (UUID)")
    type: Union[JobType, str] = Field(
        ..., description="Job type (INCREMENTAL, HISTORICAL, RESYNC_WITH_EVOLVE, RESYNC_WITH_DROP_AND_LOAD)"
    )
    status: Union[JobStatus, str] = Field(..., description="Current job status")
    created_ts: int = Field(..., description="Job creation timestamp in milliseconds since epoch")
    updated_ts: int = Field(..., description="Last update timestamp in milliseconds since epoch")

    # Event metrics
    events_ingested: int = Field(..., description="Total events extracted from sources")
    events_loaded: int = Field(..., description="Events successfully written to destination")
    events_failed: int = Field(..., description="Events that failed during loading")

    # Object metrics
    objects_success: int = Field(..., description="Successfully processed objects")
    objects_queued: int = Field(..., description="Objects awaiting processing")
    objects_skipped: int = Field(..., description="Objects bypassed during execution")
    objects_failed: int = Field(..., description="Objects that encountered errors")

    # Billing metrics
    billable_events: int = Field(..., description="Chargeable event count")
    non_billable_events: int = Field(..., description="Non-chargeable event count")

    # Performance metrics
    duration: int = Field(..., description="Execution time in milliseconds")
    min_latency: Optional[int] = Field(None, description="Minimum object-level latency in milliseconds")
    max_latency: Optional[int] = Field(None, description="Maximum object-level latency in milliseconds")
    mean_latency: Optional[int] = Field(None, description="Average latency across objects in milliseconds")

    @field_validator("type", mode="before")
    @classmethod
    def validate_job_type(cls, value: Any) -> JobType:
        """
        Validate and normalize job type values.

        :raises ValueError: If the API returns an unrecognized job type.
        """
        if isinstance(value, JobType):
            return value

        # Try to match string value to known enum
        try:
            return JobType(str(value))
        except ValueError as e:
            error_msg = (
                f"Unknown job type '{value}' received from Hevo API. "
                f"Known types: {', '.join([t.value for t in JobType])}. "
                f"Check for the latest version of the airflow-hevo provider or reach out to Hevo support."
            )
            logger.error(error_msg)
            raise ValueError(error_msg) from e

    @field_validator("status", mode="before")
    @classmethod
    def validate_job_status(cls, value: Any) -> JobStatus:
        """
        Validate and normalize job status values.

        :raises ValueError: If the API returns an unrecognized job status.
        """
        if isinstance(value, JobStatus):
            return value

        # Try to match string value to known enum
        try:
            return JobStatus(str(value))
        except ValueError as e:
            error_msg = (
                f"Unknown job status '{value}' received from Hevo API. "
                f"Known statuses: {', '.join([s.value for s in JobStatus])}. "
                f"Check for the latest version of the airflow-hevo provider or reach out to Hevo support."
            )
            logger.error(error_msg)
            raise ValueError(error_msg) from e

    @property
    def is_terminal(self) -> bool:
        """Check if the job is in a terminal state (completed, failed, or cancelled)."""
        terminal_states = {
            JobStatus.COMPLETED,
            JobStatus.COMPLETED_WITH_FAILURES,
            JobStatus.FAILED,
            JobStatus.CANCELLED,
            JobStatus.SKIPPED,
        }
        return self.status in terminal_states

    @property
    def is_failed(self) -> bool:
        """Check if the job failed completely."""
        return self.status in {
            JobStatus.FAILED,
            JobStatus.CANCELLED,
            JobStatus.DEFERRED_WITH_FAILURES,
        }


class PaginatedJobsResponse(BaseResponse):
    """
    Paginated response for listing jobs.

    Returned by GET /api/v1/pipelines/{id}/jobs
    Uses cursor-based pagination.
    """

    data: list[Job] = Field(..., description="List of jobs")
    has_more: bool = Field(..., description="Whether more results are available")
    next_cursor: Optional[str] = Field(None, description="Cursor for the next page of results")
