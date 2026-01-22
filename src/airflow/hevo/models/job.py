"""Job-related Pydantic models for Hevo API responses."""

from __future__ import annotations

import logging
from datetime import datetime
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
    COMPLETED = "COMPLETED"
    COMPLETED_WITH_FAILURES = "COMPLETED_WITH_FAILURES"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
    SKIPPED = "SKIPPED"
    DEFERRED = "DEFERRED"
    DEFERRED_WITH_FAILURE = "DEFERRED_WITH_FAILURE"
    QUEUED = "QUEUED"
    PENDING = "PENDING"
    UNKNOWN = "UNKNOWN"  # Fallback for new statuses


class JobType(str, Enum):
    """Job type values indicating the type of sync operation."""

    INCREMENTAL = "INCREMENTAL"
    HISTORICAL = "HISTORICAL"
    TRUNCATE_AND_LOAD = "TRUNCATE_AND_LOAD"
    UNKNOWN = "UNKNOWN"  # Fallback for new job types


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
      (FAILED, CANCELLED, SKIPPED, DEFERRED, DEFERRED_WITH_FAILURE, or
      COMPLETED_WITH_FAILURES when not accepting failures)
    - PENDING: Job is still running (IN_PROGRESS, QUEUED, PENDING)
    """

    COMPLETED = "completed"
    COMPLETED_WITH_FAILURES = "completed_with_failures"
    FAILED = "failed"
    PENDING = "pending"


class JobStatistics(BaseResponse):
    """Statistics about job execution."""

    rows_loaded: Optional[int] = Field(None, description="Number of rows successfully loaded")
    rows_failed: Optional[int] = Field(None, description="Number of rows that failed to load")
    bytes_transferred: Optional[int] = Field(None, description="Total bytes transferred")
    duration_seconds: Optional[float] = Field(None, description="Job duration in seconds")


class JobError(BaseResponse):
    """Error information for failed jobs."""

    error_code: Optional[str] = Field(None, description="Error code")
    error_message: Optional[str] = Field(None, description="Human-readable error message")
    error_details: Optional[dict[str, Any]] = Field(None, description="Additional error details")


class Job(BaseResponse):
    """
    Complete job model representing a pipeline sync job.

    Returned by:
    - GET /api/v1/pipelines/{id}/jobs/{job_id}
    - GET /api/v1/pipelines/{id}/jobs (list endpoint)
    """

    job_id: str = Field(..., description="Unique job identifier")
    type: Union[JobType, str] = Field(..., description="Job type (INCREMENTAL, HISTORICAL)")
    status: Union[JobStatus, str] = Field(..., description="Current job status")
    created_at: Optional[datetime] = Field(None, description="Job creation timestamp")
    started_at: Optional[datetime] = Field(None, description="Job start timestamp")
    completed_at: Optional[datetime] = Field(None, description="Job completion timestamp")
    updated_at: Optional[datetime] = Field(None, description="Last update timestamp")
    statistics: Optional[JobStatistics] = Field(None, description="Job execution statistics")
    error: Optional[JobError] = Field(None, description="Error information if job failed")
    metadata: Optional[dict[str, Any]] = Field(None, description="Additional job metadata")

    @field_validator("type", mode="before")
    @classmethod
    def validate_job_type(cls, value: Any) -> JobType:
        """
        Validate and normalize job type values.

        If the API returns a new job type that doesn't exist in our enum,
        log a warning and return JobType.UNKNOWN to prevent breaking.
        """
        if isinstance(value, JobType):
            return value

        # Try to match string value to known enum
        try:
            return JobType(str(value))
        except ValueError:
            logger.warning(
                "Unknown job type '%s' received from API. Please update JobType enum. "
                "Defaulting to JobType.UNKNOWN",
                value
            )
            return JobType.UNKNOWN

    @field_validator("status", mode="before")
    @classmethod
    def validate_job_status(cls, value: Any) -> JobStatus:
        """
        Validate and normalize job status values.

        If the API returns a new status that doesn't exist in our enum,
        log a warning and return JobStatus.UNKNOWN to prevent breaking.
        """
        if isinstance(value, JobStatus):
            return value

        # Try to match string value to known enum
        try:
            return JobStatus(str(value))
        except ValueError:
            logger.warning(
                "Unknown job status '%s' received from API. Please update JobStatus enum. "
                "Defaulting to JobStatus.UNKNOWN",
                value
            )
            return JobStatus.UNKNOWN

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
    def is_successful(self) -> bool:
        """Check if the job completed successfully."""
        return self.status == JobStatus.COMPLETED

    @property
    def has_failures(self) -> bool:
        """Check if the job completed but with some failures."""
        return self.status == JobStatus.COMPLETED_WITH_FAILURES

    @property
    def is_failed(self) -> bool:
        """Check if the job failed completely."""
        return self.status in {
            JobStatus.FAILED,
            JobStatus.CANCELLED,
            JobStatus.DEFERRED_WITH_FAILURE,
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
    count: Optional[int] = Field(None, description="Total count of jobs (if available)")
