"""Unit tests for job-related Pydantic models."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from airflow.hevo.models.job import (
    Job,
    JobCompletionStatus,
    JobStatus,
    JobType,
    PaginatedJobsResponse,
)


class TestJobEnums:
    """Tests for job-related enums."""

    def test_job_status_enum_values(self) -> None:
        """Test all JobStatus enum values are correct."""
        assert JobStatus.IN_PROGRESS == "IN_PROGRESS"
        assert JobStatus.CANCELLING == "CANCELLING"
        assert JobStatus.COMPLETED == "COMPLETED"
        assert JobStatus.COMPLETED_WITH_FAILURES == "COMPLETED_WITH_FAILURES"
        assert JobStatus.FAILED == "FAILED"
        assert JobStatus.CANCELLED == "CANCELLED"
        assert JobStatus.SKIPPED == "SKIPPED"
        assert JobStatus.DEFERRED == "DEFERRED"
        assert JobStatus.DEFERRED_WITH_FAILURES == "DEFERRED_WITH_FAILURES"

    def test_job_type_enum_values(self) -> None:
        """Test all JobType enum values are correct."""
        assert JobType.INCREMENTAL == "INCREMENTAL"
        assert JobType.HISTORICAL == "HISTORICAL"
        assert JobType.TRUNCATE_AND_LOAD == "TRUNCATE_AND_LOAD"

    def test_job_completion_status_enum_values(self) -> None:
        """Test all JobCompletionStatus enum values are correct."""
        assert JobCompletionStatus.COMPLETED == "completed"
        assert JobCompletionStatus.COMPLETED_WITH_FAILURES == "completed_with_failures"
        assert JobCompletionStatus.FAILED == "failed"
        assert JobCompletionStatus.PENDING == "pending"


class TestJobModel:
    """Tests for Job model."""

    def test_job_creation_with_all_fields(self, sample_job_response) -> None:
        """Test job model can be created with all API fields."""
        job = Job(**sample_job_response)
        assert job.job_id == "550e8400-e29b-41d4-a716-446655440001"
        assert job.type == JobType.INCREMENTAL
        assert job.status == JobStatus.IN_PROGRESS
        assert job.created_ts == 1704067200000
        assert job.updated_ts == 1704067260000
        assert job.events_ingested == 1000
        assert job.events_loaded == 950
        assert job.events_failed == 50
        assert job.objects_success == 5
        assert job.objects_queued == 2
        assert job.objects_skipped == 0
        assert job.objects_failed == 0
        assert job.billable_events == 1000
        assert job.non_billable_events == 0
        assert job.duration == 60000
        assert job.min_latency == 100
        assert job.max_latency == 5000
        assert job.mean_latency == 1500

    def test_job_timestamp_fields(self, sample_job_response) -> None:
        """Test job timestamp fields are integers in milliseconds."""
        job = Job(**sample_job_response)

        # Test timestamps are integers (milliseconds since epoch)
        assert job.created_ts == 1704067200000
        assert job.updated_ts == 1704067260000
        assert isinstance(job.created_ts, int)
        assert isinstance(job.updated_ts, int)

    def test_job_duration_field(self, sample_job_response) -> None:
        """Test job duration field is in milliseconds."""
        job = Job(**sample_job_response)
        assert job.duration == 60000  # milliseconds
        assert isinstance(job.duration, int)

    def test_job_latency_fields(self, sample_job_response) -> None:
        """Test latency fields are in milliseconds."""
        job = Job(**sample_job_response)
        assert job.min_latency == 100
        assert job.max_latency == 5000
        assert job.mean_latency == 1500
        assert isinstance(job.min_latency, int)
        assert isinstance(job.max_latency, int)
        assert isinstance(job.mean_latency, int)

    def test_job_latency_fields_nullable(self, sample_completed_job_response) -> None:
        """Test latency fields can be None."""
        # Create a job with null latency values
        data = sample_completed_job_response.copy()
        data["min_latency"] = None
        data["max_latency"] = None
        data["mean_latency"] = None

        job = Job(**data)
        assert job.min_latency is None
        assert job.max_latency is None
        assert job.mean_latency is None

    def test_job_is_terminal_property(self, sample_completed_job_response) -> None:
        """Test is_terminal property for completed job."""
        job = Job(**sample_completed_job_response)
        assert job.is_terminal is True

    def test_job_is_not_terminal(self, sample_job_response) -> None:
        """Test is_terminal property for in-progress job."""
        job = Job(**sample_job_response)
        assert job.is_terminal is False

    def test_job_is_failed(self, sample_failed_job_response) -> None:
        """Test is_failed property for failed job."""
        job = Job(**sample_failed_job_response)
        assert job.is_failed is True

    def test_job_with_unknown_status_from_api(self, sample_job_response) -> None:
        """Test that unknown status values from API raise an exception."""
        data = sample_job_response.copy()
        data["status"] = "NEW_STATUS_FROM_API"

        with pytest.raises(ValidationError) as exc_info:
            Job(**data)

        # Verify the error message contains helpful information
        error_str = str(exc_info.value)
        assert "NEW_STATUS_FROM_API" in error_str
        assert "Unknown job status" in error_str

    def test_job_with_unknown_type_from_api(self, sample_job_response) -> None:
        """Test that unknown job type values from API raise an exception."""
        data = sample_job_response.copy()
        data["type"] = "NEW_JOB_TYPE"

        with pytest.raises(ValidationError) as exc_info:
            Job(**data)

        # Verify the error message contains helpful information
        error_str = str(exc_info.value)
        assert "NEW_JOB_TYPE" in error_str
        assert "Unknown job type" in error_str

    def test_job_with_both_unknown_type_and_status(self, sample_job_response) -> None:
        """Test that unknown type raises an exception (status validation not reached)."""
        data = sample_job_response.copy()
        data["type"] = "FUTURE_TYPE"
        data["status"] = "FUTURE_STATUS"

        with pytest.raises(ValidationError) as exc_info:
            Job(**data)

        # The validation will fail on the first unknown field (type)
        error_str = str(exc_info.value)
        assert "Unknown" in error_str


class TestPaginatedJobsResponse:
    """Tests for PaginatedJobsResponse model."""

    def test_paginated_response_creation(self, sample_jobs_list_response) -> None:
        """Test paginated response can be created."""
        response = PaginatedJobsResponse(**sample_jobs_list_response)
        assert len(response.data) == 1
        assert response.has_more is False
        assert response.next_cursor is None

    def test_paginated_response_with_cursor(self, sample_job_response) -> None:
        """Test paginated response with pagination cursor."""
        data = {"data": [sample_job_response, sample_job_response], "has_more": True, "next_cursor": "cursor_abc123"}
        response = PaginatedJobsResponse(**data)
        assert len(response.data) == 2
        assert response.has_more is True
        assert response.next_cursor == "cursor_abc123"

    def test_paginated_response_data_is_list_of_jobs(self, sample_jobs_list_response) -> None:
        """Test paginated response data contains Job instances."""
        response = PaginatedJobsResponse(**sample_jobs_list_response)
        assert isinstance(response.data[0], Job)
        assert response.data[0].job_id == "550e8400-e29b-41d4-a716-446655440001"
