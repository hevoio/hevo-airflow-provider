"""Unit tests for job-related Pydantic models."""

from __future__ import annotations

import pytest

from airflow.hevo.models.job import (
    Job,
    JobCompletionStatus,
    JobError,
    JobStatistics,
    JobStatus,
    JobType,
    PaginatedJobsResponse,
)


class TestJobEnums:
    """Tests for job-related enums."""

    def test_job_status_enum_values(self):
        """Test all JobStatus enum values are correct."""
        assert JobStatus.IN_PROGRESS == "IN_PROGRESS"
        assert JobStatus.COMPLETED == "COMPLETED"
        assert JobStatus.COMPLETED_WITH_FAILURES == "COMPLETED_WITH_FAILURES"
        assert JobStatus.FAILED == "FAILED"
        assert JobStatus.CANCELLED == "CANCELLED"

    def test_job_type_enum_values(self):
        """Test all JobType enum values are correct."""
        assert JobType.INCREMENTAL == "INCREMENTAL"
        assert JobType.HISTORICAL == "HISTORICAL"

    def test_job_completion_status_enum_values(self):
        """Test all JobCompletionStatus enum values are correct."""
        assert JobCompletionStatus.COMPLETED == "completed"
        assert JobCompletionStatus.COMPLETED_WITH_FAILURES == "completed_with_failures"
        assert JobCompletionStatus.FAILED == "failed"
        assert JobCompletionStatus.PENDING == "pending"

    def test_job_status_unknown_enum_exists(self):
        """Test UNKNOWN enum value exists for JobStatus."""
        assert JobStatus.UNKNOWN == "UNKNOWN"

    def test_job_type_unknown_enum_exists(self):
        """Test UNKNOWN enum value exists for JobType."""
        assert JobType.UNKNOWN == "UNKNOWN"


class TestJobModel:
    """Tests for Job model."""

    def test_job_creation_with_required_fields(self, sample_job_response):
        """Test job model can be created with required fields."""
        job = Job(**sample_job_response)
        assert job.job_id == "job_789"
        assert job.type == JobType.INCREMENTAL
        assert job.status == JobStatus.IN_PROGRESS

    def test_job_with_statistics(self, sample_job_response):
        """Test job model with statistics field."""
        job = Job(**sample_job_response)
        assert job.statistics is not None
        assert job.statistics.rows_loaded == 1000
        assert job.statistics.rows_failed == 0

    def test_job_is_terminal_property(self, sample_completed_job_response):
        """Test is_terminal property for completed job."""
        job = Job(**sample_completed_job_response)
        assert job.is_terminal is True

    def test_job_is_not_terminal(self, sample_job_response):
        """Test is_terminal property for in-progress job."""
        job = Job(**sample_job_response)
        assert job.is_terminal is False

    def test_job_is_successful(self, sample_completed_job_response):
        """Test is_successful property for completed job."""
        job = Job(**sample_completed_job_response)
        assert job.is_successful is True

    def test_job_has_failures(self):
        """Test has_failures property."""
        job_data = {
            "job_id": "job_123",
            "pipeline_id": 123,
            "type": "INCREMENTAL",
            "status": "COMPLETED_WITH_FAILURES"
        }
        job = Job(**job_data)
        assert job.has_failures is True

    def test_job_is_failed(self, sample_failed_job_response):
        """Test is_failed property for failed job."""
        job = Job(**sample_failed_job_response)
        assert job.is_failed is True

    def test_job_with_unknown_status_from_api(self):
        """Test that unknown status values from API are handled gracefully."""
        # Simulate API returning a new status that doesn't exist in our enum
        job_data = {
            "job_id": "job_123",
            "type": "INCREMENTAL",
            "status": "NEW_STATUS_FROM_API"
        }
        job = Job(**job_data)
        # Should be converted to UNKNOWN instead of raising an error
        assert job.status == JobStatus.UNKNOWN
        assert job.job_id == "job_123"

    def test_job_with_unknown_type_from_api(self):
        """Test that unknown job type values from API are handled gracefully."""
        # Simulate API returning a new job type that doesn't exist in our enum
        job_data = {
            "job_id": "job_456",
            "type": "NEW_JOB_TYPE",
            "status": "IN_PROGRESS"
        }
        job = Job(**job_data)
        # Should be converted to UNKNOWN instead of raising an error
        assert job.type == JobType.UNKNOWN
        assert job.status == JobStatus.IN_PROGRESS

    def test_job_with_both_unknown_type_and_status(self):
        """Test that both unknown type and status are handled together."""
        job_data = {
            "job_id": "job_789",
            "type": "FUTURE_TYPE",
            "status": "FUTURE_STATUS"
        }
        job = Job(**job_data)
        assert job.type == JobType.UNKNOWN
        assert job.status == JobStatus.UNKNOWN

    def test_job_unknown_status_is_not_terminal(self):
        """Test that jobs with UNKNOWN status are not considered terminal."""
        job_data = {
            "job_id": "job_123",
            "type": "INCREMENTAL",
            "status": "UNKNOWN"
        }
        job = Job(**job_data)
        # UNKNOWN should not be in terminal states - we should keep monitoring
        assert job.is_terminal is False


class TestPaginatedJobsResponse:
    """Tests for PaginatedJobsResponse model."""

    def test_paginated_response_creation(self, sample_jobs_list_response):
        """Test paginated response can be created."""
        response = PaginatedJobsResponse(**sample_jobs_list_response)
        assert len(response.data) == 1
        assert response.has_more is False
        assert response.next_cursor is None

    def test_paginated_response_with_cursor(self, sample_job_response):
        """Test paginated response with pagination cursor."""
        data = {
            "data": [sample_job_response, sample_job_response],
            "has_more": True,
            "next_cursor": "cursor_abc123"
        }
        response = PaginatedJobsResponse(**data)
        assert len(response.data) == 2
        assert response.has_more is True
        assert response.next_cursor == "cursor_abc123"


class TestJobStatistics:
    """Tests for JobStatistics model."""

    def test_statistics_creation(self):
        """Test job statistics model creation."""
        stats = JobStatistics(
            rows_loaded=1000,
            rows_failed=10,
            bytes_transferred=50000,
            duration_seconds=300.5
        )
        assert stats.rows_loaded == 1000
        assert stats.rows_failed == 10
        assert stats.bytes_transferred == 50000
        assert stats.duration_seconds == 300.5

    def test_statistics_optional_fields(self):
        """Test statistics with all optional fields."""
        stats = JobStatistics()
        assert stats.rows_loaded is None
        assert stats.rows_failed is None


class TestJobError:
    """Tests for JobError model."""

    def test_job_error_creation(self):
        """Test job error model creation."""
        error = JobError(
            error_code="ERR_001",
            error_message="Connection timeout",
            error_details={"host": "db.example.com"}
        )
        assert error.error_code == "ERR_001"
        assert error.error_message == "Connection timeout"
        assert error.error_details["host"] == "db.example.com"
