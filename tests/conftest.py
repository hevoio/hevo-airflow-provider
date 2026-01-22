"""Pytest configuration and shared fixtures for Hevo Airflow Provider tests."""

from __future__ import annotations

import os
from datetime import datetime
from unittest.mock import MagicMock

import pytest


@pytest.fixture(scope="session", autouse=True)
def setup_test_environment():
    """
    Setup test environment variables.

    Runs once per test session to configure Airflow in test mode.
    """
    os.environ["AIRFLOW__CORE__UNIT_TEST_MODE"] = "True"
    os.environ["AIRFLOW__CORE__LOAD_EXAMPLES"] = "False"
    yield
    # Cleanup after all tests
    os.environ.pop("AIRFLOW__CORE__UNIT_TEST_MODE", None)
    os.environ.pop("AIRFLOW__CORE__LOAD_EXAMPLES", None)


@pytest.fixture
def mock_airflow_context():
    """
    Create a mock Airflow execution context.

    Provides the context dictionary passed to execute() methods in operators and sensors.
    """
    context = MagicMock()
    context.get.return_value = None
    # Add task instance for XCom operations if needed
    context["task_instance"] = MagicMock()
    context["task_instance"].xcom_pull.return_value = None
    return context


# ===================================================================
# Sample API Response Fixtures
# ===================================================================


@pytest.fixture
def sample_pipeline_response():
    """
    Sample pipeline API response matching Hevo API structure.

    Returns a dictionary representing a valid pipeline object with
    INITIALIZED status, source, destination, and configuration.
    """
    return {
        "id": 123,
        "name": "Test Pipeline",
        "status": "INITIALIZED",
        "source": {
            "source_id": "src_123",
            "source_name": "PostgreSQL Source",
            "source_type": "PostgreSQL"
        },
        "destination": {
            "destination_id": "dest_456",
            "destination_name": "Snowflake Destination",
            "destination_type": "Snowflake"
        },
        "config": {
            "sync_type": "ON_DEMAND",
            "objects": []
        },
        "created_at": "2024-01-01T00:00:00Z",
        "updated_at": "2024-01-01T00:00:00Z"
    }


@pytest.fixture
def sample_job_response():
    """
    Sample job API response for an in-progress INCREMENTAL job.

    Returns a dictionary representing an active job with statistics.
    """
    return {
        "job_id": "job_789",
        "type": "INCREMENTAL",
        "status": "IN_PROGRESS",
        "created_at": "2024-01-01T00:00:00Z",
        "started_at": "2024-01-01T00:01:00Z",
        "statistics": {
            "rows_loaded": 1000,
            "rows_failed": 0
        }
    }


@pytest.fixture
def sample_completed_job_response():
    """
    Sample job API response for a completed job.

    Returns a dictionary representing a successfully completed job
    with final statistics and completion timestamp.
    """
    return {
        "job_id": "job_completed",
        "type": "INCREMENTAL",
        "status": "COMPLETED",
        "created_at": "2024-01-01T00:00:00Z",
        "started_at": "2024-01-01T00:01:00Z",
        "completed_at": "2024-01-01T00:05:00Z",
        "statistics": {
            "rows_loaded": 1000,
            "rows_failed": 0
        }
    }


@pytest.fixture
def sample_failed_job_response():
    """Sample job API response for a failed job."""
    return {
        "job_id": "job_failed",
        "type": "INCREMENTAL",
        "status": "FAILED",
        "created_at": "2024-01-01T00:00:00Z",
        "started_at": "2024-01-01T00:01:00Z",
        "completed_at": "2024-01-01T00:03:00Z",
        "statistics": {
            "rows_loaded": 500,
            "rows_failed": 500
        }
    }


@pytest.fixture
def sample_jobs_list_response(sample_job_response):
    """
    Sample paginated jobs list API response.

    Returns a dictionary representing a page of jobs with pagination metadata.
    """
    return {
        "data": [sample_job_response],
        "has_more": False,
        "next_cursor": None,
        "count": 1
    }


@pytest.fixture
def sample_pipelines_list_response(sample_pipeline_response):
    """
    Sample paginated pipelines list API response.

    Returns a dictionary representing a page of pipelines with pagination metadata.
    """
    return {
        "data": [sample_pipeline_response],
        "has_more": False,
        "next_cursor": None,
        "count": 1
    }


@pytest.fixture
def sample_destination_response():
    """
    Sample destination API response matching Hevo API structure.

    Returns a dictionary representing a valid destination object.
    """
    return {
        "id": 456,
        "name": "Test Snowflake Destination",
        "type": "SNOWFLAKE",
        "config": {
            "account": "abc123.us-east-1",
            "database": "analytics",
            "warehouse": "COMPUTE_WH",
            "schema": "public"
        },
        "status": "ACTIVE",
        "created_at": "2024-01-01T00:00:00Z",
        "updated_at": "2024-01-01T00:00:00Z"
    }


@pytest.fixture
def sample_destinations_list_response(sample_destination_response):
    """
    Sample paginated destinations list API response.

    Returns a dictionary representing a page of destinations with pagination metadata.
    """
    return {
        "data": [sample_destination_response],
        "has_more": False,
        "next_cursor": None,
        "count": 1
    }


@pytest.fixture
def sample_object_response():
    """
    Sample pipeline object API response.

    Returns a dictionary representing a pipeline object (table/collection).
    """
    return {
        "id": "object_123",
        "name": "users",
        "type": "TABLE",
        "schema": {
            "columns": [
                {"name": "id", "type": "INTEGER"},
                {"name": "email", "type": "VARCHAR"},
                {"name": "created_at", "type": "TIMESTAMP"}
            ]
        },
        "selected": True,
        "sync_mode": "INCREMENTAL",
        "updated_at": "2024-01-01T00:00:00Z"
    }


@pytest.fixture
def sample_objects_list_response(sample_object_response):
    """
    Sample paginated objects list API response.

    Returns a dictionary representing a page of objects with pagination metadata.
    """
    return {
        "data": [sample_object_response],
        "has_more": False,
        "next_cursor": None,
        "count": 1
    }


@pytest.fixture
def sample_source_connectors_response():
    """
    Sample source connectors metadata API response.

    Returns a dictionary representing supported source connector types.
    """
    return {
        "data": [
            {
                "type": "MYSQL",
                "name": "MySQL",
                "category": "DATABASE",
                "capabilities": ["INCREMENTAL", "HISTORICAL"]
            },
            {
                "type": "POSTGRESQL",
                "name": "PostgreSQL",
                "category": "DATABASE",
                "capabilities": ["INCREMENTAL", "HISTORICAL"]
            },
            {
                "type": "SALESFORCE",
                "name": "Salesforce",
                "category": "SAAS",
                "capabilities": ["INCREMENTAL"]
            }
        ],
        "count": 3
    }


@pytest.fixture
def sample_destination_connectors_response():
    """
    Sample destination connectors metadata API response.

    Returns a dictionary representing supported destination connector types.
    """
    return {
        "data": [
            {
                "type": "SNOWFLAKE",
                "name": "Snowflake",
                "category": "DATA_WAREHOUSE",
                "capabilities": ["BATCH", "STREAMING"]
            },
            {
                "type": "BIGQUERY",
                "name": "Google BigQuery",
                "category": "DATA_WAREHOUSE",
                "capabilities": ["BATCH", "STREAMING"]
            },
            {
                "type": "REDSHIFT",
                "name": "Amazon Redshift",
                "category": "DATA_WAREHOUSE",
                "capabilities": ["BATCH"]
            }
        ],
        "count": 3
    }


@pytest.fixture
def sample_job_objects_response():
    """
    Sample job objects API response.

    Returns a dictionary representing objects processed in a job.
    """
    return {
        "data": [
            {
                "object_name": "users",
                "rows_processed": 1000,
                "rows_failed": 0,
                "status": "COMPLETED"
            },
            {
                "object_name": "orders",
                "rows_processed": 5000,
                "rows_failed": 10,
                "status": "COMPLETED_WITH_FAILURES"
            }
        ],
        "has_more": False,
        "next_cursor": None,
        "count": 2
    }
