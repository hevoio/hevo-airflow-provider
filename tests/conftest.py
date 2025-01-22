"""Pytest configuration and shared fixtures for Hevo Airflow Provider tests."""

from __future__ import annotations

import os
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
    Sample pipeline API response matching the actual Hevo API structure.

    Returns a dictionary representing a valid pipeline object matching
    https://edge-api-docs.hevodata.com/reference/get_api-v1-pipelines-pipeline-id
    """
    return {
        "id": 123,
        "name": "Test Pipeline",
        "status": "INITIALIZED",
        "source": {
            "name": "PostgreSQL Source",
            "connector_id": "src_123",
            "source_type": "PostgreSQL",
            "config": {"host": "localhost", "port": 5432, "database": "mydb"},
        },
        "destination": {
            "id": 456,
            "name": "Snowflake Destination",
            "connector_id": "dest_456",
            "destination_type": "Snowflake",
            "status": "ACTIVE",
        },
        "destination_prefix": "hevo_",
        "replication_type": "HISTORICAL_AND_INCREMENTAL",
        "load_mode": "MERGE",
        "schema_evolution": "ALLOW_ALL",
        "schedule": {"sync_type": "ON_DEMAND"},
        "failure_handling_policy": {"object_failure_level": "PIPELINE", "object_failure_threshold": 3},
        "source_schema_status": {"status": "SYNCED", "refreshed_ts": 1704067200000},
        "latency_alert": {"enabled": False, "threshold_minutes": 60},
        "created_by_email": "user@example.com",
        "updated_by_email": "user@example.com",
        "created_ts": 1704067200000,  # 2024-01-01T00:00:00Z in milliseconds
        "updated_ts": 1704067200000,
    }


def create_job_response(**overrides) -> dict:
    """
    Create a complete job response with all required fields.

    Provides sensible defaults for all required Job model fields,
    allowing tests to override specific fields as needed.

    :param overrides: Fields to override in the default response
    :returns: Complete job response dictionary
    """
    default_response = {
        "job_id": "job_default_id",
        "type": "INCREMENTAL",
        "status": "IN_PROGRESS",
        "created_ts": 1704067200000,
        "updated_ts": 1704067260000,
        "events_ingested": 1000,
        "events_loaded": 950,
        "events_failed": 50,
        "objects_success": 5,
        "objects_queued": 2,
        "objects_skipped": 0,
        "objects_failed": 0,
        "billable_events": 1000,
        "non_billable_events": 0,
        "duration": 60000,
        "min_latency": 100,
        "max_latency": 5000,
        "mean_latency": 1500,
    }
    default_response.update(overrides)
    return default_response


@pytest.fixture
def sample_job_response():
    """
    Sample job API response for an in-progress INCREMENTAL job.

    Returns a dictionary matching the actual API schema from
    https://edge-api-docs.hevodata.com/reference/get_api-v1-pipelines-pipeline-id-jobs
    """
    return {
        "job_id": "550e8400-e29b-41d4-a716-446655440001",
        "type": "INCREMENTAL",
        "status": "IN_PROGRESS",
        "created_ts": 1704067200000,  # 2024-01-01T00:00:00Z in milliseconds
        "updated_ts": 1704067260000,  # 2024-01-01T00:01:00Z in milliseconds
        "events_ingested": 1000,
        "events_loaded": 950,
        "events_failed": 50,
        "objects_success": 5,
        "objects_queued": 2,
        "objects_skipped": 0,
        "objects_failed": 0,
        "billable_events": 1000,
        "non_billable_events": 0,
        "duration": 60000,  # 60 seconds in milliseconds
        "min_latency": 100,
        "max_latency": 5000,
        "mean_latency": 1500,
    }


@pytest.fixture
def sample_completed_job_response():
    """
    Sample job API response for a completed job.

    Returns a dictionary representing a successfully completed job
    matching the actual API schema.
    """
    return {
        "job_id": "550e8400-e29b-41d4-a716-446655440002",
        "type": "INCREMENTAL",
        "status": "COMPLETED",
        "created_ts": 1704067200000,  # 2024-01-01T00:00:00Z
        "updated_ts": 1704067500000,  # 2024-01-01T00:05:00Z
        "events_ingested": 10000,
        "events_loaded": 10000,
        "events_failed": 0,
        "objects_success": 10,
        "objects_queued": 0,
        "objects_skipped": 0,
        "objects_failed": 0,
        "billable_events": 10000,
        "non_billable_events": 0,
        "duration": 300000,  # 5 minutes in milliseconds
        "min_latency": 50,
        "max_latency": 3000,
        "mean_latency": 800,
    }


@pytest.fixture
def sample_failed_job_response():
    """
    Sample job API response for a failed job.

    Returns a dictionary representing a failed job matching the actual API schema.
    """
    return {
        "job_id": "550e8400-e29b-41d4-a716-446655440003",
        "type": "INCREMENTAL",
        "status": "FAILED",
        "created_ts": 1704067200000,  # 2024-01-01T00:00:00Z
        "updated_ts": 1704067380000,  # 2024-01-01T00:03:00Z
        "events_ingested": 1000,
        "events_loaded": 500,
        "events_failed": 500,
        "objects_success": 3,
        "objects_queued": 0,
        "objects_skipped": 0,
        "objects_failed": 2,
        "billable_events": 500,
        "non_billable_events": 0,
        "duration": 180000,  # 3 minutes in milliseconds
        "min_latency": 200,
        "max_latency": 8000,
        "mean_latency": 2500,
    }


@pytest.fixture
def sample_jobs_list_response(sample_job_response):
    """
    Sample paginated jobs list API response.

    Returns a dictionary representing a page of jobs with pagination metadata
    matching the actual API schema.
    """
    return {"data": [sample_job_response], "has_more": False, "next_cursor": None}


@pytest.fixture
def sample_pipelines_list_response(sample_pipeline_response):
    """
    Sample paginated pipelines list API response.

    Returns a dictionary representing a page of pipelines with pagination metadata
    matching the actual API schema.
    """
    return {"data": [sample_pipeline_response], "has_more": False, "next_cursor": None}


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
            "schema": "public",
        },
        "status": "ACTIVE",
        "created_at": "2024-01-01T00:00:00Z",
        "updated_at": "2024-01-01T00:00:00Z",
    }


@pytest.fixture
def sample_destinations_list_response(sample_destination_response):
    """
    Sample paginated destinations list API response.

    Returns a dictionary representing a page of destinations with pagination metadata.
    """
    return {"data": [sample_destination_response], "has_more": False, "next_cursor": None, "count": 1}


@pytest.fixture
def sample_object_response():
    """
    Sample pipeline object API response.

    Returns a dictionary representing a pipeline object (table/collection)
    matching the actual API schema from https://edge-api-docs.hevodata.com/reference/get_api-v1-pipelines-pipeline-id-objects
    """
    return {
        "object_id": "550e8400-e29b-41d4-a716-446655440000",
        "source_namespace": {"k0": "users", "k1": "public", "k2": "postgres_db"},
        "destination_namespace": {"k0": "users", "k1": "hevo_schema", "k2": "data_warehouse"},
        "field_count": 5,
        "status": "ACTIVE",
        "replication_status": "REPLICATED",
        "object_name": "users",
        "destination_table_name": "users",
        "load_mode": "MERGE",
        "fields": [
            {
                "source_name": "id",
                "source_type": "INTEGER",
                "destination_name": "id",
                "destination_type": "BIGINT",
                "status": "ACTIVE",
                "primary_key": True,
            },
            {
                "source_name": "email",
                "source_type": "VARCHAR",
                "destination_name": "email",
                "destination_type": "VARCHAR",
                "status": "ACTIVE",
                "primary_key": False,
            },
            {
                "source_name": "created_at",
                "source_type": "TIMESTAMP",
                "destination_name": "created_at",
                "destination_type": "TIMESTAMP",
                "status": "ACTIVE",
                "primary_key": False,
            },
        ],
    }


@pytest.fixture
def sample_objects_list_response(sample_object_response):
    """
    Sample paginated objects list API response.

    Returns a dictionary representing a page of objects with pagination metadata.
    """
    return {"data": [sample_object_response], "has_more": False, "next_cursor": None, "count": 1}


@pytest.fixture
def sample_source_connectors_response():
    """
    Sample source connectors metadata API response.

    Returns a dictionary representing supported source connector types.
    """
    return {
        "data": [
            {"type": "MYSQL", "name": "MySQL", "category": "DATABASE", "capabilities": ["INCREMENTAL", "HISTORICAL"]},
            {
                "type": "POSTGRESQL",
                "name": "PostgreSQL",
                "category": "DATABASE",
                "capabilities": ["INCREMENTAL", "HISTORICAL"],
            },
            {"type": "SALESFORCE", "name": "Salesforce", "category": "SAAS", "capabilities": ["INCREMENTAL"]},
        ],
        "count": 3,
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
                "capabilities": ["BATCH", "STREAMING"],
            },
            {
                "type": "BIGQUERY",
                "name": "Google BigQuery",
                "category": "DATA_WAREHOUSE",
                "capabilities": ["BATCH", "STREAMING"],
            },
            {"type": "REDSHIFT", "name": "Amazon Redshift", "category": "DATA_WAREHOUSE", "capabilities": ["BATCH"]},
        ],
        "count": 3,
    }


@pytest.fixture
def sample_job_objects_response():
    """
    Sample job objects API response.

    Returns a dictionary representing objects processed in a job.
    """
    return {
        "data": [
            {"object_name": "users", "rows_processed": 1000, "rows_failed": 0, "status": "COMPLETED"},
            {"object_name": "orders", "rows_processed": 5000, "rows_failed": 10, "status": "COMPLETED_WITH_FAILURES"},
        ],
        "has_more": False,
        "next_cursor": None,
        "count": 2,
    }
