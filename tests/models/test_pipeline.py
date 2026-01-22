"""Unit tests for pipeline-related Pydantic models."""

from __future__ import annotations

import pytest

from airflow.hevo.models.pipeline import (
    PaginatedPipelinesResponse,
    Pipeline,
    PipelineConfig,
    PipelineDestination,
    PipelineSource,
    PipelineStatus,
    SyncType,
)


class TestPipelineEnums:
    """Tests for pipeline-related enums."""

    def test_pipeline_status_enum_values(self):
        """Test all PipelineStatus enum values are correct."""
        assert PipelineStatus.INITIALIZED == "INITIALIZED"
        assert PipelineStatus.PAUSED == "PAUSED"
        assert PipelineStatus.STOPPED == "STOPPED"
        assert PipelineStatus.FAILED == "FAILED"
        assert PipelineStatus.INCOMPLETE == "INCOMPLETE"

    def test_sync_type_enum_values(self):
        """Test all SyncType enum values are correct."""
        assert SyncType.ON_DEMAND == "ON_DEMAND"
        assert SyncType.SCHEDULED == "SCHEDULED"


class TestPipelineSource:
    """Tests for PipelineSource model."""

    def test_source_creation(self):
        """Test pipeline source model creation."""
        source = PipelineSource(
            source_id="src_123",
            source_name="PostgreSQL Source",
            source_type="PostgreSQL",
            connection_params={"host": "localhost", "port": 5432}
        )
        assert source.source_id == "src_123"
        assert source.source_name == "PostgreSQL Source"
        assert source.source_type == "PostgreSQL"
        assert source.connection_params["host"] == "localhost"


class TestPipelineDestination:
    """Tests for PipelineDestination model."""

    def test_destination_creation(self):
        """Test pipeline destination model creation."""
        dest = PipelineDestination(
            destination_id="dest_456",
            destination_name="Snowflake Destination",
            destination_type="Snowflake"
        )
        assert dest.destination_id == "dest_456"
        assert dest.destination_name == "Snowflake Destination"
        assert dest.destination_type == "Snowflake"


class TestPipelineConfig:
    """Tests for PipelineConfig model."""

    def test_config_creation_with_sync_type_enum(self):
        """Test config creation with SyncType enum."""
        config = PipelineConfig(
            sync_type=SyncType.ON_DEMAND,
            objects=[{"name": "users", "mode": "FULL"}]
        )
        assert config.sync_type == SyncType.ON_DEMAND
        assert len(config.objects) == 1

    def test_config_creation_with_sync_type_string(self):
        """Test config creation with sync_type as string."""
        config = PipelineConfig(
            sync_type="SCHEDULED",
            schedule={"interval": "hourly"}
        )
        assert config.sync_type == "SCHEDULED"
        assert config.schedule["interval"] == "hourly"


class TestPipeline:
    """Tests for Pipeline model."""

    def test_pipeline_creation(self, sample_pipeline_response):
        """Test pipeline creation with full data."""
        pipeline = Pipeline(**sample_pipeline_response)
        assert pipeline.id == 123
        assert pipeline.name == "Test Pipeline"
        assert pipeline.status == PipelineStatus.INITIALIZED
        assert pipeline.source.source_id == "src_123"
        assert pipeline.destination.destination_id == "dest_456"
        assert pipeline.config.sync_type == SyncType.ON_DEMAND

    def test_pipeline_with_minimal_fields(self):
        """Test pipeline with only required fields."""
        data = {
            "id": 123,
            "name": "Minimal Pipeline",
            "status": "INITIALIZED"
        }
        pipeline = Pipeline(**data)
        assert pipeline.id == 123
        assert pipeline.name == "Minimal Pipeline"
        assert pipeline.status == PipelineStatus.INITIALIZED
        assert pipeline.source is None
        assert pipeline.destination is None

    def test_pipeline_allows_extra_fields(self):
        """Test that pipeline allows extra fields."""
        data = {
            "id": 123,
            "name": "Test Pipeline",
            "status": "INITIALIZED",
            "custom_field": "custom_value"
        }
        pipeline = Pipeline(**data)
        assert pipeline.id == 123
        assert pipeline.name == "Test Pipeline"


class TestPaginatedPipelinesResponse:
    """Tests for PaginatedPipelinesResponse model."""

    def test_paginated_pipelines_response(self, sample_pipeline_response):
        """Test paginated pipelines response creation."""
        data = {
            "data": [sample_pipeline_response],
            "has_more": False,
            "next_cursor": None,
            "count": 1
        }
        response = PaginatedPipelinesResponse(**data)
        assert len(response.data) == 1
        assert response.has_more is False
        assert response.count == 1

    def test_paginated_with_cursor(self, sample_pipeline_response):
        """Test paginated response with cursor."""
        data = {
            "data": [sample_pipeline_response, sample_pipeline_response],
            "has_more": True,
            "next_cursor": "cursor_xyz"
        }
        response = PaginatedPipelinesResponse(**data)
        assert len(response.data) == 2
        assert response.has_more is True
        assert response.next_cursor == "cursor_xyz"
