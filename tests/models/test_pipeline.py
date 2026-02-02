"""Unit tests for pipeline-related Pydantic models."""

from __future__ import annotations

from airflow.hevo.models.pipeline import (
    Destination,
    FailureHandlingPolicy,
    LatencyAlert,
    LoadMode,
    PaginatedPipelinesResponse,
    Pipeline,
    PipelineStatus,
    ReplicationType,
    Schedule,
    SchemaEvolution,
    Source,
    SourceSchemaStatus,
    SyncType,
)


class TestPipelineEnums:
    """Tests for pipeline-related enums."""

    def test_pipeline_status_enum_values(self) -> None:
        """Test PipelineStatus enum values are correct."""
        assert PipelineStatus.INITIALIZED == "INITIALIZED"
        assert PipelineStatus.DISABLED == "DISABLED"
        assert PipelineStatus.ENABLED == "ENABLED"
        assert PipelineStatus.DELETED == "DELETED"

    def test_sync_type_enum_values(self) -> None:
        """Test all SyncType enum values are correct."""
        assert SyncType.ON_DEMAND == "ON_DEMAND"
        assert SyncType.SCHEDULED == "SCHEDULED"

    def test_replication_type_enum_values(self) -> None:
        """Test ReplicationType enum values are correct."""
        assert ReplicationType.HISTORICAL_AND_INCREMENTAL == "HISTORICAL_AND_INCREMENTAL"
        assert ReplicationType.INCREMENTAL_ONLY == "INCREMENTAL_ONLY"

    def test_load_mode_enum_values(self) -> None:
        """Test LoadMode enum values are correct."""
        assert LoadMode.APPEND == "APPEND"
        assert LoadMode.MERGE == "MERGE"

    def test_schema_evolution_enum_values(self) -> None:
        """Test SchemaEvolution enum values are correct."""
        assert SchemaEvolution.ALLOW_ALL == "ALLOW_ALL"
        assert SchemaEvolution.BLOCK_ALL == "BLOCK_ALL"
        assert SchemaEvolution.ALLOW_COLUMN_LEVEL == "ALLOW_COLUMN_LEVEL"


class TestPipelineSource:
    """Tests for Source model."""

    def test_source_creation(self) -> None:
        """Test pipeline source model creation."""
        source = Source(
            name="PostgreSQL Source",
            connector_id="src_123",
            source_type="PostgreSQL",
            config={"host": "localhost", "port": 5432},
        )
        assert source.name == "PostgreSQL Source"
        assert source.connector_id == "src_123"
        assert source.source_type == "PostgreSQL"
        assert source.config["host"] == "localhost"


class TestPipelineDestination:
    """Tests for Destination model."""

    def test_destination_creation(self) -> None:
        """Test pipeline destination model creation."""
        dest = Destination(
            id=456, name="Snowflake Destination", connector_id="dest_456", destination_type="Snowflake", status="ACTIVE"
        )
        assert dest.id == 456
        assert dest.name == "Snowflake Destination"
        assert dest.connector_id == "dest_456"
        assert dest.destination_type == "Snowflake"
        assert dest.status == "ACTIVE"


class TestSchedule:
    """Tests for Schedule model."""

    def test_schedule_on_demand(self) -> None:
        """Test schedule with ON_DEMAND sync type."""
        schedule = Schedule(sync_type=SyncType.ON_DEMAND)
        assert schedule.sync_type == SyncType.ON_DEMAND
        assert schedule.frequency_minutes is None

    def test_schedule_with_frequency(self) -> None:
        """Test schedule with SCHEDULED sync type and frequency."""
        schedule = Schedule(sync_type=SyncType.SCHEDULED, frequency_minutes=60)
        assert schedule.sync_type == SyncType.SCHEDULED
        assert schedule.frequency_minutes == 60


class TestFailureHandlingPolicy:
    """Tests for FailureHandlingPolicy model."""

    def test_failure_handling_policy_creation(self) -> None:
        """Test failure handling policy model creation."""
        policy = FailureHandlingPolicy(object_failure_level="PIPELINE", object_failure_threshold=3)
        assert policy.object_failure_level == "PIPELINE"
        assert policy.object_failure_threshold == 3


class TestLatencyAlert:
    """Tests for LatencyAlert model."""

    def test_latency_alert_enabled(self) -> None:
        """Test latency alert enabled."""
        alert = LatencyAlert(enabled=True, threshold_minutes=60)
        assert alert.enabled is True
        assert alert.threshold_minutes == 60

    def test_latency_alert_disabled(self) -> None:
        """Test latency alert disabled."""
        alert = LatencyAlert(enabled=False)
        assert alert.enabled is False
        assert alert.threshold_minutes is None


class TestSourceSchemaStatus:
    """Tests for SourceSchemaStatus model."""

    def test_source_schema_status_creation(self) -> None:
        """Test source schema status model creation."""
        status = SourceSchemaStatus(status="SYNCED", refreshed_ts=1704067200000)
        assert status.status == "SYNCED"
        assert status.refreshed_ts == 1704067200000


class TestPipeline:
    """Tests for Pipeline model."""

    def test_pipeline_creation(self, sample_pipeline_response) -> None:
        """Test pipeline creation with full API data."""
        pipeline = Pipeline(**sample_pipeline_response)
        assert pipeline.id == 123
        assert pipeline.name == "Test Pipeline"
        assert pipeline.status == PipelineStatus.INITIALIZED
        assert pipeline.source.name == "PostgreSQL Source"
        assert pipeline.source.connector_id == "src_123"
        assert pipeline.destination.id == 456
        assert pipeline.destination.name == "Snowflake Destination"
        assert pipeline.destination_prefix == "hevo_"
        assert pipeline.replication_type == ReplicationType.HISTORICAL_AND_INCREMENTAL
        assert pipeline.load_mode == LoadMode.MERGE
        assert pipeline.schema_evolution == SchemaEvolution.ALLOW_ALL
        assert pipeline.schedule.sync_type == SyncType.ON_DEMAND
        assert pipeline.failure_handling_policy.object_failure_threshold == 3
        assert pipeline.source_schema_status.status == "SYNCED"
        assert pipeline.created_by_email == "user@example.com"
        assert pipeline.updated_by_email == "user@example.com"
        assert pipeline.created_ts == 1704067200000
        assert pipeline.updated_ts == 1704067200000

    def test_pipeline_with_scheduled_sync(self, sample_pipeline_response) -> None:
        """Test pipeline with scheduled sync type."""
        data = sample_pipeline_response.copy()
        data["schedule"] = {"sync_type": "SCHEDULED", "frequency_minutes": 30}
        pipeline = Pipeline(**data)
        assert pipeline.schedule.sync_type == SyncType.SCHEDULED
        assert pipeline.schedule.frequency_minutes == 30

    def test_pipeline_latency_alert_optional(self, sample_pipeline_response) -> None:
        """Test that latency_alert field is optional."""
        data = sample_pipeline_response.copy()
        data.pop("latency_alert", None)
        pipeline = Pipeline(**data)
        assert pipeline.latency_alert is None

    def test_pipeline_allows_extra_fields(self, sample_pipeline_response) -> None:
        """Test that pipeline allows extra fields."""
        data = sample_pipeline_response.copy()
        data["custom_field"] = "custom_value"
        pipeline = Pipeline(**data)
        assert pipeline.id == 123
        assert pipeline.name == "Test Pipeline"

    def test_pipeline_unknown_status_from_api(self, sample_pipeline_response) -> None:
        """Test that unknown status values from API are handled gracefully."""
        data = sample_pipeline_response.copy()
        data["status"] = "NEW_STATUS_FROM_API"

        pipeline = Pipeline(**data)
        # Should be converted to UNKNOWN instead of raising an error
        assert pipeline.status == PipelineStatus.UNKNOWN
        assert pipeline.id == 123
        assert pipeline.name == "Test Pipeline"

    def test_pipeline_unknown_replication_type_from_api(self, sample_pipeline_response) -> None:
        """Test that unknown replication type values from API are handled gracefully."""
        data = sample_pipeline_response.copy()
        data["replication_type"] = "NEW_REPLICATION_TYPE_FROM_API"

        pipeline = Pipeline(**data)
        # Should be converted to UNKNOWN instead of raising an error
        assert pipeline.replication_type == ReplicationType.UNKNOWN
        assert pipeline.id == 123
        assert pipeline.name == "Test Pipeline"

    def test_pipeline_unknown_load_mode_from_api(self, sample_pipeline_response) -> None:
        """Test that unknown load mode values from API are handled gracefully."""
        data = sample_pipeline_response.copy()
        data["load_mode"] = "NEW_LOAD_MODE_FROM_API"

        pipeline = Pipeline(**data)
        # Should be converted to UNKNOWN instead of raising an error
        assert pipeline.load_mode == LoadMode.UNKNOWN
        assert pipeline.id == 123
        assert pipeline.name == "Test Pipeline"

    def test_pipeline_unknown_schema_evolution_from_api(self, sample_pipeline_response) -> None:
        """Test that unknown schema evolution values from API are handled gracefully."""
        data = sample_pipeline_response.copy()
        data["schema_evolution"] = "NEW_SCHEMA_EVOLUTION_FROM_API"

        pipeline = Pipeline(**data)
        # Should be converted to UNKNOWN instead of raising an error
        assert pipeline.schema_evolution == SchemaEvolution.UNKNOWN
        assert pipeline.id == 123
        assert pipeline.name == "Test Pipeline"


class TestPaginatedPipelinesResponse:
    """Tests for PaginatedPipelinesResponse model."""

    def test_paginated_pipelines_response(self, sample_pipeline_response) -> None:
        """Test paginated pipelines response creation."""
        data = {"data": [sample_pipeline_response], "has_more": False, "next_cursor": None}
        response = PaginatedPipelinesResponse(**data)
        assert len(response.data) == 1
        assert response.has_more is False
        assert response.next_cursor is None

    def test_paginated_with_cursor(self, sample_pipeline_response) -> None:
        """Test paginated response with cursor."""
        data = {
            "data": [sample_pipeline_response, sample_pipeline_response],
            "has_more": True,
            "next_cursor": "cursor_xyz",
        }
        response = PaginatedPipelinesResponse(**data)
        assert len(response.data) == 2
        assert response.has_more is True
        assert response.next_cursor == "cursor_xyz"

    def test_paginated_response_data_is_list_of_pipelines(self, sample_pipeline_response) -> None:
        """Test paginated response data contains Pipeline instances."""
        data = {"data": [sample_pipeline_response], "has_more": False, "next_cursor": None}
        response = PaginatedPipelinesResponse(**data)
        assert isinstance(response.data[0], Pipeline)
        assert response.data[0].id == 123
