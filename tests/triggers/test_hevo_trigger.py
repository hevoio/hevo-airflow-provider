"""Unit tests for HevoTrigger."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from airflow.exceptions import AirflowException
from airflow.triggers.base import TriggerEvent

from airflow.hevo.models.job import Job, JobCompletionStatus, JobType
from airflow.hevo.trigger import HevoTrigger
from tests.conftest import create_job_response


class TestHevoTriggerInit:
    """Tests for HevoTrigger initialization."""

    def test_trigger_initialization_with_defaults(self) -> None:
        """Test trigger initializes with default parameters."""
        trigger = HevoTrigger(pipeline_id=123, job_id="job_123", connection_id="hevo_test_connection")
        assert trigger.pipeline_id == 123
        assert trigger.job_id == "job_123"
        assert trigger.job_type == JobType.INCREMENTAL.value
        assert trigger.poke_interval == 15
        assert trigger.accept_completed_with_failures is False
        assert trigger.connection_id == "hevo_test_connection"

    def test_trigger_initialization_with_custom_params(self) -> None:
        """Test trigger initializes with all custom parameters."""
        trigger = HevoTrigger(
            pipeline_id=456,
            job_id="job_custom",
            job_type=JobType.HISTORICAL,
            poke_interval=10,
            accept_completed_with_failures=True,
            connection_id="custom_conn",
        )
        assert trigger.pipeline_id == 456
        assert trigger.job_id == "job_custom"
        assert trigger.job_type == JobType.HISTORICAL.value
        assert trigger.poke_interval == 10
        assert trigger.accept_completed_with_failures is True
        assert trigger.connection_id == "custom_conn"

    def test_trigger_initialization_without_job_id(self) -> None:
        """Test trigger initialization for auto-discovery mode."""
        trigger = HevoTrigger(
            pipeline_id=123, job_id=None, job_type=JobType.INCREMENTAL, connection_id="hevo_test_connection"
        )
        assert trigger.job_id is None
        assert trigger.job_type == JobType.INCREMENTAL.value


class TestHevoTriggerSerialize:
    """Tests for serialize method."""

    def test_serialize_with_all_params(self) -> None:
        """Test serialization includes all parameters."""
        trigger = HevoTrigger(
            pipeline_id=123,
            job_id="job_123",
            job_type=JobType.HISTORICAL,
            poke_interval=10,
            accept_completed_with_failures=True,
            connection_id="test_conn",
        )

        class_path, params = trigger.serialize()

        assert class_path == "airflow.hevo.trigger.HevoTrigger"
        assert params["pipeline_id"] == 123
        assert params["job_id"] == "job_123"
        assert params["job_type"] == JobType.HISTORICAL.value
        assert params["poke_interval"] == 10
        assert params["accept_completed_with_failures"] is True
        assert params["connection_id"] == "test_conn"

    def test_serialize_with_minimal_params(self) -> None:
        """Test serialization with minimal parameters."""
        trigger = HevoTrigger(pipeline_id=456, job_id=None, connection_id="test_conn")

        class_path, params = trigger.serialize()

        assert class_path == "airflow.hevo.trigger.HevoTrigger"
        assert params["pipeline_id"] == 456
        assert params["job_id"] is None
        assert params["connection_id"] == "test_conn"

    def test_serialization_round_trip(self) -> None:
        """Test that serialized params can reconstruct trigger."""
        trigger1 = HevoTrigger(
            pipeline_id=123, job_id="job_123", job_type=JobType.INCREMENTAL, poke_interval=7, connection_id="test_conn"
        )

        class_path, params = trigger1.serialize()

        # Reconstruct trigger (simulates Airflow deserialization)
        trigger2 = HevoTrigger(**params)

        assert trigger2.pipeline_id == trigger1.pipeline_id
        assert trigger2.job_id == trigger1.job_id
        assert trigger2.job_type == trigger1.job_type
        assert trigger2.poke_interval == trigger1.poke_interval


@patch("airflow.hevo.trigger.HevoPipelineHook")
class TestHevoTriggerRunCompleted:
    """Tests for run method with completed status."""

    @pytest.mark.asyncio
    async def test_run_with_explicit_job_id_completed(self, mock_hook_class) -> None:
        """Test run with explicit job_id that completes immediately."""
        mock_hook = MagicMock()
        mock_hook_class.return_value = mock_hook
        mock_hook.get_job_completion_status_async = AsyncMock(return_value=JobCompletionStatus.COMPLETED)

        trigger = HevoTrigger(pipeline_id=123, job_id="job_123", poke_interval=1, connection_id="connection_id")

        events = []
        async for event in trigger.run():
            events.append(event)

        assert len(events) == 1
        event = events[0]
        assert isinstance(event, TriggerEvent)
        assert event.payload["status"] == "success"
        assert event.payload["pipeline_id"] == 123
        assert event.payload["job_id"] == "job_123"
        assert "synced successfully" in event.payload["message"]
        assert "completed_with_failures" not in event.payload

    @pytest.mark.asyncio
    async def test_run_pending_then_completed(self, mock_hook_class) -> None:
        """Test run with pending status then completed."""
        mock_hook = MagicMock()
        mock_hook_class.return_value = mock_hook
        # First two calls: pending, third: completed
        mock_hook.get_job_completion_status_async = AsyncMock(
            side_effect=[JobCompletionStatus.PENDING, JobCompletionStatus.PENDING, JobCompletionStatus.COMPLETED]
        )

        trigger = HevoTrigger(pipeline_id=123, job_id="job_123", poke_interval=1, connection_id="hevo_test_connection")

        events = []
        async for event in trigger.run():
            events.append(event)

        assert len(events) == 1
        assert events[0].payload["status"] == "success"
        assert mock_hook.get_job_completion_status_async.call_count == 3


@patch("airflow.hevo.trigger.HevoPipelineHook")
class TestHevoTriggerRunCompletedWithFailures:
    """Tests for run method with completed_with_failures status."""

    @pytest.mark.asyncio
    async def test_run_completed_with_failures_accepted(self, mock_hook_class) -> None:
        """Test run with completed_with_failures when accepting failures."""
        mock_hook = MagicMock()
        mock_hook_class.return_value = mock_hook
        mock_hook.get_job_completion_status_async = AsyncMock(return_value=JobCompletionStatus.COMPLETED_WITH_FAILURES)

        trigger = HevoTrigger(
            pipeline_id=123,
            job_id="job_123",
            accept_completed_with_failures=True,
            poke_interval=1,
            connection_id="hevo_test_connection",
        )

        events = []
        async for event in trigger.run():
            events.append(event)

        assert len(events) == 1
        event = events[0]
        assert event.payload["status"] == "success"
        assert event.payload["completed_with_failures"] is True
        assert "completed with failures" in event.payload["message"]

    @pytest.mark.asyncio
    async def test_run_completed_with_failures_not_accepted(self, mock_hook_class) -> None:
        """Test run with completed_with_failures when not accepting failures."""
        mock_hook = MagicMock()
        mock_hook_class.return_value = mock_hook
        # When not accepting failures, hook returns FAILED
        mock_hook.get_job_completion_status_async = AsyncMock(return_value=JobCompletionStatus.FAILED)

        trigger = HevoTrigger(
            pipeline_id=123,
            job_id="job_123",
            accept_completed_with_failures=False,
            poke_interval=1,
            connection_id="hevo_test_connection",
        )

        events = []
        async for event in trigger.run():
            events.append(event)

        assert len(events) == 1
        event = events[0]
        assert event.payload["status"] == "error"
        assert "failed" in event.payload["message"]


@patch("airflow.hevo.trigger.HevoPipelineHook")
class TestHevoTriggerRunFailed:
    """Tests for run method with failed status."""

    @pytest.mark.asyncio
    async def test_run_job_failed(self, mock_hook_class) -> None:
        """Test run with failed job status."""
        mock_hook = MagicMock()
        mock_hook_class.return_value = mock_hook
        mock_hook.get_job_completion_status_async = AsyncMock(return_value=JobCompletionStatus.FAILED)

        trigger = HevoTrigger(pipeline_id=123, job_id="job_123", poke_interval=1, connection_id="hevo_test_connection")

        events = []
        async for event in trigger.run():
            events.append(event)

        assert len(events) == 1
        event = events[0]
        assert event.payload["status"] == "error"
        assert event.payload["pipeline_id"] == 123
        assert event.payload["job_id"] == "job_123"
        assert "failed" in event.payload["message"]

    @pytest.mark.asyncio
    async def test_run_pending_then_failed(self, mock_hook_class) -> None:
        """Test run with pending status then failed."""
        mock_hook = MagicMock()
        mock_hook_class.return_value = mock_hook
        mock_hook.get_job_completion_status_async = AsyncMock(
            side_effect=[JobCompletionStatus.PENDING, JobCompletionStatus.FAILED]
        )

        trigger = HevoTrigger(pipeline_id=123, job_id="job_123", poke_interval=1, connection_id="hevo_test_connection")

        events = []
        async for event in trigger.run():
            events.append(event)

        assert len(events) == 1
        assert events[0].payload["status"] == "error"
        assert mock_hook.get_job_completion_status_async.call_count == 2


@patch("airflow.hevo.trigger.HevoPipelineHook")
class TestHevoTriggerRunAutoDiscovery:
    """Tests for run method with auto-discovery."""

    @pytest.mark.asyncio
    async def test_run_auto_discovery_success(self, mock_hook_class, sample_job_response) -> None:
        """Test run with auto-discovery that finds job."""
        mock_hook = MagicMock()
        mock_hook_class.return_value = mock_hook
        # Auto-discovery returns job
        mock_hook.find_active_job_by_type_async = AsyncMock(return_value=Job(**sample_job_response))
        # Job completes immediately
        mock_hook.get_job_completion_status_async = AsyncMock(return_value=JobCompletionStatus.COMPLETED)

        trigger = HevoTrigger(
            pipeline_id=123,
            job_id=None,  # Auto-discovery mode
            job_type=JobType.INCREMENTAL,
            poke_interval=1,
            connection_id="hevo_test_connection",
        )

        events = []
        async for event in trigger.run():
            events.append(event)

        assert len(events) == 1
        assert events[0].payload["status"] == "success"
        assert events[0].payload["job_id"] == sample_job_response["job_id"]  # From sample_job_response
        # Verify auto-discovery was called
        mock_hook.find_active_job_by_type_async.assert_called_once()

    @pytest.mark.asyncio
    async def test_run_auto_discovery_failure(self, mock_hook_class) -> None:
        """Test run with auto-discovery that fails to find job."""
        mock_hook = MagicMock()
        mock_hook_class.return_value = mock_hook
        # Auto-discovery raises exception
        mock_hook.find_active_job_by_type_async = AsyncMock(side_effect=AirflowException("No active job found"))

        trigger = HevoTrigger(pipeline_id=123, job_id=None, poke_interval=1, connection_id="hevo_test_connection")

        events = []
        async for event in trigger.run():
            events.append(event)

        assert len(events) == 1
        event = events[0]
        assert event.payload["status"] == "error"
        assert "No active job found" in event.payload["message"]

    @pytest.mark.asyncio
    async def test_run_auto_discovery_historical_job(self, mock_hook_class) -> None:
        """Test run with auto-discovery for HISTORICAL job type."""
        mock_hook = MagicMock()
        mock_hook_class.return_value = mock_hook

        historical_job = create_job_response(job_id="job_hist_456", type="HISTORICAL", status="IN_PROGRESS")
        mock_hook.find_active_job_by_type_async = AsyncMock(return_value=Job(**historical_job))
        mock_hook.get_job_completion_status_async = AsyncMock(return_value=JobCompletionStatus.COMPLETED)

        trigger = HevoTrigger(
            pipeline_id=123,
            job_id=None,
            job_type=JobType.HISTORICAL,
            poke_interval=1,
            connection_id="hevo_test_connection",
        )

        events = []
        async for event in trigger.run():
            events.append(event)

        assert len(events) == 1
        assert events[0].payload["job_id"] == "job_hist_456"


@patch("airflow.hevo.trigger.HevoPipelineHook")
class TestHevoTriggerRunExceptionHandling:
    """Tests for run method exception handling."""

    @pytest.mark.asyncio
    async def test_run_airflow_exception_during_polling(self, mock_hook_class) -> None:
        """Test run handles AirflowException during polling."""
        mock_hook = MagicMock()
        mock_hook_class.return_value = mock_hook
        # Status check raises AirflowException
        mock_hook.get_job_completion_status_async = AsyncMock(side_effect=AirflowException("API connection error"))

        trigger = HevoTrigger(pipeline_id=123, job_id="job_123", poke_interval=1, connection_id="hevo_test_connection")

        events = []
        async for event in trigger.run():
            events.append(event)

        assert len(events) == 1
        event = events[0]
        assert event.payload["status"] == "error"
        assert "API connection error" in event.payload["message"]

    @pytest.mark.asyncio
    async def test_run_unexpected_exception(self, mock_hook_class) -> None:
        """Test run handles unexpected exceptions."""
        mock_hook = MagicMock()
        mock_hook_class.return_value = mock_hook
        # Status check raises unexpected exception
        mock_hook.get_job_completion_status_async = AsyncMock(side_effect=RuntimeError("Unexpected error"))

        trigger = HevoTrigger(pipeline_id=123, job_id="job_123", poke_interval=1, connection_id="hevo_test_connection")

        events = []
        async for event in trigger.run():
            events.append(event)

        assert len(events) == 1
        event = events[0]
        assert event.payload["status"] == "error"
        assert "Unexpected error" in event.payload["message"]

    @pytest.mark.asyncio
    async def test_run_exception_includes_context(self, mock_hook_class) -> None:
        """Test exception events include pipeline_id and job_id context."""
        mock_hook = MagicMock()
        mock_hook_class.return_value = mock_hook
        mock_hook.get_job_completion_status_async = AsyncMock(side_effect=AirflowException("Test error"))

        trigger = HevoTrigger(
            pipeline_id=999, job_id="job_error", poke_interval=1, connection_id="hevo_test_connection"
        )

        events = []
        async for event in trigger.run():
            events.append(event)

        assert len(events) == 1
        event = events[0]
        assert event.payload["pipeline_id"] == 999
        assert event.payload["job_id"] == "job_error"


@patch("airflow.hevo.trigger.HevoPipelineHook")
class TestHevoTriggerHookProperty:
    """Tests for hook cached property."""

    def test_hook_property_creates_hook_instance(self, mock_hook_class) -> None:
        """Test hook property creates HevoPipelineHook with correct parameters."""
        trigger = HevoTrigger(pipeline_id=123, job_id="job_123", connection_id="test_conn")
        _ = trigger.hook  # Access hook property to trigger creation

        mock_hook_class.assert_called_once_with(connection_id="test_conn")


@patch("airflow.hevo.trigger.HevoPipelineHook")
class TestHevoTriggerIntegrationScenarios:
    """Integration-style tests for common trigger usage patterns."""

    @pytest.mark.asyncio
    async def test_trigger_full_workflow_auto_discovery_to_completion(
        self, mock_hook_class, sample_job_response
    ) -> None:
        """Test full trigger workflow: auto-discovery -> polling -> completion."""
        mock_hook = MagicMock()
        mock_hook_class.return_value = mock_hook
        # Auto-discover job
        mock_hook.find_active_job_by_type_async = AsyncMock(return_value=Job(**sample_job_response))

        # Poll: pending -> pending -> completed
        mock_hook.get_job_completion_status_async = AsyncMock(
            side_effect=[JobCompletionStatus.PENDING, JobCompletionStatus.PENDING, JobCompletionStatus.COMPLETED]
        )

        trigger = HevoTrigger(pipeline_id=123, job_id=None, poke_interval=1, connection_id="hevo_test_connection")

        events = []
        async for event in trigger.run():
            events.append(event)

        assert len(events) == 1
        assert events[0].payload["status"] == "success"
        assert events[0].payload["job_id"] == sample_job_response["job_id"]

        # Verify auto-discovery was called
        mock_hook.find_active_job_by_type_async.assert_called_once()
        # Verify polling happened 3 times
        assert mock_hook.get_job_completion_status_async.call_count == 3

    @pytest.mark.asyncio
    async def test_trigger_multiple_pending_polls_then_success(self, mock_hook_class) -> None:
        """Test trigger with multiple pending polls before success."""
        mock_hook = MagicMock()
        mock_hook_class.return_value = mock_hook
        # Simulate long-running job with 5 pending polls
        mock_hook.get_job_completion_status_async = AsyncMock(
            side_effect=[
                JobCompletionStatus.PENDING,
                JobCompletionStatus.PENDING,
                JobCompletionStatus.PENDING,
                JobCompletionStatus.PENDING,
                JobCompletionStatus.PENDING,
                JobCompletionStatus.COMPLETED,
            ]
        )

        trigger = HevoTrigger(
            pipeline_id=123, job_id="job_long_running", poke_interval=1, connection_id="hevo_test_connection"
        )

        events = []
        async for event in trigger.run():
            events.append(event)

        assert len(events) == 1
        assert events[0].payload["status"] == "success"
        assert mock_hook.get_job_completion_status_async.call_count == 6

    @pytest.mark.asyncio
    async def test_trigger_with_all_custom_params(self, mock_hook_class, sample_job_response) -> None:
        """Test trigger with all custom parameters."""
        mock_hook = MagicMock()
        mock_hook_class.return_value = mock_hook

        historical_job = dict(sample_job_response)
        historical_job["type"] = "HISTORICAL"
        historical_job["job_id"] = "job_custom_999"

        mock_hook.find_active_job_by_type_async = AsyncMock(return_value=Job(**historical_job))
        mock_hook.get_job_completion_status_async = AsyncMock(return_value=JobCompletionStatus.COMPLETED_WITH_FAILURES)

        trigger = HevoTrigger(
            pipeline_id=999,
            job_id=None,
            job_type=JobType.HISTORICAL,
            poke_interval=1,
            accept_completed_with_failures=True,
            connection_id="prod_conn",
        )

        events = []
        async for event in trigger.run():
            events.append(event)

        assert len(events) == 1
        event = events[0]
        assert event.payload["status"] == "success"
        assert event.payload["completed_with_failures"] is True
        assert event.payload["pipeline_id"] == 999
        assert event.payload["job_id"] == "job_custom_999"

    @pytest.mark.asyncio
    async def test_trigger_poke_interval_respected(self, mock_hook_class) -> None:
        """Test that poke_interval is respected between polls."""
        mock_hook = MagicMock()
        mock_hook_class.return_value = mock_hook
        mock_hook.get_job_completion_status_async = AsyncMock(
            side_effect=[JobCompletionStatus.PENDING, JobCompletionStatus.COMPLETED]
        )

        trigger = HevoTrigger(
            pipeline_id=123,
            job_id="job_123",
            poke_interval=5,  # 5s between polls
            connection_id="hevo_test_connection",
        )

        import time

        start_time = time.time()

        events = []
        async for event in trigger.run():
            events.append(event)

        elapsed_time = time.time() - start_time

        # Should take at least 500ms due to poke_interval
        assert elapsed_time >= 0.5
        assert len(events) == 1


@patch("airflow.hevo.trigger.HevoPipelineHook")
class TestHevoTriggerEventPayloads:
    """Tests for TriggerEvent payload structure."""

    @pytest.mark.asyncio
    async def test_success_event_payload_structure(self, mock_hook_class) -> None:
        """Test success event has correct payload structure."""
        mock_hook = MagicMock()
        mock_hook_class.return_value = mock_hook
        mock_hook.get_job_completion_status_async = AsyncMock(return_value=JobCompletionStatus.COMPLETED)

        trigger = HevoTrigger(pipeline_id=123, job_id="job_123", connection_id="hevo_test_connection")

        events = []
        async for event in trigger.run():
            events.append(event)

        payload = events[0].payload
        assert "status" in payload
        assert "message" in payload
        assert "pipeline_id" in payload
        assert "job_id" in payload
        assert payload["status"] == "success"

    @pytest.mark.asyncio
    async def test_error_event_payload_structure(self, mock_hook_class) -> None:
        """Test error event has correct payload structure."""
        mock_hook = MagicMock()
        mock_hook_class.return_value = mock_hook
        mock_hook.get_job_completion_status_async = AsyncMock(return_value=JobCompletionStatus.FAILED)

        trigger = HevoTrigger(pipeline_id=123, job_id="job_123", connection_id="hevo_test_connection")

        events = []
        async for event in trigger.run():
            events.append(event)

        payload = events[0].payload
        assert "status" in payload
        assert "message" in payload
        assert "pipeline_id" in payload
        assert "job_id" in payload
        assert payload["status"] == "error"

    @pytest.mark.asyncio
    async def test_completed_with_failures_includes_flag(self, mock_hook_class) -> None:
        """Test completed_with_failures event includes flag in payload."""
        mock_hook = MagicMock()
        mock_hook_class.return_value = mock_hook
        mock_hook.get_job_completion_status_async = AsyncMock(return_value=JobCompletionStatus.COMPLETED_WITH_FAILURES)

        trigger = HevoTrigger(
            pipeline_id=123, job_id="job_123", accept_completed_with_failures=True, connection_id="hevo_test_connection"
        )

        events = []
        async for event in trigger.run():
            events.append(event)

        payload = events[0].payload
        assert "completed_with_failures" in payload
        assert payload["completed_with_failures"] is True
