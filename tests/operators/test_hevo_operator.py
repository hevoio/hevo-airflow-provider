"""Unit tests for HevoOperator."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from airflow.exceptions import AirflowException, TaskDeferred

from airflow.hevo.models.job import Job, JobCompletionStatus, JobType
from airflow.hevo.models.pipeline import Pipeline, PipelineAction, PipelineStatus, ResyncMode
from airflow.hevo.operators import HevoPipelineOperator


class TestHevoOperatorInit:
    """Tests for HevoOperator initialization and configuration."""

    def test_operator_initialization_with_defaults(self) -> None:
        """Test operator initializes with default parameters."""
        op = HevoPipelineOperator(task_id="test_task", pipeline_id=123, connection_id="test_conn")
        assert op.pipeline_id == 123
        assert op.action == PipelineAction.SYNC_NOW
        assert op.job_type == JobType.INCREMENTAL
        assert op.connection_id == "test_conn"
        assert op.poll_interval == 15
        assert op.retry_limit == 10
        assert op.deferrable is True
        assert op.wait_for_completion is True
        assert op.accept_completed_with_failures is False

    def test_operator_initialization_with_custom_params(self) -> None:
        """Test operator initializes with all custom parameters."""
        op = HevoPipelineOperator(
            task_id="test_task",
            pipeline_id=456,
            job_type=JobType.HISTORICAL,
            connection_id="custom_conn",
            poll_interval=10,
            retry_limit=5,
            deferrable=False,
            wait_for_completion=False,
            accept_completed_with_failures=True,
        )
        assert op.pipeline_id == 456
        assert op.job_type == JobType.HISTORICAL
        assert op.connection_id == "custom_conn"
        assert op.poll_interval == 10
        assert op.retry_limit == 5
        assert op.deferrable is False
        assert op.wait_for_completion is False
        assert op.accept_completed_with_failures is True

    def test_template_fields_defined(self) -> None:
        """Test template_fields attribute is correctly defined."""
        assert hasattr(HevoPipelineOperator, "template_fields")
        assert "pipeline_id" in HevoPipelineOperator.template_fields

    def test_operator_initialization_with_resync_action(self) -> None:
        """Test operator initializes with RESYNC action."""
        op = HevoPipelineOperator(
            task_id="test_task",
            pipeline_id=123,
            connection_id="test_conn",
            action=PipelineAction.RESYNC,
        )
        assert op.pipeline_id == 123
        assert op.action == PipelineAction.RESYNC
        assert op.deferrable is True
        assert op.wait_for_completion is True

    def test_operator_initialization_with_sync_now_action(self) -> None:
        """Test operator initializes with explicit SYNC_NOW action."""
        op = HevoPipelineOperator(
            task_id="test_task",
            pipeline_id=123,
            connection_id="test_conn",
            action=PipelineAction.SYNC_NOW,
        )
        assert op.pipeline_id == 123
        assert op.action == PipelineAction.SYNC_NOW


@patch("airflow.hevo.operators.HevoPipelineHook")
class TestHevoOperatorExecuteFireAndForget:
    """Tests for execute method in fire-and-forget mode (wait_for_completion=False)."""

    def test_execute_returns_job_id_without_waiting(
        self, mock_hook_class, mock_airflow_context, sample_job_response
    ) -> None:
        """Test execute returns job_id immediately when wait_for_completion=False."""
        # Setup mock hook instance
        mock_hook = MagicMock()
        mock_hook_class.return_value = mock_hook
        mock_hook.validate_pipeline.return_value = None
        mock_hook.trigger_pipeline_sync.return_value = None
        mock_hook.find_active_job_by_type_sync.return_value = Job(**sample_job_response)

        # Create and execute operator
        op = HevoPipelineOperator(
            task_id="test_task", connection_id="test_conn", pipeline_id=123, wait_for_completion=False
        )
        result = op.execute(mock_airflow_context)

        # Assertions
        assert result == "550e8400-e29b-41d4-a716-446655440001"
        mock_hook.validate_pipeline.assert_called_once_with(123)
        mock_hook.trigger_pipeline_sync.assert_called_once_with(123, True)
        mock_hook.find_active_job_by_type_sync.assert_called()

    def test_execute_raises_when_no_active_job_found(self, mock_hook_class, mock_airflow_context) -> None:
        """Test execute raises AirflowException when no active job found after retries."""
        mock_hook = MagicMock()
        mock_hook_class.return_value = mock_hook
        mock_hook.validate_pipeline.return_value = None
        mock_hook.trigger_pipeline_sync.return_value = None
        # Hook raises exception when no job found (realistic behavior)
        mock_hook.find_active_job_by_type_sync.side_effect = AirflowException(
            "Pipeline 123 doesn't have any active jobs of type INCREMENTAL"
        )

        op = HevoPipelineOperator(
            task_id="test_task", connection_id="test_conn", pipeline_id=123, wait_for_completion=False, retry_limit=2
        )

        with patch("airflow.hevo.operators.sleep"):
            with pytest.raises(AirflowException, match="No active INCREMENTAL job found"):
                op.execute(mock_airflow_context)

            # Verify it retried
            assert mock_hook.find_active_job_by_type_sync.call_count == 2

    def test_execute_retries_and_succeeds_finding_job(
        self, mock_hook_class, mock_airflow_context, sample_job_response
    ) -> None:
        """Test execute retries when job not found initially, then succeeds."""
        mock_hook = MagicMock()
        mock_hook_class.return_value = mock_hook
        mock_hook.validate_pipeline.return_value = None
        mock_hook.trigger_pipeline_sync.return_value = None

        # First two attempts: no job found (exception)
        # Third attempt: job found
        mock_hook.find_active_job_by_type_sync.side_effect = [
            AirflowException("No active jobs found"),
            AirflowException("No active jobs found"),
            Job(**sample_job_response),
        ]

        op = HevoPipelineOperator(
            task_id="test_task", connection_id="test_conn", pipeline_id=123, wait_for_completion=False
        )

        with patch("airflow.hevo.operators.sleep"):
            result = op.execute(mock_airflow_context)

        # Should succeed after retries
        assert result == "550e8400-e29b-41d4-a716-446655440001"
        # Verify it tried 3 times before succeeding
        assert mock_hook.find_active_job_by_type_sync.call_count == 3


@patch("airflow.hevo.operators.HevoPipelineHook")
class TestHevoOperatorExecuteDeferrable:
    """Tests for execute method in deferrable mode."""

    def test_execute_defers_to_trigger(self, mock_hook_class, mock_airflow_context, sample_job_response) -> None:
        """Test execute defers to HevoTrigger when deferrable=True."""
        mock_hook = MagicMock()
        mock_hook_class.return_value = mock_hook
        mock_hook.validate_pipeline.return_value = None
        mock_hook.trigger_pipeline_sync.return_value = None
        mock_hook.find_active_job_by_type_sync.return_value = Job(**sample_job_response)

        op = HevoPipelineOperator(
            task_id="test_task", connection_id="test_conn", pipeline_id=123, deferrable=True, wait_for_completion=True
        )

        with pytest.raises(TaskDeferred) as exc_info:
            op.execute(mock_airflow_context)

        # Verify TaskDeferred exception contains correct trigger
        deferred = exc_info.value
        assert deferred.trigger.pipeline_id == 123
        assert deferred.trigger.job_id == "550e8400-e29b-41d4-a716-446655440001"
        assert deferred.trigger.job_type == JobType.INCREMENTAL.value
        assert deferred.method_name == "execute_complete"

    def test_execute_defers_with_custom_params(self, mock_hook_class, mock_airflow_context) -> None:
        """Test execute defers with custom parameters passed to trigger."""
        mock_hook = MagicMock()
        mock_hook_class.return_value = mock_hook
        mock_hook.validate_pipeline.return_value = None
        mock_hook.trigger_pipeline_sync.return_value = None

        # Create a mock job with just the job_id attribute
        mock_job = MagicMock()
        mock_job.job_id = "job_hist_123"
        mock_hook.find_active_job_by_type_sync.return_value = mock_job

        op = HevoPipelineOperator(
            task_id="test_task",
            pipeline_id=456,
            job_type=JobType.HISTORICAL,
            poll_interval=10,
            accept_completed_with_failures=True,
            connection_id="custom_conn",
            deferrable=True,
            wait_for_completion=True,
        )

        with pytest.raises(TaskDeferred) as exc_info:
            op.execute(mock_airflow_context)

        trigger = exc_info.value.trigger
        assert trigger.pipeline_id == 456
        assert trigger.job_id == "job_hist_123"
        assert trigger.job_type == JobType.HISTORICAL.value
        assert trigger.poke_interval == 10
        assert trigger.accept_completed_with_failures is True
        assert trigger.connection_id == "custom_conn"


@patch("airflow.hevo.operators.HevoPipelineHook")
@patch("airflow.hevo.operators.sleep")
class TestHevoOperatorExecuteSynchronous:
    """Tests for execute method in synchronous wait mode."""

    def test_execute_waits_synchronously(
        self, mock_sleep, mock_hook_class, mock_airflow_context, sample_job_response
    ) -> None:
        """Test execute waits synchronously when deferrable=False."""
        mock_hook = MagicMock()
        mock_hook_class.return_value = mock_hook
        mock_hook.validate_pipeline.return_value = None
        mock_hook.trigger_pipeline_sync.return_value = None
        mock_hook.find_active_job_by_type_sync.return_value = Job(**sample_job_response)
        mock_hook.get_job_completion_status_sync.return_value = JobCompletionStatus.COMPLETED

        op = HevoPipelineOperator(
            task_id="test_task", connection_id="test_conn", pipeline_id=123, deferrable=False, wait_for_completion=True
        )

        result = op.execute(mock_airflow_context)

        assert result == "550e8400-e29b-41d4-a716-446655440001"
        mock_hook.get_job_completion_status_sync.assert_called_once()


class TestHevoOperatorExecuteComplete:
    """Tests for execute_complete callback method."""

    def test_execute_complete_success(self, mock_airflow_context) -> None:
        """Test execute_complete processes success event correctly."""
        op = HevoPipelineOperator(task_id="test_task", connection_id="test_conn", pipeline_id=123)

        event = {
            "status": "success",
            "message": "Job completed successfully",
            "job_id": "550e8400-e29b-41d4-a716-446655440001",
        }

        # Should not raise
        op.execute_complete(mock_airflow_context, event)

    def test_execute_complete_success_with_failures(self, mock_airflow_context) -> None:
        """Test execute_complete handles completed_with_failures flag."""
        op = HevoPipelineOperator(task_id="test_task", connection_id="test_conn", pipeline_id=123)

        event = {
            "status": "success",
            "message": "Job completed with failures",
            "job_id": "550e8400-e29b-41d4-a716-446655440001",
            "completed_with_failures": True,
        }

        # Should not raise, just log warning
        op.execute_complete(mock_airflow_context, event)

    def test_execute_complete_error(self, mock_airflow_context) -> None:
        """Test execute_complete raises on error event."""
        op = HevoPipelineOperator(task_id="test_task", connection_id="test_conn", pipeline_id=123)

        event = {"status": "error", "message": "Job failed", "job_id": "550e8400-e29b-41d4-a716-446655440001"}

        with pytest.raises(AirflowException, match="Job 550e8400-e29b-41d4-a716-446655440001 failed"):
            op.execute_complete(mock_airflow_context, event)

    def test_execute_complete_none_event(self, mock_airflow_context) -> None:
        """Test execute_complete raises when event is None."""
        op = HevoPipelineOperator(task_id="test_task", connection_id="test_conn", pipeline_id=123)

        with pytest.raises(AirflowException, match="Trigger event is None"):
            op.execute_complete(mock_airflow_context, None)

    def test_execute_complete_unexpected_status(self, mock_airflow_context) -> None:
        """Test execute_complete raises on unexpected status."""
        op = HevoPipelineOperator(task_id="test_task", connection_id="test_conn", pipeline_id=123)

        event = {"status": "unknown", "message": "Unknown status", "job_id": "550e8400-e29b-41d4-a716-446655440001"}

        with pytest.raises(AirflowException, match="Unexpected trigger event status"):
            op.execute_complete(mock_airflow_context, event)


@patch("airflow.hevo.operators.HevoPipelineHook")
@patch("airflow.hevo.operators.sleep")
class TestHevoOperatorWaitSynchronously:
    """Tests for _wait_synchronously private method."""

    def test_wait_completes_immediately(self, mock_sleep, mock_hook_class) -> None:
        """Test synchronous wait when job completes immediately."""
        mock_hook = MagicMock()
        mock_hook_class.return_value = mock_hook
        mock_hook.get_job_completion_status_sync.return_value = JobCompletionStatus.COMPLETED

        op = HevoPipelineOperator(task_id="test_task", connection_id="test_conn", pipeline_id=123, poll_interval=1)
        op._wait_synchronously("job_123")

        mock_hook.get_job_completion_status_sync.assert_called_once()
        mock_sleep.assert_not_called()

    def test_wait_polls_until_completion(self, mock_sleep, mock_hook_class) -> None:
        """Test synchronous wait polls multiple times until completion."""
        mock_hook = MagicMock()
        mock_hook_class.return_value = mock_hook
        mock_hook.get_job_completion_status_sync.side_effect = [
            JobCompletionStatus.PENDING,
            JobCompletionStatus.PENDING,
            JobCompletionStatus.COMPLETED,
        ]

        op = HevoPipelineOperator(task_id="test_task", connection_id="test_conn", pipeline_id=123, poll_interval=5)
        op._wait_synchronously("job_123")

        assert mock_hook.get_job_completion_status_sync.call_count == 3
        assert mock_sleep.call_count == 2
        mock_sleep.assert_called_with(5)

    def test_wait_raises_on_failure(self, mock_sleep, mock_hook_class) -> None:
        """Test synchronous wait raises when job fails."""
        mock_hook = MagicMock()
        mock_hook_class.return_value = mock_hook
        mock_hook.get_job_completion_status_sync.return_value = JobCompletionStatus.FAILED

        op = HevoPipelineOperator(task_id="test_task", connection_id="test_conn", pipeline_id=123)

        with pytest.raises(AirflowException, match="Job job_123 failed"):
            op._wait_synchronously("job_123")

    def test_wait_accepts_completed_with_failures(self, mock_sleep, mock_hook_class) -> None:
        """Test synchronous wait succeeds on completed_with_failures."""
        mock_hook = MagicMock()
        mock_hook_class.return_value = mock_hook
        mock_hook.get_job_completion_status_sync.return_value = JobCompletionStatus.COMPLETED_WITH_FAILURES

        op = HevoPipelineOperator(task_id="test_task", connection_id="test_conn", pipeline_id=123)
        op._wait_synchronously("job_123")

        mock_hook.get_job_completion_status_sync.assert_called_once()


@patch("airflow.hevo.operators.HevoPipelineHook")
class TestHevoOperatorHookProperty:
    """Tests for hook cached property."""

    def test_hook_property_creates_hook_instance(self, mock_hook_class) -> None:
        """Test hook property creates HevoPipelineHook with correct parameters."""
        op = HevoPipelineOperator(task_id="test_task", pipeline_id=123, connection_id="test_conn")

        # Access the hook property to trigger its creation
        _ = op.hook

        mock_hook_class.assert_called_once_with(connection_id="test_conn")


@patch("airflow.hevo.operators.HevoPipelineHook")
class TestHevoOperatorResyncAction:
    """Tests for RESYNC action functionality."""

    def test_resync_action_triggers_resync_not_sync(
        self, mock_hook_class, mock_airflow_context, sample_job_response
    ) -> None:
        """Test RESYNC action calls resync_pipeline_sync instead of trigger_pipeline_sync."""
        mock_hook = MagicMock()
        mock_hook_class.return_value = mock_hook
        mock_hook.resync_pipeline_sync.return_value = None
        mock_hook.find_active_job_by_type_sync.return_value = Job(**sample_job_response)

        # Mock get_pipeline_sync to return a pipeline with INITIALIZED status
        mock_pipeline = MagicMock()
        mock_pipeline.status = PipelineStatus.INITIALIZED
        mock_hook.get_pipeline_sync.return_value = mock_pipeline

        op = HevoPipelineOperator(
            task_id="test_task",
            connection_id="test_conn",
            pipeline_id=123,
            action=PipelineAction.RESYNC,
            wait_for_completion=False,
        )
        result = op.execute(mock_airflow_context)

        # Should call resync_pipeline_sync with default resync_mode, not trigger_pipeline_sync
        mock_hook.resync_pipeline_sync.assert_called_once_with(123, ResyncMode.EVOLVE_AND_MERGE)
        mock_hook.trigger_pipeline_sync.assert_not_called()
        # Should NOT call validate_pipeline for RESYNC action
        mock_hook.validate_pipeline.assert_not_called()
        assert result == "550e8400-e29b-41d4-a716-446655440001"

    def test_sync_now_action_validates_and_triggers(
        self, mock_hook_class, mock_airflow_context, sample_job_response
    ) -> None:
        """Test SYNC_NOW action validates pipeline and triggers sync."""
        mock_hook = MagicMock()
        mock_hook_class.return_value = mock_hook
        mock_hook.validate_pipeline.return_value = None
        mock_hook.trigger_pipeline_sync.return_value = None
        mock_hook.find_active_job_by_type_sync.return_value = Job(**sample_job_response)

        op = HevoPipelineOperator(
            task_id="test_task",
            connection_id="test_conn",
            pipeline_id=123,
            action=PipelineAction.SYNC_NOW,
            wait_for_completion=False,
        )
        result = op.execute(mock_airflow_context)

        # Should validate pipeline first
        mock_hook.validate_pipeline.assert_called_once_with(123)
        # Should call trigger_pipeline_sync, not resync_pipeline_sync
        mock_hook.trigger_pipeline_sync.assert_called_once_with(123, True)
        mock_hook.resync_pipeline_sync.assert_not_called()
        assert result == "550e8400-e29b-41d4-a716-446655440001"

    def test_resync_action_with_deferrable_mode(
        self, mock_hook_class, mock_airflow_context, sample_job_response
    ) -> None:
        """Test RESYNC action works with deferrable mode."""
        mock_hook = MagicMock()
        mock_hook_class.return_value = mock_hook
        mock_hook.resync_pipeline_sync.return_value = None
        mock_hook.find_active_job_by_type_sync.return_value = Job(**sample_job_response)

        # Mock get_pipeline_sync to return a pipeline with INITIALIZED status
        mock_pipeline = MagicMock()
        mock_pipeline.status = PipelineStatus.INITIALIZED
        mock_hook.get_pipeline_sync.return_value = mock_pipeline

        op = HevoPipelineOperator(
            task_id="test_task",
            connection_id="test_conn",
            pipeline_id=123,
            action=PipelineAction.RESYNC,
            deferrable=True,
            wait_for_completion=True,
        )

        with pytest.raises(TaskDeferred) as exc_info:
            op.execute(mock_airflow_context)

        # Verify resync was called with default resync_mode
        mock_hook.resync_pipeline_sync.assert_called_once_with(123, ResyncMode.EVOLVE_AND_MERGE)
        mock_hook.validate_pipeline.assert_not_called()

        # Verify trigger was created correctly
        trigger = exc_info.value.trigger
        assert trigger.pipeline_id == 123
        assert trigger.job_id == "550e8400-e29b-41d4-a716-446655440001"

    def test_resync_action_with_synchronous_wait(
        self, mock_hook_class, mock_airflow_context, sample_job_response
    ) -> None:
        """Test RESYNC action works with synchronous wait."""
        mock_hook = MagicMock()
        mock_hook_class.return_value = mock_hook
        mock_hook.resync_pipeline_sync.return_value = None
        mock_hook.find_active_job_by_type_sync.return_value = Job(**sample_job_response)
        mock_hook.get_job_completion_status_sync.return_value = JobCompletionStatus.COMPLETED

        # Mock get_pipeline_sync to return a pipeline with INITIALIZED status
        mock_pipeline = MagicMock()
        mock_pipeline.status = PipelineStatus.INITIALIZED
        mock_hook.get_pipeline_sync.return_value = mock_pipeline

        op = HevoPipelineOperator(
            task_id="test_task",
            connection_id="test_conn",
            pipeline_id=123,
            action=PipelineAction.RESYNC,
            deferrable=False,
            wait_for_completion=True,
        )

        with patch("airflow.hevo.operators.sleep"):
            result = op.execute(mock_airflow_context)

        # Verify resync was called with default resync_mode
        mock_hook.resync_pipeline_sync.assert_called_once_with(123, ResyncMode.EVOLVE_AND_MERGE)
        mock_hook.validate_pipeline.assert_not_called()
        mock_hook.get_job_completion_status_sync.assert_called_once()
        assert result == "550e8400-e29b-41d4-a716-446655440001"

    def test_resync_action_failure(self, mock_hook_class, mock_airflow_context) -> None:
        """Test RESYNC action raises when resync_pipeline_sync fails."""
        mock_hook = MagicMock()
        mock_hook_class.return_value = mock_hook
        mock_hook.resync_pipeline_sync.side_effect = AirflowException("Resync failed")

        # Mock get_pipeline_sync to return a pipeline with INITIALIZED status
        mock_pipeline = MagicMock()
        mock_pipeline.status = PipelineStatus.INITIALIZED
        mock_hook.get_pipeline_sync.return_value = mock_pipeline

        op = HevoPipelineOperator(
            task_id="test_task",
            connection_id="test_conn",
            pipeline_id=123,
            action=PipelineAction.RESYNC,
        )

        with pytest.raises(AirflowException, match="Resync failed"):
            op.execute(mock_airflow_context)

    def test_resync_action_with_drop_and_load_resync_mode(
        self, mock_hook_class, mock_airflow_context, sample_job_response
    ) -> None:
        """Test RESYNC action with resync_mode=DROP_AND_LOAD."""
        mock_hook = MagicMock()
        mock_hook_class.return_value = mock_hook
        mock_hook.resync_pipeline_sync.return_value = None
        mock_hook.find_active_job_by_type_sync.return_value = Job(**sample_job_response)

        # Mock get_pipeline_sync to return a pipeline with INITIALIZED status
        mock_pipeline = MagicMock()
        mock_pipeline.status = PipelineStatus.INITIALIZED
        mock_hook.get_pipeline_sync.return_value = mock_pipeline

        op = HevoPipelineOperator(
            task_id="test_task",
            connection_id="test_conn",
            pipeline_id=123,
            action=PipelineAction.RESYNC,
            resync_mode=ResyncMode.DROP_AND_LOAD,
            wait_for_completion=False,
        )
        result = op.execute(mock_airflow_context)

        # Should call resync_pipeline_sync with ResyncMode.DROP_AND_LOAD
        mock_hook.resync_pipeline_sync.assert_called_once_with(123, ResyncMode.DROP_AND_LOAD)
        assert result == "550e8400-e29b-41d4-a716-446655440001"

    def test_resync_action_defaults_to_resync_with_evolve_job_type(
        self, mock_hook_class, mock_airflow_context, sample_job_response
    ) -> None:
        """Test RESYNC action defaults to RESYNC_WITH_EVOLVE job type when resync_mode=EVOLVE_AND_MERGE."""
        mock_hook = MagicMock()
        mock_hook_class.return_value = mock_hook
        mock_hook.resync_pipeline_sync.return_value = None
        # Return a job with RESYNC_WITH_EVOLVE type
        resync_job = {**sample_job_response, "type": "RESYNC_WITH_EVOLVE"}
        mock_hook.find_active_job_by_type_sync.return_value = Job(**resync_job)

        # Mock get_pipeline_sync to return a pipeline with INITIALIZED status
        mock_pipeline = MagicMock()
        mock_pipeline.status = PipelineStatus.INITIALIZED
        mock_hook.get_pipeline_sync.return_value = mock_pipeline

        # Don't specify job_type - should default to RESYNC_WITH_EVOLVE for RESYNC action with default resync_mode
        op = HevoPipelineOperator(
            task_id="test_task",
            connection_id="test_conn",
            pipeline_id=123,
            action=PipelineAction.RESYNC,
            wait_for_completion=False,
        )
        result = op.execute(mock_airflow_context)

        # Verify it's looking for RESYNC_WITH_EVOLVE job type
        mock_hook.find_active_job_by_type_sync.assert_called_once_with(pipeline_id=123, job_type=JobType.RESYNC_WITH_EVOLVE)
        assert result == resync_job["job_id"]

    def test_resync_action_with_drop_and_load_mode_defaults_to_resync_with_drop_and_load_job_type(
        self, mock_hook_class, mock_airflow_context, sample_job_response
    ) -> None:
        """Test RESYNC action defaults to RESYNC_WITH_DROP_AND_LOAD job type when resync_mode=DROP_AND_LOAD."""
        mock_hook = MagicMock()
        mock_hook_class.return_value = mock_hook
        mock_hook.resync_pipeline_sync.return_value = None
        # Return a job with RESYNC_WITH_DROP_AND_LOAD type
        resync_drop_job = {**sample_job_response, "type": "RESYNC_WITH_DROP_AND_LOAD"}
        mock_hook.find_active_job_by_type_sync.return_value = Job(**resync_drop_job)

        # Mock get_pipeline_sync to return a pipeline with INITIALIZED status
        mock_pipeline = MagicMock()
        mock_pipeline.status = PipelineStatus.INITIALIZED
        mock_hook.get_pipeline_sync.return_value = mock_pipeline

        # Don't specify job_type - should default to RESYNC_WITH_DROP_AND_LOAD for RESYNC with resync_mode=DROP_AND_LOAD
        op = HevoPipelineOperator(
            task_id="test_task",
            connection_id="test_conn",
            pipeline_id=123,
            action=PipelineAction.RESYNC,
            resync_mode=ResyncMode.DROP_AND_LOAD,
            wait_for_completion=False,
        )
        result = op.execute(mock_airflow_context)

        # Verify it's looking for RESYNC_WITH_DROP_AND_LOAD job type
        mock_hook.find_active_job_by_type_sync.assert_called_once_with(
            pipeline_id=123, job_type=JobType.RESYNC_WITH_DROP_AND_LOAD
        )
        assert result == resync_drop_job["job_id"]

    def test_sync_now_action_defaults_to_incremental_job_type(
        self, mock_hook_class, mock_airflow_context, sample_job_response
    ) -> None:
        """Test SYNC_NOW action defaults to INCREMENTAL job type."""
        mock_hook = MagicMock()
        mock_hook_class.return_value = mock_hook
        mock_hook.validate_pipeline.return_value = None
        mock_hook.trigger_pipeline_sync.return_value = None
        mock_hook.find_active_job_by_type_sync.return_value = Job(**sample_job_response)

        # Don't specify job_type - should default to INCREMENTAL for SYNC_NOW
        op = HevoPipelineOperator(
            task_id="test_task",
            connection_id="test_conn",
            pipeline_id=123,
            action=PipelineAction.SYNC_NOW,
            wait_for_completion=False,
        )
        result = op.execute(mock_airflow_context)

        # Verify it's looking for INCREMENTAL job type
        mock_hook.find_active_job_by_type_sync.assert_called_once_with(pipeline_id=123, job_type=JobType.INCREMENTAL)
        assert result == sample_job_response["job_id"]

    def test_resync_action_with_explicit_incremental_job_type(
        self, mock_hook_class, mock_airflow_context, sample_job_response
    ) -> None:
        """Test RESYNC action with explicit INCREMENTAL job type override."""
        mock_hook = MagicMock()
        mock_hook_class.return_value = mock_hook
        mock_hook.resync_pipeline_sync.return_value = None
        # Return an incremental job (not RESYNC)
        incremental_job = {**sample_job_response, "type": "INCREMENTAL"}
        mock_hook.find_active_job_by_type_sync.return_value = Job(**incremental_job)

        # Mock get_pipeline_sync to return a pipeline with INITIALIZED status
        mock_pipeline = MagicMock()
        mock_pipeline.status = PipelineStatus.INITIALIZED
        mock_hook.get_pipeline_sync.return_value = mock_pipeline

        # Explicitly set job_type to INCREMENTAL (override default)
        op = HevoPipelineOperator(
            task_id="test_task",
            connection_id="test_conn",
            pipeline_id=123,
            action=PipelineAction.RESYNC,
            job_type=JobType.INCREMENTAL,  # Explicit override
            wait_for_completion=False,
        )
        result = op.execute(mock_airflow_context)

        # Verify it's looking for INCREMENTAL job type (not default RESYNC)
        mock_hook.find_active_job_by_type_sync.assert_called_once_with(pipeline_id=123, job_type=JobType.INCREMENTAL)
        assert result == incremental_job["job_id"]

    def test_resync_action_drop_and_load_mode_with_deferrable(
        self, mock_hook_class, mock_airflow_context, sample_job_response
    ) -> None:
        """Test RESYNC action with resync_mode=DROP_AND_LOAD in deferrable mode."""
        mock_hook = MagicMock()
        mock_hook_class.return_value = mock_hook
        mock_hook.resync_pipeline_sync.return_value = None
        resync_drop_job = {**sample_job_response, "type": "RESYNC_WITH_DROP_AND_LOAD"}
        mock_hook.find_active_job_by_type_sync.return_value = Job(**resync_drop_job)

        # Mock get_pipeline_sync to return a pipeline with INITIALIZED status
        mock_pipeline = MagicMock()
        mock_pipeline.status = PipelineStatus.INITIALIZED
        mock_hook.get_pipeline_sync.return_value = mock_pipeline

        op = HevoPipelineOperator(
            task_id="test_task",
            connection_id="test_conn",
            pipeline_id=123,
            action=PipelineAction.RESYNC,
            resync_mode=ResyncMode.DROP_AND_LOAD,
            deferrable=True,
            wait_for_completion=True,
        )

        with pytest.raises(TaskDeferred) as exc_info:
            op.execute(mock_airflow_context)

        # Verify resync_mode=DROP_AND_LOAD was passed
        mock_hook.resync_pipeline_sync.assert_called_once_with(123, ResyncMode.DROP_AND_LOAD)

        # Verify trigger was created with correct job_id
        trigger = exc_info.value.trigger
        assert trigger.pipeline_id == 123
        assert trigger.job_id == resync_drop_job["job_id"]

    def test_resync_action_drop_and_load_mode_with_synchronous_wait(
        self, mock_hook_class, mock_airflow_context, sample_job_response
    ) -> None:
        """Test RESYNC action with resync_mode=DROP_AND_LOAD in synchronous wait mode."""
        mock_hook = MagicMock()
        mock_hook_class.return_value = mock_hook
        mock_hook.resync_pipeline_sync.return_value = None
        resync_drop_job = {**sample_job_response, "type": "RESYNC_WITH_DROP_AND_LOAD"}
        mock_hook.find_active_job_by_type_sync.return_value = Job(**resync_drop_job)
        mock_hook.get_job_completion_status_sync.return_value = JobCompletionStatus.COMPLETED

        # Mock get_pipeline_sync to return a pipeline with INITIALIZED status
        mock_pipeline = MagicMock()
        mock_pipeline.status = PipelineStatus.INITIALIZED
        mock_hook.get_pipeline_sync.return_value = mock_pipeline

        op = HevoPipelineOperator(
            task_id="test_task",
            connection_id="test_conn",
            pipeline_id=123,
            action=PipelineAction.RESYNC,
            resync_mode=ResyncMode.DROP_AND_LOAD,
            deferrable=False,
            wait_for_completion=True,
        )

        with patch("airflow.hevo.operators.sleep"):
            result = op.execute(mock_airflow_context)

        # Verify resync_mode=DROP_AND_LOAD was passed
        mock_hook.resync_pipeline_sync.assert_called_once_with(123, ResyncMode.DROP_AND_LOAD)
        mock_hook.get_job_completion_status_sync.assert_called_once()
        assert result == resync_drop_job["job_id"]

    def test_resync_waits_for_pipeline_initialized(
        self, mock_hook_class, mock_airflow_context, sample_job_response, sample_pipeline_response
    ) -> None:
        """Test RESYNC action waits for pipeline to reach INITIALIZED status before triggering."""
        mock_hook = MagicMock()
        mock_hook_class.return_value = mock_hook

        # Simulate pipeline in RESTARTING state, then INITIALIZED after some polls
        pipeline_restarting = Pipeline(**{**sample_pipeline_response, "status": "RESTARTING"})
        pipeline_initialized = Pipeline(**{**sample_pipeline_response, "status": "INITIALIZED"})

        # First two calls return RESTARTING, third call returns INITIALIZED
        mock_hook.get_pipeline_sync.side_effect = [
            pipeline_restarting,
            pipeline_restarting,
            pipeline_initialized,
        ]

        mock_hook.resync_pipeline_sync.return_value = None
        mock_hook.find_active_job_by_type_sync.return_value = Job(**sample_job_response)

        op = HevoPipelineOperator(
            task_id="test_task",
            connection_id="test_conn",
            pipeline_id=123,
            action=PipelineAction.RESYNC,
            poll_interval=1,  # Short interval for test
            wait_for_completion=False,
        )

        with patch("airflow.hevo.operators.sleep") as mock_sleep:
            result = op.execute(mock_airflow_context)

        # Verify pipeline status was checked 3 times (2 RESTARTING, 1 INITIALIZED)
        assert mock_hook.get_pipeline_sync.call_count == 3
        mock_hook.get_pipeline_sync.assert_called_with(123)

        # Verify sleep was called 2 times (once for each RESTARTING status)
        assert mock_sleep.call_count == 2
        mock_sleep.assert_called_with(1)  # poll_interval

        # Verify resync was called after pipeline became INITIALIZED
        mock_hook.resync_pipeline_sync.assert_called_once_with(123, ResyncMode.EVOLVE_AND_MERGE)

        # Should NOT call validate_pipeline for RESYNC action
        mock_hook.validate_pipeline.assert_not_called()

        assert result == "550e8400-e29b-41d4-a716-446655440001"

    def test_resync_waits_indefinitely_for_pipeline_initialized(
        self, mock_hook_class, mock_airflow_context, sample_job_response, sample_pipeline_response
    ) -> None:
        """Test RESYNC action has no retry limit when waiting for INITIALIZED status."""
        mock_hook = MagicMock()
        mock_hook_class.return_value = mock_hook

        # Simulate pipeline staying in RESTARTING state for 50 attempts
        pipeline_restarting = Pipeline(**{**sample_pipeline_response, "status": "RESTARTING"})
        pipeline_initialized = Pipeline(**{**sample_pipeline_response, "status": "INITIALIZED"})

        # Return RESTARTING 50 times, then INITIALIZED
        side_effects = [pipeline_restarting] * 50 + [pipeline_initialized]
        mock_hook.get_pipeline_sync.side_effect = side_effects

        mock_hook.resync_pipeline_sync.return_value = None
        mock_hook.find_active_job_by_type_sync.return_value = Job(**sample_job_response)

        op = HevoPipelineOperator(
            task_id="test_task",
            connection_id="test_conn",
            pipeline_id=123,
            action=PipelineAction.RESYNC,
            poll_interval=1,
            wait_for_completion=False,
        )

        with patch("airflow.hevo.operators.sleep") as mock_sleep:
            result = op.execute(mock_airflow_context)

        # Verify pipeline status was checked 51 times (no retry limit)
        assert mock_hook.get_pipeline_sync.call_count == 51

        # Verify sleep was called 50 times
        assert mock_sleep.call_count == 50

        # Verify resync was eventually called
        mock_hook.resync_pipeline_sync.assert_called_once_with(123, ResyncMode.EVOLVE_AND_MERGE)

        assert result == "550e8400-e29b-41d4-a716-446655440001"

    def test_resync_raises_when_pipeline_not_found(self, mock_hook_class, mock_airflow_context) -> None:
        """Test RESYNC action raises when pipeline does not exist."""
        mock_hook = MagicMock()
        mock_hook_class.return_value = mock_hook

        # Simulate pipeline not found
        mock_hook.get_pipeline_sync.return_value = None

        op = HevoPipelineOperator(
            task_id="test_task",
            connection_id="test_conn",
            pipeline_id=999,
            action=PipelineAction.RESYNC,
            poll_interval=1,
        )

        with pytest.raises(AirflowException, match="Pipeline 999 does not exist"):
            op.execute(mock_airflow_context)

        # Verify we tried to get pipeline status
        mock_hook.get_pipeline_sync.assert_called_once_with(999)

        # Verify resync was never called
        mock_hook.resync_pipeline_sync.assert_not_called()


@patch("airflow.hevo.operators.HevoPipelineHook")
class TestHevoOperatorErrorHandling:
    """Tests for error handling in various scenarios."""

    def test_execute_validation_failure(self, mock_hook_class, mock_airflow_context) -> None:
        """Test execute raises when pipeline validation fails."""
        mock_hook = MagicMock()
        mock_hook_class.return_value = mock_hook
        mock_hook.validate_pipeline.side_effect = AirflowException("Pipeline not in INITIALIZED state")

        op = HevoPipelineOperator(task_id="test_task", connection_id="test_conn", pipeline_id=123)

        with pytest.raises(AirflowException, match="Pipeline not in INITIALIZED state"):
            op.execute(mock_airflow_context)

    def test_execute_trigger_failure(self, mock_hook_class, mock_airflow_context) -> None:
        """Test execute raises when sync trigger fails."""
        mock_hook = MagicMock()
        mock_hook_class.return_value = mock_hook
        mock_hook.validate_pipeline.return_value = None
        mock_hook.trigger_pipeline_sync.side_effect = AirflowException("Sync trigger failed")

        op = HevoPipelineOperator(task_id="test_task", connection_id="test_conn", pipeline_id=123)

        with pytest.raises(AirflowException, match="Sync trigger failed"):
            op.execute(mock_airflow_context)
