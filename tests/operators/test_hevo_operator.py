"""Unit tests for HevoOperator."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from airflow.exceptions import AirflowException, TaskDeferred

from airflow.hevo.models.job import Job, JobCompletionStatus, JobType
from airflow.hevo.models.pipeline import Pipeline, SyncType
from airflow.hevo.operators.hevo_operator import HevoOperator


class TestHevoOperatorInit:
    """Tests for HevoOperator initialization and configuration."""

    def test_operator_initialization_with_defaults(self):
        """Test operator initializes with default parameters."""
        op = HevoOperator(
            task_id="test_task",
            pipeline_id=123
        )
        assert op.pipeline_id == 123
        assert op.sync_type == SyncType.ON_DEMAND
        assert op.job_type == JobType.INCREMENTAL
        assert op.connection_id is None
        assert op.poll_interval == 5
        assert op.retry_limit == 10
        assert op.deferrable is True
        assert op.wait_for_completion is True
        assert op.accept_completed_with_failures is False

    def test_operator_initialization_with_custom_params(self):
        """Test operator initializes with all custom parameters."""
        op = HevoOperator(
            task_id="test_task",
            pipeline_id=456,
            sync_type=SyncType.SCHEDULED,
            job_type=JobType.HISTORICAL,
            connection_id="custom_conn",
            poll_interval=10,
            retry_limit=5,
            deferrable=False,
            wait_for_completion=False,
            accept_completed_with_failures=True
        )
        assert op.pipeline_id == 456
        assert op.sync_type == SyncType.SCHEDULED
        assert op.job_type == JobType.HISTORICAL
        assert op.connection_id == "custom_conn"
        assert op.poll_interval == 10
        assert op.retry_limit == 5
        assert op.deferrable is False
        assert op.wait_for_completion is False
        assert op.accept_completed_with_failures is True

    def test_template_fields_defined(self):
        """Test template_fields attribute is correctly defined."""
        assert hasattr(HevoOperator, "template_fields")
        assert "pipeline_id" in HevoOperator.template_fields


@patch("airflow.hevo.operators.hevo_operator.HevoPipelineHook")
class TestHevoOperatorExecuteFireAndForget:
    """Tests for execute method in fire-and-forget mode (wait_for_completion=False)."""

    def test_execute_returns_job_id_without_waiting(
        self, mock_hook_class, mock_airflow_context, sample_job_response
    ):
        """Test execute returns job_id immediately when wait_for_completion=False."""
        # Setup mock hook instance
        mock_hook = MagicMock()
        mock_hook_class.return_value = mock_hook
        mock_hook.validate_pipeline.return_value = None
        mock_hook.trigger_pipeline_sync.return_value = None
        mock_hook.find_active_job_by_type.return_value = Job(**sample_job_response)

        # Create and execute operator
        op = HevoOperator(
            task_id="test_task",
            pipeline_id=123,
            wait_for_completion=False
        )
        result = op.execute(mock_airflow_context)

        # Assertions
        assert result == "job_789"
        mock_hook.validate_pipeline.assert_called_once_with(123, SyncType.ON_DEMAND)
        mock_hook.trigger_pipeline_sync.assert_called_once_with(123)
        mock_hook.find_active_job_by_type.assert_called()

    def test_execute_raises_when_no_active_job_found(
        self, mock_hook_class, mock_airflow_context
    ):
        """Test execute raises AirflowException when no active job found after retries."""
        mock_hook = MagicMock()
        mock_hook_class.return_value = mock_hook
        mock_hook.validate_pipeline.return_value = None
        mock_hook.trigger_pipeline_sync.return_value = None
        # Hook raises exception when no job found (realistic behavior)
        mock_hook.find_active_job_by_type.side_effect = AirflowException(
            "Pipeline 123 doesn't have any active jobs of type INCREMENTAL"
        )

        op = HevoOperator(
            task_id="test_task",
            pipeline_id=123,
            wait_for_completion=False,
            retry_limit=2
        )

        with patch("airflow.hevo.operators.hevo_operator.sleep"):
            with pytest.raises(AirflowException, match="No active INCREMENTAL job found"):
                op.execute(mock_airflow_context)

            # Verify it retried
            assert mock_hook.find_active_job_by_type.call_count == 2

    def test_execute_retries_and_succeeds_finding_job(
        self, mock_hook_class, mock_airflow_context, sample_job_response
    ):
        """Test execute retries when job not found initially, then succeeds."""
        mock_hook = MagicMock()
        mock_hook_class.return_value = mock_hook
        mock_hook.validate_pipeline.return_value = None
        mock_hook.trigger_pipeline_sync.return_value = None

        # First two attempts: no job found (exception)
        # Third attempt: job found
        mock_hook.find_active_job_by_type.side_effect = [
            AirflowException("No active jobs found"),
            AirflowException("No active jobs found"),
            Job(**sample_job_response)
        ]

        op = HevoOperator(
            task_id="test_task",
            pipeline_id=123,
            wait_for_completion=False
        )

        with patch("airflow.hevo.operators.hevo_operator.sleep"):
            result = op.execute(mock_airflow_context)

        # Should succeed after retries
        assert result == "job_789"
        # Verify it tried 3 times before succeeding
        assert mock_hook.find_active_job_by_type.call_count == 3


@patch("airflow.hevo.operators.hevo_operator.HevoPipelineHook")
class TestHevoOperatorExecuteDeferrable:
    """Tests for execute method in deferrable mode."""

    def test_execute_defers_to_trigger(
        self, mock_hook_class, mock_airflow_context, sample_job_response
    ):
        """Test execute defers to HevoTrigger when deferrable=True."""
        mock_hook = MagicMock()
        mock_hook_class.return_value = mock_hook
        mock_hook.validate_pipeline.return_value = None
        mock_hook.trigger_pipeline_sync.return_value = None
        mock_hook.find_active_job_by_type.return_value = Job(**sample_job_response)

        op = HevoOperator(
            task_id="test_task",
            pipeline_id=123,
            deferrable=True,
            wait_for_completion=True
        )

        with pytest.raises(TaskDeferred) as exc_info:
            op.execute(mock_airflow_context)

        # Verify TaskDeferred exception contains correct trigger
        deferred = exc_info.value
        assert deferred.trigger.pipeline_id == 123
        assert deferred.trigger.job_id == "job_789"
        assert deferred.trigger.job_type == JobType.INCREMENTAL.value
        assert deferred.method_name == "execute_complete"

    def test_execute_defers_with_custom_params(
        self, mock_hook_class, mock_airflow_context
    ):
        """Test execute defers with custom parameters passed to trigger."""
        mock_hook = MagicMock()
        mock_hook_class.return_value = mock_hook
        mock_hook.validate_pipeline.return_value = None
        mock_hook.trigger_pipeline_sync.return_value = None

        historical_job = {
            "job_id": "job_hist_123",
            "pipeline_id": 456,
            "type": "HISTORICAL",
            "status": "IN_PROGRESS"
        }
        mock_hook.find_active_job_by_type.return_value = Job(**historical_job)

        op = HevoOperator(
            task_id="test_task",
            pipeline_id=456,
            job_type=JobType.HISTORICAL,
            poll_interval=10,
            accept_completed_with_failures=True,
            connection_id="custom_conn",
            deferrable=True,
            wait_for_completion=True
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


@patch("airflow.hevo.operators.hevo_operator.HevoPipelineHook")
@patch("airflow.hevo.operators.hevo_operator.sleep")
class TestHevoOperatorExecuteSynchronous:
    """Tests for execute method in synchronous wait mode."""

    def test_execute_waits_synchronously(
        self, mock_sleep, mock_hook_class, mock_airflow_context, sample_job_response
    ):
        """Test execute waits synchronously when deferrable=False."""
        mock_hook = MagicMock()
        mock_hook_class.return_value = mock_hook
        mock_hook.validate_pipeline.return_value = None
        mock_hook.trigger_pipeline_sync.return_value = None
        mock_hook.find_active_job_by_type.return_value = Job(**sample_job_response)
        mock_hook.get_job_completion_status.return_value = JobCompletionStatus.COMPLETED

        op = HevoOperator(
            task_id="test_task",
            pipeline_id=123,
            deferrable=False,
            wait_for_completion=True
        )

        result = op.execute(mock_airflow_context)

        assert result == "job_789"
        mock_hook.get_job_completion_status.assert_called_once()


class TestHevoOperatorExecuteComplete:
    """Tests for execute_complete callback method."""

    def test_execute_complete_success(self, mock_airflow_context):
        """Test execute_complete processes success event correctly."""
        op = HevoOperator(task_id="test_task", pipeline_id=123)

        event = {
            "status": "success",
            "message": "Job completed successfully",
            "job_id": "job_789"
        }

        # Should not raise
        op.execute_complete(mock_airflow_context, event)

    def test_execute_complete_success_with_failures(self, mock_airflow_context):
        """Test execute_complete handles completed_with_failures flag."""
        op = HevoOperator(task_id="test_task", pipeline_id=123)

        event = {
            "status": "success",
            "message": "Job completed with failures",
            "job_id": "job_789",
            "completed_with_failures": True
        }

        # Should not raise, just log warning
        op.execute_complete(mock_airflow_context, event)

    def test_execute_complete_error(self, mock_airflow_context):
        """Test execute_complete raises on error event."""
        op = HevoOperator(task_id="test_task", pipeline_id=123)

        event = {
            "status": "error",
            "message": "Job failed",
            "job_id": "job_789"
        }

        with pytest.raises(AirflowException, match="Job job_789 failed"):
            op.execute_complete(mock_airflow_context, event)

    def test_execute_complete_none_event(self, mock_airflow_context):
        """Test execute_complete raises when event is None."""
        op = HevoOperator(task_id="test_task", pipeline_id=123)

        with pytest.raises(AirflowException, match="Trigger event is None"):
            op.execute_complete(mock_airflow_context, None)

    def test_execute_complete_unexpected_status(self, mock_airflow_context):
        """Test execute_complete raises on unexpected status."""
        op = HevoOperator(task_id="test_task", pipeline_id=123)

        event = {
            "status": "unknown",
            "message": "Unknown status",
            "job_id": "job_789"
        }

        with pytest.raises(AirflowException, match="Unexpected trigger event status"):
            op.execute_complete(mock_airflow_context, event)


@patch("airflow.hevo.operators.hevo_operator.HevoPipelineHook")
@patch("airflow.hevo.operators.hevo_operator.sleep")
class TestHevoOperatorWaitSynchronously:
    """Tests for _wait_synchronously private method."""

    def test_wait_completes_immediately(
        self, mock_sleep, mock_hook_class
    ):
        """Test synchronous wait when job completes immediately."""
        mock_hook = MagicMock()
        mock_hook_class.return_value = mock_hook
        mock_hook.get_job_completion_status.return_value = JobCompletionStatus.COMPLETED

        op = HevoOperator(task_id="test_task", pipeline_id=123, poll_interval=1)
        op._wait_synchronously("job_123")

        mock_hook.get_job_completion_status.assert_called_once()
        mock_sleep.assert_not_called()

    def test_wait_polls_until_completion(
        self, mock_sleep, mock_hook_class
    ):
        """Test synchronous wait polls multiple times until completion."""
        mock_hook = MagicMock()
        mock_hook_class.return_value = mock_hook
        mock_hook.get_job_completion_status.side_effect = [
            JobCompletionStatus.PENDING,
            JobCompletionStatus.PENDING,
            JobCompletionStatus.COMPLETED
        ]

        op = HevoOperator(task_id="test_task", pipeline_id=123, poll_interval=5)
        op._wait_synchronously("job_123")

        assert mock_hook.get_job_completion_status.call_count == 3
        assert mock_sleep.call_count == 2
        mock_sleep.assert_called_with(5)

    def test_wait_raises_on_failure(
        self, mock_sleep, mock_hook_class
    ):
        """Test synchronous wait raises when job fails."""
        mock_hook = MagicMock()
        mock_hook_class.return_value = mock_hook
        mock_hook.get_job_completion_status.return_value = JobCompletionStatus.FAILED

        op = HevoOperator(task_id="test_task", pipeline_id=123)

        with pytest.raises(AirflowException, match="Job job_123 failed"):
            op._wait_synchronously("job_123")

    def test_wait_accepts_completed_with_failures(
        self, mock_sleep, mock_hook_class
    ):
        """Test synchronous wait succeeds on completed_with_failures."""
        mock_hook = MagicMock()
        mock_hook_class.return_value = mock_hook
        mock_hook.get_job_completion_status.return_value = JobCompletionStatus.COMPLETED_WITH_FAILURES

        op = HevoOperator(task_id="test_task", pipeline_id=123)
        op._wait_synchronously("job_123")

        mock_hook.get_job_completion_status.assert_called_once()


@patch("airflow.hevo.operators.hevo_operator.HevoPipelineHook")
class TestHevoOperatorHookProperty:
    """Tests for hook cached property."""

    def test_hook_property_creates_hook_instance(self, mock_hook_class):
        """Test hook property creates HevoPipelineHook with correct parameters."""
        op = HevoOperator(
            task_id="test_task",
            pipeline_id=123,
            connection_id="test_conn"
        )

        hook = op.hook

        mock_hook_class.assert_called_once_with(
            pipeline_id=123,
            connection_id="test_conn"
        )


@patch("airflow.hevo.operators.hevo_operator.HevoPipelineHook")
class TestHevoOperatorErrorHandling:
    """Tests for error handling in various scenarios."""

    def test_execute_validation_failure(
        self, mock_hook_class, mock_airflow_context
    ):
        """Test execute raises when pipeline validation fails."""
        mock_hook = MagicMock()
        mock_hook_class.return_value = mock_hook
        mock_hook.validate_pipeline.side_effect = AirflowException(
            "Pipeline not in INITIALIZED state"
        )

        op = HevoOperator(task_id="test_task", pipeline_id=123)

        with pytest.raises(AirflowException, match="Pipeline not in INITIALIZED state"):
            op.execute(mock_airflow_context)

    def test_execute_trigger_failure(
        self, mock_hook_class, mock_airflow_context
    ):
        """Test execute raises when sync trigger fails."""
        mock_hook = MagicMock()
        mock_hook_class.return_value = mock_hook
        mock_hook.validate_pipeline.return_value = None
        mock_hook.trigger_pipeline_sync.side_effect = AirflowException(
            "Sync trigger failed"
        )

        op = HevoOperator(task_id="test_task", pipeline_id=123)

        with pytest.raises(AirflowException, match="Sync trigger failed"):
            op.execute(mock_airflow_context)

    def test_execute_missing_job_id(
        self, mock_hook_class, mock_airflow_context
    ):
        """Test execute raises when active job has no job_id."""
        mock_hook = MagicMock()
        mock_hook_class.return_value = mock_hook
        mock_hook.validate_pipeline.return_value = None
        mock_hook.trigger_pipeline_sync.return_value = None

        # Create job mock without job_id
        mock_job = MagicMock()
        mock_job.job_id = None
        mock_hook.find_active_job_by_type.return_value = mock_job

        op = HevoOperator(task_id="test_task", pipeline_id=123)

        with pytest.raises(AirflowException, match="Active job found but missing ID"):
            op.execute(mock_airflow_context)
