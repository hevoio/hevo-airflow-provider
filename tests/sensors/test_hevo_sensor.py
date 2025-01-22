"""Unit tests for HevoSensor."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from airflow.exceptions import AirflowException, TaskDeferred
from airflow.sensors.base import BaseSensorOperator

from airflow.hevo.models.job import Job, JobCompletionStatus, JobType
from airflow.hevo.sensor import HevoSensor
from tests.conftest import create_job_response


class TestHevoSensorInit:
    """Tests for HevoSensor initialization and configuration."""

    def test_sensor_initialization_with_defaults(self) -> None:
        """Test sensor initializes with default parameters."""
        sensor = HevoSensor(task_id="test_sensor", pipeline_id=123, connection_id="hevo_test_connection")
        assert sensor.pipeline_id == 123
        assert sensor.job_id is None
        assert sensor.job_type == JobType.INCREMENTAL
        assert sensor.connection_id == "hevo_test_connection"
        assert sensor.poke_interval == 20
        assert sensor.accept_completed_with_failures is False
        assert sensor.deferrable is True
        assert sensor.wait_for_job_max_attempts == 10
        assert sensor.wait_for_job_interval == 5
        assert sensor.wait_for_job_initial_delay == 10

    def test_sensor_initialization_with_custom_params(self) -> None:
        """Test sensor initializes with all custom parameters."""
        sensor = HevoSensor(
            task_id="test_sensor",
            pipeline_id=456,
            job_id="job_custom_123",
            job_type=JobType.HISTORICAL,
            connection_id="custom_conn",
            poke_interval=10,
            accept_completed_with_failures=True,
            deferrable=False,
            wait_for_job_max_attempts=5,
            wait_for_job_interval=3,
            wait_for_job_initial_delay=5,
        )
        assert sensor.pipeline_id == 456
        assert sensor.job_id == "job_custom_123"
        assert sensor.job_type == JobType.HISTORICAL
        assert sensor.connection_id == "custom_conn"
        assert sensor.poke_interval == 10
        assert sensor.accept_completed_with_failures is True
        assert sensor.deferrable is False
        assert sensor.wait_for_job_max_attempts == 5
        assert sensor.wait_for_job_interval == 3
        assert sensor.wait_for_job_initial_delay == 5

    def test_template_fields_defined(self) -> None:
        """Test template_fields attribute is correctly defined."""
        assert hasattr(HevoSensor, "template_fields")
        assert "pipeline_id" in HevoSensor.template_fields
        assert "job_id" in HevoSensor.template_fields
        assert "job_type" not in HevoSensor.template_fields


class TestHevoSensorExecute:
    """Tests for execute method."""

    @patch.object(BaseSensorOperator, "execute")
    def test_execute_non_deferrable_calls_parent(self, mock_super_execute, mock_airflow_context) -> None:
        """Test execute in non-deferrable mode delegates to parent class."""
        sensor = HevoSensor(
            task_id="test_sensor",
            pipeline_id=123,
            job_id="job_123",
            deferrable=False,
            connection_id="hevo_test_connection",
        )

        sensor.execute(mock_airflow_context)

        mock_super_execute.assert_called_once_with(context=mock_airflow_context)

    def test_execute_deferrable_job_not_completed(self, mock_airflow_context) -> None:
        """Test execute in deferrable mode defers when job not completed."""
        sensor = HevoSensor(
            task_id="test_sensor",
            pipeline_id=123,
            job_id="job_123",
            deferrable=True,
            connection_id="hevo_test_connection",
        )

        with patch.object(sensor, "poke", return_value=False):
            with pytest.raises(TaskDeferred) as exc_info:
                sensor.execute(mock_airflow_context)

            deferred = exc_info.value
            assert deferred.trigger.pipeline_id == 123
            assert deferred.trigger.job_id == "job_123"
            assert deferred.method_name == "execute_complete"

    def test_execute_deferrable_job_already_completed(self, mock_airflow_context) -> None:
        """Test execute in deferrable mode returns when job already completed."""
        sensor = HevoSensor(
            task_id="test_sensor",
            pipeline_id=123,
            job_id="job_123",
            deferrable=True,
            connection_id="hevo_test_connection",
        )

        with patch.object(sensor, "poke", return_value=True):
            # Should not raise TaskDeferred, returns normally
            sensor.execute(mock_airflow_context)


@patch("airflow.hevo.sensor.sleep")
@patch("airflow.hevo.sensor.HevoPipelineHook")
class TestHevoSensorGetJobId:
    """Tests for _get_job_id method."""

    def test_get_job_id_returns_explicit_job_id(self, mock_hook_class, mock_sleep, mock_airflow_context) -> None:
        """Test _get_job_id returns explicit job_id if provided."""
        sensor = HevoSensor(
            task_id="test_sensor", pipeline_id=123, job_id="job_explicit_123", connection_id="hevo_test_connection"
        )

        result = sensor._get_job_id()

        assert result == "job_explicit_123"
        mock_hook_class.assert_not_called()  # Should not need hook

    def test_get_job_id_auto_discovery_success(
        self, mock_hook_class, mock_airflow_context, sample_job_response
    ) -> None:
        """Test _get_job_id auto-discovers job successfully."""
        mock_hook = MagicMock()
        mock_hook_class.return_value = mock_hook
        mock_hook.find_active_job_by_type_sync.return_value = Job(**sample_job_response)

        sensor = HevoSensor(
            task_id="test_sensor", pipeline_id=123, wait_for_job_initial_delay=1, connection_id="hevo_test_connection"
        )

        result = sensor._get_job_id()

        assert result == sample_job_response["job_id"]
        mock_hook.find_active_job_by_type_sync.assert_called()

    def test_get_job_id_auto_discovery_with_retries(
        self, mock_hook_class, mock_sleep, mock_airflow_context, sample_job_response
    ) -> None:
        """Test _get_job_id auto-discovery retries before finding job."""
        mock_hook = MagicMock()
        mock_hook_class.return_value = mock_hook
        # First two calls raise exception, third succeeds
        mock_hook.find_active_job_by_type_sync.side_effect = [
            AirflowException("No active job"),
            AirflowException("No active job"),
            Job(**sample_job_response),
        ]

        sensor = HevoSensor(
            task_id="test_sensor",
            pipeline_id=123,
            wait_for_job_initial_delay=0,
            wait_for_job_interval=1,
            connection_id="hevo_test_connection",
        )

        result = sensor._get_job_id()

        assert result == sample_job_response["job_id"]
        assert mock_hook.find_active_job_by_type_sync.call_count == 3

    def test_get_job_id_auto_discovery_failure(self, mock_hook_class, mock_sleep, mock_airflow_context) -> None:
        """Test _get_job_id raises when auto-discovery fails after max attempts."""
        mock_hook = MagicMock()
        mock_hook_class.return_value = mock_hook
        mock_hook.find_active_job_by_type_sync.side_effect = AirflowException("No active job")

        sensor = HevoSensor(
            task_id="test_sensor",
            pipeline_id=123,
            wait_for_job_initial_delay=0,
            wait_for_job_interval=0,
            wait_for_job_max_attempts=3,
            connection_id="hevo_test_connection",
        )

        with pytest.raises(AirflowException, match="No active INCREMENTAL job found"):
            sensor._get_job_id()


@patch("airflow.hevo.sensor.HevoPipelineHook")
class TestHevoSensorPoke:
    """Tests for poke method."""

    def test_poke_with_explicit_job_id_completed(self, mock_hook_class, mock_airflow_context) -> None:
        """Test poke returns True when job is completed."""
        mock_hook = MagicMock()
        mock_hook_class.return_value = mock_hook
        mock_hook.get_job_completion_status_sync.return_value = JobCompletionStatus.COMPLETED

        sensor = HevoSensor(
            task_id="test_sensor", pipeline_id=123, job_id="job_123", connection_id="hevo_test_connection"
        )

        result = sensor.poke(mock_airflow_context)

        assert result is True
        mock_hook.get_job_completion_status_sync.assert_called_once()

    def test_poke_with_explicit_job_id_pending(self, mock_hook_class, mock_airflow_context) -> None:
        """Test poke returns False when job is still pending."""
        mock_hook = MagicMock()
        mock_hook_class.return_value = mock_hook
        mock_hook.get_job_completion_status_sync.return_value = JobCompletionStatus.PENDING

        sensor = HevoSensor(
            task_id="test_sensor", pipeline_id=123, job_id="job_123", connection_id="hevo_test_connection"
        )

        result = sensor.poke(mock_airflow_context)

        assert result is False

    def test_poke_with_explicit_job_id_failed(self, mock_hook_class, mock_airflow_context) -> None:
        """Test poke raises when job fails."""
        mock_hook = MagicMock()
        mock_hook_class.return_value = mock_hook
        mock_hook.get_job_completion_status_sync.return_value = JobCompletionStatus.FAILED

        sensor = HevoSensor(
            task_id="test_sensor", pipeline_id=123, job_id="job_123", connection_id="hevo_test_connection"
        )

        with pytest.raises(AirflowException, match="Job job_123 failed"):
            sensor.poke(mock_airflow_context)

    def test_poke_completed_with_failures_accepted(self, mock_hook_class, mock_airflow_context) -> None:
        """Test poke returns True when completed_with_failures and accepting."""
        mock_hook = MagicMock()
        mock_hook_class.return_value = mock_hook
        mock_hook.get_job_completion_status_sync.return_value = JobCompletionStatus.COMPLETED_WITH_FAILURES

        sensor = HevoSensor(
            task_id="test_sensor",
            pipeline_id=123,
            job_id="job_123",
            accept_completed_with_failures=True,
            connection_id="hevo_test_connection",
        )

        result = sensor.poke(mock_airflow_context)

        assert result is True

    def test_poke_completed_with_failures_not_accepted(self, mock_hook_class, mock_airflow_context) -> None:
        """Test poke raises when completed_with_failures and not accepting."""
        mock_hook = MagicMock()
        mock_hook_class.return_value = mock_hook
        mock_hook.get_job_completion_status_sync.return_value = JobCompletionStatus.COMPLETED_WITH_FAILURES

        sensor = HevoSensor(
            task_id="test_sensor",
            pipeline_id=123,
            job_id="job_123",
            accept_completed_with_failures=False,
            connection_id="hevo_test_connection",
        )

        with pytest.raises(AirflowException, match="Job job_123 failed"):
            sensor.poke(mock_airflow_context)

    def test_poke_auto_discovery_no_job_yet(self, mock_hook_class, mock_airflow_context) -> None:
        """Test poke returns False when auto-discovery finds no job yet."""
        sensor = HevoSensor(task_id="test_sensor", pipeline_id=123, connection_id="hevo_test_connection")

        with patch.object(sensor, "_get_job_id", return_value=None):
            result = sensor.poke(mock_airflow_context)

            assert result is False

    def test_poke_auto_discovery_finds_and_caches_job(self, mock_hook_class, mock_airflow_context) -> None:
        """Test poke auto-discovers job and caches the job_id."""
        mock_hook = MagicMock()
        mock_hook_class.return_value = mock_hook
        mock_hook.get_job_completion_status_sync.return_value = JobCompletionStatus.PENDING

        sensor = HevoSensor(task_id="test_sensor", pipeline_id=123, connection_id="hevo_test_connection")

        with patch.object(sensor, "_get_job_id", return_value="job_discovered"):
            result = sensor.poke(mock_airflow_context)

            assert result is False
            # Job ID should be cached for subsequent polls
            assert sensor.job_id == "job_discovered"


class TestHevoSensorExecuteComplete:
    """Tests for execute_complete callback method."""

    def test_execute_complete_success(self, mock_airflow_context) -> None:
        """Test execute_complete processes success event correctly."""
        sensor = HevoSensor(task_id="test_sensor", pipeline_id=123, connection_id="hevo_test_connection")

        event = {"status": "success", "message": "Job completed successfully", "job_id": "job_123"}

        # Should not raise
        sensor.execute_complete(mock_airflow_context, event)

    def test_execute_complete_success_with_failures(self, mock_airflow_context) -> None:
        """Test execute_complete handles completed_with_failures flag."""
        sensor = HevoSensor(task_id="test_sensor", pipeline_id=123, connection_id="hevo_test_connection")

        event = {
            "status": "success",
            "message": "Job completed with failures",
            "job_id": "job_123",
            "completed_with_failures": True,
        }

        # Should not raise, just log warning
        sensor.execute_complete(mock_airflow_context, event)

    def test_execute_complete_error(self, mock_airflow_context) -> None:
        """Test execute_complete raises on error event."""
        sensor = HevoSensor(task_id="test_sensor", pipeline_id=123, connection_id="hevo_test_connection")

        event = {"status": "error", "message": "Job failed", "job_id": "job_123"}

        with pytest.raises(AirflowException, match="error: Job failed"):
            sensor.execute_complete(mock_airflow_context, event)

    def test_execute_complete_none_event(self, mock_airflow_context) -> None:
        """Test execute_complete handles None event gracefully."""
        sensor = HevoSensor(task_id="test_sensor", pipeline_id=123, connection_id="hevo_test_connection")

        # Should not raise, just log warning
        sensor.execute_complete(mock_airflow_context, None)


@patch("airflow.hevo.sensor.HevoPipelineHook")
class TestHevoSensorHookProperty:
    """Tests for hook cached property."""

    def test_hook_property_creates_hook_instance(self, mock_hook_class) -> None:
        """Test hook property creates HevoPipelineHook with correct parameters."""
        sensor = HevoSensor(task_id="test_sensor", pipeline_id=123, connection_id="test_conn")
        _ = sensor.hook  # Access hook property to trigger creation

        mock_hook_class.assert_called_once_with(connection_id="test_conn")


@patch("airflow.hevo.sensor.HevoPipelineHook")
class TestHevoSensorIntegrationScenarios:
    """Integration-style tests for common sensor usage patterns."""

    def test_sensor_with_xcom_job_id_template(self, mock_hook_class, mock_airflow_context) -> None:
        """Test sensor with templated job_id from XCom (already resolved)."""
        mock_hook = MagicMock()
        mock_hook_class.return_value = mock_hook
        mock_hook.get_job_completion_status_sync.return_value = JobCompletionStatus.COMPLETED

        # Simulates: job_id="{{ ti.xcom_pull(task_ids='trigger') }}"
        sensor = HevoSensor(
            task_id="test_sensor",
            pipeline_id=123,
            job_id="job_from_xcom",  # Already resolved by templating
            connection_id="hevo_test_connection",
        )

        result = sensor.poke(mock_airflow_context)

        assert result is True

    @patch("airflow.hevo.sensor.sleep")
    def test_sensor_auto_discovery_historical_job(self, mock_sleep, mock_hook_class, mock_airflow_context) -> None:
        """Test sensor auto-discovery for HISTORICAL job type."""
        mock_hook = MagicMock()
        mock_hook_class.return_value = mock_hook

        historical_job = create_job_response(job_id="job_hist_456", type="HISTORICAL", status="IN_PROGRESS")
        mock_hook.find_active_job_by_type_sync.return_value = Job(**historical_job)

        sensor = HevoSensor(
            task_id="test_sensor",
            pipeline_id=123,
            job_type=JobType.HISTORICAL,
            wait_for_job_initial_delay=0,
            connection_id="hevo_test_connection",
        )

        job_id = sensor._get_job_id()

        assert job_id == "job_hist_456"

    def test_sensor_full_workflow_auto_discovery_to_completion(self, mock_hook_class, mock_airflow_context) -> None:
        """Test full sensor workflow: auto-discovery -> caching -> completion."""
        mock_hook = MagicMock()
        mock_hook_class.return_value = mock_hook

        sensor = HevoSensor(task_id="test_sensor", pipeline_id=123, connection_id="hevo_test_connection")

        # First poke: auto-discover job
        with patch.object(sensor, "_get_job_id", return_value="job_auto_123"):
            mock_hook.get_job_completion_status_sync.return_value = JobCompletionStatus.PENDING
            result1 = sensor.poke(mock_airflow_context)
            assert result1 is False
            assert sensor.job_id == "job_auto_123"  # Cached

        # Second poke: use cached job_id, still pending
        mock_hook.get_job_completion_status_sync.return_value = JobCompletionStatus.PENDING
        result2 = sensor.poke(mock_airflow_context)
        assert result2 is False

        # Third poke: job completed
        mock_hook.get_job_completion_status_sync.return_value = JobCompletionStatus.COMPLETED
        result3 = sensor.poke(mock_airflow_context)
        assert result3 is True
