"""Unit tests for HevoPipelineHook."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from airflow.exceptions import AirflowException

from airflow.hevo.hooks import HevoPipelineHook
from airflow.hevo.models.job import Job, JobCompletionStatus, JobStatus, JobType
from airflow.hevo.models.pipeline import Pipeline, PipelineStatus, ResyncMode


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


class TestHevoPipelineHookInit:
    """Tests for HevoPipelineHook initialization."""

    def test_hook_initialization_with_defaults(self) -> None:
        """Test hook initializes with default parameters."""
        hook = HevoPipelineHook()
        assert hook.connection_id is HevoPipelineHook.default_conn_name
        assert hook.retry_limit == 3  # Default retry limit is 3

    def test_hook_initialization_with_custom_params(self) -> None:
        """Test hook initializes with custom parameters."""
        # Mock the get_connection to avoid needing actual connection setup
        with patch.object(HevoPipelineHook, "get_connection") as mock_get_conn:
            mock_conn = MagicMock()
            mock_conn.host = "us.hevodata.com"
            mock_conn.login = "test_user"
            mock_conn.password = "test_pass"
            mock_conn.extra_dejson = {}
            mock_get_conn.return_value = mock_conn

            hook = HevoPipelineHook(connection_id="custom_conn", retry_limit=5)
            assert hook.connection_id == "custom_conn"
            assert hook.retry_limit == 5


class TestGetPipelineAsync:
    """Tests for get_pipeline_async method."""

    @pytest.mark.asyncio
    async def test_get_pipeline_success(self, sample_pipeline_response) -> None:
        """Test successful pipeline retrieval."""
        hook = HevoPipelineHook()

        with patch.object(hook, "execute_api_request_async", new_callable=AsyncMock) as mock_request:
            mock_request.return_value = sample_pipeline_response

            pipeline = await hook.get_pipeline_async(123)

            assert isinstance(pipeline, Pipeline)
            assert pipeline.id == 123
            assert pipeline.name == "Test Pipeline"
            assert pipeline.status == PipelineStatus.INITIALIZED
            mock_request.assert_called_once_with(method="GET", endpoint="/api/v1/pipelines/123")

    @pytest.mark.asyncio
    async def test_get_pipeline_not_found(self) -> None:
        """Test pipeline not found returns None."""
        hook = HevoPipelineHook()

        with patch.object(hook, "execute_api_request_async", new_callable=AsyncMock) as mock_request:
            mock_request.side_effect = AirflowException("404 Not Found")

            pipeline = await hook.get_pipeline_async(123)

            assert pipeline is None

    @pytest.mark.asyncio
    async def test_get_pipeline_api_error(self) -> None:
        """Test pipeline retrieval with non-404 error raises."""
        hook = HevoPipelineHook()

        with patch.object(hook, "execute_api_request_async", new_callable=AsyncMock) as mock_request:
            mock_request.side_effect = AirflowException("500 Server Error")

            with pytest.raises(AirflowException, match="500 Server Error"):
                await hook.get_pipeline_async(123)


class TestGetPipelineSync:
    """Tests for get_pipeline synchronous wrapper."""

    def test_get_pipeline_sync_wrapper(self, sample_pipeline_response) -> None:
        """Test synchronous wrapper calls async method."""
        hook = HevoPipelineHook()

        with patch.object(hook, "get_pipeline_async", new_callable=AsyncMock) as mock_async:
            mock_async.return_value = Pipeline(**sample_pipeline_response)

            pipeline = hook.get_pipeline_sync(123)

            assert isinstance(pipeline, Pipeline)
            assert pipeline.id == 123
            mock_async.assert_called_once_with(123)


class TestValidatePipelineAsync:
    """Tests for validate_pipeline_async method."""

    @pytest.mark.asyncio
    async def test_validate_pipeline_success(self, sample_pipeline_response) -> None:
        """Test successful pipeline validation."""
        hook = HevoPipelineHook()

        with patch.object(hook, "get_pipeline_async", new_callable=AsyncMock) as mock_get:
            mock_get.return_value = Pipeline(**sample_pipeline_response)

            # Should not raise
            await hook._validate_pipeline_async(123)

            mock_get.assert_called_once_with(123)

    @pytest.mark.asyncio
    async def test_validate_pipeline_not_found(self) -> None:
        """Test validation when pipeline not found."""
        hook = HevoPipelineHook()

        with patch.object(hook, "get_pipeline_async", new_callable=AsyncMock) as mock_get:
            mock_get.return_value = None

            with pytest.raises(AirflowException, match="does not exist"):
                await hook._validate_pipeline_async(123)

    @pytest.mark.asyncio
    async def test_validate_pipeline_not_initialized(self, sample_pipeline_response) -> None:
        """Test validation when pipeline not in INITIALIZED state."""
        hook = HevoPipelineHook()
        sample_pipeline_response["status"] = "DISABLED"

        with patch.object(hook, "get_pipeline_async", new_callable=AsyncMock) as mock_get:
            mock_get.return_value = Pipeline(**sample_pipeline_response)

            with pytest.raises(AirflowException, match="not in active state"):
                await hook._validate_pipeline_async(123)


class TestValidatePipelineSync:
    """Tests for validate_pipeline synchronous wrapper."""

    def test_validate_pipeline_sync(self, sample_pipeline_response) -> None:
        """Test synchronous validate_pipeline wrapper."""
        hook = HevoPipelineHook()

        with patch.object(hook, "_validate_pipeline_async", new_callable=AsyncMock) as mock_async:
            mock_async.return_value = None

            # Should not raise
            hook.validate_pipeline(123)

            mock_async.assert_called_once()


class TestTriggerPipelineSyncAsync:
    """Tests for trigger_pipeline_sync_async method."""

    @pytest.mark.asyncio
    async def test_trigger_sync_success(self) -> None:
        """Test successful sync trigger."""
        hook = HevoPipelineHook()

        with patch.object(hook, "execute_api_request_async", new_callable=AsyncMock) as mock_request:
            mock_request.return_value = {}

            await hook.trigger_pipeline_sync_async(123)

            mock_request.assert_called_once_with(method="POST", endpoint="/api/v1/pipelines/123/actions/sync-now")

    @pytest.mark.asyncio
    async def test_trigger_sync_with_ensure_new_job_conflict(self) -> None:
        """Test trigger with ensure_new_job when job already exists."""
        hook = HevoPipelineHook()

        with patch.object(hook, "execute_api_request_async", new_callable=AsyncMock) as mock_request:
            mock_request.side_effect = AirflowException("500 job is in progress")

            with pytest.raises(AirflowException, match="job is in progress"):
                await hook.trigger_pipeline_sync_async(123, ensure_new_job=True)


class TestTriggerPipelineSyncSync:
    """Tests for trigger_pipeline_sync synchronous wrapper."""

    def test_trigger_pipeline_sync_sync(self) -> None:
        """Test synchronous trigger_pipeline_sync wrapper."""
        hook = HevoPipelineHook()

        with patch.object(hook, "trigger_pipeline_sync_async", new_callable=AsyncMock) as mock_async:
            mock_async.return_value = None

            hook.trigger_pipeline_sync(123)

            mock_async.assert_called_once()


class TestFindActiveJobByTypeAsync:
    """Tests for find_active_job_by_type_async method."""

    @pytest.mark.asyncio
    async def test_find_active_job_success(self, sample_job_response, sample_jobs_list_response) -> None:
        """Test successful job discovery."""
        hook = HevoPipelineHook()

        with patch.object(hook, "execute_api_request_async", new_callable=AsyncMock) as mock_request:
            mock_request.return_value = sample_jobs_list_response

            job = await hook.find_active_job_by_type_async(123, JobType.INCREMENTAL)

            assert isinstance(job, Job)
            assert job.job_id == sample_job_response["job_id"]
            assert job.status == JobStatus.IN_PROGRESS

    @pytest.mark.asyncio
    async def test_find_active_job_no_match(self, sample_completed_job_response) -> None:
        """Test job discovery when no matching jobs found."""
        hook = HevoPipelineHook()

        # Return a completed job (not IN_PROGRESS)
        response = {"data": [sample_completed_job_response], "has_more": False}

        with patch.object(hook, "execute_api_request_async", new_callable=AsyncMock) as mock_request:
            mock_request.return_value = response

            with pytest.raises(AirflowException, match="doesn't have any active jobs"):
                await hook.find_active_job_by_type_async(123, JobType.INCREMENTAL)

    @pytest.mark.asyncio
    async def test_find_active_job_pagination(self, sample_job_response) -> None:
        """Test job discovery with pagination."""
        hook = HevoPipelineHook()

        # First page: no matches
        first_page = {
            "data": [create_job_response(job_id="job_other", type="HISTORICAL", status="IN_PROGRESS")],
            "has_more": True,
            "next_cursor": "cursor_123",
        }

        # Second page: has match
        second_page = {"data": [sample_job_response], "has_more": False}

        with patch.object(hook, "execute_api_request_async", new_callable=AsyncMock) as mock_request:
            mock_request.side_effect = [first_page, second_page]

            job = await hook.find_active_job_by_type_async(123, JobType.INCREMENTAL)

            assert job.job_id == sample_job_response["job_id"]
            assert mock_request.call_count == 2

    @pytest.mark.asyncio
    async def test_find_active_job_with_string_job_type(self, sample_jobs_list_response) -> None:
        """Test job discovery rejects string job_type (type safety)."""
        hook = HevoPipelineHook()

        # Pass string instead of enum - should raise validation error
        with pytest.raises(AirflowException, match="job_type must be a JobType enum"):
            await hook.find_active_job_by_type_async(123, "INCREMENTAL")

    @pytest.mark.asyncio
    async def test_find_active_job_type_comparison_with_enum(self) -> None:
        """Test job type comparison works correctly with enum inputs.

        Verifies that job type filtering works with JobType enums.
        """
        hook = HevoPipelineHook()

        # Create jobs with different types (all IN_PROGRESS status)
        jobs_response = {
            "data": [
                create_job_response(job_id="job_historical", type="HISTORICAL", status="IN_PROGRESS"),
                create_job_response(job_id="job_incremental", type="INCREMENTAL", status="IN_PROGRESS"),
            ],
            "has_more": False,
        }

        with patch.object(hook, "execute_api_request_async", new_callable=AsyncMock) as mock_request:
            mock_request.return_value = jobs_response

            # Test 1: Pass JobType enum, should find INCREMENTAL job
            job = await hook.find_active_job_by_type_async(123, JobType.INCREMENTAL)
            assert job.job_id == "job_incremental"
            assert job.type == JobType.INCREMENTAL

            # Reset mock
            mock_request.reset_mock()
            mock_request.return_value = jobs_response

            # Test 2: Pass HISTORICAL enum, should find HISTORICAL job
            job = await hook.find_active_job_by_type_async(123, JobType.HISTORICAL)
            assert job.job_id == "job_historical"
            assert job.type == JobType.HISTORICAL

    @pytest.mark.asyncio
    async def test_find_active_job_type_mismatch_raises_exception(self) -> None:
        """Test that searching for non-existent job type raises exception."""
        hook = HevoPipelineHook()

        # Only INCREMENTAL jobs available
        jobs_response = {
            "data": [create_job_response(job_id="job_incremental", type="INCREMENTAL", status="IN_PROGRESS")],
            "has_more": False,
        }

        with patch.object(hook, "execute_api_request_async", new_callable=AsyncMock) as mock_request:
            mock_request.return_value = jobs_response

            # Search for HISTORICAL job type (not present)
            with pytest.raises(AirflowException, match="doesn't have any active jobs of type HISTORICAL"):
                await hook.find_active_job_by_type_async(123, JobType.HISTORICAL)


class TestFindActiveJobByTypeSync:
    """Tests for find_active_job_by_type synchronous wrapper."""

    def test_find_active_job_by_type_sync(self, sample_job_response, sample_jobs_list_response) -> None:
        """Test synchronous find_active_job_by_type wrapper."""
        hook = HevoPipelineHook()

        with patch.object(hook, "find_active_job_by_type_async", new_callable=AsyncMock) as mock_async:
            mock_async.return_value = Job(**sample_job_response)

            job = hook.find_active_job_by_type_sync(123, JobType.INCREMENTAL)

            assert isinstance(job, Job)
            assert job.job_id == sample_job_response["job_id"]
            mock_async.assert_called_once()


class TestGetJobCompletionStatusAsync:
    """Tests for get_job_completion_status_async method."""

    @pytest.mark.asyncio
    async def test_get_status_completed(self, sample_completed_job_response) -> None:
        """Test getting status for completed job."""
        hook = HevoPipelineHook()

        with patch.object(hook, "execute_api_request_async", new_callable=AsyncMock) as mock_request:
            mock_request.return_value = sample_completed_job_response

            status = await hook.get_job_completion_status_async(123, "job_completed")

            assert status == JobCompletionStatus.COMPLETED

    @pytest.mark.asyncio
    async def test_get_status_completed_with_failures_accepted(self) -> None:
        """Test status for job completed with failures (accepted)."""
        hook = HevoPipelineHook()

        job_data = create_job_response(job_id="job_123", type="INCREMENTAL", status="COMPLETED_WITH_FAILURES")

        with patch.object(hook, "execute_api_request_async", new_callable=AsyncMock) as mock_request:
            mock_request.return_value = job_data

            status = await hook.get_job_completion_status_async(123, "job_123", accept_completed_with_failures=True)

            assert status == JobCompletionStatus.COMPLETED_WITH_FAILURES

    @pytest.mark.asyncio
    async def test_get_status_completed_with_failures_not_accepted(self) -> None:
        """Test status for job completed with failures (not accepted)."""
        hook = HevoPipelineHook()

        job_data = create_job_response(job_id="job_123", type="INCREMENTAL", status="COMPLETED_WITH_FAILURES")

        with patch.object(hook, "execute_api_request_async", new_callable=AsyncMock) as mock_request:
            mock_request.return_value = job_data

            status = await hook.get_job_completion_status_async(123, "job_123")

            assert status == JobCompletionStatus.FAILED

    @pytest.mark.asyncio
    async def test_get_status_failed(self, sample_failed_job_response) -> None:
        """Test getting status for failed job."""
        hook = HevoPipelineHook()

        with patch.object(hook, "execute_api_request_async", new_callable=AsyncMock) as mock_request:
            mock_request.return_value = sample_failed_job_response

            status = await hook.get_job_completion_status_async(123, "job_failed")

            assert status == JobCompletionStatus.FAILED

    @pytest.mark.asyncio
    async def test_get_status_pending(self, sample_job_response) -> None:
        """Test getting status for pending job."""
        hook = HevoPipelineHook()

        with patch.object(hook, "execute_api_request_async", new_callable=AsyncMock) as mock_request:
            mock_request.return_value = sample_job_response

            status = await hook.get_job_completion_status_async(123, "job_789")

            assert status == JobCompletionStatus.PENDING

    @pytest.mark.asyncio
    async def test_get_status_all_failure_states(self) -> None:
        """Test that various failure states map to FAILED."""
        hook = HevoPipelineHook()

        failure_states = ["FAILED", "CANCELLED", "SKIPPED", "DEFERRED_WITH_FAILURES"]

        for failure_state in failure_states:
            job_data = create_job_response(job_id="job_123", type="INCREMENTAL", status=failure_state)

            with patch.object(hook, "execute_api_request_async", new_callable=AsyncMock) as mock_request:
                mock_request.return_value = job_data

                status = await hook.get_job_completion_status_async(123, "job_123")

                assert status == JobCompletionStatus.FAILED, f"Status {failure_state} should map to FAILED"

    @pytest.mark.asyncio
    async def test_get_status_unknown_status_from_api(self) -> None:
        """Test that unknown status from API raises an exception."""
        hook = HevoPipelineHook()

        # Simulate API returning a new status that doesn't exist in our enum
        job_data = create_job_response(job_id="job_new", type="INCREMENTAL", status="NEW_FUTURE_STATUS")

        with patch.object(hook, "execute_api_request_async", new_callable=AsyncMock) as mock_request:
            mock_request.return_value = job_data

            # Unknown status should raise ValidationError
            with pytest.raises(Exception) as exc_info:  # noqa: PT011
                await hook.get_job_completion_status_async(123, "job_new")

            # Verify it's a validation error with helpful message
            assert "Unknown job status" in str(exc_info.value) or "NEW_FUTURE_STATUS" in str(exc_info.value)


class TestGetJobCompletionStatusSync:
    """Tests for get_job_completion_status synchronous wrapper."""

    def test_get_job_completion_status_sync(self, sample_completed_job_response) -> None:
        """Test synchronous get_job_completion_status wrapper."""
        hook = HevoPipelineHook()

        with patch.object(hook, "get_job_completion_status_async", new_callable=AsyncMock) as mock_async:
            mock_async.return_value = JobCompletionStatus.COMPLETED

            status = hook.get_job_completion_status_sync(123, "job_completed")

            assert status == JobCompletionStatus.COMPLETED
            mock_async.assert_called_once()


class TestHevoPipelineHookIntegration:
    """Integration-style tests for common hook usage patterns."""

    @pytest.mark.asyncio
    async def test_full_workflow_validate_trigger_discover_monitor(
        self, sample_pipeline_response, sample_job_response, sample_completed_job_response, sample_jobs_list_response
    ) -> None:
        """Test full workflow: validate -> trigger -> discover -> monitor."""
        hook = HevoPipelineHook()

        with patch.object(hook, "execute_api_request_async", new_callable=AsyncMock) as mock_request:
            # Step 1: Validate pipeline
            mock_request.return_value = sample_pipeline_response
            pipeline = await hook.get_pipeline_async(123)
            assert pipeline.status == PipelineStatus.INITIALIZED

            # Step 2: Trigger sync
            mock_request.return_value = {}
            await hook.trigger_pipeline_sync_async(123)

            # Step 3: Discover active job
            mock_request.return_value = sample_jobs_list_response
            job = await hook.find_active_job_by_type_async(123, JobType.INCREMENTAL)
            assert job.job_id == sample_job_response["job_id"]

            # Step 4: Monitor job status (pending -> completed)
            mock_request.side_effect = [
                sample_job_response,  # IN_PROGRESS
                sample_completed_job_response,  # COMPLETED
            ]

            status1 = await hook.get_job_completion_status_async(123, sample_job_response["job_id"])
            assert status1 == JobCompletionStatus.PENDING

            status2 = await hook.get_job_completion_status_async(123, sample_job_response["job_id"])
            assert status2 == JobCompletionStatus.COMPLETED

    def test_sync_wrappers_use_asyncio_run(self) -> None:
        """Test that sync wrappers properly use asyncio.run."""
        hook = HevoPipelineHook()

        with patch("asyncio.run") as mock_asyncio_run:
            with patch.object(hook, "get_pipeline_async", new_callable=AsyncMock):
                # This should call asyncio.run internally
                hook.get_pipeline_sync(123)

                # Verify asyncio.run was called
                mock_asyncio_run.assert_called_once()


class TestHevoPipelineHookErrorHandling:
    """Tests for error handling scenarios."""

    @pytest.mark.asyncio
    async def test_api_request_failure_propagates(self) -> None:
        """Test that API request failures propagate correctly."""
        hook = HevoPipelineHook()

        with patch.object(hook, "execute_api_request_async", new_callable=AsyncMock) as mock_request:
            mock_request.side_effect = AirflowException("Network error")

            with pytest.raises(AirflowException, match="Network error"):
                await hook.get_pipeline_async(123)

    @pytest.mark.asyncio
    async def test_invalid_pipeline_id(self) -> None:
        """Test handling of invalid pipeline ID."""
        hook = HevoPipelineHook()

        with patch.object(hook, "execute_api_request_async", new_callable=AsyncMock) as mock_request:
            mock_request.side_effect = AirflowException("404 Not Found")

            pipeline = await hook.get_pipeline_async(999)
            assert pipeline is None

    @pytest.mark.asyncio
    async def test_pagination_max_limit(self, sample_job_response) -> None:
        """Test pagination works through multiple pages with correct page_limit."""
        hook = HevoPipelineHook()

        # Create a sequence of responses - first two pages have wrong type, last page signals end
        responses = [
            # Page 1: HISTORICAL job (wrong type), has more pages
            {
                "data": [create_job_response(job_id="job_historical_1", type="HISTORICAL", status="IN_PROGRESS")],
                "has_more": True,
                "next_cursor": "cursor_2",
            },
            # Page 2: Another HISTORICAL job, has more pages
            {
                "data": [create_job_response(job_id="job_historical_2", type="HISTORICAL", status="IN_PROGRESS")],
                "has_more": True,
                "next_cursor": "cursor_3",
            },
            # Page 3: Still wrong type, but no more pages
            {
                "data": [create_job_response(job_id="job_historical_3", type="HISTORICAL", status="IN_PROGRESS")],
                "has_more": False,
                "next_cursor": None,
            },
        ]

        with patch.object(hook, "execute_api_request_async", new_callable=AsyncMock) as mock_request:
            # Return different responses for each call
            mock_request.side_effect = responses

            with pytest.raises(AirflowException, match="doesn't have any active jobs"):
                # Should paginate through all pages looking for INCREMENTAL job
                await hook.find_active_job_by_type_async(123, JobType.INCREMENTAL, page_limit=1)

            # Verify it tried all 3 pages
            assert mock_request.call_count == 3


class TestUpdatePipelineAsync:
    """Tests for update_pipeline_async method."""

    @pytest.mark.asyncio
    async def test_update_pipeline_success(self, sample_pipeline_response) -> None:
        """Test successful pipeline update."""
        hook = HevoPipelineHook()
        update_config = {"name": "Updated Pipeline"}

        with patch.object(hook, "execute_api_request_async", new_callable=AsyncMock) as mock_request:
            mock_request.return_value = sample_pipeline_response

            pipeline = await hook.update_pipeline_async(123, update_config)

            assert isinstance(pipeline, Pipeline)
            mock_request.assert_called_once_with(
                method="PATCH", endpoint="/api/v1/pipelines/123", payload=update_config
            )


class TestUpdatePipelineSync:
    """Tests for update_pipeline synchronous wrapper."""

    def test_update_pipeline_sync_wrapper(self, sample_pipeline_response) -> None:
        """Test synchronous wrapper calls async method."""
        hook = HevoPipelineHook()
        update_config = {"name": "Updated"}

        with patch.object(hook, "update_pipeline_async", new_callable=AsyncMock) as mock_async:
            mock_async.return_value = Pipeline(**sample_pipeline_response)

            pipeline = hook.update_pipeline_sync(123, update_config)

            assert isinstance(pipeline, Pipeline)
            mock_async.assert_called_once_with(123, update_config)


class TestDisablePipelineAsync:
    """Tests for disable_pipeline_async method."""

    @pytest.mark.asyncio
    async def test_disable_pipeline_success(self) -> None:
        """Test successful pipeline disable."""
        hook = HevoPipelineHook()

        with patch.object(hook, "execute_api_request_async", new_callable=AsyncMock) as mock_request:
            mock_request.return_value = {}

            await hook.disable_pipeline_async(123)

            mock_request.assert_called_once_with(
                method="POST", endpoint="/api/v1/pipelines/123/actions/disable", payload={"cancel_active_jobs": False}
            )


class TestDisablePipelineSync:
    """Tests for disable_pipeline synchronous wrapper."""

    def test_disable_pipeline_sync_wrapper(self) -> None:
        """Test synchronous wrapper calls async method."""
        hook = HevoPipelineHook()

        with patch.object(hook, "disable_pipeline_async", new_callable=AsyncMock) as mock_async:
            mock_async.return_value = None

            hook.disable_pipeline_sync(123)

            mock_async.assert_called_once_with(123)


class TestEnablePipelineAsync:
    """Tests for enable_pipeline_async method."""

    @pytest.mark.asyncio
    async def test_enable_pipeline_success(self) -> None:
        """Test successful pipeline enable."""
        hook = HevoPipelineHook()

        with patch.object(hook, "execute_api_request_async", new_callable=AsyncMock) as mock_request:
            mock_request.return_value = {}

            await hook.enable_pipeline_async(123)

            mock_request.assert_called_once_with(method="POST", endpoint="/api/v1/pipelines/123/actions/enable")


class TestEnablePipelineSync:
    """Tests for enable_pipeline synchronous wrapper."""

    def test_enable_pipeline_sync_wrapper(self) -> None:
        """Test synchronous wrapper calls async method."""
        hook = HevoPipelineHook()

        with patch.object(hook, "enable_pipeline_async", new_callable=AsyncMock) as mock_async:
            mock_async.return_value = None

            hook.enable_pipeline_sync(123)

            mock_async.assert_called_once_with(123)


class TestResyncPipelineAsync:
    """Tests for resync_pipeline_async method."""

    @pytest.mark.asyncio
    async def test_resync_pipeline_success(self) -> None:
        """Test successful pipeline resync with default resync_mode."""
        hook = HevoPipelineHook()

        with patch.object(hook, "execute_api_request_async", new_callable=AsyncMock) as mock_request:
            mock_request.return_value = {}

            await hook.resync_pipeline_async(123)

            mock_request.assert_called_once_with(
                method="POST", endpoint="/api/v1/pipelines/123/actions/resync", payload={"resync_mode": "EVOLVE_AND_MERGE"}
            )

    @pytest.mark.asyncio
    async def test_resync_pipeline_with_drop_and_load_mode(self) -> None:
        """Test pipeline resync with resync_mode=DROP_AND_LOAD."""
        hook = HevoPipelineHook()

        with patch.object(hook, "execute_api_request_async", new_callable=AsyncMock) as mock_request:
            mock_request.return_value = {}

            await hook.resync_pipeline_async(123, resync_mode=ResyncMode.DROP_AND_LOAD)

            mock_request.assert_called_once_with(
                method="POST", endpoint="/api/v1/pipelines/123/actions/resync", payload={"resync_mode": "DROP_AND_LOAD"}
            )


class TestResyncPipelineSync:
    """Tests for resync_pipeline synchronous wrapper."""

    def test_resync_pipeline_sync_wrapper(self) -> None:
        """Test synchronous wrapper calls async method with default resync_mode."""
        hook = HevoPipelineHook()

        with patch.object(hook, "resync_pipeline_async", new_callable=AsyncMock) as mock_async:
            mock_async.return_value = None

            hook.resync_pipeline_sync(123)

            mock_async.assert_called_once_with(123, ResyncMode.EVOLVE_AND_MERGE)

    def test_resync_pipeline_sync_wrapper_with_drop_and_load_mode(self) -> None:
        """Test synchronous wrapper calls async method with resync_mode=DROP_AND_LOAD."""
        hook = HevoPipelineHook()

        with patch.object(hook, "resync_pipeline_async", new_callable=AsyncMock) as mock_async:
            mock_async.return_value = None

            hook.resync_pipeline_sync(123, resync_mode=ResyncMode.DROP_AND_LOAD)

            mock_async.assert_called_once_with(123, ResyncMode.DROP_AND_LOAD)


class TestCancelJobAsync:
    """Tests for cancel_job_async method."""

    @pytest.mark.asyncio
    async def test_cancel_job_success(self) -> None:
        """Test successful job cancellation."""
        hook = HevoPipelineHook()

        with patch.object(hook, "execute_api_request_async", new_callable=AsyncMock) as mock_request:
            mock_request.return_value = {}

            await hook.cancel_job_async(123, "job_789")

            mock_request.assert_called_once_with(
                method="POST", endpoint="/api/v1/pipelines/123/jobs/job_789/actions/cancel"
            )


class TestCancelJobSync:
    """Tests for cancel_job synchronous wrapper."""

    def test_cancel_job_sync_wrapper(self) -> None:
        """Test synchronous wrapper calls async method."""
        hook = HevoPipelineHook()

        with patch.object(hook, "cancel_job_async", new_callable=AsyncMock) as mock_async:
            mock_async.return_value = None

            hook.cancel_job_sync(123, "job_789")

            mock_async.assert_called_once_with(123, "job_789")


class TestGetJobObjectsAsync:
    """Tests for get_job_objects_async method."""

    @pytest.mark.asyncio
    async def test_get_job_objects_success(self, sample_job_objects_response) -> None:
        """Test successful job objects retrieval."""
        hook = HevoPipelineHook()

        with patch.object(hook, "execute_api_request_async", new_callable=AsyncMock) as mock_request:
            mock_request.return_value = sample_job_objects_response

            result = await hook.get_job_objects_async(123, "job_789", limit=100)

            assert "data" in result
            assert len(result["data"]) == 2
            assert result["data"][0]["object_name"] == "users"
            mock_request.assert_called_once_with(
                method="GET", endpoint="/api/v1/pipelines/123/jobs/job_789/objects", params={"limit": 100}
            )

    @pytest.mark.asyncio
    async def test_get_job_objects_with_cursor(self, sample_job_objects_response) -> None:
        """Test job objects retrieval with cursor pagination."""
        hook = HevoPipelineHook()

        with patch.object(hook, "execute_api_request_async", new_callable=AsyncMock) as mock_request:
            mock_request.return_value = sample_job_objects_response

            await hook.get_job_objects_async(123, "job_789", limit=50, cursor="cursor_xyz")

            mock_request.assert_called_once_with(
                method="GET",
                endpoint="/api/v1/pipelines/123/jobs/job_789/objects",
                params={"limit": 50, "cursor": "cursor_xyz"},
            )


class TestGetJobObjectsSync:
    """Tests for get_job_objects synchronous wrapper."""

    def test_get_job_objects_sync_wrapper(self, sample_job_objects_response) -> None:
        """Test synchronous wrapper calls async method."""
        hook = HevoPipelineHook()

        with patch.object(hook, "get_job_objects_async", new_callable=AsyncMock) as mock_async:
            mock_async.return_value = sample_job_objects_response

            result = hook.get_job_objects_sync(123, "job_789", limit=100)

            assert "data" in result
            mock_async.assert_called_once_with(123, "job_789", 100, None)
