"""Unit tests for HevoPipelineHook."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from airflow.exceptions import AirflowException

from airflow.hevo.hooks.hevo_pipeline_hook import HevoPipelineHook
from airflow.hevo.models.job import Job, JobCompletionStatus, JobStatus, JobType, PaginatedJobsResponse
from airflow.hevo.models.pipeline import Pipeline, PipelineStatus, SyncType


class TestHevoPipelineHookInit:
    """Tests for HevoPipelineHook initialization."""

    def test_hook_initialization_with_defaults(self):
        """Test hook initializes with default parameters."""
        hook = HevoPipelineHook(pipeline_id=123)
        assert hook.pipeline_id == 123
        assert hook.connection_id is HevoPipelineHook.default_conn_name
        assert hook.retry_limit == 3  # Default retry limit is 3

    def test_hook_initialization_with_custom_params(self):
        """Test hook initializes with custom parameters."""
        # Mock the get_connection to avoid needing actual connection setup
        with patch.object(HevoPipelineHook, 'get_connection') as mock_get_conn:
            mock_conn = MagicMock()
            mock_conn.host = "us.hevodata.com"
            mock_conn.login = "test_user"
            mock_conn.password = "test_pass"
            mock_conn.extra_dejson = {}
            mock_get_conn.return_value = mock_conn

            hook = HevoPipelineHook(
                pipeline_id=456,
                connection_id="custom_conn",
                retry_limit=5
            )
            assert hook.pipeline_id == 456
            assert hook.connection_id == "custom_conn"
            assert hook.retry_limit == 5


class TestGetPipelineAsync:
    """Tests for get_pipeline_async method."""

    @pytest.mark.asyncio
    async def test_get_pipeline_success(self, sample_pipeline_response):
        """Test successful pipeline retrieval."""
        hook = HevoPipelineHook(pipeline_id=123)

        with patch.object(hook, 'execute_api_request_async', new_callable=AsyncMock) as mock_request:
            mock_request.return_value = sample_pipeline_response

            pipeline = await hook.get_pipeline_async(123)

            assert isinstance(pipeline, Pipeline)
            assert pipeline.id == 123
            assert pipeline.name == "Test Pipeline"
            assert pipeline.status == PipelineStatus.INITIALIZED
            mock_request.assert_called_once_with(
                method="GET",
                endpoint="/api/v1/pipelines/123"
            )

    @pytest.mark.asyncio
    async def test_get_pipeline_not_found(self):
        """Test pipeline not found returns None."""
        hook = HevoPipelineHook(pipeline_id=123)

        with patch.object(hook, 'execute_api_request_async', new_callable=AsyncMock) as mock_request:
            mock_request.side_effect = AirflowException("404 Not Found")

            pipeline = await hook.get_pipeline_async(123)

            assert pipeline is None

    @pytest.mark.asyncio
    async def test_get_pipeline_api_error(self):
        """Test pipeline retrieval with non-404 error raises."""
        hook = HevoPipelineHook(pipeline_id=123)

        with patch.object(hook, 'execute_api_request_async', new_callable=AsyncMock) as mock_request:
            mock_request.side_effect = AirflowException("500 Server Error")

            with pytest.raises(AirflowException, match="500 Server Error"):
                await hook.get_pipeline_async(123)


class TestGetPipelineSync:
    """Tests for get_pipeline synchronous wrapper."""

    def test_get_pipeline_sync_wrapper(self, sample_pipeline_response):
        """Test synchronous wrapper calls async method."""
        hook = HevoPipelineHook(pipeline_id=123)

        with patch.object(hook, 'get_pipeline_async', new_callable=AsyncMock) as mock_async:
            mock_async.return_value = Pipeline(**sample_pipeline_response)

            pipeline = hook.get_pipeline(123)

            assert isinstance(pipeline, Pipeline)
            assert pipeline.id == 123
            mock_async.assert_called_once_with(123)


class TestValidatePipelineAsync:
    """Tests for validate_pipeline_async method."""

    @pytest.mark.asyncio
    async def test_validate_pipeline_success(self, sample_pipeline_response):
        """Test successful pipeline validation."""
        hook = HevoPipelineHook(pipeline_id=123)

        with patch.object(hook, 'get_pipeline_async', new_callable=AsyncMock) as mock_get:
            mock_get.return_value = Pipeline(**sample_pipeline_response)

            # Should not raise
            await hook.validate_pipeline_async(123, SyncType.ON_DEMAND)

            mock_get.assert_called_once_with(123)

    @pytest.mark.asyncio
    async def test_validate_pipeline_not_found(self):
        """Test validation when pipeline not found."""
        hook = HevoPipelineHook(pipeline_id=123)

        with patch.object(hook, 'get_pipeline_async', new_callable=AsyncMock) as mock_get:
            mock_get.return_value = None

            with pytest.raises(AirflowException, match="does not exist"):
                await hook.validate_pipeline_async(123)

    @pytest.mark.asyncio
    async def test_validate_pipeline_not_initialized(self, sample_pipeline_response):
        """Test validation when pipeline not in INITIALIZED state."""
        hook = HevoPipelineHook(pipeline_id=123)
        sample_pipeline_response["status"] = "PAUSED"

        with patch.object(hook, 'get_pipeline_async', new_callable=AsyncMock) as mock_get:
            mock_get.return_value = Pipeline(**sample_pipeline_response)

            with pytest.raises(AirflowException, match="not in active state"):
                await hook.validate_pipeline_async(123)


class TestValidatePipelineSync:
    """Tests for validate_pipeline synchronous wrapper."""

    def test_validate_pipeline_sync(self, sample_pipeline_response):
        """Test synchronous validate_pipeline wrapper."""
        hook = HevoPipelineHook(pipeline_id=123)

        with patch.object(hook, 'validate_pipeline_async', new_callable=AsyncMock) as mock_async:
            mock_async.return_value = None

            # Should not raise
            hook.validate_pipeline(123)

            mock_async.assert_called_once()


class TestTriggerPipelineSyncAsync:
    """Tests for trigger_pipeline_sync_async method."""

    @pytest.mark.asyncio
    async def test_trigger_sync_success(self):
        """Test successful sync trigger."""
        hook = HevoPipelineHook(pipeline_id=123)

        with patch.object(hook, 'execute_api_request_async', new_callable=AsyncMock) as mock_request:
            mock_request.return_value = {}

            await hook.trigger_pipeline_sync_async(123)

            mock_request.assert_called_once_with(
                method="POST",
                endpoint="/api/v1/pipelines/123/actions/sync-now"
            )

    @pytest.mark.asyncio
    async def test_trigger_sync_with_ensure_new_job_conflict(self):
        """Test trigger with ensure_new_job when job already exists."""
        hook = HevoPipelineHook(pipeline_id=123)

        with patch.object(hook, 'execute_api_request_async', new_callable=AsyncMock) as mock_request:
            mock_request.side_effect = AirflowException("500 job is in progress")

            with pytest.raises(AirflowException, match="job already in progress"):
                await hook.trigger_pipeline_sync_async(123, ensure_new_job=True)


class TestTriggerPipelineSyncSync:
    """Tests for trigger_pipeline_sync synchronous wrapper."""

    def test_trigger_pipeline_sync_sync(self):
        """Test synchronous trigger_pipeline_sync wrapper."""
        hook = HevoPipelineHook(pipeline_id=123)

        with patch.object(hook, 'trigger_pipeline_sync_async', new_callable=AsyncMock) as mock_async:
            mock_async.return_value = None

            hook.trigger_pipeline_sync(123)

            mock_async.assert_called_once()


class TestFindActiveJobByTypeAsync:
    """Tests for find_active_job_by_type_async method."""

    @pytest.mark.asyncio
    async def test_find_active_job_success(self, sample_job_response, sample_jobs_list_response):
        """Test successful job discovery."""
        hook = HevoPipelineHook(pipeline_id=123)

        with patch.object(hook, 'execute_api_request_async', new_callable=AsyncMock) as mock_request:
            mock_request.return_value = sample_jobs_list_response

            job = await hook.find_active_job_by_type_async(123, JobType.INCREMENTAL)

            assert isinstance(job, Job)
            assert job.job_id == "job_789"
            assert job.status == JobStatus.IN_PROGRESS

    @pytest.mark.asyncio
    async def test_find_active_job_no_match(self, sample_completed_job_response):
        """Test job discovery when no matching jobs found."""
        hook = HevoPipelineHook(pipeline_id=123)

        # Return a completed job (not IN_PROGRESS)
        response = {
            "data": [sample_completed_job_response],
            "has_more": False
        }

        with patch.object(hook, 'execute_api_request_async', new_callable=AsyncMock) as mock_request:
            mock_request.return_value = response

            with pytest.raises(AirflowException, match="doesn't have any active jobs"):
                await hook.find_active_job_by_type_async(123, JobType.INCREMENTAL)

    @pytest.mark.asyncio
    async def test_find_active_job_pagination(self, sample_job_response):
        """Test job discovery with pagination."""
        hook = HevoPipelineHook(pipeline_id=123)

        # First page: no matches
        first_page = {
            "data": [{
                "job_id": "job_other",
                "pipeline_id": 123,
                "type": "HISTORICAL",  # Different type
                "status": "IN_PROGRESS"
            }],
            "has_more": True,
            "next_cursor": "cursor_123"
        }

        # Second page: has match
        second_page = {
            "data": [sample_job_response],
            "has_more": False
        }

        with patch.object(hook, 'execute_api_request_async', new_callable=AsyncMock) as mock_request:
            mock_request.side_effect = [first_page, second_page]

            job = await hook.find_active_job_by_type_async(123, JobType.INCREMENTAL)

            assert job.job_id == "job_789"
            assert mock_request.call_count == 2

    @pytest.mark.asyncio
    async def test_find_active_job_with_string_job_type(self, sample_jobs_list_response):
        """Test job discovery accepts string job_type for backwards compatibility."""
        hook = HevoPipelineHook(pipeline_id=123)

        with patch.object(hook, 'execute_api_request_async', new_callable=AsyncMock) as mock_request:
            mock_request.return_value = sample_jobs_list_response

            # Pass string instead of enum
            job = await hook.find_active_job_by_type_async(123, "INCREMENTAL")

            assert job.job_id == "job_789"

    @pytest.mark.asyncio
    async def test_find_active_job_type_comparison_with_enum_and_string(self):
        """Test job type comparison works correctly with both enum and string inputs.

        Regression test for bug where job.type (JobType enum) == job_type (string)
        was returning False. Verifies normalization to strings for comparison.
        """
        hook = HevoPipelineHook(pipeline_id=123)

        # Create jobs with different types (all IN_PROGRESS status)
        jobs_response = {
            "data": [
                {
                    "job_id": "job_historical",
                    "pipeline_id": 123,
                    "type": "HISTORICAL",
                    "status": "IN_PROGRESS"
                },
                {
                    "job_id": "job_incremental",
                    "pipeline_id": 123,
                    "type": "INCREMENTAL",
                    "status": "IN_PROGRESS"
                }
            ],
            "has_more": False
        }

        with patch.object(hook, 'execute_api_request_async', new_callable=AsyncMock) as mock_request:
            mock_request.return_value = jobs_response

            # Test 1: Pass JobType enum, should find INCREMENTAL job
            job = await hook.find_active_job_by_type_async(123, JobType.INCREMENTAL)
            assert job.job_id == "job_incremental"
            assert job.type == JobType.INCREMENTAL

            # Reset mock
            mock_request.reset_mock()
            mock_request.return_value = jobs_response

            # Test 2: Pass string, should find INCREMENTAL job
            job = await hook.find_active_job_by_type_async(123, "INCREMENTAL")
            assert job.job_id == "job_incremental"
            assert job.type == JobType.INCREMENTAL

            # Reset mock
            mock_request.reset_mock()
            mock_request.return_value = jobs_response

            # Test 3: Pass HISTORICAL enum, should find HISTORICAL job
            job = await hook.find_active_job_by_type_async(123, JobType.HISTORICAL)
            assert job.job_id == "job_historical"
            assert job.type == JobType.HISTORICAL

            # Reset mock
            mock_request.reset_mock()
            mock_request.return_value = jobs_response

            # Test 4: Pass HISTORICAL string, should find HISTORICAL job
            job = await hook.find_active_job_by_type_async(123, "HISTORICAL")
            assert job.job_id == "job_historical"
            assert job.type == JobType.HISTORICAL

    @pytest.mark.asyncio
    async def test_find_active_job_type_mismatch_raises_exception(self):
        """Test that searching for non-existent job type raises exception."""
        hook = HevoPipelineHook(pipeline_id=123)

        # Only INCREMENTAL jobs available
        jobs_response = {
            "data": [
                {
                    "job_id": "job_incremental",
                    "pipeline_id": 123,
                    "type": "INCREMENTAL",
                    "status": "IN_PROGRESS"
                }
            ],
            "has_more": False
        }

        with patch.object(hook, 'execute_api_request_async', new_callable=AsyncMock) as mock_request:
            mock_request.return_value = jobs_response

            # Search for HISTORICAL job type (not present)
            with pytest.raises(AirflowException, match="doesn't have any active jobs of type HISTORICAL"):
                await hook.find_active_job_by_type_async(123, JobType.HISTORICAL)


class TestFindActiveJobByTypeSync:
    """Tests for find_active_job_by_type synchronous wrapper."""

    def test_find_active_job_by_type_sync(self, sample_job_response, sample_jobs_list_response):
        """Test synchronous find_active_job_by_type wrapper."""
        hook = HevoPipelineHook(pipeline_id=123)

        with patch.object(hook, 'find_active_job_by_type_async', new_callable=AsyncMock) as mock_async:
            mock_async.return_value = Job(**sample_job_response)

            job = hook.find_active_job_by_type(123, JobType.INCREMENTAL)

            assert isinstance(job, Job)
            assert job.job_id == "job_789"
            mock_async.assert_called_once()


class TestGetJobCompletionStatusAsync:
    """Tests for get_job_completion_status_async method."""

    @pytest.mark.asyncio
    async def test_get_status_completed(self, sample_completed_job_response):
        """Test getting status for completed job."""
        hook = HevoPipelineHook(pipeline_id=123)

        with patch.object(hook, 'execute_api_request_async', new_callable=AsyncMock) as mock_request:
            mock_request.return_value = sample_completed_job_response

            status = await hook.get_job_completion_status_async(123, "job_completed")

            assert status == JobCompletionStatus.COMPLETED

    @pytest.mark.asyncio
    async def test_get_status_completed_with_failures_accepted(self):
        """Test status for job completed with failures (accepted)."""
        hook = HevoPipelineHook(pipeline_id=123)

        job_data = {
            "job_id": "job_123",
            "pipeline_id": 123,
            "type": "INCREMENTAL",
            "status": "COMPLETED_WITH_FAILURES"
        }

        with patch.object(hook, 'execute_api_request_async', new_callable=AsyncMock) as mock_request:
            mock_request.return_value = job_data

            status = await hook.get_job_completion_status_async(
                123, "job_123", accept_completed_with_failures=True
            )

            assert status == JobCompletionStatus.COMPLETED_WITH_FAILURES

    @pytest.mark.asyncio
    async def test_get_status_completed_with_failures_not_accepted(self):
        """Test status for job completed with failures (not accepted)."""
        hook = HevoPipelineHook(pipeline_id=123)

        job_data = {
            "job_id": "job_123",
            "pipeline_id": 123,
            "type": "INCREMENTAL",
            "status": "COMPLETED_WITH_FAILURES"
        }

        with patch.object(hook, 'execute_api_request_async', new_callable=AsyncMock) as mock_request:
            mock_request.return_value = job_data

            status = await hook.get_job_completion_status_async(123, "job_123")

            assert status == JobCompletionStatus.FAILED

    @pytest.mark.asyncio
    async def test_get_status_failed(self, sample_failed_job_response):
        """Test getting status for failed job."""
        hook = HevoPipelineHook(pipeline_id=123)

        with patch.object(hook, 'execute_api_request_async', new_callable=AsyncMock) as mock_request:
            mock_request.return_value = sample_failed_job_response

            status = await hook.get_job_completion_status_async(123, "job_failed")

            assert status == JobCompletionStatus.FAILED

    @pytest.mark.asyncio
    async def test_get_status_pending(self, sample_job_response):
        """Test getting status for pending job."""
        hook = HevoPipelineHook(pipeline_id=123)

        with patch.object(hook, 'execute_api_request_async', new_callable=AsyncMock) as mock_request:
            mock_request.return_value = sample_job_response

            status = await hook.get_job_completion_status_async(123, "job_789")

            assert status == JobCompletionStatus.PENDING

    @pytest.mark.asyncio
    async def test_get_status_all_failure_states(self):
        """Test that various failure states map to FAILED."""
        hook = HevoPipelineHook(pipeline_id=123)

        failure_states = ["FAILED", "CANCELLED", "SKIPPED", "DEFERRED_WITH_FAILURE"]

        for failure_state in failure_states:
            job_data = {
                "job_id": "job_123",
                "pipeline_id": 123,
                "type": "INCREMENTAL",
                "status": failure_state
            }

            with patch.object(hook, 'execute_api_request_async', new_callable=AsyncMock) as mock_request:
                mock_request.return_value = job_data

                status = await hook.get_job_completion_status_async(123, "job_123")

                assert status == JobCompletionStatus.FAILED, f"Status {failure_state} should map to FAILED"

    @pytest.mark.asyncio
    async def test_get_status_unknown_status_from_api(self):
        """Test that unknown status from API is treated as pending."""
        hook = HevoPipelineHook(pipeline_id=123)

        # Simulate API returning a new status that doesn't exist in our enum
        job_data = {
            "job_id": "job_new",
            "pipeline_id": 123,
            "type": "INCREMENTAL",
            "status": "NEW_FUTURE_STATUS"
        }

        with patch.object(hook, 'execute_api_request_async', new_callable=AsyncMock) as mock_request:
            mock_request.return_value = job_data

            status = await hook.get_job_completion_status_async(123, "job_new")

            # Unknown status should be treated as PENDING to continue monitoring
            assert status == JobCompletionStatus.PENDING


class TestGetJobCompletionStatusSync:
    """Tests for get_job_completion_status synchronous wrapper."""

    def test_get_job_completion_status_sync(self, sample_completed_job_response):
        """Test synchronous get_job_completion_status wrapper."""
        hook = HevoPipelineHook(pipeline_id=123)

        with patch.object(hook, 'get_job_completion_status_async', new_callable=AsyncMock) as mock_async:
            mock_async.return_value = JobCompletionStatus.COMPLETED

            status = hook.get_job_completion_status(123, "job_completed")

            assert status == JobCompletionStatus.COMPLETED
            mock_async.assert_called_once()


class TestHevoPipelineHookIntegration:
    """Integration-style tests for common hook usage patterns."""

    @pytest.mark.asyncio
    async def test_full_workflow_validate_trigger_discover_monitor(
        self, sample_pipeline_response, sample_job_response, sample_completed_job_response,
        sample_jobs_list_response
    ):
        """Test full workflow: validate -> trigger -> discover -> monitor."""
        hook = HevoPipelineHook(pipeline_id=123)

        with patch.object(hook, 'execute_api_request_async', new_callable=AsyncMock) as mock_request:
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
            assert job.job_id == "job_789"

            # Step 4: Monitor job status (pending -> completed)
            mock_request.side_effect = [
                sample_job_response,  # IN_PROGRESS
                sample_completed_job_response  # COMPLETED
            ]

            status1 = await hook.get_job_completion_status_async(123, "job_789")
            assert status1 == JobCompletionStatus.PENDING

            status2 = await hook.get_job_completion_status_async(123, "job_789")
            assert status2 == JobCompletionStatus.COMPLETED

    def test_sync_wrappers_use_asyncio_run(self):
        """Test that sync wrappers properly use asyncio.run."""
        hook = HevoPipelineHook(pipeline_id=123)

        with patch('asyncio.run') as mock_asyncio_run:
            with patch.object(hook, 'get_pipeline_async', new_callable=AsyncMock):
                # This should call asyncio.run internally
                hook.get_pipeline(123)

                # Verify asyncio.run was called
                mock_asyncio_run.assert_called_once()


class TestHevoPipelineHookErrorHandling:
    """Tests for error handling scenarios."""

    @pytest.mark.asyncio
    async def test_api_request_failure_propagates(self):
        """Test that API request failures propagate correctly."""
        hook = HevoPipelineHook(pipeline_id=123)

        with patch.object(hook, 'execute_api_request_async', new_callable=AsyncMock) as mock_request:
            mock_request.side_effect = AirflowException("Network error")

            with pytest.raises(AirflowException, match="Network error"):
                await hook.get_pipeline_async(123)

    @pytest.mark.asyncio
    async def test_invalid_pipeline_id(self):
        """Test handling of invalid pipeline ID."""
        hook = HevoPipelineHook(pipeline_id=999)

        with patch.object(hook, 'execute_api_request_async', new_callable=AsyncMock) as mock_request:
            mock_request.side_effect = AirflowException("404 Not Found")

            pipeline = await hook.get_pipeline_async(999)
            assert pipeline is None

    @pytest.mark.asyncio
    async def test_pagination_max_limit(self, sample_job_response):
        """Test pagination works through multiple pages with correct page_limit."""
        hook = HevoPipelineHook(pipeline_id=123)

        # Create a sequence of responses - first two pages have wrong type, last page signals end
        responses = [
            # Page 1: HISTORICAL job (wrong type), has more pages
            {
                "data": [{
                    "job_id": "job_historical_1",
                    "pipeline_id": 123,
                    "type": "HISTORICAL",
                    "status": "IN_PROGRESS"
                }],
                "has_more": True,
                "next_cursor": "cursor_2"
            },
            # Page 2: Another HISTORICAL job, has more pages
            {
                "data": [{
                    "job_id": "job_historical_2",
                    "pipeline_id": 123,
                    "type": "HISTORICAL",
                    "status": "IN_PROGRESS"
                }],
                "has_more": True,
                "next_cursor": "cursor_3"
            },
            # Page 3: Still wrong type, but no more pages
            {
                "data": [{
                    "job_id": "job_historical_3",
                    "pipeline_id": 123,
                    "type": "HISTORICAL",
                    "status": "IN_PROGRESS"
                }],
                "has_more": False,
                "next_cursor": None
            }
        ]

        with patch.object(hook, 'execute_api_request_async', new_callable=AsyncMock) as mock_request:
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
    async def test_update_pipeline_success(self, sample_pipeline_response):
        """Test successful pipeline update."""
        hook = HevoPipelineHook(pipeline_id=123)
        update_config = {"name": "Updated Pipeline"}

        with patch.object(hook, 'execute_api_request_async', new_callable=AsyncMock) as mock_request:
            mock_request.return_value = sample_pipeline_response

            pipeline = await hook.update_pipeline_async(123, update_config)

            assert isinstance(pipeline, Pipeline)
            mock_request.assert_called_once_with(
                method="PATCH",
                endpoint="/api/v1/pipelines/123",
                json=update_config
            )


class TestUpdatePipelineSync:
    """Tests for update_pipeline synchronous wrapper."""

    def test_update_pipeline_sync_wrapper(self, sample_pipeline_response):
        """Test synchronous wrapper calls async method."""
        hook = HevoPipelineHook(pipeline_id=123)
        update_config = {"name": "Updated"}

        with patch.object(hook, 'update_pipeline_async', new_callable=AsyncMock) as mock_async:
            mock_async.return_value = Pipeline(**sample_pipeline_response)

            pipeline = hook.update_pipeline(123, update_config)

            assert isinstance(pipeline, Pipeline)
            mock_async.assert_called_once_with(123, update_config)

class TestUpdatePipelineSourcesAsync:
    """Tests for update_pipeline_sources_async method."""

    @pytest.mark.asyncio
    async def test_update_sources_success(self):
        """Test successful source update."""
        hook = HevoPipelineHook(pipeline_id=123)
        sources_config = {"config": {"host": "newhost.com"}}

        with patch.object(hook, 'execute_api_request_async', new_callable=AsyncMock) as mock_request:
            mock_request.return_value = sources_config

            result = await hook.update_pipeline_sources_async(123, sources_config)

            assert result == sources_config
            mock_request.assert_called_once_with(
                method="PATCH",
                endpoint="/api/v1/pipelines/123/sources",
                json=sources_config
            )


class TestUpdatePipelineSourcesSync:
    """Tests for update_pipeline_sources synchronous wrapper."""

    def test_update_sources_sync_wrapper(self):
        """Test synchronous wrapper calls async method."""
        hook = HevoPipelineHook(pipeline_id=123)
        sources_config = {"config": {}}

        with patch.object(hook, 'update_pipeline_sources_async', new_callable=AsyncMock) as mock_async:
            mock_async.return_value = sources_config

            result = hook.update_pipeline_sources(123, sources_config)

            assert result == sources_config
            mock_async.assert_called_once_with(123, sources_config)

class TestDisablePipelineAsync:
    """Tests for disable_pipeline_async method."""

    @pytest.mark.asyncio
    async def test_disable_pipeline_success(self):
        """Test successful pipeline disable."""
        hook = HevoPipelineHook(pipeline_id=123)

        with patch.object(hook, 'execute_api_request_async', new_callable=AsyncMock) as mock_request:
            mock_request.return_value = {}

            await hook.disable_pipeline_async(123)

            mock_request.assert_called_once_with(
                method="POST",
                endpoint="/api/v1/pipelines/123/actions/disable"
            )


class TestDisablePipelineSync:
    """Tests for disable_pipeline synchronous wrapper."""

    def test_disable_pipeline_sync_wrapper(self):
        """Test synchronous wrapper calls async method."""
        hook = HevoPipelineHook(pipeline_id=123)

        with patch.object(hook, 'disable_pipeline_async', new_callable=AsyncMock) as mock_async:
            mock_async.return_value = None

            hook.disable_pipeline(123)

            mock_async.assert_called_once_with(123)


class TestEnablePipelineAsync:
    """Tests for enable_pipeline_async method."""

    @pytest.mark.asyncio
    async def test_enable_pipeline_success(self):
        """Test successful pipeline enable."""
        hook = HevoPipelineHook(pipeline_id=123)

        with patch.object(hook, 'execute_api_request_async', new_callable=AsyncMock) as mock_request:
            mock_request.return_value = {}

            await hook.enable_pipeline_async(123)

            mock_request.assert_called_once_with(
                method="POST",
                endpoint="/api/v1/pipelines/123/actions/enable"
            )


class TestEnablePipelineSync:
    """Tests for enable_pipeline synchronous wrapper."""

    def test_enable_pipeline_sync_wrapper(self):
        """Test synchronous wrapper calls async method."""
        hook = HevoPipelineHook(pipeline_id=123)

        with patch.object(hook, 'enable_pipeline_async', new_callable=AsyncMock) as mock_async:
            mock_async.return_value = None

            hook.enable_pipeline(123)

            mock_async.assert_called_once_with(123)


class TestResyncPipelineAsync:
    """Tests for resync_pipeline_async method."""

    @pytest.mark.asyncio
    async def test_resync_pipeline_success(self):
        """Test successful pipeline resync."""
        hook = HevoPipelineHook(pipeline_id=123)

        with patch.object(hook, 'execute_api_request_async', new_callable=AsyncMock) as mock_request:
            mock_request.return_value = {}

            await hook.resync_pipeline_async(123)

            mock_request.assert_called_once_with(
                method="POST",
                endpoint="/api/v1/pipelines/123/actions/resync"
            )


class TestResyncPipelineSync:
    """Tests for resync_pipeline synchronous wrapper."""

    def test_resync_pipeline_sync_wrapper(self):
        """Test synchronous wrapper calls async method."""
        hook = HevoPipelineHook(pipeline_id=123)

        with patch.object(hook, 'resync_pipeline_async', new_callable=AsyncMock) as mock_async:
            mock_async.return_value = None

            hook.resync_pipeline(123)

            mock_async.assert_called_once_with(123)


class TestCancelJobAsync:
    """Tests for cancel_job_async method."""

    @pytest.mark.asyncio
    async def test_cancel_job_success(self):
        """Test successful job cancellation."""
        hook = HevoPipelineHook(pipeline_id=123)

        with patch.object(hook, 'execute_api_request_async', new_callable=AsyncMock) as mock_request:
            mock_request.return_value = {}

            await hook.cancel_job_async(123, "job_789")

            mock_request.assert_called_once_with(
                method="POST",
                endpoint="/api/v1/pipelines/123/jobs/job_789/actions/cancel"
            )


class TestCancelJobSync:
    """Tests for cancel_job synchronous wrapper."""

    def test_cancel_job_sync_wrapper(self):
        """Test synchronous wrapper calls async method."""
        hook = HevoPipelineHook(pipeline_id=123)

        with patch.object(hook, 'cancel_job_async', new_callable=AsyncMock) as mock_async:
            mock_async.return_value = None

            hook.cancel_job(123, "job_789")

            mock_async.assert_called_once_with(123, "job_789")


class TestGetJobObjectsAsync:
    """Tests for get_job_objects_async method."""

    @pytest.mark.asyncio
    async def test_get_job_objects_success(self, sample_job_objects_response):
        """Test successful job objects retrieval."""
        hook = HevoPipelineHook(pipeline_id=123)

        with patch.object(hook, 'execute_api_request_async', new_callable=AsyncMock) as mock_request:
            mock_request.return_value = sample_job_objects_response

            result = await hook.get_job_objects_async(123, "job_789", limit=100)

            assert "data" in result
            assert len(result["data"]) == 2
            assert result["data"][0]["object_name"] == "users"
            mock_request.assert_called_once_with(
                method="GET",
                endpoint="/api/v1/pipelines/123/jobs/job_789/objects",
                params={"limit": 100}
            )

    @pytest.mark.asyncio
    async def test_get_job_objects_with_cursor(self, sample_job_objects_response):
        """Test job objects retrieval with cursor pagination."""
        hook = HevoPipelineHook(pipeline_id=123)

        with patch.object(hook, 'execute_api_request_async', new_callable=AsyncMock) as mock_request:
            mock_request.return_value = sample_job_objects_response

            await hook.get_job_objects_async(123, "job_789", limit=50, cursor="cursor_xyz")

            mock_request.assert_called_once_with(
                method="GET",
                endpoint="/api/v1/pipelines/123/jobs/job_789/objects",
                params={"limit": 50, "cursor": "cursor_xyz"}
            )


class TestGetJobObjectsSync:
    """Tests for get_job_objects synchronous wrapper."""

    def test_get_job_objects_sync_wrapper(self, sample_job_objects_response):
        """Test synchronous wrapper calls async method."""
        hook = HevoPipelineHook(pipeline_id=123)

        with patch.object(hook, 'get_job_objects_async', new_callable=AsyncMock) as mock_async:
            mock_async.return_value = sample_job_objects_response

            result = hook.get_job_objects(123, "job_789", limit=100)

            assert "data" in result
            mock_async.assert_called_once_with(123, "job_789", 100, None)
