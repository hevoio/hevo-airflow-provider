"""Unit tests for HevoObjectHook."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest
from airflow.exceptions import AirflowException

from airflow.hevo.hooks.hevo_object_hook import HevoObjectHook


class TestHevoObjectHookInit:
    """Tests for HevoObjectHook initialization."""

    def test_hook_initialization_with_defaults(self):
        """Test hook initializes with default parameters."""
        hook = HevoObjectHook(pipeline_id=123)
        assert hook.pipeline_id == 123
        assert hook.connection_id == HevoObjectHook.default_conn_name

    def test_hook_initialization_with_custom_params(self):
        """Test hook initializes with custom parameters."""
        hook = HevoObjectHook(
            pipeline_id=456,
            connection_id="custom_conn",
            retry_limit=5
        )
        assert hook.pipeline_id == 456
        assert hook.connection_id == "custom_conn"
        assert hook.retry_limit == 5


class TestListObjectsAsync:
    """Tests for list_objects_async method."""

    @pytest.mark.asyncio
    async def test_list_objects_success(self, sample_objects_list_response):
        """Test successful objects list retrieval."""
        hook = HevoObjectHook(pipeline_id=123)

        with patch.object(hook, 'execute_api_request_async', new_callable=AsyncMock) as mock_request:
            mock_request.return_value = sample_objects_list_response

            result = await hook.list_objects_async(123, limit=100)

            assert "data" in result
            assert len(result["data"]) == 1
            assert result["data"][0]["name"] == "users"
            assert result["has_more"] is False
            mock_request.assert_called_once_with(
                method="GET",
                endpoint="/api/v1/pipelines/123/objects",
                params={"limit": 100}
            )

    @pytest.mark.asyncio
    async def test_list_objects_with_cursor(self, sample_objects_list_response):
        """Test objects list with cursor pagination."""
        hook = HevoObjectHook(pipeline_id=123)

        with patch.object(hook, 'execute_api_request_async', new_callable=AsyncMock) as mock_request:
            mock_request.return_value = sample_objects_list_response

            await hook.list_objects_async(123, limit=50, cursor="cursor_abc")

            mock_request.assert_called_once_with(
                method="GET",
                endpoint="/api/v1/pipelines/123/objects",
                params={"limit": 50, "cursor": "cursor_abc"}
            )


class TestListObjectsSync:
    """Tests for list_objects synchronous wrapper."""

    def test_list_objects_sync_wrapper(self, sample_objects_list_response):
        """Test synchronous wrapper calls async method."""
        hook = HevoObjectHook(pipeline_id=123)

        with patch.object(hook, 'list_objects_async', new_callable=AsyncMock) as mock_async:
            mock_async.return_value = sample_objects_list_response

            result = hook.list_objects(123, limit=100)

            assert "data" in result
            mock_async.assert_called_once_with(123, 100, None)


class TestGetObjectAsync:
    """Tests for get_object_async method."""

    @pytest.mark.asyncio
    async def test_get_object_success(self, sample_object_response):
        """Test successful object retrieval."""
        hook = HevoObjectHook(pipeline_id=123)

        with patch.object(hook, 'execute_api_request_async', new_callable=AsyncMock) as mock_request:
            mock_request.return_value = sample_object_response

            result = await hook.get_object_async(123, "users")

            assert result["name"] == "users"
            assert result["type"] == "TABLE"
            mock_request.assert_called_once_with(
                method="GET",
                endpoint="/api/v1/pipelines/123/objects/users"
            )

    @pytest.mark.asyncio
    async def test_get_object_not_found(self):
        """Test object not found raises exception."""
        hook = HevoObjectHook(pipeline_id=123)

        with patch.object(hook, 'execute_api_request_async', new_callable=AsyncMock) as mock_request:
            mock_request.side_effect = AirflowException("404 Not Found")

            with pytest.raises(AirflowException, match="404 Not Found"):
                await hook.get_object_async(123, "nonexistent")


class TestGetObjectSync:
    """Tests for get_object synchronous wrapper."""

    def test_get_object_sync_wrapper(self, sample_object_response):
        """Test synchronous wrapper calls async method."""
        hook = HevoObjectHook(pipeline_id=123)

        with patch.object(hook, 'get_object_async', new_callable=AsyncMock) as mock_async:
            mock_async.return_value = sample_object_response

            result = hook.get_object(123, "users")

            assert result["name"] == "users"
            mock_async.assert_called_once_with(123, "users")


class TestRefreshSchemaAsync:
    """Tests for refresh_schema_async method."""

    @pytest.mark.asyncio
    async def test_refresh_schema_success(self):
        """Test successful schema refresh."""
        hook = HevoObjectHook(pipeline_id=123)

        with patch.object(hook, 'execute_api_request_async', new_callable=AsyncMock) as mock_request:
            mock_request.return_value = {"status": "success"}

            result = await hook.refresh_schema_async(123)

            assert result["status"] == "success"
            mock_request.assert_called_once_with(
                method="POST",
                endpoint="/api/v1/pipelines/123/objects/actions/refresh-schema",
                json={}
            )

    @pytest.mark.asyncio
    async def test_refresh_schema_with_config(self):
        """Test schema refresh with custom config."""
        hook = HevoObjectHook(pipeline_id=123)
        refresh_config = {"objects": ["users", "orders"]}

        with patch.object(hook, 'execute_api_request_async', new_callable=AsyncMock) as mock_request:
            mock_request.return_value = {"status": "success"}

            await hook.refresh_schema_async(123, refresh_config)

            mock_request.assert_called_once_with(
                method="POST",
                endpoint="/api/v1/pipelines/123/objects/actions/refresh-schema",
                json=refresh_config
            )


class TestRefreshSchemaSync:
    """Tests for refresh_schema synchronous wrapper."""

    def test_refresh_schema_sync_wrapper(self):
        """Test synchronous wrapper calls async method."""
        hook = HevoObjectHook(pipeline_id=123)

        with patch.object(hook, 'refresh_schema_async', new_callable=AsyncMock) as mock_async:
            mock_async.return_value = {"status": "success"}

            result = hook.refresh_schema(123)

            assert result["status"] == "success"
            mock_async.assert_called_once_with(123, None)


class TestResyncObjectsAsync:
    """Tests for resync_objects_async method."""

    @pytest.mark.asyncio
    async def test_resync_objects_success(self):
        """Test successful objects resync."""
        hook = HevoObjectHook(pipeline_id=123)
        resync_config = {"objects": ["users", "orders"]}

        with patch.object(hook, 'execute_api_request_async', new_callable=AsyncMock) as mock_request:
            mock_request.return_value = {"status": "success", "job_id": "job_resync_123"}

            result = await hook.resync_objects_async(123, resync_config)

            assert result["status"] == "success"
            assert result["job_id"] == "job_resync_123"
            mock_request.assert_called_once_with(
                method="POST",
                endpoint="/api/v1/pipelines/123/objects/actions/resync",
                json=resync_config
            )


class TestResyncObjectsSync:
    """Tests for resync_objects synchronous wrapper."""

    def test_resync_objects_sync_wrapper(self):
        """Test synchronous wrapper calls async method."""
        hook = HevoObjectHook(pipeline_id=123)
        resync_config = {"objects": ["users"]}

        with patch.object(hook, 'resync_objects_async', new_callable=AsyncMock) as mock_async:
            mock_async.return_value = {"status": "success"}

            result = hook.resync_objects(123, resync_config)

            assert result["status"] == "success"
            mock_async.assert_called_once_with(123, resync_config)


class TestHevoObjectHookErrorHandling:
    """Tests for error handling scenarios."""

    @pytest.mark.asyncio
    async def test_api_request_failure_propagates(self):
        """Test that API request failures propagate correctly."""
        hook = HevoObjectHook(pipeline_id=123)

        with patch.object(hook, 'execute_api_request_async', new_callable=AsyncMock) as mock_request:
            mock_request.side_effect = AirflowException("Network error")

            with pytest.raises(AirflowException, match="Network error"):
                await hook.list_objects_async(123)

    @pytest.mark.asyncio
    async def test_invalid_pipeline_id(self):
        """Test handling of invalid pipeline ID."""
        hook = HevoObjectHook(pipeline_id=999)

        with patch.object(hook, 'execute_api_request_async', new_callable=AsyncMock) as mock_request:
            mock_request.side_effect = AirflowException("404 Not Found")

            with pytest.raises(AirflowException, match="404 Not Found"):
                await hook.list_objects_async(999)

    def test_sync_wrappers_use_asyncio_run(self):
        """Test that sync wrappers properly use asyncio.run."""
        hook = HevoObjectHook(pipeline_id=123)

        with patch('asyncio.run') as mock_asyncio_run:
            with patch.object(hook, 'list_objects_async', new_callable=AsyncMock):
                hook.list_objects(123)

                mock_asyncio_run.assert_called_once()
