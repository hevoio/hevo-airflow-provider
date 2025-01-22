"""Unit tests for HevoObjectHook."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest
from airflow.exceptions import AirflowException

from airflow.hevo.hooks import HevoObjectHook


class TestHevoObjectHookInit:
    """Tests for HevoObjectHook initialization."""

    def test_hook_initialization_with_defaults(self) -> None:
        """Test hook initializes with default parameters."""
        hook = HevoObjectHook()
        assert hook.connection_id == HevoObjectHook.default_conn_name

    def test_hook_initialization_with_custom_params(self) -> None:
        """Test hook initializes with custom parameters."""
        hook = HevoObjectHook(connection_id="custom_conn", retry_limit=5)
        assert hook.connection_id == "custom_conn"
        assert hook.retry_limit == 5


class TestListObjectsAsync:
    """Tests for list_objects_async method."""

    @pytest.mark.asyncio
    async def test_list_objects_success(self, sample_objects_list_response) -> None:
        """Test successful objects list retrieval."""
        hook = HevoObjectHook()

        with patch.object(hook, "execute_api_request_async", new_callable=AsyncMock) as mock_request:
            mock_request.return_value = sample_objects_list_response

            result = await hook.list_objects_async(123, limit=100)

            # Verify result is a PaginatedObjectsResponse Pydantic model
            assert hasattr(result, "data")
            assert len(result.data) == 1
            assert result.data[0].source_namespace.k0 == "users"
            assert result.has_more is False
            mock_request.assert_called_once_with(
                method="GET", endpoint="/api/v1/pipelines/123/objects", params={"limit": 100}
            )

    @pytest.mark.asyncio
    async def test_list_objects_with_cursor(self, sample_objects_list_response) -> None:
        """Test objects list with cursor pagination."""
        hook = HevoObjectHook()

        with patch.object(hook, "execute_api_request_async", new_callable=AsyncMock) as mock_request:
            mock_request.return_value = sample_objects_list_response

            await hook.list_objects_async(123, limit=50, cursor="cursor_abc")

            mock_request.assert_called_once_with(
                method="GET", endpoint="/api/v1/pipelines/123/objects", params={"limit": 50, "cursor": "cursor_abc"}
            )


class TestListObjectsSync:
    """Tests for list_objects synchronous wrapper."""

    def test_list_objects_sync_wrapper(self, sample_objects_list_response) -> None:
        """Test synchronous wrapper calls async method."""
        from airflow.hevo.models.object import PaginatedObjectsResponse

        hook = HevoObjectHook()

        with patch.object(hook, "list_objects_async", new_callable=AsyncMock) as mock_async:
            # Mock should return a PaginatedObjectsResponse object
            mock_async.return_value = PaginatedObjectsResponse(**sample_objects_list_response)

            result = hook.list_objects_sync(123, limit=100)

            assert hasattr(result, "data")
            mock_async.assert_called_once_with(123, 100, None)


class TestGetObjectAsync:
    """Tests for get_object_async method."""

    @pytest.mark.asyncio
    async def test_get_object_success(self, sample_object_response) -> None:
        """Test successful object retrieval."""
        hook = HevoObjectHook()

        with patch.object(hook, "execute_api_request_async", new_callable=AsyncMock) as mock_request:
            mock_request.return_value = sample_object_response

            result = await hook.get_object_async(123, "550e8400-e29b-41d4-a716-446655440000")

            # Verify result is a PipelineObject Pydantic model
            assert result.source_namespace.k0 == "users"
            assert result.status == "ACTIVE"
            assert result.field_count == 5
            mock_request.assert_called_once_with(
                method="GET", endpoint="/api/v1/pipelines/123/objects/550e8400-e29b-41d4-a716-446655440000"
            )

    @pytest.mark.asyncio
    async def test_get_object_not_found(self) -> None:
        """Test object not found raises exception."""
        hook = HevoObjectHook()

        with patch.object(hook, "execute_api_request_async", new_callable=AsyncMock) as mock_request:
            mock_request.side_effect = AirflowException("404 Not Found")

            with pytest.raises(AirflowException, match="404 Not Found"):
                await hook.get_object_async(123, "nonexistent")


class TestGetObjectSync:
    """Tests for get_object synchronous wrapper."""

    def test_get_object_sync_wrapper(self, sample_object_response) -> None:
        """Test synchronous wrapper calls async method."""
        from airflow.hevo.models.object import PipelineObject

        hook = HevoObjectHook()

        with patch.object(hook, "get_object_async", new_callable=AsyncMock) as mock_async:
            # Mock should return a PipelineObject
            mock_async.return_value = PipelineObject(**sample_object_response)

            result = hook.get_object_sync(123, "550e8400-e29b-41d4-a716-446655440000")

            assert result.source_namespace.k0 == "users"
            mock_async.assert_called_once_with(123, "550e8400-e29b-41d4-a716-446655440000")


class TestPipelineObjectDetails:
    """Tests for PipelineObject with detailed fields."""

    @pytest.mark.asyncio
    async def test_get_object_with_all_details(self, sample_object_response) -> None:
        """Test that detailed object response includes all fields."""
        from airflow.hevo.models.object import LoadMode

        hook = HevoObjectHook()

        with patch.object(hook, "execute_api_request_async", new_callable=AsyncMock) as mock_request:
            mock_request.return_value = sample_object_response

            result = await hook.get_object_async(123, "550e8400-e29b-41d4-a716-446655440000")

            # Verify basic fields
            assert result.object_id == "550e8400-e29b-41d4-a716-446655440000"
            assert result.field_count == 5
            assert result.status == "ACTIVE"
            assert result.replication_status == "REPLICATED"

            # Verify detailed fields
            assert result.object_name == "users"
            assert result.destination_table_name == "users"
            assert result.load_mode == LoadMode.MERGE
            assert result.fields is not None
            assert len(result.fields) == 3

            # Verify field details
            id_field = result.fields[0]
            assert id_field.source_name == "id"
            assert id_field.destination_name == "id"
            assert id_field.source_type == "INTEGER"
            assert id_field.destination_type == "BIGINT"
            assert id_field.primary_key is True

            email_field = result.fields[1]
            assert email_field.source_name == "email"
            assert email_field.primary_key is False

    @pytest.mark.asyncio
    async def test_get_object_without_optional_fields(self) -> None:
        """Test that object response works without optional detailed fields."""
        hook = HevoObjectHook()

        minimal_response = {
            "object_id": "550e8400-e29b-41d4-a716-446655440000",
            "source_namespace": {"k0": "users", "k1": None, "k2": None},
            "destination_namespace": {"k0": "users", "k1": None, "k2": None},
            "field_count": 5,
            "status": "ACTIVE",
            "replication_status": "PENDING",
        }

        with patch.object(hook, "execute_api_request_async", new_callable=AsyncMock) as mock_request:
            mock_request.return_value = minimal_response

            result = await hook.get_object_async(123, "550e8400-e29b-41d4-a716-446655440000")

            # Verify basic fields work
            assert result.object_id == "550e8400-e29b-41d4-a716-446655440000"
            assert result.field_count == 5

            # Verify optional fields are None
            assert result.object_name is None
            assert result.destination_table_name is None
            assert result.load_mode is None
            assert result.fields is None


class TestRefreshSchemaAsync:
    """Tests for refresh_schema_async method."""

    @pytest.mark.asyncio
    async def test_refresh_schema_success(self) -> None:
        """Test successful schema refresh."""
        hook = HevoObjectHook()

        with patch.object(hook, "execute_api_request_async", new_callable=AsyncMock) as mock_request:
            mock_request.return_value = {"status": "success"}

            result = await hook.refresh_schema_async(123)

            assert result is None
            mock_request.assert_called_once_with(
                method="POST", endpoint="/api/v1/pipelines/123/objects/actions/refresh-schema"
            )

    @pytest.mark.asyncio
    async def test_refresh_schema_with_config(self) -> None:
        """Test schema refresh without config parameter."""
        hook = HevoObjectHook()

        with patch.object(hook, "execute_api_request_async", new_callable=AsyncMock) as mock_request:
            mock_request.return_value = {"status": "success"}

            result = await hook.refresh_schema_async(123)

            assert result is None
            mock_request.assert_called_once_with(
                method="POST", endpoint="/api/v1/pipelines/123/objects/actions/refresh-schema"
            )


class TestRefreshSchemaSync:
    """Tests for refresh_schema synchronous wrapper."""

    def test_refresh_schema_sync_wrapper(self) -> None:
        """Test synchronous wrapper calls async method."""
        hook = HevoObjectHook()

        with patch.object(hook, "refresh_schema_async", new_callable=AsyncMock) as mock_async:
            mock_async.return_value = None

            result = hook.refresh_schema_sync(123)

            assert result is None
            mock_async.assert_called_once_with(123)


class TestResyncObjectsAsync:
    """Tests for resync_objects_async method."""

    @pytest.mark.asyncio
    async def test_resync_objects_success(self) -> None:
        """Test successful objects resync."""
        hook = HevoObjectHook()
        resync_config = {"objects": ["users", "orders"]}

        with patch.object(hook, "execute_api_request_async", new_callable=AsyncMock) as mock_request:
            mock_request.return_value = {"status": "success", "job_id": "job_resync_123"}

            result = await hook.resync_objects_async(123, resync_config)

            assert result is None
            mock_request.assert_called_once_with(
                method="POST", endpoint="/api/v1/pipelines/123/objects/actions/resync", payload=resync_config
            )


class TestResyncObjectsSync:
    """Tests for resync_objects synchronous wrapper."""

    def test_resync_objects_sync_wrapper(self) -> None:
        """Test synchronous wrapper calls async method."""
        hook = HevoObjectHook()
        resync_config = {"objects": ["users"]}

        with patch.object(hook, "resync_objects_async", new_callable=AsyncMock) as mock_async:
            mock_async.return_value = None

            result = hook.resync_objects_sync(123, resync_config)

            assert result is None
            mock_async.assert_called_once_with(123, resync_config)


class TestHevoObjectHookErrorHandling:
    """Tests for error handling scenarios."""

    @pytest.mark.asyncio
    async def test_api_request_failure_propagates(self) -> None:
        """Test that API request failures propagate correctly."""
        hook = HevoObjectHook()

        with patch.object(hook, "execute_api_request_async", new_callable=AsyncMock) as mock_request:
            mock_request.side_effect = AirflowException("Network error")

            with pytest.raises(AirflowException, match="Network error"):
                await hook.list_objects_async(123)

    @pytest.mark.asyncio
    async def test_invalid_pipeline_id(self) -> None:
        """Test handling of invalid pipeline ID."""
        hook = HevoObjectHook()

        with patch.object(hook, "execute_api_request_async", new_callable=AsyncMock) as mock_request:
            mock_request.side_effect = AirflowException("404 Not Found")

            with pytest.raises(AirflowException, match="404 Not Found"):
                await hook.list_objects_async(999)

    def test_sync_wrappers_use_asyncio_run(self) -> None:
        """Test that sync wrappers properly use asyncio.run."""
        hook = HevoObjectHook()

        with patch("asyncio.run") as mock_asyncio_run:
            with patch.object(hook, "list_objects_async", new_callable=AsyncMock):
                hook.list_objects_sync(123)

                mock_asyncio_run.assert_called_once()
