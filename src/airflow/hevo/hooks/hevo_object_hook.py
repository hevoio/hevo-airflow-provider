from __future__ import annotations

import asyncio
from typing import Any, Optional

from airflow.hevo.hooks.base import BaseHevoHook
from airflow.hevo.models.object import PaginatedObjectsResponse, PipelineObject


class HevoObjectHook(BaseHevoHook):
    """
    Hook for interacting with Hevo pipeline objects APIs.

    Provides methods for:
    - Listing objects in a pipeline
    - Retrieving object details
    - Refreshing object schemas from source
    - Resyncing specific objects

    **Inherited from BaseHevoHook**:
    - execute_api_request_async() - Execute HTTP requests with retry logic
    - build_async_request_kwargs() - Build request parameters with auth and headers
    - Connection management with lazy loading
    """

    # Async API Methods

    async def list_objects_async(
            self, pipeline_id: int, limit: int = 100, cursor: Optional[str] = None
    ) -> PaginatedObjectsResponse:
        """
        List all objects in a pipeline (async).

        Returns paginated list of all objects (tables/collections) configured
        in the pipeline, including their selection status and configuration.

        :param pipeline_id: Unique pipeline identifier.
        :param limit: Maximum number of objects to return per page (default: 100).
        :param cursor: Pagination cursor for fetching next page of results.
        :returns: PaginatedObjectsResponse with list of objects and pagination metadata.
        :raises AirflowException: For API errors (auth, network, server errors).
        """
        self.log.info("Fetching objects for pipeline %s (limit=%s, cursor=%s)", pipeline_id, limit, cursor)
        params: dict[str, Any] = {"limit": limit}
        if cursor:
            params["cursor"] = cursor

        response = await self.execute_api_request_async(
            method="GET", endpoint=f"/api/v1/pipelines/{pipeline_id}/objects", params=params
        )
        return PaginatedObjectsResponse(**response)

    async def get_object_async(self, pipeline_id: int, object_id: str) -> PipelineObject:
        """
        Retrieve details for a specific object (async).

        Fetches complete information about a single object including its schema,
        configuration, and sync status.

        :param pipeline_id: Unique pipeline identifier.
        :param object_id: Unique object identifier (typically table/collection name).
        :returns: PipelineObject with complete object details.
        :raises AirflowException: For API errors (auth, network, server errors, object not found).
        """
        self.log.info("Fetching object %s for pipeline %s", object_id, pipeline_id)
        response = await self.execute_api_request_async(
            method="GET", endpoint=f"/api/v1/pipelines/{pipeline_id}/objects/{object_id}"
        )
        self.log.info("Fetched object %s successfully", object_id)
        return PipelineObject(**response)

    async def refresh_schema_async(self, pipeline_id: int) -> None:
        """
        Refresh object schemas from source (async).

        Updates the schema information for pipeline objects by fetching the latest
        schema from the source system. Useful when source tables/collections have
        been modified.

        :param pipeline_id: Unique pipeline identifier.
        :raises AirflowException: For API errors (auth, network, server errors).
        """
        self.log.info("Refreshing schema for pipeline %s", pipeline_id)
        await self.execute_api_request_async(
            method="POST",
            endpoint=f"/api/v1/pipelines/{pipeline_id}/objects/actions/refresh-schema",
        )
        self.log.info("Schema refreshed successfully for pipeline %s", pipeline_id)

    async def resync_objects_async(self, pipeline_id: int, resync_config: dict[str, Any]) -> None:
        """
        Resync specific objects (async).

        Triggers a historical resync for specified objects, re-ingesting their
        data from the source. This is useful for reprocessing data for specific
        tables/collections without resyncing the entire pipeline.

        :param pipeline_id: Unique pipeline identifier.
        :param resync_config: Configuration specifying which objects to resync.
                              Typically contains 'objects' list with object IDs.
                              Requires object_ids and drop_and_load parameter.
        :raises AirflowException: For API errors (auth, network, server errors, validation errors).
        """
        self.log.info("Resyncing objects for pipeline %s with config: %s", pipeline_id, resync_config)
        await self.execute_api_request_async(
            method="POST", endpoint=f"/api/v1/pipelines/{pipeline_id}/objects/actions/resync", payload=resync_config
        )
        self.log.info("Objects resync triggered successfully for pipeline %s", pipeline_id)

    # Synchronous Wrappers
    # These methods wrap the async methods above using asyncio.run()

    def list_objects_sync(
            self, pipeline_id: int, limit: int = 100, cursor: Optional[str] = None
    ) -> PaginatedObjectsResponse:
        """
        List all objects in a pipeline (sync wrapper).

        See list_objects_async() for full documentation.
        """
        return asyncio.run(self.list_objects_async(pipeline_id, limit, cursor))

    def get_object_sync(self, pipeline_id: int, object_id: str) -> PipelineObject:
        """
        Retrieve details for a specific object (sync wrapper).

        See get_object_async() for full documentation.
        """
        return asyncio.run(self.get_object_async(pipeline_id, object_id))

    def refresh_schema_sync(self, pipeline_id: int) -> None:
        """
        Refresh object schemas from source (sync wrapper).

        See refresh_schema_async() for full documentation.
        """
        asyncio.run(self.refresh_schema_async(pipeline_id))

    def resync_objects_sync(self, pipeline_id: int, resync_config: dict[str, Any]) -> None:
        """
        Resync specific objects (sync wrapper).

        See resync_objects_async() for full documentation.
        """
        asyncio.run(self.resync_objects_async(pipeline_id, resync_config))
