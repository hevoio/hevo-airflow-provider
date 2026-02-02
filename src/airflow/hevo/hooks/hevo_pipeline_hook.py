from __future__ import annotations

import asyncio
from typing import Any

from airflow.exceptions import AirflowException

from airflow.hevo.hooks.base import BaseHevoHook
from airflow.hevo.models import Job, PaginatedJobsResponse, Pipeline
from airflow.hevo.models.job import JobCompletionStatus, JobStatus, JobType
from airflow.hevo.models.pipeline import PipelineStatus, SyncType


class HevoPipelineHook(BaseHevoHook):
    """
    Hook for interacting with Hevo pipeline and job management APIs.

    Provides methods for:
    - Pipeline validation and status checking
    - Triggering pipeline syncs
    - Job discovery and status monitoring
    - Job completion status normalization

    **Async-First Architecture**:
    All API interactions are implemented as async methods (suffixed with '_async') with
    corresponding synchronous wrapper methods that use asyncio.run() internally.
    For example, `get_pipeline_async()` has a `get_pipeline()` sync wrapper.

    **When to use async vs sync**:
    - Async methods: Use in async contexts (triggers, async functions)
    - Sync methods: Use in sync contexts (operators, sensors execute/poke)

    **Inherited from BaseHevoHook**:
    - execute_api_request_async() - Execute HTTP requests with retry logic
    - build_async_request_kwargs() - Build request parameters with auth and headers
    - Connection management with lazy loading
    """

    # Async API Methods

    async def get_pipeline_async(self, pipeline_id: int) -> Pipeline | None:
        """
        Retrieve pipeline details from Hevo API (async).

        Fetches complete pipeline information including status, source, destination,
        and configuration. Returns a type-safe Pipeline model with automatic
        validation.

        :param pipeline_id: Unique pipeline identifier.
        :returns: Pipeline model with all details, or ``None`` if pipeline not found (404).
        :raises AirflowException: For non-404 API errors (auth, network, server errors).
        """
        self.log.info("Fetching pipeline details for pipeline_id=%s", pipeline_id)
        try:
            response = await self.execute_api_request_async(method="GET", endpoint=f"/api/v1/pipelines/{pipeline_id}")
            return Pipeline(**response)
        except AirflowException as e:
            # Return None for 404 errors (pipeline not found)
            if "404" in str(e):
                self.log.info("Pipeline %s not found (404)", pipeline_id)
                return None
            # Re-raise all other errors
            self.log.error("Error fetching pipeline %s: %s", pipeline_id, e)
            raise e

    async def _validate_pipeline_async(self, pipeline_id: int) -> None:
        """
        Validate that the pipeline exists and is in an active state (async).

        Checks that:
        1. Pipeline exists in Hevo (returns valid Pipeline model)
        2. Pipeline status is INITIALIZED (not PAUSED, STOPPED, FAILED, or INCOMPLETE)

        This validation should be called before triggering a sync to ensure the
        pipeline is ready to accept sync requests.

        :param pipeline_id: Unique pipeline identifier to validate.
        :raises AirflowException: When the pipeline does not exist or is not in INITIALIZED state.
        """
        self.log.info("Validating pipeline %s", pipeline_id)
        pipeline = await self.get_pipeline_async(pipeline_id)
        self.log.debug("Pipeline details: %s with type %s", pipeline, type(pipeline))

        if not pipeline:
            self.log.error("Pipeline %s does not exist", pipeline_id)
            raise AirflowException(f"Pipeline {pipeline_id} does not exist, Please ensure the id is correct.")

        if pipeline.status != PipelineStatus.INITIALIZED:
            self.log.error(
                "Pipeline %s is not in active state. Current status: %s",
                pipeline_id,
                pipeline.status,
            )
            raise AirflowException(
                f"Pipeline {pipeline_id} is not in active state, "
                f"Please ensure the pipeline is enabled and is in initialized state."
            )

    async def trigger_pipeline_sync_async(self, pipeline_id: int, ensure_new_job: bool = True) -> None:
        """
        Trigger a sync operation for the given pipeline (async).

        Sends a sync-now request to the Hevo API. The pipeline must be in INITIALIZED
        state for the sync to be accepted. After triggering, a job will be created
        asynchronously (may take a few seconds to appear in the jobs list).

        :param pipeline_id: Unique pipeline identifier.
        :param ensure_new_job: When ``True``, raises an error if a job is already in progress.
                              When ``False`` (default), the API may queue or reject duplicate requests
                              based on pipeline configuration. Useful for preventing duplicate syncs.
        :raises AirflowException: When sync fails, or when ``ensure_new_job=True`` and a job
                                 is already in progress.
        """
        self.log.info("Triggering sync for pipeline %s (ensure_new_job=%s)", pipeline_id, ensure_new_job)
        try:
            await self.execute_api_request_async(
                method="POST", endpoint=f"/api/v1/pipelines/{pipeline_id}/actions/sync-now"
            )
            self.log.info("Pipeline %s sync triggered successfully", pipeline_id)
        except AirflowException as e:
            if not ensure_new_job:
                error_msg = str(e).lower()
                if "job is in progress" in error_msg or "job already" in error_msg:
                    self.log.debug(
                        "Pipeline %s has a job in progress and ensure_new_job is False, "
                        "Continuing poll on the active job.",
                        pipeline_id,
                    )
                    return
            self.log.error("Error syncing pipeline %s: %s", pipeline_id, e)
            raise e

    async def find_active_job_by_type_async(
        self, pipeline_id: int, job_type: JobType = JobType.INCREMENTAL, page_limit: int = 10
    ) -> Job:
        """
        Find and return the first active (IN_PROGRESS) job for the given pipeline and type (async).

        Searches through paginated job listings to find a job matching both the specified
        job_type and IN_PROGRESS status. Uses cursor-based pagination to handle large
        job histories efficiently.

        Common job types:
        - JobType.INCREMENTAL: Regular incremental sync (default)
        - JobType.HISTORICAL: Historical data backfill

        :param pipeline_id: Unique pipeline identifier.
        :param job_type: Job type to filter for (default: JobType.INCREMENTAL).
        :param page_limit: Number of jobs to fetch per API request (default: 10).
                          Larger values reduce API calls but increase response size.
        :returns: Job model with complete job details (job_id, status, type, statistics, etc.).
        :raises AirflowException: When no active jobs of the specified type are found after
                                 searching all pages.
        """
        # Validate job_type is a JobType enum
        if not isinstance(job_type, JobType):
            raise AirflowException(
                f"job_type must be a JobType enum, got {type(job_type).__name__}. "
                f"Use JobType.INCREMENTAL, JobType.HISTORICAL, etc."
            )

        cursor = None
        while True:
            response = await self.execute_api_request_async(
                method="GET",
                endpoint=f"/api/v1/pipelines/{pipeline_id}/jobs",
                params={"limit": page_limit, "cursor": cursor} if cursor else {"limit": page_limit},
            )

            jobs_response = PaginatedJobsResponse(**response)

            for job in jobs_response.data:
                # Log warning for unknown job types or statuses
                if job.type == JobType.UNKNOWN:
                    self.log.warning(
                        "Job %s has unknown type. This may indicate a new job type was added to the Hevo API.",
                        job.job_id,
                    )
                if job.status == JobStatus.UNKNOWN:
                    self.log.warning(
                        "Job %s has unknown status. This may indicate a new status was added to the Hevo API.",
                        job.job_id,
                    )

                # Compare job types (handle both enum and string inputs)
                job_type_value = job.type.value if isinstance(job.type, JobType) else str(job.type)
                expected_type_value = job_type.value if isinstance(job_type, JobType) else str(job_type)
                job_type_match = job_type_value == expected_type_value
                job_status_active = job.status == JobStatus.IN_PROGRESS

                if job_type_match and job_status_active:
                    self.log.info("Found active %s job: %s", job_type, job.job_id)
                    return job

            if not jobs_response.has_more:
                break

            cursor = jobs_response.next_cursor
            if not cursor:
                break

        job_type_str = job_type.value if isinstance(job_type, JobType) else str(job_type)
        raise AirflowException(f"Pipeline {pipeline_id} doesn't have any active jobs of type {job_type_str}.")

    async def get_job_completion_status_async(
        self, pipeline_id: int, job_id: str, accept_completed_with_failures: bool = False
    ) -> JobCompletionStatus:
        """
        Get the normalized completion status of a job (async).

        Fetches job details from the API, parses into a Job model, and maps
        the job's status to one of four canonical states for easier consumption by
        operators, sensors, and triggers.

        Status mapping:
        - ``JobCompletionStatus.COMPLETED``: Job finished successfully (JobStatus.COMPLETED)
        - ``JobCompletionStatus.COMPLETED_WITH_FAILURES``: Job finished with some failures
          (JobStatus.COMPLETED_WITH_FAILURES). Only returned as success when
          accept_completed_with_failures=True
        - ``JobCompletionStatus.FAILED``: Job failed, cancelled, skipped, or deferred with failure
          (JobStatus.FAILED, CANCELLED, SKIPPED, DEFERRED, DEFERRED_WITH_FAILURES,
          or COMPLETED_WITH_FAILURES when not accepting failures)
        - ``JobCompletionStatus.PENDING``: Job still running
          (JobStatus.IN_PROGRESS, QUEUED, PENDING)

        :param pipeline_id: Unique pipeline identifier.
        :param job_id: Unique job identifier.
        :param accept_completed_with_failures: When ``True``, treat COMPLETED_WITH_FAILURES as
                                              success and return COMPLETED_WITH_FAILURES status.
                                              When ``False`` (default), treat it as a failure.
        :returns: JobCompletionStatus enum value indicating normalized job state.
        :raises AirflowException: For API errors (auth, network, server errors, job not found).
        """
        response = await self.execute_api_request_async(
            method="GET",
            endpoint=f"/api/v1/pipelines/{pipeline_id}/jobs/{job_id}",
        )

        job = Job(**response)

        if job.status == JobStatus.COMPLETED:
            self.log.info("Job %s completed successfully", job_id)
            return JobCompletionStatus.COMPLETED
        if job.status == JobStatus.COMPLETED_WITH_FAILURES and accept_completed_with_failures:
            self.log.warning("Job %s completed with failures (accepting as success)", job_id)
            return JobCompletionStatus.COMPLETED_WITH_FAILURES
        if job.status in [
            JobStatus.COMPLETED_WITH_FAILURES,
            JobStatus.FAILED,
            JobStatus.CANCELLED,
            JobStatus.SKIPPED,
            JobStatus.DEFERRED,
            JobStatus.DEFERRED_WITH_FAILURES,
        ]:
            self.log.info("Job %s failed with status %s", job_id, job.status)
            return JobCompletionStatus.FAILED
        if job.status == JobStatus.UNKNOWN:
            self.log.warning(
                "Job %s has unknown status. Treating as pending to continue monitoring. "
                "This may indicate a new status was added to the Hevo API.",
                job_id,
            )
            return JobCompletionStatus.PENDING
        return JobCompletionStatus.PENDING

    async def update_pipeline_async(self, pipeline_id: int, pipeline_config: dict[str, Any]) -> Pipeline:
        """
        Update pipeline configuration (async).

        Modifies existing pipeline settings. Only provided fields will be updated.

        :param pipeline_id: Unique pipeline identifier.
        :param pipeline_config: Partial or full pipeline configuration to update.
        :returns: Updated Pipeline model with full details.
        :raises AirflowException: For API errors (auth, network, server errors, validation errors).
        """
        self.log.info("Updating pipeline %s with config: %s", pipeline_id, pipeline_config)
        response = await self.execute_api_request_async(
            method="PATCH", endpoint=f"/api/v1/pipelines/{pipeline_id}", payload=pipeline_config
        )
        pipeline = Pipeline(**response)
        self.log.info("Pipeline %s updated successfully", pipeline_id)
        return pipeline

    async def disable_pipeline_async(self, pipeline_id: int) -> None:
        """
        Disable/pause a pipeline (async).

        Stops the pipeline from running scheduled syncs. Manual syncs will also
        be rejected until the pipeline is re-enabled.

        :param pipeline_id: Unique pipeline identifier.
        :raises AirflowException: For API errors (auth, network, server errors).
        """
        self.log.info("Disabling pipeline %s", pipeline_id)
        await self.execute_api_request_async(method="POST", endpoint=f"/api/v1/pipelines/{pipeline_id}/actions/disable")
        self.log.info("Pipeline %s disabled successfully", pipeline_id)

    async def enable_pipeline_async(self, pipeline_id: int) -> None:
        """
        Enable/resume a pipeline (async).

        Resumes pipeline operations, allowing scheduled and manual syncs to execute.

        :param pipeline_id: Unique pipeline identifier.
        :raises AirflowException: For API errors (auth, network, server errors).
        """
        self.log.info("Enabling pipeline %s", pipeline_id)
        await self.execute_api_request_async(method="POST", endpoint=f"/api/v1/pipelines/{pipeline_id}/actions/enable")
        self.log.info("Pipeline %s enabled successfully", pipeline_id)

    async def resync_pipeline_async(self, pipeline_id: int, drop_and_load: bool = False) -> None:
        """
        Trigger a full historical resync for the pipeline (async).

        Initiates a complete historical data reload, re-ingesting all data from
        the source. This is useful when data needs to be reprocessed or after
        schema changes.

        :param pipeline_id: Unique pipeline identifier.
        :param drop_and_load: When ``True``, drops existing destination tables before loading.
                             Ensures a clean slate by recreating tables from scratch. Default: ``False``.
        :raises AirflowException: For API errors (auth, network, server errors).
        """
        self.log.info("Triggering resync for pipeline %s (drop_and_load=%s)", pipeline_id, drop_and_load)
        payload = {"drop_and_load": drop_and_load}
        await self.execute_api_request_async(
            method="POST", endpoint=f"/api/v1/pipelines/{pipeline_id}/actions/resync", payload=payload
        )
        self.log.info("Pipeline %s resync triggered successfully", pipeline_id)

    async def cancel_job_async(self, pipeline_id: int, job_id: str) -> None:
        """
        Cancel an active job (async).

        Stops a running job execution. The job status will change to CANCELLED.

        :param pipeline_id: Unique pipeline identifier.
        :param job_id: Unique job identifier.
        :raises AirflowException: For API errors (auth, network, server errors, job not found).
        """
        self.log.info("Cancelling job %s for pipeline %s", job_id, pipeline_id)
        await self.execute_api_request_async(
            method="POST", endpoint=f"/api/v1/pipelines/{pipeline_id}/jobs/{job_id}/actions/cancel"
        )
        self.log.info("Job %s cancelled successfully", job_id)

    async def get_job_objects_async(
        self, pipeline_id: int, job_id: str, limit: int = 100, cursor: str | None = None
    ) -> dict[str, Any]:
        """
        Get objects processed in a specific job (async).

        Returns paginated list of all objects (tables/collections) that were
        processed during the job execution, including their statistics.

        :param pipeline_id: Unique pipeline identifier.
        :param job_id: Unique job identifier.
        :param limit: Maximum number of objects to return per page (default: 100).
        :param cursor: Pagination cursor for fetching next page of results.
        :returns: Dictionary with 'data' (list of objects), 'has_more', and 'next_cursor'.
        :raises AirflowException: For API errors (auth, network, server errors, job not found).
        """
        params = {"limit": limit}
        if cursor:
            params["cursor"] = cursor

        return await self.execute_api_request_async(
            method="GET", endpoint=f"/api/v1/pipelines/{pipeline_id}/jobs/{job_id}/objects", params=params
        )

    # Synchronous Wrappers
    # These methods wrap the async methods above using asyncio.run()

    def get_pipeline_sync(self, pipeline_id: int) -> Pipeline | None:
        """
        Retrieve pipeline details from Hevo API (sync wrapper).

        See get_pipeline_async() for full documentation.
        """
        return asyncio.run(self.get_pipeline_async(pipeline_id))

    def validate_pipeline(self, pipeline_id: int) -> None:
        """
        Validate that the pipeline exists and is in an active state (sync wrapper).

        See validate_pipeline_async() for full documentation.
        """
        return asyncio.run(self._validate_pipeline_async(pipeline_id))

    def trigger_pipeline_sync(self, pipeline_id: int, ensure_new_job: bool = True) -> None:
        """
        Trigger a sync operation for the given pipeline (sync wrapper).

        See trigger_pipeline_sync_async() for full documentation.
        """
        return asyncio.run(self.trigger_pipeline_sync_async(pipeline_id, ensure_new_job))

    def find_active_job_by_type_sync(
        self, pipeline_id: int, job_type: JobType = JobType.INCREMENTAL, page_limit: int = 10
    ) -> Job:
        """
        Find and return the first active (IN_PROGRESS) job for the given pipeline and type (sync wrapper).

        See find_active_job_by_type_async() for full documentation.
        """
        return asyncio.run(self.find_active_job_by_type_async(pipeline_id, job_type, page_limit))

    def get_job_completion_status_sync(
        self, pipeline_id: int, job_id: str, accept_completed_with_failures: bool = False
    ) -> JobCompletionStatus:
        """
        Get the normalized completion status of a job (sync wrapper).

        See get_job_completion_status_async() for full documentation.
        """
        return asyncio.run(self.get_job_completion_status_async(pipeline_id, job_id, accept_completed_with_failures))

    def update_pipeline_sync(self, pipeline_id: int, pipeline_config: dict[str, Any]) -> Pipeline:
        """
        Update pipeline configuration (sync wrapper).

        See update_pipeline_async() for full documentation.
        """
        return asyncio.run(self.update_pipeline_async(pipeline_id, pipeline_config))

    def disable_pipeline_sync(self, pipeline_id: int) -> None:
        """
        Disable/pause a pipeline (sync wrapper).

        See disable_pipeline_async() for full documentation.
        """
        return asyncio.run(self.disable_pipeline_async(pipeline_id))

    def enable_pipeline_sync(self, pipeline_id: int) -> None:
        """
        Enable/resume a pipeline (sync wrapper).

        See enable_pipeline_async() for full documentation.
        """
        return asyncio.run(self.enable_pipeline_async(pipeline_id))

    def resync_pipeline_sync(self, pipeline_id: int, drop_and_load: bool = False) -> None:
        """
        Trigger a full historical resync for the pipeline (sync wrapper).

        See resync_pipeline_async() for full documentation.
        """
        return asyncio.run(self.resync_pipeline_async(pipeline_id, drop_and_load))

    def cancel_job_sync(self, pipeline_id: int, job_id: str) -> None:
        """
        Cancel an active job (sync wrapper).

        See cancel_job_async() for full documentation.
        """
        return asyncio.run(self.cancel_job_async(pipeline_id, job_id))

    def get_job_objects_sync(
        self, pipeline_id: int, job_id: str, limit: int = 100, cursor: str | None = None
    ) -> dict[str, Any]:
        """
        Get objects processed in a specific job (sync wrapper).

        See get_job_objects_async() for full documentation.
        """
        return asyncio.run(self.get_job_objects_async(pipeline_id, job_id, limit, cursor))
