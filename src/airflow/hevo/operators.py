from __future__ import annotations

from functools import cached_property
from time import sleep
from typing import TYPE_CHECKING, Any, Optional

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator

from airflow.hevo.hooks import HevoPipelineHook
from airflow.hevo.models.job import JobCompletionStatus, JobType
from airflow.hevo.models.pipeline import PipelineAction, PipelineStatus
from airflow.hevo.trigger import HevoTrigger

if TYPE_CHECKING:
    from airflow.utils.context import Context

    from airflow.hevo.models.object import PipelineObject

    from .openlineage import OperatorLineage


class HevoPipelineOperator(BaseOperator):
    """
    Operator to trigger and optionally wait for Hevo pipeline syncs or resyncs.

    Supports two pipeline actions:
    1. SYNC_NOW: Trigger a regular sync (POST /pipelines/{id}/actions/sync-now)
       - Validates pipeline is in INITIALIZED state (single check, fails if not)
    2. RESYNC: Trigger a full historical resync (POST /pipelines/{id}/actions/resync)
       - Waits for pipeline to reach INITIALIZED state (infinite retries with poll_interval)

    Supports three execution modes:
    1. Fire-and-forget: ``wait_for_completion=False`` - Returns job_id
    2. Synchronous wait: ``deferrable=False, wait_for_completion=True`` - Blocks worker
    3. Deferrable wait: ``deferrable=True, wait_for_completion=True`` - Releases worker (recommended)

    :param pipeline_id: The Hevo pipeline ID to sync or resync.
    :param action: Pipeline action to trigger (default: PipelineAction.SYNC_NOW).
                  - SYNC_NOW: Regular sync (validates pipeline in INITIALIZED state, fails if not)
                  - RESYNC: Full historical resync (waits indefinitely for INITIALIZED state before triggering)
    :param job_type: Type of job to wait for. Defaults intelligently based on action:
                    - SYNC_NOW: JobType.INCREMENTAL (default)
                    - RESYNC: JobType.TRUNCATE_AND_LOAD (default)
                    Used when discovering the active job after triggering.
    :param connection_id: Airflow connection ID for Hevo API credentials (default: hevo_airflow_conn_id).
    :param poll_interval: Seconds between status checks when waiting (default: 15).
                         For RESYNC action, also used as the interval for polling pipeline INITIALIZED status.
    :param retry_limit: Maximum HTTP retry attempts for API requests (default: 10).
                       Note: Does not apply to RESYNC pipeline status polling, which has infinite retries.
    :param deferrable: Use deferrable execution to release worker slot while waiting (default: True).
                      Requires Airflow triggerer service to be running.
    :param wait_for_completion: Wait for the job to complete before returning (default: True).
                               If False, returns job_id immediately via XCom.
    :param accept_completed_with_failures: Treat COMPLETED_WITH_FAILURES status as success (default: False).
                                          Useful when partial failures are acceptable.
    :param ensure_new_job: If True, fails if a job is already in progress for the pipeline (default: True).
                          If False, proceeds normally even if a job already exists.
                          Useful to prevent triggering duplicate jobs when previous jobs are still running.
                          Only applies to SYNC_NOW action.
    :param drop_and_load: If True, drops existing destination tables before loading (default: False).
                         Only applies to RESYNC action. When enabled, destination tables are dropped
                         and recreated, ensuring a clean slate for the historical data reload.
    """

    template_fields = ("pipeline_id",)

    def __init__(  # noqa: PLR0913
        self,
        pipeline_id: int,
        connection_id: str = "hevo_airflow_conn_id",
        action: PipelineAction = PipelineAction.SYNC_NOW,
        job_type: Optional[JobType] = None,
        poll_interval: int = 15,
        retry_limit: int = 10,
        deferrable: bool = True,
        wait_for_completion: bool = True,
        accept_completed_with_failures: bool = False,
        ensure_new_job: bool = True,
        drop_and_load: bool = False,
        **kwargs,
    ) -> None:
        self.pipeline_id = pipeline_id
        self.action = action
        self.poll_interval = poll_interval
        self.connection_id = connection_id
        if job_type is None:
            self.job_type = JobType.TRUNCATE_AND_LOAD if action == PipelineAction.RESYNC else JobType.INCREMENTAL
        else:
            self.job_type = job_type
        self.deferrable = deferrable
        self.retry_limit = retry_limit
        self.wait_for_completion = wait_for_completion
        self.accept_completed_with_failures = accept_completed_with_failures
        self.ensure_new_job = ensure_new_job
        self.drop_and_load = drop_and_load
        super().__init__(**kwargs)

    def execute(self, context: Context) -> None | str:  # noqa: ARG002
        """
        Execute the pipeline sync or resync operation.

        Execution flow:
        1. Triggers action based on self.action:
           - SYNC_NOW: Validates pipeline is in INITIALIZED state, then triggers sync
             (POST /pipelines/{id}/actions/sync-now)
           - RESYNC: Waits for pipeline to reach INITIALIZED state (with infinite retries),
             then triggers full historical resync (POST /pipelines/{id}/actions/resync)
        2. Polls up to retry_limit times (poll_interval seconds) for active job to appear
        3. Depending on configuration:
           - wait_for_completion=False: Returns job_id via XCom
           - deferrable=True: Defers to HevoTrigger for async monitoring
           - deferrable=False: Polls synchronously until completion

        :param context: Airflow execution context with task instance, DAG info, etc.
        :returns: Job ID string if ``wait_for_completion=False``, otherwise ``None``.
        :raises AirflowException: If pipeline validation fails (SYNC_NOW only), action trigger fails,
                                 or no active job is found after retry_limit attempts.
        """
        hook = self.hook

        # Trigger the appropriate action
        if self.action == PipelineAction.SYNC_NOW:
            hook.validate_pipeline(self.pipeline_id)
            self.log.info("Triggering sync for pipeline %s", self.pipeline_id)
            hook.trigger_pipeline_sync(self.pipeline_id, self.ensure_new_job)
            self.log.info("Sync triggered successfully for pipeline %s", self.pipeline_id)
        elif self.action == PipelineAction.RESYNC:
            # Wait for pipeline to reach INITIALIZED status before triggering
            self._wait_for_pipeline_initialized()

            self.log.info(
                "Triggering full historical resync for pipeline %s (drop_and_load=%s)",
                self.pipeline_id,
                self.drop_and_load,
            )
            hook.resync_pipeline_sync(self.pipeline_id, self.drop_and_load)
            self.log.info("Resync triggered successfully for pipeline %s", self.pipeline_id)

        # Wait for the active job to appear
        self.log.info("Waiting for active %s job for pipeline %s", self.job_type.value, self.pipeline_id)
        wait_for_job = 0
        active_job = None

        while wait_for_job < self.retry_limit:
            try:
                active_job = hook.find_active_job_by_type_sync(pipeline_id=self.pipeline_id, job_type=self.job_type)
                if active_job is not None:
                    self.log.info("Found active job on attempt %s", wait_for_job + 1)
                    break
            except AirflowException as e:
                wait_for_job += 1
                self.log.debug("No active job found on attempt %s: %s", wait_for_job + 1, str(e))
                # Continue to retry - job may not have appeared yet
            sleep(self.poll_interval)

        if active_job is None:
            self.log.error(
                "No active %s job found for pipeline %s after %s attempts",
                self.job_type.value,
                self.pipeline_id,
                self.retry_limit,
            )
            raise AirflowException(
                f"No active {self.job_type.value} job found for pipeline {self.pipeline_id} "
                f"after {self.retry_limit} attempts"
            )

        job_id = active_job.job_id
        self.log.info("Found active job %s for pipeline %s", job_id, self.pipeline_id)

        if not self.wait_for_completion:
            self.log.info("Not waiting for completion, returning job_id %s", job_id)
            return job_id

        if self.deferrable:
            self.log.info("Deferring execution using trigger for job %s", job_id)
            self.defer(
                timeout=self.execution_timeout,
                trigger=HevoTrigger(
                    pipeline_id=self.pipeline_id,
                    job_id=job_id,
                    job_type=self.job_type,
                    poke_interval=self.poll_interval,
                    accept_completed_with_failures=self.accept_completed_with_failures,
                    connection_id=self.connection_id,
                ),
                method_name="execute_complete",
            )
        else:
            self.log.info("Waiting synchronously for job %s to complete", job_id)
            self._wait_synchronously(job_id)
        return job_id

    def execute_complete(self, context: Context, event: dict[str, Any] | None = None) -> None:  # noqa: ARG002
        """
        Handle the trigger completion event (deferrable mode callback).

        Called by Airflow when the HevoTrigger fires, indicating that the job
        has reached a terminal state (completed, failed, or error).

        :param context: Airflow execution context with task instance, DAG info, etc.
        :param event: Trigger event payload with keys:
                     - status: "success" or "error"
                     - message: Description of the result
                     - job_id: Job identifier
                     - completed_with_failures: (optional) Boolean flag
        :raises AirflowException: If event is None, status is "error", or unexpected status received.
        """
        self.log.info("Received trigger completion event")
        if event is None:
            self.log.error("Trigger event is None")
            raise AirflowException("Trigger event is None")

        status = event.get("status")
        message = event.get("message", "Unknown status")
        job_id = event.get("job_id")
        self.log.info("Trigger event status: %s, job_id: %s", status, job_id)

        if status == "success":
            self.log.info(message)
            if event.get("completed_with_failures"):
                self.log.warning("Job %s completed with some failures", job_id)
        elif status == "error":
            self.log.error("Job %s failed: %s", job_id, message)
            raise AirflowException(f"Job {job_id} failed: {message}")
        else:
            self.log.error("Unexpected trigger event status: %s", status)
            raise AirflowException(f"Unexpected trigger event status: {status}")

    def _wait_for_pipeline_initialized(self) -> None:
        """
        Wait for the pipeline to reach INITIALIZED status before triggering resync.

        Continuously polls the pipeline status at poll_interval until the pipeline
        reaches INITIALIZED state. This method has no retry limit and will poll
        indefinitely until the pipeline is ready.

        Used exclusively for RESYNC action to ensure the pipeline is in a valid
        state before triggering a full historical resync operation.

        :raises AirflowException: If pipeline does not exist or if there's an API error.
        """
        attempt = 0

        while True:
            attempt += 1
            pipeline = self.hook.get_pipeline_sync(self.pipeline_id)
            if pipeline is None:
                raise AirflowException(f"Pipeline {self.pipeline_id} does not exist")
            current_status = pipeline.status
            if current_status == PipelineStatus.INITIALIZED:
                self.log.info("Pipeline %s is now in INITIALIZED status, ready for resync", self.pipeline_id)
                return
            if current_status != PipelineStatus.RESTARTING:
                raise AirflowException(f"Pipeline {self.pipeline_id} is not in INITIALIZED state: {current_status}")
            # Pipeline not initialized yet, wait and retry
            sleep(self.poll_interval)

    def _wait_synchronously(self, job_id: str) -> None:
        """
        Wait for the job to complete synchronously (blocks worker).

        Continuously polls the job status at poll_interval until the job reaches
        a terminal state. This blocks the Airflow worker slot during the entire
        wait period, which can be inefficient for long-running jobs.

        **Recommendation**: Use ``deferrable=True`` instead to release the worker
        slot and allow the triggerer service to handle polling asynchronously.

        :param job_id: Job identifier to monitor.
        :raises AirflowException: If the job fails (status "failed").
        """
        self.log.info("Waiting synchronously for job %s to complete", job_id)

        while True:
            is_completed = self.hook.get_job_completion_status_sync(
                self.pipeline_id, job_id, self.accept_completed_with_failures
            )
            if is_completed in [JobCompletionStatus.COMPLETED, JobCompletionStatus.COMPLETED_WITH_FAILURES]:
                self.log.info("Job %s completed successfully (status: %s)", job_id, is_completed.value)
                return
            if is_completed == JobCompletionStatus.PENDING:
                sleep(self.poll_interval)
            else:
                self.log.error("Job %s failed with status: %s", job_id, is_completed.value)
                raise AirflowException(f"Job {job_id} failed.")

    def get_openlineage_facets_on_start(self) -> OperatorLineage | None:
        """
        Return OpenLineage facets when the operator starts execution.

        Called by Airflow's OpenLineage integration at task start time.
        Returns basic pipeline information since job details are not yet available.

        This method is automatically called by the OpenLineage listener when installed.
        If OpenLineage dependencies are not installed, returns None gracefully.

        :returns: OperatorLineage with job facets, or None if OpenLineage not available.
        """
        try:
            from .openlineage import (  # noqa: PLC0415
                OPENLINEAGE_AVAILABLE,
                DocumentationDatasetFacet,
                OperatorLineage,
            )

            if not OPENLINEAGE_AVAILABLE:
                self.log.debug("OpenLineage not available, skipping facet generation")
                return None

            # Get pipeline info for documentation
            pipeline = self.hook.get_pipeline_sync(self.pipeline_id)
            if pipeline is None:
                self.log.warning("Pipeline %s not found, cannot generate lineage", self.pipeline_id)
                return None

            # Create job facets with pipeline documentation
            job_facets = {
                "documentation": DocumentationDatasetFacet(
                    description=(
                        f"Hevo pipeline '{pipeline.name}' (ID: {pipeline.id}) "
                        f"triggered via {self.action.value} action. "
                        f"Source: {pipeline.source.source_type} ({pipeline.source.name}) -> "
                        f"Destination: {pipeline.destination.destination_type} ({pipeline.destination.name})"
                    )
                )
            }

            # At start time, we don't have object-level details yet
            # Return basic lineage with job facets only
            return OperatorLineage(
                inputs=[],
                outputs=[],
                job_facets=job_facets,
                run_facets={},
            )

        except ImportError:
            self.log.debug("OpenLineage dependencies not installed")
            return None
        except Exception as e:
            self.log.warning("Failed to generate OpenLineage facets on start: %s", e)
            return None

    def _fetch_pipeline_objects_for_lineage(self) -> list[PipelineObject]:
        """
        Fetch pipeline objects with full details.

        Uses hook methods to fetch pipeline objects with pagination and
        full object details including fields for schema information.

        :returns: List of PipelineObject models (with field details)
        """
        objects: list[PipelineObject] = []
        cursor = None

        try:
            while True:
                # Fetch list of objects using hook method
                response = self.hook.list_pipeline_objects_sync(
                    pipeline_id=self.pipeline_id,
                    limit=100,
                    cursor=cursor,
                )

                # For each object, fetch full details including fields
                for obj in response.data:
                    try:
                        full_obj = self.hook.get_pipeline_object_sync(
                            pipeline_id=self.pipeline_id,
                            object_id=obj.object_id,
                        )
                        objects.append(full_obj)
                    except Exception as obj_err:
                        self.log.debug("Failed to fetch object %s details: %s", obj.object_id, obj_err)
                        # Fall back to basic object data from list
                        objects.append(obj)

                # Check for more pages
                if not response.has_more:
                    break
                cursor = response.next_cursor

        except Exception as e:
            self.log.warning("Failed to fetch pipeline objects for lineage: %s", e)

        return objects

    def get_openlineage_facets_on_complete(self, task_instance: Any) -> OperatorLineage | None:
        """
        Return OpenLineage facets when the operator completes execution.

        Called by Airflow's OpenLineage integration at task completion.
        Fetches pipeline objects and creates full dataset lineage including
        source (input) and destination (output) datasets with schema information.

        This method is automatically called by the OpenLineage listener when installed.
        If OpenLineage dependencies are not installed, returns None gracefully.

        :param task_instance: Airflow TaskInstance with execution context
        :returns: OperatorLineage with inputs, outputs, and facets, or None if not available.
        """
        try:
            from .openlineage import (  # noqa: PLC0415
                OPENLINEAGE_AVAILABLE,
                DocumentationDatasetFacet,
                ErrorMessageRunFacet,
                OperatorLineage,
            )
            from .openlineage.utils import create_datasets_from_pipeline_objects  # noqa: PLC0415

            if not OPENLINEAGE_AVAILABLE:
                self.log.debug("OpenLineage not available, skipping facet generation")
                return None

            pipeline = self.hook.get_pipeline_sync(self.pipeline_id)
            if pipeline is None:
                self.log.warning("Pipeline %s not found, cannot generate lineage", self.pipeline_id)
                return None
            # Create job facets with pipeline documentation
            job_facets = {
                "documentation": DocumentationDatasetFacet(
                    description=(
                        f"Hevo pipeline '{pipeline.name}' (ID: {pipeline.id}) "
                        f"completed via {self.action.value} action. "
                        f"Source: {pipeline.source.source_type} ({pipeline.source.name}) -> "
                        f"Destination: {pipeline.destination.destination_type} ({pipeline.destination.name})"
                    )
                )
            }
            objects = self._fetch_pipeline_objects_for_lineage()
            inputs, outputs = create_datasets_from_pipeline_objects(pipeline, objects)

            # Check if task failed and add error facet
            run_facets: dict[str, object] = {}
            if task_instance and hasattr(task_instance, "state") and str(task_instance.state) == "failed":
                run_facets["errorMessage"] = ErrorMessageRunFacet(
                    message=f"Hevo pipeline {self.pipeline_id} sync failed",
                    programmingLanguage="python",
                )

            return OperatorLineage(
                inputs=inputs,
                outputs=outputs,
                job_facets=job_facets,
                run_facets=run_facets,
            )

        except ImportError:
            self.log.debug("OpenLineage dependencies not installed")
            return None
        except Exception as e:
            self.log.warning("Failed to generate OpenLineage facets on complete: %s", e)
            return None

    @cached_property
    def hook(self) -> HevoPipelineHook:
        """
        Create and return a cached HevoPipelineHook instance.

        Uses cached_property to ensure only one hook instance is created per
        operator execution, improving efficiency.

        :returns: Configured HevoPipelineHook with connection_id.
        """
        return HevoPipelineHook(
            connection_id=self.connection_id,
        )
