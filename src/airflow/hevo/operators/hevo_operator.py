from __future__ import annotations

from functools import cached_property
from time import sleep
from typing import TYPE_CHECKING, Any

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator

from airflow.hevo.hooks.hevo_pipeline_hook import HevoPipelineHook
from airflow.hevo.models.job import JobCompletionStatus, JobType
from airflow.hevo.models.pipeline import SyncType
from airflow.hevo.triggers.hevo_trigger import HevoTrigger

if TYPE_CHECKING:
    from airflow.utils.context import Context


class HevoOperator(BaseOperator):
    """
    Operator to trigger and optionally wait for Hevo pipeline syncs.

    Supports three execution modes:
    1. Fire-and-forget: ``wait_for_completion=False`` - Returns job_id
    2. Synchronous wait: ``deferrable=False, wait_for_completion=True`` - Blocks worker
    3. Deferrable wait: ``deferrable=True, wait_for_completion=True`` - Releases worker (recommended)

    :param pipeline_id: The Hevo pipeline ID to sync (must be in INITIALIZED state).
    :param sync_type: Type of sync (default: SyncType.ON_DEMAND).
    :param job_type: Type of job to wait for (default: JobType.INCREMENTAL).
                    Used when discovering the active job after triggering.
    :param connection_id: Airflow connection ID for Hevo API credentials (default: None uses default connection).
    :param poll_interval: Seconds between status checks when waiting (default: 5).
    :param retry_limit: Maximum HTTP retry attempts for API requests (default: 10).
    :param deferrable: Use deferrable execution to release worker slot while waiting (default: True).
                      Requires Airflow triggerer service to be running.
    :param wait_for_completion: Wait for the job to complete before returning (default: True).
                               If False, returns job_id immediately via XCom.
    :param accept_completed_with_failures: Treat COMPLETED_WITH_FAILURES status as success (default: False).
                                          Useful when partial failures are acceptable.
    :param ensure_new_job: If True, fails if a job is already in progress for the pipeline (default: True).
                          If False, proceeds normally even if a job already exists.
                          Useful to prevent triggering duplicate jobs when previous jobs are still running.
    """

    template_fields = ("pipeline_id",)

    def __init__(
            self,
            pipeline_id: int = None,
            sync_type: SyncType = SyncType.ON_DEMAND,
            job_type: JobType = JobType.INCREMENTAL,
            connection_id: str = None,
            poll_interval: int = 5,
            retry_limit: int = 10,
            deferrable: bool = True,
            wait_for_completion: bool = True,
            accept_completed_with_failures: bool = False,
            ensure_new_job: bool = True,
            **kwargs) -> None:
        self.pipeline_id = pipeline_id
        self.poll_interval = poll_interval
        self.sync_type = sync_type
        self.connection_id = connection_id
        self.job_type = job_type
        self.deferrable = deferrable
        self.retry_limit = retry_limit
        self.wait_for_completion = wait_for_completion
        self.accept_completed_with_failures = accept_completed_with_failures
        self.ensure_new_job = ensure_new_job
        super().__init__(**kwargs)

    def execute(self, context: Context) -> None | str:
        """
        Execute the pipeline sync operation.

        Execution flow:
        1. Validates pipeline is in INITIALIZED state
        2. Triggers sync via API (POST /pipelines/{id}/actions/sync-now)
        3. Polls up to 10 times (5s intervals) for active job to appear
        4. Depending on configuration:
           - wait_for_completion=False: Returns job_id via XCom
           - deferrable=True: Defers to HevoTrigger for async monitoring
           - deferrable=False: Polls synchronously until completion

        :param context: Airflow execution context with task instance, DAG info, etc.
        :returns: Job ID string if ``wait_for_completion=False``, otherwise ``None``.
        :raises AirflowException: If pipeline validation fails, sync trigger fails,
                                 or no active job is found after 10 attempts.
        """
        hook = self.hook
        hook.validate_pipeline(self.pipeline_id, self.sync_type)

        hook.trigger_pipeline_sync(self.pipeline_id, self.ensure_new_job)

        self.log.info("Waiting for active job for pipeline %s", self.pipeline_id)
        wait_for_job = 0
        active_job = None

        while wait_for_job < self.retry_limit:
            try:
                active_job = hook.find_active_job_by_type(pipeline_id=self.pipeline_id, job_type=self.job_type)
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
                self.job_type.value, self.pipeline_id, self.retry_limit
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

    def execute_complete(self, context: Context, event: dict[str, Any] | None = None) -> None:
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
            is_completed = self.hook.get_job_completion_status(
                self.pipeline_id, job_id, self.accept_completed_with_failures
            )
            if is_completed in [JobCompletionStatus.COMPLETED, JobCompletionStatus.COMPLETED_WITH_FAILURES]:
                self.log.info("Job %s completed successfully (status: %s)", job_id, is_completed.value)
                return
            elif is_completed == JobCompletionStatus.PENDING:
                sleep(self.poll_interval)
            else:
                self.log.error("Job %s failed with status: %s", job_id, is_completed.value)
                raise AirflowException(f"Job {job_id} failed.")

    @cached_property
    def hook(self) -> HevoPipelineHook:
        """
        Create and return a cached HevoPipelineHook instance.

        Uses cached_property to ensure only one hook instance is created per
        operator execution, improving efficiency.

        :returns: Configured HevoPipelineHook with pipeline_id and connection_id.
        """
        return HevoPipelineHook(
            pipeline_id=self.pipeline_id,
            connection_id=self.connection_id,
        )
