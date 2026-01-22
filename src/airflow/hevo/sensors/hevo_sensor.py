from __future__ import annotations

from functools import cached_property
from time import sleep
from typing import TYPE_CHECKING, Any, Optional

from airflow.exceptions import AirflowException
from airflow.sensors.base import BaseSensorOperator

if TYPE_CHECKING:
    from airflow.utils.context import Context

from airflow.hevo.hooks.hevo_pipeline_hook import HevoPipelineHook
from airflow.hevo.models.job import JobCompletionStatus, JobType
from airflow.hevo.triggers.hevo_trigger import HevoTrigger


class HevoSensor(BaseSensorOperator):
    """
    Sensor to monitor Hevo pipeline job completion with auto-discovery support.

    Monitors a Hevo pipeline job until it reaches a terminal state (completed or failed).
    Supports two modes:
    1. **Explicit job_id**: Monitor a specific job (useful after HevoOperator with wait_for_completion=False)
    2. **Auto-discovery**: Automatically find and monitor the active job for a pipeline

    **Typical Usage**:
    ```python
    # With explicit job_id from XCom
    sensor = HevoSensor(
        task_id="wait_for_sync",
        pipeline_id=123,
        job_id="{{ ti.xcom_pull(task_ids='trigger_sync') }}",
        deferrable=True
    )

    # With auto-discovery
    sensor = HevoSensor(
        task_id="wait_for_sync",
        pipeline_id=123,
        job_type="INCREMENTAL",
        deferrable=True
    )
    ```

    :param pipeline_id: Unique Hevo pipeline identifier to monitor.
    :param job_id: Optional job identifier. If provided, monitors this specific job.
                  If not provided, discovers the active job via auto-discovery.
                  Supports Jinja templating for XCom pulls.
    :param job_type: Job type for auto-discovery (default: JobType.INCREMENTAL).
                    Can be JobType enum or string for backwards compatibility.
                    Only used when job_id is not provided.
    :param connection_id: Airflow connection ID for Hevo API credentials (default: None uses default).
    :param poke_interval: Seconds between status checks (default: 5).
    :param accept_completed_with_failures: Treat COMPLETED_WITH_FAILURES as success (default: False).
    :param deferrable: Use deferrable mode to release worker slot while waiting (default: True).
                      Requires Airflow triggerer service.
    :param wait_for_job_max_attempts: Max attempts to find active job in auto-discovery (default: 10).
    :param wait_for_job_interval: Seconds between discovery attempts (default: 5).
    :param wait_for_job_initial_delay: Initial delay before first discovery attempt (default: 10).
    """

    template_fields = ("pipeline_id", "job_id")

    def __init__(
            self,
            pipeline_id: int,
            job_id: Optional[str] = None,
            job_type: JobType = JobType.INCREMENTAL,
            connection_id: Optional[str] = None,
            poke_interval: int = 5,
            accept_completed_with_failures: bool = False,
            deferrable: bool = True,
            wait_for_job_max_attempts: int = 10,
            wait_for_job_interval: int = 5,
            wait_for_job_initial_delay: int = 10,
            **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.pipeline_id = pipeline_id
        self.job_id = job_id
        self.job_type = job_type
        self.connection_id = connection_id
        self.poke_interval = poke_interval
        self.accept_completed_with_failures = accept_completed_with_failures
        self.deferrable = deferrable
        self.wait_for_job_max_attempts = wait_for_job_max_attempts
        self.wait_for_job_interval = wait_for_job_interval
        self.wait_for_job_initial_delay = wait_for_job_initial_delay

    def execute(self, context: Context) -> None:
        """
        Execute sensor logic - check job status and defer if needed.

        Execution flow:
        1. If deferrable=False: Uses standard poke() polling
        2. If deferrable=True: Checks poke() once, then defers to HevoTrigger

        :param context: Airflow execution context.
        """
        self.log.info("Starting sensor execution for pipeline %s", self.pipeline_id)
        if not self.deferrable:
            self.log.info("Running in non-deferrable mode")
            super().execute(context=context)
        elif not self.poke(context):
            self.defer(
                timeout=self.execution_timeout,
                trigger=HevoTrigger(
                    pipeline_id=self.pipeline_id,
                    job_id=self.job_id,
                    job_type=self.job_type,
                    poke_interval=self.poke_interval,
                    accept_completed_with_failures=self.accept_completed_with_failures,
                    connection_id=self.connection_id,
                ),
                method_name="execute_complete",
            )

    def _get_job_id(self) -> Optional[str]:
        """
        Get the job_id via explicit parameter or auto-discovery.

        If self.job_id is set, returns it immediately. Otherwise, searches for the
        active job matching the specified job_type. Uses configurable retry logic
        to handle the delay between sync trigger and job appearance.

        Auto-discovery flow:
        1. Initial delay (wait_for_job_initial_delay seconds)
        2. Poll up to wait_for_job_max_attempts times
        3. Sleep wait_for_job_interval between attempts
        4. Return Job model's job_id field when found

        :param context: Airflow execution context.
        :returns: Job ID string if found, ``None`` if not found (only in quick check mode).
        :raises AirflowException: When with_wait=True and no job found after all attempts.
        """
        if self.job_id:
            self.log.debug("Using provided job_id: %s", self.job_id)
            return self.job_id

        self.log.info("Job ID not provided, discovering active job (with_wait=%s)", with_wait)
        hook = self.hook

        wait_for_job = 0
        active_job = None
        max_attempts = self.wait_for_job_max_attempts

        while wait_for_job < max_attempts:
            try:
                active_job = hook.find_active_job_by_type(self.pipeline_id, self.job_type)
                if active_job is not None:
                    self.log.info("Found active job on attempt %s", wait_for_job + 1)
                    break
            except AirflowException as e:
                # No active job found yet, continue waiting
                self.log.debug("No active job found on attempt %s: %s", wait_for_job + 1, e)
                pass

            if wait_for_job < max_attempts - 1:
                sleep(self.wait_for_job_interval)
            wait_for_job += 1

        if active_job is None:
            job_type_str = self.job_type.value if hasattr(self.job_type, 'value') else str(self.job_type)
            self.log.error("No active %s job found for pipeline %s after %s attempts", job_type_str,
                           self.pipeline_id, max_attempts)
            raise AirflowException(
                f"No active {job_type_str} job found for pipeline {self.pipeline_id}"
            )

        # Job model always has job_id (required field)
        job_id = active_job.job_id
        return job_id

    def poke(self, context: Context) -> bool:
        """
        Check if the job has completed (sensor polling method).

        Called repeatedly by Airflow's sensor mechanism at poke_interval until
        it returns True or times out. Handles both explicit job_id and auto-discovery.

        Status handling:
        - "completed": Returns True (success)
        - "completed_with_failures": Returns True if accept_completed_with_failures=True, else raises exception
        - "failed": Raises AirflowException
        - "pending": Returns False (continues polling)

        :param context: Airflow execution context.
        :returns: ``True`` if job completed successfully, ``False`` if still pending.
        :raises AirflowException: If job failed or completed with failures (when not accepting failures).
        """
        self.log.debug("Poking to check job completion status")
        hook = self.hook

        # Get job_id if not provided (quick check without waiting)
        job_id = self.job_id
        if not job_id:
            job_id = self._get_job_id()
            if job_id:
                # Cache the discovered job_id to avoid re-discovering
                self.job_id = job_id
            else:
                # No active job found yet
                return False

        if not job_id:
            return False

        # Check job status
        status = hook.get_job_completion_status(
            self.pipeline_id,
            job_id,
            self.accept_completed_with_failures,
        )

        if status == JobCompletionStatus.COMPLETED:
            self.log.info("Job %s completed successfully", job_id)
            return True
        elif status == JobCompletionStatus.COMPLETED_WITH_FAILURES and self.accept_completed_with_failures:
            self.log.warning("Job %s completed with failures (accepting as success)", job_id)
            return True
        elif status in [JobCompletionStatus.FAILED, JobCompletionStatus.COMPLETED_WITH_FAILURES]:
            self.log.error("Job %s failed with status: %s", job_id, status.value)
            raise AirflowException(f"Job {job_id} failed for pipeline {self.pipeline_id}")
        else:
            # Status is JobCompletionStatus.PENDING
            return False

    def execute_complete(self, context: Context, event: dict[Any, Any] | None = None) -> None:
        """
        Handle trigger completion event (deferrable mode callback).

        Called by Airflow when HevoTrigger fires. The trigger handles most logic,
        so this method primarily validates the event and logs the result.

        :param context: Airflow execution context.
        :param event: Trigger event payload with keys:
                     - status: "success" or "error"
                     - message: Description of result
                     - job_id: Job identifier
                     - completed_with_failures: (optional) Boolean flag
        :raises AirflowException: If status is "error" indicating job failure.
        """
        self.log.info("Received trigger completion event in sensor")
        if event:
            status = event.get("status")
            job_id = event.get("job_id")
            message = event.get("message", "Unknown")
            self.log.info("Trigger event status: %s, job_id: %s", status, job_id)
            if status == "error":
                msg = "{0}: {1}".format(status, message)
                raise AirflowException(msg)
            if status == "success":
                self.log.info("Job %s completed successfully: %s", job_id, message)
                if event.get("completed_with_failures"):
                    self.log.warning("Job %s completed with some failures", job_id)
        else:
            self.log.warning("Trigger event is None")

    @cached_property
    def hook(self) -> HevoPipelineHook:
        """
        Create and return a cached HevoPipelineHook instance.

        Uses cached_property to ensure only one hook instance is created per
        sensor execution, improving efficiency and reusing HTTP session.

        :returns: Configured HevoPipelineHook with pipeline_id and connection_id.
        """
        return HevoPipelineHook(
            pipeline_id=self.pipeline_id,
            connection_id=self.connection_id,
        )
