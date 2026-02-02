from __future__ import annotations

import asyncio
from functools import cached_property
from typing import TYPE_CHECKING, Any

from airflow.exceptions import AirflowException
from airflow.triggers.base import BaseTrigger, TriggerEvent

from airflow.hevo.hooks import HevoPipelineHook
from airflow.hevo.models.job import JobCompletionStatus, JobType

if TYPE_CHECKING:
    from collections.abc import AsyncIterator


class HevoTrigger(BaseTrigger):
    """
    Async trigger for monitoring Hevo pipeline job completion in deferrable mode.

    Runs in Airflow's triggerer service (separate process), continuously polling
    job status via async HTTP requests until the job reaches a terminal state.

    Supports two discovery modes:
    1. **Explicit job_id**: Monitors specified job immediately
    2. **Auto-discovery**: Searches for active job by type, then monitors it

    Yields a TriggerEvent when:
    - Job completes successfully
    - Job completes with failures (success if accept_completed_with_failures=True)
    - Job fails
    - Unrecoverable error occurs

    :param pipeline_id: Unique pipeline identifier to monitor.
    :param job_id: Optional job identifier. If ``None``, auto-discovers active job by type.
    :param job_type: Job type for auto-discovery (default: JobType.INCREMENTAL).
                    Can be JobType enum or string for compatibility with deserialization.
    :param poke_interval: Seconds between status checks (default: 5).
    :param accept_completed_with_failures: Treat COMPLETED_WITH_FAILURES as success (default: False).
    :param connection_id: Airflow connection ID for Hevo API credentials (default: None uses default connection).
    """

    def __init__(
        self,
        pipeline_id: int,
        connection_id: str,
        job_id: str | None = None,
        job_type: JobType = JobType.INCREMENTAL,
        poke_interval: int = 5,
        accept_completed_with_failures: bool = False,
    ) -> None:
        super().__init__()
        self.pipeline_id = pipeline_id
        self.job_id = job_id
        self.job_type = job_type
        self.poke_interval = poke_interval
        self.accept_completed_with_failures = accept_completed_with_failures
        self.connection_id = connection_id

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """
        Serialize trigger parameters for persistence across Airflow restarts.

        Required by Airflow's deferrable mechanism. All parameters must be
        JSON-serializable (primitives, dicts, lists - no Pydantic models or functions).

        :returns: Tuple of (fully qualified class path, parameter dictionary).
        """
        return (
            "airflow.hevo.trigger.HevoTrigger",
            {
                "pipeline_id": self.pipeline_id,
                "job_id": self.job_id,
                "job_type": self.job_type.value if isinstance(self.job_type, JobType) else self.job_type,
                "poke_interval": self.poke_interval,
                "accept_completed_with_failures": self.accept_completed_with_failures,
                "connection_id": self.connection_id,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """
        Async polling loop that monitors job status until terminal state.

        Execution flow:
        1. Create HevoPipelineHook (uses async methods)
        2. If job_id is None: Auto-discover active job via find_active_job_by_type_async()
        3. Poll job status via get_job_completion_status_async() every poke_interval seconds
        4. Check JobCompletionStatus enum (COMPLETED, COMPLETED_WITH_FAILURES, FAILED, PENDING)
        5. Yield TriggerEvent when terminal state reached

        TriggerEvent payload:
        - status: "success" or "error"
        - message: Human-readable description
        - pipeline_id: Pipeline identifier
        - job_id: Job identifier
        - completed_with_failures: (optional) Boolean flag for partial failures

        :yields: TriggerEvent when job completes, fails, or error occurs.
        :raises AirflowException: For job discovery failures (caught and yielded as error event).
        """
        self.log.info(
            "Starting trigger for pipeline %s, job %s",
            self.pipeline_id,
            self.job_id or "auto-discover",
        )

        try:
            # Create hook with connection details directly
            hook = self.hook

            # Discover job ID if not provided
            if self.job_id is None:
                active_job = await hook.find_active_job_by_type_async(self.pipeline_id, self.job_type)
                # find_active_job_by_type_async raises exception if no job found
                self.job_id = active_job.job_id
            else:
                self.log.info("Using provided job_id: %s", self.job_id)

            # Poll until completion
            self.log.info("Starting to poll job %s status", self.job_id)
            poll_count = 0
            while True:
                poll_count += 1
                status = await hook.get_job_completion_status_async(
                    self.pipeline_id,
                    self.job_id,
                    accept_completed_with_failures=self.accept_completed_with_failures,
                )

                if status == JobCompletionStatus.COMPLETED:
                    message = f"Pipeline {self.pipeline_id} synced successfully under job {self.job_id}"
                    self.log.info("Job %s completed successfully after %s polls", self.job_id, poll_count)
                    yield TriggerEvent(
                        {
                            "status": "success",
                            "message": message,
                            "pipeline_id": self.pipeline_id,
                            "job_id": self.job_id,
                        }
                    )
                    return

                if status == JobCompletionStatus.COMPLETED_WITH_FAILURES:
                    message = f"Pipeline {self.pipeline_id} completed with failures under job {self.job_id}"
                    self.log.warning(
                        "Job %s completed with failures after %s polls (accepting as success)", self.job_id, poll_count
                    )
                    yield TriggerEvent(
                        {
                            "status": "success",
                            "message": message,
                            "pipeline_id": self.pipeline_id,
                            "job_id": self.job_id,
                            "completed_with_failures": True,
                        }
                    )
                    return

                if status == JobCompletionStatus.FAILED:
                    message = f"Pipeline {self.pipeline_id} job {self.job_id} failed"
                    self.log.error("Job %s failed after %s polls", self.job_id, poll_count)
                    yield TriggerEvent(
                        {
                            "status": "error",
                            "message": message,
                            "pipeline_id": self.pipeline_id,
                            "job_id": self.job_id,
                        }
                    )
                    return

                # Status is JobCompletionStatus.PENDING - continue polling
                self.log.debug(
                    "Job %s for pipeline %s is still pending. Waiting %s seconds before next check (poll %s).",
                    self.job_id,
                    self.pipeline_id,
                    self.poke_interval,
                    poll_count,
                )
                await asyncio.sleep(self.poke_interval)

        except AirflowException as exc:
            yield TriggerEvent(
                {
                    "status": "error",
                    "message": str(exc),
                    "pipeline_id": self.pipeline_id,
                    "job_id": self.job_id,
                }
            )
            return
        except Exception as exc:
            error_message = f"Unexpected error while monitoring pipeline {self.pipeline_id}: {exc}"
            yield TriggerEvent(
                {
                    "status": "error",
                    "message": error_message,
                    "pipeline_id": self.pipeline_id,
                    "job_id": self.job_id,
                }
            )
            return

    @cached_property
    def hook(self) -> HevoPipelineHook:
        """
        Create and return a cached HevoPipelineHook instance.

        Uses cached_property to ensure only one hook instance is created per
        sensor execution, improving efficiency and reusing HTTP session.

        :returns: Configured HevoPipelineHook with pipeline_id and connection_id.
        """
        return HevoPipelineHook(
            connection_id=self.connection_id,
        )
