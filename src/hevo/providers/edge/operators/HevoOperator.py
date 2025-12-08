from __future__ import annotations

from asyncio import sleep
from functools import cached_property
from typing import TYPE_CHECKING

from airflow.models import BaseOperator, BaseOperatorLink

from src.hevo.providers.edge.hooks.base_edge import EdgeBaseHook

from src.hevo.providers.edge.triggers.edge import EdgeTrigger

if TYPE_CHECKING:
    from airflow.utils.context import Context


class HevoOperator(BaseOperator):
    def __init__(
            self,
            pipeline_id: int = None,
            sync_type: str = "ON_DEMAND",
            job_type: str = "INCREMENTAL",
            poll_interval: int = 30,
            retry_limit: int = 5,
            deferrable: bool = False,
            wait_for_completion: bool = True,
            accept_completed_with_failures: bool = False,
            **kwargs) -> None:
        self.pipeline_id = pipeline_id
        self.poll_interval = poll_interval
        self.sync_type = sync_type
        self.job_type = job_type
        self.deferrable = deferrable
        self.retry_limit = retry_limit
        self.wait_for_completion = wait_for_completion
        self.accept_completed_with_failures = accept_completed_with_failures
        super().__init__(**kwargs)

    def execute(self, context: Context) -> None | str:
        """Start the sync using synchronous hook"""
        hook = self.hook
        hook.validate_pipeline(self.pipeline_id, self.sync_type)
        # start_sync
        hook.sync_pipeline(self.pipeline_id)
        job_id = hook.get_active_job_for_type(self.pipeline_id, self.job_type)

        if not self.wait_for_completion:
            return job_id

        if self.deferrable:
            self.defer(
                timeout=self.execution_timeout,
                trigger=EdgeTrigger(
                    job_id=job_id,
                    pipeline_id=self.pipeline_id,
                    poke_interval=self.poll_interval,
                ),
                method_name="execute_complete",
            )

        self._wait_synchronously(job_id)
        return None

    def _wait_synchronously(self, job_id: str) -> None:
        """
        Wait for the task synchronously.

        It is recommended that you do not use this, and instead set
        `deferrable=True` and use a Triggerer if you want to wait for the task
        to complete.
        """
        while True:
            is_completed = self.hook.job_completed_successfully(
                self.pipeline_id, job_id,  self.accept_completed_with_failures
            )
            if is_completed:
                return
            else:
                self.log.info("sync is still running...")
                self.log.info("sleeping for %s seconds.", self.poll_frequency)
                sleep(self.poll_frequency)

    @cached_property
    def hook(self) -> EdgeBaseHook:
        """Create and return a FivetranHook."""
        return EdgeBaseHook(
            self.pipeline_id,
        )
