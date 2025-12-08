from __future__ import annotations

import asyncio
from typing import Any

import aiohttp
from aiohttp import ClientResponseError
from airflow.exceptions import AirflowException
from airflow.models import Connection
from airflow.utils.helpers import is_container
from requests import HTTPError
from requests import exceptions as requests_exceptions

from src.hevo.providers.edge.hooks.base import BaseHevoHook


class EdgeHook(BaseHevoHook):

    def __init__(self,
                 pipeline_id: int = None,
                 hevo_connection: Connection | None = None,
                 retry_limit: int = 3,
                 retry_delay: int = 5,
                 timeout: int = 30,
                 extra_headers: dict[str, str] | None = None,
                 ) -> None:
        super().__init__()
        self.pipeline_id = pipeline_id
        self.hevo_connection = hevo_connection
        self.retry_limit = retry_limit
        self.retry_delay = retry_delay
        self.timeout = timeout
        self.extra_headers = extra_headers or {}

    def validate_pipeline(self, pipeline_id: int, sync_type: str = "ON_DEMAND") -> None:
        """ Validates that the pipeline exists and is active """
        pipeline = self.get_pipeline(pipeline_id)
        if not pipeline:
            raise AirflowException(f"Pipeline {pipeline_id} does not exist")
        if pipeline["status"]["type"] != 'INITIALIZED':
            raise AirflowException(f"Pipeline {pipeline_id} is not in active state")
        if pipeline["config"]["sync_type"] != sync_type:
            raise AirflowException(f"Pipeline {pipeline_id} is expected to have sync type {sync_type}")

    def get_pipeline(self, pipeline_id: int) -> dict[str, Any] | None:
        """ Fetches the pipeline details from the API """
        try:
            return self.trigger_api_call(method="GET", endpoint=f"/external-api/v1/pipelines/{pipeline_id}")
        except HTTPError as e:
            if e.response.status_code == 404:
                return None
            raise e

    def sync_pipeline(self, pipeline_id: int, ensure_new_job: bool = False) -> None:
        """ Starts the sync for the given pipeline """
        try:
            self.trigger_api_call(method="POST", endpoint=f"/external-api/v1/pipelines/{pipeline_id}/actions/sync")
        except HTTPError as e:
            if e.response.status_code == 500 and e.response.content.decode() == ("A job is in progress. To sync now "
                                                                                 "you need to cancel the current "
                                                                                 "job") and ensure_new_job is True:
                raise AirflowException(f"Pipeline {pipeline_id} does not exist")
            raise e

    def get_active_job_for_type(self, pipeline_id: int, job_type: str = "INCREMENTAL", page_limit: int = 10) -> \
            dict[str, Any] | None:
        """ Fetches the active job for the given pipeline """
        job = None
        cursor = None
        while job is None:
            jobs = self.trigger_api_call(method="GET", endpoint=f"/external-api/v1/pipelines/{pipeline_id}/jobs",
                                         params={"limit": page_limit, "cursor": cursor})
            for job in jobs["data"]:
                if job["type"] == job_type and job["status"]["value"] == "IN_PROGRESS":
                    return job
                cursor = job["page"]

            if cursor is None:
                raise AirflowException(f"Pipeline {pipeline_id} doesn't have any active jobs of type {job_type}.")

    def job_completed_successfully(self, pipeline_id: int, job_id: str,
                                   accept_completed_with_failures: bool = False) -> bool:
        """ Checks if the job is completed successfully """
        job = self.trigger_api_call(method="GET", endpoint=f"/external-api/v1/pipelines/{pipeline_id}/jobs/{job_id}")
        if job["status"]["value"] == "COMPLETED":
            return job["result"]["value"] == "SUCCESS"
        if job["status"]["value"] == "COMPLETED_WITH_FAILURES" and accept_completed_with_failures:
            return True
        return False


class EdgeAsyncHook(BaseHevoHook):
    """
    Edge API interaction hook extending BaseHevoHook for asynchronous fuctionality.

    :param pipeline_id: Reference to the pipeline id
    :param hevo_connection: Reference to the Hevo connection
    :param retry_limit: The number of times to retry the connection in case of
        service outages.
    :param retry_delay: Time (in seconds) to wait between each retry.
    :param timeout: The amount of time in seconds the requests library
        will wait before timing out.
    :param extra_headers: Optional additional headers to be added to the request.
    :param extra_kwargs: Optional additional keyword arguments to be passed to the request.
    """

    def __init__(
            self,
            pipeline_id: int = None,
            hevo_connection: Connection | None = None,
            retry_limit: int = 3,
            retry_delay: float = 1.0,
            timeout: int = 30,
            extra_headers: dict[str, str] | None = None,
            extra_kwargs: dict[str, Any] | None = None,
    ) -> None:
        super().__init__()
        self.pipeline_id = pipeline_id
        self.hevo_connection = hevo_connection
        self.retry_limit = retry_limit
        self.retry_delay = retry_delay
        self.timeout = timeout
        self.extra_headers = extra_headers or {}
        self.extra_kwargs = extra_kwargs or {}

    async def trigger_api_call_async(
            self, method: str, endpoint: str = None, **kwargs: Any  # type: ignore[assignment]
    ) -> dict[str, Any]:
        url = f"{self.api_protocol}://{self.api_host}/{endpoint}"

        kwargs = self._prepare_api_call_kwargs_async(method, **kwargs)

        async with aiohttp.ClientSession() as session:
            attempt_num = 1
            while True:
                try:
                    response = await session.request(method, url, **kwargs)
                    response.raise_for_status()
                    return await response.json()
                except ClientResponseError as e:
                    if not _retryable_error_async(e):
                        # In this case, the user probably made a mistake.
                        # Don't retry.
                        return {"Response": {e.message}, "Status Code": {e.status}}
                    self._log_request_error(attempt_num, str(e))

                if attempt_num == self.retry_limit:
                    raise AirflowException(f"API requests to Fivetran failed {self.retry_limit} times." " Giving up.")

                attempt_num += 1
                await asyncio.sleep(self.retry_delay)

    def _prepare_api_call_kwargs_async(self, method: str, **kwargs: Any) -> dict[str, Any]:
        kwargs = self._prepare_api_call_kwargs(method, **kwargs)
        auth = kwargs.get("auth")
        if auth is not None and is_container(auth) and 2 <= len(auth) <= 3:
            kwargs["auth"] = aiohttp.BasicAuth(*auth)
        return kwargs

    async def get_active_job_for_type_async(self, pipeline_id: int, job_type: str = "INCREMENTAL",
                                            page_limit: int = 10) -> \
            dict[str, Any] | None:
        """ Fetches the active job for the given pipeline """
        job = None
        cursor = None
        while job is None:
            jobs = await self.trigger_api_call_async(method="GET",
                                                     endpoint=f"/external-api/v1/pipelines/{pipeline_id}/jobs",
                                                     params={"limit": page_limit, "cursor": cursor})
            for job in jobs["data"]:
                if job["type"] == job_type and job["status"]["value"] == "IN_PROGRESS":
                    return job
                cursor = job["page"]

            if cursor is None:
                raise AirflowException(f"Pipeline {pipeline_id} doesn't have any active jobs of type {job_type}.")

    async def job_completed_successfully_async(self, pipeline_id: int, job_id: str,
                                               accept_completed_with_failures: bool = False) -> str:
        """ Checks if the job is completed successfully """
        job = await self.trigger_api_call_async(method="GET",
                                                endpoint=f"/external-api/v1/pipelines/{pipeline_id}/jobs/{job_id}")
        if job["status"]["value"] == "COMPLETED":
            return "completed"
        if job["status"]["value"] == "COMPLETED_WITH_FAILURES" and accept_completed_with_failures:
            return "completed_with_failures"
        if job["status"]["value"] in ["COMPLETED_WITH_FAILURES",
                                      "FAILED",
                                      "CANCELLED",
                                      "SKIPPED",
                                      "DEFERRED",
                                      "DEFERRED_WITH_FAILURE"]:
            self.log.info(f"Job {job_id} failed with status {job['status']['value']}.")
            return "failed"
        return "pending"


def _retryable_error_async(exception: ClientResponseError) -> bool:
    return exception.status >= 500


def _retryable_error(exception: Exception) -> bool:
    return isinstance(
        exception,
        (requests_exceptions.ConnectionError, requests_exceptions.Timeout),
    ) or (
            getattr(exception, "response", None) is not None
            and getattr(exception, "response").status_code >= 500  # noqa: B009
    )
