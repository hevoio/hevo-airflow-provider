from __future__ import annotations

from functools import cached_property
from time import sleep
from typing import Any, Iterable

import requests
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from airflow.models import Connection
from requests import exceptions as requests_exceptions


class BaseHevoHook(BaseHook):
    """
    Common helper for interacting with the Hevo Edge API.

    Handles connection lookup, request construction, retries, and logging so
    concrete hooks can stay focused on business logic.
    """

    conn_name_attr = "conn_id"
    default_conn_name = "hevo_airflow_conn_id"
    conn_type = "hevo_edge"
    hook_name = "Hevo Edge"
    api_user_agent = "hevo_airflow_provider"

    def __init__(
        self,
        pipeline_id: int | None = None,
        conn_id: str | None = None,
        hevo_connection: Connection | None = None,
        retry_limit: int = 3,
        retry_delay: int = 5,
        timeout: int = 30,
        extra_headers: dict[str, str] | None = None,
    ) -> None:

        super().__init__()
        self.pipeline_id = pipeline_id
        self.conn_id = conn_id or self.default_conn_name
        self.hevo_connection = hevo_connection
        self.retry_limit = retry_limit
        self.retry_delay = retry_delay
        self.timeout = timeout
        self.extra_headers = extra_headers or {}

    @cached_property
    def connection(self) -> Connection:
        """
        Resolve and cache the Airflow connection. Raises if host is missing to
        avoid unclear request failures later.
        """
        conn = self.hevo_connection or self.get_connection(self.conn_id)
        if not conn.host:
            raise AirflowException("Hevo connection must define a host (base API domain).")
        return conn

    @cached_property
    def base_url(self) -> str:
        schema = self.connection.schema or "https"
        host = self.connection.host.rstrip("/")
        return f"{schema}://{host}"

    def _build_url(self, endpoint: str) -> str:
        endpoint = endpoint.lstrip("/")
        return f"{self.base_url}/{endpoint}" if endpoint else self.base_url

    def _prepare_api_call_kwargs(self, method: str, **kwargs: Any) -> dict[str, Any]:
        """Add authentication, headers, and defaults to the API call."""
        kwargs.setdefault("auth", (self.connection.login, self.connection.password))
        kwargs.setdefault("timeout", self.timeout)

        merged_headers: dict[str, str] = {
            "User-Agent": self.api_user_agent + self._get_airflow_version(),
        }
        # Allow connection extras and caller-supplied overrides.
        merged_headers.update(self.connection.extra_dejson.get("headers", {}))
        merged_headers.update(self.extra_headers)
        merged_headers.update(kwargs.pop("headers", {}) or {})

        kwargs["headers"] = merged_headers
        return kwargs

    @staticmethod
    def _get_airflow_version() -> str:
        """
        Fetch and return the current Airflow version.

        Adapted from AWS provider:
        https://github.com/apache/airflow/blob/ae25a52ae342c9e0bc3afdb21d613447c3687f6c/airflow/providers/amazon/aws/hooks/base_aws.py#L536
        """
        try:
            from airflow import __version__ as airflow_version  # local import avoids circular dependency

            return "-airflow_version/" + airflow_version
        except Exception:
            # Under no condition should an error here ever cause an issue for the user.
            return ""

    def trigger_api_call(
        self,
        method: str,
        endpoint: str,
        expected_status: Iterable[int] | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """
        Perform an HTTP request with retries and helpful logging.

        Returns the JSON payload (or {} when no body is present). Raises
        AirflowException with the underlying requests exception attached once
        retry attempts are exhausted.
        """
        url = self._build_url(endpoint)
        kwargs = self._prepare_api_call_kwargs(method, **kwargs)

        attempt_num = 1
        while True:
            try:
                response = requests.request(method=method, url=url, **kwargs)
                if expected_status and response.status_code not in expected_status:
                    raise AirflowException(
                        f"Unexpected status {response.status_code} for {method} {url}: {response.text}"
                    )
                response.raise_for_status()
                return response.json() if response.content else {}
            except requests_exceptions.RequestException as exc:
                self._log_request_error(attempt_num, method, url, exc)

                if attempt_num >= self.retry_limit:
                    raise AirflowException(
                        f"Edge API request failed after {attempt_num} attempts"
                    ) from exc

            attempt_num += 1
            sleep(self.retry_delay)

    def _log_request_error(self, attempt_num: int, method: str, url: str, error: Exception) -> None:
        self.log.warning(
            "Attempt %s API request %s %s failed: %s",
            attempt_num,
            method,
            url,
            error,
        )
