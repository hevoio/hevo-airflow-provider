from __future__ import annotations

import asyncio
from typing import Any

import aiohttp
from aiohttp import ClientResponseError
from airflow import __version__ as airflow_version
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from airflow.models import Connection
from asgiref.sync import sync_to_async


class BaseHevoHook(BaseHook):
    """
    Base hook for interacting with the Hevo API.

    This class provides common functionality for making authenticated HTTP requests
    to the Hevo API, including connection management, request construction, automatic
    retries, and comprehensive error handling.

    The hook supports:
    - Automatic connection resolution from Airflow connections
    - Configurable retry logic with fixed delay between attempts
    - Request/response logging
    - Custom headers and authentication
    - Status code validation

    Subclasses should implement async methods (suffixed with '_async') and provide
    corresponding sync wrappers that use asyncio.run() internally.
    """

    conn_name_attr = "conn_id"
    default_conn_name = "hevo_airflow_conn_id"
    hook_name = "Hevo"
    api_user_agent = "hevo_airflow_provider"

    def __init__(
            self,
            pipeline_id: int | None = None,
            connection_id: str | None = None,
            retry_limit: int = 3,
            retry_delay: int = 2,
            timeout: int = 30,
            extra_headers: dict[str, str] | None = None,
            extra_kwargs: dict[str, Any] | None = None,
            retryable_status_codes: list[int] | None = None,
    ) -> None:
        """
        Initialize a Hevo API hook with common request defaults.

        :param pipeline_id: Optional pipeline identifier used by downstream hooks.
        :param connection_id: Airflow connection id to resolve when no explicit connection is provided.
            Defaults to ``hevo_airflow_conn_id`` if not specified.
        :param retry_limit: Maximum number of request attempts before surfacing an error.
            Must be a positive integer. Defaults to 3.
        :param retry_delay: Seconds to wait between retry attempts. Must be a positive integer.
            Defaults to 2 seconds.
        :param timeout: HTTP request timeout in seconds. Must be a positive integer.
            Defaults to 30 seconds.
        :param extra_headers: Additional headers merged into every request. These headers
            take precedence over connection-level headers but can be overridden by
            per-request headers.
        :param extra_kwargs: Additional kwargs for aiohttp requests.
        :param retryable_status_codes: List of HTTP status codes that should trigger retries.
            Defaults to all 5xx server errors (500-599). Examples:
            - ``[500, 502, 503, 504]`` - Only retry on specific server errors
            - ``[429, 500, 502, 503]`` - Include rate limiting (429) in retries
            - ``[]`` - Disable status code-based retries (only network errors)
        """
        super().__init__()
        self.pipeline_id = pipeline_id
        self.connection_id = connection_id or self.default_conn_name

        # Lazy connection retrieval - don't fetch in __init__ to avoid AsyncToSync issues
        # when hook is created from async context (e.g., in triggers)
        self._connection: Connection | None = None
        self._connection_retrieved = False

        self.retry_limit = retry_limit
        self.retry_delay = retry_delay
        self.timeout = timeout
        self.extra_headers = extra_headers or {}
        self.extra_kwargs = extra_kwargs or {}
        self.airflow_version = airflow_version

        # Default to all 5xx server errors if not specified
        if retryable_status_codes is None:
            self.retryable_status_codes = list(range(500, 600))
        else:
            self.retryable_status_codes = retryable_status_codes

    async def _get_connection_async(self) -> Connection:
        """
        Get the connection asynchronously, using a thread pool to avoid AsyncToSync issues.
        
        This method safely retrieves the connection even when called from within an async
        event loop by running the synchronous get_connection() in a separate thread.
        
        :returns: The Airflow Connection object.
        :raises AirflowException: If connection cannot be retrieved or is invalid.
        """
        if self._connection_retrieved:
            if self._connection is None:
                raise AirflowException(
                    f"Connection {self.connection_id} was not properly initialized."
                )
            return self._connection
        
        # Use asyncio.to_thread to run the synchronous get_connection in a separate thread
        # This avoids the AsyncToSync error when called from within an async event loop
        try:
            self._connection = await sync_to_async(self.get_connection)(self.connection_id)
        except Exception as e:
            raise AirflowException(
                f"Failed to retrieve connection {self.connection_id}: {e}"
            ) from e
        
        if self._connection is None:
            raise AirflowException(f"Connection {self.connection_id} not found.")
        
        if not self._connection.host:
            raise AirflowException("Hevo connection must define a host (base API domain).")
        
        self._connection_retrieved = True
        return self._connection

    async def execute_api_request_async(
            self,
            method: str,
            endpoint: str | None = None,
            params: dict[str, Any] | None = None,
            json: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """
        Perform an asynchronous API request with retries.

        :param method: HTTP method (GET, POST, PUT, PATCH, DELETE, etc.).
        :param endpoint: API endpoint path relative to connection host.
        :param params: Query parameters to append to URL (for GET requests).
        :param json: JSON body to send with request (for POST/PUT/PATCH requests).
        :returns: JSON response as dictionary, or empty dict for 204/no-content responses.
        :raises AirflowException: On request failure after retries or non-retryable errors.
        """
        # Get connection asynchronously to avoid AsyncToSync issues
        conn = await self._get_connection_async()

        schema = conn.schema or "https"
        host = conn.host.rstrip("/") if conn.host else ""
        if not host:
            raise AirflowException("Hevo connection must define a host (base API domain).")

        # Build URL
        cleaned_endpoint = (endpoint or "").lstrip("/")
        url = f"{schema}://{host}/{cleaned_endpoint}" if cleaned_endpoint else f"{schema}://{host}"

        request_kwargs = self.build_async_request_kwargs(conn=conn)

        async with aiohttp.ClientSession() as session:
            attempt_num = 1
            last_exception = None

            while attempt_num <= self.retry_limit:
                try:
                    response = await session.request(method, url, params=params, json=json, **request_kwargs)
                    self.log.info("Request to %s returned status %s", url, response.status)

                    response.raise_for_status()

                    # Check if there's content to parse
                    if response.content_length == 0 or response.status == 204:
                        return {}

                    return await response.json()

                except ClientResponseError as e:
                    last_exception = e
                    if not self._retryable_error_async(e):
                        self.log.error("Non-retryable error: %s - %s", e.status, e.message)
                        raise AirflowException(
                            f"API request failed with status {e.status}: {e.message}"
                        ) from e

                    self.log.warning(
                        "Attempt %s failed for %s %s: %s - %s",
                        attempt_num, method, url, e.status, e.message
                    )

                except Exception as e:
                    last_exception = e
                    self.log.warning(
                        "Attempt %s failed for %s %s: %s",
                        attempt_num, method, url, str(e)
                    )

                # Check if we should retry
                if attempt_num >= self.retry_limit:
                    break

                attempt_num += 1
                await asyncio.sleep(self.retry_delay)

            # If we get here, all retries exhausted
            raise AirflowException(
                f"API request to {url} failed after {self.retry_limit} attempts"
            ) from last_exception

    def build_async_request_kwargs(self, conn: Connection) -> dict[str, Any]:
        """
        Prepare request kwargs for aiohttp.

        :param conn: Airflow connection object.

        :returns: Request kwargs dictionary for aiohttp.
        """
        merged_kwargs = {**self.extra_kwargs}

        # Set auth
        auth_tuple = (conn.login, conn.password)
        if auth_tuple[0] and auth_tuple[1]:
            merged_kwargs["auth"] = aiohttp.BasicAuth(*auth_tuple)

        # Set timeout
        merged_kwargs["timeout"] = aiohttp.ClientTimeout(total=self.timeout)

        # Build headers
        merged_headers: dict[str, str] = {
            "User-Agent": f"{self.api_user_agent}-airflow/{self.airflow_version}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        merged_headers.update(self.extra_headers)

        merged_kwargs["headers"] = merged_headers

        return merged_kwargs

    def _retryable_error_async(self, exception: ClientResponseError) -> bool:
        """
        Check if an async HTTP error should trigger a retry.

        :param exception: The ClientResponseError from aiohttp.
        :returns: True if the error status code is in the retryable list.
        """
        return exception.status in self.retryable_status_codes
