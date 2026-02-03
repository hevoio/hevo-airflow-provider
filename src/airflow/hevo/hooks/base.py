from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any, Optional

import aiohttp
from aiohttp import ClientResponseError
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from asgiref.sync import sync_to_async

from airflow import __version__ as airflow_version

if TYPE_CHECKING:
    from airflow.models import Connection


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
            connection_id: Optional[str] = None,
            retry_limit: int = 3,
            retry_delay: int = 2,
            timeout: int = 30,
            retryable_status_codes: Optional[list[int]] = None,
    ) -> None:
        """
        Initialize a Hevo API hook with common request defaults.

        :param connection_id: Airflow connection id to resolve when no explicit connection is provided.
            Defaults to ``hevo_airflow_conn_id`` if not specified.
        :param retry_limit: Maximum number of request attempts before surfacing an error.
            Must be a positive integer. Defaults to 3.
        :param retry_delay: Seconds to wait between retry attempts. Must be a positive integer.
            Defaults to 2 seconds.
        :param timeout: HTTP request timeout in seconds. Must be a positive integer.
            Defaults to 30 seconds.
        :param retryable_status_codes: List of HTTP status codes that should trigger retries.
            Defaults to all 5xx server errors (500-599). Examples:
            - ``[500, 502, 503, 504]`` - Only retry on specific server errors
            - ``[429, 500, 502, 503]`` - Include rate limiting (429) in retries
            - ``[]`` - Disable status code-based retries (only network errors)
        """
        super().__init__()
        self.connection_id = connection_id or self.default_conn_name

        # Lazy connection retrieval - don't fetch in __init__ to avoid AsyncToSync issues
        # when hook is created from async context (e.g., in triggers)
        self._airflow_connection: Optional[Connection] = None

        self.retry_limit = retry_limit
        self.retry_delay = retry_delay
        self.timeout = timeout
        self.airflow_version = airflow_version

        # Default to all 5xx server errors if not specified
        if retryable_status_codes is None:
            self.retryable_status_codes = list(range(500, 600))
        else:
            self.retryable_status_codes = retryable_status_codes

    async def get_airflow_connection_async(self) -> Connection:
        """
        Get the Airflow connection asynchronously, using a thread pool to avoid AsyncToSync issues.

        This method safely retrieves the connection even when called from within an async
        event loop by running the synchronous get_connection() in a separate thread.

        :returns: The Airflow Connection object.
        :raises AirflowException: If connection cannot be retrieved or is invalid.
        """
        if self._airflow_connection is not None:
            return self._airflow_connection

        # Use asyncio.to_thread to run the synchronous get_connection in a separate thread
        # This avoids the AsyncToSync error when called from within an async event loop
        try:
            self._airflow_connection = await sync_to_async(self.get_connection)(self.connection_id)
        except Exception as e:
            raise AirflowException(f"Failed to retrieve connection {self.connection_id}: {e}") from e

        if self._airflow_connection is None:
            raise AirflowException(f"Connection {self.connection_id} not found.")

        if not self._airflow_connection.host:
            raise AirflowException("Hevo connection must define a host (base API domain).")

        return self._airflow_connection

    def _build_api_url(self, airflow_hevo_connection: Connection, endpoint: str) -> str:
        """
        Build the full API URL from connection details and endpoint.

        :param airflow_hevo_connection: Airflow connection object.
        :param endpoint: API endpoint path relative to connection host.
        :returns: Complete URL for the API request.
        :raises AirflowException: If connection host is not defined.
        """
        schema = airflow_hevo_connection.schema or "https"
        host = airflow_hevo_connection.host.rstrip("/") if airflow_hevo_connection.host else ""
        if not host:
            raise AirflowException("Hevo connection must define a host (base API domain).")

        # Build URL
        cleaned_endpoint = (endpoint or "").lstrip("/")
        url = f"{schema}://{host}/{cleaned_endpoint}" if cleaned_endpoint else f"{schema}://{host}"
        return url

    async def execute_api_request_async(
            self,
            method: str,
            endpoint: str,
            params: dict[str, Any] | None = None,
            payload: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """
        Perform an asynchronous API request with retries.

        :param method: HTTP method (GET, POST, PUT, PATCH, DELETE, etc.).
        :param endpoint: API endpoint path relative to connection host.
        :param params: Query parameters to append to URL (for GET requests).
        :param payload: JSON body to send with request (for POST/PUT/PATCH requests).
        :returns: JSON response as dictionary, or empty dict for 204/no-content responses.
        :raises AirflowException: On request failure after retries or non-retryable errors.
        """
        # Get connection asynchronously to avoid AsyncToSync issues
        airflow_hevo_connection = await self.get_airflow_connection_async()

        # Build request URL and parameters
        url = self._build_api_url(airflow_hevo_connection, endpoint)
        auth = self._get_auth_from_connection(airflow_hevo_connection)
        headers = self._get_headers()
        timeout = aiohttp.ClientTimeout(total=self.timeout)

        async with aiohttp.ClientSession(auth=auth, headers=headers, timeout=timeout) as session:
            attempt_num = 1
            while attempt_num <= self.retry_limit:
                try:
                    response = await session.request(
                        method, url, params=params, json=payload)
                    response.raise_for_status()

                    # Check if there's content to parse
                    if response.content_length == 0 or response.status == 204:
                        return {}

                    return await response.json()

                except ClientResponseError as e:
                    last_exception = e
                    if not (e.status in self.retryable_status_codes):
                        self.log.error("Non-retryable error: %s - %s", e.status, e.message)
                        raise AirflowException(f"API request failed with status {e.status}: {e.message}") from e

                    self.log.warning(
                        "Attempt %s failed for %s %s: %s - %s", attempt_num, method, url, e.status, e.message
                    )

                # Check if we should retry
                if attempt_num >= self.retry_limit:
                    raise AirflowException(
                        f"API request to {url} failed after {self.retry_limit} attempts") from last_exception

                attempt_num += 1
                await asyncio.sleep(self.retry_delay)

    def _get_auth_from_connection(self, airflow_connection: Connection) -> aiohttp.BasicAuth | None:
        """
        Extract authentication details from the Airflow connection.

        :param airflow_connection: Airflow connection object.
        :returns: BasicAuth object if credentials are available, None otherwise.
        """
        if airflow_connection.login and airflow_connection.password:
            return aiohttp.BasicAuth(airflow_connection.login, airflow_connection.password)
        return None

    def _get_headers(self) -> dict[str, str]:
        """
        Build HTTP headers for API requests.

        :returns: Dictionary of HTTP headers.
        """
        return {
            "User-Agent": f"{self.api_user_agent}-airflow/{self.airflow_version}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
