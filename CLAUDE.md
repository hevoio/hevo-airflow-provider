# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Apache Airflow provider for Hevo Data's External Orchestration API. Enables triggering and monitoring Hevo pipeline syncs from Airflow DAGs.

**API Documentation**: https://hevo-edge.readme.io/reference

## Development Commands

### Setup

```bash
# Quick setup with uv (recommended)
uv venv
source .venv/bin/activate
uv pip install -e ".[dev]"

# Alternative with pip
python3.9 -m venv venv
source venv/bin/activate
pip install -e ".[dev]"
```

### Testing & Quality

```bash
uv run pytest                 # Run pytest
uv run pytest tests/test_specific.py::test_name  # Single test

uv run ruff check             # Run ruff linter
uv run ruff check --fix       # Auto-fix lint issues
uv run ruff format            # Format code with ruff
uv run mypy src               # Run mypy type checker
```

### Build & Clean

```bash
python -m build               # Build distribution packages
rm -rf build/ dist/ *.egg-info .pytest_cache .mypy_cache .ruff_cache  # Remove build artifacts and caches
rm -rf .venv                  # Remove virtual environment
```

### Running Examples

```bash
# Available examples in dag_examples/ directory
# Copy examples to your Airflow DAGs folder and run via Airflow UI
cp dag_examples/*.py ~/airflow/dags/
```

## Architecture

### Component Hierarchy

```
DAG Layer (User-facing)
    ↓
Operators/Sensors (HevoOperator, HevoSensor)
    ↓
Hook (HevoPipelineHook - async-first with sync wrappers)
    ↓
Triggers (HevoTrigger - async execution)
    ↓
API (Hevo REST API via aiohttp)
```

### Key Components

**`src/airflow/hevo/hooks/`**
- `base.py`: `BaseHevoHook` - Foundation for all API interactions (connection mgmt, error handling)
  - `get_airflow_connection_async()` - Retrieves and caches Airflow connection
  - `execute_api_request_async()` - Core async HTTP request executor with retry logic
- `hevo_pipeline_hook.py`:
  - `HevoPipelineHook` - Unified hook with async-first architecture for pipeline operations
    - Async methods (`*_async`) - All API operations using `aiohttp`
    - Sync wrapper methods - Use `asyncio.run()` to call async methods
- `hevo_object_hook.py`:
  - `HevoObjectHook` - Hook for pipeline object operations (tables/collections)
    - `list_objects_async()` / `list_objects_sync()` - List all objects in a pipeline
    - `get_object_async()` / `get_object_sync()` - Get specific object details including fields
    - `refresh_schema_async()` / `refresh_schema_sync()` - Trigger schema refresh
    - `resync_objects_async()` / `resync_objects_sync()` - Trigger object-level resync

**`src/airflow/hevo/operators/`**
- `hevo_operator.py`: `HevoPipelineOperator` - Trigger pipeline syncs or resyncs with:
  - **Two pipeline actions**:
    1. `SYNC_NOW` (default): Regular incremental sync via `POST /pipelines/{id}/actions/sync-now`
    2. `RESYNC`: Full historical resync via `POST /pipelines/{id}/actions/resync`
  - **Three execution modes**:
    1. Fire-and-forget (`wait_for_completion=False`)
    2. Synchronous wait (`deferrable=False, wait_for_completion=True`)
    3. Deferrable wait (`deferrable=True, wait_for_completion=True`) - **recommended**

**`src/airflow/hevo/sensors/`**
- `hevo_sensor.py`: `HevoSensor` - Monitor job completion, supports auto-discovery of active jobs

**`src/airflow/hevo/triggers/`**
- `hevo_trigger.py`: `HevoTrigger` - Async polling for deferrable operators/sensors

### Critical Architectural Patterns

#### 1. Async-First Architecture

The codebase uses an async-first design with sync wrappers:

**Core Implementation:**
- All API interactions implemented as async methods in `HevoPipelineHook`
- Uses `aiohttp` for HTTP requests
- Single source of truth - no code duplication

**Synchronous Path (non-deferrable):**
- `HevoOperator`/`HevoSensor` → `HevoPipelineHook.method()` → `asyncio.run(method_async())`
- Sync methods use `asyncio.run()` to call async methods internally
- Blocks worker slot during polling
- Uses `time.sleep()` for delays
- Simple but inefficient for long-running jobs

**Asynchronous Path (Deferrable):**
- `HevoOperator`/`HevoSensor` → `HevoTrigger` → `HevoPipelineHook.method_async()` → `aiohttp`
- Direct async method calls, no `asyncio.run()` wrapper
- Releases worker slot via `self.defer()`
- Triggerer process handles async polling with `asyncio.sleep()`
- Enables thousands of concurrent waiting tasks

**When to use deferrable:**
- Default to `deferrable=True` for production
- Only use `deferrable=False` for very short jobs (<5 min) or testing

#### 2. Pipeline Actions

`HevoPipelineOperator` supports two types of pipeline actions via the `action` parameter:

**SYNC_NOW (default)**:
```python
HevoPipelineOperator(
    task_id="sync_pipeline",
    connection_id="hevo_airflow_conn_id",
    pipeline_id=123,
    action=PipelineAction.SYNC_NOW  # Default - can be omitted
)
```
- Triggers regular incremental sync: `POST /api/v1/pipelines/{id}/actions/sync-now`
- Validates pipeline is in `INITIALIZED` state before triggering
- Honors `ensure_new_job` parameter (default: True)
- **Default job type**: `INCREMENTAL`
- **Use for**: Regular scheduled syncs, incremental data updates

**RESYNC**:
```python
HevoPipelineOperator(
    task_id="resync_pipeline",
    connection_id="hevo_airflow_conn_id",
    pipeline_id=123,
    action=PipelineAction.RESYNC  # Full historical reload
)
```
- Triggers full historical resync: `POST /api/v1/pipelines/{id}/actions/resync`
- **Waits for INITIALIZED state** - polls pipeline status with infinite retries until INITIALIZED
- Uses `poll_interval` parameter for wait time between status checks (default: 15 seconds)
- **No retry limit** - will wait indefinitely until pipeline reaches INITIALIZED status
- Re-ingests all data from the source (complete historical reload)
- Ignores `ensure_new_job` parameter
- **Default job type**:
  - `RESYNC_WITH_EVOLVE` when `drop_and_load=False` (default)
  - `RESYNC_WITH_DROP_AND_LOAD` when `drop_and_load=True`
- **Optional parameter**: `drop_and_load` (default: False) to drop data and load them to destination tables
- **Use for**:
  - Reprocessing data after schema changes
  - Recovering from data corruption
  - Applying new transformations to historical data
  - Migrating to a new destination with full data reload

**Key Differences**:
| Feature | SYNC_NOW | RESYNC |
|---------|----------|--------|
| Validation | Required (INITIALIZED state) | Waits for INITIALIZED (infinite retries) |
| Validation Behavior | Immediate check, fails if not INITIALIZED | Polls until INITIALIZED, never fails |
| Poll Interval | N/A | Uses `poll_interval` parameter |
| Retry Limit | N/A | No limit (waits indefinitely) |
| Data Scope | Incremental updates | Full historical reload |
| Duration | Minutes | Hours (depends on data volume) |
| API Endpoint | `/actions/sync-now` | `/actions/resync` |

#### 3. Three Execution Modes

```python
# Mode 1: Fire-and-forget (returns job_id via XCom)
HevoOperator(wait_for_completion=False)
# → Use with HevoSensor for decoupled monitoring

# Mode 2: Synchronous wait (blocks worker)
HevoOperator(deferrable=False, wait_for_completion=True)
# → Simple but inefficient

# Mode 3: Deferrable wait (recommended)
HevoOperator(deferrable=True, wait_for_completion=True)
# → Resource-efficient, requires triggerer service
```

#### 4. Job Discovery Strategy

Both operator and sensor wait for jobs to "appear" after triggering:

```python
# In operator.execute() and sensor._get_job_id():
active_job = None
for attempt in range(retry_limit):  # Max retry_limit attempts (default: 10)
    try:
        active_job = hook.find_active_job_by_type_sync(pipeline_id, job_type)
        if active_job is not None:
            break  # Found the job!
    except AirflowException as e:
        # Job not found yet - hook raises exception when no active jobs
        # Continue retrying - job may not have appeared yet
        pass

    sleep(poll_interval)  # Wait before next attempt (default: 5s)

if active_job is None:
    raise AirflowException(f"No active job found after {retry_limit} attempts")
```

**Rationale**: Small delay between sync-now API call and job appearing in jobs list.

**Exception Handling**: `find_active_job_by_type()` raises `AirflowException` when no active jobs are found. The operator/sensor catches this exception and retries, treating it as "job not ready yet" rather than a fatal error.

#### 5. State Abstraction

Hooks abstract API job states into four canonical states:

```python
def get_job_completion_status(...) -> str:
    # API states: COMPLETED, COMPLETED_WITH_FAILURES, FAILED,
    #             IN_PROGRESS, CANCELLED, SKIPPED, etc.

    return "completed"                 # Success
         | "completed_with_failures"   # Partial success
         | "failed"                    # Failure
         | "pending"                   # Still running
```

This shields operators/sensors from API complexity.

#### 6. XCom Communication Pattern

Fire-and-forget mode uses XCom for inter-task communication:

```python
# Task 1: Trigger without waiting
trigger = HevoOperator(
    task_id="trigger",
    wait_for_completion=False  # Returns job_id via XCom
)

# Task 2: Monitor using sensor with templated job_id
sensor = HevoSensor(
    task_id="wait",
    job_id="{{ ti.xcom_pull(task_ids='trigger') }}"  # Template pulls job_id
)

trigger >> sensor
```

#### 7. Trigger Serialization

Triggers must be serializable for persistence across Airflow restarts:

```python
def serialize(self) -> tuple[str, dict]:
    return (
        "airflow.hevo.triggers.HevoTrigger",
        {
            "pipeline_id": self.pipeline_id,
            "job_id": self.job_id,
            # ... other params
        }
    )
```

Only include JSON-serializable types in trigger parameters.

### API Interaction Patterns

#### Core Endpoints

```
GET  /api/v1/pipelines/{id}
     → Validate pipeline exists and is INITIALIZED

POST /api/v1/pipelines/{id}/actions/sync-now
     → Trigger new sync job

GET  /api/v1/pipelines/{id}/jobs
     → List jobs (paginated, cursor-based)

GET  /api/v1/pipelines/{id}/jobs/{job_id}
     → Get job status
```

#### Request Flow

All API requests follow consistent pattern in `BaseHevoHook.execute_api_request_async()`:

1. Build URL from connection host + endpoint
2. Add authentication tuple (login/password) from Airflow connection
3. Build headers:
   - `User-Agent`: `hevo_airflow_provider-airflow/{version}`
   - `Content-Type`: `application/json`
   - `Accept`: `application/json`
   - Plus any connection extras or overrides from `extra_headers`
4. Add JSON body if provided (for POST/PUT/PATCH requests)
5. Execute with timeout (default 30s) using `aiohttp`
6. Retry on network errors or 5xx (up to 3 times, 2s delay)
7. Non-retryable: 4xx errors (client errors)
8. Return JSON response or empty dict on 204/no-content

**Note**: Sync methods wrap async methods using `asyncio.run()`, so the actual HTTP request always goes through the async implementation.

#### Error Handling Layers

**1. HTTP Request Retry** (in hooks):
- Network errors: always retry (connection failures, timeouts)
- Configurable status codes: retry based on `retryable_status_codes` parameter (default: 500-599)
- Non-retryable: 4xx errors by default (client errors)
- Default: 3 attempts, 2-second delay
- Customizable: Pass `retryable_status_codes=[429, 500, 502, 503]` to include rate limiting

**2. Job Discovery Retry** (in operator/sensor):
- 10s initial delay + 10 attempts every 5s
- Waits for job to appear in API after sync trigger

**3. Status Polling Retry** (in operator/sensor/trigger):
- Continuous polling until terminal state
- Respects `poll_interval` between checks

### Connection Configuration

Create Airflow connection with ID `hevo_airflow_conn_id` (or custom):

```python
# Required fields
Host: us.hevodata.com       # Or your Hevo region
Schema: https
Login: <your_api_username>
Password: <your_api_key>

```

Connection is resolved via `BaseHevoHook.connection` cached property.

## Code Patterns & Conventions

### Import Order

```python
from __future__ import annotations

# Standard library
from time import sleep
from typing import TYPE_CHECKING

# Third-party
import requests

# Airflow
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator

# Local
from airflow.hevo.hooks import HevoPipelineHook
```

### Type Hints

Use PEP 585 syntax: `list`, `dict`, not `List`, `Dict`

```python
def method(items: list[str]) -> dict[str, Any]:
    ...
```

### Logging

Use `self.log` in operators/sensors/hooks:

```python
self.log.info("Starting sync for pipeline %s", pipeline_id)
self.log.debug("Polling attempt %s", attempt)
self.log.error("Job %s failed: %s", job_id, error)
```

### Docstrings

PEP 257 style with Sphinx-compatible parameter descriptions:

```python
def method(param: str, flag: bool = False) -> str:
    """
    Brief one-line description.

    Longer description if needed, explaining behavior,
    use cases, and important details.

    :param param: Parameter description
    :param flag: Flag description (default: False)
    :returns: Return value description
    :raises AirflowException: When it raises and why
    """

```

### Template Fields

Declare template fields for Airflow variable/XCom substitution:

```python
class HevoOperator(BaseOperator):
    template_fields = ("pipeline_id",)

    # Enables:
    # pipeline_id="{{ var.value.pipeline_id }}"
```

## Testing Patterns

### Unit Tests

Test individual methods in isolation:

```python
def test_job_status_mapping(mock_hook):
    # Test status abstraction logic
    assert hook.get_job_completion_status_sync(...) == "completed"
```

### Integration Tests

Use Airflow's test utilities:

```python
from airflow.utils.state import DagRunState
from airflow.models import DagBag

def test_operator_execution():
    dagbag = DagBag(dag_folder="dags/", include_examples=False)
    dag = dagbag.get_dag("hevo_triggerer_example")
    # Test DAG execution
```

### Manual Testing

Use example DAGs in `dags/` directory:
- `triggerer_example_dag.py` - Deferrable operator
- `sync_sensor_wait_example_dag.py` - Operator + Sensor
- `sync_synchronous_wait_example_dag.py` - Synchronous wait
- `sync_no_wait_example_dag.py` - Fire-and-forget

## Common Implementation Scenarios

### Adding New API Endpoint Support

Follow the async-first pattern:

1. Add async method to `HevoPipelineHook` (suffixed with `_async`)
2. Use `execute_api_request_async()` for HTTP requests
3. Handle response and map to appropriate return type
4. Add sync wrapper method using `asyncio.run()`

Example:

```python
# In HevoPipelineHook

# 1. Async implementation
async def get_pipeline_objects_async(self, pipeline_id: int) -> list[dict]:
    """Fetch all objects for a pipeline (async)."""
    endpoint = f"/api/v1/pipelines/{pipeline_id}/objects"
    response = await self.execute_api_request_async("GET", endpoint)
    return response.get("data", [])


# 2. Sync wrapper
def get_pipeline_objects(self, pipeline_id: int) -> list[dict]:
    """Fetch all objects for a pipeline (sync wrapper)."""
    return asyncio.run(self.get_pipeline_objects_async(pipeline_id))


# Example with JSON body (for POST/PUT/PATCH)
async def update_pipeline_config_async(self, pipeline_id: int, config: dict) -> dict:
    """Update pipeline configuration (async)."""
    endpoint = f"/api/v1/pipelines/{pipeline_id}/config"
    response = await self.execute_api_request_async(
        method="PATCH",
        endpoint=endpoint,
        payload=config  # JSON body for request
    )
    return response


def update_pipeline_config(self, pipeline_id: int, config: dict) -> dict:
    """Update pipeline configuration (sync wrapper)."""
    return asyncio.run(self.update_pipeline_config_async(pipeline_id, config))
```

This approach maintains a single source of truth while supporting both sync and async usage.

### Creating New Operator

1. Inherit from `BaseOperator`
2. Declare `template_fields` for dynamic values
3. Implement `execute(context)` method
5. For deferrable: implement `execute_complete(context, event)`

### Creating New Sensor

1. Inherit from `BaseSensorOperator`
2. Implement `poke(context) -> bool`
3. For deferrable: override `execute()` to call `self.defer()`
4. Implement `execute_complete(context, event)`

### Using Async Methods Directly

When working in async contexts (like triggers), use async methods directly:

```python
# In trigger's run() method
hook = HevoPipelineHook(connection_id="hevo_default")

# Call async methods directly (no asyncio.run needed)
pipeline = await hook.get_pipeline_async(pipeline_id)
status = await hook.get_job_completion_status_async(pipeline_id, job_id)
```

Async methods:
- Use `aiohttp` for HTTP requests internally
- Use `await asyncio.sleep()` for delays
- Handle `ClientResponseError` exceptions
- Return JSON or raise `AirflowException`

## Debugging Tips

### Connection Issues

```bash
# Test connection in Airflow
airflow connections get hevo_airflow_conn_id

# Check connection in Python
from airflow.hooks.base import BaseHook
conn = BaseHook.get_connection("hevo_airflow_conn_id")
print(f"Host: {conn.host}, Login: {conn.login}")
```

### API Request Debugging

Enable debug logging in hooks:

```python
self.log.setLevel("DEBUG")
self.log.debug("Request URL: %s", url)
self.log.debug("Request headers: %s", headers)
```

### Job Discovery Failures

Common causes:
- Pipeline not in `INITIALIZED` state
- Wrong `job_type` (INCREMENTAL vs FULL vs other)
- Job hasn't appeared yet (increase `wait_for_job_max_attempts`)

### Triggerer Not Running

Deferrable mode requires triggerer service:

```bash
# Check if triggerer is running
airflow triggerer

# In production: systemd/supervisor service
# Dev: run in separate terminal
```

## File Organization

```
src/airflow/hevo/
├── __init__.py
├── hooks/
│   ├── __init__.py
│   ├── base.py                   # BaseHevoHook (foundation)
│   ├── hevo_pipeline_hook.py     # HevoPipelineHook (async-first with sync wrappers)
│   └── hevo_object_hook.py       # HevoObjectHook (pipeline object operations)
├── models/
│   ├── __init__.py
│   ├── common.py                 # Common enums (FieldStatusEnum)
│   ├── job.py                    # Job models (JobType, JobStatus, Job)
│   ├── object.py                 # Object models (Object, Field, Namespace)
│   └── pipeline.py               # Pipeline models (PipelineAction, PipelineStatus, Pipeline)
├── operators.py                  # HevoPipelineOperator
├── sensor.py                     # HevoSensor
└── trigger.py                    # HevoTrigger

dag_examples/                     # Example DAGs for testing
├── triggerer_example_dag.py      # Deferrable operator example
├── sync_sensor_wait_example_dag.py    # Operator + Sensor pattern
├── sync_synchronous_wait_example_dag.py  # Synchronous wait example
├── sync_no_wait_example_dag.py   # Fire-and-forget example
├── resync_example_dag.py         # RESYNC action example
└── dbt_example_dag.py            # DBT integration example

bin/                              # Setup and utility scripts
├── add-git-precommit-hook.sh     # Install pre-commit hooks
└── setup-uv.sh                   # UV package manager setup

tests/                            # Test suite
├── hooks/                        # Hook tests
├── operators/                    # Operator tests
├── sensors/                      # Sensor tests
├── triggers/                     # Trigger tests
└── models/                       # Model tests

docker/                           # Docker setup for local testing
```

## Key Dependencies

- `apache-airflow>=2.4.0` - Core Airflow (requires deferrable support)
- `aiohttp==3.13.2` - Async HTTP client for triggers
- `requests==2.32.5` - Sync HTTP client for hooks
- `ruff==0.13.1` - Linting and formatting
- `mypy==1.18.2` - Type checking
- `pytest==8.4.2` - Testing

## Important Notes

### Pipeline State Validation

**SYNC_NOW Action:**
Validates pipeline is in `INITIALIZED` state (single check, fails immediately if not):

```python
hook.validate_pipeline(pipeline_id)
# Raises AirflowException if not INITIALIZED
```

**RESYNC Action:**
Automatically waits for pipeline to reach `INITIALIZED` state with infinite retries:

```python
# No manual validation needed - operator handles it automatically
HevoPipelineOperator(
    action=PipelineAction.RESYNC,
    poll_interval=15,  # Seconds between INITIALIZED status checks
    # Will poll indefinitely until pipeline is INITIALIZED
)
```

The operator will:
1. Check pipeline status
2. If not INITIALIZED, wait `poll_interval` seconds
3. Retry indefinitely until INITIALIZED
4. Then trigger the resync

### Job Type Consistency

Ensure `job_type` matches pipeline configuration:
- `INCREMENTAL` - Most common, default
- `FULL` - Full historical load
- Others as defined by Hevo API

### Accept Completed with Failures

Set `accept_completed_with_failures=True` to treat partial failures as success:

```python
HevoOperator(
    pipeline_id=123,
    accept_completed_with_failures=True  # Don't fail on some record failures
)
```

### Ensuring New Jobs

Set `ensure_new_job=True` to fail if a job is already running for the pipeline:

```python
HevoOperator(
    pipeline_id=123,
    ensure_new_job=True  # Fail if job already exists, prevent duplicate triggers
)
```

This is useful when you want to prevent triggering duplicate jobs while a previous job is still running. When enabled, the operator checks for existing active jobs before triggering a new sync and raises an exception if one is found.

### Retry Configuration

Configure retries at multiple levels:

```python
# API-level retries (hook)
HevoPipelineHook(
    retry_limit=5,
    retry_delay=3,
    retryable_status_codes=[429, 500, 502, 503, 504]  # Include rate limiting
)

# Or use default 5xx only
HevoPipelineHook(retry_limit=5, retry_delay=3)  # Defaults to [500-599]

# Disable status code retries (network errors only)
HevoPipelineHook(retry_limit=5, retry_delay=3, retryable_status_codes=[])

# Airflow task retries (operator)
HevoOperator(
    task_id="sync",
    retries=2,
    retry_delay=timedelta(minutes=5)
)
```

**Note**: The `retryable_status_codes` parameter controls which HTTP status codes trigger automatic retries. By default, all 5xx server errors (500-599) are retryable. Network errors (connection failures, timeouts) are always retried regardless of this setting.

### Resource Efficiency

For production with many concurrent pipelines:
- Always use `deferrable=True`
- Configure appropriate `poll_interval` (default 5s)
- Consider using sensors for long-running background jobs

## Version Compatibility

- Python: >=3.9
- Airflow: >=2.4.0 (requires deferrable support)
- Tested on: Python 3.9, 3.10, 3.11, 3.12, 3.13
