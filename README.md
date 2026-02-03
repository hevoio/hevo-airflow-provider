## hevo-airflow-provider

Apache Airflow provider for Hevo Data's External Orchestration API. Enables triggering and monitoring Hevo pipeline syncs from Airflow DAGs with deferrable execution support.

## Quick Start

### Installation

```bash
# For production use (when published)
pip install apache-airflow-providers-hevo

# For development
git clone https://github.com/hevoio/hevo-airflow-provider.git
cd hevo-airflow-provider

# Quick setup with uv (recommended)
uv venv
source .venv/bin/activate
uv pip install -e ".[dev]"
```

**📖 For detailed setup instructions**, see **[SETUP.md](SETUP.md)** which covers:
- Development setup with UV or pip
- Production installation in existing Airflow
- Docker setup for local testing
- Troubleshooting common issues

### Basic Usage

```python
from airflow import DAG
from airflow.hevo.operators import HevoPipelineOperator
from datetime import datetime

with DAG("hevo_sync", start_date=datetime(2024, 1, 1), schedule_interval="@daily") as dag:
  sync = HevoPipelineOperator(
    task_id="sync_pipeline",
    pipeline_id=123,
    deferrable=True,
    wait_for_completion=True
  )
```

## Architecture

### Hevo Hooks

The provider exposes an async-first hook to interact with the Hevo External Orchestration API:

- **`HevoPipelineHook`** (`src/airflow/hevo/hooks/hevo_pipeline_hook.py`): unified hook with async-first architecture. All API interactions are implemented as async methods, with sync wrapper methods using `asyncio.run()` for compatibility.
  - **Async methods** (suffixed with `_async`):
    - `execute_api_request_async(method, endpoint, **kwargs) -> dict`: low-level async HTTP client with retry logic.
    - `get_pipeline_async(pipeline_id: int) -> dict | None`: fetch pipeline details asynchronously.
    - `_validate_pipeline_async(pipeline_id: int)`: validate pipeline state asynchronously.
    - `trigger_pipeline_sync_async(pipeline_id: int, ensure_new_job: bool = True)`: trigger pipeline sync asynchronously; defaults to failing if a job already exists.
    - `resync_pipeline_async(pipeline_id: int, drop_and_load: bool = False)`: trigger full historical resync asynchronously.
    - `find_active_job_by_type_async(pipeline_id: int, job_type: str = "INCREMENTAL") -> dict`: find active jobs asynchronously.
    - `get_job_completion_status_async(pipeline_id: int, job_id: str, accept_completed_with_failures: bool = False) -> str`: check job status asynchronously.

  - **Sync wrapper methods** (use `asyncio.run()` internally):
    - `get_pipeline(pipeline_id: int) -> dict | None`: fetch pipeline details, returning `None` if not found (404).
    - `validate_pipeline(pipeline_id: int)`: ensures the pipeline exists and is in an active (`INITIALIZED`) state.
    - `trigger_pipeline_sync(pipeline_id: int, ensure_new_job: bool = True)`: trigger a sync on the pipeline; by default, raises if a job is already in progress (set to `False` to allow concurrent jobs).
    - `resync_pipeline_sync(pipeline_id: int, drop_and_load: bool = False)`: trigger full historical resync; drops and recreates tables if `drop_and_load=True`.
    - `find_active_job_by_type(pipeline_id: int, job_type: str = "INCREMENTAL") -> dict`: returns the currently active job of the given type or raises if none is found.
    - `get_job_completion_status(pipeline_id: int, job_id: str, accept_completed_with_failures: bool = False) -> str`: returns one of `"completed"`, `"completed_with_failures"`, `"failed"`, or `"pending"`.

The hook relies on an Airflow connection of type `hevo_connection` (default ID: `hevo_airflow_conn_id`), and honours `retry_limit`, `retry_delay`, `timeout`, `retryable_status_codes`, and optional `extra_headers` provided at initialization.

**Configurable Retries**: Control which HTTP status codes trigger retries via the `retryable_status_codes` parameter (defaults to all 5xx errors: 500-599). Examples:
- `[500, 502, 503, 504]` - Only retry specific server errors
- `[429, 500, 502, 503]` - Include rate limiting (429) in retries
- `[]` - Disable status code-based retries (network errors only)

**Architecture**: The async-first design eliminates code duplication - all API logic is implemented once in async methods, with sync wrapper methods using `asyncio.run()` to call the async implementations. This provides a single source of truth while maintaining backward compatibility.

### Operators

#### HevoOperator

The `HevoOperator` triggers Hevo pipeline syncs with three execution modes:

**1. Deferrable Mode (Recommended for Production)**

```python
from airflow.hevo.operators import HevoPipelineOperator

sync_task = HevoPipelineOperator(
  task_id="sync_pipeline",
  pipeline_id=123,
  deferrable=True,  # Releases worker slot
  wait_for_completion=True,  # Waits for job to complete
  poll_interval=10,  # Check status every 10s
  ensure_new_job=True,  # Default: fails if job already running
  accept_completed_with_failures=False
)
```

**2. Fire-and-Forget Mode**
```python
trigger = HevoOperator(
    task_id="trigger_sync",
    pipeline_id=123,
    wait_for_completion=False    # Returns job_id via XCom immediately
)
```

**3. Synchronous Wait Mode**
```python
sync_task = HevoOperator(
    task_id="sync_pipeline",
    pipeline_id=123,
    deferrable=False,            # Blocks worker slot
    wait_for_completion=True
)
```

**Key Parameters:**
- `pipeline_id` (int, required): Hevo pipeline ID to sync or resync
- `action` (PipelineAction, default: `SYNC_NOW`): Pipeline action - `SYNC_NOW` for regular sync or `RESYNC` for full historical reload
- `deferrable` (bool, default: `True`): Use deferrable execution to release worker slot
- `wait_for_completion` (bool, default: `True`): Wait for job to complete before returning
- `ensure_new_job` (bool, default: `True`): Fail if job already in progress for pipeline (prevents duplicate jobs by default) - applies to SYNC_NOW only
- `accept_completed_with_failures` (bool, default: `False`): Treat partial failures as success
- `job_type` (JobType, intelligent default): Type of job to wait for - defaults to `INCREMENTAL` for SYNC_NOW, `TRUNCATE_AND_LOAD` for RESYNC
- `drop_and_load` (bool, default: `False`): Drop and recreate destination tables before loading (RESYNC action only)
- `poll_interval` (int, default: `15`): Seconds between status checks
- `retry_limit` (int, default: `10`): Maximum attempts to find active job after triggering

**Features:**
- ✅ **Automatic retry logic** for transient API failures (3 consecutive retries in synchronous mode)
- ✅ **Duplicate job prevention** with `ensure_new_job=True`
- ✅ **Resource-efficient deferrable execution** (releases worker slot while waiting)
- ✅ **Job discovery with retry** (waits up to 10 attempts for job to appear after triggering)

### Sensors

#### HevoSensor

The `HevoSensor` monitors Hevo pipeline job completion with auto-discovery support:

**With Explicit Job ID (from XCom)**
```python
from airflow.hevo.sensor import HevoSensor

wait_task = HevoSensor(
    task_id="wait_for_completion",
    pipeline_id=123,
    job_id="{{ ti.xcom_pull(task_ids='trigger_sync') }}",  # From previous task
    deferrable=True,
    poke_interval=10,
    accept_completed_with_failures=False
)
```

**With Auto-Discovery**
```python
wait_task = HevoSensor(
    task_id="wait_for_completion",
    pipeline_id=123,
    job_type=JobType.INCREMENTAL,     # No job_id - auto-discovers active job
    wait_for_job_initial_delay=10,    # Initial delay before first check
    wait_for_job_max_attempts=10,     # Max attempts to find job
    wait_for_job_interval=5,          # Seconds between discovery attempts
    deferrable=True
)
```

**Key Parameters:**
- `pipeline_id` (int, required): Hevo pipeline ID to monitor
- `job_id` (str, optional): Specific job ID to monitor (supports Jinja templating)
- `job_type` (JobType, default: `INCREMENTAL`): Job type for auto-discovery (when job_id not provided)
- `deferrable` (bool, default: `True`): Use deferrable mode
- `poke_interval` (int, default: `15`): Seconds between status checks
- `wait_for_job_initial_delay` (int, default: `10`): Initial delay before first discovery attempt
- `wait_for_job_max_attempts` (int, default: `10`): Max attempts to find active job
- `wait_for_job_interval` (int, default: `5`): Seconds between discovery attempts
- `accept_completed_with_failures` (bool, default: `False`): Treat partial failures as success

**Auto-Discovery Behavior:**
When `job_id` is not provided, the sensor automatically discovers the active job matching the specified `job_type`. This is useful when monitoring jobs triggered externally or when the job_id is not available.

### Triggers

#### HevoTrigger

The `HevoTrigger` is used internally by deferrable operators and sensors for async monitoring. Users typically don't instantiate triggers directly - they're created automatically when using `deferrable=True`.

**Key Features:**
- Runs in Airflow's triggerer service (separate process)
- Uses async HTTP requests via `aiohttp` for efficiency
- Yields `TriggerEvent` when job reaches terminal state
- Automatically serialized/deserialized for persistence

### Configuration

#### Airflow Connection

Create an Airflow connection for Hevo API credentials:

```bash
# Via Airflow UI or CLI
Connection ID: hevo_default (or custom via connection_id parameter)
Connection Type: HTTP
Host: us.hevodata.com (or your region: eu.hevodata.com, in.hevodata.com)
Schema: https
Login: <your_api_username>
Password: <your_api_key>
```

### Example DAGs

#### Pattern 1: Deferrable Operator (Recommended)

```python
from airflow import DAG
from airflow.hevo.operators import HevoPipelineOperator
from datetime import datetime, timedelta

with DAG(
        "hevo_deferrable_sync",
        start_date=datetime(2024, 1, 1),
        schedule_interval="@daily",
        catchup=False
) as dag:
  sync_pipeline = HevoPipelineOperator(
    task_id="sync_pipeline",
    pipeline_id=123,
    deferrable=True,
    wait_for_completion=True,
    poll_interval=10,
    ensure_new_job=True
  )
```

#### Pattern 2: Fire-and-Forget + Sensor

```python
from airflow import DAG
from airflow.hevo.operators import HevoPipelineOperator
from airflow.hevo.sensor import HevoSensor
from datetime import datetime

with DAG(
        "hevo_trigger_and_wait",
        start_date=datetime(2024, 1, 1),
        catchup=False
) as dag:
  # Task 1: Trigger sync without waiting
  trigger = HevoPipelineOperator(
    task_id="trigger_sync",
    pipeline_id=123,
    wait_for_completion=False,  # Returns job_id via XCom
    ensure_new_job=True
  )

  # Task 2: Wait for completion using sensor
  wait = HevoSensor(
    task_id="wait_for_completion",
    pipeline_id=123,
    job_id="{{ ti.xcom_pull(task_ids='trigger_sync') }}",
    deferrable=True,
    poke_interval=15,
    timeout=7200  # 2 hour timeout
  )

  trigger >> wait
```

#### Pattern 3: Auto-Discovery with Historical Load

```python
from airflow.hevo.sensor import HevoSensor
from airflow.hevo.models.job import JobType

wait_historical = HevoSensor(
    task_id="wait_historical_load",
    pipeline_id=456,
    job_type=JobType.HISTORICAL,           # Monitor historical load
    wait_for_job_initial_delay=30,         # Historical jobs may take longer
    wait_for_job_max_attempts=20,
    deferrable=True
)
```

### Advanced Features

#### 1. Preventing Duplicate Jobs

The operator **defaults to preventing duplicate jobs** with `ensure_new_job=True`:

```python
HevoOperator(
    task_id="sync_pipeline",
    pipeline_id=123,
    ensure_new_job=True  # Default: True - fails if job already exists
)
```

This is useful when:
- DAG is manually triggered multiple times
- External systems trigger the same pipeline concurrently
- Strict job isolation is required

To **allow** concurrent jobs, set `ensure_new_job=False`:

```python
HevoOperator(
    task_id="sync_pipeline",
    pipeline_id=123,
    ensure_new_job=False  # Allows triggering even if job exists
)
```

#### 2. Accepting Partial Failures

Set `accept_completed_with_failures=True` to treat jobs with some failed records as successful:

```python
HevoOperator(
    task_id="sync_pipeline",
    pipeline_id=123,
    accept_completed_with_failures=True  # Don't fail on partial failures
)
```

#### 3. Retry Configuration

Configure retries at multiple levels:

```python
# Hook-level (API retries)
from airflow.hevo.hooks import HevoPipelineHook

hook = HevoPipelineHook(
    retry_limit=5,
    retry_delay=3,
    retryable_status_codes=[429, 500, 502, 503, 504]  # Include rate limiting
)

# Operator-level (Airflow task retries)
HevoOperator(
    task_id="sync_pipeline",
    pipeline_id=123,
    retries=3,                           # Airflow task retries
    retry_delay=timedelta(minutes=5)
)
```

**Built-in Retry Logic:**
- Synchronous wait mode: Automatically retries up to 3 consecutive API failures
- Job discovery: Retries up to 10 times (configurable via `retry_limit`) to find active job after triggering
- Network errors: Always retried regardless of status code configuration

### Testing

```bash
# Run all tests
uv run pytest

# Run with coverage
uv run pytest --cov --cov-report=html

# Run specific test
uv run pytest tests/operators/test_hevo_operator.py::test_operator_execute

# Run all checks (lint, typecheck, tests)
uv run ruff check && uv run mypy src && uv run pytest
```

**📖 For detailed testing and development setup**, see **[SETUP.md](SETUP.md)**.

### Documentation

- **[SETUP.md](SETUP.md)**: Setup and installation guide (development, production, Docker)
- **[CLAUDE.md](CLAUDE.md)**: Development guide and architecture details
- **[CONFIGURATION_PARAMETERS.md](CONFIGURATION_PARAMETERS.md)**: Comprehensive parameter reference
- **[dags/](dag_examples/)**: Example DAGs for various use cases

### Requirements

- Python: >=3.9
- Airflow: >=2.4.0 (requires deferrable support)
- Tested on: Python 3.9, 3.10, 3.11, 3.12, 3.13

### Key Dependencies

- `apache-airflow>=2.4.0` - Core Airflow
- `aiohttp==3.13.2` - Async HTTP client
- `requests==2.32.5` - Sync HTTP client
- `pydantic>=2.0.0` - Data validation

### Links

- **Hevo API Documentation**: https://hevo-edge.readme.io/reference
