# Hevo Airflow Provider - Configuration Parameters Reference

This document provides a comprehensive reference of all configurable parameters available in the Hevo Airflow Provider.

---

## Table of Contents

1. [HevoOperator Parameters](#hevooperator-parameters)
2. [HevoSensor Parameters](#hevosensor-parameters)
3. [HevoTrigger Parameters](#hevotrigger-parameters)
4. [Hook Parameters (BaseHevoHook)](#hook-parameters-basehevohook)
5. [Parameter Quick Reference Table](#parameter-quick-reference-table)
6. [Common Configuration Patterns](#common-configuration-patterns)
7. [External Hevo APIs Used](#external-hevo-apis-used)

---

## HevoOperator Parameters

The `HevoOperator` triggers and optionally waits for Hevo pipeline syncs or resyncs.

### Required Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `pipeline_id` | `int` | The Hevo pipeline ID to sync or resync. Supports Jinja templating. |

### Optional Parameters

| Parameter | Type | Default                                                                                                                                                                       | Description                                                                                                                                                                                                                                                              |
|-----------|------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `action` | `PipelineAction` | `PipelineAction.SYNC_NOW`                                                                                                                                                     | Pipeline action to trigger:<br>- `SYNC_NOW`: Regular incremental sync (POST `/pipelines/{id}/actions/sync-now`). Requires pipeline in INITIALIZED state.<br>- `RESYNC`: Full historical resync (POST `/pipelines/{id}/actions/resync`). Re-ingests all data from source. |
| `job_type` | `JobType` or `str` | **Intelligent default**<br>`INCREMENTAL` (SYNC_NOW)<br>`RESYNC_WITH_EVOLVE` (RESYNC with resync_mode=EVOLVE_AND_MERGE)<br>`RESYNC_WITH_DROP_AND_LOAD` (RESYNC with resync_mode=DROP_AND_LOAD) | Type of job to wait for when discovering the active job after triggering. Defaults intelligently based on action and resync_mode parameter. Can be explicitly set to `INCREMENTAL`, `HISTORICAL`, `RESYNC_WITH_EVOLVE`, or `RESYNC_WITH_DROP_AND_LOAD`.                |
| `connection_id` | `str` | `hevo_airflow_conn_id` (uses default)                                                                                                                                         | Airflow connection ID for Hevo API credentials. If not provided, uses `hevo_airflow_conn_id`.                                                                                                                                                                            |
| `poll_interval` | `int` | `15`                                                                                                                                                                          | Seconds between status checks when waiting for completion.                                                                                                                                                                                                               |
| `retry_limit` | `int` | `10`                                                                                                                                                                          | Maximum number of attempts to find the active job after triggering sync.                                                                                                                                                                                                 |
| `deferrable` | `bool` | `True`                                                                                                                                                                        | Use deferrable execution to release worker slot while waiting. Requires Airflow triggerer service to be running. **Recommended for production.**                                                                                                                         |
| `wait_for_completion` | `bool` | `True`                                                                                                                                                                        | Wait for the job to complete before returning. If `False`, returns `job_id` immediately via XCom for use with `HevoSensor`.                                                                                                                                              |
| `accept_completed_with_failures` | `bool` | `False`                                                                                                                                                                       | Treat `COMPLETED_WITH_FAILURES` status as success. Useful when partial failures (e.g., some records failing) are acceptable.                                                                                                                                             |
| `ensure_new_job` | `bool` | `True`                                                                                                                                                                        | If `True`, fails if a job is already in progress for the pipeline (default behavior). If `False`, proceeds normally even if a job already exists. Useful to prevent triggering duplicate jobs when previous jobs are still running. **Only applies to SYNC_NOW action.** |
| `resync_mode` | `ResyncMode` | `ResyncMode.EVOLVE_AND_MERGE`                                                                                                                                                 | Controls how destination tables are handled during resync. **Only applies to RESYNC action.**<br>- `EVOLVE_AND_MERGE`: Triggers historical resync without dropping destination tables (default).<br>- `DROP_AND_LOAD`: Drops existing destination tables before loading. Ensures a clean slate by recreating tables from scratch. |

### Pipeline Actions

The operator supports two types of pipeline actions:

1. **`SYNC_NOW`** (default): Triggers a regular incremental sync
   - API Endpoint: `POST /api/v1/pipelines/{id}/actions/sync-now`
   - Validates pipeline is in `INITIALIZED` state before triggering
   - Honors `ensure_new_job` parameter (default: True)
   - **Default job type**: `INCREMENTAL` (can be overridden)
   - **Use Cases**: Regular scheduled syncs, incremental data updates

2. **`RESYNC`**: Triggers a full historical resync
   - API Endpoint: `POST /api/v1/pipelines/{id}/actions/resync`
   - **No validation required** - can be triggered on any pipeline
   - Re-ingests all data from the source (full historical reload)
   - Ignores `ensure_new_job` parameter
   - **Default job type**:
     - `RESYNC_WITH_EVOLVE` when `resync_mode=EVOLVE_AND_MERGE` (default)
     - `RESYNC_WITH_DROP_AND_LOAD` when `resync_mode=DROP_AND_LOAD`
   - **Optional**: `resync_mode` parameter to control destination table handling
   - **Use Cases**:
     - Reprocessing data after schema changes
     - Recovering from data corruption
     - Applying new transformations to historical data
     - Migrating to a new destination with full data reload
     - Clean slate refresh with `resync_mode=ResyncMode.DROP_AND_LOAD`

**Example**:
```python
from airflow.hevo.models.pipeline import PipelineAction
from airflow.hevo.operators import HevoPipelineOperator

# Regular sync
sync_task = HevoPipelineOperator(
    task_id="sync_pipeline",
    pipeline_id=123,
    action=PipelineAction.SYNC_NOW  # Default
)

# Full historical resync (default: keeps existing tables)
resync_task = HevoPipelineOperator(
    task_id="resync_pipeline",
    pipeline_id=123,
    action=PipelineAction.RESYNC  # Full reload
)

# Full historical resync with DROP_AND_LOAD (drops and recreates tables)
from airflow.hevo.models.pipeline import ResyncMode

resync_clean_task = HevoPipelineOperator(
    task_id="resync_clean",
    pipeline_id=123,
    action=PipelineAction.RESYNC,
    resync_mode=ResyncMode.DROP_AND_LOAD  # Drop data from existing tables before loading
)
```

### Execution Modes

The operator supports three execution modes based on parameter combinations:

1. **Fire-and-forget**: `wait_for_completion=False`
   - Triggers sync and returns job_id immediately
   - Use with HevoSensor for decoupled monitoring

2. **Synchronous wait**: `deferrable=False, wait_for_completion=True`
   - Blocks worker slot until completion
   - Simple but inefficient for long-running jobs
   - **Built-in retry logic**: Automatically retries up to 3 consecutive API failures during job status polling

3. **Deferrable wait** (recommended): `deferrable=True, wait_for_completion=True`
   - Releases worker slot, monitored by triggerer service
   - Resource-efficient

### Built-in Resilience Features

The operator includes automatic retry mechanisms at multiple levels:

1. **Job Discovery Retries**: After triggering a sync, the operator polls up to `retry_limit` times (default: 10) to find the active job, as jobs may take 5-15 seconds to appear in the API.

2. **Synchronous Wait Retries**: When using `deferrable=False`, the operator automatically retries up to 3 consecutive API failures during status polling. Successful checks reset the retry counter.

3. **Network-Level Retries**: The underlying hook retries network errors and configurable HTTP status codes (default: 500-599) based on the `retry_limit` parameter.

### Example Usage

```python
from airflow import DAG
from airflow.hevo.models.job import JobType
from airflow.hevo.models.pipeline import PipelineAction
from airflow.hevo.operators import HevoPipelineOperator

# Deferrable operator with SYNC_NOW (default)
trigger_sync = HevoPipelineOperator(
    task_id="trigger_pipeline_sync",
    pipeline_id=123,
    action=PipelineAction.SYNC_NOW,  # Default - can be omitted
    job_type=JobType.INCREMENTAL,  # Default for SYNC_NOW - can be omitted
    deferrable=True,
    wait_for_completion=True,
    poll_interval=10,
    accept_completed_with_failures=False,
    ensure_new_job=True,  # Default: True - prevents duplicate jobs
    connection_id="hevo_production"
)

# Full historical resync
resync_pipeline = HevoPipelineOperator(
    task_id="resync_pipeline",
    pipeline_id=123,
    action=PipelineAction.RESYNC,  # Full historical reload
    deferrable=True,
    wait_for_completion=True,
    poll_interval=30,  # Less frequent polling for long jobs
    accept_completed_with_failures=True
)

# Fire-and-forget mode
trigger_only = HevoPipelineOperator(
    task_id="trigger_only",
    pipeline_id=456,
    wait_for_completion=False  # Returns job_id via XCom
)
```

---

## HevoSensor Parameters

The `HevoSensor` monitors Hevo pipeline job completion with auto-discovery support.

### Required Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `pipeline_id` | `int` | Unique Hevo pipeline identifier to monitor. Supports Jinja templating. |

### Optional Parameters

| Parameter | Type | Default                               | Description                                                                                                                                                                   |
|-----------|------|---------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `job_id` | `str` | `None`                                | Optional job identifier. If provided, monitors this specific job. If not provided, discovers the active job via auto-discovery. Supports Jinja templating (e.g., XCom pulls). |
| `job_type` | `JobType` or `str` | `JobType.INCREMENTAL`                 | Job type for auto-discovery. Only used when `job_id` is not provided. Can be `INCREMENTAL`, `HISTORICAL`, `RESYNC_WITH_EVOLVE`, or `RESYNC_WITH_DROP_AND_LOAD`.               |
| `connection_id` | `str` | `hevo_airflow_conn_id` (uses default) | Airflow connection ID for Hevo API credentials. If not provided, uses `hevo_airflow_conn_id`.                                                                                 |
| `poke_interval` | `int` | `15`                                  | Seconds between status checks when polling for job completion.                                                                                                                |
| `accept_completed_with_failures` | `bool` | `False`                               | Treat `COMPLETED_WITH_FAILURES` status as success.                                                                                                                            |
| `deferrable` | `bool` | `True`                                | Use deferrable mode to release worker slot while waiting. Requires Airflow triggerer service.                                                                                 |
| `wait_for_job_max_attempts` | `int` | `10`                                  | Maximum attempts to find active job in auto-discovery mode.                                                                                                                   |
| `wait_for_job_interval` | `int` | `5`                                   | Seconds between discovery attempts when looking for active job.                                                                                                               |
| `wait_for_job_initial_delay` | `int` | `10`                                  | Initial delay (in seconds) before first discovery attempt. Allows time for job to be created in Hevo system.                                                                  |

### Discovery Modes

1. **Explicit job_id mode**: Monitors a specific job
   ```python
   sensor = HevoSensor(
       task_id="wait_for_sync",
       pipeline_id=123,
       job_id="{{ ti.xcom_pull(task_ids='trigger_sync') }}"
   )
   ```

2. **Auto-discovery mode**: Automatically finds and monitors active job
   ```python
   sensor = HevoSensor(
       task_id="wait_for_sync",
       pipeline_id=123,
       job_type=JobType.INCREMENTAL  # No job_id provided
   )
   ```

**Auto-Discovery Behavior:**
When `job_id` is not provided, the sensor waits for an active job to appear:
- Applies initial delay (`wait_for_job_initial_delay`) to allow job creation
- Polls up to `wait_for_job_max_attempts` times to find the job
- Waits `wait_for_job_interval` seconds between discovery attempts
- Useful when monitoring jobs triggered externally or when job_id is unavailable

### Example Usage

```python
from airflow.hevo.sensor import HevoSensor
from airflow.hevo.models.job import JobType

# With explicit job_id from XCom
wait_for_job = HevoSensor(
    task_id="wait_for_sync",
    pipeline_id=123,
    job_id="{{ ti.xcom_pull(task_ids='trigger_sync') }}",
    poke_interval=10,
    accept_completed_with_failures=True,
    deferrable=True,
    timeout=3600  # Airflow sensor parameter
)

# With auto-discovery
auto_discover_sensor = HevoSensor(
    task_id="wait_historical_sync",
    pipeline_id=456,
    job_type=JobType.HISTORICAL,
    wait_for_job_initial_delay=15,
    wait_for_job_max_attempts=20,
    wait_for_job_interval=5,
    deferrable=True
)
```

---

## HevoTrigger Parameters

The `HevoTrigger` is used internally by deferrable operators and sensors for async monitoring.

**Note**: Users typically don't instantiate triggers directly - they're created automatically when using `deferrable=True`.

### Parameters

| Parameter | Type | Default                               | Description |
|-----------|------|---------------------------------------|-------------|
| `pipeline_id` | `int` | Required                              | Unique pipeline identifier to monitor. |
| `job_id` | `str` | `None`                                | Optional job identifier. If `None`, auto-discovers active job by type. |
| `job_type` | `JobType` or `str` | `JobType.INCREMENTAL`                 | Job type for auto-discovery. Can be enum or string for compatibility with deserialization. |
| `poke_interval` | `int` | `15`                                  | Seconds between status checks during async polling. |
| `accept_completed_with_failures` | `bool` | `False`                               | Treat `COMPLETED_WITH_FAILURES` status as success. |
| `connection_id` | `str` | `hevo_airflow_conn_id` (uses default) | Airflow connection ID for Hevo API credentials. |

---

## Hook Parameters (BaseHevoHook)

These parameters are available when directly instantiating `HevoPipelineHook` or other hook subclasses. Operators, sensors, and triggers inherit these parameters.

### Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `pipeline_id` | `int` | `None` | Optional pipeline identifier used by downstream hooks. |
| `connection_id` | `str` | `None` | Airflow connection ID to resolve. Defaults to `hevo_airflow_conn_id` if not specified. |
| `retry_limit` | `int` | `3` | Maximum number of HTTP request attempts before surfacing an error. Must be a positive integer. |
| `retry_delay` | `int` | `2` | Seconds to wait between retry attempts. Must be a positive integer. |
| `timeout` | `int` | `30` | HTTP request timeout in seconds. Must be a positive integer. |
| `extra_headers` | `dict[str, str]` | `None` | Additional headers merged into every request. These take precedence over connection-level headers but can be overridden by per-request headers. |
| `extra_kwargs` | `dict[str, Any]` | `None` | Additional keyword arguments passed to `aiohttp` requests. |
| `retryable_status_codes` | `list[int]` | `[500-599]` | List of HTTP status codes that should trigger retries. Examples:<br>- `[500, 502, 503, 504]` - Only retry on specific server errors<br>- `[429, 500, 502, 503]` - Include rate limiting (429) in retries<br>- `[]` - Disable status code-based retries (only network errors) |

### Retry Behavior

- **Network errors**: Always retried (connection failures, timeouts)
- **Status codes**: Configurable via `retryable_status_codes` parameter
- **4xx errors**: Not retried by default (client errors)
- **Delay**: Fixed delay of `retry_delay` seconds between attempts

### Example Usage

```python
from airflow.hevo.hooks import HevoPipelineHook

# Custom retry configuration
hook = HevoPipelineHook(
    connection_id="hevo_production",
    retry_limit=5,
    retry_delay=3,
    timeout=60,
    retryable_status_codes=[429, 500, 502, 503, 504],  # Include rate limiting
    extra_headers={"X-Custom-Header": "value"}
)

# Disable status code retries (network errors only)
hook_no_retry = HevoPipelineHook(
    retry_limit=3,
    retryable_status_codes=[]  # Only retry network errors
)
```

---

## Parameter Quick Reference Table

### Timing & Polling Parameters

| Parameter | Operator | Sensor | Trigger | Hook | Default                | Description |
|-----------|----------|--------|---------|------|------------------------|-------------|
| `poll_interval` | ✅ | ✅ | ✅ | ❌ | `15`                   | Seconds between status checks |
| `poke_interval` | ❌ | ✅ | ✅ | ❌ | `15`                   | Seconds between status checks (sensor-specific name) |
| `retry_limit` | ✅ | ❌ | ❌ | ✅ | `10` (op) / `3` (hook) | Max attempts for job discovery (op) or HTTP retries (hook) |
| `retry_delay` | ❌ | ❌ | ❌ | ✅ | `2`                    | Seconds between HTTP retry attempts |
| `timeout` | ❌ | ❌ | ❌ | ✅ | `30`                   | HTTP request timeout in seconds |

### Job Discovery Parameters

| Parameter | Operator | Sensor | Trigger | Hook | Default | Description |
|-----------|----------|--------|---------|------|---------|-------------|
| `wait_for_job_max_attempts` | ❌ | ✅ | ❌ | ❌ | `10` | Max attempts to find active job |
| `wait_for_job_interval` | ❌ | ✅ | ❌ | ❌ | `5` | Seconds between discovery attempts |
| `wait_for_job_initial_delay` | ❌ | ✅ | ❌ | ❌ | `10` | Initial delay before first discovery attempt |

### Execution Mode Parameters

| Parameter | Operator | Sensor | Trigger | Hook | Default | Description |
|-----------|----------|--------|---------|------|---------|-------------|
| `deferrable` | ✅ | ✅ | N/A | ❌ | `True` | Use deferrable execution mode |
| `wait_for_completion` | ✅ | N/A | ❌ | ❌ | `True` | Wait for job completion |

### Identification Parameters

| Parameter | Operator | Sensor | Trigger | Hook | Default | Description                                                                                                                                  |
|-----------|----------|--------|---------|------|---------|----------------------------------------------------------------------------------------------------------------------------------------------|
| `pipeline_id` | ✅ | ✅ | ✅ | ✅ | Required | Hevo pipeline identifier                                                                                                                     |
| `job_id` | ❌ | ✅ | ✅ | ❌ | `None` | Explicit job ID to monitor                                                                                                                   |
| `job_type` | ✅ | ✅ | ✅ | ❌ | Intelligent default | Job type for discovery/triggering (INCREMENTAL for SYNC_NOW, RESYNC_WITH_EVOLVE/RESYNC_WITH_DROP_AND_LOAD for RESYNC based on resync_mode) |
| `action` | ✅ | ❌ | ❌ | ❌ | `SYNC_NOW` | Pipeline action type (SYNC_NOW or RESYNC)                                                                                                    |
| `connection_id` | ✅ | ✅ | ✅ | ✅ | `hevo_airflow_conn_id` | Airflow connection ID                                                                                                                        |

### Completion & Error Handling Parameters

| Parameter | Operator | Sensor | Trigger | Hook | Default | Description |
|-----------|----------|--------|---------|------|---------|-------------|
| `accept_completed_with_failures` | ✅ | ✅ | ✅ | ❌ | `False` | Treat partial failures as success |
| `ensure_new_job` | ✅ | ❌ | ❌ | ❌ | `True` | Fail if job already in progress for pipeline |
| `retryable_status_codes` | ❌ | ❌ | ❌ | ✅ | `[500-599]` | HTTP status codes to retry |

### Advanced Parameters

| Parameter | Operator | Sensor | Trigger | Hook | Default | Description |
|-----------|----------|--------|---------|------|---------|-------------|
| `extra_headers` | ❌ | ❌ | ❌ | ✅ | `None` | Additional HTTP headers |
| `extra_kwargs` | ❌ | ❌ | ❌ | ✅ | `None` | Additional aiohttp kwargs |

---

## Common Configuration Patterns

### Pattern 1: Production Deferrable Pipeline

**Scenario**: Trigger and wait for sync with minimal worker resource usage.

```python
HevoOperator(
    task_id="sync_pipeline",
    pipeline_id=123,
    deferrable=True,           # Release worker slot
    wait_for_completion=True,   # Wait until done
    poll_interval=10,           # Check every 10s
    retry_limit=15,             # Allow 15 attempts to find job
    accept_completed_with_failures=False  # Strict success
)
```

### Pattern 2: Fire-and-Forget with Sensor

**Scenario**: Decouple triggering from monitoring for better DAG structure.

```python
# Task 1: Trigger
trigger = HevoOperator(
    task_id="trigger_sync",
    pipeline_id=123,
    wait_for_completion=False  # Return job_id immediately
)

# Task 2: Monitor
sensor = HevoSensor(
    task_id="wait_for_sync",
    pipeline_id=123,
    job_id="{{ ti.xcom_pull(task_ids='trigger_sync') }}",
    deferrable=True,
    poke_interval=15,
    timeout=7200  # 2 hour timeout
)

trigger >> sensor
```

### Pattern 3: Auto-Discovery Historical Load

**Scenario**: Monitor a historical load without explicit job_id.

```python
HevoSensor(
    task_id="wait_historical",
    pipeline_id=456,
    job_type=JobType.HISTORICAL,
    wait_for_job_initial_delay=30,  # Historical jobs may take longer to appear
    wait_for_job_max_attempts=20,
    wait_for_job_interval=10,
    deferrable=True
)
```

### Pattern 4: Tolerating Partial Failures

**Scenario**: Accept job completion even if some records fail.

```python
HevoOperator(
    task_id="sync_with_tolerance",
    pipeline_id=789,
    accept_completed_with_failures=True,  # Accept partial failures
    deferrable=True,
    poll_interval=5
)
```

### Pattern 5: Custom Retry Strategy

**Scenario**: Aggressive retry configuration for flaky networks.

```python
from airflow.hevo.hooks import HevoPipelineHook

hook = HevoPipelineHook(
    retry_limit=10,             # More retry attempts
    retry_delay=5,              # Longer delay between retries
    timeout=60,                 # Longer timeout
    retryable_status_codes=[429, 500, 502, 503, 504]  # Include rate limiting
)
```

### Pattern 6: Synchronous Wait (Testing/Simple Use Cases)

**Scenario**: Simple synchronous execution for testing or short jobs.

```python
HevoOperator(
    task_id="quick_sync",
    pipeline_id=123,
    deferrable=False,           # Use synchronous polling
    wait_for_completion=True,
    poll_interval=5,
    retry_limit=10
)
```

### Pattern 7: Multiple Pipelines in Parallel

**Scenario**: Trigger multiple pipelines concurrently.

```python
from airflow.decorators import task_group

@task_group
def sync_all_pipelines():
    pipelines = [101, 102, 103, 104]

    for pipeline_id in pipelines:
        HevoOperator(
            task_id=f"sync_pipeline_{pipeline_id}",
            pipeline_id=pipeline_id,
            deferrable=True,
            wait_for_completion=True,
            poll_interval=10
        )

sync_all_pipelines()
```

### Pattern 8: Ensure a new job is registered by the operator

**Scenario**: Ensure a new job is triggered and monitored by the operator.

```python
HevoOperator(
    task_id="sync_pipeline",
    pipeline_id=123,
    ensure_new_job=True,        # Default: True - fail if job already exists
    deferrable=True,
    wait_for_completion=True,
    poll_interval=10
)
```

To allow monitoring an already existing job, set `ensure_new_job=False`:

```python
HevoOperator(
    task_id="sync_pipeline",
    pipeline_id=123,
    ensure_new_job=False,       # Allow monitoring already existing job
    deferrable=True,
    wait_for_completion=True,
    poll_interval=10
)
```

### Pattern 9: Full Historical Resync

**Scenario**: Trigger a complete historical data reload after schema changes or data corruption.

```python
from airflow.hevo.models.job import JobType
from airflow.hevo.models.pipeline import PipelineAction, ResyncMode
from airflow.hevo.operators import HevoPipelineOperator

# Standard resync (keeps existing destination tables)
HevoPipelineOperator(
    task_id="resync_pipeline",
    pipeline_id=123,
    action=PipelineAction.RESYNC,  # Full historical reload
    job_type=JobType.RESYNC_WITH_EVOLVE,  # Default for RESYNC with resync_mode=EVOLVE_AND_MERGE, can be omitted
    deferrable=True,
    wait_for_completion=True,
    poll_interval=30,  # Less frequent polling for long-running resync jobs
    retry_limit=20,  # More attempts to find job (resyncs may take longer to appear)
    accept_completed_with_failures=True  # Allow partial failures during large resyncs
)

# Clean slate resync (drops all data and loads to destination tables)
HevoPipelineOperator(
    task_id="resync_clean_slate",
    pipeline_id=123,
    action=PipelineAction.RESYNC,
    resync_mode=ResyncMode.DROP_AND_LOAD,  # Drop existing tables before loading
    job_type=JobType.RESYNC_WITH_DROP_AND_LOAD,  # Default for RESYNC with resync_mode=DROP_AND_LOAD, can be omitted
    deferrable=True,
    wait_for_completion=True,
    poll_interval=30,
    retry_limit=20,
    accept_completed_with_failures=True
)
```

---

## Connection Configuration

All components use Airflow connections for authentication. Create a connection with:

```
Connection ID: hevo_airflow_conn_id (or custom via connection_id parameter)
Connection Type: HTTP
Host: us.hevodata.com (or your region)
Schema: https
Login: <your_api_username>
Password: <your_api_key>
Extra: {"headers": {"X-Custom-Header": "value"}}  # Optional
```

---