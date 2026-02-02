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

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `action` | `PipelineAction` | `PipelineAction.SYNC_NOW` | Pipeline action to trigger:<br>- `SYNC_NOW`: Regular incremental sync (POST `/pipelines/{id}/actions/sync-now`). Requires pipeline in INITIALIZED state.<br>- `RESYNC`: Full historical resync (POST `/pipelines/{id}/actions/resync`). Re-ingests all data from source. |
| `job_type` | `JobType` or `str` | **Intelligent default**<br>`INCREMENTAL` (SYNC_NOW)<br>`TRUNCATE_AND_LOAD` (RESYNC) | Type of job to wait for when discovering the active job after triggering. Defaults intelligently based on action. Can be explicitly set to `INCREMENTAL`, `HISTORICAL`, or `TRUNCATE_AND_LOAD`. |
| `connection_id` | `str` | `hevo_airflow_conn_id` (uses default) | Airflow connection ID for Hevo API credentials. If not provided, uses `hevo_airflow_conn_id`. |
| `poll_interval` | `int` | `5` | Seconds between status checks when waiting for completion. |
| `retry_limit` | `int` | `10` | Maximum number of attempts to find the active job after triggering sync. |
| `deferrable` | `bool` | `True` | Use deferrable execution to release worker slot while waiting. Requires Airflow triggerer service to be running. **Recommended for production.** |
| `wait_for_completion` | `bool` | `True` | Wait for the job to complete before returning. If `False`, returns `job_id` immediately via XCom for use with `HevoSensor`. |
| `accept_completed_with_failures` | `bool` | `False` | Treat `COMPLETED_WITH_FAILURES` status as success. Useful when partial failures (e.g., some records failing) are acceptable. |
| `ensure_new_job` | `bool` | `True` | If `True`, fails if a job is already in progress for the pipeline (default behavior). If `False`, proceeds normally even if a job already exists. Useful to prevent triggering duplicate jobs when previous jobs are still running. **Only applies to SYNC_NOW action.** |
| `drop_and_load` | `bool` | `False` | If `True`, drops existing destination tables before loading. Ensures a clean slate by recreating tables from scratch. **Only applies to RESYNC action.** Useful for completely refreshing data when schema or data quality issues require a clean restart. |

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
   - **Default job type**: `TRUNCATE_AND_LOAD` (can be overridden)
   - **Optional**: `drop_and_load` parameter to drop/recreate destination tables
   - **Use Cases**:
     - Reprocessing data after schema changes
     - Recovering from data corruption
     - Applying new transformations to historical data
     - Migrating to a new destination with full data reload
     - Clean slate refresh with `drop_and_load=True`

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

# Full historical resync with drop_and_load (drops and recreates tables)
resync_clean_task = HevoPipelineOperator(
    task_id="resync_clean",
    pipeline_id=123,
    action=PipelineAction.RESYNC,
    drop_and_load=True  # Drop existing tables before loading
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
   - Resource-efficient, ideal for production

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

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `job_id` | `str` | `None` | Optional job identifier. If provided, monitors this specific job. If not provided, discovers the active job via auto-discovery. Supports Jinja templating (e.g., XCom pulls). |
| `job_type` | `JobType` or `str` | `JobType.INCREMENTAL` | Job type for auto-discovery. Only used when `job_id` is not provided. Can be `INCREMENTAL`, `HISTORICAL`, or `TRUNCATE_AND_LOAD`. |
| `connection_id` | `str` | `hevo_airflow_conn_id` (uses default) | Airflow connection ID for Hevo API credentials. If not provided, uses `hevo_airflow_conn_id`. |
| `poke_interval` | `int` | `5` | Seconds between status checks when polling for job completion. |
| `accept_completed_with_failures` | `bool` | `False` | Treat `COMPLETED_WITH_FAILURES` status as success. |
| `deferrable` | `bool` | `True` | Use deferrable mode to release worker slot while waiting. Requires Airflow triggerer service. |
| `wait_for_job_max_attempts` | `int` | `10` | Maximum attempts to find active job in auto-discovery mode. |
| `wait_for_job_interval` | `int` | `5` | Seconds between discovery attempts when looking for active job. |
| `wait_for_job_initial_delay` | `int` | `10` | Initial delay (in seconds) before first discovery attempt. Allows time for job to be created in Hevo system. |

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

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `pipeline_id` | `int` | Required | Unique pipeline identifier to monitor. |
| `job_id` | `str` | `None` | Optional job identifier. If `None`, auto-discovers active job by type. |
| `job_type` | `JobType` or `str` | `JobType.INCREMENTAL` | Job type for auto-discovery. Can be enum or string for compatibility with deserialization. |
| `poke_interval` | `int` | `5` | Seconds between status checks during async polling. |
| `accept_completed_with_failures` | `bool` | `False` | Treat `COMPLETED_WITH_FAILURES` status as success. |
| `connection_id` | `str` | `hevo_airflow_conn_id` (uses default) | Airflow connection ID for Hevo API credentials. |

### Technical Notes

- Runs in Airflow's triggerer service (separate process)
- Uses async HTTP requests via `aiohttp`
- Yields `TriggerEvent` when job reaches terminal state
- Automatically serialized/deserialized for persistence across Airflow restarts

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

| Parameter | Operator | Sensor | Trigger | Hook | Default | Description |
|-----------|----------|--------|---------|------|---------|-------------|
| `poll_interval` | ✅ | ✅ | ✅ | ❌ | `5` | Seconds between status checks |
| `poke_interval` | ❌ | ✅ | ✅ | ❌ | `5` | Seconds between status checks (sensor-specific name) |
| `retry_limit` | ✅ | ❌ | ❌ | ✅ | `10` (op) / `3` (hook) | Max attempts for job discovery (op) or HTTP retries (hook) |
| `retry_delay` | ❌ | ❌ | ❌ | ✅ | `2` | Seconds between HTTP retry attempts |
| `timeout` | ❌ | ❌ | ❌ | ✅ | `30` | HTTP request timeout in seconds |

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

| Parameter | Operator | Sensor | Trigger | Hook | Default | Description |
|-----------|----------|--------|---------|------|---------|-------------|
| `pipeline_id` | ✅ | ✅ | ✅ | ✅ | Required | Hevo pipeline identifier |
| `job_id` | ❌ | ✅ | ✅ | ❌ | `None` | Explicit job ID to monitor |
| `job_type` | ✅ | ✅ | ✅ | ❌ | Intelligent default | Job type for discovery/triggering (INCREMENTAL for SYNC_NOW, TRUNCATE_AND_LOAD for RESYNC) |
| `action` | ✅ | ❌ | ❌ | ❌ | `SYNC_NOW` | Pipeline action type (SYNC_NOW or RESYNC) |
| `connection_id` | ✅ | ✅ | ✅ | ✅ | `hevo_airflow_conn_id` | Airflow connection ID |

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

### Pattern 8: Preventing Duplicate Job Triggers

**Scenario**: Ensure no duplicate jobs are triggered when a job is already running (default behavior).

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

This is the **default behavior** (since `ensure_new_job=True` by default) and is useful in scenarios where:
- DAG is manually triggered multiple times
- External systems trigger the same pipeline concurrently
- You want to prevent queueing multiple jobs for the same pipeline
- Strict job isolation is required for your use case

To allow triggering even when a job exists, set `ensure_new_job=False`:

```python
HevoOperator(
    task_id="sync_pipeline",
    pipeline_id=123,
    ensure_new_job=False,       # Allow triggering if job already running
    deferrable=True,
    wait_for_completion=True,
    poll_interval=10
)
```

### Pattern 9: Full Historical Resync

**Scenario**: Trigger a complete historical data reload after schema changes or data corruption.

```python
from airflow.hevo.models.job import JobType
from airflow.hevo.models.pipeline import PipelineAction
from airflow.hevo.operators import HevoPipelineOperator

# Standard resync (keeps existing destination tables)
HevoPipelineOperator(
    task_id="resync_pipeline",
    pipeline_id=123,
    action=PipelineAction.RESYNC,  # Full historical reload
    job_type=JobType.TRUNCATE_AND_LOAD,  # Default for RESYNC, can be omitted
    deferrable=True,
    wait_for_completion=True,
    poll_interval=30,  # Less frequent polling for long-running resync jobs
    retry_limit=20,  # More attempts to find job (resyncs may take longer to appear)
    accept_completed_with_failures=True  # Allow partial failures during large resyncs
)

# Clean slate resync (drops and recreates destination tables)
HevoPipelineOperator(
    task_id="resync_clean_slate",
    pipeline_id=123,
    action=PipelineAction.RESYNC,
    drop_and_load=True,  # Drop existing tables before loading
    job_type=JobType.TRUNCATE_AND_LOAD,  # Monitors TRUNCATE_AND_LOAD job type
    deferrable=True,
    wait_for_completion=True,
    poll_interval=30,
    retry_limit=20,
    accept_completed_with_failures=True
)
```

**Use Cases**:
- **Schema Changes**: Reprocess all data after modifying source schema or adding transformations
- **Data Corruption**: Recover from data quality issues by re-ingesting from source
- **Clean Slate Refresh**: Use `drop_and_load=True` to drop and recreate tables, ensuring no remnants from previous loads
- **Destination Migration**: Populate a new destination with full historical data
- **Compliance**: Re-apply updated data masking or privacy rules to historical records

**Job Type**: RESYNC operations default to tracking `TRUNCATE_AND_LOAD` job type (can be overridden to `INCREMENTAL` or `HISTORICAL` if needed)

---

## Parameter Validation Rules

### Type Validation

- **Integer parameters**: Must be positive integers
  - `pipeline_id`, `poll_interval`, `retry_limit`, etc.

- **Boolean parameters**: Must be `True` or `False`
  - `deferrable`, `wait_for_completion`, `accept_completed_with_failures`

- **Enum parameters**: Can be enum or string
  - `job_type`: `JobType.INCREMENTAL` or `"INCREMENTAL"`
  - `action`: `PipelineAction.SYNC_NOW` or `PipelineAction.RESYNC`

### Logical Constraints

1. **Deferrable mode requires triggerer service**
   - Setting `deferrable=True` without a running triggerer will cause tasks to hang

2. **Fire-and-forget returns job_id**
   - `wait_for_completion=False` makes operator return job_id string via XCom
   - `wait_for_completion=True` makes operator return `None`

3. **Sensor requires either job_id or auto-discovery**
   - Provide explicit `job_id` OR rely on auto-discovery
   - Auto-discovery uses `job_type` to find the active job

4. **Retry limits affect different behaviors**
   - **Operator `retry_limit`**: Max attempts to find job after triggering (default: 10)
   - **Hook `retry_limit`**: Max HTTP request retries for any API call (default: 3)

---

## Environment-Specific Recommendations

### Development

```python
HevoOperator(
    pipeline_id=123,
    deferrable=False,      # Simpler debugging
    poll_interval=5,
    retry_limit=5,
    timeout=30
)
```

### Staging

```python
HevoOperator(
    pipeline_id=123,
    deferrable=True,       # Test deferrable mode
    poll_interval=10,
    retry_limit=10,
    timeout=45
)
```

### Production

```python
HevoOperator(
    pipeline_id=123,
    deferrable=True,                    # Always deferrable
    poll_interval=15,                   # Reduce API load
    retry_limit=20,                     # More resilient
    timeout=60,                         # Longer timeout
    accept_completed_with_failures=True # Based on business logic
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

## Performance Tuning Guidelines

### Reduce API Load

- **Increase `poll_interval`**: Check less frequently (10-30 seconds for production)
- **Use deferrable mode**: Reduces worker resource consumption

### Improve Responsiveness

- **Decrease `poll_interval`**: Check more frequently (5 seconds minimum recommended)
- **Increase `retry_limit`**: More attempts to find jobs after triggering

### Handle Flaky Networks

- **Increase `timeout`**: Allow longer for responses (30-60 seconds)
- **Increase `retry_delay`**: Give more time between retries (3-5 seconds)
- **Include rate limiting in retries**: `retryable_status_codes=[429, 500, 502, 503]`

### Optimize for Long-Running Jobs

- **Use deferrable mode**: Essential for jobs taking >5 minutes
- **Increase sensor `timeout`**: Set appropriate timeout for job duration
- **Use fire-and-forget pattern**: Decouple triggering from monitoring

---

## Troubleshooting

### Job Not Found After Triggering

**Symptoms**: `AirflowException: No active job found after X attempts`

**Solutions**:
- Increase `retry_limit` (operator) or `wait_for_job_max_attempts` (sensor)
- Increase `wait_for_job_initial_delay` (sensor) - job may take longer to appear
- Verify pipeline is in `INITIALIZED` state
- Check `job_type` matches the triggered job type

### Worker Slot Exhaustion

**Symptoms**: All workers busy, tasks queuing

**Solutions**:
- Set `deferrable=True` on all long-running tasks
- Ensure triggerer service is running
- Increase `poll_interval` to reduce active polling

### API Rate Limiting

**Symptoms**: HTTP 429 errors in logs

**Solutions**:
- Add 429 to `retryable_status_codes`: `[429, 500, 502, 503]`
- Increase `poll_interval` to reduce request frequency
- Increase `retry_delay` to space out retries

---

## External Hevo APIs Used

This section documents all external Hevo API endpoints called by the Airflow provider components (operators, sensors, triggers, and hooks).

### API Base URL

All API requests are made to the Hevo API endpoint configured in your Airflow connection:

```
Base URL: https://{region}.hevodata.com
Examples:
  - https://us.hevodata.com (US region)
  - https://eu.hevodata.com (EU region)
  - https://in.hevodata.com (India region)
```

### Authentication

All API requests require HTTP Basic Authentication using credentials from the Airflow connection:
- **Username**: API username (connection `login` field)
- **Password**: API key (connection `password` field)

### API Endpoints Reference

#### 1. Get Pipeline Details

**Endpoint**: `GET /api/v1/pipelines/{pipeline_id}`

**Description**: Retrieves complete pipeline information including status, source, destination, and configuration.

**Used By**:
- `HevoOperator.execute()` - Validates pipeline before triggering sync
- `HevoPipelineHook.get_pipeline_async()` - Direct pipeline info retrieval
- `HevoPipelineHook.validate_pipeline_async()` - Pipeline state validation

**Request**:
```http
GET /api/v1/pipelines/123 HTTP/1.1
Host: us.hevodata.com
Authorization: Basic <base64-encoded-credentials>
User-Agent: hevo_airflow_provider-airflow/{version}
Accept: application/json
```

**Response (Success - 200)**:
```json
{
  "id": 123,
  "name": "PostgreSQL to Snowflake",
  "status": "INITIALIZED",
  "source": {
    "source_id": "src_456",
    "source_name": "Production DB",
    "source_type": "PostgreSQL"
  },
  "destination": {
    "destination_id": "dest_789",
    "destination_name": "Analytics Warehouse",
    "destination_type": "Snowflake"
  },
  "config": {
    "sync_type": "ON_DEMAND",
    "objects": [...]
  },
  "created_at": "2024-01-01T00:00:00Z",
  "updated_at": "2024-01-15T10:30:00Z"
}
```

**Response (Not Found - 404)**:
```json
{
  "error": "Pipeline not found",
  "message": "Pipeline with ID 123 does not exist"
}
```

**Error Handling**:
- `404`: Returns `None` in `get_pipeline_async()`, raises exception in `validate_pipeline_async()`
- `401/403`: Authentication error - check connection credentials
- `500+`: Server error - automatic retry based on `retryable_status_codes`

---

#### 2. Trigger Pipeline Sync

**Endpoint**: `POST /api/v1/pipelines/{pipeline_id}/actions/sync-now`

**Description**: Triggers a sync operation for the specified pipeline. The pipeline must be in `INITIALIZED` state.

**Used By**:
- `HevoOperator.execute()` - Triggers sync operation
- `HevoPipelineHook.trigger_pipeline_sync_async()` - Direct sync triggering

**Request**:
```http
POST /api/v1/pipelines/123/actions/sync-now HTTP/1.1
Host: us.hevodata.com
Authorization: Basic <base64-encoded-credentials>
User-Agent: hevo_airflow_provider-airflow/{version}
Content-Type: application/json
Accept: application/json
```

**Response (Success - 200/204)**:
```json
{
  "message": "Sync triggered successfully",
  "pipeline_id": 123
}
```
*Note: Response may be 204 No Content with empty body*

**Response (Job Already in Progress - 409 or 500)**:
```json
{
  "error": "Conflict",
  "message": "A job is already in progress for this pipeline"
}
```

**Error Handling**:
- `409/500` with "job is in progress": Handled by `ensure_new_job` parameter logic
- `400`: Bad request - pipeline not in valid state (e.g., PAUSED, STOPPED)
- `401/403`: Authentication error
- `404`: Pipeline not found
- `500+`: Server error - automatic retry

**Important Notes**:
- Jobs may take 5-15 seconds to appear in the jobs list after triggering
- If a job is already running, API behavior depends on pipeline configuration
- Use `ensure_new_job=True` to explicitly fail if job already exists

---

#### 3. Trigger Pipeline Resync

**Endpoint**: `POST /api/v1/pipelines/{pipeline_id}/actions/resync`

**Description**: Triggers a full historical resync for the specified pipeline, re-ingesting all data from the source. Unlike sync-now, this does not require the pipeline to be in any specific state.

**Used By**:
- `HevoOperator.execute()` - When `action=PipelineAction.RESYNC`
- `HevoPipelineHook.resync_pipeline_sync()` - Direct resync triggering

**Request (Default)**:
```http
POST /api/v1/pipelines/123/actions/resync HTTP/1.1
Host: us.hevodata.com
Authorization: Basic <base64-encoded-credentials>
User-Agent: hevo_airflow_provider-airflow/{version}
Content-Type: application/json
Accept: application/json

{
  "drop_and_load": false
}
```

**Request (With Drop and Load)**:
```http
POST /api/v1/pipelines/123/actions/resync HTTP/1.1
Host: us.hevodata.com
Authorization: Basic <base64-encoded-credentials>
User-Agent: hevo_airflow_provider-airflow/{version}
Content-Type: application/json
Accept: application/json

{
  "drop_and_load": true
}
```

**Request Body Parameters**:
| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `drop_and_load` | `bool` | No | `false` | If `true`, drops existing destination tables before loading. Ensures a clean slate by recreating tables from scratch. |

**Response (Success - 200/204)**:
```json
{
  "message": "Resync triggered successfully",
  "pipeline_id": 123
}
```
*Note: Response may be 204 No Content with empty body*

**Error Handling**:
- `401/403`: Authentication error
- `404`: Pipeline not found
- `500+`: Server error - automatic retry

**Important Notes**:
- Triggers a full historical data reload from the source
- Does not require pipeline validation or specific state
- Jobs may take 5-15 seconds to appear in the jobs list after triggering
- Use for reprocessing data after schema changes, corruption recovery, or destination migration
- **Default job type**: Tracks `TRUNCATE_AND_LOAD` job type (operator defaults to this)
- Set `drop_and_load=true` to drop and recreate destination tables for a completely clean reload

**Comparison with Sync-Now**:

| Feature | sync-now | resync |
|---------|----------|--------|
| Validation Required | Yes (INITIALIZED state) | No |
| Data Scope | Incremental updates | Full historical reload |
| Honors ensure_new_job | Yes | No |
| Default Job Type | `INCREMENTAL` | `TRUNCATE_AND_LOAD` |
| Drop and Load Support | No | Yes (via `drop_and_load` param) |
| Typical Duration | Minutes | Hours (depending on data volume) |
| Use Case | Regular scheduled syncs | Schema changes, recovery, migration |

---

#### 4. List Pipeline Jobs

**Endpoint**: `GET /api/v1/pipelines/{pipeline_id}/jobs`

**Description**: Lists all jobs for a pipeline with cursor-based pagination. Used for discovering active jobs by type.

**Used By**:
- `HevoOperator.execute()` - Finds active job after triggering
- `HevoSensor.poke()` / `HevoSensor._get_job_id()` - Auto-discovers active job
- `HevoTrigger.run()` - Auto-discovers active job in deferrable mode
- `HevoPipelineHook.find_active_job_by_type_async()` - Job discovery

**Request**:
```http
GET /api/v1/pipelines/123/jobs?limit=10&cursor=abc123 HTTP/1.1
Host: us.hevodata.com
Authorization: Basic <base64-encoded-credentials>
User-Agent: hevo_airflow_provider-airflow/{version}
Accept: application/json
```

**Query Parameters**:
| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `limit` | `int` | No | `10` | Number of jobs per page (max: 100) |
| `cursor` | `string` | No | `null` | Pagination cursor for next page |

**Response (Success - 200)**:
```json
{
  "data": [
    {
      "job_id": "job_abc123",
      "type": "INCREMENTAL",
      "status": "IN_PROGRESS",
      "created_at": "2024-01-15T10:00:00Z",
      "started_at": "2024-01-15T10:01:00Z",
      "statistics": {
        "rows_loaded": 5000,
        "rows_failed": 10
      }
    },
    {
      "job_id": "job_xyz789",
      "type": "HISTORICAL",
      "status": "COMPLETED",
      "created_at": "2024-01-14T08:00:00Z",
      "started_at": "2024-01-14T08:01:00Z",
      "completed_at": "2024-01-14T09:30:00Z",
      "statistics": {
        "rows_loaded": 1000000,
        "rows_failed": 0
      }
    }
  ],
  "has_more": true,
  "next_cursor": "cursor_next_page",
  "count": 2
}
```

**Response Fields**:
- `data`: Array of job objects
- `has_more`: Boolean indicating if more pages exist
- `next_cursor`: Cursor string for next page (null if no more pages)
- `count`: Number of jobs in current page

**Job Types**:
- `INCREMENTAL`: Regular incremental sync
- `HISTORICAL`: Historical data backfill
- `TRUNCATE_AND_LOAD`: Full reload with truncate
- `UNKNOWN`: Unmapped/future job types (handled gracefully)

**Job Statuses**:
- `IN_PROGRESS`: Job currently running
- `QUEUED`: Job waiting to start
- `PENDING`: Job scheduled
- `COMPLETED`: Job finished successfully
- `COMPLETED_WITH_FAILURES`: Job finished but some records failed
- `FAILED`: Job failed completely
- `CANCELLED`: Job was cancelled
- `SKIPPED`: Job was skipped
- `DEFERRED`: Job deferred
- `DEFERRED_WITH_FAILURES`: Job deferred with failures
- `UNKNOWN`: Unmapped/future statuses (handled gracefully)

**Error Handling**:
- `404`: Pipeline not found
- `401/403`: Authentication error
- `500+`: Server error - automatic retry

**Pagination Example**:
```python
# First page
GET /api/v1/pipelines/123/jobs?limit=10

# Second page using cursor from first response
GET /api/v1/pipelines/123/jobs?limit=10&cursor=abc123def456

# Continue until has_more=false
```

---

#### 5. Get Job Status

**Endpoint**: `GET /api/v1/pipelines/{pipeline_id}/jobs/{job_id}`

**Description**: Retrieves detailed status and statistics for a specific job.

**Used By**:
- `HevoOperator._wait_synchronously()` - Polls job status in synchronous mode
- `HevoSensor.poke()` - Checks job completion
- `HevoTrigger.run()` - Polls job status in async mode
- `HevoPipelineHook.get_job_completion_status_async()` - Gets job status

**Request**:
```http
GET /api/v1/pipelines/123/jobs/job_abc123 HTTP/1.1
Host: us.hevodata.com
Authorization: Basic <base64-encoded-credentials>
User-Agent: hevo_airflow_provider-airflow/{version}
Accept: application/json
```


**Status Mapping**:

The provider maps Hevo API statuses to four canonical states:

| API Status                | Mapped Status | Success | Notes |
|---------------------------|---------------|---------|-------|
| `COMPLETED`               | `completed` | ✅ Yes | Job finished successfully |
| `COMPLETED_WITH_FAILURES` | `completed_with_failures` | ⚠️ Conditional | Success if `accept_completed_with_failures=True`, failure otherwise |
| `FAILED`                  | `failed` | ❌ No | Job failed completely |
| `CANCELLED`               | `failed` | ❌ No | Job was cancelled |
| `SKIPPED`                 | `failed` | ❌ No | Job was skipped |
| `DEFERRED`                | `failed` | ❌ No | Job deferred (treated as failure) |
| `DEFERRED_WITH_FAILURES`  | `failed` | ❌ No | Job deferred with errors |
| `IN_PROGRESS`             | `pending` | ⏳ Pending | Job still running |
| `QUEUED`                  | `pending` | ⏳ Pending | Job waiting to start |
| `PENDING`                 | `pending` | ⏳ Pending | Job scheduled |
| `UNKNOWN`                 | `pending` | ⏳ Pending | Unknown status (continues monitoring) |

**Error Handling**:
- `404`: Job not found (may not have been created yet after trigger)
- `401/403`: Authentication error
- `500+`: Server error - automatic retry
