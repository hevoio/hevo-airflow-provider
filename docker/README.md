# Docker Setup for Hevo Airflow Provider

This directory contains Docker configurations for running the Hevo Airflow Provider example DAGs with different Airflow and Python versions.

## Table of Contents

1. [Getting Started with Docker](#getting-started-with-docker)
2. [Available Configurations](#available-configurations)
3. [Quick Start](#quick-start)
4. [Configuration](#configuration)
5. [Understanding Operators vs Sensors](#understanding-operators-vs-sensors)
6. [Configuration Parameters Reference](#configuration-parameters-reference)
7. [Available Example DAGs](#available-example-dags)
8. [Container Management](#container-management)
9. [Development Workflow](#development-workflow)
10. [Advanced Configuration](#advanced-configuration)
11. [Troubleshooting](#troubleshooting)

---

## Getting Started with Docker

### What You'll Get

This Docker setup provides a complete, isolated Airflow environment with:
- Airflow webserver, scheduler, and triggerer (standalone mode)
- Hevo Airflow Provider pre-installed and ready to use
- Example DAGs demonstrating different usage patterns

### Prerequisites

Before starting, ensure you have:
- Docker Engine 20.10+ installed ([Install Docker](https://docs.docker.com/get-docker/))
- Docker Compose 2.0+ installed (bundled with Docker Desktop)
- Your Hevo API credentials:
  - API Username
  - API Key
  - Region endpoint (e.g., us.hevodata.com, eu.hevodata.com)

### 5-Minute Quick Setup

1. **Navigate to your preferred Airflow version**:
   ```bash
   cd docker/airflow-3.0/  # Or airflow-2.4/
   ```

2. **Configure Hevo credentials** in `docker-compose.yml`:
   ```yaml
   environment:
     - AIRFLOW_CONN_HEVO_DEFAULT=http://your_username:your_api_key@region.hevodata.com
   ```

3. **Start Airflow**:
   ```bash
   docker-compose up --build
   ```

4. **Access Airflow UI**:
   - Open http://localhost:8080
   - Login: `admin` / `admin`

5. **Run your first DAG**:
   - Enable the `triggerer_example_dag`
   - Update the `pipeline_id` with your Hevo pipeline ID
   - Click "Trigger DAG"

That's it! You're now running Hevo pipelines from Airflow.

---

## Available Configurations

### 1. Airflow 2.4.x + Python 3.9
- **Directory**: `docker/airflow-2.4/`
- **Airflow Version**: 2.4.3
- **Python Version**: 3.9
- **Use Case**: Testing compatibility with Airflow 2.x series (minimum supported version)

### 2. Airflow 3.0.x + Python 3.13
- **Directory**: `docker/airflow-3.0/`
- **Airflow Version**: 3.0.6
- **Python Version**: 3.13
- **Use Case**: Testing with latest Airflow 3.x series

---

## Quick Start

### Prerequisites

- Docker Engine 20.10+
- Docker Compose 2.0+
- Hevo API credentials (username and API key)

### Option 1: Using Docker Compose (Recommended)

#### For Airflow 2.4:
```bash
# Navigate to the Airflow 2.4 directory
cd docker/airflow-2.4/

# Update docker-compose.yml with your Hevo credentials (see Configuration section)
# Build and start the container
docker-compose up --build

# Access Airflow UI at http://localhost:8080
# Username: admin
# Password: admin
```

#### For Airflow 3.0:
```bash
# Navigate to the Airflow 3.0 directory
cd docker/airflow-3.0/

# Update docker-compose.yml with your Hevo credentials (see Configuration section)
# Build and start the container
docker-compose up --build

# Access Airflow UI at http://localhost:8080
# Username: admin
# Password: admin
```

### Option 2: Using Docker CLI

#### For Airflow 2.4:
```bash
# Build the image
docker build -t hevo-airflow-2.4 -f docker/airflow-2.4/Dockerfile .

# Run the container
docker run -d \
  --name hevo-airflow-2.4 \
  -p 8080:8080 \
  -v $(pwd)/dags:/opt/airflow/dags \
  -v $(pwd)/docker/airflow-2.4/logs:/opt/airflow/logs \
  -e AIRFLOW_CONN_HEVO_DEFAULT=http://your_api_username:your_api_key@us.hevodata.com \
  hevo-airflow-2.4
```

#### For Airflow 3.0:
```bash
# Build the image
docker build -t hevo-airflow-3.0 -f docker/airflow-3.0/Dockerfile .

# Run the container
docker run -d \
  --name hevo-airflow-3.0 \
  -p 8080:8080 \
  -v $(pwd)/dags:/opt/airflow/dags \
  -v $(pwd)/docker/airflow-3.0/logs:/opt/airflow/logs \
  -e AIRFLOW_CONN_HEVO_DEFAULT=http://your_api_username:your_api_key@us.hevodata.com \
  hevo-airflow-3.0
```

## Configuration

### Basic Configuration

#### Hevo Connection Setup

Before running containers, configure your Hevo API credentials:

**Method 1: Environment Variable** (Recommended for Docker):
```yaml
# In docker-compose.yml
environment:
  - AIRFLOW_CONN_HEVO_DEFAULT=http://your_api_username:your_api_key@us.hevodata.com
```

**Method 2: Airflow UI** (For manual setup):
1. Access http://localhost:8080 → Admin → Connections
2. Create connection:
   - **Connection Id**: `hevo_default`
   - **Connection Type**: HTTP
   - **Host**: `us.hevodata.com` (or your region: `eu`, `in`, `ap`)
   - **Schema**: `https`
   - **Login**: Your API username
   - **Password**: Your API key

#### Port Configuration

Change Airflow webserver port in `docker-compose.yml`:
```yaml
ports:
  - "9090:8080"  # Host:Container
```

#### Pipeline IDs

Update example DAG files with your Hevo pipeline IDs:
```python
# In dags/triggerer_example_dag.py
trigger_sync = HevoOperator(
    task_id="sync_pipeline",
    pipeline_id=123,  # ← Replace with your pipeline ID
    ...
)
```

---

## Understanding Operators vs Sensors

The Hevo Airflow Provider offers two main components for orchestrating pipeline syncs. Understanding when to use each is crucial for building efficient DAGs.

### HevoOperator

**Purpose**: Triggers and optionally waits for Hevo pipeline syncs.

**When to Use**:
- ✅ You want to trigger a pipeline sync and wait for completion in a single task
- ✅ You need deferrable execution (recommended for production)
- ✅ You want a simple, all-in-one solution
- ✅ You're triggering a sync as part of a larger workflow

**Execution Modes**:

1. **Deferrable Wait** (⭐ Recommended for Production)
   ```python
   HevoOperator(
       task_id="sync_pipeline",
       pipeline_id=123,
       deferrable=True,           # Release worker slot
       wait_for_completion=True   # Wait for completion
   )
   ```
   - **Pros**: Resource-efficient, releases worker slot, handles thousands of concurrent tasks
   - **Cons**: Requires triggerer service running
   - **Use when**: Production environments, long-running syncs, many concurrent pipelines

2. **Synchronous Wait**
   ```python
   HevoOperator(
       task_id="sync_pipeline",
       pipeline_id=123,
       deferrable=False,          # Block worker slot
       wait_for_completion=True   # Wait for completion
   )
   ```
   - **Pros**: Simple, no triggerer needed
   - **Cons**: Blocks worker slot, inefficient for long syncs
   - **Use when**: Short syncs (<5 min), development/testing, no triggerer available

3. **Fire-and-Forget**
   ```python
   HevoOperator(
       task_id="trigger_sync",
       pipeline_id=123,
       wait_for_completion=False  # Return job_id immediately
   )
   ```
   - **Pros**: Immediate return, decoupled from monitoring
   - **Cons**: Requires separate sensor for monitoring
   - **Use when**: Need to trigger multiple pipelines, separate monitoring logic, complex dependencies

### HevoSensor

**Purpose**: Monitors Hevo pipeline job completion with optional auto-discovery.

**When to Use**:
- ✅ You triggered a sync with `HevoOperator(wait_for_completion=False)`
- ✅ You want to monitor a sync triggered outside of Airflow
- ✅ You need to wait for a sync but didn't trigger it yourself
- ✅ You want separate trigger and monitor tasks for better control

**Discovery Modes**:

1. **Explicit Job ID** (Most Common)
   ```python
   # Trigger task
   trigger = HevoOperator(
       task_id="trigger_sync",
       pipeline_id=123,
       wait_for_completion=False
   )

   # Sensor task - monitors specific job
   wait = HevoSensor(
       task_id="wait_for_sync",
       pipeline_id=123,
       job_id="{{ ti.xcom_pull(task_ids='trigger_sync') }}",  # XCom from trigger
       deferrable=True
   )

   trigger >> wait
   ```
   - **Pros**: Precise monitoring, works with any job
   - **Cons**: Requires job_id from trigger
   - **Use when**: Following a fire-and-forget operator

2. **Auto-Discovery**
   ```python
   HevoSensor(
       task_id="wait_for_sync",
       pipeline_id=123,
       job_type=JobType.INCREMENTAL,  # Finds active incremental job
       deferrable=True
   )
   ```
   - **Pros**: No job_id needed, monitors externally triggered syncs
   - **Cons**: May find wrong job if multiple active, small discovery delay
   - **Use when**: Monitoring externally triggered syncs, don't have job_id

### Decision Matrix

| Scenario | Recommended Component | Configuration |
|----------|----------------------|---------------|
| **Production pipeline sync** | `HevoOperator` | `deferrable=True, wait_for_completion=True` |
| **Development/testing sync** | `HevoOperator` | `deferrable=False, wait_for_completion=True` |
| **Trigger multiple pipelines, wait for all** | `HevoOperator` + `HevoSensor` | Operator: `wait_for_completion=False`<br>Sensor: Use XCom for job_id |
| **Monitor external sync** | `HevoSensor` | `job_type=JobType.INCREMENTAL` (auto-discovery) |
| **Complex dependencies** | `HevoOperator` + `HevoSensor` | Separate trigger and monitor tasks |
| **Short sync (<5 min)** | `HevoOperator` | `deferrable=False, wait_for_completion=True` |
| **Long sync (>5 min)** | `HevoOperator` | `deferrable=True, wait_for_completion=True` |

### Common Patterns

#### Pattern 1: Simple Sync (Most Common)
```python
# Single task - trigger and wait
sync_task = HevoOperator(
    task_id="sync_customer_data",
    pipeline_id=123,
    deferrable=True,
    wait_for_completion=True
)
```

#### Pattern 2: Trigger Multiple, Wait for All
```python
# Trigger multiple pipelines
trigger_customers = HevoOperator(
    task_id="trigger_customers",
    pipeline_id=123,
    wait_for_completion=False
)

trigger_orders = HevoOperator(
    task_id="trigger_orders",
    pipeline_id=456,
    wait_for_completion=False
)

# Wait for all to complete
wait_customers = HevoSensor(
    task_id="wait_customers",
    pipeline_id=123,
    job_id="{{ ti.xcom_pull(task_ids='trigger_customers') }}",
    deferrable=True
)

wait_orders = HevoSensor(
    task_id="wait_orders",
    pipeline_id=456,
    job_id="{{ ti.xcom_pull(task_ids='trigger_orders') }}",
    deferrable=True
)

# Dependencies
[trigger_customers, trigger_orders] >> [wait_customers, wait_orders]
```

#### Pattern 3: Chain with Downstream Tasks
```python
# Sync data first
sync_data = HevoOperator(
    task_id="sync_hevo_data",
    pipeline_id=123,
    deferrable=True
)

# Then run DBT models
run_dbt = BashOperator(
    task_id="run_dbt_models",
    bash_command="dbt run"
)

sync_data >> run_dbt
```

#### Pattern 4: Optional Sync (Failure Doesn't Block Downstream)
```python
from airflow.utils.trigger_rule import TriggerRule
from datetime import timedelta

# Optional sync with increased timeout
optional_sync = HevoOperator(
    task_id="optional_hevo_sync",
    pipeline_id=123,
    deferrable=True,
    execution_timeout=timedelta(hours=2),  # Increase timeout to 2 hours
    poll_interval=30,  # Check every 30 seconds
)

# These tasks run whether sync succeeds or fails
process_data = PythonOperator(
    task_id="process_data",
    python_callable=lambda: print("Processing with available data..."),
    trigger_rule=TriggerRule.ALL_DONE  # Key: runs after upstream completes
)

generate_report = BashOperator(
    task_id="generate_report",
    bash_command="echo 'Generating report...'",
    trigger_rule=TriggerRule.ALL_DONE  # Runs regardless of sync status
)

# Downstream tasks always run
optional_sync >> process_data >> generate_report
```

**Key Configuration**:
- `execution_timeout=timedelta(hours=2)` - Task timeout (default is very long)
- `trigger_rule=TriggerRule.ALL_DONE` - Run after upstream completes (success OR failure)
- `poll_interval=30` - Reduce API calls by checking less frequently

**Available Trigger Rules**:
- `ALL_DONE` - Run after all upstream complete (success or failure) ⭐
- `NONE_FAILED_MIN_ONE_SUCCESS` - Run if ≥1 succeeded and none failed
- `ALL_SUCCESS` - Run only if all succeeded (default)

**See Also**:
- Simple example: `dags/simple_optional_sync_dag.py`
- Advanced patterns: `dags/optional_sync_example_dag.py`

---

## Configuration Parameters Reference

### HevoOperator Parameters

The `HevoOperator` triggers Hevo pipeline syncs with three execution modes.

#### Required Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `pipeline_id` | `int` | Hevo pipeline ID (must be in INITIALIZED state). Supports Jinja templating. |

#### Optional Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `sync_type` | `SyncType` | `ON_DEMAND` | Type of sync operation. Usually `ON_DEMAND`. |
| `job_type` | `JobType` | `INCREMENTAL` | Job type to wait for: `INCREMENTAL`, `HISTORICAL`, or `TRUNCATE_AND_LOAD`. |
| `connection_id` | `str` | `hevo_default` | Airflow connection ID for Hevo credentials. |
| `poll_interval` | `int` | `5` | Seconds between status checks when waiting. |
| `retry_limit` | `int` | `10` | Max attempts to find active job after triggering. |
| `deferrable` | `bool` | `True` | Release worker slot while waiting (requires triggerer). ⭐ **Recommended** |
| `wait_for_completion` | `bool` | `True` | Wait for completion or return job_id immediately. |
| `accept_completed_with_failures` | `bool` | `False` | Treat partial failures as success. |

#### Example Configurations

**Production (Recommended)**:
```python
HevoOperator(
    task_id="sync_pipeline",
    pipeline_id=123,
    deferrable=True,                      # Resource-efficient
    wait_for_completion=True,             # Wait for completion
    poll_interval=10,                     # Check every 10 seconds
    accept_completed_with_failures=True,  # Allow partial failures
    connection_id="hevo_production"
)
```

**Development/Testing**:
```python
HevoOperator(
    task_id="sync_pipeline",
    pipeline_id=123,
    deferrable=False,  # Simpler, no triggerer needed
    poll_interval=5
)
```

**Fire-and-Forget**:
```python
HevoOperator(
    task_id="trigger_sync",
    pipeline_id=123,
    wait_for_completion=False  # Returns job_id via XCom
)
```

### HevoSensor Parameters

The `HevoSensor` monitors Hevo job completion with auto-discovery.

#### Required Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `pipeline_id` | `int` | Hevo pipeline ID to monitor. Supports Jinja templating. |

#### Optional Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `job_id` | `str` | `None` | Specific job ID to monitor. If `None`, uses auto-discovery. Supports Jinja templating. |
| `job_type` | `JobType` | `INCREMENTAL` | Job type for auto-discovery when `job_id` is not provided. |
| `connection_id` | `str` | `hevo_default` | Airflow connection ID. |
| `poke_interval` | `int` | `5` | Seconds between status checks. |
| `deferrable` | `bool` | `True` | Release worker slot while waiting. ⭐ **Recommended** |
| `accept_completed_with_failures` | `bool` | `False` | Treat partial failures as success. |
| `wait_for_job_max_attempts` | `int` | `10` | Max attempts to find active job (auto-discovery only). |
| `wait_for_job_interval` | `int` | `5` | Seconds between discovery attempts. |
| `wait_for_job_initial_delay` | `int` | `10` | Initial delay before first discovery attempt. |

#### Example Configurations

**With Explicit Job ID** (Most Common):
```python
HevoSensor(
    task_id="wait_for_sync",
    pipeline_id=123,
    job_id="{{ ti.xcom_pull(task_ids='trigger_sync') }}",  # From XCom
    poke_interval=10,
    deferrable=True,
    timeout=3600  # Airflow sensor timeout (1 hour)
)
```

**With Auto-Discovery**:
```python
HevoSensor(
    task_id="wait_historical_sync",
    pipeline_id=456,
    job_type=JobType.HISTORICAL,           # Find historical job
    wait_for_job_initial_delay=15,         # Wait 15s before searching
    wait_for_job_max_attempts=20,          # Try 20 times
    deferrable=True
)
```

### Connection Configuration

Create an Airflow connection with your Hevo credentials:

**Via Environment Variable** (Docker Compose):
```yaml
environment:
  - AIRFLOW_CONN_HEVO_DEFAULT=http://api_username:api_key@us.hevodata.com
```

**Via Airflow UI**:
1. Go to Admin → Connections
2. Create new connection:
   - **Connection Id**: `hevo_default`
   - **Connection Type**: HTTP
   - **Host**: `us.hevodata.com` (or your region: `eu.hevodata.com`, `in.hevodata.com`)
   - **Schema**: `https`
   - **Login**: Your Hevo API username
   - **Password**: Your Hevo API key

**Via CLI**:
```bash
docker exec -it hevo-airflow-3.0 /bin/bash
source /opt/airflow/.venv/bin/activate
airflow connections add hevo_default \
  --conn-type http \
  --conn-host us.hevodata.com \
  --conn-schema https \
  --conn-login your_api_username \
  --conn-password your_api_key
```

### Job Types Reference

| Job Type | Description | When to Use |
|----------|-------------|-------------|
| `INCREMENTAL` | Syncs only new/changed data since last sync | Most common, default for regular syncs |
| `HISTORICAL` | Full historical load of all data | Initial load, backfill scenarios |
| `TRUNCATE_AND_LOAD` | Truncates destination and reloads all data | Data refresh, schema changes |

### Sync Types Reference

| Sync Type | Description |
|-----------|-------------|
| `ON_DEMAND` | Manually triggered sync via API (default) |
| `SCHEDULED` | Scheduled sync (typically not used via Airflow) |

---

## Directory Structure

```
docker/
├── README.md                    # This file
├── airflow-2.4/
│   ├── Dockerfile              # Dockerfile for Airflow 2.4 + Python 3.9
│   ├── docker-compose.yml      # Docker Compose configuration
│   ├── requirements.txt        # Python dependencies for Airflow 2.4
│   ├── start_airflow.sh        # Startup script
│   └── logs/                   # Airflow logs (created at runtime)
└── airflow-3.0/
    ├── Dockerfile              # Dockerfile for Airflow 3.0 + Python 3.13
    ├── docker-compose.yml      # Docker Compose configuration
    ├── requirements.txt        # Python dependencies for Airflow 3.0
    ├── start_airflow.sh        # Startup script
    └── logs/                   # Airflow logs (created at runtime)
```

## Available Example DAGs

The following example DAGs are automatically loaded from the `dags/` directory:

### Production-Ready Examples

1. **`triggerer_example_dag.py`** ⭐ **Recommended**
   - Deferrable operator with wait for completion
   - Best for production environments
   - Resource-efficient (releases worker slot)
   - **Use case**: Standard pipeline sync in production

2. **`sync_sensor_wait_example_dag.py`**
   - Fire-and-forget operator + sensor pattern
   - Separate trigger and monitor tasks
   - **Use case**: Triggering multiple pipelines, complex dependencies

### Development/Testing Examples

3. **`sync_synchronous_wait_example_dag.py`**
   - Synchronous wait (blocks worker)
   - Simpler setup, no triggerer needed
   - **Use case**: Development, short syncs, testing

4. **`sync_no_wait_example_dag.py`**
   - Fire-and-forget only (no waiting)
   - Returns job_id via XCom
   - **Use case**: Trigger and forget, monitor elsewhere

### Error Handling Examples

5. **`simple_optional_sync_dag.py`** ⭐ **Recommended**
   - Simple pattern: downstream tasks run regardless of sync status
   - Uses `trigger_rule=TriggerRule.ALL_DONE`
   - Includes timeout configuration (`execution_timeout`)
   - **Use case**: Optional/supplementary syncs, non-blocking workflows

6. **`optional_sync_example_dag.py`** (Advanced)
   - 6 advanced patterns for complex failure handling scenarios
   - Multiple optional sources, graceful degradation, callbacks
   - **Use case**: Complex multi-source pipelines, quality-based processing

### Integration Examples

6. **`dbt_example_dag.py`**
   - Hevo sync → DBT transformation chain
   - **Use case**: ELT pipelines with transformations

7. **`spark_example_dag.py`**
   - Hevo sync → Spark processing chain
   - **Use case**: Big data processing workflows

### Running Example DAGs

**Step 1: Configure Pipeline IDs**
```bash
# Edit DAG files with your Hevo pipeline IDs
vim dags/triggerer_example_dag.py
# Change: pipeline_id=123  →  pipeline_id=YOUR_PIPELINE_ID
```

**Step 2: Access Airflow UI**
- URL: http://localhost:8080
- Username: `admin`
- Password: `admin`

**Step 3: Enable and Trigger DAG**
1. Toggle the DAG switch to enable it
2. Click "Trigger DAG" button (play icon)
3. Monitor in Graph view or Grid view

**Step 4: View Logs**
- Click on task → View Log
- Check for sync status and any errors

### Real-World Usage Examples

#### Example 1: Daily Sales Data Sync
```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.hevo.operators.hevo_operator import HevoOperator

with DAG(
    dag_id="daily_sales_sync",
    schedule="0 2 * * *",  # 2 AM daily
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)}
) as dag:

    sync_sales = HevoOperator(
        task_id="sync_sales_data",
        pipeline_id=123,  # Your sales pipeline
        deferrable=True,
        wait_for_completion=True,
        accept_completed_with_failures=True,  # Allow partial failures
        poll_interval=30  # Check every 30 seconds
    )
```

#### Example 2: Multi-Pipeline ELT Workflow
```python
from airflow import DAG
from airflow.hevo.operators.hevo_operator import HevoOperator
from airflow.hevo.sensors.hevo_sensor import HevoSensor
from airflow.operators.bash import BashOperator

with DAG(dag_id="multi_source_elt", schedule="@daily") as dag:

    # Trigger multiple source syncs in parallel
    sync_crm = HevoOperator(
        task_id="sync_crm",
        pipeline_id=101,
        wait_for_completion=False  # Fire-and-forget
    )

    sync_payments = HevoOperator(
        task_id="sync_payments",
        pipeline_id=102,
        wait_for_completion=False
    )

    # Wait for all syncs to complete
    wait_crm = HevoSensor(
        task_id="wait_crm",
        pipeline_id=101,
        job_id="{{ ti.xcom_pull(task_ids='sync_crm') }}",
        deferrable=True
    )

    wait_payments = HevoSensor(
        task_id="wait_payments",
        pipeline_id=102,
        job_id="{{ ti.xcom_pull(task_ids='sync_payments') }}",
        deferrable=True
    )

    # Run DBT transformations after all data loaded
    run_dbt = BashOperator(
        task_id="transform_data",
        bash_command="cd /dbt && dbt run --models +customer_analytics"
    )

    # Dependencies
    [sync_crm, sync_payments] >> [wait_crm, wait_payments] >> run_dbt
```

#### Example 3: Conditional Historical Load
```python
from airflow import DAG
from airflow.hevo.operators.hevo_operator import HevoOperator
from airflow.hevo.models.job import JobType
from airflow.operators.python import BranchPythonOperator

def decide_sync_type(**context):
    # Check if historical load is needed
    if context['dag_run'].conf.get('historical'):
        return 'historical_load'
    return 'incremental_load'

with DAG(dag_id="conditional_sync", schedule=None) as dag:

    branch = BranchPythonOperator(
        task_id="decide_load_type",
        python_callable=decide_sync_type
    )

    incremental_load = HevoOperator(
        task_id="incremental_load",
        pipeline_id=123,
        job_type=JobType.INCREMENTAL,
        deferrable=True
    )

    historical_load = HevoOperator(
        task_id="historical_load",
        pipeline_id=123,
        job_type=JobType.HISTORICAL,
        deferrable=True,
        poll_interval=60  # Longer polling for historical
    )

    branch >> [incremental_load, historical_load]
```

#### Example 4: Sync with Retry Logic
```python
from airflow import DAG
from airflow.hevo.operators.hevo_operator import HevoOperator
from datetime import timedelta

with DAG(
    dag_id="resilient_sync",
    schedule="@hourly",
    default_args={
        "retries": 3,  # Airflow-level retries
        "retry_delay": timedelta(minutes=10),
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(hours=1)
    }
) as dag:

    sync_with_retry = HevoOperator(
        task_id="sync_critical_data",
        pipeline_id=123,
        deferrable=True,
        wait_for_completion=True,
        accept_completed_with_failures=False,  # Fail on any errors
        retry_limit=15,  # Job discovery retries
        poll_interval=20
    )
```

#### Example 5: Optional Sync (Simple - Downstream Always Runs)
```python
from airflow import DAG
from airflow.hevo.operators.hevo_operator import HevoOperator
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta

with DAG(
    dag_id="simple_optional_sync",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False
) as dag:

    # Optional Hevo sync with timeout
    # Can fail without blocking downstream tasks
    optional_sync = HevoOperator(
        task_id="optional_hevo_sync",
        pipeline_id=123,  # Replace with your pipeline ID
        deferrable=True,
        execution_timeout=timedelta(hours=2),  # Task timeout: 2 hours
        poll_interval=30,  # Check every 30 seconds (reduces API calls)
        accept_completed_with_failures=True,
    )

    # These tasks ALWAYS run, regardless of sync status
    process_task = BashOperator(
        task_id="process_data",
        bash_command='echo "Processing with available data..."',
        trigger_rule=TriggerRule.ALL_DONE,  # Runs after sync (success OR failure)
    )

    report_task = BashOperator(
        task_id="generate_report",
        bash_command='echo "Report generated"',
        trigger_rule=TriggerRule.ALL_DONE,  # Runs regardless of upstream status
    )

    # Workflow: sync can fail, but process and report always run
    optional_sync >> process_task >> report_task
```

**Key Configuration**:
- ✅ `execution_timeout=timedelta(hours=2)` - Prevents task from running forever
- ✅ `trigger_rule=TriggerRule.ALL_DONE` - Run after upstream finishes (success or failure)
- ✅ `poll_interval=30` - Check every 30s instead of 5s (reduces API calls)
- ✅ No dependencies = tasks run independently

**When to Use**:
- Supplementary/optional data syncs
- Non-critical analytics pipelines
- Monitoring/logging data
- Any sync that shouldn't block your main workflow

**See Also**:
- `dags/simple_optional_sync_dag.py` - Simple, ready-to-use examples
- `dags/optional_sync_example_dag.py` - Advanced patterns (6 variations)

---

## Container Management

### View Container Logs
```bash
# Docker Compose
docker-compose logs -f

# Docker CLI
docker logs -f hevo-airflow-2.4
# or
docker logs -f hevo-airflow-3.0
```

### Stop Container
```bash
# Docker Compose
docker-compose down

# Docker CLI
docker stop hevo-airflow-2.4
# or
docker stop hevo-airflow-3.0
```

### Restart Container
```bash
# Docker Compose
docker-compose restart

# Docker CLI
docker restart hevo-airflow-2.4
# or
docker restart hevo-airflow-3.0
```

### Remove Container and Volumes
```bash
# Docker Compose (removes volumes)
docker-compose down -v

# Docker CLI
docker rm -f hevo-airflow-2.4
docker volume rm airflow-db
# or
docker rm -f hevo-airflow-3.0
docker volume rm airflow-db
```

### Shell Access
```bash
# Docker Compose
docker-compose exec airflow /bin/bash

# Docker CLI
docker exec -it hevo-airflow-2.4 /bin/bash
# or
docker exec -it hevo-airflow-3.0 /bin/bash
```

## Development Workflow

### Live DAG Development

The DAGs directory is mounted as a volume, so changes to DAG files are automatically picked up by Airflow:

1. Edit DAG files in `dags/` directory
2. Wait 30 seconds for Airflow to detect changes
3. Refresh the Airflow UI to see updates

### Testing Local Changes

To test changes to the Hevo provider code:

1. Make changes to the provider code in `src/`
2. Rebuild the Docker image:
   ```bash
   docker-compose up --build
   ```
3. The provider will be reinstalled with your changes

### Debugging

#### View Airflow Logs
```bash
# Container logs
docker-compose logs -f airflow

# Task logs via UI
# Go to http://localhost:8080 → DAG → Task Instance → View Log
```

#### Check Airflow Configuration
```bash
docker exec -it hevo-airflow-2.4 /bin/bash
source /opt/airflow/.venv/bin/activate
airflow config list
```

#### Test Hevo Connection
```bash
docker exec -it hevo-airflow-2.4 /bin/bash
source /opt/airflow/.venv/bin/activate
airflow connections get hevo_default
```

## Advanced Configuration

### Custom Airflow Configuration

You can override Airflow configuration via environment variables in `docker-compose.yml`:

```yaml
environment:
  - AIRFLOW__CORE__PARALLELISM=32
  - AIRFLOW__CORE__MAX_ACTIVE_RUNS_PER_DAG=16
  - AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=False
```

See [Airflow Configuration Reference](https://airflow.apache.org/docs/apache-airflow/stable/configurations-ref.html) for all available options.

### Using PostgreSQL Instead of SQLite

For production-like testing, you can use PostgreSQL:

1. Add PostgreSQL service to `docker-compose.yml`:
```yaml
services:
  postgres:
    image: postgres:15
    environment:
      - POSTGRES_USER=airflow
      - POSTGRES_PASSWORD=airflow
      - POSTGRES_DB=airflow
    volumes:
      - postgres-db:/var/lib/postgresql/data

  airflow:
    # ... existing config
    environment:
      - AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres/airflow
    depends_on:
      - postgres

volumes:
  postgres-db:
```

2. Add `psycopg2` to `requirements.txt`:
```
psycopg2-binary==2.9.9
```

### Resource Limits

To set resource limits:

```yaml
services:
  airflow:
    # ... existing config
    deploy:
      resources:
        limits:
          cpus: '2.0'
          memory: 4G
        reservations:
          cpus: '1.0'
          memory: 2G
```

## Troubleshooting

### Configuration Issues

#### Problem: Hevo Connection Not Found

**Symptoms**:
```
airflow.exceptions.AirflowNotFoundException: The conn_id `hevo_default` isn't defined
```

**Solutions**:
1. Verify connection environment variable in `docker-compose.yml`:
   ```yaml
   environment:
     - AIRFLOW_CONN_HEVO_DEFAULT=http://username:api_key@us.hevodata.com
   ```

2. Check connection in Airflow:
   ```bash
   docker exec -it hevo-airflow-3.0 /bin/bash
   source /opt/airflow/.venv/bin/activate
   airflow connections get hevo_default
   ```

3. Create connection manually via UI or CLI (see Configuration section)

#### Problem: Authentication Failed

**Symptoms**:
```
401 Unauthorized - Invalid API credentials
```

**Solutions**:
1. Verify API credentials are correct:
   - Login = API Username (not email)
   - Password = API Key (not account password)

2. Check region endpoint matches your Hevo account:
   - US: `us.hevodata.com`
   - EU: `eu.hevodata.com`
   - India: `in.hevodata.com`
   - Asia Pacific: `ap.hevodata.com`

3. Test credentials directly:
   ```bash
   curl -u "username:api_key" https://us.hevodata.com/api/v1/pipelines
   ```

#### Problem: Pipeline Not Found or Not INITIALIZED

**Symptoms**:
```
Pipeline 123 is not in INITIALIZED state
Pipeline 456 not found
```

**Solutions**:
1. Verify pipeline ID is correct in your Hevo dashboard
2. Check pipeline status in Hevo UI (must be INITIALIZED)
3. Ensure you have access permissions to the pipeline
4. Use correct connection for the pipeline's region

#### Problem: Job Not Found After Triggering

**Symptoms**:
```
No active job found after 10 attempts
```

**Solutions**:
1. Increase `retry_limit` in operator:
   ```python
   HevoOperator(
       pipeline_id=123,
       retry_limit=20,  # Increased from default 10
       ...
   )
   ```

2. Check if pipeline is already running a job
3. Verify `job_type` matches what the pipeline supports
4. Check Hevo dashboard to confirm sync was triggered

#### Problem: Deferrable Tasks Not Working

**Symptoms**:
```
Task stuck in deferred state
No triggerer service available
```

**Solutions**:
1. Verify triggerer is running in standalone mode (included by default)
2. Check triggerer logs:
   ```bash
   docker-compose logs | grep triggerer
   ```

3. For separate triggerer, ensure it's configured in docker-compose.yml
4. Fallback to synchronous mode if triggerer unavailable:
   ```python
   HevoOperator(
       pipeline_id=123,
       deferrable=False,  # Use synchronous mode
       ...
   )
   ```

#### Problem: Task Timeout

**Symptoms**:
```
Task exceeded timeout of 3600 seconds
```

**Solutions**:
1. Increase sensor timeout:
   ```python
   HevoSensor(
       pipeline_id=123,
       timeout=7200,  # 2 hours
       ...
   )
   ```

2. Adjust poll interval for less frequent checks:
   ```python
   HevoOperator(
       pipeline_id=123,
       poll_interval=30,  # Check every 30 seconds instead of 5
       ...
   )
   ```

3. Check if pipeline is actually stuck in Hevo dashboard

#### Problem: Partial Failures Causing Task Failure

**Symptoms**:
```
Job completed with failures - some records failed
```

**Solutions**:
Accept partial failures if acceptable for your use case:
```python
HevoOperator(
    pipeline_id=123,
    accept_completed_with_failures=True,  # Don't fail on partial failures
    ...
)
```

### Container Issues

#### Container Won't Start

**Check logs:**
```bash
docker-compose logs
```

**Common issues:**
- Port 8080 already in use → Change port in docker-compose.yml
- Insufficient memory → Increase Docker memory allocation (Settings → Resources)
- Build failures → Check system dependencies are installed

#### DAGs Not Appearing

1. Check DAG folder is correctly mounted:
   ```bash
   docker exec -it hevo-airflow-3.0 ls -la /opt/airflow/dags
   ```

2. Check for DAG import errors:
   ```bash
   docker exec -it hevo-airflow-3.0 /bin/bash
   source /opt/airflow/.venv/bin/activate
   airflow dags list-import-errors
   ```

3. Check DAG file syntax:
   ```bash
   docker exec -it hevo-airflow-3.0 /bin/bash
   source /opt/airflow/.venv/bin/activate
   python /opt/airflow/dags/your_dag.py
   ```

#### Performance Issues

1. **Increase worker resources** in docker-compose.yml:
   ```yaml
   deploy:
     resources:
       limits:
         cpus: '2.0'
         memory: 4G
   ```

2. **Use LocalExecutor** (already default) instead of SequentialExecutor

3. **Reduce DAG parsing frequency**:
   ```yaml
   environment:
     - AIRFLOW__SCHEDULER__DAG_DIR_LIST_INTERVAL=300  # 5 minutes
   ```

4. **Increase parallelism**:
   ```yaml
   environment:
     - AIRFLOW__CORE__PARALLELISM=32
     - AIRFLOW__CORE__MAX_ACTIVE_RUNS_PER_DAG=16
   ```

### Provider-Specific Issues

#### Import Errors

**Symptoms**:
```python
ModuleNotFoundError: No module named 'airflow.hevo'
```

**Solutions**:
1. Verify provider is installed:
   ```bash
   docker exec -it hevo-airflow-3.0 /bin/bash
   source /opt/airflow/.venv/bin/activate
   pip list | grep hevo
   ```

2. Rebuild container if provider was updated:
   ```bash
   docker-compose down
   docker-compose up --build
   ```

#### API Rate Limiting

**Symptoms**:
```
429 Too Many Requests
```

**Solutions**:
1. Configure retry with exponential backoff in hook:
   ```python
   from airflow.hevo.hooks.hevo_pipeline_hook import HevoPipelineHook

   hook = HevoPipelineHook(
       pipeline_id=123,
       retry_limit=5,
       retry_delay=5,
       retryable_status_codes=[429, 500, 502, 503]  # Include 429
   )
   ```

2. Increase poll intervals to reduce API calls:
   ```python
   HevoOperator(
       pipeline_id=123,
       poll_interval=60,  # Check every minute instead of 5 seconds
       ...
   )
   ```

3. Contact Hevo support to increase rate limits

### Getting Help

If you're still experiencing issues:

1. **Check Logs**:
   - Container logs: `docker-compose logs -f`
   - Task logs: Airflow UI → Task → View Log
   - Airflow logs: `/opt/airflow/logs/`

2. **Enable Debug Logging**:
   ```yaml
   environment:
     - AIRFLOW__LOGGING__LOGGING_LEVEL=DEBUG
   ```

3. **Test Connection**:
   ```bash
   docker exec -it hevo-airflow-3.0 /bin/bash
   source /opt/airflow/.venv/bin/activate
   python -c "
   from airflow.hevo.hooks.hevo_pipeline_hook import HevoPipelineHook
   hook = HevoPipelineHook(connection_id='hevo_default')
   print(hook.get_connection())
   "
   ```

4. **Check Provider Documentation**:
   - Main README: `../README.md`
   - Configuration: `../CONFIGURATION_PARAMETERS.md`
   - Claude guidance: `../CLAUDE.md`

5. **Report Issues**:
   - Check existing issues on GitHub
   - Provide full error logs and configuration
   - Include Airflow/Python versions

## CI/CD Integration

### GitHub Actions Example

```yaml
name: Test Hevo Provider

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        airflow-version: ['2.4', '3.0']

    steps:
      - uses: actions/checkout@v3

      - name: Build Docker image
        run: |
          docker build -t hevo-airflow-${{ matrix.airflow-version }} \
            -f docker/airflow-${{ matrix.airflow-version }}/Dockerfile .

      - name: Run tests
        run: |
          docker run --rm hevo-airflow-${{ matrix.airflow-version }} \
            /bin/bash -c "source /opt/airflow/.venv/bin/activate && pytest tests/"
```

## Quick Reference

### Common Commands

```bash
# Start Airflow
cd docker/airflow-3.0/
docker-compose up --build

# Stop Airflow
docker-compose down

# View logs
docker-compose logs -f

# Access shell
docker exec -it hevo-airflow-3.0 /bin/bash

# Rebuild after code changes
docker-compose up --build

# Clean restart
docker-compose down -v && docker-compose up --build
```

### Configuration Checklist

- [ ] Docker and Docker Compose installed
- [ ] Hevo API credentials obtained (username + API key)
- [ ] Updated `AIRFLOW_CONN_HEVO_DEFAULT` in `docker-compose.yml`
- [ ] Updated pipeline IDs in example DAG files
- [ ] Allocated at least 4GB RAM to Docker
- [ ] Port 8080 available (or changed in config)

### Recommended Production Settings

```python
HevoOperator(
    task_id="sync_pipeline",
    pipeline_id=YOUR_PIPELINE_ID,
    deferrable=True,                      # Release worker slot ⭐
    wait_for_completion=True,             # Wait for completion
    poll_interval=30,                     # Check every 30 seconds
    accept_completed_with_failures=True,  # Allow partial failures
    retry_limit=15,                       # Job discovery attempts
    connection_id="hevo_production"
)
```

### Key Configuration Parameters

| Parameter | Default | Production Value | Description |
|-----------|---------|------------------|-------------|
| `deferrable` | `True` | `True` | Release worker slot (requires triggerer) |
| `poll_interval` | `5` | `30-60` | Seconds between checks (reduce API calls) |
| `retry_limit` | `10` | `15-20` | Job discovery attempts |
| `accept_completed_with_failures` | `False` | `True` | Allow partial failures |
| `timeout` (sensor) | `604800` | `3600-7200` | Max wait time in seconds |

### When to Use What

| Use Case | Component | Mode |
|----------|-----------|------|
| Production sync | `HevoOperator` | `deferrable=True` |
| Development/testing | `HevoOperator` | `deferrable=False` |
| Multiple parallel syncs | `HevoOperator` + `HevoSensor` | `wait_for_completion=False` |
| Monitor external sync | `HevoSensor` | Auto-discovery mode |
| Short sync (<5 min) | `HevoOperator` | `deferrable=False` |
| Long sync (>5 min) | `HevoOperator` | `deferrable=True` |

---

## Additional Resources

### Documentation

- **[Hevo API Documentation](https://hevo-edge.readme.io/reference)** - Hevo External Orchestration API
- **[Apache Airflow Documentation](https://airflow.apache.org/docs/)** - Official Airflow docs
- **[Docker Documentation](https://docs.docker.com/)** - Docker and Docker Compose
- **[Hevo Airflow Provider README](../README.md)** - Main provider documentation
- **[Configuration Parameters](../CONFIGURATION_PARAMETERS.md)** - Detailed parameter reference
- **[Developer Guide](../CLAUDE.md)** - Architecture and development patterns

### Example DAG Files

All examples are in the `dags/` directory:
- `triggerer_example_dag.py` - Production deferrable example
- `sync_sensor_wait_example_dag.py` - Trigger + monitor pattern
- `sync_synchronous_wait_example_dag.py` - Simple synchronous wait
- `sync_no_wait_example_dag.py` - Fire-and-forget
- `dbt_example_dag.py` - ELT with DBT transformations
- `spark_example_dag.py` - Big data processing

### Community & Support

**For issues related to:**
- **Hevo Airflow Provider**: Open an issue on GitHub
- **Hevo API/Platform**: Contact [Hevo Support](https://hevodata.com/support)
- **Apache Airflow**: Check [Airflow documentation](https://airflow.apache.org/docs/) or [Airflow Slack](https://apache-airflow.slack.com)
- **Docker**: Check [Docker documentation](https://docs.docker.com/) or [Docker forums](https://forums.docker.com)

**Before reporting issues:**
1. Check logs: `docker-compose logs -f`
2. Review troubleshooting section above
3. Test connection: `airflow connections get hevo_default`
4. Verify configuration against examples
5. Include full error messages and configuration in report

---

## License

This Docker setup is provided as-is for development and testing purposes.

## Next Steps

1. ✅ **Set up Docker environment** - You're ready!
2. 📝 **Configure credentials** - Update `docker-compose.yml`
3. 🚀 **Start Airflow** - `docker-compose up --build`
4. 🔧 **Update pipeline IDs** - Edit DAG files with your IDs
5. ▶️ **Run first DAG** - Trigger `triggerer_example_dag`
6. 📊 **Monitor execution** - Check logs and Airflow UI
7. 🏗️ **Build your workflows** - Create custom DAGs for your pipelines

**Happy orchestrating! 🎉**
