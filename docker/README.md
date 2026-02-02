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
- **Base Image**: `apache/airflow:2.4.3-python3.9`
- **Use Case**: Testing compatibility with Airflow 2.x series (minimum supported version)

### 2. Airflow 3.0.x + Python 3.12
- **Directory**: `docker/airflow-3.0/`
- **Airflow Version**: 3.0.6
- **Python Version**: 3.12
- **Base Image**: `apache/airflow:3.0.6-python3.12`
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

-------
## Troubleshooting

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
   from airflow.hevo.hooks import HevoPipelineHook

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
   from airflow.hevo.hooks import HevoPipelineHook
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
