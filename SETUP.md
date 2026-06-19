# Hevo Airflow Provider - Setup Guide

This guide provides instructions for setting up the Hevo Airflow Provider package in different environments.

## Prerequisites

- Python 3.9+ (for local development)
- Docker (for Docker setup)
- Apache Airflow 2.4.0+ (for production use)

---

## Option 1: Development Setup with UV

### Step 1: Install UV

[uv](https://github.com/astral-sh/uv) is a fast Python package installer and resolver, significantly faster than pip.

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
# or with Homebrew:
brew install uv
```

### Step 2: Clone Repository

```bash
git clone https://github.com/hevoio/hevo-airflow-provider.git
cd hevo-airflow-provider
```

### Step 3: Create Virtual Environment

```bash
uv venv
```

### Step 4: Activate Virtual Environment

```bash
source .venv/bin/activate
```

### Step 5: Install Dependencies

```bash
uv pip install -e ".[dev]"
```

### Step 6: Verify Installation

```bash
# Run tests
uv run pytest

# Run all checks
uv run ruff check && uv run mypy src && uv run pytest
```

### Development Commands

```bash
uv run pytest                 # Run tests
uv run pytest --cov --cov-report=html  # Run tests with coverage report
uv run pytest tests/operators/test_hevo_operator.py  # Run specific test
uv run ruff check src/        # Lint
uv run ruff check --fix src/  # Auto-fix lint issues
uv run ruff format src/       # Format code
uv run mypy src/              # Type check
python -m build               # Build distribution packages
```

---

## Option 2: Development Setup with pip

### Step 1: Clone Repository

```bash
git clone https://github.com/hevoio/hevo-airflow-provider.git
cd hevo-airflow-provider
```

### Step 2: Create Virtual Environment

```bash
python3.9 -m venv venv
source venv/bin/activate  # On macOS/Linux
# venv\Scripts\activate   # On Windows
```

### Step 3: Install Dependencies

```bash
# Install in editable mode with dev dependencies
pip install -e ".[dev]"

# For custom python environment 
pip install -e /path/to/hevo-airflow-provider
```

### Step 4: Verify Installation

```bash
pytest
ruff check src/
```

---

## Option 3: Docker Setup

The provider includes Docker configurations for testing with different Airflow versions.

### Step 1: Choose Airflow Version

Available Docker configurations:
- `docker/airflow-2.4/` - Airflow 2.4
- `docker/airflow-3.0/` - Airflow 3.0

### Step 2: Navigate to Docker Directory

```bash
cd docker/airflow-2.4
# or
cd docker/airflow-3.0
```

### Step 3: Build Docker Image

```bash
docker build -t hevo-airflow-3.0 -f docker/airflow-3.0/Dockerfile .
```

This will:
- Install Apache Airflow (version-specific)
- Install the Hevo provider in editable mode
- Setup Airflow database
- Create admin user

### Step 4: Start Airflow Container

```bash
docker run -d \
      --name hevo-airflow-3.0 \
      -p 8080:8080 \
      -v ./dag_examples:/opt/airflow/dag_examples \
      hevo-airflow-3.0
```

### Step 5: Access Airflow UI

Open your browser and navigate to `http://localhost:8080`

**Default credentials:**
- Username: `admin`
- Password: `admin`

### Step 6: Configure Airflow Variables for Example DAGs

Go to **Admin → Variables** in the Airflow UI and add the following variables.

**Shared variable (used by all Snowflake DAGs):**

| Key | Value |
|-----|-------|
| `snowflake_warehouse` | Your Snowflake warehouse name |

**Per-DAG variables** — set only for the DAGs you intend to run:

| DAG | Key | Value |
|-----|-----|-------|
| `hevo_triggerer_example` | `triggerer_example_pipeline_id` | Your Hevo pipeline ID |
| | `triggerer_example_snowflake_database` | Your Snowflake database |
| | `triggerer_example_snowflake_schema` | Your Snowflake schema |
| `hevo_wait_sync_table_operator_example` | `sync_synchronous_wait_pipeline_id` | Your Hevo pipeline ID |
| | `sync_synchronous_wait_snowflake_database` | Your Snowflake database |
| | `sync_synchronous_wait_snowflake_schema` | Your Snowflake schema |
| `hevo_trigger_sync_with_sensor_wait_example` | `sync_sensor_wait_pipeline_id` | Your Hevo pipeline ID |
| | `sync_sensor_wait_snowflake_database` | Your Snowflake database |
| | `sync_sensor_wait_snowflake_schema` | Your Snowflake schema |
| `hevo_resync_example` | `resync_example_pipeline_id` | Your Hevo pipeline ID |
| | `resync_example_snowflake_database` | Your Snowflake database |
| | `resync_example_snowflake_schema` | Your Snowflake schema |
| `hevo_no_wait_operator_example` | `no_wait_pipeline_id` | Your Hevo pipeline ID |
| `hevo_dbt_example` | `pipeline_id_1` | First Hevo pipeline ID |
| | `pipeline_id_2` | Second Hevo pipeline ID |

### Step 7: View Container Logs (Optional)

```bash
docker logs -f hevo-airflow-3.0
```

### Step 8: Stop Container (When Needed)

```bash
docker stop hevo-airflow-3.0
```

## Next Steps

### For Developers (Contributing to Provider):

1. Read [CLAUDE.md](CLAUDE.md) for architecture and development guidelines
2. Review [CONFIGURATION_PARAMETERS.md](CONFIGURATION_PARAMETERS.md) for parameter details
3. Check existing tests in `tests/` for examples
4. Run all checks before committing changes: `uv run ruff check && uv run mypy src && uv run pytest`

### For Users (Using Provider in DAGs):

1. Configure Hevo connection in Airflow UI (see Option 3, Step 4)
2. Review example DAGs in `dags/` directory
3. Read the [README.md](README.md) for operator and sensor documentation
4. Check [CONFIGURATION_PARAMETERS.md](CONFIGURATION_PARAMETERS.md) for all available parameters

### Creating Your First DAG:

```python
from airflow import DAG
from airflow.hevo.operators import HevoPipelineOperator
from datetime import datetime

with DAG(
        "my_first_hevo_dag",
        start_date=datetime(2024, 1, 1),
        schedule_interval="@daily",
        catchup=False
) as dag:
  sync_pipeline = HevoPipelineOperator(
    task_id="sync_pipeline",
    pipeline_id=123,  # Your Hevo pipeline ID
    deferrable=True,
    wait_for_completion=True
  )
```

---

## Troubleshooting

### Development Setup Issues:

**Problem:** `uv: command not found`
- **Solution:** Install uv using the installation command above, then restart your terminal

**Problem:** Virtual environment activation fails
- **Solution:** Ensure you're in the project directory and run `source .venv/bin/activate`

**Problem:** Tests fail with import errors
- **Solution:** Ensure you installed the package in editable mode: `pip install -e ".[dev]"`

### Production Installation Issues:

**Problem:** `No module named 'airflow.hevo'`
- **Solution:** Reinstall the provider: `pip uninstall apache-airflow-providers-hevo && pip install apache-airflow-providers-hevo`

**Problem:** Provider not showing in `airflow providers list`
- **Solution:** Restart Airflow webserver and scheduler after installation

**Problem:** Connection test fails
- **Solution:** Verify your Hevo API credentials and ensure the host URL is correct for your region

### Docker Setup Issues:

**Problem:** Port 8080 already in use
- **Solution:** Stop the conflicting service or change the port mapping in `docker-compose.yml`: `"8081:8080"`

**Problem:** Container fails to start
- **Solution:** Check logs with `docker-compose logs` and ensure Docker has enough memory allocated (minimum 4GB recommended)

**Problem:** DAGs not appearing in UI
- **Solution:**
  - Check that DAGs are in the mounted directory
  - Verify `AIRFLOW__CORE__DAGS_FOLDER` is set correctly
  - Check DAG file syntax: `docker exec <container> airflow dags list-import-errors`

**Problem:** Changes to provider code not reflected
- **Solution:** The provider is installed in editable mode. Restart the container: `docker-compose restart`

### Common Issues:

**Problem:** `AirflowException: No connection found with connection_id: hevo_default`
- **Solution:** Create the Hevo connection in Airflow UI (see Option 3, Step 4)

**Problem:** API authentication errors
- **Solution:**
  - Verify your API credentials in the connection
  - Check that your API user has appropriate permissions in Hevo
  - Ensure the host matches your Hevo region (us/eu/in.hevodata.com)

**Problem:** Triggerer not running (deferrable tasks stuck)
- **Solution:**
  - Start the triggerer: `airflow triggerer`
  - For Docker: Ensure triggerer service is defined in docker-compose.yml
  - Check triggerer logs: `airflow triggerer --stdout`

**Problem:** `ensure_new_job=True` fails immediately
- **Solution:** This is expected behavior if a job is already running. Either:
  - Wait for the current job to complete
  - Set `ensure_new_job=False` to allow concurrent jobs
  - Check job status in Hevo UI

---

## Additional Resources

- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [Hevo API Documentation](https://hevo-edge.readme.io/reference)
- [UV Package Manager](https://github.com/astral-sh/uv)
- [Project README](README.md)
- [Configuration Parameters](CONFIGURATION_PARAMETERS.md)
- [Development Guide](CLAUDE.md)

---

## Support

For issues and questions:
- **Provider Issues**: Create an issue in the GitHub repository
- **Hevo API Issues**: Contact Hevo support or check API documentation
- **Airflow Issues**: Refer to Apache Airflow documentation

---

## Version Compatibility

- **Python**: 3.9, 3.10, 3.11, 3.12, 3.13
- **Apache Airflow**: 2.4.0+ (requires deferrable support)
- **Tested Airflow Versions**: 2.4.0, 2.8.0, 3.0.0

---

## Clean Up

### Remove Development Environment:

```bash
# UV setup
rm -rf .venv

# pip setup
deactivate
rm -rf venv
```

### Remove Docker Environment:

```bash
cd docker/airflow-2.4  # or airflow-3.0
docker rm hevo-airflow-2.4  # Remove image
```

### Uninstall Provider (Production):

```bash
pip uninstall apache-airflow-providers-hevo
```
