# Docker Setup for Hevo Airflow Provider

This directory contains Docker configurations for running the Hevo Airflow Provider example DAGs with different Airflow and Python versions.

## Table of Contents

1. [Getting Started with Docker](#getting-started-with-docker)
2. [Available Configurations](#available-configurations)
3. [Quick Start](#quick-start)
4. [Configuration](#configuration)
5. [Container Management](#container-management)
6. [Development Workflow](#development-workflow)
7. [Troubleshooting](#troubleshooting)

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
- Your Hevo API credentials (Required):
  - API Username
  - API Key
  - Region endpoint (e.g., us.hevodata.com, eu.hevodata.com)
- MySQL credentials (Optional - if using MySQL-based DAGs):
  - MySQL host endpoint
  - MySQL username and password
  - Database/schema name
- Snowflake credentials (Optional - if using Snowflake-based DAGs):
  - Snowflake account identifier
  - Username and password
  - Warehouse, database, and role details

### 5-Minute Quick Setup

1. **Build the Docker image**:
   ```bash
   # For Airflow 3.0
   docker build -t hevo-airflow-3.0 -f docker/airflow-3.0/Dockerfile .

   # Or for Airflow 2.4
   docker build -t hevo-airflow-2.4 -f docker/airflow-2.4/Dockerfile .
   ```

2. **Start Airflow container**:

   **Standard Mode** (for running DAGs):
   ```bash
   # For Airflow 3.0
   docker run -d  \
      --name hevo-airflow-3.0   \
      -p 8080:8080   \
      -e AIRFLOW__WEBSERVER__WEB_SERVER_HOST=0.0.0.0   \
      -v ./dag_examples:/opt/airflow/dag_examples   \
      hevo-airflow-3.0

   # Or for Airflow 2.4
   docker run -d \
      --name hevo-airflow-2.4   \
      -p 8080:8080   \
      -e AIRFLOW__WEBSERVER__WEB_SERVER_HOST=0.0.0.0   \
      -v ./dag_examples:/opt/airflow/dag_examples   \
      hevo-airflow-2.4
   ```

   **Development Mode** (for modifying provider code - add source mount):
   ```bash
   # For Airflow 3.0 with source code mounted
   docker run -d  \
      --name hevo-airflow-3.0-dev   \
      -p 8080:8080   \
      -e AIRFLOW__WEBSERVER__WEB_SERVER_HOST=0.0.0.0   \
      -v ./src:/opt/hevo-airflow-provider/src:rw   \
      -v ./dag_examples:/opt/airflow/dag_examples   \
      hevo-airflow-3.0

   # Or for Airflow 2.4 with source code mounted
   docker run -d  \
      --name hevo-airflow-2.4-dev   \
      -p 8080:8080   \
      -e AIRFLOW__WEBSERVER__WEB_SERVER_HOST=0.0.0.0   \
      -v ./src:/opt/hevo-airflow-provider/src:rw   \
      -v ./dag_examples:/opt/airflow/dag_examples   \
      hevo-airflow-2.4
   ```

   **Note**: Development mode mounts your local `src/` directory. Changes to hook files (like `base.py`) will be reflected after a simple container restart - no rebuild needed!

3. **Get admin credentials**:
   Watch the container logs to find the auto-generated password:
   ```bash
   # For Airflow 3.0
   docker logs -f hevo-airflow-3.0 | grep -A 5 "admin"

   # For Airflow 2.4
   docker logs -f hevo-airflow-2.4 | grep -A 5 "admin"
   ```

   Look for output like:
   ```
   admin: Password <randomly-generated-password>
   ```

4. **Access Airflow UI**:
   - Open http://localhost:8080
   - Login with the admin username and password from the logs

5. **Configure connections** in Airflow UI (Admin → Connections):

   **a. Hevo Connection** (Required):
   - Click Add → New connection
   - Connection Id: `hevo_airflow_conn_id`
   - Connection Type: `HTTP`
   - Host: `us.hevodata.com` (or your region: `eu.hevodata.com`, `in.hevodata.com`, `ap.hevodata.com`)
   - Schema: `https`
   - Login: Your Hevo API username
   - Password: Your Hevo API key
   - Click Save

   **b. MySQL Connection** (Optional - for MySQL-based DAGs):
   - Click Add → New connection
   - Connection Id: `mysql_default`
   - Connection Type: `MySQL`
   - Host: Your MySQL host endpoint (e.g., `localhost` or `mysql.example.com`)
   - Login: Your MySQL username
   - Password: Your MySQL password
   - Port: `3306` (or your custom port)
   - Schema: `airflow_x_hevo`
   - Click Save

   **c. Snowflake Connection** (Optional - for Snowflake-based DAGs):
   - Click Add → New connection
   - Connection Id: `snowflake_default`
   - Connection Type: `Snowflake`
   - Login: Your Snowflake username
   - Password: Your Snowflake password
   - Extra: Add the following JSON:
     ```json
     {
       "account": "your_account_name",
       "region": "your_region",
       "warehouse": "your_warehouse",
       "database": "your_database"
     }
     ```
   - Click Save

6. **Configure pipeline variable** in Airflow UI (Admin → Variables):
   - Go to Admin → Variables
   - Click "+" to add a new variable
   - Key: `pipeline_id`
   - Value: Your Hevo pipeline ID (e.g., `123`)
   - Click Save

   **Note**: You can find your pipeline ID in the Hevo dashboard URL or pipeline details page.

7. **Run your first DAG**:
   - Enable the `triggerer_example_dag`
   - The DAG will automatically use the `pipeline_id` variable you configured
   - Click "Trigger DAG"

That's it! You're now running Hevo pipelines from Airflow.


### Stop Container
```bash
docker stop hevo-airflow-3.0
# or
docker stop hevo-airflow-2.4
```

### Restart Container
```bash
docker restart hevo-airflow-3.0
# or
docker restart hevo-airflow-2.4
```

### Remove Container and Volumes
```bash
# Stop and remove container
docker stop hevo-airflow-3.0
docker rm hevo-airflow-3.0

# Remove persistent volume
docker volume rm hevo-airflow-data
```
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

## Development Workflow

> 📖 **For detailed development workflows, see [DEVELOPMENT.md](DEVELOPMENT.md)** - Complete guide with examples, troubleshooting, and pro tips!

### TL;DR - Quick Answer

**Question**: *"I changed `base.py` - do I need to rebuild?"*

**Answer**: **NO** - if you started the container in **Development Mode** with source code mounted:
```bash
# Just restart the container
docker restart hevo-airflow-3.0-dev
```

Changes to any file in `src/` will be reflected after restart. No rebuild needed!

If you're using **Standard Mode** (no source mount), then yes, you need to rebuild.

---

### Live DAG Development

The DAGs directory is mounted as a volume, so changes to DAG files are automatically picked up by Airflow:

1. Edit DAG files in `dag_examples/` directory
2. Wait 30 seconds for Airflow to detect changes
3. Refresh the Airflow UI to see updates

### Testing Local Changes to Provider Code

There are two approaches depending on your workflow:

#### Option 1: Development Mode (Recommended for Active Development)

**Mount source code as a volume** - Changes are reflected immediately without rebuilding:

**Step 1: Run container with source code mounted**
```bash
docker run -d \
      --name hevo-airflow-3.0   \
      -p 8080:8080   \
      -e AIRFLOW__WEBSERVER__WEB_SERVER_HOST=0.0.0.0   \
      -v ./src:/opt/hevo-airflow-provider/src:rw   \
      -v ./dag_examples:/opt/airflow/dag_examples   \
      hevo-airflow-3.0
```

**Key change**: Added `-v "$(pwd)/src:/opt/hevo-airflow-provider/src:rw"` to mount your local `src/` directory

**Step 2: Make changes to hook files**
```bash
# Edit any file in src/
vim src/airflow/hevo/hooks/base.py
```

**Step 3: Restart Airflow services** (no rebuild needed!)

**Option A: Restart entire container** (easiest):
```bash
docker restart hevo-airflow-3.0-dev
```

**Option B: Restart Airflow processes only** (faster):
```bash
docker exec hevo-airflow-3.0-dev pkill -f "airflow"
# Airflow standalone will auto-restart
```

**Option C: Restart specific components**:
```bash
# For webserver changes
docker exec hevo-airflow-3.0-dev pkill -f "airflow webserver"

# For scheduler/worker changes (where your hooks run)
docker exec hevo-airflow-3.0-dev pkill -f "airflow scheduler"
```

**Advantages**:
- ✅ No rebuild needed for code changes
- ✅ Instant feedback loop
- ✅ Great for debugging and iterating
- ✅ See changes by just restarting container

**Notes**:
- Python bytecode (`.pyc` files) may be cached - restart container to clear
- The provider is installed in editable mode (`pip install -e`), so changes are reflected immediately
- Works because the source directory is mounted from your host machine

#### Option 2: Production Mode (Rebuild for Each Change)

**Build source code into the image** - More stable but requires rebuild:

**Step 1: Make changes to provider code**
```bash
vim src/airflow/hevo/hooks/base.py
```

**Step 2: Rebuild the Docker image**
```bash
docker build -t hevo-airflow-3.0 -f docker/airflow-3.0/Dockerfile .
```

**Step 3: Stop and remove old container**
```bash
docker stop hevo-airflow-3.0
docker rm hevo-airflow-3.0
```

**Step 4: Start new container**
```bash
docker run -d \
      --name hevo-airflow-3.0   \
      -p 8080:8080   \
      -e AIRFLOW__WEBSERVER__WEB_SERVER_HOST=0.0.0.0   \
      -v ./src:/opt/hevo-airflow-provider/src:rw   \
      -v ./dag_examples:/opt/airflow/dag_examples   \
      hevo-airflow-3.0
```

**Advantages**:
- ✅ Code is baked into image (no external dependencies)
- ✅ Identical to production deployment
- ✅ Can share image without source code

**Use this approach when**:
- Testing final builds before release
- Deploying to production
- Sharing images with others

### Quick Comparison

| Aspect | Development Mode | Production Mode |
|--------|------------------|-----------------|
| **Source code location** | Mounted from host | Built into image |
| **Rebuild required?** | ❌ No | ✅ Yes |
| **Restart to apply changes?** | ✅ Yes (just restart) | ✅ Yes (after rebuild) |
| **Best for** | Active development | Testing/Production |
| **Change feedback** | Seconds | Minutes |

## Troubleshooting

#### Problem: Can't Find Admin Password

**Symptoms**:
```
Don't know the admin password for Airflow UI
```

**Solutions**:

**Search container logs**
```bash
# View all logs and search for admin credentials
docker logs hevo-airflow-3.0 | grep -A 5 "Admin User"
```

**Note**: The credentials are only displayed once during the first container startup.

#### Problem: Container Exits Immediately

**Symptoms**:
```
Container starts but exits immediately
```

**Solutions**:
1. **Check container logs**:
   ```bash
   docker logs hevo-airflow-3.0
   ```

2. **Verify volume paths exist**:
   ```bash
   # Make sure these directories exist
   mkdir -p dag_examples
   mkdir -p docker/airflow-3.0/logs
   ```

3. **Run in foreground for debugging**:
   ```bash
   # Remove -d flag to see output directly
   docker run \
     --name hevo-airflow-3.0-debug \
     # ... rest of flags
     hevo-airflow-3.0
   ```
   
#### Problem: Hevo API Authentication Failed

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
2. Check pipeline status in Hevo UI (must be INITIALIZED for SYNC_NOW action)
3. Ensure you have access permissions to the pipeline
4. Use correct connection for the pipeline's region

#### Problem: MySQL Connection Failed

**Symptoms**:
```
Can't connect to MySQL server on 'host'
Access denied for user 'username'@'host'
```

**Solutions**:
1. **Verify MySQL connection details**:
   ```bash
   docker exec hevo-airflow-3.0 airflow connections get mysql_default
   ```

2. **Test connection from container**:
   ```bash
   docker exec -it hevo-airflow-3.0 /bin/bash
   mysql -h your_mysql_host -u your_username -p -P 3306
   ```

3. **Check MySQL host accessibility**:
   - If MySQL is on `localhost`, use `host.docker.internal` (Mac/Windows) or `172.17.0.1` (Linux) instead
   - Ensure MySQL is configured to accept remote connections
   - Check firewall rules allow connections on port 3306

4. **Verify user permissions**:
   ```sql
   -- Run this on your MySQL server
   GRANT ALL PRIVILEGES ON airflow_x_hevo.* TO 'your_username'@'%';
   FLUSH PRIVILEGES;
   ```

5. **Create schema if it doesn't exist**:
   ```sql
   CREATE DATABASE IF NOT EXISTS airflow_x_hevo;
   ```

#### Problem: Snowflake Connection Failed

**Symptoms**:
```
Failed to connect to DB: account.snowflakecomputing.com:443
250001: Could not connect to Snowflake backend
Invalid username or password
```

**Solutions**:
1. **Verify Snowflake connection**:
   ```bash
   docker exec hevo-airflow-3.0 airflow connections get snowflake_default
   ```

2. **Check account identifier format**:
   - Format: `{account_name}.{region_id}` or `{account_locator}.{cloud_region_id}.{cloud}`
   - Example: `xy12345.us-east-1` or `abc12345.us-east-1.aws`
   - Don't include `.snowflakecomputing.com` in the account field

3. **Verify Extra JSON format**:
   ```json
   {
     "account": "your_account_identifier",
     "region": "us-east-1",
     "warehouse": "COMPUTE_WH",
     "database": "YOUR_DATABASE"
   }
   ```

#### Problem: Provider Module Not Found

**Symptoms**:
```python
ModuleNotFoundError: No module named 'airflow.hevo'
```

**Solutions**:
1. **Verify provider is installed**:
   ```bash
   docker exec hevo-airflow-3.0 pip list | grep hevo
   ```

2. **Rebuild container if provider was updated**:
   ```bash
   docker build -t hevo-airflow-3.0 -f docker/airflow-3.0/Dockerfile .
   docker stop hevo-airflow-3.0
   docker rm hevo-airflow-3.0
   # Then run the container again
   ```

---

#### Problem: Code Changes Not Reflected (Development Mode)

**Symptoms**:
```
Made changes to base.py but they don't appear when running DAGs
Changes to hook files not taking effect
```

**Solutions**:

1. **Verify source code is mounted**:
   ```bash
   docker inspect hevo-airflow-3.0-dev | grep -A 5 "Mounts"
   ```
   Look for `/opt/hevo-airflow-provider/src` in the output

2. **Check you started container in development mode**:
   ```bash
   # Container should have been started with source mount:
   # -v "$(pwd)/src:/opt/hevo-airflow-provider/src:rw"
   ```

3. **Restart container to clear Python cache**:
   ```bash
   docker restart hevo-airflow-3.0-dev
   ```

4. **Or restart Airflow scheduler only** (faster):
   ```bash
   # Changes to hooks/operators require scheduler restart
   docker exec hevo-airflow-3.0-dev pkill -f "airflow scheduler"
   # Wait 5 seconds for auto-restart, then trigger your DAG
   ```

5. **Clear Python bytecode cache**:
   ```bash
   # Remove .pyc files from mounted directory
   find src/ -type f -name "*.pyc" -delete
   find src/ -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true

   # Then restart container
   docker restart hevo-airflow-3.0-dev
   ```

6. **Verify editable install is active**:
   ```bash
   docker exec hevo-airflow-3.0-dev pip show hevo-airflow-provider
   # Look for: Location: /opt/hevo-airflow-provider
   # Look for: Editable project location: /opt/hevo-airflow-provider
   ```

7. **Test your changes**:
   ```bash
   # Access container and import your module
   docker exec -it hevo-airflow-3.0-dev python -c "
   from airflow.hevo.hooks.base import BaseHevoHook
   print('Import successful!')
   print(BaseHevoHook.__file__)
   "
   # Should show: /opt/hevo-airflow-provider/src/airflow/hevo/hooks/base.py
   ```

**Common Mistakes**:
- Started container in Standard Mode instead of Development Mode (missing source mount)
- Changed file but didn't restart Airflow processes
- Edited file outside the container (not in the mounted `src/` directory)
- Python cached the old `.pyc` files

**Quick Development Workflow**:
```bash
# 1. Edit your hook file
vim src/airflow/hevo/hooks/base.py

# 2. Restart container (clears all caches)
docker restart hevo-airflow-3.0-dev

# 3. Wait for container to be healthy (30-60 seconds)
docker logs -f hevo-airflow-3.0-dev

# 4. Test your changes by triggering a DAG
```

---

### Configuration Checklist

#### For All Users
- [ ] Docker Engine installed (20.10+)
- [ ] Hevo API credentials obtained (username + API key)
- [ ] Docker image built successfully

#### Container Startup (Choose One)
- [ ] **Standard Mode**: Container started for running DAGs
- [ ] **Development Mode**: Container started with source mounted (`-v ./src:/opt/hevo-airflow-provider/src:rw`)

#### Post-Startup Configuration
- [ ] Container started with correct user permissions (`--user "$(id -u):0"`)
- [ ] Admin credentials retrieved from container logs
- [ ] **Hevo connection** configured in Airflow UI (Admin → Connections) - **Required**
- [ ] MySQL connection configured (if using MySQL-based DAGs) - Optional
- [ ] Snowflake connection configured (if using Snowflake-based DAGs) - Optional
- [ ] Pipeline IDs updated in example DAG files
- [ ] Port 8080 available (or changed in run command)
- [ ] At least 4GB RAM allocated to Docker

### Helpful Resources

- **Development Guide**: See [DEVELOPMENT.md](DEVELOPMENT.md) for detailed development workflows
- **Airflow Documentation**: https://airflow.apache.org/docs/
- **Hevo API Documentation**: https://hevo-edge.readme.io/reference
- **Provider Documentation**: See main README.md in repository root
- **Docker Documentation**: https://docs.docker.com/
