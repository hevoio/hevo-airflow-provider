# Dockerfile for Airflow 3.0.x
# Uses official Apache Airflow Docker image as base
# For running Hevo Airflow Provider example DAGs
FROM apache/airflow:3.0.6-python3.12

# Set environment variables
ENV AIRFLOW__CORE__LOAD_EXAMPLES=False \
    AIRFLOW__CORE__DAGS_FOLDER=/opt/airflow/dag_examples \
    AIRFLOW__WEBSERVER__EXPOSE_CONFIG=True \
    AIRFLOW__WEBSERVER__WEB_SERVER_PORT=8087 \
    AIRFLOW__API__HOST=0.0.0.0 \
    AIRFLOW__CORE__AUTH_MANAGER=airflow.providers.fab.auth_manager.fab_auth_manager.FabAuthManager \
    _AIRFLOW_WWW_USER_CREATE=true \
    _AIRFLOW_WWW_USER_USERNAME=admin \
    _AIRFLOW_WWW_USER_PASSWORD=admin \
    _AIRFLOW_WWW_USER_ROLE=Admin \
    _AIRFLOW_WWW_USER_FIRSTNAME=Admin \
    _AIRFLOW_WWW_USER_LASTNAME=User \
    _AIRFLOW_WWW_USER_EMAIL=admin@example.com

# Switch to root to install system dependencies
USER root

# Install system dependencies if needed
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    pkg-config \
    default-libmysqlclient-dev \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Switch back to airflow user
USER airflow

# Set working directory
WORKDIR /opt/airflow

# Create directory for Airflow dag examples and ensure proper permissions
RUN mkdir -p /opt/airflow/dag_examples /opt/airflow/logs

# Copy Hevo provider source code
COPY --chown=airflow:airflow . /opt/hevo-airflow-provider

# Copy requirements file
COPY --chown=airflow:airflow docker/airflow-3.0/requirements.txt ./requirements.txt

# Copy entrypoint script
COPY --chown=airflow:airflow docker/airflow-3.0/entrypoint.sh ./entrypoint.sh
RUN chmod +x ./entrypoint.sh

# Install additional dependencies and Hevo provider
RUN pip install --no-cache-dir -r requirements.txt && \
    pip install --no-cache-dir -e /opt/hevo-airflow-provider

# Expose Airflow webserver port
EXPOSE 8087

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD airflow jobs check --job-type SchedulerJob --hostname "$(hostname)"

# Use custom entrypoint to init DB, create user, and start all services
ENTRYPOINT ["/bin/bash", "/opt/airflow/entrypoint.sh"]

