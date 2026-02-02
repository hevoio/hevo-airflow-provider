# Dockerfile for Airflow 2.4.x
# Uses official Apache Airflow Docker image as base
# For running Hevo Airflow Provider example DAGs
FROM apache/airflow:2.4.3-python3.9

# Set environment variables
ENV AIRFLOW__CORE__LOAD_EXAMPLES=False \
    AIRFLOW__CORE__DAGS_FOLDER=/opt/airflow/dag_examples \
    AIRFLOW__WEBSERVER__EXPOSE_CONFIG=True

# Switch to root to install system dependencies
USER root

# Install system dependencies if needed
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    && rm -rf /var/lib/apt/lists/*

# Switch back to airflow user
USER airflow

# Set working directory
WORKDIR /opt/airflow

# Copy Hevo provider source code
COPY --chown=airflow:airflow ../../src /opt/hevo-airflow-provider/src
COPY --chown=airflow:airflow ../../pyproject.toml /opt/hevo-airflow-provider/pyproject.toml
COPY --chown=airflow:airflow ../../README.md /opt/hevo-airflow-provider/README.md

# Copy requirements file
COPY --chown=airflow:airflow requirements.txt ./requirements.txt

# Install additional dependencies and Hevo provider
RUN pip install --no-cache-dir -r requirements.txt && \
    pip install --no-cache-dir -e /opt/hevo-airflow-provider

# Copy example DAGs
COPY --chown=airflow:airflow ../../dag_examples /opt/airflow/dag_examples

# Initialize Airflow database
RUN airflow db migrate

# Create default admin user
RUN airflow users create \
        --username admin \
        --firstname Admin \
        --lastname User \
        --role Admin \
        --email admin@example.com \
        --password admin

# Expose Airflow webserver port
EXPOSE 8080

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD airflow jobs check --job-type SchedulerJob --hostname "$(hostname)"

# Default command - run Airflow standalone
CMD ["airflow", "standalone"]
