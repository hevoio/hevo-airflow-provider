# Dockerfile for Airflow 3.0.x with Python 3.13
# For running Hevo Airflow Provider example DAGs
FROM python:3.13-slim

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    AIRFLOW_HOME=/opt/airflow \
    AIRFLOW__CORE__LOAD_EXAMPLES=False \
    AIRFLOW__CORE__DAGS_FOLDER=/opt/airflow/dags \
    AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=sqlite:////opt/airflow/airflow.db \
    AIRFLOW__CORE__EXECUTOR=LocalExecutor \
    AIRFLOW__WEBSERVER__EXPOSE_CONFIG=True \
    # Compatibility fixes
    no_proxy='*' \
    PYTHONFAULTHANDLER=true \
    OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    libpq-dev \
    libmariadb-dev \
    libmariadb-dev-compat \
    pkg-config \
    curl \
    git \
    && rm -rf /var/lib/apt/lists/*

# Install UV package manager for faster dependency installation
RUN curl -LsSf https://astral.sh/uv/install.sh | sh && \
    mv /root/.local/bin/uv /usr/local/bin/ && \
    mv /root/.local/bin/uvx /usr/local/bin/ && \
    chmod +x /usr/local/bin/uv /usr/local/bin/uvx

# Create airflow user and directories
RUN useradd -ms /bin/bash -d /opt/airflow airflow && \
    mkdir -p /opt/airflow/dags /opt/airflow/logs /opt/airflow/plugins && \
    chown -R airflow:airflow /opt/airflow

# Set working directory
WORKDIR /opt/airflow

# Copy requirements file
COPY --chown=airflow:airflow requirements.txt ./requirements.txt

# Copy Hevo provider source code
COPY --chown=airflow:airflow ../../src /opt/hevo-airflow-provider/src
COPY --chown=airflow:airflow ../../pyproject.toml /opt/hevo-airflow-provider/pyproject.toml
COPY --chown=airflow:airflow ../../README.md /opt/hevo-airflow-provider/README.md

# Copy example DAGs
COPY --chown=airflow:airflow ../../dags /opt/airflow/dags

# Copy startup script
COPY --chown=airflow:airflow start_airflow.sh /opt/airflow/start_airflow.sh

# Switch to airflow user
USER airflow

# Create virtual environment and install dependencies
RUN uv venv /opt/airflow/.venv && \
    . /opt/airflow/.venv/bin/activate && \
    uv pip install -r requirements.txt && \
    uv pip install -e /opt/hevo-airflow-provider

# Initialize Airflow database
RUN . /opt/airflow/.venv/bin/activate && \
    airflow db migrate

# Create default admin user
RUN . /opt/airflow/.venv/bin/activate && \
    airflow users create \
        --username admin \
        --firstname Admin \
        --lastname User \
        --role Admin \
        --email admin@example.com \
        --password admin

# Make startup script executable
RUN chmod +x start_airflow.sh

# Expose Airflow webserver port
EXPOSE 8080

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD ["/opt/airflow/.venv/bin/airflow", "jobs", "check", "--job-type", "SchedulerJob", "--hostname", "$(hostname)"]

# Default command - run Airflow standalone
CMD ["/bin/bash", "-c", "source /opt/airflow/.venv/bin/activate && airflow standalone"]
