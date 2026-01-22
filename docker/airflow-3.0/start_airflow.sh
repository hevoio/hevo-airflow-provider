#!/bin/bash
# Startup script for Airflow 3.0.x with compatibility fixes

set -e

# Set environment variables to prevent SIGSEGV on macOS
export no_proxy='*'
export PYTHONFAULTHANDLER=true
export OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES

# Set AIRFLOW_HOME
export AIRFLOW_HOME=/opt/airflow

echo "======================================"
echo "Starting Airflow 3.0.x"
echo "======================================"
echo "AIRFLOW_HOME: $AIRFLOW_HOME"
echo "Python version: $(python --version)"
echo "Airflow version: $(airflow version)"
echo ""

# Activate virtual environment
source /opt/airflow/.venv/bin/activate

# Check if database needs initialization
if [ ! -f "$AIRFLOW_HOME/airflow.db" ]; then
    echo "Initializing Airflow database..."
    airflow db migrate
fi

echo ""
echo "======================================"
echo "Airflow webserver will be available at:"
echo "http://localhost:8080"
echo ""
echo "Default credentials:"
echo "Username: admin"
echo "Password: admin"
echo "======================================"
echo ""

# Start Airflow standalone (webserver + scheduler + triggerer)
exec airflow standalone
