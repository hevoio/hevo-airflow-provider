#!/bin/bash
set -e

echo "Initializing Airflow database..."
airflow db migrate

echo "Creating admin user..."
airflow users create \
    --username "${_AIRFLOW_WWW_USER_USERNAME:-admin}" \
    --password "${_AIRFLOW_WWW_USER_PASSWORD:-admin}" \
    --firstname "${_AIRFLOW_WWW_USER_FIRSTNAME:-Admin}" \
    --lastname "${_AIRFLOW_WWW_USER_LASTNAME:-User}" \
    --role "${_AIRFLOW_WWW_USER_ROLE:-Admin}" \
    --email "${_AIRFLOW_WWW_USER_EMAIL:-admin@example.com}" 2>/dev/null || echo "User already exists, skipping."

echo "Starting Airflow dag-processor..."
airflow dag-processor &

echo "Starting Airflow scheduler..."
airflow scheduler &

echo "Starting Airflow triggerer..."
airflow triggerer &

echo "Starting Airflow api-server on port 8080..."
exec airflow api-server --port 8080
