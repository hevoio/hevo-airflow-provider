"""
Example DAG demonstrating HevoOperator with wait_for_completion=False and HevoSensor.

This DAG:
1. Loads sample data into a MySQL table
2. Triggers HevoOperator without waiting for completion (returns job_id)
3. Uses HevoSensor to wait for the job to complete
4. Fetches and logs the latest entry from the warehouse
"""

import json
from datetime import datetime, timedelta
from typing import Any

from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from airflow import DAG
from airflow.hevo.operators import HevoPipelineOperator
from airflow.hevo.sensor import HevoSensor

# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    "hevo_trigger_sync_with_sensor_wait_example",
    default_args=default_args,
    description="Example DAG using HevoOperator with wait_for_completion=False and HevoSensor",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags={"hevo", "example", "sensor", "no_wait_operator"},
)


def load_data_to_mysql(**context: Any) -> None:
    """
    Load sample data into the MySQL table.

    This function generates sample data and inserts it into the table.
    """
    mysql_hook = MySqlHook(mysql_conn_id="mysql_default")

    # Generate batch_id from DAG run
    batch_id = f"batch_{context['dag_run'].run_id.replace('-', '_')}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    # Sample data to insert
    sample_data = [
        {
            "batch_id": batch_id,
            "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "value_int": 100,
            "value_text": "Sample text entry",
            "payload": json.dumps({"key": "value", "number": 42}),
        },
        {
            "batch_id": batch_id,
            "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "value_int": 200,
            "value_text": "Another sample entry",
            "payload": json.dumps({"key": "value2", "number": 84}),
        },
    ]

    # Insert data
    for record in sample_data:
        insert_query = """
        INSERT INTO sensor_wait_example_table (batch_id, generated_at, value_int, value_text, payload)
        VALUES (%(batch_id)s, %(generated_at)s, %(value_int)s, %(value_text)s, %(payload)s)
        """
        mysql_hook.run(insert_query, parameters=record)

    return batch_id


def fetch_and_log_latest_entry_from_warehouse(**context: Any) -> None:
    """Fetch the latest entry for the current batch_id from Snowflake and log it."""
    batch_id = context["ti"].xcom_pull(task_ids="load_data_to_mysql")
    if not batch_id:
        return

    snowflake_hook = SnowflakeHook(snowflake_conn_id="snowflake_default", warehouse="HOGWARTS", database="RON")

    query = """
    SELECT id, batch_id, generated_at, value_int, value_text, payload
    FROM NO_WAIT_AIRFLOW_X_HEVO.sensor_wait_example_table
    WHERE batch_id = %s
    ORDER BY id DESC
    LIMIT 1;
    """

    result = snowflake_hook.get_first(query, parameters=[batch_id])

    if result:
        latest_entry = {
            "id": result[0],
            "batch_id": result[1],
            "generated_at": result[2],
            "value_int": result[3],
            "value_text": result[4],
            "payload": result[5],
        }
        for _key, _value in latest_entry.items():
            pass
    else:
        pass


# Task 1: Load data to MySQL
load_data_task = PythonOperator(
    task_id="load_data_to_mysql",
    python_callable=load_data_to_mysql,
    dag=dag,
)

# Task 2: Trigger HevoOperator without waiting for completion
# This will return the job_id via XCom
trigger_hevo_task = HevoPipelineOperator(
    task_id="trigger_hevo_sync",
    pipeline_id="{{ var.value.pipeline_id }}",  # Update with your pipeline ID
    connection_id="hevo_airflow_conn_id",
    wait_for_completion=False,  # Key parameter: don't wait for completion, returns job_id
    deferrable=False,
    dag=dag,
)

# Task 3: Use HevoSensor to wait for the job to complete
# The sensor uses the job_id from the operator via XCom templating
wait_for_job_sensor = HevoSensor(
    task_id="wait_for_job_completion",
    pipeline_id="{{ var.value.pipeline_id }}",  # Update with your pipeline ID
    connection_id="hevo_airflow_conn_id",
    job_id="{{ ti.xcom_pull(task_ids='trigger_hevo_sync') }}",  # Get job_id from operator via XCom
    deferrable=True,  # Use deferrable mode for async execution
    poke_interval=5,
    accept_completed_with_failures=False,
    dag=dag,
)

# Task 4: Fetch and log the latest entry from warehouse
fetch_latest_data_from_warehouse = PythonOperator(
    task_id="fetch_and_log_latest_entry",
    python_callable=fetch_and_log_latest_entry_from_warehouse,
    dag=dag,
)

load_data_task >> trigger_hevo_task >> wait_for_job_sensor >> fetch_latest_data_from_warehouse
