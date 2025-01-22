"""
Example DAG demonstrating HevoPipelineOperator with RESYNC action.

This DAG:
1. Loads sample data into a MySQL table
2. Triggers a full historical resync of a Hevo pipeline
3. Fetches and logs the latest entry from the warehouse

Full pipeline resync re-ingests all data from the source, useful when data needs to be
reprocessed or after schema changes.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta
from typing import Any

from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from airflow import DAG
from airflow.hevo.models.pipeline import PipelineAction
from airflow.hevo.operators import HevoPipelineOperator

# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


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
            "value_text": "Sample text entry for resync",
            "payload": json.dumps({"key": "value", "number": 42}),
        },
        {
            "batch_id": batch_id,
            "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "value_int": 200,
            "value_text": "Another sample entry for resync",
            "payload": json.dumps({"key": "value2", "number": 84}),
        },
    ]

    # Insert data
    for record in sample_data:
        insert_query = """
        INSERT INTO resync_example_table (batch_id, generated_at, value_int, value_text, payload)
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
    FROM NO_WAIT_AIRFLOW_X_HEVO.resync_example_table
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


# Create the DAG
dag = DAG(
    dag_id="hevo_resync_example",
    default_args=default_args,
    description="Example DAG demonstrating full pipeline resync with wait for completion",
    schedule=None,  # Manually triggered
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["hevo", "example", "resync"],
)

# Task 1: Load data to MySQL
load_data_task = PythonOperator(
    task_id="load_data_to_mysql",
    python_callable=load_data_to_mysql,
    dag=dag,
)

# Task 2: Resync with deferrable wait
resync_deferrable = HevoPipelineOperator(
    task_id="resync_pipeline_deferrable",
    connection_id="hevo_airflow_conn_id",
    pipeline_id="{{ var.value.pipeline_id }}",
    action=PipelineAction.RESYNC,  # Trigger full historical resync
    deferrable=True,  # Release worker slot (requires triggerer)
    wait_for_completion=True,  # Wait for job to complete
    poll_interval=10,  # Check every 10 seconds
    accept_completed_with_failures=True,  # Allow partial failures
    dag=dag,
)

# Task 3: Fetch and log the latest entry from warehouse
fetch_latest_data_from_warehouse = PythonOperator(
    task_id="fetch_and_log_latest_entry",
    python_callable=fetch_and_log_latest_entry_from_warehouse,
    dag=dag,
)

# Define task dependencies
load_data_task >> resync_deferrable >> fetch_latest_data_from_warehouse
