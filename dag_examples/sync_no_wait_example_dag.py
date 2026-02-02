"""
Example DAG demonstrating HevoOperator with wait_for_completion=False.

This DAG:
1. Loads sample data into a MySQL table
2. Triggers HevoOperator without waiting for completion
3. Fetches and logs the latest entry from the MySQL table
"""

import json

from datetime import datetime, timedelta
from typing import Any

from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook

from airflow import DAG
from airflow.hevo.operators import HevoPipelineOperator

# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    "hevo_no_wait_operator_example",
    default_args=default_args,
    description="Example DAG using HevoOperator with wait_for_completion=False",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags={"hevo", "example", "no_wait"},
)


def load_data_to_mysql(**context: Any) -> None:
    """
    Load sample data into the MySQL no_wait_sync_trigger table.

    This function generates sample data and inserts it into the table.
    """

    mysql_hook = MySqlHook(mysql_conn_id="mysql_default")

    # Generate batch_id from DAG run
    batch_id = f"batch_{context['dag_run'].run_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

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
        INSERT INTO no_wait_sync_trigger (batch_id, generated_at, value_int, value_text, payload)
        VALUES (%(batch_id)s, %(generated_at)s, %(value_int)s, %(value_text)s, %(payload)s)
        """
        mysql_hook.run(insert_query, parameters=record)

    return batch_id


# Task 1: Load data to MySQL
load_data_task = PythonOperator(
    task_id="load_data_to_mysql",
    python_callable=load_data_to_mysql,
    dag=dag,
)

# Task 2: Trigger HevoOperator without waiting for completion (returns job_id)
trigger_hevo_task = HevoPipelineOperator(
    task_id="trigger_hevo_sync",
    pipeline_id="{{ var.value.pipeline_id }}",
    connection_id="hevo_airflow_conn_id",
    wait_for_completion=False,  # Key parameter: don't wait for completion
    deferrable=False,
    dag=dag,
)

# Define task dependencies
load_data_task >> trigger_hevo_task
