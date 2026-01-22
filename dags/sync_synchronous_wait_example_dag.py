"""
Example DAG demonstrating HevoOperator with wait_for_completion=True and deferrable=True.

This DAG:
1. Loads sample data into a MySQL table
2. Triggers HevoOperator while waiting for completion using deferrable mode
3. Fetches and logs the latest entry from the MySQL table
"""

from datetime import datetime, timedelta
from typing import Any

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from airflow.hevo.operators.hevo_operator import HevoOperator

# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    "hevo_wait_sync_table_operator_example",
    default_args=default_args,
    description="Example DAG using HevoOperator with wait_for_completion=True and deferrable=True",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags={"hevo", "example", "triggerer"},
)


def load_data_to_mysql(**context: Any) -> None:
    """
    Load sample data into the MySQL triggerer_example_table table.

    This function generates sample data and inserts it into the table.
    """
    import json
    from datetime import datetime

    mysql_hook = MySqlHook(mysql_conn_id="mysql_default")

    # Generate batch_id from DAG run
    batch_id = f"batch_{context['dag_run'].run_id.replace('-', '_')}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    # Sample data to insert
    sample_data = [
        {
            "batch_id": batch_id,
            "generated_at": datetime.now().strftime("%Y_%m_%d %H:%M:%S"),
            "value_int": 100,
            "value_text": "Sample text entry",
            "payload": json.dumps({"key": "value", "number": 42}),
        },
        {
            "batch_id": batch_id,
            "generated_at": datetime.now().strftime("%Y_%m_%d %H:%M:%S"),
            "value_int": 200,
            "value_text": "Another sample entry",
            "payload": json.dumps({"key": "value2", "number": 84}),
        },
    ]

    # Insert data
    for record in sample_data:
        insert_query = """
        INSERT INTO wait_sync_table_operator (batch_id, generated_at, value_int, value_text, payload)
        VALUES (%(batch_id)s, %(generated_at)s, %(value_int)s, %(value_text)s, %(payload)s)
        """
        mysql_hook.run(insert_query, parameters=record)

    print(f"Successfully loaded {len(sample_data)} records with batch_id: {batch_id}")
    return batch_id


def fetch_and_log_latest_entry_from_warehouse(**context: Any) -> None:
    """
    Fetch the latest entry for the current batch_id from Snowflake and log it.
    """
    batch_id = context["ti"].xcom_pull(task_ids="load_data_to_mysql")
    if not batch_id:
        print("batch_id not found in XCom; skipping Snowflake fetch")
        return

    snowflake_hook = SnowflakeHook(snowflake_conn_id="snowflake_default_latest", warehouse="HOGWARTS", database="RON")


    query = """
    SELECT id, batch_id, generated_at, value_int, value_text, payload
    FROM NO_WAIT_AIRFLOW_X_HEVO.wait_sync_table_operator
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
        print("=" * 50)
        print(f"Latest Snowflake entry for batch_id={batch_id}:")
        print("=" * 50)
        for key, value in latest_entry.items():
            print(f"{key}: {value}")
        print("=" * 50)
    else:
        print(f"No Snowflake entries found for batch_id={batch_id}")


# Task 1: Load data to MySQL
load_data_task = PythonOperator(
    task_id="load_data_to_mysql",
    python_callable=load_data_to_mysql,
    dag=dag,
)

# Task 2: Trigger HevoOperator waiting for completion in deferrable mode
trigger_hevo_task = HevoOperator(
    task_id="trigger_hevo_sync",
    pipeline_id="{{ var.value.pipeline_id }}",
    connection_id="hevo_airflow_conn_id",
    wait_for_completion=True,  # wait for completion
    deferrable=False,  # run in synchronous mode
    dag=dag,
)

# Task 3: Fetch and log the latest entry
fetch_latest_data_from_warehouse = PythonOperator(
    task_id="fetch_and_log_latest_entry",
    python_callable=fetch_and_log_latest_entry_from_warehouse,
    dag=dag,
)

# Define task dependencies
load_data_task >> trigger_hevo_task >> fetch_latest_data_from_warehouse
