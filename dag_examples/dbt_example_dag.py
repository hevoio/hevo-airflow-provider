"""
Example DAG demonstrating:
1. Two HevoOperator tasks triggering Hevo syncs with different pipeline IDs
2. DBT execution to transform the synced data.

This DAG shows a typical workflow:
- Load data from multiple sources via Hevo pipelines
- Run dbt transformations on the synced data
"""

from datetime import datetime, timedelta

from airflow.operators.bash import BashOperator

from airflow import DAG
from airflow.hevo.operators import HevoPipelineOperator

# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "hevo_dbt_example",
    default_args=default_args,
    description="Two Hevo syncs with different pipeline IDs → DBT transformation",
    start_date=datetime(2024, 1, 1),
    tags={"hevo", "example", "dbt", "transformation"},
) as dag:
    # Task 1: Trigger Hevo sync for Pipeline 1 using HevoOperator
    trigger_pipeline_1 = HevoPipelineOperator(
        task_id="trigger_hevo_pipeline_1",
        pipeline_id="{{ var.value.pipeline_id_1 }}",  # First pipeline ID
        connection_id="hevo_airflow_conn_id",
        wait_for_completion=True,
        deferrable=False,
        dag=dag,
    )

    # Task 2: Trigger Hevo sync for Pipeline 2 using HevoOperator
    trigger_pipeline_2 = HevoPipelineOperator(
        task_id="trigger_hevo_pipeline_2",
        pipeline_id="{{ var.value.pipeline_id_2 }}",  # Second pipeline ID
        connection_id="hevo_airflow_conn_id",
        wait_for_completion=True,
        deferrable=False,
        dag=dag,
    )

    # Task 3: Run dbt transformations
    # This assumes dbt is installed and configured in your environment
    run_dbt_models = BashOperator(
        task_id="dbt_transformations",
        bash_command="""
        cd dbt_transformations && \
        dbt run --profiles-dir ~/.dbt --select models/staging/* models/marts/*
        """,
        dag=dag,
    )

    # Define the pipeline flow
    # Both pipelines can run in parallel, then dbt runs after both complete
    [trigger_pipeline_1, trigger_pipeline_2] >> run_dbt_models
