"""
Example DAG demonstrating a simple pipeline:
1. Trigger Hevo sync and wait for completion
2. Run a Spark transformation job
3. Query the transformed table to verify results

"""

from datetime import timedelta, datetime
from typing import Any

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from airflow.hevo.operators.hevo_operator import HevoOperator

# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    'hevo_spark_table_query_example',
    default_args=default_args,
    description='Hevo sync → Spark job → Query table',
    start_date=datetime(2024, 1, 1),
    tags={'hevo', 'example', 'spark', 'snowflake'},
) as dag:
    
    # Step 1: Trigger Hevo sync and wait for completion
    trigger_hevo_sync = HevoOperator(
        task_id="trigger_hevo_sync",
        pipeline_id="{{ var.value.pipeline_id }}",  # Update with your pipeline ID
        connection_id="hevo_airflow_conn_id",
        wait_for_completion=True,  # Wait for sync to complete
        deferrable=False,  # Run in synchronous mode
        dag=dag,
    )

    # Step 2: Run Spark transformation job
    # This processes the data loaded by Hevo
    spark_transform = SparkSubmitOperator(
        task_id='spark_transform_data',
        application='/opt/spark/jobs/transform_data.py', 
        conn_id='spark_default',
        application_args=[
            '--input-table', 'raw.source_table',
            '--output-table', 'transformed.enriched_table',
            '--execution-date', '{{ ds }}'
        ],
        conf={
            'spark.executor.memory': '2g',
            'spark.executor.cores': '2',
            'spark.driver.memory': '1g',
        },
        verbose=True,
        dag=dag,
    )

    # Step 3: Query the transformed table to verify results
    def get_table_data(**context: Any) -> None:
        """
        Simple function to query the transformed table and log the results.
        This demonstrates how to access data after Spark processing.
        """
        snowflake_hook = SnowflakeHook(
            snowflake_conn_id='snowflake_default',
            warehouse='TRANSFORM_WH',
            database='ANALYTICS_DB'
        )
        
        # Simple query to get table data
        execution_date = context['ds']  # Get execution date from context
        query = f"""
        SELECT *
        FROM transformed.enriched_table
        WHERE execution_date = '{execution_date}'::DATE
        ORDER BY created_at DESC
        LIMIT 20
        """
        
        # Execute query and get results
        results = snowflake_hook.get_records(query)
        
        return results

    query_transformed_table = PythonOperator(
        task_id='query_transformed_table',
        python_callable=get_table_data,
        dag=dag,
    )

    # Define the pipeline flow
    trigger_hevo_sync >> spark_transform >> query_transformed_table
