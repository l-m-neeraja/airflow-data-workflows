from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd
import os

OUTPUT_DIR = "/opt/airflow/output"

default_args = {
    "owner": "airflow"
}

dag = DAG(
    dag_id="postgres_to_parquet_export",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="@weekly",
    catchup=False,
)

def check_table_exists_and_has_data():
    hook = PostgresHook(postgres_conn_id="postgres_default")

    records = hook.get_first(
        "SELECT COUNT(*) FROM employees_transformed"
    )

    if records[0] == 0:
        raise ValueError("employees_transformed table is empty")

    return True


def export_to_parquet(**context):
    hook = PostgresHook(postgres_conn_id="postgres_default")
    df = hook.get_pandas_df("SELECT * FROM employees_transformed")

    execution_date = context["ds"]
    file_path = f"{OUTPUT_DIR}/employees_{execution_date}.parquet"

    df.to_parquet(
        file_path,
        engine="pyarrow",
        compression="snappy",
        index=False
    )

    return {
        "file_path": file_path,
        "row_count": len(df),
        "file_size_bytes": os.path.getsize(file_path)
    }


def validate_parquet(**context):
    execution_date = context["ds"]
    file_path = f"{OUTPUT_DIR}/employees_{execution_date}.parquet"

    if not os.path.exists(file_path):
        raise FileNotFoundError("Parquet file not found")

    df = pd.read_parquet(file_path)

    if df.empty:
        raise ValueError("Parquet file is empty")

    return True


check_task = PythonOperator(
    task_id="check_source_table",
    python_callable=check_table_exists_and_has_data,
    dag=dag,
)

export_task = PythonOperator(
    task_id="export_to_parquet",
    python_callable=export_to_parquet,
    provide_context=True,
    dag=dag,
)

validate_task = PythonOperator(
    task_id="validate_parquet_file",
    python_callable=validate_parquet,
    provide_context=True,
    dag=dag,
)

check_task >> export_task >> validate_task
