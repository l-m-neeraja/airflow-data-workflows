from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd

def create_employee_table():
    hook = PostgresHook(postgres_conn_id="postgres_default")
    sql = """
    CREATE TABLE IF NOT EXISTS raw_employee_data (
        id INTEGER PRIMARY KEY,
        name VARCHAR(255),
        age INTEGER,
        city VARCHAR(100),
        salary FLOAT,
        join_date DATE
    );
    """
    hook.run(sql)

def truncate_employee_table():
    hook = PostgresHook(postgres_conn_id="postgres_default")
    hook.run("TRUNCATE TABLE raw_employee_data;")

def load_csv_data():
    df = pd.read_csv("/opt/airflow/data/input.csv")
    hook = PostgresHook(postgres_conn_id="postgres_default")
    engine = hook.get_sqlalchemy_engine()
    df.to_sql(
        "raw_employee_data",
        engine,
        if_exists="append",
        index=False
    )
    return len(df)

with DAG(
    dag_id="csv_to_postgres_ingestion",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    description="Ingest employee data from CSV into PostgreSQL",
) as dag:

    create_table = PythonOperator(
        task_id="create_table_if_not_exists",
        python_callable=create_employee_table,
    )

    truncate_table = PythonOperator(
        task_id="truncate_table",
        python_callable=truncate_employee_table,
    )

    load_csv = PythonOperator(
        task_id="load_csv_to_postgres",
        python_callable=load_csv_data,
    )

    create_table >> truncate_table >> load_csv
