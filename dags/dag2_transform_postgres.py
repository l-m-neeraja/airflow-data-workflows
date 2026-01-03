from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd

def transform_employee_data():
    hook = PostgresHook(postgres_conn_id="postgres_default")
    df = hook.get_pandas_df("SELECT * FROM raw_employee_data")


    def categorize_salary(salary):
        if salary < 50000:
            return "LOW"
        elif salary <= 80000:
            return "MEDIUM"
        else:
            return "HIGH"

    df["salary_category"] = df["salary"].apply(categorize_salary)

    engine = hook.get_sqlalchemy_engine()
    df.to_sql(
        "employees_transformed",
        engine,
        if_exists="replace",
        index=False
    )

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1)
}

with DAG(
    dag_id="postgres_transformation",
    default_args=default_args,
    schedule_interval=None,
    catchup=False
) as dag:

    transform_task = PythonOperator(
        task_id="transform_employee_data",
        python_callable=transform_employee_data
    )
