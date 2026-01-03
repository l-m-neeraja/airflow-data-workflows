from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime


# -----------------------------
# Notification functions
# -----------------------------
def notify_success():
    print("✅ NOTIFICATION: Data pipeline completed successfully.")


def notify_failure():
    print("❌ NOTIFICATION: Data pipeline failed. Please check logs.")


# -----------------------------
# DAG definition
# -----------------------------
with DAG(
    dag_id="notification_workflow",
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,   # manual trigger
    catchup=False,
    tags=["notification", "monitoring"],
) as dag:

    start = EmptyOperator(
        task_id="start"
    )

    # Simulated main task (represents previous pipeline)
    main_task = PythonOperator(
        task_id="main_pipeline_task",
        python_callable=lambda: print("Running main pipeline task...")
    )

    success_notification = PythonOperator(
        task_id="notify_success",
        python_callable=notify_success,
        trigger_rule=TriggerRule.ALL_SUCCESS
    )

    failure_notification = PythonOperator(
        task_id="notify_failure",
        python_callable=notify_failure,
        trigger_rule=TriggerRule.ONE_FAILED
    )

    end = EmptyOperator(
        task_id="end",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
    )

    # -----------------------------
    # DAG flow
    # -----------------------------
    start >> main_task >> [success_notification, failure_notification] >> end
