
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime

def test_function():
    print("SUCCESS: Python task executed in Cloud Composer!")

with DAG(
    dag_id="demo_test_pipeline",
    start_date=datetime(2026, 10, 17),
    schedule=None,
    catchup=False,
    is_paused_upon_creation=True,
    tags=["demo", "testing"],
) as dag:

    start = EmptyOperator(task_id="start")

    python_task = PythonOperator(
        task_id="test_python",
        python_callable=test_function,
    )

    end = EmptyOperator(task_id="end")

    start >> python_task >> end

