from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def hello_world():
    print("Hello, World!")

with DAG(
    'simple_test_dag',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:
    task_hello = PythonOperator(
        task_id='hello_task',
        python_callable=hello_world
    )