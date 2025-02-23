from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def hello_world(checked_at:str):
    print(f"Hello, World! {checked_at}")
    print("Hello, World!")

with DAG(
    'simple_test_dag',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:
    
    # 이전 DAG에서 전달된 데이터 받기
    checked_at = "{{ dag_run.conf['checked_at'] }}"

    task_hello = PythonOperator(
        task_id='hello_task',
        python_callable=hello_world,
        op_kwargs={'checked_at': checked_at}
    )

    start = PythonOperator(
        task_id='start',
        python_callable=lambda: print('Start')
    )

    end = PythonOperator(
        task_id='end',
        python_callable=lambda: print('End')
    )

    start >> task_hello >> end