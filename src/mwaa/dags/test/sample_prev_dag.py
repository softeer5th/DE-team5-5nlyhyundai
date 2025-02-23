from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow import DAG
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.exceptions import AirflowException
from airflow.operators.python import get_current_context
import psycopg2.extras
from airflow.decorators import task
import json
from typing import List, Dict
from datetime import timezone
import math

# DAG 기본 설정
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 2, 14),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=30),  # 빠른 재시도를 위해 30초로 설정
    'execution_timeout': timedelta(minutes=15)  # 5분 제한 설정
}

def print_hello(**context):
    checked_at = Variable.get("checked_at")
    print(f'Hello, Airflow! {checked_at}')
    context['task_instance'].xcom_push(key='checked_at', value=checked_at)


with DAG(
    '2sample_prev_dag',
    default_args=default_args,
    description='Trigger Lambda functions and Load data to S3',
    schedule_interval=None,
    catchup=False
) as dag:
    
    hello_task = PythonOperator(
        task_id='hello',
        python_callable=print_hello,
        dag=dag,
    )
    

    trigger_second_dag = TriggerDagRunOperator(
        task_id='simple_test_dag',
        trigger_dag_id='simple_test_dag',
        conf={
            'from_dag': 'sample_prev_dag',
            'checked_at': "{{ task_instance.xcom_pull(task_ids='hello', key='checked_at') }}"
        },
        wait_for_completion = False,
        trigger_rule='all_success'
    )

    hello_task >> trigger_second_dag


    