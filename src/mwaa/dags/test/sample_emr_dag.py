from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from airflow import DAG
from datetime import datetime, timedelta, timezone
from airflow.decorators import task
import json
from typing import List, Dict
from airflow.operators.python import PythonOperator

# DAG 기본 설정
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 2, 14),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(seconds=30),  # 빠른 재시도를 위해 30초로 설정
    'execution_timeout': timedelta(minutes=5),  # 5분 제한 설정
    'dagrun_timeout': timedelta(minutes=5),
}

with DAG(
    'sample_emr_dag',
    default_args=default_args,
    description='sample emr dag',
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=5),
    catchup=False
) as dag:

    checked_at = datetime.strptime("2025-02-18T01:00:00", "%Y-%m-%dT%H:%M:%S")
    
    print_hello = PythonOperator(
        task_id='print_hello',
        python_callable=lambda : print('Hello World'),
    )

    # EMR 작업에서 사용
    emr_task = EmrAddStepsOperator(
        task_id='spark-job',
        job_flow_id='j-PRCL9X5U75MM',
        aws_conn_id='aws_default',
        steps=[
            {
                'Name': 'Spark Job',
                'ActionOnFailure': 'TERMINATE_JOB_FLOW',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': [
                        'spark-submit', '--deploy-mode', 'cluster',
                        's3://transform-emr/EMR.py',
                        '--checked_at', checked_at.strftime('%Y-%m-%dT%H:%M:%S'),
                    ]
                }
            }
        ],
    )

    emr_sensor = EmrStepSensor(
        task_id='emr_sensor',
        job_flow_id='j-PRCL9X5U75MM',
        step_id="{{ task_instance.xcom_pull(task_ids='spark-job', key='return_value')[0] }}",
        aws_conn_id='aws_default',
    )

    print_hello >> emr_task