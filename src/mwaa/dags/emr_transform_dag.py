from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow import DAG
from datetime import datetime, timedelta
from airflow.decorators import task
import json
from typing import List, Dict
from datetime import timezone
from airflow.operators.python import PythonOperator
from airflow.models import Variable


def start_dag(checked_at:str):
    print("[INFO] EMR transform dag 시작")
    print(f'크롤링 시작 시간: {checked_at}')

# DAG 기본 설정
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 2, 14),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(seconds=30),  # 빠른 재시도를 위해 30초로 설정
    'execution_timeout': timedelta(minutes=5)  # 5분 제한 설정
}

with DAG(
    'emr_transform_dag',
    default_args=default_args,
    description='Triggered by lambda_to_s3_workflow',
    schedule_interval=None,
    catchup=False
) as dag:
    
    # 이전 DAG에서 전달된 데이터 받기
    # checked_at = "{{ dag_run.conf['checked_at'] }}"
    checked_at = datetime.strptime("2025-02-18T01:00:00", "%Y-%m-%dT%H:%M:%S").strftime("%Y-%m-%dT%H:%M:%S")
    
    start_dag_task = PythonOperator(
        task_id='start_dag',
        python_callable=start_dag,
        op_kwargs={'checked_at': checked_at}
    )

    # EMR 작업에서 사용
    emr_task = EmrAddStepsOperator(
        task_id='spark-job',
        job_flow_id=Variable.get("EMR_JOB_FLOW_ID"),
        aws_conn_id='aws_default',
        wait_for_completion=True,
        do_xcom_push=True, # if True, job_flow_id is pushed to XCom with key job_flow_id.        
        steps=[
            {
                'Name': 'Spark Job',
                'ActionOnFailure': 'CANCEL_AND_WAIT',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': [
                        'spark-submit', 
                        '--deploy-mode', 'cluster',
                        '--conf', 'spark.yarn.maxAppAttempts=4',  # YARN 레벨의 재시도 설정
                        '--conf', 'spark.task.maxFailures=4',     # Spark 태스크 레벨의 재시도 설정
                        '--conf', 'yarn.app.attempt.failure.validity.interval=10s',  # 2분 간격으로 설정
                        's3://transform-emr/EMR.py',
                        '--checked_at', checked_at,
                    ]
                }
            }
        ],
        retry_delay=timedelta(seconds=30),  # 재시도 간격
        retries=2,  # 최대 재시도 횟수
        # 재시도 2*4 = 8회.
    )
    # # EMR의 step이 잘 끝났는지 확인하기
    # step_sensor = EmrStepSensor(
    #     task_id = 'Step_Sensor',
    #     job_flow_id = 'j-HIZRBHTSPUJX',
    #     step_id = {{ task_instance.xcom_pull(task_ids="Spark Job", key="return_value")[0] }}',
    #     aws_conn_id = 'aws_default'
    # )

    s3_sensor = S3KeySensor(
        task_id='s3_sensor',
        bucket_name='transform-emr',
        bucket_key='output.parquet/',
        wildcard_match=True,
        timeout=3*60,  # 3분
        poke_interval=10,  # 1분
    )

    start_dag_task >> emr_task >> s3_sensor