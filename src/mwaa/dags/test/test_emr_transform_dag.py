from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
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

@task(task_id='checked_at_checker')
def checked_at_checker(**context):
    """
        checked_at 한국 시간 변환
    """
    print("[INFO] EMR transform dag 시작 및 형변환")
    checked_at = context['dag_run'].conf['checked_at']
    context['task_instance'].xcom_push(key='checked_at', value=checked_at)
    print(f'크롤링 시작 시간 (KST 기준): {checked_at}')
    return checked_at

# DAG 기본 설정
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 2, 24),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(seconds=30),  # 빠른 재시도를 위해 30초로 설정
    'execution_timeout': timedelta(minutes=5)  # 5분 제한 설정
}

with DAG(
    'test_emr_transform_dag',
    default_args=default_args,
    description='[TEST] Triggered by lambda_to_s3_workflow',
    schedule_interval=None,
    catchup=False
) as dag:
    # JOB_SCRIPT_PATH 설정. (s3://transform-emr/EMR.py)

    # 시간 처리
    # 이전 DAG에서 전달된 데이터 받기 (checked_at: dag_run.conf['checked_at']) 
    checked_at_task = checked_at_checker() # 이후, crawling_metadata를 확인하여 dag 의존성 감소로 진행.

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
                'ActionOnFailure': 'CONTINUE',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': [
                        'spark-submit', 
                        '--deploy-mode', 'cluster',
                        '--conf', 'spark.yarn.maxAppAttempts=4',  # YARN 레벨의 재시도 설정
                        '--conf', 'spark.task.maxFailures=4',     # Spark 태스크 레벨의 재시도 설정 (2 이gk이면 에러코드 13)
                                    # spark.excludeOnFailure.task.maxTaskAttemptsPerNode보다 spark.task.maxFailures=4 무조건 커야 함.
                        "--conf","spark.hadoop.fs.s3a.connection.timeout=600000",
                        "--conf","spark.hadoop.fs.s3a.socket.timeout=600000",
                        "--conf","spark.hadoop.fs.s3a.connection.establish.timeout=600000",
                        "--conf","spark.hadoop.fs.s3a.connection.maximum=10000",
                        "--conf","spark.hadoop.fs.s3a.read.timeout=600000",
                        "--conf","spark.hadoop.fs.s3a.connection.ssl.enabled=true",
                        "--conf","spark.hadoop.fs.s3a.attempts.maximum=100",
                        Variable.get("JOB_SCRIPT_PATH"), #'s3://transform-emr/EMR-2.py'
                        '--checked_at', "{{ task_instance.xcom_pull(task_ids='checked_at_checker', key='checked_at') }}",
                    ]
                }
            }
        ],
        # "--conf","spark.driver.memory=2g",
        # "--conf","spark.executor.memory=g",
        # "--conf","spark.executor.cores=2",               
    )
    #  fs.s3a.block.size; default value “32M” for 32 Megabytes. (더 줄일 수 있나..?)


    
    trigger_s3_redshift_dag = TriggerDagRunOperator(
    task_id='test_trigger_s3_redshift_dag',
    trigger_dag_id='test_s3_redshift',
    conf={
        'from_dag': 'test_emr_transform_dag',
        'checked_at': "{{ task_instance.xcom_pull(task_ids='checked_at_checker', key='checked_at') }}"
    },
    wait_for_completion = False,
    trigger_rule='all_success'
    )

    checked_at_task >> emr_task >> trigger_s3_redshift_dag

