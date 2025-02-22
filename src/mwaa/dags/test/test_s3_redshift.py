from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow import DAG
from datetime import datetime, timedelta
from airflow.decorators import task
import json
from typing import List, Dict
from datetime import timezone
from airflow.operators.python import PythonOperator
from airflow.models import Variable

@task(task_id='checked_at_checker')
def checked_at_checker(checked_at:str, **context):
    print("[INFO] EMR transform dag 시작 및 형변환")
    checked_at = context['dag_run'].conf['checked_at']
    kst_checked_at = datetime.strptime(checked_at, "%Y-%m-%dT%H:%M:%S").replace(tzinfo=None) + timedelta(hours=9)  # UTC+9 (KST)
    print(f'크롤링 시작 시간: {checked_at}')
    date = kst_checked_at.date().strftime("%Y-%m-%d")
    hour = str(kst_checked_at.hour)
    minute = str(kst_checked_at.minute)
    
    emr_output_bucket = Variable.get("EMR_OUTPUT_BUCKET")

    # S3 키 경로 설정
    s3_posts_key = f"benz_output/posts/{date}/{hour}/{minute}/posts"
    print(f"[INFO] s3_posts_key: {s3_posts_key}")
    context['task_instance'].xcom_push(key='s3_posts_key', value=s3_posts_key)
    s3_comments_key = f"benz_output/comments/{date}/{hour}/{minute}/comments"
    print(f"[INFO] s3_comments_key: {s3_comments_key}")
    context['task_instance'].xcom_push(key='s3_comments_key', value=s3_comments_key)
    s3_summary_key = f"benz_output/summary/{date}/{hour}/{minute}/summary"
    print(f"[INFO] s3_summary_key: {s3_summary_key}")
    context['task_instance'].xcom_push(key='s3_summary_key', value=s3_summary_key)
    return checked_at

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
    'test_s3_redshift',
    default_args=default_args,
    description='[TEST] Triggered by test_emr_transform dag',
    schedule_interval=None,
    catchup=False
) as dag:
    # 변수 체크
    Variable.get("EMR_OUTPUT_BUCKET")

    # 이전 DAG에서 전달된 데이터 받기
    checked_at = "{{ dag_run.conf['checked_at'] }}"
    # checked_at = "2080-12-30T00:00:00" # UTC 시간
    # 시간 처리 (한국시간 변환)
    checked_at = checked_at_checker(checked_at)    
    
    # Post
    load_to_redshift1 = S3ToRedshiftOperator(
        task_id='load_to_redshift_post',
        schema='public',                # Redshift 스키마
        table='benz_post',                  # 대상 테이블명
        s3_bucket=Variable.get('EMR_OUTPUT_BUCKET'),             # S3 버킷명
        s3_key="{{ task_instance.xcom_pull(task_ids='checked_at_checker', key='s3_posts_key') }}",  # S3 파일 경로
        copy_options=[
            "FORMAT AS PARQUET",
        ],
        aws_conn_id='aws_default',           # AWS Connection ID
        redshift_conn_id='redshift_default', # Redshift Connection ID
        dag=dag
    )
    # Comment
    load_to_redshift2 = S3ToRedshiftOperator(
        task_id='load_to_redshift_comment',
        schema='public',                # Redshift 스키마
        table='benz_comment',                  # 대상 테이블명
        s3_bucket=Variable.get('EMR_OUTPUT_BUCKET'),             # S3 버킷명
        s3_key="{{ task_instance.xcom_pull(task_ids='checked_at_checker', key='s3_comments_key') }}",  # S3 파일 경로,  # S3 파일 경로
        copy_options=[
            "FORMAT AS PARQUET",
        ],
        aws_conn_id='aws_default',           # AWS Connection ID
        redshift_conn_id='redshift_default', # Redshift Connection ID
        dag=dag
    )
    # Summary
    load_to_redshift3 = S3ToRedshiftOperator(
        task_id='load_to_redshift_summary',
        schema='public',                # Redshift 스키마
        table='summary',                  # 대상 테이블명
        s3_bucket=Variable.get('EMR_OUTPUT_BUCKET'),             # S3 버킷명
        s3_key="{{ task_instance.xcom_pull(task_ids='checked_at_checker', key='s3_summary_key') }}",  # S3 파일 경로
        copy_options=[
            "FORMAT AS PARQUET",
        ],
        aws_conn_id='aws_default',           # AWS Connection ID
        redshift_conn_id='redshift_default', # Redshift Connection ID
        dag=dag
    )

    checked_at >> [
            load_to_redshift1, 
            load_to_redshift2,
            load_to_redshift3
        ]