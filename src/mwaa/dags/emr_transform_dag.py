from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow import DAG
from datetime import datetime, timedelta
from airflow.decorators import task
from airflow.models import Variable

@task(task_id='checked_at_checker')
def checked_at_checker(**context):
    """
        checked_at 한국 시간 변환
    """
    print("[INFO] EMR transform dag 시작 및 형변환")
    checked_at = context['dag_run'].conf['checked_at']
    context['task_instance'].xcom_push(key='checked_at', value=checked_at)
    print(f'크롤링 시작 시간 (UTC+9 기준): {checked_at}')
    return checked_at

# DAG 기본 설정
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 2, 24),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(seconds=60),  # 빠른 재시도를 위해 30초로 설정
    'execution_timeout': timedelta(minutes=5)  # 5분 제한 설정
}

with DAG(
    'emr_transform_dag',
    default_args=default_args,
    description='Triggered by lambda_to_s3_workflow',
    schedule_interval=None,
    catchup=False
) as dag:
    
    # 변수 체크
    Variable.get("JOB_SCRIPT_PATH")
    Variable.get("EMR_JOB_FLOW_ID")

    # 이전 DAG에서 전달된 데이터 받기 
    # (checked_at: dag_run.conf['checked_at'])
    checked_at_task = checked_at_checker()

    # EMR 작업에서 사용
    emr_task = EmrAddStepsOperator(
        task_id='spark-job',
        job_flow_id=Variable.   get("EMR_JOB_FLOW_ID"),
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
                        "--conf","spark.hadoop.fs.s3a.connection.timeout=120000", # 커넥션 풀에서 나온 timeout. 2분
                        "--conf","spark.hadoop.fs.s3a.socket.timeout=120000", # socket timeout. 2분
                        "--conf","spark.hadoop.fs.s3a.connection.establish.timeout=120000", # 3way handshake. timeout 2분
                        "--conf","spark.hadoop.fs.s3a.connection.acquisition.timeout=60000", # 커넥션 풀에서 커넥션 가져오는 timeout. 60초
                        "--conf","spark.hadoop.fs.s3a.connection.maximum=100", # 커넥션 풀 최대 갯수. 
                        "--conf","spark.hadoop.fs.s3a.connection.idle.time=120000", # 유휴 기본값: 60000ms
                        "--conf","spark.hadoop.fs.s3a.read.timeout=120000", # 읽기 timeout. 2분
                        "--conf","spark.hadoop.fs.s3a.connection.ssl.enabled=true",
                        "--conf","spark.hadoop.fs.s3a.attempts.maximum=1000",
                        "--conf", "spark.hadoop.fs.s3a.block.size=1048576", # 1MB block size
                        Variable.get("JOB_SCRIPT_PATH"), #'s3://transform-emr/emr_realtime_v3.py'
                        '--checked_at', "{{ task_instance.xcom_pull(task_ids='checked_at_checker', key='checked_at') }}",
                    ]
                }
            }
        ],
        retries=0,  # 최대 재시도 횟수
    )

    # spark-submit --deploy-mode cluster --conf spark.yarn.maxAppAttempts=4 --conf spark.task.maxFailures=4 --conf spark.hadoop.fs.s3a.connection.timeout=300000 --conf spark.hadoop.fs.s3a.socket.timeout=300000 --conf spark.hadoop.fs.s3a.connection.establish.timeout=300000 --conf spark.hadoop.fs.s3a.connection.maximum=1000 --conf spark.hadoop.fs.s3a.read.timeout=300000 --conf spark.hadoop.fs.s3a.connection.ssl.enabled=true --conf spark.hadoop.fs.s3a.attempts.maximum=100 --conf spark.hadoop.fs.s3a.block.size=1048576 s3://transform-emr/emr_realtime_v2.py --checked_at 2088-02-10T01:00:03

    trigger_s3_redshift_dag = TriggerDagRunOperator(
    task_id='trigger_s3_redshift_dag',
    trigger_dag_id='s3_redshift',
    conf={
        'from_dag': 'emr_transform_dag',
        'checked_at': "{{ task_instance.xcom_pull(task_ids='checked_at_checker', key='checked_at') }}"
    },
    wait_for_completion = False,
    trigger_rule='all_success'
    )    

    checked_at_task >> emr_task >> trigger_s3_redshift_dag