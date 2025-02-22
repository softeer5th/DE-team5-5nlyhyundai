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

@task(task_id='check_task_env')
def check_task_env(**context):
    """
    테스트를 위하여 시간을 설정합니다.

    checked_at, start_date, end_date
    """
    print(f"[INFO] ENVIRONMENT: {type(Variable.get('ENVIRONMENT'))}")
    if Variable.get('ENVIRONMENT') == 'PROD':
        print(f"[INFO] PROD 환경에서 실행합니다.")
        checked_at = datetime.now(timezone.utc).replace(tzinfo=None).isoformat(sep='T') + timedelta(hours=9) # UTC+9
        start_date = checked_at - timedelta(days=3) # 3일 전부터
        end_date = checked_at + timedelta(days=1) # 하루 뒤까지 (반드시 오늘까지 포함)
        checked_at = checked_at - timedelta(hours=9) # UTC+0
    else:
        print(f"[INFO] DEV 환경에서 실행합니다.")
        print(f"[INFO] UTC+0기준 checked_at: {Variable.get('checked_at')}")
        checked_at = datetime.strptime(Variable.get('checked_at'), '%Y-%m-%dT%H:%M:%S')
        checked_at = datetime.strftime(checked_at, '%Y-%m-%dT%H:%M:%S')
        start_date = datetime.strptime(Variable.get('start_date'), '%Y-%m-%d')
        start_date = datetime.strftime(start_date, '%Y-%m-%d')
        end_date = datetime.strptime(Variable.get('end_date'), '%Y-%m-%d')
        end_date = datetime.strftime(end_date, '%Y-%m-%d')
    print(f"[INFO] UTC+0 기준 checked_at: {checked_at}")
    print(f"[INFO] UTC+9 기준 start_date: {start_date}")
    print(f"[INFO] UTC+9 기준 end_date: {end_date}")
    context['task_instance'].xcom_push(key='checked_at', value=checked_at)
    context['task_instance'].xcom_push(key='start_date', value=start_date)
    context['task_instance'].xcom_push(key='end_date', value=end_date)

@task
def generate_lambda_search_configs(checked_at, **context) -> List[Dict]:
    """Lambda 함수 설정을 생성
    
    Args:
        context: Airflow context
        
    Returns:
        List[Dict]: Lambda 설정 리스트
    """
    # Connection에서 관리
    def _get_keyword_from_rds():
        """
        RDS에서 키워드 목록을 가져와서 XCom에 저장
    
        postgres_conn 필요

        Returns:
            None
        
        XCom Pushes:
            - key: keywords
            - value: List[str] 키워드 리스트
        """
        pg_hook = PostgresHook(postgres_conn_id='postgres_conn')
        
        keyword_set_name = Variable.get('keyword_set_name')    
        print(f"키워드 세트 이름: {keyword_set_name}")
        with pg_hook.get_conn() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                cursor.execute('''
                            SELECT 
                                keywords 
                            FROM 
                                keyword_set 
                            WHERE 
                                name = %s
                            ''', 
                            (keyword_set_name,)
                        )
                keywords = cursor.fetchone()
                keywords:List[str] = keywords['keywords']
        print(f"키워드 목록: {keywords}")
        context['task_instance'].xcom_push(key='keywords', value=keywords)
        return keywords
    
    keywords = _get_keyword_from_rds()

    lambda_search_configs = []
    function_names_str = Variable.get('LAMBDA_SEARCH_FUNC')
    function_names = function_names_str.split(",")
    # function_names = Variable("LAMBDA_SEARCH_FUNC").split(",")
    
    for keyword in keywords:
        for function_name in function_names:
            lambda_search_configs.append({
                'function_name': function_name,
                'func_id': keyword,
                'payload': json.dumps({
                    'checked_at': checked_at,
                    'keyword': keyword,
                    'start_date': '2025-02-01',
                })
            })
    print(f"lambda_search_configs : {lambda_search_configs}")
    return lambda_search_configs

def invoke_lambda_handler(task_id, **context):
    task_instance = context['task_instance']
    response = task_instance.xcom_pull(task_ids='lambda_task')
    if response.json().get('status_code') == '403':
        raise AirflowException(f"Lambda {task_id} returned 403 Forbidden") 

@task
def invoke_lambda(config, invocation_type='RequestResponse', **context):
    task_id = f"invoke_lambda_{config['function_name']}_{config.get('func_id', '')}".replace(' ', '_')
    try:
        operator = LambdaInvokeFunctionOperator(
            task_id=task_id,
            function_name=config['function_name'],
            payload=config['payload'],
            retries=2,
            invocation_type=invocation_type,
            on_execution_callback=lambda context: print(f"✅ Lambda {task_id} invoked"),
            retry_delay=timedelta(seconds=5),
            execution_timeout=timedelta(minutes=15),
            on_execute_callback=invoke_lambda_handler,
            # https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/operators/lambda.html
            # aws client 설정.
            botocore_config={
                'connect_timeout': 900,
                'read_timeout': 900,
                'tcp_keepalive': True,
                'max_pool_connections': 100, # default 10
                'retries': {
                    'max_attempts': 3,
                    'total_max_attempts': 100,
                    'mode': 'standard',
                }
            },
            dag=dag
        )
        return operator.execute(context=get_current_context())
    except Exception as e:
        print(f"Error invoking Lambda: {e}")
        raise  # Re-raise the exception to mark the task as failed

@task
def generate_lambda_detail_configs() -> List[dict]:
    """
    Lambda 함수 중 detail을 설정을 생성하는 함수
    
    xcom: details_count:Dict
    """
    lambda_detail_configs = []
    function_names_str = Variable.get('LAMBDA_DETAIL_FUNC')
    function_names = function_names_str.split(",")
    # function_names = Variable("LAMBDA_DETAIL_FUNC").split(",")
    # total_count = sum(details_count.values())
    # for func_name, count in zip(details_count.values()): # dict 순서가 보장되기 때문에 zip 사용
    #     for _ in range(max(count // 10, 1)): # 최대 10개 정도씩 처리 유도.
    #         lambda_configs.append({
    #             'function_name': func_name,
    #             'payload': json.dumps({})
    #         })
    for idx, function_name in enumerate(function_names):
        lambda_detail_configs.append({
            'function_name': function_name,
            'func_id': idx,
            'payload': json.dumps({})
        })
    return lambda_detail_configs

with DAG(
    'lambda_to_s3_workflow',
    default_args=default_args,
    description='Trigger Lambda functions and Load data to S3',
    schedule_interval=timedelta(minutes=15),
    catchup=False
) as dag:
    
    checked_at = datetime.fromisoformat(datetime.now(timezone.utc).isoformat(sep='T'))
    checked_at = datetime.strftime(checked_at, '%Y-%m-%dT%H:%M:%S')
    # RDS에서 키워드 가져오기
    lambda_search_configs = generate_lambda_search_configs(checked_at)
    # Dynamic task mapping!
    search_lambda_tasks = invoke_lambda.partial(invocation_type='Event').expand(config=lambda_search_configs)
    
    lambda_detail_configs = generate_lambda_detail_configs()

    detail_lambda_tasks = invoke_lambda.partial(invocation_type='RequestResponse').expand(config=lambda_detail_configs)

    trigger_second_dag = TriggerDagRunOperator(
        task_id='trigger_emr_dag',
        trigger_dag_id='emr_transform_dag',
        conf={
            'from_dag': 'lambda_to_s3_workflow',
            'checked_at': checked_at
        },
        wait_for_completion = False,
    )

    lambda_search_configs >> search_lambda_tasks 
    search_lambda_tasks >> lambda_detail_configs >> detail_lambda_tasks
    detail_lambda_tasks >> trigger_second_dag


    