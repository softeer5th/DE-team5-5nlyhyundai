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
    'start_date': datetime(2025, 2, 23),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(seconds=30),  # 빠른 재시도를 위해 30초로 설정
    'execution_timeout': timedelta(minutes=5)  # 10분 제한 설정
}

@task(task_id='set_environment')
def set_environment(**context):
    """
    테스트를 위하여 시간을 설정합니다.

    """
    print(f"[INFO] ENVIRONMENT: {type(Variable.get('ENVIRONMENT'))}")
    print(f"[INFO] UTC+9기준 checked_at: {Variable.get('checked_at')}")
    checked_at = datetime.strptime(Variable.get('checked_at'), '%Y-%m-%dT%H:%M:%S')
    checked_at = datetime.strftime(checked_at, '%Y-%m-%dT%H:%M:%S')
    start_date = datetime.strptime(Variable.get('start_date'), '%Y-%m-%d')
    start_date = datetime.strftime(start_date, '%Y-%m-%d')
    end_date = datetime.strptime(Variable.get('end_date'), '%Y-%m-%d')
    end_date = datetime.strftime(end_date, '%Y-%m-%d')
    print(f"[INFO] UTC+9 기준 checked_at: {checked_at}")
    print(f"[INFO] UTC+9 기준 start_date: {start_date}")
    print(f"[INFO] UTC+9 기준 end_date: {end_date}")
    context['task_instance'].xcom_push(key='checked_at', value=checked_at)
    context['task_instance'].xcom_push(key='start_date', value=start_date)
    context['task_instance'].xcom_push(key='end_date', value=end_date)

@task(trigger_rule='all_success')
def generate_lambda_search_configs(**context) -> List[Dict]:
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
        return keywords
    
    keywords = _get_keyword_from_rds()

    lambda_search_configs = []
    checked_at = context['task_instance'].xcom_pull(task_ids='set_environment', key='checked_at')
    start_date = context['task_instance'].xcom_pull(task_ids='set_environment', key='start_date')
    end_date = context['task_instance'].xcom_pull(task_ids='set_environment', key='end_date')

    function_names_str = Variable.get('TEST_LAMBDA_SEARCH_FUNC')
    function_names = function_names_str.split(",")
    print(f"[INFO] 함수들 이름: {function_names}")
    
    for keyword in keywords:
        for function_name in function_names:
            lambda_search_configs.append({
                'function_name': function_name,
                'task_id': f"invoke_lambda_{function_name}_{keyword}".replace(' ', '_'),
                'payload': json.dumps({
                    'checked_at': checked_at,
                    'keyword': keyword,
                    'start_date': start_date,
                    'end_date': end_date,
                })
            })
            # clien은 한번 더.
            if 'clien' in function_name.lower():
                lambda_search_configs.append({
                    'function_name': function_name,
                    'task_id': f"invoke_lambda_{function_name}_all".replace(' ', '_'),
                    'payload': json.dumps({
                        'checked_at': checked_at,
                        'keyword': keyword,
                        'start_date': start_date,
                        'end_date': end_date,
                    })
                })
    
    print(f"총 search 람다 갯수 : {len(lambda_search_configs)}")
    return lambda_search_configs

# def invoke_lambda_handler(config, context):
#     task_id = config['task_id'] # config 사용
#     response = context['ti'].xcom_pull(task_ids=task_id, key='response') # response key로 저장
#     if response and response.get('status_code') == 403: # statusCode로 확인
#         context['ti'].xcom_push(key=task_id, value=f"retry")
#         raise AirflowException(f"[ERROR] Lambda {task_id} returned 403 / 크롤러 밴")
#     if response and response.get('status_code') == 500: # statusCode로 확인
#         context['ti'].xcom_push(key=task_id, value=f"retry")
#         raise AirflowException(f"[ERROR] Lambda {task_id} returned 500 / DB 연결 오류")

@task
def invoke_lambda(config, retries:int, invocation_type='RequestResponse', **context):
    print(f"Invoke Lambda: {config['function_name']}")
    try:
        operator = LambdaInvokeFunctionOperator(
            task_id=config['task_id'],
            function_name=config['function_name'],
            payload=config['payload'],
            retries=retries,
            invocation_type=invocation_type,
            retry_delay=timedelta(seconds=10), # PRODUCTION
            execution_timeout=timedelta(minutes=2), # PRODUCTION
            # https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/operators/lambda.html
            # aws client 설정.
            botocore_config={
                'connect_timeout': 120,
                'read_timeout': 120,
                'tcp_keepalive': True,
                'max_pool_connections': 100, # default 10
                'retries': {
                    'max_attempts': 2,
                    'total_max_attempts': 10,
                    'mode': 'standard',
                }
            },
            dag=dag
        )
        return operator.execute(context=get_current_context())
    except Exception as e:
        print(f"Error invoking Lambda: {e}")
        raise  # Re-raise the exception to mark the task as failed

with DAG(
    'test-lambda-search',
    default_args=default_args,
    description='[TEST] Backfill for trigger Lambda search functions',
    schedule_interval=None,
    catchup=False
) as dag:
    # 변수 체크
    Variable.get('LAMBDA_SEARCH_FUNC')
    Variable.get('LAMBDA_DETAIL_FUNC')
    Variable.get('DETAIL_BATCH_SIZE')
    Variable.get('keyword_set_name')
    Variable.get('ENVIRONMENT')
    # Variable.get('checked_at')
    Variable.get('start_date')
    Variable.get('end_date')
    
    set_environment_task = set_environment()
    # RDS에서 키워드 가져오기
    lambda_search_configs = generate_lambda_search_configs()
    # Dynamic task mapping!
    search_lambda_tasks = invoke_lambda.partial(invocation_type='RequestResponse', retries=3).expand(config=lambda_search_configs)
    
    set_environment_task >> lambda_search_configs >> search_lambda_tasks 