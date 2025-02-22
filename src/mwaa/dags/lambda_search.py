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

def get_details_count(**context) -> List[str]:
    """
    detail 단계에서 처리할 url들을 가져옵니다.
    """
    pg_hook = PostgresHook(postgres_conn_id='postgres_conn')
    with pg_hook.get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute('''
                        SELECT 
                            (SELECT COUNT(*) FROM probe_bobae WHERE status != 'UNCHANGED') as bobae_count,
                            (SELECT COUNT(*) FROM probe_clien WHERE status != 'UNCHANGED') as clien_count,
                            (SELECT COUNT(*) FROM probe_dcmotors WHERE status != 'UNCHANGED') as dc_count
                        ''')
        
            details_count = cursor.fetchone()  # fetchall() 대신 fetchone() 사용
    print("변경된 데이터 개수")
    print(f"보배: {details_count['bobae_count']}, 클리앙: {details_count['clien_count']}, 디시인사이드: {details_count['dc_count']}")        
    context['task_instance'].xcom_push(key='details_count', value=details_count)
    return details_count


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
                'task_id': f"invoke_lambda_{function_name}_{keyword}".replace(' ', '_'),
                'payload': json.dumps({
                    'checked_at': checked_at,
                    'keyword': keyword,
                    'start_date': '2025-02-01',
                })
            })
    print(f"lambda_search_configs : {lambda_search_configs}")
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
    try:
        operator = LambdaInvokeFunctionOperator(
            task_id=config['task_id'],
            function_name=config['function_name'],
            payload=config['payload'],
            retries=retries,
            invocation_type=invocation_type,
            retry_delay=timedelta(seconds=5),
            execution_timeout=timedelta(minutes=15),
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
    pg_hook = PostgresHook(postgres_conn_id='postgres_conn')
    with pg_hook.get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute('''
                        SELECT 
                            (SELECT COUNT(*) FROM probe_bobae WHERE status != 'UNCHANGED') as bobae_count,
                            (SELECT COUNT(*) FROM probe_clien WHERE status != 'UNCHANGED') as clien_count,
                            (SELECT COUNT(*) FROM probe_dcmotors WHERE status != 'UNCHANGED') as dc_count
                        ''')
        
            details_count = cursor.fetchone()  # fetchall() 대신 fetchone() 사용
    
    print("변경된 데이터 개수")
    print(f"보배: {details_count['bobae_count']}, 클리앙: {details_count['clien_count']}, 디시인사이드: {details_count['dc_count']}")

    # 20개 당 하나씩 처리
    detail_batch_size = Variable.get('DETAIL_BATCH') # 현재 20
    crawler_counts = [max(math.ceil(v / detail_batch_size), 1) for v in details_count.values()]
    detail_batch_size = 20
    print(f"크롤러 별 배치 사이즈: {crawler_counts}")
    for function_name in function_names:
        for idx in crawler_counts: # bobae, clien, dc. 순서
            lambda_detail_configs.append({
                'function_name': function_name,
                'task_id': f"invoke_lambda_{function_name}{idx}".replace(' ', '_'),
                'func_id': idx,
                'payload': json.dumps({})
            })
    return lambda_detail_configs

with DAG(
    'lambda_search',
    default_args=default_args,
    description='Trigger Lambda search',
    schedule_interval=timedelta(minutes=60),
    catchup=False
) as dag:
    
    checked_at = datetime.fromisoformat(datetime.now(timezone.utc).isoformat(sep='T'))
    checked_at = datetime.strftime(checked_at, '%Y-%m-%dT%H:%M:%S')
    # RDS에서 키워드 가져오기
    lambda_search_configs = generate_lambda_search_configs(checked_at)
    # Dynamic task mapping!
    search_lambda_tasks = invoke_lambda.partial(invocation_type='RequestResponse', retries=3).expand(config=lambda_search_configs)
    
    trigger_second_dag = TriggerDagRunOperator(
        task_id='trigger_detail_dag',
        trigger_dag_id='lambda_detail_to_s3',
        conf={
            'from_dag': 'lambda_search',
            'checked_at': checked_at
        },
        wait_for_completion = False,
    )

    lambda_search_configs >> search_lambda_tasks 
    search_lambda_tasks  >> trigger_second_dag


    