import psycopg2
from typing import Dict
from datetime import datetime

from settings import (
    DB_HOST,
    DB_NAME,
    DB_USER,
    DB_PASSWORD,
    DB_PORT,
    VIEW_THRESHOLD,
    S3_BUCKET,
)

# 전역 변수로 connection 관리
db_conn = None

def get_db_connection():
    global db_conn
    
    # 기존 연결이 있고 유효한지 확인
    if db_conn is not None:
        try:
            # 간단한 쿼리로 연결 상태 확인
            with db_conn.cursor() as cur:
                cur.execute('SELECT 1')
            return db_conn
        except Exception:
            # 연결이 끊어졌다면 None으로 설정
            db_conn = None
    
    # 새로운 연결 생성
    try:
        if db_conn is None:
            db_conn = psycopg2.connect(
                host=DB_HOST,
                database=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD,
                port=DB_PORT,
                cursor_factory=psycopg2.extras.RealDictCursor,  # 기본 cursor factory 설정
                # Timeout 설정 추가
                connect_timeout=5,        # 연결 시도 timeout (초)
                keepalives=1,            # TCP keepalive 활성화
                keepalives_idle=1800,      # TCP keepalive idle time (초)
                keepalives_interval=10,   # TCP keepalive interval (초)
                keepalives_count=3       # TCP keepalive retry count            
            )
            db_conn.autocommit = True  # 필요에 따라 설정
    except Exception as e:
        print(f"DB 연결 에러: {e}")
        return None

    return db_conn

def update_search_status(
        conn,
        table_name,
        start_dt, 
        data: Dict
        ):
    """
    data: {
        url: str,
        post_id: str,
        status: str (CHANGED, UNCHANGED, FAILED, BANNED)
        comment_count: int,
        view: int,
        created_at: datetime,
        checked_at: datetime
    }
    """
    assert isinstance(start_dt, datetime), "start_dt는 datetime 객체여야 합니다."
    assert isinstance(data['url'], str), "url은 문자열이어야 합니다."
    assert isinstance(data['post_id'], str), "post_id는 문자열이여야 합니다."
    assert isinstance(data['status'], str), "status는 문자열이어야 합니다."
    assert isinstance(data['comment_count'], int), "comment_count는 정수여야 합니다."
    assert isinstance(data['view'], int), "view는 정수여야 합니다."
    assert isinstance(data['created_at'], datetime), "created_at은 datetime 객체여야 합니다."
    assert isinstance(data['checked_at'], datetime), "checked_at은 문자열이어야 합니다."

    url = data.get('url')
    with conn.cursor() as cursor:
        sql = f"SELECT * FROM {table_name} WHERE url = %s"
        cursor.execute(
            sql,
            (url,)
            )
        
        result = cursor.fetchone()

        if result is None:
            sql = f"""
            INSERT INTO {table_name} (
                url,
                post_id,
                status,
                comment_count,
                view,
                created_at,
                checked_at
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s
            )
            """
            cursor.execute(
                sql,
                (
                    url,
                    data['post_id'],
                    data['status'],
                    data['comment_count'],
                    data['view'],
                    data['created_at'],
                    data['checked_at'],
                )
            )
        else:
            update_condition = (
                data['view'] - result['view'] > VIEW_THRESHOLD
                or data['comment_count'] > result['comment_count']
                or result['status'] != "UNCHANGED"
            )
            if update_condition:
                sql = f"""
                UPDATE {table_name}
                SET
                    status = %s,
                    comment_count = %s,
                    view = %s,
                    created_at = %s,
                    checked_at = %s
                WHERE url = %s
                """
                cursor.execute(
                    sql,
                    (
                        data['status'],
                        data['comment_count'],
                        data['view'],
                        data['created_at'],
                        data['checked_at'],
                        url
                    )
                ) 

def get_detail_job(
        conn,
        table_name,
        ):
    with conn.cursor() as cursor:
        sql = f"""
        SELECT 
            * 
        FROM 
            {table_name} 
        WHERE status != 'UNCHANGED'
        """
        cursor.execute(sql)
        
        result = cursor.fetchall()
        

    




if __name__ == '__main__':
    conn = get_db_connection()
