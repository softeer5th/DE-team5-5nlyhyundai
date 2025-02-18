from collections import defaultdict
from typing import Dict, List, Optional
from datetime import datetime
import traceback

import psycopg2
import psycopg2.extras
import pyarrow as pa
import pyarrow.parquet as pq
import boto3
import smart_open
import requests

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

# 전역 변수로 s3_client 관리
s3_client = None

def get_s3_client():
    global s3_client

    if s3_client is not None:
        return s3_client

    try:
        s3_client = boto3.client('s3')
    except Exception as e:
        print(f"S3 클라이언트 생성 실패: {e}")
        return None

    return s3_client

def get_search_keywords(
        conn,
        keyword_set_name: str,
    ) -> Optional[List[str]]:
    """
    이후에 airflow DAG에서 사용할 수 있도록 키워드들을 함수로 분리합니다.

    병렬 search 작업을 위해 키워드 세트를 가져옵니다.
    Args:
        conn: PostgreSQL 데이터베이스 연결 객체
        keyword_set_name: 키워드 세트 이름
    
    Returns:
        키워드 리스트
    """
    with conn.cursor() as cursor:
        sql = f"""
        SELECT 
            keywords
        FROM 
            keyword_set
        WHERE
            name = '{keyword_set_name}'            
        """
        cursor.execute(sql)
        result = cursor.fetchone()
        if result is None:
            return None
        return result['keywords']
        

def upsert_post_tracking_data(
        conn,
        table_name, 
        payload: Dict
        ) -> Optional[bool]:
    """
    한 포스팅을 search 단계에서 파싱할 때 마다 불러와야 합니다.

    1. 실패 혹은 차단된 경우: url, status, checked_at 필드가 필요합니다.
    2. 새로운 것 들어올 때: 모든 payload 필드가 필요합니다.
    2. 기존 것 업데이트: url, status, comment_count, view, created_at, checked_at, keyword 필드가 필요합니다.
    
    payload: {
        url: str,
        post_id: str,
        status: str (CHANGED, UNCHANGED, FAILED, BANNED) (실패 혹은 차단된 경우 필요)
        comment_count: int,
        view: int,
        created_at: datetime,
        checked_at: datetime, 실패 혹은 차단된 경우에도 필요.
        keyword: str,
    }
    
    return None: 실패, True: 성공
    """
    try:
        assert isinstance(payload['url'], str), "url은 문자열이어야 합니다."
        assert isinstance(payload['checked_at'], datetime), "checked_at은 datetime 객체여야 합니다."
        url = payload.get('url')
        with conn.cursor() as cursor:
            sql = f"SELECT * FROM {table_name} WHERE url = %s"
            cursor.execute(
                sql,
                (url,)
                )
            
            result = cursor.fetchone()

            if result is None:
                print(f"[INFO] 새로운 데이터: {url}")
                assert isinstance(payload['post_id'], str), "post_id는 문자열이여야 합니다."
                assert isinstance(payload['status'], str), "status는 문자열이어야 합니다."
                assert isinstance(payload['comment_count'], int), "comment_count는 정수여야 합니다."
                assert isinstance(payload['view'], int), "view는 정수여야 합니다."
                assert isinstance(payload['created_at'], datetime), "created_at은 datetime 객체여야 합니다."
                assert isinstance(payload['keyword'], str), "keyword은 문자열이어야 합니다."
                sql = f"""
                INSERT INTO {table_name} (
                    url,
                    post_id,
                    status,
                    comment_count,
                    view,
                    created_at,
                    checked_at,
                    keywords
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, ARRAY[%s]::TEXT[]
                )
                """
                cursor.execute(
                    sql,
                    (
                        url,
                        payload['post_id'],
                        payload['status'],
                        payload['comment_count'],
                        payload['view'],
                        payload['created_at'],
                        payload['checked_at'],
                        payload['keyword'],
                    )
                )
            else:                
                assert isinstance(payload['status'], str), "status는 문자열이어야 합니다."
                unstable_status = payload['status'] in ["FAILED", "BANNED"]
                if unstable_status:
                    print(f"[WARN] 크롤링 문제 발생 / 상태 업데이트 / 기존 데이터: {url}")
                    if payload['status'] == "FAILED":                        
                        update_status_failed(conn, table_name, url, payload['checked_at'])
                    elif payload['status'] == "BANNED":
                        update_status_banned(conn, table_name, url, payload['checked_at'])
                    return True
                
                assert isinstance(payload['comment_count'], int), "comment_count는 정수여야 합니다."
                assert isinstance(payload['view'], int), "view는 정수여야 합니다."
                assert isinstance(payload['keyword'], str), "keyword은 문자열이어야 합니다."
                has_valuable_change = (
                    result['view'] - payload['view'] > VIEW_THRESHOLD
                    or result['comment_count'] > payload['comment_count']
                )
                new_keyword_event = payload['keyword'] != "" and payload['keyword'] not in result['keywords']
                if has_valuable_change or new_keyword_event:
                    print(f"[INFO] 업데이트 시행 / 기존 데이터: {url}")
                    sql = f"""
                    UPDATE {table_name}
                    SET
                        status = %s,
                        comment_count = %s,
                        view = %s,
                        created_at = %s,
                        checked_at = %s,
                        keywords = CASE 
                            WHEN %s = ANY(keywords) THEN keywords
                            ELSE array_append(keywords, %s) 
                        END
                    WHERE url = %s
                    """
                    cursor.execute(
                        sql,
                        (
                            payload['status'],
                            payload['comment_count'],
                            payload['view'],
                            payload['created_at'],
                            payload['checked_at'],
                            payload['keyword'],
                            payload['keyword'],
                            url,
                        )
                    )
                    conn.commit()
                    return True                
                
                print(f"[INFO] 업데이트 불필요 / 기존 데이터: {url}")
            
            return True
    except Exception as e:
        print(f"[ERROR] DB 업데이트 에러: {e}")
        traceback.print_exc()
        return None

def get_details_to_parse(
        conn,
        table_name,
        ) -> Optional[List[Dict]]:
    """
    detail 단계에서 처리할 url들을 가져옵니다.
    """
    try:
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
            return result
    except Exception as e:
        print(f"[ERROR] DB 조회 에러: {e}")
        return None

def update_search_status(
       conn,
       table_name: str,
       url: str,
       checked_at: Optional[datetime],
       status: Optional[str] = None,
       _prevent_direct_status: str = None  # 직접 status 파라미터를 받지 못하게 하는 파라미터
    ) -> Optional[bool]:
    """DB 레코드의 상태를 업데이트하는 내부 함수입니다. 직접 호출하지 마세요.
    대신 update_status_failed(), update_status_banned() 등의 함수를 사용하세요.

    Args:
        conn: PostgreSQL 데이터베이스 연결 객체
        table_name: 업데이트할 테이블 이름
        url: 업데이트할 레코드의 URL
        checked_at: 확인일자
        status: 업데이트할 상태

    Returns:
        성공 시 True, 실패 시 None
    """
    if _prevent_direct_status is None:
        raise ValueError("이 함수는 직접 호출하지 마세요. 대신 update_status_failed() 등을 사용하세요.")
       
    with conn.cursor() as cursor:  # cursor() 메서드로 수정
        try:
            if checked_at is None:
                    sql = f"""
                    UPDATE {table_name}
                    SET
                        status = %s
                    WHERE url = %s
                    """
                    cursor.execute(
                        sql,
                        (
                            status,
                            url
                        )
                    )
            else:
                    sql = f"""
                    UPDATE {table_name}
                    SET
                        status = %s
                        checked_at = %s
                    WHERE url = %s
                    """
                    cursor.execute(
                        sql,
                        (
                            status,
                            checked_at,
                            url
                        )
                    )
            conn.commit()
            return True
        except Exception as e:
            print(f"[ERROR] DB status, checked_at 에러 / 테이블 이름: {table_name}, status: {status}, checked_at: {checked_at} / 에러 내용: {e}")
            return None

def update_status_failed(
        conn,
        table_name: str,
        url: str,
        checked_at: Optional[datetime] = None
        ) -> Optional[bool]:
    """
    detail 단계에서 실패한 url들을 업데이트합니다.
    URL의 상태를 FAILED로 업데이트합니다.

    Args:
       conn: PostgreSQL 데이터베이스 연결 객체
       table_name: 업데이트할 테이블 이름
       url: 업데이트할 레코드의 URL

    Returns:
       성공 시 True, 실패 시 None
    """
    return update_search_status(conn, table_name, url, checked_at, status="FAILED", _prevent_direct_status="used")

def update_status_banned(
        conn,
        table_name: str,
        url: str,
        checked_at: Optional[datetime] = None
        ) -> Optional[bool]:
    """
    detail 단계에서 차단된 url들을 업데이트합니다.
    URL의 상태를 BANNED로 업데이트합니다.

    Args:
       conn: PostgreSQL 데이터베이스 연결 객체
       table_name: 업데이트할 테이블 이름
       url: 업데이트할 레코드의 URL

    Returns:
       성공 시 True, 실패 시 None
    """
    return update_search_status(conn, table_name, url, checked_at, status="BANNED", _prevent_direct_status="used")

def update_status_unchanged(
        conn,
        table_name: str,
        url:str,
        checked_at: Optional[datetime] = None
        ) -> Optional[bool]:
    """
    detail 단계에서 완료된 url들을 업데이트합니다.
    URL의 상태를 UNCHANGED로 업데이트합니다.

    Args:
       conn: PostgreSQL 데이터베이스 연결 객체
       table_name: 업데이트할 테이블 이름
       url: 업데이트할 레코드의 URL

    Returns:
       성공 시 True, 실패 시 None
    """
    return update_search_status(conn, table_name, url, checked_at, status="UNCHANGED", _prevent_direct_status="used")

def update_status_changed(
        conn,
        table_name: str,
        url: str,
        checked_at: Optional[datetime] = None
        ) -> Optional[bool]:
    """
    detail 단계에서 변경된 url들을 업데이트합니다.
    URL의 상태를 CHANGED로 업데이트합니다.

    Args:
       conn: PostgreSQL 데이터베이스 연결 객체
       table_name: 업데이트할 테이블 이름
       url: 업데이트할 레코드의 URL

    Returns:
       성공 시 True, 실패 시 None
    """
    return update_search_status(conn, table_name, url, checked_at, status="CHANGED", _prevent_direct_status="used")

def update_changed_stats(
        conn,
        table_name: str,
        url: str,
        comment_count: int,
        view: int,
        created_at: datetime,
    ) -> Optional[bool]:
    """
    detail에서 변화된 포스트의 정보를 업데이트합니다.
    UNCHANGED로 변경도 진행합니다.
    Args:
       conn: PostgreSQL 데이터베이스 연결 객체
       table_name: 업데이트할 테이블 이름
       url: 업데이트할 레코드의 URL
       comment_count: 댓글 수
       view: 조회수
       created_at: 작성시간
       checked_at: 확인일자
    """
    with conn.cursor() as cursor:
        try:
            sql = f"""
            UPDATE {table_name}
            SET
                comment_count = %s,
                view = %s,
                created_at = %s,
                status = 'UNCHANGED'
            WHERE url = %s
            """
            cursor.execute(
                sql,
                (
                    comment_count,
                    view,
                    created_at,
                    url
                )
            )
            return True
        except Exception as e:
            print(f"[ERROR] comment_count, view 수정 에러: {e}")
            traceback.print_exc()
            return None

def log_crawling_metadata(
        conn,
        checked_at: datetime,
        keywords_str: str,
        platform: str,
    ):
    """
    checked_at: datetime 객체
    """
    try:
        with conn.cursor() as cursor:
            sql = f"""
            INSERT INTO crawling_metadata (
                checked_at,
                keywords_str,
                platform,
                bucket_name
            ) VALUES (
                %s, %s, %s, %s
            )
            """
            cursor.execute(
                sql,
                (
                    checked_at,
                    keywords_str,
                    platform,
                    S3_BUCKET
                )
            )
            conn.commit()
            return True
    except Exception as e:
        print(f"[ERROR] DB 메타데이터 로깅 에러: {e}")
        traceback.print_exc()
        return None
    

def save_s3_bucket_by_parquet(
        checked_at_dt: datetime,
        platform: str,
        data: List[Dict],
    ) -> Optional[bool]:
    """
    checked_at_dt: datetime 객체

    platform: 적용한 플랫폼
    
    data: [ # 포스팅
        {
        플랫폼 "platform"
        검색 키워드 “keyword”
        포스트 id “post_id”
        제목 "title"
        url:  "url"
        내용 "content"
        조회수 "view"
        작성일자 "created_at"
        좋아요 수 "like"
        싫어요 수 "dislike"
        댓글수 (웹페이지 기반): "comment_count"
        댓글: [ {
        # 댓글 "comment"
        작성일자: "created_at"
        내용: "content"
        좋아요 수: "like"
        싫어요 수: "dislike"
        },
        {
        ...
        },
        ]
        },
        ...                
    ]

    """
    s3_client = get_s3_client()

    if s3_client is None:
        print("[ERROR] S3 클라이언트 생성 실패")
        return None
    
    date = checked_at_dt.date().strftime("%Y-%m-%d")
    hour = str(checked_at_dt.hour)
    minute = str(checked_at_dt.minute)

    # 코멘트 데이터
    keywords_posts = defaultdict(list)
    keywords_comments = defaultdict(list)

    for post in data:
        # 코멘트 제거
        post.pop('id', None)
        post.pop('status', None)
        post.pop('checked_at', None)
        post['platform'] = platform
        try:
            post['like'] = int(post['like'])
        except:
            post['like'] = None
        try:
            post['dislike'] = int(post['dislike']) 
        except:
            post['dislike'] = None
        post['comment_count'] = int(post['comment_count'])
        post['view'] = int(post['view'])
        post_comments = post.pop('comment', [])
        keywords = post.get('keywords', ["no_keyword"])
        joined_keywords = "-".join((keywords))
        keywords_posts[joined_keywords].append(post)
        # post_id를 기준으로 연결
        for comment in post_comments:
            comment['post_id'] = post['post_id']
            # 좋아요, 싫어요 수가 없는 경우 None으로 처리
            try:
                comment['like'] = int(comment['like'])
            except:
                comment['like'] = None            
            try:
                comment['dislike'] = int(comment['dislike'])
            except:
                comment['dislike'] = None
            keywords_comments[joined_keywords].append(comment)
   # 게시물 스키마 정의
    posts_schema = pa.schema([
        ('platform', pa.string()),
        ('title', pa.string()),
        ('post_id', pa.string()),
        ('url', pa.string()),
        ('content', pa.string()),
        ('view', pa.int64()),
        ('created_at', pa.timestamp('s')),  # 'ns' 대신 's' 사용
        ('like', pa.int64()),
        ('dislike', pa.int64()),
        ('comment_count', pa.int64()),
        ('keywords', pa.list_(pa.string()))
    ])

    # 댓글 스키마 정의
    comments_schema = pa.schema([
        ('created_at', pa.timestamp('s')),  # 'ns' 대신 's' 사용
        ('content', pa.string()),
        ('like', pa.int64()),
        ('dislike', pa.int64()),
        ('post_id', pa.string()),
    ])

    try:
        for keyword, posts in keywords_posts.items():
            # Parquet로 변환
            posts_table = pa.Table.from_pylist(posts, schema=posts_schema)
            comments_table = pa.Table.from_pylist(keywords_comments[keyword], schema=comments_schema)

            # S3 업로드 경로 설정
            s3_posts_key = f"{date}/{hour}/{minute}/{keyword}/{platform}_posts.parquet"
            s3_comments_key = f"{date}/{hour}/{minute}/{keyword}/{platform}_comments.parquet"

            # 게시물 데이터 업로드
            with smart_open.open(f"s3://{S3_BUCKET}/{s3_posts_key}", "wb") as s3_file:
                pq.write_table(posts_table, s3_file, compression='snappy')            
            
            # 댓글 데이터 업로드
            with smart_open.open(f"s3://{S3_BUCKET}/{s3_comments_key}", "wb") as s3_file:
                pq.write_table(comments_table, s3_file, compression='snappy')
            
            print(f"[INFO] S3 업로드 완료 (키워드: {keyword}): {s3_posts_key}, {s3_comments_key}")
            conn = get_db_connection()
            log_crawling_metadata(conn, checked_at_dt, keyword, platform)

        return True
        
    except Exception as e:
        import traceback
        print(f"[ERROR] S3 업로드 실패: {str(e)}")
        traceback.print_exc()
        return None    

def get_my_ip():
    try:
        # Option 1: Using ipify API
        response = requests.get('https://api.ipify.org')
        print(f"[INFO] AWS NAT Gateway 변환 이후 IP: {response.text.strip()}")
    except:
        try:
            # Option 2: Alternative IP service if ipify fails
            response = requests.get('https://checkip.amazonaws.com')
            print(f"[INFO] AWS NAT Gateway 변환 이후 IP: {response.text.strip()}")
        except:
            return "Failed to get IP address"


if __name__ == "__main__":
    db_conn = get_db_connection()


