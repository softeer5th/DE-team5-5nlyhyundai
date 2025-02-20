import re
from typing import List, Dict, Optional, Union
from datetime import datetime, timezone, timedelta
import time
import random
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from bs4 import BeautifulSoup

from bobaedream_utils import (
    clean_date_string,
    prepare_for_spark,
    save_html,
)

from common_utils import (
    get_db_connection,
    save_s3_bucket_by_parquet,
    upsert_post_tracking_data,
    get_details_to_parse,
    update_status_banned,
    update_status_failed,
    update_status_changed,
    update_status_unchanged,
    update_changed_stats,
    get_my_ip,
    analyze_post_with_gpt,
)

# 멀티스레드를 위한 설정
analysis_executor = ThreadPoolExecutor(max_workers=20)

linebreak_ptrn = re.compile(r'(\n){2,}')  # 줄바꿈 문자 매칭

def analyze_post(payload):
    """크롤링된 데이터를 감성 분석 수행"""
    try:
        print(f"🎭 감성 분석 시작: {payload['url']}")
        analyzed_post = analyze_post_with_gpt(payload)
        print(f"✅ 감성 분석 완료: {payload['url']}")
        return analyzed_post
    except Exception as e:
        print(f"❌ 감성 분석 오류: {e}")
        payload['sentiment'] = None
        for comment in payload['comment']:
            comment['sentiment'] = None
        return payload
    
def process_batch(futures: List) -> List[Dict]:
    """배치 단위로 감성 분석 처리"""
    results = []
       
    # 완료된 작업들의 결과를 수집
    for future in as_completed(futures):
        try:
            result = future.result()
            if result:
                results.append(result)
        except Exception as e:
            print(f"Error processing batch item: {e}")
    
    return results


def parse_post_meta(post, post_meta):
    # 포스팅 메타데이터 추가
    title_elem = post_meta.find('dt')
    if title_elem is None:
        print("[ERROR] 제목 파싱 실패.")    
    post['title'] = title_elem['title']    
    count_group = post_meta.find('span', class_='countGroup')
    count_group_em = count_group.find_all('em')
    try:
        view = count_group_em[0].text
        view = int(view.replace(',', ''))  # 쉼표 제거
    except Exception as e:
        print(f"[ERROR] 조회수 파싱 실패: {e}")
        view = -999
    try:
        like = count_group_em[2].text
    except Exception as e:
        print(f"[ERROR] 좋아요 파싱 실패: {e}")
        like = -999
    date_raw = count_group.text
    date_str = date_raw.split('|')[-1].strip()
    try:
        posting_datetime = clean_date_string(date_str)
    except Exception as e:
        print(f"[ERROR] 날짜 파싱 실패: {e}")
        posting_datetime = datetime.strptime('0000-01-01', '%Y-%m-%d')
    post['view'] = view
    post['like'] = like # like 등은 여기서 처리해야 함.        
    post['dislike'] = None
    post['created_at'] = posting_datetime

def parse_detail() -> Union[List[Dict] | int | None]:
    """
    검색 결과의 각 게시물에 대해 상세 정보를 파싱하여 추가합니다.
    
    Args:
        search_results: 검색된 게시물 목록 ([{title, link, ...}, ...])
    
    Returns:
        상세 정보가 추가된 게시물 목록
    """
    headers = {
        # 'User-Agent': UserAgent,
        'Host': 'www.bobaedream.co.kr',
        'Origin': 'https://www.bobaedream.co.kr',
        'Referer': 'https://www.bobaedream.co.kr/search',
        "Accept-Language":"ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
        "Accept-Encoding":"gzip, deflate, br, zstd",
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8'
        }
    
    conn = get_db_connection()
    if conn is None:
        print("[ERROR] DB 연결 실패")
        return 500
    
    # DB에서 상세 정보를 가져올 게시물 목록
    table_name = 'probe_bobae'
    details = get_details_to_parse(conn, table_name)
    if details is None:
        print("[ERROR] DB 조회 실패")
        return 500
    
    if details == []:
        print("[INFO] 파싱할 게시물이 없습니다.")
        return 204
    
    payloads = []
    current_batch = []
    BATCH_SIZE = min(50, len(details))

    for post in details:
        try:
            post_url = post['url']
            print(f"요청 플랫폼: 보배드림 / '{post_url}' 검색 중...")

            # 상세 페이지 요청
            time.sleep(1 + random.random())
            response = requests.get(
                    post_url,
                    headers=headers
                )
            response.encoding = 'utf-8'

            # cookies = response.cookies
            # response_headers = response.headers

            if response.status_code != 200:
                print(f"[ERROR] 확인 필요! status code: {response.status_code}")
                if response.status_code == 403:
                    print(f"IP 차단됨! {response.status_code}")
                    update_status_banned(conn, table_name, post['url'])
                return 403, payloads
                    
            soup = BeautifulSoup(response.text, 'html.parser', from_encoding='utf-8')
            # 댓글이 없는 경우를 위한 처리
            has_comment = True
            try:
                comment_list = soup.find('div', id='cmt_list').find('ul', class_='basiclist').find_all('li')
                comment_count = len(comment_list)
            except Exception as e:
                print(f"[INFO] 댓글이 없습니다. {e} / post_url: {post_url}")
                has_comment = False
                comment_count = 0
            finally:
                post['comment_count'] = comment_count

            # 본문 내용 파싱
            content_element = soup.find('div', class_='bodyCont')
            try:
                cleaned_content = content_element.text.strip().replace('\xa0', ' ')
                cleaned_content = linebreak_ptrn.sub('\n', cleaned_content)
                post['content'] = cleaned_content
            except Exception as e:
                print(f"[ERROR] 본문 내용 파싱 실패: {e}")
                continue
            try:
                post_meta = soup.find('div', class_='writerProfile').find('dl')
            except Exception as e:
                print(f"[ERROR] 포스팅 메타데이터 파싱 실패: {e}")
                post_meta = None
                continue
            if post_meta is None:
                print("[ERROR] 포스팅 메타데이터 파싱 실패.")
            else:
                # 포스팅 메타데이터 파싱
                parse_post_meta(post, post_meta)
            
            # 댓글 처리
            comment_data = []
            if has_comment:
                for comment in comment_list: # 
                    if "삭제된 댓글입니다" in comment.text:
                        comment_data.append({
                            'created_at': None,
                            'content': None,
                            'like': None,
                            'dislike': None
                        })
                        continue
                    try:
                        comment_meta = comment.find('dl').find('dt').find_all('span')
                    except Exception as e:
                        print(f"[ERROR] 댓글 메타데이터 파싱 실패: {e}")
                        continue
                    
                    if comment_meta is None:
                        print("[ERROR] 댓글 메타데이터 파싱 실패.")
                        continue
                    try:
                        # comment_name = comment_meta[1].text
                        comment_date = datetime.strptime(comment_meta[3].text, '%y.%m.%d %H:%M')
                        comment_content = comment.find('dd').text.strip()
                        comment_like_dislike = comment.find('div', class_='updownbox').find_all('dd')
                        comment_like = comment_like_dislike[0].text.replace('추천 ', '') 
                        comment_dislike = comment_like_dislike[1].text.replace('반대 ', '')
                        comment_data.append({
                            'created_at': comment_date,
                            'content': comment_content,
                            'like': comment_like,
                            'dislike': comment_dislike
                        })
                    except Exception as e:
                        print(f"[ERROR] 댓글 파싱 실패: {e}")
                        continue
            payload = {
                'checked_at': post['checked_at'],
                'platform': 'bobaedream',
                'title': post['title'],
                'post_id': post['post_id'],
                'url': post['url'],
                'content': post['content'],
                'view': post['view'],
                'created_at': post['created_at'],
                'like': post['like'],
                'dislike': post['dislike'],
                'comment_count': post['comment_count'],
                'keywords': post['keywords'],
                'comment': comment_data,
                'status': 'UNCHANGED',
            }       
            
            post['status'] = 'UNCHANGED'
            
            # 모든 포스트에 대해 분석 작업 제출
            future = analysis_executor.submit(analyze_post, payload)
            current_batch.append(future)
            # 완료된 작업들의 결과를 수집
            if len(current_batch) >= BATCH_SIZE:
                print(f"배치 처리 시작 (크기: {len(current_batch)})")
                batch_results = process_batch(current_batch)
                payloads.extend(batch_results)
                current_batch = []

            is_success = update_changed_stats(conn, table_name, post['url'], post['comment_count'], post['view'], post['created_at'])            
            if is_success:
                print(f"[INFO] {post['url']} 업데이트 성공")
            else:
                print(f"[INFO] {post['url']} 업데이트 실패")
            
        except Exception as e:
            post['status'] = 'FAILED'
            payloads.append({
                'status': 'FAILED',
            })
            update_status_failed(conn, table_name, post['url'])
            print(f"[ERROR] {post['url']} 업데이트 실패 / 이유: {e}")    

    # 마지막 배치 처리
    if current_batch:
        print(f"마지막 배치 처리 (크기: {len(current_batch)})")
        batch_results = process_batch(current_batch)
        payloads.extend(batch_results)
        
    return [payload for payload in payloads if payload['status'] == 'UNCHANGED']
        

def lambda_handler(event, context):
    # python -m bobaedream.bobaedream_exec 로 실행
    get_my_ip()
    table_name = 'probe_bobae'
    details_data = parse_detail()
    if details_data == 500:
        return {
            "status_code": 500,
            "body": "[ERROR] DETAIL / DB 연결 실패"
        }
    if details_data == 204:
        return {
            "status_code": 204,
            "body": "[INFO] DETAIL / 파싱할 데이터가 없습니다."
        }
    elif details_data == []:
        return {
            "status_code": 201,
            "body": "[INFO] DETAIL / 업데이트할 데이터가 없습니다."
        }
    try:
        checked_at_dt = details_data[0]['checked_at']
        save_s3_bucket_by_parquet(
            checked_at_dt=checked_at_dt,
            platform="bobaedream", 
            data=details_data
        )
        status_code = 200
        if isinstance(details_data, tuple) and details_data[0] == 403:
            status_code = details_data[0]
            details_data = details_data[1]
            print("[WARNING] DETAIL / 보배드림 IP 차단됨!")
        if status_code == 403:
            return {
                "status_code": 403,
                "body": f"[WARNING] DETAIL / IP 차단됨 / 크롤링 데이터: {len(details_data)} 건"
            }
        return {
            "status_code": 200,
            "body": f"[INFO] S3 저장 완료: {len(details_data)} 건"
        }
    except Exception as e:
        print(f"[ERROR] S3 저장 실패: {e}")
        conn = get_db_connection()
        if details_data:
            for detail in details_data:
                update_status_failed(conn, table_name, detail["url"])
        return {
            "status_code": 500,
            "body": "[ERROR] S3 저장 실패"
        }