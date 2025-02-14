import re
from typing import List, Dict, Optional
from datetime import datetime, timezone, timedelta
import time

import requests
from bs4 import BeautifulSoup

from bobaedream_utils import (
    post_id_salt
)

from common_utils import (
    get_db_connection,
    save_s3_bucket_by_parquet,
    upsert_post_tracking_data,
)
linebreak_ptrn = re.compile(r'(\n){2,}')  # 줄바꿈 문자 매칭

def extract_bobaedream(start_date, page_num, keyword):
    form_data = {
        'keyword': keyword,
        'colle': 'community',
        'searchField': 'ALL',
        'page': page_num,
        'sort': 'DATE',
        'startDate': start_date,
    }
    # 이후 막히면 수정 필요할 수도.
    # UA, cookies, proxies, headers 등을 추가해야 할 수도 있음.
    # 쿠키는  
    # 쿠키는 한 번 selenium으로 로그인해서 받아오면 그걸로 쓰면 됨.
    # 막히면 모바일로도 고려 (touch 등 js 코드에 없어서 모바일로 하면 무조건 가능할 듯).
    
    # UserAgent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36'
    headers = {
        # 'User-Agent': UserAgent,
        'Host': 'www.bobaedream.co.kr',
        'Origin': 'https://www.bobaedream.co.kr',
        'Referer': 'https://www.bobaedream.co.kr/search',
        "Accept-Language":"ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
        "Accept-Encoding":"gzip, deflate, br, zstd",
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8'
        }
    
    print(f"요청 플랫폼: 보배드림 / 페이지 {page_num}에서 '{keyword}' 검색 중...")
    time.sleep(1)
    response = requests.post(
            'https://www.bobaedream.co.kr/search', 
            data=form_data,
            headers=headers
        )
    # cookies = response.cookies
    # response_headers = response.headers
    if response.status_code != 200:
        print(f"확인 필요! status code: {response.status_code}")
        if response.status_code == 403:
            print(f"IP 차단됨. {response.status_code}")
        return None
    
    return response.text

def parse_search(
        html, 
        start_dt: datetime, 
        end_dt: datetime, 
        checked_at: datetime,
        keyword: str
    ) -> Optional[bool]:
    soup = BeautifulSoup(html, 'html.parser', from_encoding='utf-8')
    
    community_results = soup.find_all('div', class_='search_Community')
    # search_data = []
    if not community_results:
        print('[ERROR] 검색 결과가 없거나, 에러가 발생했습니다.')
        return
    
    conn = get_db_connection()
    if conn is None:
        print("[ERROR] DB 연결 실패")
        return
    
    table_name = 'probe_bobae'
    for community_result in community_results:
        # ul 태그들 찾기
        lis = community_result.find_all('li')
        if not lis:
            print('[INFO] 검색 결과가 더이상 없습니다.')
            # save_html('htmls/search', html)
            return True

        for li in lis:
            payload = {
                'platform': 'bobaedream',
                'checked_at': checked_at,
            }
            # 각 li 안에서 dt > a 찾기
            try:
                a_tag = li.find('dt').find('a')
                # 타이틀과 url 파싱
                title = a_tag.text.strip()
                url = a_tag['href']
            except Exception as e:
                print(f"dt 혹은 a 태그가 없습니다. : {e}")
                continue
            else:
                payload['title'] = title
                payload['url'] = f"https://www.bobaedream.co.kr{url}"                

            # 각 li 안에서 dd > span 찾기
            try:    
                spans = li.find('dd', class_='path').find_all('span')
                # span 태그 파싱
                if spans[0].text == 'news':
                    print(f"뉴스: {title}, url: {payload['url']}")
                    # continue
                payload['category'] = spans[0].text
                payload['writer'] = spans[1].text
                payload['post_id'] = url.split('No=')[1]
                payload['post_id'] = post_id_salt(payload['post_id'], payload['category'])
                created_at = spans[2].text
            except Exception as e:
                print(f"span 태그가 없습니다. : {e}")
                continue
            else:
                created_at_dt = datetime.strptime(created_at, '%y. %m. %d')
                payload['created_at'] = created_at_dt
                    # 2000년대로 가정
                if created_at_dt.year < 100:
                    created_at_dt = created_at_dt.replace(year=created_at_dt.year + 2000)

                if created_at_dt > end_dt:
                    print(f'[INFO] 기간이 더 뒤이기에 넘어갑니다. {end_dt} / 게시글 날짜: {created_at_dt}')
                    continue
                
                if created_at_dt < start_dt:
                    print(f'[INFO] 기간이 더 앞이기에 종료합니다. {start_dt} / 게시글 날짜: {created_at_dt}')
                    return True

                if payload['category'] == '내차사진':
                    print(f'[WARNING] 내차사진이어서 스킵합니다. {title}')
                    continue
            
            payload['view'] = -999 # 반드시 업데이트되게끔 설정함. # 보배드림 특이 케이스임. (int여야 함!)
            payload['comment_count'] = -999 # 반드시 업데이트되게끔 설정함. # 보배드림 특이 케이스임. (int여야 함!)
            payload['status'] = 'CHANGED'  # 반드시 업데이트되게끔 설정함.
            payload['keyword'] = keyword
            # DB에 변경사항 저장
            # comment_count, view를 확인할 수 있으면 바로 업데이트.
            upsert_post_tracking_data(
                    conn=conn,
                    table_name=table_name,
                    payload=payload
                )

def lambda_handler(event, context):
    """
    keyword, checked_at 필수.
    start_date, 
    end_date는 선택.
    """
    # python -m bobaedream.bobaedream_exec 로 실행
    # 이게 크롤링한 시간.
    checked_at_str = event.get('checked_at')
    # ISO 8601 형식 → UTC 기준
    if checked_at_str:
        event_time = datetime.fromisoformat(checked_at_str.replace("Z", "+00:00"))
    else:
        event_time = datetime.now(timezone.utc)  # Fallback
    # UTC 시간에 9시간을 더해 KST 시간으로 변환
    checked_at = event_time + timedelta(hours=9)  # UTC+9 (KST)
    # KST 시간 출력 (형식: ‘YYYY-MM-DD HH:MM:SS’)
    print("한국 시간:", checked_at.strftime('%Y-%m-%d %H:%M:%S'))

    # 게시글 시작 날짜
    start_date = event.get('start_date')
    if start_date is None:
        start_dt = checked_at - timedelta(days=14)
    else:
        start_dt = datetime.strptime(start_date, '%Y-%m-%d')    
    
    # 게시글 종료 날짜
    end_date = event.get('end_date')
    if end_date is None:
        end_dt = checked_at + timedelta(days=1)
    else:
        end_dt = datetime.strptime(end_date, '%Y-%m-%d')
    
    # 검색할 키워드
    keyword = event.get('keyword')
    for i in range(1, 1000):
        html = extract_bobaedream(start_date, page_num=i, keyword=keyword)
        # save_html('htmls/search', html)
        has_done = parse_search(html, start_dt=start_dt, end_dt=end_dt, checked_at=checked_at, keyword=keyword)   
        if has_done:
            print(f"[INFO] 보배드림 검색 종료: {keyword}")
            break