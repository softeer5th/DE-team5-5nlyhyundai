import requests
from bs4 import BeautifulSoup
import json
import urllib.parse
import time
from datetime import datetime, timezone, timedelta
import os

from common_utils import (
    get_db_connection,
    upsert_post_tracking_data,
)

# ✅ User-Agent 설정 (크롤링 차단 방지)
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "Accept-Encoding": "gzip, deflate, br, zstd",
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
    "Connection": "keep-alive"
}

# ✅ 기본 URL 설정
BASIC_URL = "https://gall.dcinside.com/board/lists/?id=car_new1&page={page_num}&s_type=search_subject_memo&s_keyword={query}"
DCINSIDE_URL = "https://gall.dcinside.com"

# ✅ 직접 설정할 변수들
max_pages = 20  # 🔹 각 search_pos에서 최대 몇 개의 페이지를 탐색할지 설정

# ✅ Lambda Handler 함수
def lambda_handler(event, context):
    # ✅ DB 연결
    conn = get_db_connection()
    if conn is None:
        print("[ERROR] DB 연결 실패")
        raise Exception("500 - DB 연결 실패")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": "DB 연결 실패"})
        }

    # 이게 크롤링한 시간.
    checked_at_str = event.get('checked_at')
    # ISO 8601 형식 → UTC 기준
    if checked_at_str:
        event_time = datetime.fromisoformat(checked_at_str)
    else:
        event_time = datetime.now(timezone.utc).replace(tzinfo=None)   # Fallback
    # UTC 시간에 9시간을 더해 KST 시간으로 변환
    checked_at = event_time + timedelta(hours=9)  # UTC+9 (KST)
    # KST 시간 출력 (형식: ‘YYYY-MM-DD HH:MM:SS’)
    print("한국 시간:", checked_at.strftime('%Y-%m-%d %H:%M:%S'))
    
    # 검색할 키워드
    keyword = event.get('keyword')
    post_links = []  # ✅ 크롤링한 데이터 저장 리스트

    encoded_query = urllib.parse.quote(keyword)

    prev_page_post_ids = set()

    for page in range(1, max_pages + 1):
        print(f"📄 현재 페이지: {page}, 검색어: {keyword}")

        # 🔹 검색 페이지 URL 생성
        url = BASIC_URL.format(page_num=page, query=encoded_query)

        # 🔹 게시글 목록 요청
        response = requests.get(url, headers=HEADERS)
        if response.status_code != 200:
            print(f"🔴 요청 실패 (Status Code: {response.status_code}) - {url}")
            continue

        soup = BeautifulSoup(response.content, "html.parser")

        # 🔹 게시글 리스트 가져오기
        articles = soup.select("tr.ub-content.us-post")

        if not articles:
            print(f"🔴 페이지 {page}에서 게시글 없음")
            break

        current_page_post_ids = set()

        for article in articles:
            try:
                # ✅ 게시글 제목 및 URL 가져오기
                title_element = article.select_one("td.gall_tit.ub-word a:nth-child(1)")
                if not title_element:
                    continue

                title = title_element.text.strip()
                link = DCINSIDE_URL + title_element["href"]

                # ✅ 댓글 수 가져오기
                reply_num_span = article.select_one("td.gall_tit.ub-word a.reply_numbox span.reply_num")
                if reply_num_span:
                    comment_count = int(reply_num_span.text.strip().replace("[", "").replace("]", ""))
                else:
                    comment_count = 0

                post_id = article.get("data-no")
                current_page_post_ids.add(post_id)

                # ✅ 조회수 가져오기
                view_count_element = article.select_one("td.gall_count")
                view_count = int(view_count_element.text.strip().replace("조회 ", "").replace(",", "")) if view_count_element else 0

                # ✅ 작성일 가져오기
                created_at_element = article.select_one("td.gall_date")
                created_at = created_at_element.get("title") if created_at_element else "Unknown"
                created_at = datetime.strptime(created_at, "%Y-%m-%d %H:%M:%S")

                print(f"제목: {title} | 댓글 수: {comment_count} | 키워드: {keyword}")

                # ✅ 데이터 저장 (checked_at 추가)
                post_data = {
                    "title": title,
                    "url": link,
                    "comment_count": comment_count,
                    "status": "CHANGED",
                    "keyword": keyword,
                    "checked_at": checked_at,
                    "post_id": post_id,
                    "view": view_count,
                    "created_at": created_at
                }
                post_links.append(post_data)

                # ✅ 개별 게시글 데이터를 하나씩 DB에 저장
                upsert_post_tracking_data(
                    conn=conn,
                    table_name="probe_dcmotors",
                    payload=post_data
                )

            except Exception as e:
                print(f"❌ 게시글 크롤링 오류: {e}")
                continue
        
        if prev_page_post_ids and prev_page_post_ids == current_page_post_ids:
            print(f"🔴 동일한 페이지 크롤링 반복 감지, 크롤링 종료.")
            break

        prev_page_post_ids = current_page_post_ids

        # 🔹 요청 간격 조절 (서버 부하 방지)
        time.sleep(1)

    # ✅ DB 연결 종료
    conn.close()

    print(f"✅ 게시글 URL 크롤링 완료! {len(post_links)}개의 URL을 저장했습니다.")

    # ✅ Lambda에서 JSON 형식으로 반환
    return {
        "statusCode": 200,
        "body": json.dumps({"message": "크롤링 완료", "total_posts": len(post_links)}, ensure_ascii=False)
    }