import requests
from bs4 import BeautifulSoup
import re
import urllib.parse
import time
import random
import json
from datetime import datetime, timezone, timedelta

from common_utils import (
    get_db_connection,
    save_s3_bucket_by_parquet,
    upsert_post_tracking_data,
    get_details_to_parse,
    update_status_banned,
    update_status_changed,
    update_status_unchanged,
    update_changed_stats,
)

BASIC_URL = "https://www.clien.net/service/search?q={query}&sort=recency&p={page_num}&boardCd=&isBoard=false"
CLIEN_URL = "https://www.clien.net"

SEARCH_TERM = timedelta(days=14)

SEARCH_TABLE = "probe_clien"


SI_PREFIX = {"k":1000, "M":1000000, "G": 1000000000}

def search(event, context):
    # parameters
    timestamp = event.get("timestamp")
    query = event.get("query")

    event_time = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S.%f%z")
    event_time = datetime.now() - timedelta(hours=9)
    kst_time = event_time + timedelta(hours=9)  # UTC+9 (KST)
    
    conn = get_db_connection()

    isNextPage = True
    p = 0

    while isNextPage:
        full_url = BASIC_URL.format(query=urllib.parse.quote(query), page_num=p)
        headers = {
            "Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "User-Agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36",
        }

        REQUEST_REST = 1 + random.random()
        print("try:", full_url)
        response = requests.get(full_url, headers=headers, allow_redirects=False)
        if response.status_code != 200:
            print("status code:", response.status_code)
            print("headers:", response.headers)
            print("body:", response.text)
            continue

        soup = BeautifulSoup(response.content, "html.parser")
        if not soup.find("a", class_="board-nav-page active"):
            break

        posts_raw = soup.find_all("div", class_="list_item symph_row jirum")
        for post in posts_raw:
            
            created_at_str = post.find("span", class_="timestamp").text
            created_at = datetime.strptime(created_at_str, "%Y-%m-%d %H:%M:%S")
            if (created_at < kst_time - SEARCH_TERM):
                isNextPage = False
                break
            
            hit_raw = post.find("span", class_="hit").text.split(" ")
            hit = float(hit_raw[0])

            if len(hit_raw) > 1:
                hit *= SI_PREFIX[hit_raw[1]]
            hit = int(hit)

            try:comment_cnt = post.find("span", class_="rSymph05").text
            except: comment_cnt = 0
            else: comment_cnt = int(comment_cnt)
            
            post_content = {
                "url": CLIEN_URL + post.find("a", class_="subject_fixed")["href"],
                "post_id": post["data-board-sn"],
                "status": "CHANGED",
                "comment_count": comment_cnt,
                "created_at": created_at,
                "checked_at": kst_time,
                "view": hit,
                "keyword": query
            }
            upsert_post_tracking_data(conn, SEARCH_TABLE ,post_content)
        
        time.sleep(REQUEST_REST)
        p += 1
        
def detail(event, context):
    # parameters
    timestamp = event.get("timestamp")
    event_time = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S.%f%z")
    event_time = datetime.now() - timedelta(hours=9)
    kst_time = event_time + timedelta(hours=9)  # UTC+9 (KST)
    
    all_post = []

    headers = {
            "Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "User-Agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36",
        }
    
    conn = get_db_connection()
    if conn is None:
        print("[ERROR] DB 연결 실패")
        return 
    
    # DB에서 상세 정보를 가져올 게시물 목록
    table_name = 'probe_clien'
    details = get_details_to_parse(conn, table_name)
    if details is None:
        print("[ERROR] DB 조회 실패")
        return
    
    if details == []:
        print("[INFO] 파싱할 게시물이 없습니다.")
        return

    for post in details:
        post_url = post["url"]
        REQUEST_REST = 1 + random.random()

        response = requests.get(post_url, headers=headers, allow_redirects=False)
        if response.status_code != 200:
            print(f"{post_url} - Status Code: {response.status_code}")
            print("headers:", response.headers)
            print("body:", response.text)
            update_status_banned(conn, table_name, post['url'])
            continue
        
        update_status_unchanged(conn, table_name, post['url'])
        soup = BeautifulSoup(response.content, "html.parser")

        comments_raw = soup.find_all("div", class_="comment_row")
        all_comments = []

        for row in comments_raw:
            if "blocked" in row.get("class", []):
                continue  # 차단된 댓글 제외

            comment_content = row.find("div", class_="comment_view").get_text(separator="\n", strip=True)
            comment_created_at = row.find("span", class_="timestamp").get_text(strip=True)
            comment_created_at = re.search(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}", comment_created_at).group(0)

            comment_like = row.find("div", class_="comment_content_symph")
            comment_like = comment_like.find("strong").text if comment_like else "0"

            comment_data = {
                "content": comment_content,
                "created_at": comment_created_at,
                "like": comment_like,
                "dislike": None
            }
            all_comments.append(comment_data)
        print(post.keys)

        hit = soup.find("div", class_="post_author").find("span", class_="view_count").find("strong").text
        try: hit = int(hit)
        except: hit = post["view"]

        post_data = {
            "platform": "clien",
            "keywords": post["keywords"],
            "post_id": post["post_id"],
            "title": soup.find("h3", class_="post_subject").find_all("span")[0].text,
            "url": post_url,
            "content": soup.find("div", class_="post_article").get_text(separator="\n", strip=True),
            "view": hit,
            "created_at": post["created_at"],
            "like": soup.find("a", class_="symph_count").find("strong").text if soup.find("a", class_="symph_count") else "0",
            "dislike": "0",
            "comment_count": int(soup.find("a", class_="post_reply").find("span").text if soup.find("a", class_="post_reply") else "0"),
            "comment": all_comments
        }    
        all_post.append(post_data)
        update_changed_stats(conn, table_name, post_data['url'], post_data['comment_count'], post_data['view'], post_data['created_at'])
        print(f"{post_url} - Done!")
        time.sleep(REQUEST_REST)

    save_s3_bucket_by_parquet(kst_time, platform='clien', data=all_post)


"""
    # KST 시간 출력 (형식: 'YYYY-MM-DD HH:MM:SS')
    kst_time_str = kst_time.strftime('%Y-%m-%d %H:%M:%S')

    print("input:", event_time, "\noutput:", kst_time_str)

    return {
        "statusCode": 200,
        "body": json.dumps({"timestamp": timestamp, "kst_time": kst_time_str})
    }
"""

