import requests
from bs4 import BeautifulSoup
import re
import urllib.parse
import time
import random
import json
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError

from common_utils import (
    get_db_connection,
    save_s3_bucket_by_parquet,
    upsert_post_tracking_data,
    get_details_to_parse,
    update_status_banned,
    update_status_changed,
    update_status_unchanged,
    update_changed_stats,
    analyze_post_with_gpt,
    get_my_ip,
)

BASIC_URL = "https://www.clien.net/service/search?q={query}&sort=recency&p={page_num}&boardCd=&isBoard=false"
CLIEN_URL = "https://www.clien.net"

SEARCH_TABLE = "probe_clien"
EXECUTOR_MAX = 20

# 멀티스레드를 위한 설정
analysis_executor = ThreadPoolExecutor(max_workers=EXECUTOR_MAX)

def detail(event, context):
# parameters
    get_my_ip()
    timestamp = 0
    
    all_post = []
    futures = []

    executing = 0

    headers = {
        "Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
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

    timestamp = details[0]["checked_at"]
    
    for post in details:
        post_url = post["url"]
        REQUEST_REST = 1 + random.random()
        
        response = requests.get(post_url, headers=headers, allow_redirects=False)
        print(f"{post_url} - {response.status_code}")
        if response.status_code != 200:
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
            comment_created_at = datetime.strptime(comment_created_at, "%Y-%m-%d %H:%M:%S")

            comment_like = row.find("div", class_="comment_content_symph")
            comment_like = comment_like.find("strong").text if comment_like else "0"

            comment_data = {
                "created_at": comment_created_at,
                "content": comment_content,
                "like": comment_like,
                "dislike": None
            }
            all_comments.append(comment_data)

        try: 
            hit = soup.find("div", class_="post_author").find("span", class_="view_count").find("strong").text
            hit = int(hit)
        except: 
            hit = post["view"]
        
        post_data = {
            "title": soup.find("h3", class_="post_subject").find_all("span")[0].text,
            "post_id": post["post_id"],
            "url": post_url,
            "content": soup.find("div", class_="post_article").get_text(separator="\n", strip=True),
            "view": hit,
            "created_at": post["created_at"],
            "like": int(soup.find("a", class_="symph_count").find("strong").text if soup.find("a", class_="symph_count") else "0"),
            "dislike": None,
            "comment_count": int(soup.find("a", class_="post_reply").find("span").text if soup.find("a", class_="post_reply") else "0"),
            "keywords": post["keywords"],
            "comment": all_comments
        }
        
        futures.append(analysis_executor.submit(analyze_post_with_gpt, post_data))
        executing += 1
        #time.sleep(REQUEST_REST)

        # as_completed를 Request_rest만큼 대기
        try:
            for future in as_completed(futures, timeout=REQUEST_REST if executing < EXECUTOR_MAX else None):
                try:
                    post_data = future.result()
                    executing -= 1
                    if post_data:
                        all_post.append(post_data)
                        update_changed_stats(conn, table_name, post_data['url'], post_data['comment_count'], post_data['view'], post_data['created_at'])
                        print(f"{post_data['url']} - Done!")
                except Exception as e:
                    print(f"Error processing post: {e}")
        except TimeoutError:
            print("gpt timeout! get next page...")
            continue
    for future in as_completed(futures):
            try:
                post_data = future.result()
                if post_data:
                    all_post.append(post_data)
                    update_changed_stats(conn, table_name, post_data['url'], post_data['comment_count'], post_data['view'], post_data['created_at'])
                    print(f"{post_data['url']} - Done!")
            except Exception as e:
                print(f"Error processing post: {e}")
    save_s3_bucket_by_parquet(timestamp, platform='clien', data=all_post)

    