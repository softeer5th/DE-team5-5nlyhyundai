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

SEARCH_TABLE = "probe_clien"

def detail(event, context):
    # parameters
    timestamp = 0
    
    all_post = []

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

        hit = soup.find("div", class_="post_author").find("span", class_="view_count").find("strong").text
        try: hit = int(hit)
        except: hit = post["view"]

        post_data = {
            "keywords": post["keywords"],
            "post_id": post["post_id"],
            "title": soup.find("h3", class_="post_subject").find_all("span")[0].text,
            "url": post_url,
            "content": soup.find("div", class_="post_article").get_text(separator="\n", strip=True),
            "view": hit,
            "created_at": post["created_at"],
            "like": int(soup.find("a", class_="symph_count").find("strong").text if soup.find("a", class_="symph_count") else "0"),
            "dislike": None,
            "comment_count": int(soup.find("a", class_="post_reply").find("span").text if soup.find("a", class_="post_reply") else "0"),
            "comment": all_comments
        }    
        all_post.append(post_data)
        update_changed_stats(conn, table_name, post_data['url'], post_data['comment_count'], post_data['view'], post_data['created_at'])
        print(f"{post_url} - Done!")
        time.sleep(REQUEST_REST)

    save_s3_bucket_by_parquet(timestamp, platform='clien', data=all_post)
