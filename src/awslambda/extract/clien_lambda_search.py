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

SEARCH_TERM = timedelta(hours=14)

SEARCH_TABLE = "probe_clien"

SI_PREFIX = {"k":1000, "M":1000000, "G": 1000000000}

def search(event, context):
    # parameters
    timestamp = event.get("checked_at")
    query = event.get("keyword")
    #start_date = event.get("start_date")
    #end_date = event.get("end_date")
    
    if timestamp:
        #event_time = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S.%f%z")
        event_time = datetime.fromisoformat(timestamp)
    else:
        event_time = datetime.now(timezone.utc)  # Fallback
    kst_time = event_time + timedelta(hours=9)  # UTC+9 (KST)
    """
    if start_date:
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    else:
        start_dt = kst_time - SEARCH_TERM
    
    if end_date:
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")
    else:
        end_dt = kst_time
    """

    conn = get_db_connection()
    if conn is None:
        raise Exception("clien search: 500 - [ERROR] SEARCH / DB 연결 실패")
        return {
            "status_code": 500,
            "body": "[ERROR] SEARCH / DB 연결 실패"
        }

    isNextPage = True
    #p = 0

    #while isNextPage:
    for p in range(10):
        full_url = BASIC_URL.format(query=urllib.parse.quote(query), page_num=p)
        headers = {
            "Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
        }

        REQUEST_REST = 1 + random.random()
        print("try:", full_url)
        response = requests.get(full_url, headers=headers, allow_redirects=False)
        if response.status_code != 200:
            print("status code:", response.status_code)
            print("headers:", response.headers)
            print("body:", response.text)
            raise Exception("clien search: 403 - [WARNING] SEARCH/clien 연결 실패")
            return  {
                "status_code": 403, 
                "body": "[WARNING] SEARCH/clien 연결 실패"
            }
            break

        soup = BeautifulSoup(response.content, "html.parser")
        if not soup.find("a", class_="board-nav-page active"):
            break

        posts_raw = soup.find_all("div", class_="list_item symph_row jirum")
        for post in posts_raw:
            
            created_at_str = post.find("span", class_="timestamp").text
            created_at = datetime.strptime(created_at_str, "%Y-%m-%d %H:%M:%S")
            """
            if created_at < start_dt:
                isNextPage = False
                break

            if (created_at > end_dt):
                continue
            """
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
        #p += 1
        


"""
    # KST 시간 출력 (형식: 'YYYY-MM-DD HH:MM:SS')
    kst_time_str = kst_time.strftime('%Y-%m-%d %H:%M:%S')

    print("input:", event_time, "\noutput:", kst_time_str)

    return {
        "statusCode": 200,
        "body": json.dumps({"timestamp": timestamp, "kst_time": kst_time_str})
    }
"""

