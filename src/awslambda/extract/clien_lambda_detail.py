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
    update_status_failed,
    update_status_banned,
    update_status_changed,
    update_status_unchanged,
    update_changed_stats,
    analyze_post_with_gpt,
    get_my_ip,
    requestPage,
    requestPageProxy
)

SEARCH_TABLE = "probe_clien"

EXECUTOR_MAX = 20
REMAINING_TIME_LIMIT = 60000 # ms, milli-second

# 멀티스레드를 위한 설정
analysis_executor = ThreadPoolExecutor(max_workers=EXECUTOR_MAX)

def detail(event, context):
# parameters
    lambda_id = event.get("id")
    checked_at = event.get("checked_at")
    checked_at = datetime.fromisoformat(checked_at)
    
    get_my_ip()
    
    all_post = []
    futures = []

    executing = 0

    headers = {
        "Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
    }
    
    conn = get_db_connection()
    if conn is None:
        print("[ERROR] DB 연결 실패")
        return  {
            "status_code": 500, 
            "body": "[ERROR] DETAIL/clien DB 연결 실패"
        }
    
    return_dict = {
        "status_code": 204,
        "body": "[INFO] clien DETAIL / 파싱할 데이터가 없습니다."
    }

    # DB에서 상세 정보를 가져올 게시물 목록
    table_name = 'probe_clien'

    while True:
        detail = get_details_to_parse(conn, table_name)
        if detail is None:
            print("[ERROR] DB 조회 실패")
            return_dict["status_code"] = 500
            return_dict["body"] = "[ERROR] DETAIL/clien DB 조회 실패"
            break
    
        if detail == []:
            print("[INFO] 파싱할 게시물이 없습니다.")
            break
        
        post = detail # 이하 호환을 위해 변수 이름 변경(및 복사)
        checked_at = post["checked_at"]

        post_url = post["url"]
        REQUEST_REST = 1 + random.random()
        
        content = requestPageProxy("clien", requests.get, url=post_url, allow_redirects=False, timeout=3)
        if content is None:
            update_status_banned(conn, table_name, post_url, checked_at)
            continue

        soup = BeautifulSoup(content, "html.parser")

        comments_raw = soup.find_all("div", class_="comment_row")
        all_comments = []

        for row in comments_raw:
                
            if "blocked" in row.get("class", []):
                continue  # 차단된 댓글 제외
            
            try:
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
            except Exception as e:
                print(f"[INFO] clien detail: 댓글 크롤링 실패 - {e}")
                continue
            else: 
                all_comments.append(comment_data)

        try: 
            hit = soup.find("div", class_="post_author").find("span", class_="view_count").find("strong").text
            hit = int(hit)
        except: 
            hit = post["view"]
        
        try:
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
        except Exception as e:
            print(f"[INFO] clien detail: 게시글 크롤링 실패 - {e}")
            update_status_failed(conn, table_name, post_url, checked_at)
            continue
        futures.append(analysis_executor.submit(analyze_post_with_gpt, post_data))
        executing += 1
        #time.sleep(REQUEST_REST)
        
        #if context.get_remaining_time_in_millis() < REMAINING_TIME_LIMIT:
        #    print(f"[INFO] clien detail: 실행시간이 {REMAINING_TIME_LIMIT}ms 보다 덜 남아서 디테일 파싱을 중지합니다.")
        #    break

        # as_completed를 Request_rest만큼 대기
        # 일단 gpt 처리된 것은 처리
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
                    print(f"CLIEN_Detail: Error processing post: {e}")
                    break
        except TimeoutError:
            print("gpt timeout! get next page...")


    for future in as_completed(futures):
        try:
            post_data = future.result()
            if post_data:
                all_post.append(post_data)
                update_changed_stats(conn, table_name, post_data['url'], post_data['comment_count'], post_data['view'], post_data['created_at'])
                print(f"{post_data['url']} - Done!")
        except Exception as e:
            print(f"Error processing post: {e}")

    if len(all_post) == 0:
        return return_dict
    save_res = save_s3_bucket_by_parquet(checked_at, platform='clien', data=all_post, id=lambda_id)
    if save_res is None:
        return {
            "status_code": 500,
            "body": "[FATAL ERROR] DETAIL / S3 저장 실패"
        }
    if return_dict["status_code"] != 204:
        return return_dict
    else:
        return {
            "status_code": 200,
            "body": "[INFO] DETAIL / S3 저장 성공"
        }

detail({'id':1 , "checked_at": "2000-01-01T19:01:01"}, None)
