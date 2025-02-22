import os
import json
import time
import queue
import threading
from typing import List, Dict, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from bs4 import BeautifulSoup
from selenium.webdriver import Chrome
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options as ChromeOptions
from tempfile import mkdtemp
from selenium.webdriver.common.by import By
from common_utils import (
    get_db_connection, get_details_to_parse, upsert_post_tracking_data,
    save_s3_bucket_by_parquet, update_status_changed, update_changed_stats,
    analyze_post_with_gpt, update_status_failed
)

# 멀티스레드를 위한 설정
analysis_executor = ThreadPoolExecutor(max_workers=20)

def setup_webdriver():
    """웹드라이버 설정 및 실행"""
    chrome_options = ChromeOptions()
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--disable-setuid-sandbox")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--disable-dev-tools")
    chrome_options.add_argument("--no-zygote")
    chrome_options.add_argument("--single-process")
    chrome_options.add_argument(f"--user-data-dir={mkdtemp()}")
    chrome_options.add_argument(f"--data-path={mkdtemp()}")
    chrome_options.add_argument(f"--user-data-dir={mkdtemp()}")
    chrome_options.add_argument("--remote-debugging-pipe")
    chrome_options.add_argument("--verbose")
    chrome_options.add_argument("--log-path=/tmp")

    # Mac 환경 특화 설정 추가
    chrome_options.add_argument("--disable-notifications")
    chrome_options.add_argument("--ignore-certificate-errors")
    chrome_options.add_argument("--disable-popup-blocking")

    prefs = {
        "profile.managed_default_content_settings.images": 2,
        "profile.managed_default_content_settings.ads": 2,
        "profile.managed_default_content_settings.media": 2,
        "profile.default_content_setting_values.notifications": 2,
        "profile.default_content_setting_values.plugins": 2
    }
    chrome_options.add_experimental_option("prefs", prefs)
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option("useAutomationExtension", False)

    try:
        chrome_options.binary_location = "/opt/chrome/chrome-linux64/chrome"
        service = Service("/opt/chrome-driver/chromedriver-linux64/chromedriver")
        driver = Chrome(service=service, options=chrome_options)
        driver.set_page_load_timeout(30)
        return driver
    except Exception as e:
        print(f"❌ 웹드라이버 실행 실패: {str(e)}")
        raise e

def crawl_post(driver, post, checked_at):
    """웹 크롤링 수행 후 데이터를 감성 분석으로 넘김"""
    try:
        url = post["url"]
        print(f"📝 크롤링 시작: {url}")
        driver.get(url)
        time.sleep(1)  # 크롤링 간격 조정하여 IP 차단 방지

        soup = BeautifulSoup(driver.page_source, "html.parser")

        title = soup.title.text.strip() if soup.title else "제목 없음"
        content = soup.select_one("div.write_div").text.strip() if soup.select_one("div.write_div") else "본문 없음"
        views = int(soup.select_one("span.gall_count").text.strip().replace("조회 ", "")) if soup.select_one("span.gall_count") else post['view']
        likes = int(soup.select_one("div.up_num_box p.up_num").text.strip()) if soup.select_one("div.up_num_box p.up_num") else 0
        dislikes = int(soup.select_one("div.down_num_box p.down_num").text.strip()) if soup.select_one("div.down_num_box p.down_num") else 0
        comments_count = int(soup.select_one("span.gall_comment").text.strip().replace("댓글 ", "")) if soup.select_one("span.gall_comment") else post['comment_count']
        post_id = post.get("post_id", None)
        keywords = post.get("keywords", [])
        created_at = post["created_at"]

        # 🔹 댓글 크롤링
        def _get_post_comments():
            comment_elements = soup.select("li.ub-content div.clear.cmt_txtbox p.usertxt.ub-word")
            comments = []

            for el in comment_elements:
                comment_text = el.text.strip() if el else "댓글 없음"
                comment_date = created_at  # 기본값

                comments.append({
                    "created_at": comment_date,
                    "content": comment_text,
                    "like": None,
                    "dislike": None
                })

            return comments

        comments = _get_post_comments()

        # 🔹 크롤링 데이터 저장 후 즉시 감성 분석으로 넘김
        temp_post = {
            "checked_at": checked_at,
            "platform": "dcinside",
            "title": title,
            "post_id": post_id,
            "url": url,
            "content": content,
            "view": views,
            "created_at": created_at,
            "like": likes,
            "dislike": dislikes,
            "comment_count": comments_count,
            "comment": comments,
            "keywords": keywords,
            "status": 'UNCHANGED'
        }
        
        print(f"✅ 크롤링 완료 및 감성 분석 시작: {url}")
        
        return temp_post

    except Exception as e:
        print(f"❌ 크롤링 실패: {e}")

def analyze_post(post):
    """크롤링된 데이터를 감성 분석 수행"""
    try:
        print(f"🎭 감성 분석 시작: {post['url']}")
        analyzed_post = analyze_post_with_gpt(post)
        print(f"✅ 감성 분석 완료: {post['url']}")
        return analyzed_post
    
    except Exception as e:
        print(f"❌ 감성 분석 오류: {e}")
        post['sentiment'] = None
        for comment in post['comment']:
            comment['sentiment'] = None
        return post
    
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

def lambda_handler(event, context):
    id = event.get('id')
    checked_at = event.get('checked_at')
    checked_at = datetime.strptime(checked_at, "%Y-%m-%dT%H:%M:%S")

    start_time = time.time()
    """AWS Lambda에서 실행되는 핸들러 함수"""
    driver = setup_webdriver()
    conn = get_db_connection()
    
    if conn is None:
        print("🔴 DB 연결 실패")
        raise "dc detail: 500 - DB 연결 실패"
        return {"status_code": 500, "body": "DB 연결 실패"}

    table_name = event.get("table_name", "probe_dcmotors")

    current_batch = []
    BATCH_SIZE = 50
    crawled_post = []

    # ✅ 크롤링 → 감성 분석 즉시 실행 (순차 처리)
    while True:
        post = get_details_to_parse(conn, table_name)

        if post is None :
            raise "dc detail: 500 - DB 연결 실패"
            return {
            "status_code": 500,
            "body": "[ERROR] DETAIL / DB 연결 실패"
            }
        
        if post == [] :
            break

        temp_post = crawl_post(driver, post, checked_at)  # 크롤링과 동시에 감성 분석 스레드 실행

        is_success = update_changed_stats(conn, table_name, post['url'], post['comment_count'], post['view'], post['created_at'])            
        if is_success:
            print(f"[INFO] {post['url']} 업데이트 성공")
        else:
            print(f"[INFO] {post['url']} 업데이트 실패")

        # 모든 포스트에 대해 분석 작업 제출
        future = analysis_executor.submit(analyze_post, temp_post)
        current_batch.append(future)

        if time.time() - start_time > 840 :
            print("14분 경과")
            break

        # 완료된 작업들의 결과를 수집
        if len(current_batch) >= BATCH_SIZE:
            print(f"배치 처리 시작 (크기: {len(current_batch)})")
            batch_results = process_batch(current_batch)
            crawled_post.extend(batch_results)
            current_batch = []

    if current_batch:
        print(f"배치 처리 시작 (크기: {len(current_batch)})")
        batch_results = process_batch(current_batch)
        crawled_post.extend(batch_results)
        current_batch = []

    try:
        save_s3_bucket_by_parquet(
            checked_at_dt=checked_at,
            platform="dcinside",
            data=list(crawled_post),
            id = id
        )
    except Exception as e:
        print(f"[ERROR] S3 저장 실패: {e}")
        conn = get_db_connection()
        for failed_post in crawled_post:
            update_status_failed(conn, table_name, failed_post['url'])
        raise "dc detail: 500 - S3 저장 실패"
        return {
            'status_code': 500,
            'body': '[ERROR] S3 저장 실패'
        }

    driver.quit()
    return {"status_code": 200, "body": json.dumps({"body": "[INFO] DETAIL / S3 저장 성공"})}

