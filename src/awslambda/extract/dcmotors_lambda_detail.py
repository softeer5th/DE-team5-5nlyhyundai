import os
import json
import time
from datetime import datetime
from bs4 import BeautifulSoup
from selenium.webdriver import Chrome
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options as ChromeOptions
from tempfile import mkdtemp
from selenium.webdriver.common.by import By
from common_utils import get_db_connection, get_details_to_parse, upsert_post_tracking_data, save_s3_bucket_by_parquet, update_status_changed, update_changed_stats

def lambda_handler(event, context):
    # ✅ 웹드라이버 옵션 설정
    chrome_options = ChromeOptions()
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--disable-dev-tools")
    chrome_options.add_argument("--no-zygote")
    chrome_options.add_argument("--single-process")
    chrome_options.add_argument(f"--user-data-dir={mkdtemp()}")
    chrome_options.add_argument(f"--data-path={mkdtemp()}")
    chrome_options.add_argument(f"--disk-cache-dir={mkdtemp()}")
    chrome_options.add_argument("--remote-debugging-pipe")
    chrome_options.add_argument("--verbose")
    chrome_options.add_argument("--log-path=/tmp")
    chrome_options.binary_location = "/opt/chrome/chrome-linux64/chrome"
    prefs = {
        "profile.managed_default_content_settings.images": 2,  # 이미지 비활성화
        "profile.managed_default_content_settings.ads": 2,     # 광고 비활성화
        "profile.managed_default_content_settings.media": 2    # 비디오, 오디오 비활성화
    }
    chrome_options.add_experimental_option("prefs", prefs)

    # ✅ Docker에 미리 설치된 Chrome과 ChromeDriver 경로 설정
    chrome_options.binary_location = "/opt/chrome/chrome-linux64/chrome"
    service = Service("/opt/chrome-driver/chromedriver-linux64/chromedriver")

    # ✅ 웹드라이버 실행
    print("🚀 웹드라이버 실행 중...")
    driver = Chrome(service=service, options=chrome_options)

    # ✅ DB 연결
    conn = get_db_connection()
    if conn is None:
        print("🔴 DB 연결 실패")
        return {"statusCode": 500, "body": "DB 연결 실패"}

    # ✅ 크롤링할 URL 가져오기
    table_name = event.get("table_name", "probe_dcmotors")  # 기본 테이블 이름
    posts_to_crawl = get_details_to_parse(conn, table_name)

    if not posts_to_crawl:
        print("🔴 크롤링할 게시글 없음")
        driver.quit()
        return {"statusCode": 200, "body": "No posts to crawl"}

    print(f"🔍 크롤링할 게시글 수: {len(posts_to_crawl)}")

    # ✅ 크롤링할 데이터 리스트
    crawled_posts = []

    for idx, post in enumerate(posts_to_crawl):
        try:
            url = post["url"]
            print(f"📝 ({idx+1}/{len(posts_to_crawl)}) 게시글 크롤링 시작: {url}")

            driver.get(url)
            time.sleep(1)  # ⏳ 페이지 로딩 대기

            soup = BeautifulSoup(driver.page_source, "html.parser")

            try:
                title = soup.title.text.strip()
            except:
                title = "제목 없음"

            try:
                content = soup.select_one("div.write_div").text.strip()
            except:
                content = "본문 없음"

            try:
                views = int(soup.select_one("span.gall_count").text.strip().replace("조회 ", ""))
            except:
                views = post['view']

            try:
                likes = int(soup.select_one("div.up_num_box p.up_num").text.strip())
            except:
                likes = 0

            try:
                dislikes = int(soup.select_one("div.down_num_box p.down_num").text.strip())
            except:
                dislikes = 0

            try:
                comments_count = int(soup.select_one("span.gall_comment").text.strip().replace("댓글 ", ""))
            except:
                comments_count = post['comment_count']

            try:
                post_id = post['post_id']
            except:
                post_id = None

            keywords = post['keywords']

            created_at = post["created_at"]

            # 🔹 댓글 크롤링
            def _get_post_comments():
                comment_elements = soup.select("li.ub-content div.clear.cmt_txtbox p.usertxt.ub-word")
                comments = []

                for el in comment_elements:
                    try:
                        comment_text = el.text.strip()
                    except:
                        comment_text = "댓글 없음"

                    try:
                        comment_date_str = el.find_parent("li").select_one("div.cmt_info span.date_time").text.strip()
                        if len(comment_date_str) == 14:  # 예: "08-06 11:04:05"
                            comment_date = datetime.strptime(comment_date_str, "%m-%d %H:%M")
                            comment_date = comment_date.replace(year=created_at.year)  # 연도 추가
                        # :흰색_확인_표시: 날짜 문자열이 "YYYY-MM-DD HH:MM" 형식인 경우
                        elif len(comment_date_str) == 19:  # 예: "2024-08-06 11:04:05"
                            comment_date = datetime.strptime(comment_date_str, "%Y-%m-%d %H:%M")
                        else:
                            print(f":x: 날짜 형식 오류: {comment_date_str}")
                    except:
                        comment_date = created_at

                    comments.append({
                        "created_at": comment_date,
                        "content": comment_text,
                        "like" : None,
                        "dislike" : None
                    })

                return comments

            comments = _get_post_comments()

            # 🔹 데이터 저장을 위해 Parquet용 리스트에 추가
            crawled_posts.append({
                "platform": "DC",
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
                "keywords" : keywords
            })

            # 🔹 DB에서 상태 업데이트
            update_result = update_changed_stats(conn, table_name, url, comments_count, views, created_at)

            if update_result:
                print(f"✅ DB 상태 업데이트 완료: {url}")
            else:
                print(f"❌ DB 상태 업데이트 실패: {url}")

        except Exception as e:
            print(f"❌ 게시글 크롤링 오류: {e}")
            continue

    # ✅ 크롤링 데이터 Parquet 저장 (S3 업로드)
    save_result = save_s3_bucket_by_parquet(
        checked_at_dt=posts_to_crawl[0]['checked_at'],
        platform="dcinside",
        data=crawled_posts
    )

    if save_result:
        print("✅ Parquet 파일 S3 저장 완료")
    else:
        print("❌ Parquet 파일 저장 실패")

    # 🔹 브라우저 종료
    driver.quit()

    return {
        "statusCode": 200,
        "body": json.dumps({"message": "Crawling & S3 upload completed"}, ensure_ascii=False, indent=4)
    }