import requests
from bs4 import BeautifulSoup
import re
from typing import List, Dict, Optional
from datetime import datetime

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
    update_status_changed,
    update_status_unchanged,
    update_changed_stats,
)

import time
from datetime import datetime, timedelta

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
                payload['post_id'] = url.split('No=')[1]

            # 각 li 안에서 dd > span 찾기
            try:    
                spans = li.find('dd', class_='path').find_all('span')
                # span 태그 파싱
                if spans[0].text == 'news':
                    print(f"뉴스: {title}, url: {payload['url']}")
                    # continue
                payload['category'] = spans[0].text
                payload['writer'] = spans[1].text
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

            # search_data.append(payload)


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
    except:
        print(f"[ERROR] 좋아요 파싱 실패: {e}")
        like = -999
    date_raw = count_group.text
    date_str = date_raw.split('|')[-1].strip()
    try:
        posting_datetime = clean_date_string(date_str)
    except Exception as e:
        print(f"[ERROR] 날짜 파싱 실패: {e} / 재시도...")
        raise e
    post['view'] = view
    try:
        post['like'] = like # like 등은 여기서 처리해야 함.        
    except Exception as e:
        print(f"[ERROR] 좋아요 수 파싱 실패: {e}")
        post['like'] = -999
    post['created_at'] = posting_datetime

def parse_detail() -> Optional[List[Dict]]:
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
        # 'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,/;q=0.8,application/signed-exchange;v=b3;q=0.7'
        }
    conn = get_db_connection()
    if conn is None:
        print("[ERROR] DB 연결 실패")
        return 
    
    # DB에서 상세 정보를 가져올 게시물 목록
    table_name = 'probe_bobae'
    details = get_details_to_parse(conn, table_name)
    if details is None:
        print("[ERROR] DB 조회 실패")
        return
    
    if details == []:
        print("[INFO] 파싱할 게시물이 없습니다.")
        return

    for post in details:
        post_url = post['url']
        print(f"요청 플랫폼: 보배드림 / '{post_url}' 검색 중...")

        # 상세 페이지 요청
        time.sleep(1)
        response = requests.get(
                post_url,
                headers=headers
            )
        response.encoding = 'utf-8'

        # cookies = response.cookies
        # response_headers = response.headers

        if response.status_code != 200:
            print(f"[ERROR] 확인 필요! status code: {response.status_code}")
            update_status_banned(conn, table_name, post['url'])
            return None
                
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
        
        post['comment_count'] = comment_count

        # 본문 내용 파싱
        content_element = soup.find('div', class_='bodyCont')
        try:
            cleaned_content = content_element.text.strip().replace('\xa0', ' ')
            cleaned_content = linebreak_ptrn.sub('\n', cleaned_content)
            post['content'] = cleaned_content
        except Exception as e:
            print(f"[ERROR] 본문 내용 파싱 실패: {e}")
        
        post_meta = soup.find('div', class_='writerProfile').find('dl')
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
                        # 'created_at': None,
                        # 'content': None,
                        # 'like': None,
                        # 'dislike': None
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
                    comment_date = comment_meta[3].text
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
                    comment_data.append({})
                    continue

        post['comment'] = comment_data
        is_success = update_changed_stats(conn, table_name, post['url'], post['comment_count'], post['view'], post['created_at'])
        if is_success:
            print(f"[INFO] {post['url']} 업데이트 성공")

    

    # 사실 metadata가 아니라 전체 데이터임.
    return details        
    
# 데이터 저장 경로 설정
# s3 = boto3.client("s3")
# bucket_name = "mysamplebucket001036"
# data_dir = "NYC_TLC_Trip_Data/"  # 디렉토리 경로

# response = requests.get(file_url, stream=True)
# if response.status_code == 200:
#     s3.put_object(Bucket=bucket_name, Key=f"{data_dir}{file_name}", Body=response.content)
        

if __name__ == '__main__':
    # python -m bobaedream.bobaedream_exec 로 실행
    # 이게 크롤링한 시간.
    checked_at = datetime.now()
    # 게시글 시작 날짜
    start_date = '2025.02.11'
    # 게시글 시작 날짜로부터 2주치 데이터를 뽑아냄.
    start_dt = datetime.strptime(start_date, '%Y.%m.%d')
    end_dt = start_dt + timedelta(days=14)
    # keyword_list = ['벤츠 배터리 화재']
    keyword_list = ['벤츠', '배터리', '화재']
    dump_list = []
    for keyword in keyword_list:
        for i in range(1, 1000):
            html = extract_bobaedream(start_date, page_num=i, keyword=keyword)
            # save_html('htmls/search', html)
            has_done = parse_search(html, start_dt=start_dt, end_dt=end_dt, checked_at=checked_at, keyword=keyword)   
            details = parse_detail()
            if details is not None:
                dump_list.extend(details)
            
            if has_done:
                print(f"[INFO] 보배드림 검색 종료: {keyword}")
                break

    save_s3_bucket_by_parquet(
        checked_at,
        platform='bobaedream', 
        details_data=dump_list
    )

    