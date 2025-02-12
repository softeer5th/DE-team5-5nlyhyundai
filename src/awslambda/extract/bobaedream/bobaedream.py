import requests
from bs4 import BeautifulSoup
import os
import re
import boto3
import json
from awslambda.extract.common_utils import (
    clean_date_string, 
    prepare_for_spark,
)
import time
from datetime import datetime, timedelta

import pyarrow as pa
import pyarrow.parquet as pq


linebreak_ptrn = re.compile(r'(\n){2,}')  # 줄바꿈 문자 매칭

# 이후에 막히면
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

def save_html(base_dir, html):
    print(f"html 저장 경로: '{base_dir}'")
    os.makedirs(base_dir, exist_ok=True)
    with open(f'{base_dir}/bobaedream.html', 'w', encoding='utf-8') as f:
        f.write(html)

def parse_search(html, start_dt, end_dt):
    soup = BeautifulSoup(html, 'html.parser', from_encoding='utf-8')
    community_results = soup.find_all('div', class_='search_Community')
    search_metadata = []
    if not community_results:
        print('검색 결과가 없거나, 에러가 발생했습니다.')
        return
    
    links = []
    for community_result in community_results:
        # ul 태그들 찾기
        lis = community_result.find_all('li')
        for li in lis:
            data = {}
            # 각 li 안에서 dt > a 찾기
            a_tag = li.find('dt').find('a')
            if not a_tag:
                print('a 태그가 없습니다.')
                continue
            # 타이틀과 링크 파싱
            title = a_tag.text.strip()
            link = a_tag['href']
            
            data['title'] = title
            data['link'] = f"https://www.bobaedream.co.kr{link}"
            data['post_id'] = link.split('No=')[1]
            links.append(link)

            dd_path = li.find('dd', class_='path')
            if not dd_path:
                print('dd 태그가 없습니다.')
                continue
            
            # span 태그 파싱
            spans = dd_path.find_all('span')
            if spans[0].text == 'news':
                print(f'뉴스: {title}, link: {link}')
                # continue
            data['category'] = spans[0].text
            data['writer'] = spans[1].text
            created_at = spans[2].text
            data['created_at'] = created_at
            
            created_at_dt = datetime.strptime(created_at, '%y. %m. %d')
                # 2000년대로 가정
            if created_at_dt.year < 100:
                created_at_dt = created_at_dt.replace(year=created_at_dt.year + 2000)

            if created_at_dt < start_dt or created_at_dt > end_dt:
                print(f'기간이 맞지 않습니다. {created_at} / 게시글 날짜: {created_at_dt}')
                continue

            if data['category'] == '내차사진':
                print(f'[WARNING] 내차사진이어서 스킵합니다. {title}')
                continue

            search_metadata.append(data)

    return search_metadata, links

def parse_detail(search_metadata, links):
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
    for metadata, link in zip(search_metadata, links):
        parsed_link = metadata['link']
        print(f"요청 플랫폼: 보배드림 / 글 제목: {metadata['title']}에서 '{parsed_link}' 검색 중...")
        time.sleep(1)
        response = requests.get(
                parsed_link,
                headers=headers
            )
        response.encoding = 'utf-8'
        # cookies = response.cookies
        # response_headers = response.headers
        if response.status_code != 200:
            print(f"확인 필요! status code: {response.status_code}")
            return None
                
        soup = BeautifulSoup(response.text, 'html.parser', from_encoding='utf-8')
        no_comment_flag = False
        try:
            comment_list = soup.find('div', id='cmt_list').find('ul', class_='basiclist').find_all('li')
            comment_count = len(comment_list)
        except Exception as e:
            print(f"댓글이 없습니다. {e}")
            no_comment_flag = True
            comment_count = 0

        
        # TODO: 여기서 변화 없으면 바로 QUIT
        metadata['comment_count'] = comment_count
        content = soup.find('div', class_='bodyCont').text.strip()

        content = content.replace('\xa0', ' ')
        content = linebreak_ptrn.sub('\n', content)

        # 본문 내용
        metadata['content'] = content
        # 포스팅 메타데이터 추가
        posting_meta = soup.find('div', class_='writerProfile').find('dl')
        title = posting_meta.find('dt')['title']
        metadata['title'] = title
        count_group = posting_meta.find('span', class_='countGroup')
        count_group_em = count_group.find_all('em')
        view = count_group_em[0].text
        like = count_group_em[2].text
        date_str = count_group.text
        date_str = date_str.split('|')[2].strip()
        posting_datetime = clean_date_string(date_str)
        metadata['view'] = view
        metadata['like'] = like
        metadata['created_at'] = posting_datetime
        
        # 댓글 처리
        comment_data = []
        if no_comment_flag:
            metadata['comment'] = comment_data
            continue

        for comment in comment_list: # 
            if "삭제된 댓글입니다" in comment.text:
                comment_data.append({
                    # 'created_at': None,
                    # 'content': None,
                    # 'like': None,
                    # 'dislike': None
                })
                continue
            comment_meta = comment.find('dl').find('dt').find_all('span')

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

        metadata['comment'] = comment_data
    

    # 사실 metadata가 아니라 전체 데이터임.
    return search_metadata        
    
# 데이터 저장 경로 설정
# s3 = boto3.client("s3")
# bucket_name = "mysamplebucket001036"
# data_dir = "NYC_TLC_Trip_Data/"  # 디렉토리 경로

# response = requests.get(file_url, stream=True)
# if response.status_code == 200:
#     s3.put_object(Bucket=bucket_name, Key=f"{data_dir}{file_name}", Body=response.content)
        

if __name__ == '__main__':
    start_date = '2024.08.01'
    start_dt = datetime.strptime(start_date, '%Y.%m.%d')
    end_dt = start_dt + timedelta(days=14)
    # 이후 2주치의 데이터를 뽑아냄.
    # keyword_list = ['벤츠', 'BMW', '아우디', '포르쉐', '람보르기니']
    # keyword_list = ['벤츠 배터리 화재', 'BMW', '아우디', '포르쉐', '람보르기니']
    keyword_list = ['벤츠 배터리 화재']
    dump_list = []
    for keyword in keyword_list:
        for i in range(1, 1000):
            html = extract_bobaedream(start_date, page_num=i, keyword=keyword)
            save_html('htmls/search', html)
            metadata, link = parse_search(html, start_dt=start_dt, end_dt=end_dt)   
            dump_list.extend(parse_detail(metadata, link))   

    prepare_for_spark(dump_list, 'parquets/')