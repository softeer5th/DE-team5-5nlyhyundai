import re
import os
from datetime import datetime

import pyarrow as pa
import pyarrow.parquet as pq

dofw_ptrn = re.compile(r'\([월화수목금토일]\)')  # 한글 요일만 매칭

def clean_date_string(date_str):
    # HTML 엔티티 제거
    date_str = date_str.replace('&nbsp;', ' ')
    # 요일 제거
    date_str = dofw_ptrn.sub('', date_str)
    # 추가 공백 제거
    date_str = ' '.join(date_str.split())
    date_datetime = datetime.strptime(date_str, '%Y.%m.%d %H:%M')
    return date_datetime


def prepare_for_spark(data_list, base_path):
    # 메인 포스트 데이터
    posts = []
    # 코멘트 데이터
    comments = []
    
    for post in data_list:
        # 코멘트 분리
        post_comments = post.pop('comment', [])
        # post_id를 기준으로 연결
        for comment in post_comments:
            comment['post_id'] = post['post_id']
            comments.append(comment)
        posts.append(post)
    
    # 각각 Parquet로 저장
    posts_table = pa.Table.from_pylist(posts)
    comments_table = pa.Table.from_pylist(comments)
    
    pq.write_table(posts_table, f'{base_path}bobaedream_posts.parquet', compression='snappy')
    pq.write_table(comments_table, f'{base_path}bobaedream_comments.parquet', compression='snappy')

def save_html(base_dir, html):
    print(f"html 저장 경로: '{base_dir}'")
    os.makedirs(base_dir, exist_ok=True)
    with open(f'{base_dir}/bobaedream.html', 'w', encoding='utf-8') as f:
        f.write(html)


def post_id_salt(post_id, category):
    category_salt = {
        "베스트글": "a",
        "자유게시판": "b",
        "정치게시판": "c",
        "시승기·배틀·목격담": "d",
        "유명인의 차": "e",
        "자동차뉴스": "f",
        "보배드림 이야기": "g",
        "제안·건의": "h",
        "이벤트": "i",
        "유머게시판": "j",
        "교통사고·블박": "k",
        "국산차게시판": "m",
        "수입차게시판": "n",
        "내차사진": "l",
        "직찍·특종발견": "o",
        "자동차사진·동영상": "p",
        "장착시공사진": "q",
        "자유사진·동영상": "r",
        "레이싱모델": "s",
        "군사·무기": "t",
        "트럭·버스·중기": "u",
        "항공·해운·철도": "v",
        "오토바이·자전거": "w",
        "올드카·추억거리": "x",
    }
    return category_salt[category] + post_id