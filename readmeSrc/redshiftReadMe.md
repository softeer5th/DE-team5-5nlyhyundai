# Load at Redshift

EMR이 주기적으로 들어온는 데이터를 처리한다면, Redshift는 EMR이 처리한 데이터를 한데 모아두고 전체 데이터를 처리하는데 사용합니다.

## 설정하기

AWS Redshift serverless로 설정합니다.

다음 설정과 권한이 필요합니다.

* 시각화 도구(superset)와 연동을 위해 외부 연결이 가능한 VPC에 설치되어 있어야 합니다.

* IAM S3 접근 권한이 필요합니다.

* 외부 연결에서 데이터 베이스 연결을 위해 namespace > actions > Edit admin credentials에 접근하여 admin ID, 비밀번호를 설정합니다.
  > 해당 계정을 바로 데이터 베이스에 연결하거나, 해당 계정으로 데이터 레드쉬프트에 접근하여 새 계정을 만들고 권한을 부여해서 레드쉬프트에 접근합니다. (postgres database 연결로 연결 가능합니다.)

* 에어플로우가 Redshift를 자동으로 관리해서 특별한 소스코드 설정은 필요 없습니다.

## 스키마

레드쉬프트에서는 EMR이 짧은 간격으로 처리한 데이터를 한데 모아 전체 데이터를 처리합니다. 다음과 같은 형태로 키워드 별로 테이블을 만들어 자료를 저장합니다:

### summary
EMR 각 키워드 처리 결과를 테이블로 저장합니다.

하나의 행은 한 배치 주기동안 게시된 게시물(포스트 + 댓글)의 통계데이터입니다.

| Column               | Type                    | 설명                                     |
|----------------------|-------------------------|-----------------------------------------|
| created_at           | timestamp w/o time zone | 해당 열이 생긴 시각입니다.                    |
| total_views          | bigint                  | 배치 기간동안의 게시글 총 조회수의 합입니다.       |
| total_posts_comments | bigint                  | 배치 기간동안의 게시글 총 게시물의 합입니다.       |
| positive             | bigint                  | 배치 기간동안의 긍정 게시물의 합입니다.           |
| neutral              | bigint                  | 배치 기간동안의 중립 게시물의 합입니다.           |
| negative             | bigint                  | 배치 기간동안의 부정 게시물의 합입니다.           |
| *_count              | bigint                  | 배치 기간동안 *에 해당하는 단어가 언급된 횟수입니다. |

*_count는 조사 키워드 수에 따라 복수일 수 있습니다.

### post
Extract 단계에서 추출한 RAW 데이터입니다.
게시글 하나당 여러개 존재할 수 있습니다. (조회수 갱신을 위해 조회수 차이가 커지면 새로 크롤링함)

| Column     | Type                     | 설명                                           |
|------------|--------------------------|-----------------------------------------------|
| platform   | character varying(255)   | 게시글이 있었던 게시판 종류를 나타냅니다.               |
| title      | character varying(255)   | 게시글 제목입니다.                                 |
| post_id    | character varying(255)   | 게시판 내 게시글 id입니다.                          |
| url        | character varying(65535) | 게시글 페이지로 가는 주소입니다.                      |
| content    | character varying(65535) | 게시글 내용입니다.                                 |
| view       | bigint                   | 조회수 입니다.                                    |
| created_at | timestamp w/o time zone  | 글이 작성된 시각입니다.                             |
| like       | bigint                   | 좋아요 수 입니다.                                 |
| dislike    | bigint                   | 싫어요 수 입니다.                                 |
| keywords   | character varying(255)   | 검색 키워드입니다.                                 |
| sentiment  | character varying(255)   | 키워드에 대한 해당 글의 의견입니다. (긍정, 중립, 부정)    |
| checked_at | timestamp w/o time zone  | 해당 열이 생긴 시각입니다. (글을 조회한 시각과 동일합니다.) |  

### comment
Extract 단계에서 추출한 RAW 데이터입니다.

| Column     | Type                     | 설명                                           |
|------------|--------------------------|-----------------------------------------------|
| post_id    | character varying(255)   | 게시판 내 게시글 id입니다.                          |
| url        | character varying(65535) | 게시글 페이지로 가는 주소입니다.                      |
| content    | character varying(65535) | 게시글 내용입니다.                                 |
| created_at | timestamp w/o time zone  | 글이 작성된 시각입니다.                             |
| like       | bigint                   | 좋아요 수 입니다.                                 |
| dislike    | bigint                   | 싫어요 수 입니다.                                 |
| sentiment  | character varying(255)   | 키워드에 대한 해당 글의 의견입니다. (긍정, 중립, 부정)    |
| checked_at | timestamp w/o time zone  | 해당 열이 생긴 시각입니다. (글을 조회한 시각과 동일합니다.) |  