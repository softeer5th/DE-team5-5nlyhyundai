# Community Board Crawler

이 디렉토리의 파일은 커뮤니티 게시판 내용물을 스크래이핑 해오는 소스코드를 담고 있습니다.

## 사용 방법

1. Postgres DB를 설정합니다.

크롤러를 돌리기 위해 Postgres 데이터 베이스가 필요합니다. 이를 위한 환경 설정이 필요합니다.
* .env.lambda 파일을 만들어서 다음 내용을 기입하시기 바랍니다.
> DB_HOST=
> DB_NAME=
> DB_USER=
> DB_PASSWORD=
> DB_PORT=
> VIEW_THRESHOLD=100 (기존 게시물 조회수와 새로 읽은 게시물의 조회수가 여기 적힌 수보다 차이가 크면 게시물의 조회수를 업데이트 합니다.)
> S3_BUCKET= (자료가 저장될 버켓 이름입니다.)
> OPENAI_API_KEY= (본 모델에서는 감정 분석을 위해 CHAT GPT를 사용합니다.)

* 데이터 베이스를 열고 본 리포지토리 /rds/create_table.sql에 적힌 쿼리를 실행합니다.

2. 다음 명령어를 통해 도커 이미지를 만들고 AWS ECR로 푸쉬합니다.

* requests 기반 크롤러

```bash
docker buildx build --platform linux/amd64 --provenance=false --push -f requests.Dockerfile -t {ECR 주소}:requests .
```

* selenium 기반 크롤러

```bash
docker buildx build --platform linux/amd64 --provenance=false --push -f selenium.Dockerfile -t {ECR 주소}:selenium .
```

* 문제 해결

만약 ECR에 푸쉬 중 AWS에 로그인이 되어있지 않다는 이야기가 뜨면 다음 코드로 로그인 합니다.

```bash
aws ecr get-login-password --region ap-northeast-2 | docker login --username AWS --password-stdin {ECR 주소}
```

이전에 AWS access key와 secret access key를 컴퓨터에 설정해 놓아야 합니다.

3. AWS Lambda 서비스에 다음 이름과 CMD를 가진 람다를 설정합니다.

|-------------------|----------|-----------------------------------------|
| 람다 이름           | 람다 이미지 | 람다 CMD 오버라이드                         |
|-------------------|----------|-----------------------------------------|
| clienSearch       | requests | clien_lambda_search.search              |
|-------------------|----------|-----------------------------------------|
| clienDetail       | requests | clien_lambda_search.detail              |
|-------------------|----------|-----------------------------------------|
| dcmotors_search   | requests | dcmotors_lambda_search.lambda_handler   |
|-------------------|----------|-----------------------------------------|
| dcmotors_detail   | selenium | (오버라이딩이 필요 없습니다)                   |
|-------------------|----------|-----------------------------------------|
| bobaedream-search | requests | bobaedream_lambda_search.lambda_handler |
|-------------------|----------|-----------------------------------------|
| bobaedream-detail | reqeusts | bobaedream_lambda_detail.lambda_handler |
|-------------------|----------|-----------------------------------------|

각 람다별 요구 메모리 사양은 다음과 같습니다:
|----------|---------------|
| 람다 이미지 | 요구 메모리 (MB) |
|----------|---------------|
| requests | 256           |
|----------|---------------|
| selenium | 1024          |
|----------|---------------|

각 람다는 실행 목적을 달성하면 미리 종료되지만 안정성을 위해 15분으로 설정해주시기 바랍니다.

* 이름이 Detail로 끝나는 람다는 IAM 권한 중 S3에 접속하는 권한이 필요합니다.
* 이상의 작업을 AWS 콘솔에서 진행합니다.



## 실행 로직

5nly HYUNDAI 팀의 Extract 로직은 다음과 크게 다음과 같은 형식으로 작동합니다:

1. Search page crawling
> 게시판의 게시글 목록이 나와있는 페이지를 서치 페이지(Search Page)로 명명합니다.
> 서치 페이지를 읽고 각 게시물 정보를 각각 데이터 베이스에 URL, 조회수, 댓글수 등 알 수 있는 정보를 취합해 넣어줍니다.

2. Detail page crawling
> 게시글 목록에서 게시물 하나를 골라 들어온 페이지를 디테일 페이지(Detail page)로 명명합니다.
> 디테일 페이지를 읽고 서치 페이지 내용에 더해 디테일 페이지에서 얻을 수 있는 글 제목, 내용, 댓글 내용등을 수집해서 AWS S3에 Parquet 형식으로 저장합니다.

3. 실행 제어
> 본 람다들은 AWS MWAA, 에어 플로우(Air Flow)로 관리됩니다.