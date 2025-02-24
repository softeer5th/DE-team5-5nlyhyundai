# RDS (PostgreSQL)

## RDS 사용 이유:
### 서치 크롤러가 수집한 데이터를 저장하는 RDS
**-> 디테일 크롤러가 이 데이터를 이용하여 분산 크롤링을 수행합니다.**

0. 모니터링할 사람이 keyword_set 테이블에 모니터링 주제에 대해 키워드 세트를 입력합니다.
1. 서치 크롤러가 키워드 세트를 이용하여 커뮤니티 분산 크롤링을 수행합니다.
2. 크롤링한 데이터를 RDS에 저장합니다.
3. 디테일 크롤러가 RDS에 저장된 데이터를 이용하여 분산 디테일 크롤링을 수행합니다.
4. 디테일 크롤링한 데이터를 S3에 저장합니다.

## 테이블 목록
- `probe_bobae` : 보배드림 커뮤니티 크롤링 메타데이터
- `probe_dcmotors` : 디씨 자동차 갤러리 크롤링 메타데이터
- `probe_clien` : 클리앙 커뮤니티 크롤링 메타데이터
- `keyword_set` : 서치 크롤러가 검색할 키워드 세트
- `crawling_metadata` : 크롤링 메타데이터. S3에 저장할 때 로깅하는 용도로 사용합니다.

## 테이블 생성 쿼리
- 보배드림의 경우. 다른 커뮤니티도 동일한 구조로 테이블을 생성합니다.
```sql
CREATE TABLE probe_bobae (
     id SERIAL PRIMARY KEY,
    url VARCHAR(1022) NOT NULL,
    keywords TEXT[]NOT NULL,
    post_id VARCHAR(255) NOT NULL,
    status VARCHAR(30) NOT NULL,
    comment_count INT NOT NULL,
    created_at TIMESTAMP NOT NULL,
    checked_at TIMESTAMP NOT NULL,
    view INT NOT NULL
);
```

## 인덱스
- 모든 커뮤니티 테이블은 url 기반 조회를 기본적으로 진행하기에 `url` 컬럼에 인덱스를 생성합니다.
```sql
CREATE INDEX probe_bobae_url_idx ON probe_bobae(url);
```

## 트리거
- keywords 컬럼이 변경될 때 마다, 트리거를 통해 중복을 제거합니다.
- 커뮤니티 테이블, 키워드 세트 테이블 모두에 적용합니다.
```sql
CREATE OR REPLACE FUNCTION unique_array()
RETURNS TRIGGER AS $$
BEGIN
    NEW.keywords = ARRAY(SELECT DISTINCT UNNEST(NEW.keywords));
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER ensure_unique_keywords_bobae
    BEFORE INSERT OR UPDATE ON probe_bobae
    FOR EACH ROW
    EXECUTE FUNCTION unique_array();
```



