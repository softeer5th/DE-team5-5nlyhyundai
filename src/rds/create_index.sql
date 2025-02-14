CREATE INDEX idx_probe_bobe_url ON probe_bobe(url);
CREATE INDEX idx_probe_dcmotors_url ON probe_dcmotors(url);
CREATE INDEX idx_probe_clien_url ON probe_clien(url);
-- keyword 테이블 (id는 PRIMARY KEY로, keyword_set은 UNIQUE 제약조건으로 이미 인덱스가 생성됨)
-- 크롤링 메타데이터 테이블
CREATE INDEX idx_crawling_metadata_checked_at ON crawling_metadata(checked_at);