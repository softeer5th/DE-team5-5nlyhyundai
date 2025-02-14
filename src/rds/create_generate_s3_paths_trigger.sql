-- s3 경로를 생성하는 트리거
CREATE OR REPLACE FUNCTION generate_s3_paths()
RETURNS TRIGGER AS $$
DECLARE
    base_path TEXT;
BEGIN
    base_path := 's3://' || NEW.bucket_name || '/' || 
                 to_char(NEW.checked_at, 'YYYY-MM-DD') || '/' || 
                 EXTRACT(HOUR FROM NEW.checked_at)::TEXT || '/' ||
                 EXTRACT(MINUTE FROM NEW.checked_at)::TEXT || '/' ||
                 NEW.keywords_str || '/' ||
                 NEW.platform;
                 
    NEW.posts_s3_path := base_path || '_posts.parquet';
    NEW.comments_s3_path := base_path || '_comments.parquet';
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- s3 경로를 생성하는 트리거를 크롤링 메타데이터 테이블에 적용
CREATE TRIGGER set_s3_paths
    BEFORE INSERT ON crawling_metadata
    FOR EACH ROW
    EXECUTE FUNCTION generate_s3_paths();