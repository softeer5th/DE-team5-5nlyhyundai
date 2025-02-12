-- 테이블 조회
SELECT * FROM pg_catalog.pg_tables 
WHERE schemaname = 'public';

-- 데이터베이스 조회
SELECT datname FROM pg_database;

SHOW transaction_isolation;