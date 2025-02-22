CREATE TABLE IF NOT EXISTS probe_bobae (
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

CREATE TABLE IF NOT EXISTS probe_dcmotors (
    id SERIAL PRIMARY KEY,
    url VARCHAR(1022) NOT NULL,
    keywords TEXT[] NOT NULL,
    post_id VARCHAR(255) NOT NULL,
    status VARCHAR(30) NOT NULL,
    comment_count INT NOT NULL,
    created_at TIMESTAMP NOT NULL,
    checked_at TIMESTAMP NOT NULL,
    view INT NOT NULL
);

CREATE TABLE IF NOT EXISTS probe_clien (
    id SERIAL PRIMARY KEY,
    url VARCHAR(1022) NOT NULL,
    keywords TEXT[] NOT NULL,
    post_id VARCHAR(255) NOT NULL,
    status VARCHAR(30) NOT NULL,
    comment_count INT NOT NULL,
    created_at TIMESTAMP NOT NULL,
    checked_at TIMESTAMP NOT NULL,
    view INT NOT NULL
);

CREATE TABLE IF NOT EXISTS keyword_set (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) UNIQUE NOT NULL,
    keywords TEXT[] NOT NULL
);

-- 크롤링 결과를 S3에서 추적을 위한 테이블
CREATE TABLE IF NOT EXISTS crawling_metadata (
    id SERIAL PRIMARY KEY,
    checked_at TIMESTAMP NOT NULL,
    keywords_str VARCHAR(255) NOT NULL,
    platform VARCHAR(30) NOT NULL,
    bucket_name VARCHAR(255) NOT NULL,
    posts_s3_path VARCHAR(1022),
    comments_s3_path VARCHAR(1022),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Seoul')
);

-- 프록시 ip를 저장하고 크롤러에게 보내주기 위한 테이블
CREATE TABLE IF NOT EXISTS proxy_ip (
    IP VARCHAR(255) PRIMARY KEY,
    dcmotors VARCHAR(8),
    bobaedream VARCHAR(8),
    clien VARCHAR(8),
    Availability BOOLEAN,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);