CREATE TABLE IF NOT EXISTS probe_bobe (
    id SERIAL PRIMARY KEY,
    url VARCHAR(1022) NOT NULL,
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
    post_id VARCHAR(255) NOT NULL,
    status VARCHAR(30) NOT NULL,
    comment_count INT NOT NULL,
    created_at TIMESTAMP NOT NULL,
    checked_at TIMESTAMP NOT NULL,
    view INT NOT NULL
);

CREATE TABLE IF NOT EXISTS keyword (
    id SERIAL PRIMARY KEY,
    keyword_set VARCHAR(255) UNIQUE NOT NULL,
    keyword VARCHAR(255) NOT NULL
);