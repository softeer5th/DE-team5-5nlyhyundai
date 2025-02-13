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