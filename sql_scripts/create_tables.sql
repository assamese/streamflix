CREATE TABLE IF NOT EXISTS events_table (
    event_id VARCHAR(255) PRIMARY KEY,
    event_ts BIGINT,
    user_id VARCHAR(255),
    content_id VARCHAR(255),
    event_type VARCHAR(50),
    playback_position INTEGER,
    device VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS users_dim (
    user_id VARCHAR(10) PRIMARY KEY,
    email VARCHAR(100) NOT NULL UNIQUE,
    plan_tier VARCHAR(10) NOT NULL,
    signup_date TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS content_dim (
    content_id VARCHAR(10) PRIMARY KEY,
    genre VARCHAR(20) NOT NULL,
    runtime INTEGER NOT NULL,
    release_year INTEGER NOT NULL,
    maturity_rating VARCHAR(5) NOT NULL
);


CREATE TABLE IF NOT EXISTS enriched_table (
    event_id VARCHAR(255) PRIMARY KEY,
    event_ts BIGINT,
    user_id VARCHAR(10),
    email VARCHAR(100),
    plan_tier VARCHAR(10),
    signup_date TIMESTAMP,
    content_id VARCHAR(10),
    genre VARCHAR(20),
    runtime INTEGER,
    release_year INTEGER,
    maturity_rating VARCHAR(5),
    event_type VARCHAR(50),
    playback_position INTEGER,
    device VARCHAR(50)
);
