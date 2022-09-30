CREATE TABLE IF NOT EXISTS authors(
    id BIGINT PRIMARY KEY UNIQUE NOT NULL,
    name VARCHAR(255),
    username VARCHAR(255),
    description TEXT,
    followers_count INT,
    following_count INT,
    tweet_count INT,
    listed_count INT
);
