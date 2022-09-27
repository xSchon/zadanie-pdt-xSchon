CREATE TABLE IF NOT EXISTS authors(
    id BIGINT PRIMARY KEY UNIQUE NOT NULL,
    name VARCHAR(255) NOT NULL,
    username VARCHAR(255) NOT NULL,
    description TEXT NOT NULL,
    followers_count INT NOT NULL,
    following_count INT NOT NULL,
    tweet_count INT NOT NULL,
    listed_count INT NOT NULL
);
