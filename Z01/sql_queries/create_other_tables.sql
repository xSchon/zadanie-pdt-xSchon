CREATE TABLE IF NOT EXISTS conversations(
  id BIGINT PRIMARY KEY UNIQUE NOT NULL,
  author_id BIGINT NOT NULL,
  content TEXT NOT NULL,
  possibly_sensitive BOOLEAN NOT NULL,
  language VARCHAR(3) NOT NULL,
  source TEXT NOT NULL,
  retweet_count INT,
  reply_count INT,
  like_count INT,
  quote_count INT,
  created_at TIMESTAMPTZ NOT NULL,

  FOREIGN KEY (author_id)
    REFERENCES authors(id)
);

CREATE TABLE IF NOT EXISTS hashtags(
    id BIGINT PRIMARY KEY UNIQUE NOT NULL,
    tag TEXT UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS conversation_hashtags(
    id BIGINT PRIMARY KEY UNIQUE NOT NULL,
    conversation_id BIGINT NOT NULL,
    hashtag_id BIGINT NOT NULL,

    FOREIGN KEY (conversation_id)
     REFERENCES conversations(id),
    FOREIGN KEY (hashtag_id)
     REFERENCES hashtags(id)
);

CREATE TABLE IF NOT EXISTS conversation_references(
    id BIGINT PRIMARY KEY UNIQUE NOT NULL,
    conversation_id BIGINT NOT NULL,
    parent_id BIGINT NOT NULL,
    type VARCHAR (20) NOT NULL,

    FOREIGN KEY (conversation_id)
     REFERENCES conversations(id),
    FOREIGN KEY (parent_id)
     REFERENCES conversations(id)


);

CREATE TABLE IF NOT EXISTS links(
    id BIGINT PRIMARY KEY UNIQUE NOT NULL,
    conversation_id BIGINT NOT NULL,
    url VARCHAR(2048) NOT NULL,
    title TEXT,
    description TEXT,

    FOREIGN KEY (conversation_id)
     REFERENCES conversations(id)
);

CREATE TABLE IF NOT EXISTS context_domains(
    id BIGINT PRIMARY KEY UNIQUE NOT NULL,
    name VARCHAR(255) NOT NULL,
    description TEXT
);

CREATE TABLE IF NOT EXISTS context_entities(
    id BIGINT PRIMARY KEY UNIQUE NOT NULL,
    name VARCHAR(255) NOT NULL,
    description TEXT
);

CREATE TABLE IF NOT EXISTS context_annotations(
    id BIGINT PRIMARY KEY UNIQUE NOT NULL,
    conversation_id BIGINT NOT NULL,
    context_domain_id BIGINT NOT NULL,
    context_entity_id BIGINT NOT NULL,
    FOREIGN KEY (conversation_id)
     REFERENCES conversations(id),
    FOREIGN KEY (context_domain_id)
     REFERENCES context_domains(id),
    FOREIGN KEY (context_entity_id)
     REFERENCES context_entities(id)
);

    CREATE TABLE IF NOT EXISTS annotations(
        id BIGINT PRIMARY KEY UNIQUE NOT NULL,
        conversation_id BIGINT NOT NULL,
        value TEXT NOT NULL,
        type TEXT NOT NULL,
        probability NUMERIC(4, 3) NOT NULL,
        FOREIGN KEY (conversation_id)
         REFERENCES conversations(id)
    );
    