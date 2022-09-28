CREATE TABLE IF NOT EXISTS conversations(
  id BIGINT PRIMARY KEY UNIQUE NOT NULL,
  author_id BIGINT,
  content TEXT,
  possibly_sensitive BOOLEAN,
  language VARCHAR(3),
  source TEXT,
  retweet_count INT NOT NULL,
  reply_count INT NOT NULL,
  like_count INT NOT NULL,
  quote_count INT NOT NULL,
  created_at TIMESTAMPTZ,

  FOREIGN KEY (author_id)
    REFERENCES authors(id)
);

CREATE TABLE IF NOT EXISTS hashtags(
    id BIGINT PRIMARY KEY UNIQUE NOT NULL,
    tag TEXT UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS conversation_hashtags(
    id BIGINT PRIMARY KEY UNIQUE NOT NULL,
    conversation_id BIGINT,
    hashtag_id BIGINT,

    FOREIGN KEY (conversation_id)
     REFERENCES conversations(id),
    FOREIGN KEY (hashtag_id)
     REFERENCES hashtags(id)
);

CREATE TABLE IF NOT EXISTS conversation_references(
    id BIGINT PRIMARY KEY UNIQUE NOT NULL,
    conversation_id BIGINT,
    parent_id BIGINT,
    type VARCHAR (20),

    FOREIGN KEY (conversation_id)
     REFERENCES conversations(id),
    FOREIGN KEY (parent_id)
     REFERENCES conversations(id)


);

CREATE TABLE IF NOT EXISTS links(
    id BIGINT PRIMARY KEY UNIQUE NOT NULL,
    conversation_id BIGINT,
    value TEXT,
    type TEXT,
    probability NUMERIC(4, 3),

    FOREIGN KEY (conversation_id)
     REFERENCES conversations(id)
);

CREATE TABLE IF NOT EXISTS context_domains(
    id BIGINT PRIMARY KEY UNIQUE NOT NULL,
    name VARCHAR(255),
    description TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS context_entities(
    id BIGINT PRIMARY KEY UNIQUE NOT NULL,
    name VARCHAR(255),
    description TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS context_annotations(
    id BIGINT PRIMARY KEY UNIQUE NOT NULL,
    conversation_id BIGINT,
    context_domain_id BIGINT,
    context_entity_id BIGINT,
    FOREIGN KEY (conversation_id)
     REFERENCES conversations(id),
    FOREIGN KEY (context_domain_id)
     REFERENCES context_domains(id),
    FOREIGN KEY (context_entity_id)
     REFERENCES context_entities(id)
);

CREATE TABLE IF NOT EXISTS annotations(
    id BIGINT PRIMARY KEY UNIQUE NOT NULL,
    conversation_id BIGINT,
    value TEXT,
    type TEXT,
    probability NUMERIC(4, 3),
    FOREIGN KEY (conversation_id)
     REFERENCES conversations(id)                                      
);