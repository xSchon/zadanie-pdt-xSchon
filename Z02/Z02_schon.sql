---  Author: Martin Schon ---
---  PDT_Z02              ---
----  S01  ----------------------------------------------------------------
EXPLAIN ANALYZE
    SELECT * FROM authors WHERE username = 'mfa_russia';
----  E01  ----------------------------------------------------------------

----  S02  ----------------------------------------------------------------
ALTER SYSTEM SET
 max_worker_processes = 1024;
ALTER SYSTEM SET
 max_parallel_workers_per_gather = 1024;
ALTER SYSTEM SET
 max_parallel_workers = 1024;
-- restart DB after changing SYSTEM SET

ALTER TABLE authors SET (parallel_workers = 8);
----  E02  ----------------------------------------------------------------

----  S03  ----------------------------------------------------------------
CREATE INDEX IF NOT EXISTS authors_usernames_btree_index ON authors
    USING btree (username);

EXPLAIN ANALYSE
    SELECT * FROM authors WHERE username = 'mfa_russia';
----  E03  ----------------------------------------------------------------

----  S04  ----------------------------------------------------------------
DROP INDEX IF EXISTS authors_followers_count_index;

EXPLAIN ANALYZE
    SELECT * FROM authors WHERE (followers_count >= 100) AND (followers_count <= 200);

EXPLAIN ANALYZE
    SELECT * FROM authors WHERE (followers_count >= 100) AND (followers_count <= 120);

----  E04  ----------------------------------------------------------------

----  S05  ----------------------------------------------------------------
CREATE INDEX IF NOT EXISTS authors_followers_count_index ON authors
    USING btree (followers_count);

EXPLAIN ANALYZE
    SELECT * FROM authors WHERE followers_count BETWEEN 100 AND 200;

EXPLAIN ANALYZE
    SELECT * FROM authors WHERE followers_count BETWEEN 100 AND 120;
----  E05  ----------------------------------------------------------------

----  S06  ----------------------------------------------------------------
CREATE INDEX IF NOT EXISTS authors_description_index ON authors
    USING btree (description);
CREATE INDEX IF NOT EXISTS authors_followers_count_index ON authors
    USING btree (followers_count);
CREATE INDEX IF NOT EXISTS authors_names_btree_index ON authors
    USING btree (name);

INSERT INTO authors (id, name, username, description,
                     followers_count, following_count, tweet_count, listed_count)
VALUES (9, 'Martin Schon', 'xschon',
        'Ziak ktory dostane Acko z PDT <3', 7, 3, 358, 2);

DROP INDEX authors_description_index;
DROP INDEX authors_followers_count_index;
DROP INDEX authors_names_btree_index;

EXPLAIN ANALYZE
INSERT INTO authors (id, name, username, description,
                     followers_count, following_count, tweet_count, listed_count)
VALUES (10, 'Martin Schon', 'xschon',
        'Ziak ktory dostane Acko z PDT <3', 7, 3, 358, 2);

DELETE FROM authors WHERE id = 9 OR id = 10;
----  E06  ----------------------------------------------------------------

----  S07  ----------------------------------------------------------------
CREATE INDEX IF NOT EXISTS conversations_retweet_count_btree_index ON conversations
USING btree (retweet_count);

CREATE INDEX IF NOT EXISTS conversations_content_btree_index ON conversations
USING btree (content);
----  E07  ----------------------------------------------------------------

----  S08  ----------------------------------------------------------------
CREATE EXTENSION pgstattuple;
CREATE EXTENSION pageinspect;

select pg_size_pretty(pg_relation_size('conversations_retweet_count_btree_index'));
select pg_size_pretty(pg_relation_size('conversations_content_btree_index'));

SELECT tree_level, index_size, root_block_no FROM pgstatindex('conversations_retweet_count_btree_index');
SELECT tree_level, index_size, root_block_no FROM pgstatindex('authors_followers_count_index');

SELECT type, avg_item_size, page_size FROM bt_page_stats('conversations_retweet_count_btree_index', 3);
SELECT type, avg_item_size, page_size FROM bt_page_stats('conversations_content_btree_index', 51);
----  E08  ----------------------------------------------------------------

----  S09  ----------------------------------------------------------------
DROP INDEX IF EXISTS conversations_content_btree_index;

EXPLAIN ANALYZE
SELECT content FROM conversations WHERE content LIKE '%Gates%';

CREATE INDEX IF NOT EXISTS conversations_content_btree_index ON conversations
USING btree (content);

EXPLAIN ANALYZE
SELECT * FROM conversations WHERE content LIKE '%Gates%';
----  E09  ----------------------------------------------------------------

----  S10  ----------------------------------------------------------------
EXPLAIN ANALYZE
SELECT content, possibly_sensitive FROM conversations WHERE content LIKE 'There are no excuses%'
                          AND possibly_sensitive;

--- a) Index over possibly_sensitive
CREATE INDEX IF NOT EXISTS conversations_sensitive_index ON conversations
USING btree (possibly_sensitive);
EXPLAIN ANALYZE
SELECT content, possibly_sensitive FROM conversations WHERE content LIKE '%There are no excuses%'
                          AND possibly_sensitive;

--- b) Index over (content, possibly_sensitive)
CREATE INDEX IF NOT EXISTS conversations_con_sen_index ON conversations
    USING btree (content, possibly_sensitive);
EXPLAIN ANALYZE
SELECT content, possibly_sensitive FROM conversations WHERE content LIKE '%There are no excuses%'
                          AND possibly_sensitive;

--- c) Index over (possibly_sensitive, content)
CREATE INDEX IF NOT EXISTS conversations_sen_con_index ON conversations
    USING btree  (possibly_sensitive, content)
EXPLAIN ANALYZE
SELECT content, possibly_sensitive FROM conversations WHERE content LIKE '%There are no excuses%'
                          AND possibly_sensitive;
----  E10  ----------------------------------------------------------------

----  S11  ----------------------------------------------------------------
SELECT * FROM conversations WHERE content LIKE '%https://t.co/pkFwLXZlEm';
SELECT reverse(content) FROM conversations WHERE content LIKE '%https://t.co/pkFwLXZlEm';

CREATE INDEX IF NOT EXISTS reverse_content_index ON conversations
USING btree (reverse(content));

EXPLAIN ANALYZE
SELECT * FROM conversations WHERE reverse(content) LIKE reverse('%https://t.co/pkFwLXZlEm');
----  E11  ----------------------------------------------------------------

----  S12  ----------------------------------------------------------------
EXPLAIN ANALYZE
SELECT * FROM conversations WHERE retweet_count >= 5000
                            AND reply_count >= 150
                            ORDER BY quote_count;

CREATE INDEX IF NOT EXISTS reply_count_index ON conversations
USING btree (reply_count);

CREATE INDEX IF NOT EXISTS retweet_count_index ON conversations
USING btree (retweet_count);

CREATE INDEX IF NOT EXISTS quote_count_index ON conversations
USING btree (quote_count);

EXPLAIN ANALYZE
SELECT * FROM conversations WHERE retweet_count >= 5000
                            AND reply_count >= 150
                            ORDER BY quote_count;
----  E12  ----------------------------------------------------------------

----  S13  ----------------------------------------------------------------
CREATE INDEX IF NOT EXISTS quote_retweet_reply_index ON conversations
USING btree (reply_count, retweet_count, quote_count);

EXPLAIN ANALYZE
SELECT * FROM conversations WHERE retweet_count >= 5000
                            AND reply_count >= 150
                            ORDER BY quote_count;
----  E13  ----------------------------------------------------------------

----  S14  ----------------------------------------------------------------
CREATE INDEX IF NOT EXISTS conversations_gin_content_index ON conversations
    USING gin(to_tsvector('english', content));

EXPLAIN ANALYZE
SELECT *
FROM conversations
WHERE to_tsvector('english', content) @@ to_tsquery('english', 'Putin & New<->World<->Order')
AND possibly_sensitive;

CREATE INDEX IF NOT EXISTS conversations_gist_content_index ON conversations
    USING gist(to_tsvector('english', content));

EXPLAIN ANALYZE
SELECT *
FROM conversations
WHERE to_tsvector('english', content) @@ to_tsquery('english', 'Putin & New<->World<->Order')
AND possibly_sensitive;

SELECT
(SELECT pg_size_pretty(pg_table_size('conversations_gist_content_index'))) AS gist_size,
(SELECT pg_size_pretty(pg_table_size('conversations_gin_content_index'))) AS gin_size;
----  E14  ----------------------------------------------------------------

----  S15  ----------------------------------------------------------------
CREATE INDEX IF NOT EXISTS links_gin_trigram_url_index ON links
    USING gin(url gin_trgm_ops);

EXPLAIN ANALYZE
SELECT * FROM links WHERE url LIKE '%darujme.sk%';
----  E15  ----------------------------------------------------------------

----  S16  ----------------------------------------------------------------
CREATE INDEX IF NOT EXISTS authors_description_gin_index ON authors
    USING gin(to_tsvector('english', description));
CREATE INDEX IF NOT EXISTS authors_username_gin_index ON authors
    USING gin(to_tsvector('english', username));
CREATE INDEX IF NOT EXISTS conversations_content_gin_index ON conversations
    USING gin(to_tsvector('english', content));

-- Query to get results
EXPLAIN ANALYZE
SELECT content.content, username.username, description.description, content.retweet_count
FROM

(SELECT id, author_id, content, retweet_count
 FROM conversations
 WHERE (to_tsvector('english', content)
            @@ to_tsquery('english', 'Володимир | Президент'))) AS content

FULL OUTER JOIN (SELECT id, username
                 FROM authors
                 WHERE (to_tsvector('english', username))
                           @@ to_tsquery('english', 'Володимир | Президент'))
                 AS username
    ON username.id = content.author_id

FULL OUTER JOIN (SELECT id, description
                FROM authors
                WHERE to_tsvector('english', description)
                          @@ to_tsquery('english', 'Володимир | Президент'))
                AS description
    ON description.id = content.author_id

ORDER BY retweet_count DESC NULLS LAST;
----  E16  ----------------------------------------------------------------
