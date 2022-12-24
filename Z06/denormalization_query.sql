    SELECT t.* AS tweets,
           annot.annot AS annotations,
           authors.authors AS authors,
           links.links AS links,
           hashtags.hashtags AS hashtags,
           to_jsonb(context_annotations.context_annotations) AS context_annotations,
           cr AS conversation_references
    FROM tweets t

    LEFT JOIN LATERAL(
        SELECT json_agg(annotations) AS annot
        FROM annotations
        WHERE annotations.conversation_id = t.id
        ) AS annot ON true

        LEFT JOIN LATERAL (
        SELECT to_jsonb(authors) AS authors
        FROM authors
        WHERE authors.id =t.author_id
        ) authors ON true

        LEFT JOIN LATERAL (
        SELECT json_agg(links) AS links
        FROM links
        WHERE links.conversation_id = t.id
        ) links ON true

        LEFT JOIN LATERAL (
        SELECT json_agg(hashtags) AS hashtags
        FROM hashtags
        LEFT JOIN conversation_hashtags ch on hashtags.id = ch.hashtag_id
        WHERE t.id = ch.conversation_id
        ) hashtags ON true

        LEFT JOIN LATERAL (
        SELECT json_agg(ca) AS context_annotations,
               json_agg(domains.context_domains) AS context_domains,
               json_agg(entities.context_entities) AS context_entities

            FROM context_annotations ca
            LEFT JOIN LATERAL(
                SELECT json_agg(context_domains) AS context_domains
                FROM context_domains
                WHERE context_domains.id = ca.context_domain_id
                ) domains ON true

            LEFT JOIN LATERAL (
                SELECT json_agg(context_entities) AS context_entities
                FROM context_entities
                WHERE context_entities.id = ca.context_entity_id
                ) entities ON true
            WHERE ca.conversation_id = t.id
        ) AS context_annotations ON true

        LEFT JOIN LATERAL(
            SELECT
               json_agg(json_build_object('type', cr.type,
                   'hashtags', to_json(h.h), 'context_annotations', to_json(context_annotations.context_annotations) ,
                   'authors', to_json(au.au), 'conversations', to_json(retweet), 'links', to_json(l.l),
                   'annotations', to_json(a.a)))

            FROM conversation_references cr
            INNER JOIN tweets retweet ON cr.parent_id = retweet.id

            LEFT JOIN LATERAL(
                    SELECT json_agg(annotations) AS a
                    FROM annotations
                    WHERE annotations.conversation_id = retweet.id
                    ) AS a ON true

                    LEFT JOIN LATERAL (
                    SELECT to_jsonb(authors) AS au
                    FROM authors
                    WHERE authors.id =retweet.author_id
                    ) au ON true

                    LEFT JOIN LATERAL (
                    SELECT to_jsonb(links) AS l
                    FROM links
                    WHERE links.conversation_id = retweet.id
                    ) l ON true

                    LEFT JOIN LATERAL (
                    SELECT to_jsonb(hashtags) AS h
                    FROM hashtags
                    LEFT JOIN conversation_hashtags ch on hashtags.id = ch.hashtag_id
                    WHERE retweet.id = ch.conversation_id
                    ) h ON true

            LEFT JOIN LATERAL (
        SELECT json_agg(ca) AS context_annotations,
               json_agg(domains.context_domains) AS context_domains,
               json_agg(entities.context_entities) AS context_entities

            FROM context_annotations ca
            LEFT JOIN LATERAL(
                SELECT to_jsonb(context_domains) AS context_domains
                FROM context_domains
                WHERE context_domains.id = ca.context_domain_id
                ) domains ON true

            LEFT JOIN LATERAL (
                SELECT to_jsonb(context_entities) AS context_entities
                FROM context_entities
                WHERE context_entities.id = ca.context_entity_id
                ) entities ON true
            WHERE ca.conversation_id = retweet.id
        ) AS context_annotations ON true
            WHERE t.id = cr.conversation_id
        ) AS cr ON true
    ) AS tweets_final;
