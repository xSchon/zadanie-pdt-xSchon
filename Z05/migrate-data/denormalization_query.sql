        SELECT to_jsonb(cnvs) AS conversations
        FROM
        (
                SELECT c.*,
                a.annot AS annotations,
                   authors.authors AS authors,
                   links.links AS links,
                   hashtags.hashtags AS hashtags,
                   domains.context_domains AS context_domains,
                   entities.context_entities AS context_entities,
                   refs.conversations AS conversation_references
            FROM   conversations c
            LEFT JOIN LATERAL (
               SELECT json_agg(annotations) AS annot
               FROM   annotations
               WHERE  annotations.conversation_id = c.id
               ) a ON true

            LEFT JOIN LATERAL (
                SELECT to_jsonb(authors) AS authors
                FROM authors
                WHERE authors.id =c.author_id
                ) authors ON true

            LEFT JOIN LATERAL (
                SELECT json_agg(links) AS links
                FROM links
                WHERE links.conversation_id = c.id
                ) links ON true

            LEFT JOIN LATERAL (
                SELECT json_agg(hashtags) AS hashtags
                FROM hashtags
                    LEFT JOIN conversation_hashtags ch on hashtags.id = ch.hashtag_id
                WHERE c.id = ch.conversation_id
                ) hashtags ON true

            LEFT JOIN LATERAL (
                SELECT json_agg(context_domains) AS context_domains
                FROM context_domains
                    LEFT JOIN context_annotations ca on context_domains.id = ca.context_domain_id
                WHERE c.id = ca.conversation_id
                ) domains ON true

            LEFT JOIN LATERAL (
                SELECT json_agg(context_entities) AS context_entities
                FROM context_entities
                    LEFT JOIN context_annotations ca on context_entities.id = ca.context_entity_id
                WHERE c.id = ca.conversation_id
                ) entities ON true

            LEFT JOIN LATERAL (
                SELECT to_jsonb(json_build_object('type', type, 'id', cr.id, 'authors',
                    a.authors, 'content', conversations.content, 'hashtags', hash.hsh)) AS conversations
                FROM conversations cc
                    LEFT JOIN conversation_references cr on cc.id = cr.conversation_id
                    LEFT JOIN conversations ON conversations.id = cr.parent_id

                    LEFT JOIN LATERAL (
                        SELECT json_agg(json_build_object('id', ath.id,'name', ath.name,'username', ath.username)) AS authors
                        FROM authors ath
                        WHERE ath.id = conversations.author_id
                    ) a ON true

                    LEFT JOIN LATERAL (
                        SELECT json_agg(h) AS hsh
                        FROM hashtags h
                            LEFT JOIN conversation_hashtags ch on h.id = ch.hashtag_id
                        WHERE conversations.id = ch.conversation_id
                    ) hash ON true

                WHERE c.id = cr.conversation_id
                ) refs ON true
            ) AS cnvs
        