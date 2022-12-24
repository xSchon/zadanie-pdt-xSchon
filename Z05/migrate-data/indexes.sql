CREATE INDEX idx_annotations_conversations_id
    ON annotations(conversation_id);

CREATE INDEX idx_conversations_author_id
    ON conversations(author_id);

CREATE INDEX idx_links_conversations_id
    ON links(conversation_id);

CREATE INDEX idx_conversations_hashtags
    ON conversation_hashtags(conversation_id, hashtag_id);

CREATE INDEX idx_context_annotations_conv
    ON context_annotations(conversation_id);

CREATE INDEX idx_context_annotations_dom
    ON context_annotations(context_domain_id);

CREATE INDEX idx_context_annotations_ent
    ON context_annotations(context_entity_id);

CREATE INDEX idx_references_conversation_id
    ON conversation_references(conversation_id);

CREATE INDEX idx_references_parent_id
    ON conversation_references(parent_id);
    
    
    
    -- COALESCE ALTERNATIVE FOR DENORMALIZATION IF NEEDED --
/*  		SELECT c.*,
                COALESCE(a.annot, '[]') AS annotations,
                   COALESCE(authors.authors, '[]') AS authors,
                   COALESCE(links.links, '[]') AS links,
                   COALESCE(hashtags.hashtags, '[]') AS hashtags,
                   COALESCE(domains.context_domains, '[]') AS context_domains,
                   COALESCE(entities.context_entities, '[]') AS context_entities,
                   COALESCE(refs.conversations, '[]') AS conversation_references
*/


