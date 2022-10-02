"""File to create conversations table and other tables related to it (apart from authors)."""
import gc
import gzip
import os 
import json
import logging
import time

import numpy as np
import pandas as pd
import psycopg2
from sqlalchemy import create_engine

from utils import config, utilities


class Fill_database:

    def __init__(self) -> None:
        self.engine=create_engine(
            f"postgresql://{config.DATABASE['USER']}:"
            f"{config.DATABASE['PASSWORD']}@"
            f"{config.DATABASE['HOST']}:"
            f"{config.DATABASE['PORT']}/"
            f"{config.DATABASE['DBNAME']}"
        )  


    def create_tables(self) -> None:
        utilities.run_written_query('create_other_tables.sql', option='from_file')
        
    
    def fill_conversations(self, tweets : pd.DataFrame) -> None:
        # Drop local and global duplicates
        conversations_table = tweets[['id', 'author_id', 'content', 'possibly_sensitive', 'language',
                    'source', 'retweet_count', 'reply_count', 'like_count', 'quote_count', 'created_at']].drop_duplicates(subset='id')
        conversations_table = conversations_table[~conversations_table.id.isin(self.conversations_existing_ids)]
        self.conversations_existing_ids = np.concatenate((self.conversations_existing_ids, conversations_table.id.values))

        # Add authors that are not in the database already - with null values for rows other than ID
        authors_to_add = conversations_table[~conversations_table.author_id.isin(self.users_existing_ids)][['author_id']]
        authors_to_add.rename(columns={'author_id' : 'id'}, inplace=True)
        authors_to_add.drop_duplicates(inplace=True)
        authors_to_add.to_sql(
                        'authors',
                        self.engine,
                        if_exists="append",
                        index=False
                                ) 
        self.users_existing_ids = np.concatenate((self.users_existing_ids, authors_to_add.id.values))
        # Upload conversations into the database
        conversations_table.to_sql(
                'conversations',
                self.engine,
                if_exists="append",
                index=False
            )
        del(conversations_table, authors_to_add)


    def fill_links(self, tweets : pd.DataFrame) -> None:
        links_table = pd.DataFrame(columns=['conversation_id', 'expanded_url', 'title', 'description'])

        entities_index = tweets.dropna(subset='entities').entities.index
        links_raw = pd.DataFrame(tweets.dropna(subset='entities').entities.to_list(), index=entities_index).dropna(subset='urls').urls

        for layer in range(links_raw.apply(lambda x : len(x)).max()):
            layer_links = links_raw.apply(lambda x : x[layer] if (len(x) > layer) else None).dropna()
            tmp_links = pd.DataFrame(layer_links.to_list(), index=layer_links.index)
            tmp_links = tmp_links.join(tweets.id).rename(columns={'id' : 'conversation_id'})

            links_table = pd.concat([links_table, tmp_links])[list(links_table.columns)]
            # Delete links with longer than 255 char URL
            links_table = links_table[links_table.expanded_url.apply(lambda url_len : len(url_len) < 256)]
            del(layer_links, tmp_links)


        # to_sql does require writing ids manually
        links_table['id'] = [new_id for new_id in range(self.last_id_link+1, self.last_id_link+1+len(links_table.index))]
        self.last_id_link = self.last_id_link+1+len(links_table.index)

        links_table.rename(columns={'expanded_url' : 'url'}, inplace=True)
        links_table.to_sql(
            'links',
            self.engine,
            if_exists="append",
            index=False
            ) 
        del(links_table, entities_index, links_raw)

    
    def fill_annotations(self, tweets : pd.DataFrame) -> None:
        annotations_table = pd.DataFrame(columns=['conversation_id', 'value', 'type', 'probability'])

        entities_index = tweets.dropna(subset='entities').entities.index
        annotations_raw = pd.DataFrame(tweets.dropna(subset='entities').entities.to_list(), index=entities_index).dropna(subset='annotations').annotations

        for layer in range(annotations_raw.apply(lambda x : len(x)).max()):
            layer_annotations = annotations_raw.apply(lambda x : x[layer] if (len(x)>layer) else None).dropna()
            tmp_annotations  = pd.DataFrame(layer_annotations.to_list(), index=layer_annotations.index)
            tmp_annotations.rename(columns={'normalized_text' : 'value'}, inplace=True)
            tmp_annotations = tmp_annotations.join(tweets.id).rename(columns={'id' : 'conversation_id'})
            
            annotations_table = pd.concat([annotations_table, tmp_annotations])[list(annotations_table.columns)]

        # to_sql does require writing ids manually
        annotations_table['id'] = [new_id for new_id in range(self.last_id_annotations+1, self.last_id_annotations+1+len(annotations_table.index))]
        self.last_id_annotations = self.last_id_annotations+1+len(annotations_table.index)

        annotations_table.to_sql(
            'annotations',
            self.engine,
            if_exists="append",
            index=False
            )
        del(annotations_table, entities_index, annotations_raw, tmp_annotations)

    
    def fill_contexts(self, tweets : pd.DataFrame) -> None:
        contexts_raw = tweets.context_annotations.dropna()
        annotations_index = contexts_raw.index

        context_annotations_table= pd.DataFrame(columns=['conversation_id', 'context_domain_id', 'context_entity_id'])
        context_entities_table = pd.DataFrame(columns=['id', 'name', 'description'])
        context_domains_table = pd.DataFrame(columns=['id', 'name', 'description'])
        domain_entities_tmp = pd.DataFrame(columns=['domains', 'entities'])

        for layer in range(contexts_raw.apply(lambda x : len(x)).max()):
            context_annotations = contexts_raw.apply(lambda x : x[layer] if (len(x) > layer) else None).dropna()
            domain_entities_tmp = pd.DataFrame(context_annotations.to_list(), index=context_annotations.index)

            ents = pd.DataFrame(domain_entities_tmp.entity.to_list(), index=context_annotations.index).join(tweets[['id']].rename(columns={'id' : 'conversation_id'}).conversation_id)
            doms = pd.DataFrame(domain_entities_tmp.domain.to_list(), index=context_annotations.index).join(tweets[['id']].rename(columns={'id' : 'conversation_id'}).conversation_id)

            context_entities_table = pd.concat([context_entities_table, ents])[context_entities_table.columns].drop_duplicates(subset='id')
            context_domains_table = pd.concat([context_domains_table, doms])[context_domains_table.columns].drop_duplicates(subset='id')
            
            anns = ents[['id', 'conversation_id']].rename(columns={'id' : 'context_entity_id'}).join(doms[['id']].rename(columns={'id' : 'context_domain_id'}))
            context_annotations_table = pd.concat([context_annotations_table, anns])[context_annotations_table.columns].drop_duplicates()

        # Drop duplicates and save used ids for the future work
        context_entities_table = context_entities_table[~context_entities_table.id.isin(self.context_entities_existing_ids)]
        context_domains_table = context_domains_table[~context_domains_table.id.isin(self.context_domains_existing_ids)]
        self.context_entities_existing_ids = np.concatenate((self.context_entities_existing_ids, context_entities_table.id.values))
        self.context_domains_existing_ids = np.concatenate((self.context_domains_existing_ids, context_domains_table.id.values))
        

        context_entities_table.to_sql(
            'context_entities',
            self.engine,
            if_exists="append",
            index=False
            )        

        context_domains_table.to_sql(
            'context_domains',
            self.engine,
            if_exists="append",
            index=False
            )        

        # to_sql does require writing ids manually
        context_annotations_table['id'] = [new_id for new_id in range(self.last_id_context_annotations+1, self.last_id_context_annotations+1+len(context_annotations_table.index))]
        self.last_id_context_annotations = self.last_id_context_annotations+1+len(context_annotations_table.index)

        context_annotations_table.to_sql(
            'context_annotations',
            self.engine,
            if_exists="append",
            index=False
            )        
        

    def fill_hashtags(self, tweets : pd.DataFrame) -> None:    
        entities_index = tweets.dropna(subset='entities').entities.index
        hashtags_raw = pd.DataFrame(tweets.entities.dropna().to_list(), index=entities_index)
        hashtags = hashtags_raw.hashtags.dropna()
        all_hashtags = pd.DataFrame(columns=['tag'])

        for col in pd.DataFrame(hashtags.to_list()).columns:
            all_hashtags = pd.concat([all_hashtags, pd.DataFrame(pd.DataFrame(hashtags.to_list())[col].to_list(), index=hashtags.index)])[['tag']]

        all_hashtags = all_hashtags.dropna()
        all_hashtags = all_hashtags.join(tweets.id).rename(columns={'id' : 'conversation_id'})

        unique_used_hashtags = all_hashtags[['tag']].drop_duplicates()
        new_hashtags = unique_used_hashtags[~unique_used_hashtags.tag.isin(self.hashtags_full_list.tag)]
        self.hashtags_full_list = pd.concat([self.hashtags_full_list, new_hashtags], ignore_index=True)
        upload_hashtags = self.hashtags_full_list[self.hashtags_full_list.tag.isin(new_hashtags.tag)]
        tmp = self.hashtags_full_list.copy()
        tmp['hashtag_id'] = tmp.index
        conversation_hashtags_table = all_hashtags.merge(tmp, on='tag')

        upload_hashtags.to_sql(
            'hashtags',
            self.engine,
            if_exists="append",
            index=True,
            index_label='id'
            )       

        conversation_hashtags_table = conversation_hashtags_table[['conversation_id', 'hashtag_id']]
        # to_sql does require writing ids manually
        conversation_hashtags_table['id'] = [new_id for new_id in range(self.last_id_hashtags+1, self.last_id_hashtags+1+len(conversation_hashtags_table.index))]
        self.last_id_hashtags = self.last_id_hashtags+1+len(conversation_hashtags_table.index)  

        conversation_hashtags_table.to_sql(
            'conversation_hashtags',
            self.engine,
            if_exists="append",
            index=False
            )       


    def fill_all_tables(self, batch_size: int = 100000):
        data_rows = []
        start_time = time.time()
        
        self.last_id_link = utilities.run_written_query('SELECT max(id) FROM links', to_dataframe=True, option='from_string')['max'].iloc[0]
        if self.last_id_link is None:
            self.last_id_link = 0
        self.last_id_annotations = utilities.run_written_query('SELECT max(id) FROM annotations', to_dataframe=True, option='from_string')['max'].iloc[0]
        if self.last_id_annotations is None:
            self.last_id_annotations = 0
        self.last_id_context_annotations = utilities.run_written_query('SELECT max(id) FROM annotations', to_dataframe=True, option='from_string')['max'].iloc[0]
        if self.last_id_context_annotations is None:
            self.last_id_context_annotations = 0
        self.last_id_hashtags = utilities.run_written_query('SELECT max(id) FROM conversation_hashtags', to_dataframe=True, option='from_string')['max'].iloc[0]
        if self.last_id_hashtags is None:
            self.last_id_hashtags = 0

        # 5 arrays helping to get information about used parts of the program 
        self.conversations_existing_ids = utilities.run_written_query('SELECT id FROM conversations;', to_dataframe=True, option='from_string').id.astype('str').values
        self.users_existing_ids = utilities.run_written_query('SELECT id FROM authors;', to_dataframe=True, option='from_string').id.astype('str').values 
        self.context_entities_existing_ids = utilities.run_written_query('SELECT id FROM context_entities;', to_dataframe=True, option='from_string').id.astype('str').values 
        self.context_domains_existing_ids = utilities.run_written_query('SELECT id FROM context_domains;', to_dataframe=True, option='from_string').id.astype('str').values 
        self.hashtags_full_list = utilities.run_written_query('SELECT tag FROM hashtags;', to_dataframe=True, option='from_string')

        with gzip.open(config.TWEETS_PATH, 'rb') as f:
            for row_number, current_tweet in enumerate(f):
                data_rows.append(json.loads(current_tweet.decode(encoding='utf-8')))

                if (row_number+1) % batch_size == 0:
                    logging.info(row_number+1)
                    
                    tweets = pd.DataFrame(data_rows)
                    metrics = pd.DataFrame(tweets.public_metrics.to_list())
                    tweets[metrics.columns] = metrics
                    tweets.rename(columns={'text' : 'content',
                                        'lang' : 'language',
                                        }, inplace=True)                    

                    
                    self.fill_conversations(tweets)
                    self.fill_links(tweets)
                    self.fill_annotations(tweets)
                    self.fill_contexts(tweets)
                    self.fill_hashtags(tweets)   
                    

                    data_rows = []
                    del(metrics)
                    del(tweets)
                    gc.collect()   
    

            if (row_number+1) % batch_size != 0: # Was not precisely divided by size
                tweets = pd.DataFrame(data_rows)
                metrics = pd.DataFrame(tweets.public_metrics.to_list())
                tweets[metrics.columns] = metrics
                tweets.rename(columns={'text' : 'content',
                                    'lang' : 'language',
                                    }, inplace=True)                    
                self.fill_conversations(tweets)
                self.fill_links(tweets)
                self.fill_annotations(tweets)
                self.fill_contexts(tweets)
                self.fill_hashtags(tweets)

        logging.info('Upload into database succesful')
        logging.info(f'Upload time was: {time.strftime("%Hh %Mm %Ss", time.gmtime(time.time() - start_time))}')
        self.engine.dispose()



