"""File to create conversations table and other tables related to it (apart from authors)."""
import csv
from datetime import datetime
import gc
import gzip
import json
import logging

import numpy as np
import pandas as pd
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
        conversations_table = tweets[['id', 'author_id', 'content', 'possibly_sensitive', 'language',
                    'source', 'retweet_count', 'reply_count', 'like_count', 'quote_count', 'created_at']].drop_duplicates(subset='id')

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


    def fill_links(self, tweets : pd.DataFrame) -> None:
        links_table = pd.DataFrame(columns=['expanded_url', 'title', 'description'])

        entities_index = tweets.dropna(subset='entities').entities.index
        links = pd.DataFrame(tweets.entities.dropna().to_list(), index=entities_index)[['urls']].dropna()
        links_df = pd.DataFrame(links.urls.to_list(), index=links.index)

        for col in links_df.columns:
            layer = pd.DataFrame(links_df[col], index=links.index).dropna()
            lnks = pd.DataFrame(layer[col].to_list(), index=layer.index)
            
            links_table = pd.concat([links_table, lnks])[links_table.columns]

        # Drop links longer than 255 characters
        links_table = links_table[links_table.expanded_url.str.len() <= 256]
        links_table = links_table.join(tweets[['id']])
        links_table.rename(columns={'expanded_url' : 'url', 'id' : 'conversation_id'}, inplace=True)

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

    
    def fill_contexts(self, tweets : pd.DataFrame) -> None:
        contexts_raw = tweets.context_annotations.dropna()

        contexts_df = pd.DataFrame(contexts_raw.to_list(), index=contexts_raw.index)
        context_entities_table = pd.DataFrame(columns=['id', 'name', 'description'])
        context_domains_table = pd.DataFrame(columns=['id', 'name', 'description'])
        context_annotations_table= pd.DataFrame(columns=['id_con_domain', 'id_con_entit'])

        for col in contexts_df.columns:
            layer = pd.DataFrame(contexts_df[col], index=contexts_df.index).dropna()
            ents = pd.DataFrame(layer[col].to_list(), index=layer.index).entity
            doms = pd.DataFrame(layer[col].to_list(), index=layer.index).domain
            ents = pd.DataFrame(ents.to_list(), index=ents.index)
            doms = pd.DataFrame(doms.to_list(), index=doms.index)

            anns = ents[['id']].join(doms[['id']], lsuffix='_con_entit', rsuffix='_con_domain')
            context_annotations_table = pd.concat([context_annotations_table, anns])
            context_entities_table = pd.concat([context_entities_table, ents]).drop_duplicates()
            context_domains_table = pd.concat([context_domains_table, doms]).drop_duplicates()
            

        context_annotations_table = context_annotations_table.join(tweets[['id']])
        context_annotations_table.rename(columns={'id' : 'conversation_id', 'id_con_domain' : 'context_domain_id', 'id_con_entit' : 'context_entity_id'},inplace=True)
          
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

        hashtags = pd.DataFrame(tweets.entities.dropna().to_list(), index=entities_index)[['hashtags']].dropna()
        all_hashtags = pd.DataFrame(columns=['tag'])
        hashtags_df = pd.DataFrame(hashtags.hashtags.to_list(), index=hashtags.index)

        for col in hashtags_df.columns:
            layer = pd.DataFrame(hashtags_df[col], index=hashtags.index).dropna()
            layer_hashtags = pd.DataFrame(layer[col].to_list(), index=layer.index)[['tag']]
            all_hashtags = pd.concat([all_hashtags, layer_hashtags])[['tag']]

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


    def fill_conversations(self, tweets : pd.DataFrame) -> None:
        refs = tweets.dropna(subset='referenced_tweets')[['id', 'referenced_tweets']].dropna(subset='referenced_tweets')
        conversation_references_table = pd.DataFrame(columns=['conversation_id', 'parent_id', 'type'])

        for layer in range(refs.referenced_tweets.apply(lambda x : len(x)).max()):
            # Select all references from given layer and find tweets they reffer to
            layer_references = refs.referenced_tweets.apply(lambda x : x[layer] if(len(x) > layer) else None).dropna()
            conv_refs = pd.DataFrame(layer_references.to_list(), index=layer_references.index)
            conv_refs.rename(columns={'id' : 'parent_id'}, inplace=True)
            conv_refs = (conv_refs.join(refs.id).rename(columns={'id' : 'conversation_id'}))

            conversation_references_table = pd.concat([conversation_references_table, conv_refs])

        conversation_references_table = conversation_references_table[conversation_references_table.parent_id.isin(self.parents_existing_ids)]
        
        # to_sql does require writing ids manually
        conversation_references_table['id'] = [new_id for new_id in range(self.last_id_references+1, self.last_id_references+1+len(conversation_references_table.index))]
        self.last_id_link = self.last_id_link+1+len(conversation_references_table.index)

        conversation_references_table.to_sql(
            'conversation_references',
            self.engine,
            if_exists="append",
            index=False
            ) 


    def fill_all_tables(self, batch_size: int = 100000):
        data_rows = []
        start_time = datetime.now()

        TIME_TRACKER_FILE_PATH = 'time_tracker_main_filling.csv'
        with open(TIME_TRACKER_FILE_PATH, 'w'):
            #clear file
            pass
        
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
        conversations_existing_ids = utilities.run_written_query('SELECT id FROM conversations;', to_dataframe=True, option='from_string').id.astype('str').values
        self.users_existing_ids = utilities.run_written_query('SELECT id FROM authors;', to_dataframe=True, option='from_string').id.astype('str').values 
        self.context_entities_existing_ids = utilities.run_written_query('SELECT id FROM context_entities;', to_dataframe=True, option='from_string').id.astype('str').values 
        self.context_domains_existing_ids = utilities.run_written_query('SELECT id FROM context_domains;', to_dataframe=True, option='from_string').id.astype('str').values 
        self.hashtags_full_list = utilities.run_written_query('SELECT tag FROM hashtags;', to_dataframe=True, option='from_string')

        last_time = datetime.now()
        with gzip.open(config.TWEETS_PATH, 'rb') as f:
            for row_number, current_tweet in enumerate(f):
                data_rows.append(json.loads(current_tweet.decode(encoding='utf-8')))

                if (row_number+1) % batch_size == 0:
                    tweets = pd.DataFrame(data_rows)
                    metrics = pd.DataFrame(tweets.public_metrics.to_list())
                    tweets[metrics.columns] = metrics
                    tweets.rename(columns={'text' : 'content',
                                        'lang' : 'language',
                                        }, inplace=True)              
                    # Drop local and global duplicates
                    tweets = tweets.drop_duplicates(subset='id')
                    tweets = tweets[~tweets.id.isin(conversations_existing_ids)]
                    conversations_existing_ids = np.concatenate((conversations_existing_ids, tweets.id.values))       
                    
                    self.fill_conversations(tweets)
                    self.fill_links(tweets)
                    self.fill_annotations(tweets)
                    self.fill_contexts(tweets)
                    self.fill_hashtags(tweets)   
                    

                    data_rows = []
                    gc.collect()   

                    with open(TIME_TRACKER_FILE_PATH, 'a') as tt:
                        writer = csv.writer(tt)
                        since_last_time = (datetime.now()-last_time).seconds
                        since_start = (datetime.now()-start_time).seconds
                        writer.writerow([last_time.isoformat(), f"{str(since_last_time//60).zfill(2)}:{str(since_last_time % 60).zfill(2)}",\
                                     f"{str(since_start//60).zfill(2)}:{str(since_start % 60).zfill(2)}"])

                        last_time = datetime.now()
                        logging.info(f"Inserted {row_number+1} conversations in: {str(since_start//60).zfill(2)}:{str(since_start % 60).zfill(2)}")
    

            if (row_number+1) % batch_size != 0: # Was not precisely divided by size
                tweets = pd.DataFrame(data_rows)
                metrics = pd.DataFrame(tweets.public_metrics.to_list())
                tweets[metrics.columns] = metrics
                tweets.rename(columns={'text' : 'content',
                                    'lang' : 'language',
                                    }, inplace=True)             

                # Drop local and global duplicates
                tweets = tweets[~tweets.id.isin(conversations_existing_ids)]
                conversations_existing_ids = np.concatenate((conversations_existing_ids, tweets.id.values)) 

                self.fill_conversations(tweets)
                self.fill_links(tweets)
                self.fill_annotations(tweets)
                self.fill_contexts(tweets)
                self.fill_hashtags(tweets)

                with open(TIME_TRACKER_FILE_PATH, 'a') as tt:
                    writer = csv.writer(tt)
                    since_last_time = (datetime.now()-last_time).seconds
                    since_start = (datetime.now()-start_time).seconds
                    writer.writerow([last_time.isoformat(), f"{str(since_last_time//60).zfill(2)}:{str(since_last_time % 60).zfill(2)}",\
                                         f"{str(since_start//60).zfill(2)}:{str(since_start % 60).zfill(2)}"])

                    last_time = datetime.now()
                    logging.info(f"Inserted {row_number+1} conversations in: {str(since_start//60).zfill(2)}:{str(since_start % 60).zfill(2)}")

        logging.info('Upload into database succesful')
        

        logging.info('Start referencing conversations')
        self.parents_existing_ids = utilities.run_written_query('SELECT id FROM conversations;', to_dataframe=True, option='from_string').id.astype('str').values
        mapped_conversations_ids = pd.DataFrame(columns=['id'])

        self.last_id_references = utilities.run_written_query('SELECT max(id) FROM conversation_references', to_dataframe=True, option='from_string')['max'].iloc[0]
        if self.last_id_references is None:
            self.last_id_references = 0

        data_rows = []
        start_time = datetime.now()
        TIME_TRACKER_FILE_PATH = 'time_tracker_references.csv'
        with open(TIME_TRACKER_FILE_PATH, 'w'):
            pass #clear file
        last_time = datetime.now()

        for row_number, current_tweet in enumerate(f):
                data_rows.append(json.loads(current_tweet.decode(encoding='utf-8')))

                if (row_number+1) % batch_size == 0:
                    tweets = pd.DataFrame(data_rows)
                    metrics = pd.DataFrame(tweets.public_metrics.to_list())
                    tweets[metrics.columns] = metrics
                    tweets.rename(columns={'text' : 'content',
                                        'lang' : 'language',
                                        }, inplace=True)              
                    # Drop local and global duplicates
                    tweets = tweets.drop_duplicates(subset='id')
                    tweets = tweets[~tweets.id.isin(mapped_conversations_ids)]
                    mapped_conversations_ids = np.concatenate((mapped_conversations_ids, tweets.id.values))       

                    self.fill_conversations(self, tweets)              

                    data_rows = []
                    gc.collect()   

                    with open(TIME_TRACKER_FILE_PATH, 'a') as tt:
                        writer = csv.writer(tt)
                        since_last_time = (datetime.now()-last_time).seconds
                        since_start = (datetime.now()-start_time).seconds
                        writer.writerow([last_time.isoformat(), f"{str(since_last_time//60).zfill(2)}:{str(since_last_time % 60).zfill(2)}",\
                                     f"{str(since_start//60).zfill(2)}:{str(since_start % 60).zfill(2)}"])
                        last_time = datetime.now()
                        logging.info(f"Inserted {row_number+1} references in: {str(since_start//60).zfill(2)}:{str(since_start % 60).zfill(2)}")

        if (row_number+1) % batch_size != 0: # Was not precisely divided by size
            tweets = pd.DataFrame(data_rows)
            metrics = pd.DataFrame(tweets.public_metrics.to_list())
            tweets[metrics.columns] = metrics
            tweets.rename(columns={'text' : 'content',
                                'lang' : 'language',
                                }, inplace=True)              
            # Drop local and global duplicates
            tweets = tweets.drop_duplicates(subset='id')
            tweets = tweets[~tweets.id.isin(mapped_conversations_ids)]
            mapped_conversations_ids = np.concatenate((mapped_conversations_ids, tweets.id.values))       

            self.fill_conversations(self, tweets)              

            data_rows = []
            gc.collect()   

            with open(TIME_TRACKER_FILE_PATH, 'a') as tt:
                writer = csv.writer(tt)
                since_last_time = (datetime.now()-last_time).seconds
                since_start = (datetime.now()-start_time).seconds
                writer.writerow([last_time.isoformat(), f"{str(since_last_time//60).zfill(2)}:{str(since_last_time % 60).zfill(2)}",\
                             f"{str(since_start//60).zfill(2)}:{str(since_start % 60).zfill(2)}"])
                last_time = datetime.now()
                logging.info(f"Inserted {row_number+1} references in: {str(since_start//60).zfill(2)}:{str(since_start % 60).zfill(2)}")
        
        logging.info("References done")
        
        self.engine.dispose()
