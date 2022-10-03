"""File to create and fill authors table based on json file of twitter users."""
import csv
from datetime import datetime
import gzip
import os 
import json
import logging
import re

import numpy as np
import pandas as pd
from sqlalchemy import create_engine

from utils import config, utilities

class Fill_authors:
    
    def __init__(self):
        self.TABLE_NAME = 'authors'

    def create_table(self) -> None:
        """Run SQL to create authors table."""
        utilities.run_written_query('create_authors.sql', option='from_file')

    def fill_table(self, batch_size: int = 100000):
        """Fill SQL authors table with data from authors.jsonl.gz
        
        Params
            batch_size (int): Default 100 000. 
                Set how many records to process and upload to database at once.
        """
        # Create engine for insert into the database
        engine = create_engine(
            f"postgresql://{config.DATABASE['USER']}:"
            f"{config.DATABASE['PASSWORD']}@"
            f"{config.DATABASE['HOST']}:"
            f"{config.DATABASE['PORT']}/"
            f"{config.DATABASE['DBNAME']}"
        )  
        start_time = datetime.now()

        TIME_TRACKER_FILE_PATH = 'time_tracker_authors_filling.csv'
        with open(TIME_TRACKER_FILE_PATH, 'w'):
            #clear file
            pass

        logging.info('DB engine connection estabilished')
        re_null = re.compile(pattern='\x00') # Dealing with UTF-8

        def insert_chunk_into_users(users : pd.DataFrame, users_existing_ids : np.array) -> np.array:
            """Uploads pandas.DataFrame to SQL with the usage of sqalchemy engine and 
               advantages of DataFrame to_sql function. This approach is very effective for big batches.

                Args:
                   users (pd.DataFrame) : DataFrame containing raw information about users from batch
                   user_existing_ids (np.array()) : Array of already logged ids to avoid duplicates
                Returns:
                    np.array: Updated user_existing_ids with new values
            """
            users[['followers_count', 'following_count', 'listed_count', 'tweet_count']] = pd.DataFrame(users.public_metrics.to_list())
            users = users[['id', 'name', 'username', 'description', 'followers_count', 'following_count', 'listed_count', 'tweet_count']]

                                                                       # Drop duplicated ids
            users = users[~users.id.isin(users_existing_ids)]  # Uploaded in previous batches
            users = users.drop_duplicates('id')                # From this batch

            updated_users_existing_ids = np.concatenate((users_existing_ids, users.id.values))
            users.replace(regex=re_null,value='', inplace=True)  # Deal with UTF-8

            users.to_sql(
                self.TABLE_NAME,
                engine,
                if_exists="append",
                index=False
            ) 
            return updated_users_existing_ids
        
        last_time = datetime.now()
        data_rows = [] # Use list of rows to create DataFrame of users for easy upload to DB
        # Hold existing user ids in array so that you can delete duplicates
        users_existing_ids = utilities.run_written_query('SELECT id \
                                                          FROM authors', to_dataframe=True, option='from_string').id.astype('str').values  
        with gzip.open(config.USERS_PATH, 'rb') as f:
            for row_number, current_user in enumerate(f):
                # Iterate over all records in jsonl file, load them as JSON
                data_rows.append(json.loads(current_user.decode(encoding='utf-8')))

                if (row_number+1) % batch_size == 0: # Batch of users
                    # Create DataFrame with desired columns
                    users = pd.DataFrame(data_rows)                    
                    users_existing_ids = insert_chunk_into_users(users, users_existing_ids)
                    # Clear row data, as these data are already uploaded in the DB
                    data_rows = []
                    users = pd.DataFrame()
                    logging.info(f'So far processed {row_number+1} users')

                    with open(TIME_TRACKER_FILE_PATH, 'a') as tt:
                        writer = csv.writer(tt)
                        since_last_time = (datetime.now()-last_time).seconds
                        since_start = (datetime.now()-start_time).seconds
                        writer.writerow([last_time.isoformat(), f"{str(since_last_time//60).zfill(2)}:{str(since_last_time % 60).zfill(2)}",\
                                     f"{str(since_start//60).zfill(2)}:{str(since_start % 60).zfill(2)}"])

                        last_time = datetime.now()
                        logging.info(f"Inserted {row_number+1} authors in: {str(since_start//60).zfill(2)}:{str(since_start % 60).zfill(2)}")

            # Run one more time for the final rows that were not part of the last batch
            if (row_number+1) % batch_size != 0: # Was not precisely divided by size
                users = pd.DataFrame(data_rows)
                users_existing_ids = insert_chunk_into_users(users, users_existing_ids)
                with open(TIME_TRACKER_FILE_PATH, 'a') as tt:
                        writer = csv.writer(tt)
                        since_last_time = (datetime.now()-last_time).seconds
                        since_start = (datetime.now()-start_time).seconds
                        writer.writerow([last_time.isoformat(), f"{str(since_last_time//60).zfill(2)}:{str(since_last_time % 60).zfill(2)}",\
                                     f"{str(since_start//60).zfill(2)}:{str(since_start % 60).zfill(2)}"])

                        last_time = datetime.now()
                        logging.info(f"Inserted {row_number+1} authors in: {str(since_start//60).zfill(2)}:{str(since_start % 60).zfill(2)}")

            logging.info('Upload into authors database succesful')

        engine.dispose()

    def __exit__(self, exc_type, exc_value, traceback):
        for file in self.files:
            os.unlink(file)
