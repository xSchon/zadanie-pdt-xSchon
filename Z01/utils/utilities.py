"""This file serves the purpose of holding helpful functions accessible from any other file."""
import csv
from datetime import datetime
import logging
import warnings
warnings.filterwarnings('ignore') # Ignore warning about usage of psycopgs at every connection

import pandas as pd
import psycopg2

from utils import config

def run_written_query(name_of_query: str, to_dataframe: bool = False, option: str= 'from_file') -> None:
    """Run queries string or saved in sql files in sql_queries directory.
    
    Args:
        name_of_query (str): Name of file saved in sql_queries, or query_string itself.
        to_dataframe (bool): Default False. If True, then SQL in run via pandas.to_sql and DataFrame generated 
                             by such query is returned. Does not work for DDL queries.
        option (str): Either 'from_file' (default) - runs query saved in sql_query 
                      or 'from_string' - uses name_of_query as query string
    Returns:
       None or pd.DataFrame: if to_dataframe is False, return None, else return downloaded DataFrame
    """
    with psycopg2.connect(
            dbname=config.DATABASE['DBNAME'],
            host=config.DATABASE['HOST'],
            user=config.DATABASE['USER'],
            password=config.DATABASE['PASSWORD'],
            port=config.DATABASE['PORT'],
    ) as conn:
        logging.info('Connection estabilished')

        if option == 'from_file':
            query = open(f"sql_queries/{name_of_query}".format(name_of_query=name_of_query), "r")
            query = query.read()
        elif option == 'from_string':
            query = name_of_query
        else:
            return None 

        if to_dataframe:
            df = pd.read_sql(query, con=conn)
            logging.info(f'Query {name_of_query} finished!') 
            return df

        conn.cursor().execute(query=query)
        logging.info(f'Query {name_of_query} finished!') 
        return None


def progress_track(file_path : str, last_time : datetime, start_time : datetime, row_number : int, name_process : str) -> datetime:
    """Function that helps to track progress of data processing and writes into .csv file
       as well as logs progres. The path in file_path is appended only!

       Args:
            file_path(str) : path to the .csv file used for tracking
            last_time(datetime) : time since last batch was processed
            start_time(datetime) : time when current calculations started
            row_number(int) : number of processed rows so far
            name_process(str) : name of tracked process to be logged
        Returns:
            datetime : current time, used to replace last_time when this fucntion called
    """
    with open(file_path, 'a') as track_file:
        writer = csv.writer(track_file)
        since_last_time = (datetime.now()-last_time).seconds
        since_start = (datetime.now()-start_time).seconds
        writer.writerow([last_time.isoformat(), f"{str(since_last_time//60).zfill(2)}:{str(since_last_time % 60).zfill(2)}",\
                                     f"{str(since_start//60).zfill(2)}:{str(since_start % 60).zfill(2)}"])

        logging.info(f"Inserted {row_number+1} {name_process} in: {str(since_start//60).zfill(2)}:{str(since_start % 60).zfill(2)}")
        return datetime.now()
                        
