"""This file serves the purpose of holding helpful functions accessible from any other file."""
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
            return pd.read_sql(query, con=conn)

        conn.cursor().execute(query=query)
        logging.info(f'Query {name_of_query} finished!') 
        return None
