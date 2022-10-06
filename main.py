from utils import config, utilities
config.initialize_logger()
import gc
import gzip
import os
import json
import logging

import pandas as pd

from fill_tables.fill_authors import Fill_authors
from fill_tables.fill_database import Fill_database

def main():
    authors = Fill_authors()
    authors.create_table()
    authors.fill_table(batch_size = 100000)
    authors_amount = utilities.run_written_query('SELECT COUNT(*) FROM authors', to_dataframe=True, option='from_string')
    logging.info(f'There is {authors_amount.values[0][0]} authors in the DB')
    del(authors)
    gc.collect()

    conversations = Fill_database()
    conversations.create_tables()
    conversations.fill_all_tables()


main()
