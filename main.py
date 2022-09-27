from utils import config, utilities
config.initialize_logger()
import gzip
import os
import json
import logging

import pandas as pd
pd.options.mode.chained_assignment = None  # default='warn'

from fill_tables.fill_authors import Fill_authors

def main():
    authors = Fill_authors()
    authors.create_table()
    authors.fill_table(batch_size = 100000)
    authors_amount = utilities.run_written_query('SELECT COUNT(*) FROM authors', to_dataframe=True, option='from_string')
    logging.info(f'There is {authors_amount.values[0][0]} authors in the DB')



main()
