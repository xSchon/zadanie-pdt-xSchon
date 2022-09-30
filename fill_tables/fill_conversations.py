"""File to create conversations table and other tables related to it (apart from authors)."""
import gzip
import os 
import json
import logging
import re
import time

import numpy as np
import pandas as pd
import psycopg2
from sqlalchemy import create_engine

from utils import config, utilities


class Fill_conversations:

    def __init__(self) -> None:
        self.TABLE_NAME = 'authors'

    def create_tables(self) -> None:
        utilities.run_written_query('create_other_tables.sql', option='from_file')
