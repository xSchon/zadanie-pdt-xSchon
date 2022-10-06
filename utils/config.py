"""
Configuration file for setting up variables throughout the project.
Currently contains:
    Logger initialization.
    Database enviromental variables declaration.
    PATHs to data files.
"""
import logging
import os

from dotenv import load_dotenv

load_dotenv()

def initialize_logger():
    logging.basicConfig(filename='uploader.log', filemode='w', format='%(filename)s - %(funcName)s - %(levelname)s - %(message)s',  level=logging.INFO)
    logging.info('Logger succesfully initialized')

DATABASE={
        'DBNAME':  os.getenv('DB_NAME'),
        'HOST':    os.getenv('DB_HOST'),
        'USER':    os.getenv('DB_USER'),
        'PASSWORD':os.getenv('DB_PASSWORD'),
        'PORT':    os.getenv('DB_PORT'),
}

USERS_PATH = 'data/authors.jsonl.gz'
TWEETS_PATH = 'data/conversations.jsonl.gz'
