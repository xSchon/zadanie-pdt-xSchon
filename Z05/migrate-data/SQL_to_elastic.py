import json
import csv
import psycopg2
import pandas as pd
import os
from dotenv import load_dotenv
import time

from elasticsearch import Elasticsearch, helpers
import warnings
warnings.filterwarnings("ignore")


load_dotenv()
DATABASE={
        'DBNAME':  os.getenv('DB_NAME'),
        'HOST':    os.getenv('DB_HOST'),
        'USER':    os.getenv('DB_USER'),
        'PASSWORD':os.getenv('DB_PASSWORD'),
        'PORT':    os.getenv('DB_PORT'),
}

ELASTIC_NAME = os.getenv('ELASTIC_NAME')
ELASTIC_PASSWORD = os.getenv('ELASTIC_PASSWORD')
name_of_query = "denormalization_query.sql"

def import_chunk(limit, offset):
    with psycopg2.connect(
            dbname=DATABASE['DBNAME'],
            host=DATABASE['HOST'],
            user=DATABASE['USER'],
            password=DATABASE['PASSWORD'],
            port=DATABASE['PORT'],
        ) as conn:

        query = open(f"{name_of_query}".format(name_of_query=name_of_query), "r")
        query = query.read()
        query = f"""{query} 
                LIMIT {limit}
                OFFSET {offset};
                """
        cur = conn.cursor()
        cur.execute(query)
        
        # parse json to dict
        r = [dict((cur.description[i][0], value) for i, value in enumerate(row)) for row in cur.fetchall()]
        if (len(r) == 0):
            return False
        
        # read conversations as rows
        to_import = [row['conversations'] for row in r]
        # connection to local Elasticsearch, without cerificate and bulk import via helpers
        es_client = Elasticsearch('https://localhost:9200/tweets/', basic_auth=(ELASTIC_NAME, ELASTIC_PASSWORD), verify_certs=False)
        helpers.bulk(es_client, to_import)
        return True

        
# import_chunk(5000, 0)
start_time = time.time()
limit = 250000
offset = 0

while(import_chunk(limit, offset)):
    print(f"Imported chunk {offset} - {offset+limit} in {round(time.time() - start_time,2)}s")
    offset += limit
    start_time = time.time()
