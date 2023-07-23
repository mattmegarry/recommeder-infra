import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()
POSTGRES_DB = os.environ.get('POSTGRES_DB')
POSTGRES_PASSWORD = os.environ.get('POSTGRES_PASSWORD')

def get_postgres_connection():
    connection = psycopg2.connect(host="postgres", user="root", port=5432, database=POSTGRES_DB, password=POSTGRES_PASSWORD)
    connection.autocommit = True # For more complex queries, if this is set to False, you'll need to call connection.commit() after cursor.execute() - and every thing is in one transaction? (I think)
    return connection