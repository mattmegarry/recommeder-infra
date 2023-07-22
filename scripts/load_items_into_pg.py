import psycopg2
import httpx
import os
from dotenv import load_dotenv

load_dotenv()

POSTGRES_DB = os.environ.get('POSTGRES_DB')
POSTGRES_PASSWORD = os.environ.get('POSTGRES_PASSWORD')


connection = psycopg2.connect(host="localhost", user="root", port=5432, database=POSTGRES_DB, password=POSTGRES_PASSWORD)
connection.autocommit = True
cursor = connection.cursor()


insert_query = "INSERT INTO items (item_key, created_at, user_id, bucket_key, type) VALUES (%s, %s, %s, %s, %s) ON CONFLICT (item_key) DO NOTHING;"
base_path = "http://135.181.118.171:7070/items/"
offset = 0


while True:
  curr_url = base_path + str(offset)
  items = httpx.get(curr_url).json()
  if (len(items)) == 0: break

  insert_data = []
  for item in items: insert_data.append((
    item["item_key"],
    item["created_at"],
    item["user_id"],
    item["bucket_key"],
    item["type"],
  ))

  cursor.executemany(insert_query, insert_data)
  print("offset", offset)
  offset += 20000