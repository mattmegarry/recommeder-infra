import fastapi
import os

import random
import psycopg2
from dotenv import load_dotenv



app = fastapi.FastAPI()

@app.on_event("startup")
async def startup_event():
  load_dotenv()
  POSTGRES_DB = os.environ.get('POSTGRES_DB')
  POSTGRES_PASSWORD = os.environ.get('POSTGRES_PASSWORD')
  app.state.connection = psycopg2.connect(host="postgres", user="root", port=5432, database=POSTGRES_DB, password=POSTGRES_PASSWORD)
  app.state.connection.autocommit = True # For more complex queries, if this is set to False, you'll need to call connection.commit() after cursor.execute() - and every thing is in one transaction? (I think)

@app.get("/")
def read_root(request: fastapi.Request):
  user_id = request.headers.get("user")
  session_id = request.headers.get("session")

  print(f"User: {user_id} Session: {session_id}")
  select_query = "SELECT item_key FROM items LIMIT 1000;"

  try:
    print(app.state.connection)
    cursor = app.state.connection.cursor()
    cursor.execute(select_query)
    row = cursor.fetchall()

    if row is None:
      item_key = None
      print("No rows returned")
    else:  
      item_key = row[random.randint(0, 999)][0]
      print(f"Item key: {item_key}")

  except psycopg2.DatabaseError as e:
    print(f"Error: {e}")

  finally:
    if cursor:
      cursor.close()

  return item_key