import fastapi
import os

# -- temporary -- #
import random
import psycopg2
from dotenv import load_dotenv
load_dotenv()
POSTGRES_DB = os.environ.get('POSTGRES_DB')
POSTGRES_PASSWORD = os.environ.get('POSTGRES_PASSWORD')
# -- end temporary -- #


app = fastapi.FastAPI()

@app.get("/")
def read_root(request: fastapi.Request):
  user_id = request.headers.get("user")
  session_id = request.headers.get("session")

  print(f"User: {user_id} Session: {session_id}")

   
  connection = psycopg2.connect(host="localhost", user="root", port=5432, database=POSTGRES_DB, password=POSTGRES_PASSWORD)
  connection.autocommit = True
  cursor = connection.cursor()
  """  
  select_query = "SELECT item_key FROM items LIMIT 100;"
  cursor.execute(select_query)
  item_key = cursor.fetchone()[random.randint(0, 99)]
  """
  
  item_key = "123e4567-e89b-12d3-a456-426614174002"
  return item_key