import fastapi
import time
import datetime

from .postgres.connection import get_postgres_connection
from .postgres.queries import getOneSemiRandomItemKey, create_fct_metric

app = fastapi.FastAPI()

@app.on_event("startup")
async def startup_event():
  app.state.connection = get_postgres_connection()
  print("FastAPI app has started")

@app.get("/")
def read_root(request: fastapi.Request):
  user_id = request.headers.get("user")
  session = request.headers.get("session")

  print(f"User: {user_id} Session: {session}")

  item_key = getOneSemiRandomItemKey(app)
  return item_key

@app.post("/evt")
def save_event(request: fastapi.Request):
  ts = int(time.time())
  cur_date = datetime.datetime.fromtimestamp(ts)
  user_id = request.headers.get("user")
  session = request.headers.get("session")
  
  ITEM_ID = "00000000-0000-0000-0000-000000000000"

  ###### - Not currently used - #######
  event = request.headers.get("event")
  ######-----------------------#######
  
  print(    
    cur_date.date(),
    cur_date.replace(minute=0, second=0, microsecond=0),
    ts,
    user_id,
    session,
    event
  )

  create_fct_metric(
    app,
    cur_date.date(),
    cur_date.replace(minute=0, second=0, microsecond=0),
    ts,
    user_id,
    session,
    ITEM_ID
  )

  return "ok"