import fastapi
import time
import datetime
import json
import confluent_kafka

from .postgres.connection import get_postgres_connection
from .postgres.queries import getOneSemiRandomItemKey, create_fct_metric

app = fastapi.FastAPI()

@app.on_event("startup")
async def startup_event():
  app.state.connection = get_postgres_connection()
  app.state.k = confluent_kafka.Producer({"bootstrap.servers": "kafka:29092"})
  print("FastAPI app has started")

@app.get("/")
def read_root(request: fastapi.Request):
  ts = int(time.time())
  user_id = request.headers.get("user")
  session = request.headers.get("session")

  print(f"User: {user_id} Session: {session}")

  item_key = getOneSemiRandomItemKey(app)

  log_msg = json.dumps({"type": "reco", "user_id": user_id, "session": session, "item_id": item_key, "ts": ts})
  app.state.k.produce("logs", log_msg)
  if (len(app.state.k) > 5): app.state.k.flush()

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