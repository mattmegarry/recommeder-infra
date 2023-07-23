import fastapi
import time
import json
import confluent_kafka
import os
import redis

from .postgres.connection import get_postgres_connection
from .postgres.queries import get_one_semi_random_item_key, create_fct_metric

REDIS_PASSWORD = os.environ.get('REDIS_PASSWORD')

app = fastapi.FastAPI()

@app.on_event("startup")
async def startup_event():
  app.state.connection = get_postgres_connection()
  app.state.k = confluent_kafka.Producer({"bootstrap.servers": "kafka:29092"})
  app.state.r = redis.Redis(host="redis", port=6379, db=0, password=REDIS_PASSWORD, encoding='utf-8')
  print("FastAPI app has started")

@app.get("/")
def read_root(request: fastapi.Request):
  ts = int(time.time())
  user_id = request.headers.get("user")
  session = request.headers.get("session")

  print(f"User: {user_id} Session: {session}")

  item_key = get_one_semi_random_item_key(app)

  reco_info = {"item_id": item_key, "ts": ts}
  app.state.r.xadd(f"x:{user_id}:{session}", reco_info, maxlen=30, approximate=True)

  log_msg = json.dumps({"type": "reco", "user_id": user_id, "session": session, "item_id": item_key, "ts": ts})
  app.state.k.produce("logs", log_msg)
  if (len(app.state.k) > 5): app.state.k.flush()

  return item_key

@app.post("/evt")
def save_event(request: fastapi.Request):
  ts = int(time.time())
  user_id = request.headers.get("user")
  session = request.headers.get("session")
  print(request.headers)

  ###### - Not currently used - #######
  event = request.headers.get("event")
  ######-----------------------#######

  redis_key = f"x:{user_id}:{session}" 
  most_recent_record = app.state.r.xrevrange(redis_key, count=1)

  if len(most_recent_record) != 0:
    # pedagogical...
    most_recent_tuple = most_recent_record[0]
    most_recent_reco_dict = most_recent_tuple[1]
    most_recent_reco_item_id = most_recent_reco_dict[b"item_id"].decode("utf-8")

    log_msg = json.dumps({"type": "evt", "user_id": user_id, "session": session, "item_id": most_recent_reco_item_id, "ts": ts})
    app.state.k.produce("logs", log_msg)
    app.state.k.flush()
    print("Event saved")
  else:
    print("TODO: What happens if we dont know about that recommendation?")

  return "ok"