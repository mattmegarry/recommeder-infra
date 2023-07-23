import fastapi

from .postgres.connection import get_postgres_connection
from .postgres.queries import getOneSemiRandomItemKey

app = fastapi.FastAPI()

@app.on_event("startup")
async def startup_event():
  app.state.connection = get_postgres_connection()
  print("FastAPI app has started")

@app.get("/")
def read_root(request: fastapi.Request):
  user_id = request.headers.get("user")
  session_id = request.headers.get("session")

  print(f"User: {user_id} Session: {session_id}")

  item_key = getOneSemiRandomItemKey(app)
  return item_key