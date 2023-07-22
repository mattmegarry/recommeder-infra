import fastapi

app = fastapi.FastAPI()

@app.get("/")
def read_root(request: fastapi.Request):
  user_id = request.headers.get("user")
  session_id = request.headers.get("session")

  print(f"User: {user_id} Session: {session_id}")