import confluent_kafka
import psycopg2
import datetime
import json
import utils
import os

POSTGRES_DB = os.environ.get('POSTGRES_DB')
POSTGRES_PASSWORD = os.environ.get('POSTGRES_PASSWORD')


utils.check_connection_status("postgres", 5432)
p = psycopg2.connect(host="postgres", user="root", port=5432, database=POSTGRES_DB, password=POSTGRES_PASSWORD)
k = confluent_kafka.Consumer({"bootstrap.servers": "kafka:29092", "group.id": "logs-group-1", "auto.offset.reset": "earliest"}) # UNDERSTAND THIS
k.subscribe(["logs"])
p.autocommit = True


def insert_to_postgres(store):

  insert_query = """
    INSERT INTO fct_hourly_metric (
      date_stamp,
      time_stamp,
      evnt_stamp,
      user_id,
      session_id,
      item_id
    ) VALUES (%s, %s, %s, %s, %s, %s)
  """

  insert_data = []

  for evt_log in store:
    cur_date = datetime.datetime.fromtimestamp(evt_log["ts"])

    insert_data.append((
      cur_date.date(),
      cur_date.replace(minute=0, second=0, microsecond=0),
      evt_log["ts"],
      evt_log["user_id"],
      evt_log["session"],
      evt_log["item_id"]
    ))

  try:
    cursor = p.cursor()
    cursor.executemany(insert_query, insert_data)
    print("insert_to_postgres", len(insert_data))
  except Exception as e:
    print("Worker error", e)


def main():
  store = []
  while True:
    msg = k.poll(1.0) # UNDERSTAND THIS
    if msg is None: continue
    if msg.error(): continue
    raw_res = msg.value().decode("utf-8")
    cur_res = json.loads(raw_res) # UNDERSTAND ENCODING ETC
    store.append(cur_res)
    if len(store) > 5:
      insert_to_postgres(store)
      store = []
      print(f"Kafta topic Logs was written to Postgres: {len(store)} rows")


if __name__ == "__main__":
  main()