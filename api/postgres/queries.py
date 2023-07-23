import psycopg2
import random

def get_one_semi_random_item_key(app):
    select_query = "SELECT item_key FROM items LIMIT 1000;"

    item_key = None

    try:
        cursor = app.state.connection.cursor()
        cursor.execute(select_query)
        row = cursor.fetchall()

        if row is None:
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

def create_fct_metric(app, date_stamp, time_stamp, evnt_stamp, user_id, session_id, item_id):
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
    insert_data = [date_stamp, time_stamp, evnt_stamp, user_id, session_id, item_id]
    
    try:
        cursor = app.state.connection.cursor()
        cursor.execute(insert_query, insert_data)

    except psycopg2.DatabaseError as e:
        print(f"Error: {e}")

    finally:
        if cursor:
            cursor.close()

    return True