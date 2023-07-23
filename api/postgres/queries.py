import psycopg2
import random

def getOneSemiRandomItemKey(app):
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

