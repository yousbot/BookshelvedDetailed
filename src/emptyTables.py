import psycopg2
import json

with open('../config/config.json') as file:
    config = json.load(file)

host = config['host']
database = config['database']
user = config['user']
password = config['password']
port = config['port']
ssl_mode = config['ssl_mode']

def truncate_table(conn, table_name):
    cursor = conn.cursor()
    try:

        cursor.execute(f"TRUNCATE TABLE {table_name} CASCADE")
        conn.commit()

        print(f"Table '{table_name}' and its dependents emptied successfully.")
        
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error emptying table '{table_name}' and its dependents:", error)
    finally:
        cursor.close()
        
# Assuming you already have the connection established
conn = psycopg2.connect(
        host=host,
        database=database,
        user=user,
        password=password,
        port=port,
        sslmode=ssl_mode
    )
# Truncate the tables
truncate_table(conn, "author")
truncate_table(conn, "book")
conn.close()
