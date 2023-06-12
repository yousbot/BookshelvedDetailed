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

# Connect to the database
conn = psycopg2.connect(
    host=host,
    database=database,
    user=user,
    password=password,
    port=port,
    sslmode=ssl_mode
)

# Create a cursor
cursor = conn.cursor()

# Drop a table
table_name = 'author_bis'
cursor.execute(f"DROP TABLE {table_name}")

# Commit the changes
conn.commit()

# Close the cursor and connection
cursor.close()
conn.close()
