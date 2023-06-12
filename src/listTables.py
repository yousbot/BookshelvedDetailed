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

def list_table_structure():
    # Connect to the database
    connection = psycopg2.connect(
        host=host,
        database=database,
        user=user,
        password=password,
        port=port,
        sslmode=ssl_mode
    )

    # Create a cursor object
    cursor = connection.cursor()

    # Get the list of table names
    cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")

    table_names = cursor.fetchall()

    # Iterate over the table names and retrieve the structure
    for table_name in table_names:
        print(f"Table: {table_name[0]}")
        print("------------------------")

        # Get the table structure
        cursor.execute(f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{table_name[0]}'")

        table_structure = cursor.fetchall()

        # Print the table structure
        for column in table_structure:
            print(f"Column: {column[0]}, Data Type: {column[1]}")

        print()

    # Close the cursor and connection
    cursor.close()
    connection.close()

# Call the function to list table structure
list_table_structure()