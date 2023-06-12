import psycopg2
import json

with open('config.json') as file:
    config = json.load(file)


host = config['host']
database = config['database']
user = config['user']
password = config['password']
port = config['port']
ssl_mode = config['ssl_mode']

def get_table_content(conn, table_name1):
    cursor = conn.cursor()
    cursor.execute(f"SELECT book_url FROM {table_name1}")
    table_content = cursor.fetchall()
    cursor.close()
    return table_content

def get_all_tables(conn):
    cursor = conn.cursor()
    cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
    tables = cursor.fetchall()
    cursor.close()
    return tables

def count_num_authors(conn):
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM author")
    row_count = cursor.fetchone()[0]
    cursor.close()
    return row_count


def close_connection(conn):
    cursor = conn.cursor()
    cursor.execute("SELECT pg_terminate_backend(pg_stat_activity.pid) FROM pg_stat_activity WHERE pg_stat_activity.datname = current_database() AND pid <> pg_backend_pid();")
    conn.commit()
    cursor.close()
    conn.close()


    
def count_num_books(conn):
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM book")
    row_count = cursor.fetchone()[0]
    cursor.close()
    
    return row_count

def get_copy_status(conn):
    
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM pg_stat_progress_copy \watch 1')
    row_count = cursor.fetchone()[0]
    cursor.close()
    return row_count

# Assuming you already have the connection established
conn = psycopg2.connect(
        host=host,
        database=database,
        user=user,
        password=password,
        port=port,
        sslmode=ssl_mode
    )

# scrapy crawl list -a file_path=/Users/youssef/Desktop/Bookshelved/1984_editions.txt --logfile=log.txt

# Call the function to get all tables
#tables = get_all_tables(conn)

# Retrieve and print the content of each table

#table_content = get_table_content(conn,'author_bis')
#for row in table_content:
#    print(row)

#print(get_table_content(conn,"book"))

print("Number of Authors : " + str(count_num_authors(conn)))
print("Number of Books : " + str(count_num_books(conn)))

#print(" Copy status : " + str(get_copy_status(conn)))
# Close the connection
conn.close()
