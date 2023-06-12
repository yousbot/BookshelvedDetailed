import psycopg2
import json
import sys

def fetch_book_urls(database_url, ssl_mode, output_file):
    try:
        # Connect to the database
        conn = psycopg2.connect(database_url, sslmode=ssl_mode)
        cursor = conn.cursor()

        # Execute the query to fetch book URLs
        query = "SELECT book_url FROM book"
        cursor.execute(query)

        # Fetch all rows and write URLs to the output file
        with open(output_file, "w") as file_out:
            for row in cursor.fetchall():
                book_url = row[0]
                file_out.write(book_url + "\n")

        # Close the cursor and connection
        cursor.close()
        conn.close()

    except Exception as e:
        print("Error fetching book URLs:", e)

if __name__ == '__main__':
    with open('config.json') as file:
        config = json.load(file)

    database_url = config['database_url']
    ssl_mode = config['ssl_mode']

    if len(sys.argv) < 2:
        print("Please provide the output file path.")
        print("Usage: python script.py output_file.txt")
        sys.exit(1)

    output_file = sys.argv[1]

    fetch_book_urls(database_url, ssl_mode, output_file)
