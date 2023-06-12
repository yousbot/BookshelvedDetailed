import concurrent.futures
import psycopg2
from psycopg2 import sql
import json

with open('../config/config.json') as file:
    config = json.load(file)

host = config['host']
database = config['database']
user = config['user']
password = config['password']
port = config['port']
ssl_mode = config['ssl_mode']

def insert_data(file_path):
    
    try:
        # Connect to the database
        conn = psycopg2.connect(
            host=host,
            database=database,
            user=user,
            password=password,
            sslmode=ssl_mode
        )
        cursor = conn.cursor()
        print("Connected.")

        # Open the file and read the data
        with open(file_path, "r") as file:
            lines = file.readlines()

        # Prepare the INSERT statement
        insert_query = sql.SQL("""
            INSERT INTO author (author_url, name, birth_date, death_date, influences, genres, avg_rating, reviews_count, ratings_count, about, author_image)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """)
        print("SQL Query Prepared.")

        # Insert the data into the database
        line_counter = 0
        for line in lines:
            try:
                # Parse the JSON line and extract the values
                data = json.loads(line)
                values = (
                    data['url'],
                    data['name'],
                    data.get('birthDate', None),
                    data.get('deathDate', None),
                    data.get('influences', None),
                    data.get('genres', None),
                    data.get('avgRating', None),
                    data.get('reviewsCount', None),
                    data.get('ratingsCount', None),
                    data.get('about', None),
                    data.get('authorImage', None)
                )
                print("Inserting " + str(data['url']))

                # Execute the INSERT statement with the values
                cursor.execute(insert_query, values)
                conn.commit()
            except Exception as e:
                print("Error inserting data:", e)
                conn.rollback()  # Rollback the transaction

        # Commit the remaining changes and close the database connection
        print("Committing ...")
        conn.commit()
        print("Closing ...")
        cursor.close()
        conn.close()
        
    except Exception as e:
        print("Error connecting to the database:", e)


def main():
    # List of file paths
    file_paths = [
        "/Users/youssef/Downloads/Bookshelved/BOOKSH/bookshelved/author_.jl",
        "/Users/youssef/Downloads/Bookshelved/BOOKSH2/bookshelved/author_.jl",
        "/Users/youssef/Downloads/Bookshelved/BOOKSH3/bookshelved/author_.jl",
        "/Users/youssef/Downloads/Bookshelved/BOOKSH4/bookshelved/author_.jl",
        "/Users/youssef/Downloads/Bookshelved/BOOKSH5/bookshelved/author_.jl",
        "/Users/youssef/Downloads/Bookshelved/BOOKSH6/bookshelved/author_.jl"

    ]

    # Create a ProcessPoolExecutor with 8 workers
    with concurrent.futures.ProcessPoolExecutor(max_workers=2) as executor:
        # Submit the tasks to the executor
        executor.map(insert_data, file_paths)
    print("Finish.")


if __name__ == "__main__":
    main()
