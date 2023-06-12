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

# SQL statements for table creation
create_author_table_query = """
CREATE TABLE author_bis (
    author_url VARCHAR PRIMARY KEY,
    name VARCHAR,
    birth_date TIMESTAMP,
    death_date TIMESTAMP,
    genres VARCHAR[],
    influences VARCHAR[],
    avg_rating NUMERIC,
    reviews_count INTEGER,
    ratings_count INTEGER,
    about TEXT,
    author_image VARCHAR
);
"""

create_book_table_query = """
CREATE TABLE book_bis (
    book_url VARCHAR PRIMARY KEY,
    title VARCHAR,
    title_complete VARCHAR,
    description TEXT,
    author JSONB,
    image_url VARCHAR,
    genres JSONB,
    isbn VARCHAR,
    isbn13 VARCHAR,
    publisher VARCHAR,
    publish_date BIGINT,
    characters JSONB,
    places JSONB,
    rating_histogram JSONB,
    ratings_count INTEGER,
    reviews_count INTEGER,
    num_pages INTEGER,
    language VARCHAR,
    awards JSONB,
    edition_url VARCHAR,
    author_url VARCHAR REFERENCES author(author_url)
);
"""

def create_tables():
    # Connect to the database
    connection = psycopg2.connect(
        host=host,
        database=database,
        user=user,
        password=password,
        port=port,
        sslmode=ssl_mode
    )

    # Create a cursor
    cursor = connection.cursor()

    try:
        # Create the Author table
        #cursor.execute(create_author_table_query)
        #print("Author table created successfully.")

        # Create the Book table
        cursor.execute(create_book_table_query)
        print("Book table created successfully.")

        # Commit the changes
        connection.commit()

    except (Exception, psycopg2.DatabaseError) as error:
        print("Error creating tables:", error)

    finally:
        # Close the cursor and connection
        cursor.close()
        connection.close()

# Call the function to create the tables
create_tables()
