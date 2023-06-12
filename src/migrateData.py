import psycopg2
import json

with open('../config/config.json') as file:
    config = json.load(file)

database_url = config['database_url']
ssl_mode = config['ssl_mode']

# Database connection details
ssl_mode = 'require'
def insert_author_data(conn, data):
    
    author_url = data.get("url")
    cursor = conn.cursor()
    
    cursor.execute("SELECT author_url FROM author WHERE author_url = %s", (author_url,))
    existing_author = cursor.fetchone()
    
    if existing_author:
        print(f"Author with URL '{author_url}' already exists. Skipping insertion.")
        return
    
    insert_query = """
        INSERT INTO author (author_url, name, birth_date, death_date, genres, influences, avg_rating, reviews_count,
        ratings_count, about, author_image)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    # Convert dictionary values to appropriate types
    values = (
        data.get("url"),
        data.get("name"),
        data.get("birthDate"),
        data.get("deathDate"),
        data.get("genres"),
        data.get("influences"),
        data.get("avgRating"),
        data.get("reviewsCount"),
        data.get("ratingsCount"),
        data.get("about"),
        data.get("authorImage")
    )
    
    cursor = conn.cursor()
    cursor.execute(insert_query, values)
    conn.commit()
    cursor.close()

# Function to insert data into the Book table
def insert_book_data(conn, data):
    # Convert genres and awards to JSON format
    genres = json.dumps(data.get("genres"))
    awards = json.dumps(data.get("awards"))
    ratingHistogram = json.dumps(data.get("ratingHistogram"))
    characters = json.dumps(data.get("characters"))
    places = json.dumps(data.get("places"))
    author = json.dumps(data.get("author"))

    # Check if the author exists in the "author" table
    author_url = data.get("author_url")
    cursor = conn.cursor()
    cursor.execute("SELECT author_url FROM author WHERE author_url = %s", (author_url,))
    existing_author = cursor.fetchone()
    cursor.close()

    if not existing_author:
        print(f"Author with URL '{author_url}' doesn't exist. Skipping insertion of the book.")
        return

    insert_query = """
        INSERT INTO book (
            book_url, title, title_complete, description, image_url, genres, author, isbn, isbn13, publisher,
            publish_date, characters, places, rating_histogram, ratings_count, reviews_count, num_pages, language,
            awards, edition_url, author_url
        )
        VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
    """
    # Convert dictionary values to appropriate types
    values = (
        data.get("url"),
        data.get("title"),
        data.get("titleComplete"),
        data.get("description"),
        data.get("imageUrl"),
        genres,
        author,
        data.get("isbn"),
        data.get("isbn13"),
        data.get("publisher"),
        data.get("publishDate"),
        characters,
        places,
        ratingHistogram,
        data.get("ratingsCount"),
        data.get("reviewsCount"),
        data.get("numPages"),
        data.get("language"),
        awards,
        data.get("edition_url"),
        data.get("author_url")
    )

    cursor = conn.cursor()
    cursor.execute(insert_query, values)
    conn.commit()
    cursor.close()


    
# Read and insert data from the "author.jl" file
def insert_author_data_from_file(conn, file_path):
    with open(file_path, "r") as file:
        for line in file:
            data = json.loads(line)
            print(data)
            insert_author_data(conn, data)

# Read and insert data from the "book.jl" file
def insert_book_data_from_file(conn, file_path):
    with open(file_path, "r") as file:
        for line in file:
            data = json.loads(line)
            print(data)
            insert_book_data(conn, data)

# Connect to the database
conn = psycopg2.connect(database_url, sslmode=ssl_mode)

# Insert data into the Author table
insert_author_data_from_file(conn, "/Users/youssef/Desktop/Starting Point/authors.txt")

# Insert data into the Book table
insert_book_data_from_file(conn, "/Users/youssef/Desktop/Starting Point/books.txt")

# Close the database connection
conn.close()