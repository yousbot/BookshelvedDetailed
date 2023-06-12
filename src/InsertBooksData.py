import psycopg2
import json
import concurrent.futures

with open('../config/config.json') as file:
    config = json.load(file)

database_url = config['database_url']
ssl_mode = config['ssl_mode']

# Function to insert data into the Book table
def insert_book_data(data):
    try:
        # Connect to the database
        conn = psycopg2.connect(database_url, sslmode=ssl_mode)
        cursor = conn.cursor()

        # Disable foreign key checks
        cursor.execute("SET CONSTRAINTS ALL DEFERRED")

        # Convert genres and awards to JSON format
        genres = json.dumps(data.get("genres"))
        awards = json.dumps(data.get("awards"))
        ratingHistogram = json.dumps(data.get("ratingHistogram"))
        characters = json.dumps(data.get("characters"))
        places = json.dumps(data.get("places"))
        author = json.dumps(data.get("author"))

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

        cursor.execute(insert_query, values)
        conn.commit()

        # Enable foreign key checks
        cursor.execute("SET CONSTRAINTS ALL IMMEDIATE")

        # Close the cursor and connection
        cursor.close()
        conn.close()

    except Exception as e:
        print("Error inserting data:", e)

# Read data from the book files
def read_book_data(file_path):
    with open(file_path, "r") as file:
        for line in file:
            data = json.loads(line)
            yield data

# Insert data into the Book table
def insert_book_data_from_files(file_paths):
    with concurrent.futures.ProcessPoolExecutor() as executor:
        for file_path in file_paths:
            try:
                data_generator = read_book_data(file_path)
                executor.map(insert_book_data, data_generator)
            except Exception as e:
                print("Error processing file:", file_path, "-", e)

if __name__ == '__main__':
    # File paths for the book data files
    '''    book_file_paths = [
        "/Users/youssef/Downloads/Bookshelved/BOOKSH/bookshelved/book_.jl",
        "/Users/youssef/Downloads/Bookshelved/BOOKSH2/bookshelved/book_.jl",
        "/Users/youssef/Downloads/Bookshelved/BOOKSH3/bookshelved/book_.jl",
        "/Users/youssef/Downloads/Bookshelved/BOOKSH4/bookshelved/book_.jl",
        "/Users/youssef/Downloads/Bookshelved/BOOKSH5/bookshelved/book_.jl",
        "/Users/youssef/Downloads/Bookshelved/BOOKSH6/bookshelved/book_.jl"
    ]
    '''
    book_file_paths = ["/Users/youssef/Desktop/Bookshelved/BackUp/book_2_120623_1115.jl"]

    # Insert data into the Book table
    insert_book_data_from_files(book_file_paths)
