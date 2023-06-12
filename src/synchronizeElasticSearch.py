import psycopg2
from elasticsearch import Elasticsearch
import json

with open('../config/config.json') as file:
    config = json.load(file)

# PostgreSQL credentials
pg_url = config['database_url']
ssl_mode = config['ssl_mode']

# Extract PostgreSQL credentials from URL
pg_credentials = pg_url.split('//')[1].split('@')[0].split(':')
pg_user = pg_credentials[0]
pg_password = pg_credentials[1]
pg_host = pg_url.split('@')[1].split(':')[0]
pg_port = pg_url.split(':')[3].split('/')[0]
pg_database = pg_url.split(':')[3].split('/')[1]

# Extract Elasticsearch URL and credentials from the configuration
es_url = config["elasticsearch_url"]
es_user = es_url.split("://")[1].split(":")[0]
es_password = es_url.split("://")[1].split("@")[0].split(":")[1]
es_host_port = es_url.split("@")[1].split(":")
es_host = es_host_port[0]
es_port = int(es_host_port[1])

# Connect to PostgreSQL
pg_connection = psycopg2.connect(
    host=pg_host,
    port=pg_port,
    database=pg_database,
    user=pg_user,
    password=pg_password,
    sslmode=ssl_mode
)
pg_cursor = pg_connection.cursor()

# Connect to Elasticsearch
es_connection = Elasticsearch(
    hosts=[{'host': es_host, 'port': es_port, 'scheme': 'https'}],
    http_auth=(es_user, es_password),
    timeout=30
)

# Create the Elasticsearch indices if they don't exist
author_index = 'author_index'
book_index = 'book_index'

if not es_connection.indices.exists(index=author_index):
    es_connection.indices.create(index=author_index)

if not es_connection.indices.exists(index=book_index):
    es_connection.indices.create(index=book_index)

# Synchronize "author" table
pg_author_query = 'SELECT * FROM author'
pg_cursor.execute(pg_author_query)
pg_author_data = pg_cursor.fetchall()

for row in pg_author_data:
    doc_id = row[0]  # Assuming the first column is the document ID
    doc = {
        'author_url': row[0],
        'name': row[1],
        'birth_date': row[2],
        'death_date': row[3],
        'genres': row[4],
        'influences': row[5],
        'avg_rating': row[6],
        'reviews_count': row[7],
        'ratings_count': row[8],
        'about': row[9],
        'author_image': row[10]
    }
    es_connection.index(index=author_index, doc_type='_doc', id=doc_id, body=doc)
    print(str(doc_id) + "author synchronized")
# Synchronize "book" table
pg_book_query = 'SELECT * FROM book'
pg_cursor.execute(pg_book_query)
pg_book_data = pg_cursor.fetchall()

for row in pg_book_data:
    doc_id = row[0]  # Assuming the first column is the document ID
    doc = {
        'book_url': row[0],
        'title': row[1],
        'title_complete': row[2],
        'description': row[3],
        'author': row[4],
        'image_url': row[5],
        'genres': row[6],
        'isbn': row[7],
        'isbn13': row[8],
        'publisher': row[9],
        'publish_date': row[10],
        'characters': row[11],
        'places': row[12],
        'rating_histogram': row[13],
        'ratings_count': row[14],
        'reviews_count': row[15],
        'num_pages': row[16],
        'language': row[17],
        'awards': row[18],
        'edition_url': row[19],
        'author_url': row[20] 
    }
    es_connection.index(index=book_index, doc_type='_doc', id=doc_id, body=doc)
    print(str(doc_id) + "book synchronized")

# Close the connections
pg_cursor.close()
pg_connection.close()
