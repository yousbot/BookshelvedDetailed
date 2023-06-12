import psycopg2
import json 

def update_table_schema():
    # Set up the database connection
    
    with open('../config/config.json') as file:
        config = json.load(file)

    database_url = config['database_url']
    conn = psycopg2.connect(database_url)
    cursor = conn.cursor()

    # Alter table schema to make columns nullable
    alter_query = """
    ALTER TABLE author_bis
    ALTER COLUMN name DROP NOT NULL,
    ALTER COLUMN birth_date DROP NOT NULL,
    ALTER COLUMN death_date DROP NOT NULL,
    ALTER COLUMN genres DROP NOT NULL,
    ALTER COLUMN influences DROP NOT NULL,
    ALTER COLUMN avg_rating DROP NOT NULL,
    ALTER COLUMN reviews_count DROP NOT NULL,
    ALTER COLUMN ratings_count DROP NOT NULL,
    ALTER COLUMN about DROP NOT NULL,
    ALTER COLUMN author_image DROP NOT NULL;
    """
    cursor.execute(alter_query)

    # Commit changes and close connection
    conn.commit()
    cursor.close()
    conn.close()

if __name__ == "__main__":
    update_table_schema()
