import json
import csv
from multiprocessing import Pool
from datetime import datetime

def process_author_line(line):
    author = json.loads(line.strip())  # Strip newline characters
    url = author.get("url", "")
    name = author.get("name", "")
    birth_date = author.get("birthDate", "null")
    death_date = author.get("deathDate", "null")
    genres = author.get("genres", [])
    influences = author.get("influences", [])
    avg_rating = author.get("avgRating", "null")
    reviews_count = author.get("reviewsCount", "null")
    ratings_count = author.get("ratingsCount", "null")
    about = author.get("about", "null")
    if about:
        about = about.replace(';', '')  # Remove semicolons from the about field
        about = about.replace('"', '')  # Remove double quotes from the description
    author_image = author.get("authorImage", "null")
    return [url, name, birth_date, death_date, genres, influences, avg_rating, reviews_count, ratings_count, about, author_image]


def parse_authors():
    with open("/Users/youssef/Desktop/Bookshelved/BackUp/author_110623_0106.jl", "r") as file:
        lines = file.readlines()

    with Pool() as pool:
        results = pool.map(process_author_line, lines)

    with open("authors.csv", "w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file, delimiter=";", quoting=csv.QUOTE_MINIMAL, quotechar='"', escapechar='\\')
        writer.writerow(["url", "name", "birth_date", "death_date", "genres", "influences", "avg_rating", "reviews_count", "ratings_count", "about", "author_image"])
        writer.writerows(results)

if __name__ == "__main__":
    parse_authors()
