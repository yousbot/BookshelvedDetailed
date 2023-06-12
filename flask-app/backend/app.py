from flask import Flask, request, jsonify
from flask_cors import CORS
from db import Database
import json

app = Flask(__name__, static_folder='../frontend/mybookapp/build')
CORS(app, resources={r"/*": {"origins": "http://localhost:3000"}})

# ES Connection
with open('config.json') as file:
    config = json.load(file)

es_url = config['elasticsearch_url']
db = Database(es_url)

@app.route('/suggest', methods=['POST'])
def suggest_books():
    search_query = request.json['query']
    suggestions = db.get_book_suggestions(search_query)
    return jsonify({'suggestions': suggestions})


@app.route('/book-details', methods=['POST'])
def get_book_details():
    book_url = request.json['bookUrl']
    book_details = db.get_book_details(book_url)
    if book_details:
        author_url = book_details.get('author_url')
        author_details = db.get_author_details(author_url, book_url)
        book_details['authorDetails'] = author_details
        book_edition_url = db.get_book_details(book_url)['edition_url']
        similar_books = db.get_similar_books(book_url, book_edition_url)
        book_details['other_editions'] = db.get_other_editions(book_edition_url)
        print(book_details['other_editions'])
        book_details['genres'] = db.get_book_details(book_url)['genres']
        print(" GENRES APP : " + str(book_details['genres']))
        book_details['similarBooks'] = similar_books
        
    return jsonify({'bookDetails': book_details})



if __name__ == '__main__':
    app.run(debug=True)