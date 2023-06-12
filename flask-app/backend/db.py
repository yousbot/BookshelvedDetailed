from elasticsearch import Elasticsearch

class Database:
    def __init__(self, es_url):
        self.es = Elasticsearch(hosts=[es_url])

    def get_book_suggestions(self, query):
        search_query = {
            "query": {
                "bool": {
                    "should": [
                        {
                            "match": {
                                "title": f"{query}*"
                            }
                        },
                        {
                            "match": {
                                "author": f"{query}*"
                            }
                        }
                    ]
                }
            },
            "size": 10,
            "_source": ["title", "book_url", "edition_url"]  # Include the edition_url field in the response
        }

        response = self.es.search(index='book_index', body=search_query)
        suggestions = []
        seen_edition_urls = set()  # Track the edition URLs already seen

        for hit in response['hits']['hits']:
            source = hit['_source']
            title = source.get('title')  # Retrieve the title field
            author = source.get('author')
            url = source.get('book_url')  # Retrieve the book_url field
            edition_url = source.get('edition_url')  # Retrieve the edition_url field

            if title and url and edition_url:
                # Check if the edition URL has been seen before
                if edition_url not in seen_edition_urls:
                    suggestions.append({'title': title, 'url': url, 'author': author})
                    seen_edition_urls.add(edition_url)

        return suggestions




    def get_author_details(self, author_url, book_url):
        search_query = {
            "query": {
                "match": {
                    "author_url": author_url
                }
            },
            "size": 1
        }

        response = self.es.search(index='author_index', body=search_query)
        if response['hits']['hits']:
            author_details = response['hits']['hits'][0]['_source']
            # Retrieve all books written by the author
            books = self.get_books_by_author(author_url, book_url)
            author_details['books'] = books
        else:
            author_details = None

        return author_details

    def get_book_details(self, suggestion):
        search_query = {
            "query": {
                "match": {
                    "book_url": suggestion
                }
            },
            "size": 1
        }
        response = self.es.search(index='book_index', body=search_query)
        if response['hits']['hits']:
            book_details = response['hits']['hits'][0]['_source']
            author_url = book_details.get('author_url')
            print(book_details['genres'])

            if author_url:
                author_details = self.get_author_details(author_url, suggestion)
                book_details['authorDetails'] = author_details
                # Retrieve other books written by the author
                other_books = self.get_books_by_author(author_details['author_url'],suggestion)
                book_details['authorDetails']['books'] = other_books
        else:
            book_details = None

        return book_details

    def get_books_by_author(self, author_url, book_url):
        search_query = {
            "query": {
                "bool": {
                    "must": [
                            {
                            "term": {
                                "author_url.keyword": author_url
                            }
                        }
                    ],
                    "must_not": [
                        {
                        "term": {
                            "book_url.keyword": book_url
                        }
                    }
                ]
                }
            },
            "size": 5
        }

        response = self.es.search(index='book_index', body=search_query)
        books = []
        unique_books = {}  # Track unique books based on book_url

        for hit in response['hits']['hits']:
            source = hit['_source']
            edition_url = source.get('edition_url')

            if edition_url not in unique_books:
                unique_books[edition_url] = source
                books.append(source)

        return books


    
    def get_similar_books(self,book_url, book_edition_url):
        search_query = {
                    "size": 5,
                    "query": {
                        "bool": {
                            "must": {
                                "more_like_this": {
                                    "fields": ["genres", "description"],
                                    "like": [
                                        {
                                            "_index": "book_index",
                                            "_id": book_url
                                        }
                                    ],
                                    "min_term_freq": 1,
                                    "min_doc_freq": 1
                                }
                            }
                        }
                    },
                    "aggs": {
                        "unique_editions": {
                            "terms": {
                                "field": "edition_url.keyword",
                                "size": 5
                            },
                            "aggs": {
                                "top_hits": {
                                    "top_hits": {
                                        "size": 1
                                    }
                                }
                            }
                        }
                    }
                }
        response = self.es.search(index="book_index", body=search_query)

        # Process the search results
        similar_books = []
        for bucket in response["aggregations"]["unique_editions"]["buckets"]:
            hit = bucket["top_hits"]["hits"]["hits"][0]
            similar_book = hit["_source"]
            similar_books.append(similar_book)
            
        # Print the similar books
        for book in similar_books:
            print(f"Title: {book['title']}")
            print(f"Author: {book['author']}")
            print(f"Description: {book['description']}")
            print("------")
        
        return similar_books
    
        
    def get_other_editions(self, book_edition_url):
        search_query = {
            "size": 5,
            "query": {
                "term": {
                    "edition_url.keyword": book_edition_url
                }
            }
        }

        response = self.es.search(index="book_index", body=search_query)
        books = []
        if response['hits']['hits']:
            books = [hit['_source'] for hit in response['hits']['hits']]
        return books

