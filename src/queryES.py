from elasticsearch import Elasticsearch

# Connect to Elasticsearch
es = Elasticsearch(['https://y7kvewdsj1:rjhw6j2xra@apricot-123116175.us-east-1.bonsaisearch.net:443'])

# Define the search query
query = {
    "query": {
        "term": {
            "author_url.keyword": 'https://www.goodreads.com/author/show/947.William_Shakespeare'
        }
    }
}

# Execute the search query
result = es.search(index="book_index", body=query)

# Process the search results
for hit in result["hits"]["hits"]:
    print(hit["_source"])
