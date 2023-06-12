import logging
import re
import scrapy

from .author_spider import AuthorSpider
from ..items import BookItem, BookLoader
from scrapy.linkextractors import LinkExtractor

class BookSpider(scrapy.Spider):

    name = "book"

    def __init__(self):
        super().__init__()
        self.author_spider = AuthorSpider()

    def parse(self, response, loader=None):
        
        if not loader:
            loader = BookLoader(BookItem(), response=response)
        
        loader.add_value('url', response.request.url)
        loader.add_css('title', 'script#__NEXT_DATA__::text')
        loader.add_css('titleComplete', 'script#__NEXT_DATA__::text')
        loader.add_css('description', 'script#__NEXT_DATA__::text')
        loader.add_css('imageUrl', 'script#__NEXT_DATA__::text')
        loader.add_css('genres', 'script#__NEXT_DATA__::text')
        loader.add_css('asin', 'script#__NEXT_DATA__::text')
        loader.add_css('isbn', 'script#__NEXT_DATA__::text')
        loader.add_css('isbn13', 'script#__NEXT_DATA__::text')
        loader.add_css('publisher', 'script#__NEXT_DATA__::text')
        loader.add_css('series', 'script#__NEXT_DATA__::text')
        loader.add_css('author', 'script#__NEXT_DATA__::text')
        loader.add_css('publishDate', 'script#__NEXT_DATA__::text')

        loader.add_css('characters', 'script#__NEXT_DATA__::text')
        loader.add_css('places', 'script#__NEXT_DATA__::text')
        loader.add_css('ratingHistogram', 'script#__NEXT_DATA__::text')
        loader.add_css("ratingsCount", 'script#__NEXT_DATA__::text')
        loader.add_css("reviewsCount", 'script#__NEXT_DATA__::text')
        loader.add_css('numPages', 'script#__NEXT_DATA__::text')
        loader.add_css("format", 'script#__NEXT_DATA__::text')
        loader.add_css('language', 'script#__NEXT_DATA__::text')
        loader.add_css("awards", 'script#__NEXT_DATA__::text')   
        loader.add_css("edition_url",'script#__NEXT_DATA__::text')
        author_url = response.css('a.ContributorLink::attr(href)').extract_first()
        loader.add_value("author_url",author_url)
        
        #if edition_id:
        #    loader.add_value('edition_id', str(edition_id))
            
        yield loader.load_item()
        

        author_url = response.css('a.ContributorLink::attr(href)').extract_first()
        if author_url is not None:
            yield response.follow(author_url, callback=self.author_spider.parse)
        else:
            self.logger.error("Author URL is None")
        

