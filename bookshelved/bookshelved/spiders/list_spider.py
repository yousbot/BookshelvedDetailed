import scrapy
from scrapy import signals
from .book_spider import BookSpider

class ListSpider(scrapy.Spider):
    """Spider to extract URL's of books from a Listopia list on Goodreads"""

    name = "list"

    def _set_crawler(self, crawler):
        super()._set_crawler(crawler)
        if self.item_scraped_callback:
            crawler.signals.connect(self.item_scraped_callback, signal=signals.item_scraped)

    def __init__(self, file_path=None, item_scraped_callback=None, **kwargs):
        super().__init__(**kwargs)
        self.book_spider = BookSpider()
        self.item_scraped_callback = item_scraped_callback

        if file_path:
            with open(file_path, 'r') as file:
                self.start_urls = [url.strip() for url in file.readlines()]

    def parse(self, response):
        yield response.follow(response.url, callback=self.book_spider.parse)
